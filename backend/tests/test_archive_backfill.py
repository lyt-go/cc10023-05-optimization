"""快照归档与回填恢复的可靠性测试。

覆盖：迁移中途的归档状态、部分输入不得返回成功、同笔回填复用输入/输出、
归档完成后挂起任务恢复、以及从任务回查来源快照和派生结果。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.seed import SEED_ROWS
from app.services import service_for
from app.store import store


def reset_store() -> None:
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}


def assert_ok(result, msg=""):
    assert result.get("ok"), f"{msg}: {result}"
    return result


def make_expired_snapshot(feature_set: str = "f_age,f_income,f_city",
                          snapshot_date: str = "2026-08-01",
                          retain_days: str = "7") -> int:
    snap_svc = service_for("feature_snapshot")
    created = snap_svc.snapshot_feature_set(feature_set, compute_id=1,
                                            snapshot_date=snapshot_date)
    snapshot_id = created["snapshot_id"]
    snap = store.find("feature_snapshot", snapshot_id)
    snap["保留天数"] = retain_days
    return snapshot_id


def test_archive_midway_blocks_partial_success():
    """归档停在迁移中途时，恢复回执和计算回执都不能算成功。"""
    reset_store()
    snap_svc = service_for("feature_snapshot")
    compute_svc = service_for("feature_compute")

    snapshot_id = make_expired_snapshot()
    archived = snap_svc.archive_expired_snapshots()
    assert snapshot_id in archived["expired_ids"], archived

    # 迁移只完成一半：归档状态是迁移中，分区不可读。
    status = snap_svc.archive_status(snapshot_id)
    assert status["归档状态"] == "迁移中", status
    assert status["归档进度"] == 50, status
    assert status["partition_readable"] is False, status

    # 另一位操作员此时发起恢复：任务可以登记，但回执不能宣称可计算。
    restored = snap_svc.restore_snapshot_to_compute(snapshot_id)
    assert restored["ok"] is False, restored
    assert restored["input_ready"] is False, restored
    assert restored.get("compute_id"), restored
    compute_id = restored["compute_id"]
    input_partition = restored["input_partition"]
    output_partition = restored["output_partition"]

    # 直接执行也必须被拒，且不能生成派生快照、不能给出成功回执。
    snapshots_before = len(store.rows("feature_snapshot"))
    executed = compute_svc.execute_and_snapshot(compute_id)
    assert executed["ok"] is False, executed
    assert executed.get("partial_inputs"), executed
    assert len(store.rows("feature_snapshot")) == snapshots_before, "不得产出部分输入的快照"
    task = store.find("feature_compute", compute_id)
    assert task["status"] == "已失败", task
    assert task["abnormal"] is True, task
    assert "输入分区不完整" in task["失败原因"], task

    # 重试同样不能进入计算窗口，仍沿用原输入/输出分区。
    retried = compute_svc.retry_failed_tasks([compute_id])
    assert compute_id in retried["failed_skipped"] or retried["waiting_for_archive"], retried
    task = store.find("feature_compute", compute_id)
    assert task["status"] == "等待中", task
    assert task["输入分区"] == input_partition, task
    assert task["输出分区"] == output_partition, task


def test_archive_completion_releases_same_backfill():
    """归档状态流转为已归档后，同一笔回填沿用原版本与输出分区恢复。"""
    reset_store()
    snap_svc = service_for("feature_snapshot")
    compute_svc = service_for("feature_compute")

    snapshot_id = make_expired_snapshot()
    snap_svc.begin_snapshot_archive(snapshot_id, operator="oncall_a")
    first = snap_svc.restore_snapshot_to_compute(snapshot_id)
    assert first["ok"] is False, first
    compute_id = first["compute_id"]
    input_partition = first["input_partition"]
    output_partition = first["output_partition"]

    # 归档收尾：状态从迁移中流转为已归档，引用验证通过。
    confirmed = snap_svc.confirm_archive(snapshot_id, operator="oncall_a")
    assert_ok(confirmed, "confirm_archive")
    assert confirmed["归档状态"] == "已归档", confirmed
    assert confirmed["归档进度"] == 100, confirmed
    assert confirmed["分区可验证"] is True, confirmed
    status = snap_svc.archive_status(snapshot_id)
    assert status["partition_readable"] is True, status

    # 再次恢复：必须复用同一任务，输入版本和输出分区一一对应。
    second = snap_svc.restore_snapshot_to_compute(snapshot_id)
    assert_ok(second, "restore_after_archive")
    assert second["resumed"] is True, second
    assert second["compute_id"] == compute_id, second
    assert second["input_partition"] == input_partition, second
    assert second["output_partition"] == output_partition, second
    assert second["input_version"] == store.find("feature_snapshot", snapshot_id)["分区校验码"]

    backfills = [
        row for row in store.rows("feature_compute")
        if row.get("回填键") == store.find("feature_compute", compute_id)["回填键"]
    ]
    assert len(backfills) == 1, f"同一快照只应有一笔回填: {backfills}"

    resumed = compute_svc.resume_backfill(compute_id)
    assert_ok(resumed, "resume_backfill")
    assert resumed["output_partition"] == output_partition, resumed
    executed = compute_svc.execute_and_snapshot(compute_id)
    assert_ok(executed, "execute_after_archive")
    derived = store.find("feature_snapshot", executed["snapshot_id"])
    assert derived["来源计算任务"] == compute_id, derived
    assert derived["来源快照"] == str(snapshot_id), derived
    assert derived["归档引用"] == input_partition, derived

    # 从任务回查原快照、归档位置和后续派生结果。
    trace = compute_svc.trace_backfill(compute_id)
    assert_ok(trace, "trace_backfill")
    assert trace["original_snapshot"]["id"] == snapshot_id, trace
    assert trace["archive"]["state"] == "已归档", trace
    assert trace["archive"]["verified"] is True, trace
    assert [s["id"] for s in trace["derived_snapshots"]] == [executed["snapshot_id"]], trace


def test_retry_after_failure_does_not_duplicate_output():
    """恢复失败后继续同一笔回填：输入版本与输出分区必须对得上。"""
    reset_store()
    snap_svc = service_for("feature_snapshot")
    compute_svc = service_for("feature_compute")

    snapshot_id = make_expired_snapshot()
    snap_svc.begin_snapshot_archive(snapshot_id)
    first = snap_svc.restore_snapshot_to_compute(snapshot_id)
    compute_id = first["compute_id"]
    input_partition = first["input_partition"]
    output_partition = first["output_partition"]

    compute_svc.execute_and_snapshot(compute_id)  # 失败
    snap_svc.confirm_archive(snapshot_id)
    retry = compute_svc.retry_failed_tasks([compute_id])
    assert compute_id in retry["retried"], retry
    task = store.find("feature_compute", compute_id)
    assert task["输入分区"] == input_partition, task
    assert task["输出分区"] == output_partition, task

    again = snap_svc.restore_snapshot_to_compute(snapshot_id)
    assert again["compute_id"] == compute_id, again
    assert again["input_partition"] == input_partition, again
    assert again["output_partition"] == output_partition, again
    assert_ok(compute_svc.execute_and_snapshot(compute_id), "execute")
    outputs = [
        row for row in store.rows("feature_compute")
        if row.get("输出分区") == output_partition
    ]
    assert len(outputs) == 1, f"输出分区只应有一份数据: {outputs}"


def test_restore_conflicting_input_version_rejected():
    """用另一笔任务的输入版本继续回填时必须显式报冲突，而不是悄悄改写。"""
    reset_store()
    snap_svc = service_for("feature_snapshot")
    compute_svc = service_for("feature_compute")

    snapshot_id = make_expired_snapshot()
    snap_svc.begin_snapshot_archive(snapshot_id)
    first = snap_svc.restore_snapshot_to_compute(snapshot_id)
    snap_svc.confirm_archive(snapshot_id)

    other = store.rows("feature_compute")[0]
    conflict = snap_svc.restore_snapshot_to_compute(
        snapshot_id, reuses_compute_id=other["id"])
    assert conflict["ok"] is False, conflict
    assert conflict["conflict_compute_id"] == first["compute_id"], conflict


def test_unrelated_snapshot_not_archived():
    """未到期的快照保持未归档，引用可直接读取。"""
    reset_store()
    snap_svc = service_for("feature_snapshot")
    snapshot_id = make_expired_snapshot(snapshot_date="2026-11-20", retain_days="30")
    archived = snap_svc.archive_expired_snapshots()
    assert snapshot_id not in archived["expired_ids"], archived
    status = snap_svc.archive_status(snapshot_id)
    assert status["归档状态"] == "未归档", status
    assert status["partition_readable"] is True, status


if __name__ == "__main__":
    tests = [
        test_archive_midway_blocks_partial_success,
        test_archive_completion_releases_same_backfill,
        test_retry_after_failure_does_not_duplicate_output,
        test_restore_conflicting_input_version_rejected,
        test_unrelated_snapshot_not_archived,
    ]
    passed = failed = 0
    for test in tests:
        try:
            test()
            print(f"[PASS] {test.__name__}")
            passed += 1
        except Exception as exc:  # noqa: BLE001
            print(f"[FAIL] {test.__name__}: {exc}")
            failed += 1
    print(f"\n总测试: {len(tests)}, 通过: {passed}, 失败: {failed}")
    sys.exit(1 if failed else 0)
