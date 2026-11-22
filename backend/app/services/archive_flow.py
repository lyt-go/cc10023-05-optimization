# -*- 数据工程模块：archive_flow.py 处理快照归档、回填分区引用与输入校验 -*-
"""快照归档与回填的共享规则：分区引用、迁移状态机、输入就绪判定。

归档不是把行删掉，而是把特征快照从在线路径搬到归档路径。迁移过程中
分区引用必须保持稳定、可校验，回填任务才能继续按原日期范围读取。这里把
“引用如何生成、迁移到哪一步、计算能不能读”集中在一处，快照服务和计算
服务都只依赖这些规则，避免两边各自解释归档状态。
"""
from __future__ import annotations

import hashlib
import re
from datetime import date
from typing import Any

from app.store import store

# 归档状态机：未归档 → 迁移中 → 已归档
ARCHIVE_NOT_STARTED = "未归档"
ARCHIVE_MIGRATING = "迁移中"
ARCHIVE_DONE = "已归档"

ARCHIVE_ROOT = "archive/snapshots"
# 过渡目录只在迁移中使用，完成前不会被计算任务当作可读输入。
ARCHIVE_STAGING = "archive/.staging"

BACKFILL_TASK_PREFIX = "COMP-BF"

_SNAPSHOT_IN_REF = re.compile(r"snapshots?/(\d+)|snapshot_(\d+)")
_RETAIN_DAYS = re.compile(r"^(\d+(?:\.\d+)?)\s*(?:天|d|days?)?$", re.IGNORECASE)


def _snapshot_date(snap: dict[str, Any]) -> str:
    return str(snap.get("快照日期", "") or "")[:10] or "unknown"


def _snapshot_id(snap: dict[str, Any]) -> int:
    try:
        return int(snap.get("id", 0) or 0)
    except (TypeError, ValueError):
        return 0


def retention_days(snap: dict[str, Any]) -> int:
    """解析保留天数；示例数据里的占位文本按 90 天处理，避免误判到期。"""
    raw = snap.get("保留天数", 90)
    try:
        return int(float(str(raw)))
    except (ValueError, TypeError):
        pass
    match = _RETAIN_DAYS.match(str(raw).strip())
    if match:
        return int(float(match.group(1)))
    return 90


def is_expired(snap: dict[str, Any], today: date | None = None) -> bool:
    """按快照日期与保留天数判断是否已到归档时点。"""
    try:
        snap_date = date.fromisoformat(_snapshot_date(snap))
    except (ValueError, TypeError):
        return False
    return ((today or date.today()) - snap_date).days > retention_days(snap)


def partition_reference(snap: dict[str, Any]) -> str:
    """回填分区引用：带原快照日期，归档前后都指向同一份数据。"""
    return str(snap.get("回填分区") or
               f"feature_snapshot/snapshot_{_snapshot_id(snap)}/dt={_snapshot_date(snap)}")


def partition_digest(snap: dict[str, Any]) -> str:
    """分区校验码只由快照身份和原日期范围决定，迁移不会改变它。"""
    payload = "|".join([
        str(_snapshot_id(snap)),
        _snapshot_date(snap),
        str(snap.get("特征集", "")),
        partition_reference(snap),
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def archive_location(snap: dict[str, Any]) -> str:
    sid = _snapshot_id(snap)
    return str(snap.get("归档位置") or f"{ARCHIVE_ROOT}/{sid}/dt={_snapshot_date(snap)}/")


def ensure_partition_reference(snap: dict[str, Any]) -> dict[str, Any]:
    """补齐可验证的分区引用字段；重复调用结果不变，供各条创建路径复用。"""
    if not snap.get("回填分区"):
        snap["回填分区"] = (
            f"feature_snapshot/snapshot_{_snapshot_id(snap)}/dt={_snapshot_date(snap)}"
        )
    snap.setdefault("数据日期范围", f"{_snapshot_date(snap)}..{_snapshot_date(snap)}")
    if not snap.get("分区校验码"):
        snap["分区校验码"] = partition_digest(snap)
    snap.setdefault("归档状态", ARCHIVE_NOT_STARTED)
    snap.setdefault("归档位置", archive_location(snap))
    snap.setdefault("归档进度", 0)
    snap.setdefault("分区可验证", False)
    return snap


def verify_partition_reference(snap: dict[str, Any]) -> dict[str, Any]:
    """校验分区引用是否仍可被计算读取。

    未归档时在线路径可读；迁移中一律不可读（此时归档位置只写了一半）；
    已归档要求迁移走到 100% 且校验码一致。任何一项不满足都返回 ok=False，
    计算任务据此拒绝给出“成功回执”。
    """
    ensure_partition_reference(snap)
    state = snap.get("归档状态", ARCHIVE_NOT_STARTED)
    expected = partition_digest(snap)
    digest_ok = snap.get("分区校验码") == expected
    if not digest_ok:
        return {"ok": False, "state": state, "reason": "分区校验码不一致",
                "reference": partition_reference(snap)}
    if state == ARCHIVE_MIGRATING:
        return {"ok": False, "state": state, "reason": "归档迁移中，分区尚未完整",
                "reference": partition_reference(snap),
                "progress": snap.get("归档进度", 0)}
    if state == ARCHIVE_DONE:
        complete = bool(snap.get("分区可验证")) and int(snap.get("归档进度", 0) or 0) >= 100
        return {"ok": complete, "state": state,
                "reason": "" if complete else "归档迁移未完成，分区不可验证",
                "reference": partition_reference(snap),
                "archive_location": archive_location(snap),
                "progress": snap.get("归档进度", 0)}
    return {"ok": True, "state": ARCHIVE_NOT_STARTED, "reason": "",
            "reference": partition_reference(snap), "archive_location": archive_location(snap)}


def begin_archive(snap: dict[str, Any], operator: str = "scheduler") -> dict[str, Any]:
    """冻结分区引用并把快照置为迁移中；重复调用幂等。"""
    ensure_partition_reference(snap)
    if snap.get("归档状态") == ARCHIVE_DONE:
        return {"ok": True, "already_archived": True, "state": ARCHIVE_DONE}
    snap["归档状态"] = ARCHIVE_MIGRATING
    # 迁移只完成了一半：归档位置已选定，但分区还不能被计算读取。
    snap["归档进度"] = 50
    snap["分区可验证"] = False
    snap["归档位置"] = archive_location(snap)
    snap["归档暂存位置"] = f"{ARCHIVE_STAGING}/{_snapshot_id(snap)}/"
    snap["归档操作人"] = operator
    return {"ok": True, "already_archived": False, "state": ARCHIVE_MIGRATING,
            "reference": partition_reference(snap)}


def complete_archive(snap: dict[str, Any], operator: str = "scheduler") -> dict[str, Any]:
    """收尾迁移：校验分区引用、置为已归档，并让挂起的回填任务恢复可计算。"""
    ensure_partition_reference(snap)
    if snap.get("归档状态") == ARCHIVE_DONE and snap.get("分区可验证"):
        _release_backfill_tasks(snap)
        return {"ok": True, "already_archived": True, "state": ARCHIVE_DONE,
                "reference": partition_reference(snap)}
    if snap.get("归档状态") != ARCHIVE_MIGRATING:
        begin_archive(snap, operator)
    snap["归档进度"] = 100
    snap["分区可验证"] = True
    snap["分区校验码"] = partition_digest(snap)
    snap["归档状态"] = ARCHIVE_DONE
    snap["归档暂存位置"] = ""
    snap["归档操作人"] = operator
    snap["归档完成时间"] = date.today().isoformat()
    if is_expired(snap):
        snap["status"] = "已过期"
        snap["pending"] = False
        snap["abnormal"] = False
        snap["快照状态"] = "已过期"
    _release_backfill_tasks(snap)
    return {"ok": True, "already_archived": False, "state": ARCHIVE_DONE,
            "reference": partition_reference(snap),
            "archive_location": archive_location(snap)}


def _release_backfill_tasks(snap: dict[str, Any]) -> None:
    """归档完成后，把等待同一快照的回填任务重新标记为输入就绪。"""
    sid = _snapshot_id(snap)
    for task in store.rows("feature_compute"):
        if _task_source_ids(task) != {sid}:
            continue
        if not task.get("回填键"):
            continue
        task["输入就绪"] = True
        task["归档状态"] = snap.get("归档状态")
        if task.get("status") in ("草稿", "等待中"):
            task["status"] = "等待中"
            task["pending"] = True
        task["abnormal"] = False


def backfill_key(snap: dict[str, Any]) -> str:
    """同一快照、同一原日期范围只对应一笔回填；重试沿用它而不是另起任务。"""
    return f"BF-{_snapshot_id(snap)}-{_snapshot_date(snap)}"


def backfill_output_partition(snap: dict[str, Any]) -> str:
    """输出分区由输入版本决定，重试不会写到第二个输出位置。"""
    return f"backfill/output/snapshot_{_snapshot_id(snap)}/dt={_snapshot_date(snap)}"


def find_backfill_task(snap: dict[str, Any]) -> dict[str, Any] | None:
    key = backfill_key(snap)
    sid = _snapshot_id(snap)
    for task in store.rows("feature_compute"):
        if task.get("回填键") == key:
            return task
        if _task_source_ids(task) == {sid} and str(task.get("任务编号", "")).startswith("COMP-RST"):
            return task
    return None


def _refs_from_value(value: Any) -> list[int]:
    """从任务字段里解析快照 id；兼容整数、逗号列表和归档路径。"""
    found: list[int] = []
    tokens = value if isinstance(value, (list, tuple, set)) else str(value or "").split(",")
    for token in tokens:
        text = str(token).strip()
        if not text:
            continue
        if text.isdigit():
            found.append(int(text))
            continue
        for match in _SNAPSHOT_IN_REF.finditer(text):
            sid = match.group(1) or match.group(2)
            if sid:
                found.append(int(sid))
    return found


def _task_source_ids(task: dict[str, Any]) -> set[int]:
    ids: set[int] = set()
    for field in ("输入快照", "来源快照"):
        ids.update(_refs_from_value(task.get(field)))
    for field in ("输入分区", "归档引用", "回填分区"):
        ids.update(_refs_from_value(task.get(field)))
    return ids


def resolve_task_inputs(task: dict[str, Any]) -> dict[str, Any]:
    """按回填分区引用核对该任务的全部输入。

    只要任务引用了托管快照，就必须逐个验证；任何一个处于迁移中、缺失或
    校验失败，整体都算“部分输入”，计算任务不能返回成功。
    """
    source_ids = sorted(_task_source_ids(task))
    if not source_ids:
        # 未引用托管快照的任务沿用原有输入路径，不参与归档迁移判定。
        return {"ready": True, "managed": False, "source_snapshot_ids": [],
                "inputs": [{"reference": task.get("输入分区", ""), "verified": True,
                            "kind": "legacy"}],
                "partial_inputs": [], "reason": ""}
    inputs: list[dict[str, Any]] = []
    partial: list[dict[str, Any]] = []
    for sid in source_ids:
        snap = store.find("feature_snapshot", sid)
        if snap is None:
            item = {"snapshot_id": sid, "reference": "", "verified": False,
                    "reason": "快照不存在或已归档丢失"}
            inputs.append(item)
            partial.append(item)
            continue
        check = verify_partition_reference(snap)
        item = {"snapshot_id": sid, "reference": check.get("reference"),
                "archive_state": check.get("state"), "verified": bool(check.get("ok")),
                "reason": check.get("reason", "")}
        expected_ref = task.get("归档引用")
        if expected_ref and str(expected_ref) not in ("", "None"):
            if str(expected_ref) != str(check.get("reference")):
                item["verified"] = False
                item["reason"] = "任务记录的归档引用与快照当前引用不一致"
        inputs.append(item)
        if not item["verified"]:
            partial.append(item)
    ready = not partial
    reason = "" if ready else "；".join(
        f"快照 {p.get('snapshot_id')}：{p.get('reason')}" for p in partial
    )
    return {"ready": ready, "managed": True, "source_snapshot_ids": source_ids,
            "inputs": inputs, "partial_inputs": partial, "reason": reason}


def trace_task(task_id: int) -> dict[str, Any]:
    """从回填任务反查来源快照、归档引用和后续派生结果。"""
    task = store.find("feature_compute", task_id)
    if task is None:
        return {"ok": False, "message": f"计算任务 {task_id} 不存在"}
    source_ids = sorted(_task_source_ids(task))
    snapshots = [store.find("feature_snapshot", sid) for sid in source_ids]
    snapshots = [s for s in snapshots if s is not None]
    source = snapshots[0] if snapshots else None
    derived_snapshots = [
        row for row in store.rows("feature_snapshot")
        if int(row.get("来源计算任务", 0) or 0) == task_id
    ]
    corr = [
        row for row in store.rows("feature_corr")
        if int(row.get("关联计算任务", 0) or 0) == task_id
    ]
    drift = [
        row for row in store.rows("drift_monitor")
        if int(row.get("关联计算任务", 0) or 0) == task_id
    ]
    archive = None
    if source is not None:
        ensure_partition_reference(source)
        check = verify_partition_reference(source)
        archive = {
            "state": source.get("归档状态"),
            "location": archive_location(source),
            "reference": partition_reference(source),
            "digest": source.get("分区校验码"),
            "verified": check.get("ok"),
            "progress": source.get("归档进度"),
        }
    return {
        "ok": True,
        "task": task,
        "backfill_key": task.get("回填键"),
        "source_snapshots": snapshots,
        "original_snapshot": source,
        "archive": archive,
        "derived_snapshots": derived_snapshots,
        "derived": {
            "feature_snapshot": derived_snapshots,
            "feature_corr": corr,
            "drift_monitor": drift,
        },
        "input_partition": task.get("输入分区"),
        "output_partition": task.get("输出分区"),
        "input_ready": resolve_task_inputs(task)["ready"],
    }
