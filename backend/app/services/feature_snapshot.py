"""特征快照管理领域服务：特征数据集的定时快照、版本追溯与回滚。"""
from __future__ import annotations

from datetime import date
from typing import Any

from app.services import archive_flow
from app.services.base import BaseModuleService
from app.store import store


class FeatureSnapshotService(BaseModuleService):
    MODULE = "feature_snapshot"
    ENTITY = "特征快照"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["快照编号", "特征集", "快照日期", "特征数量", "数据行数", "存储路径", "保留天数", "快照状态"]
    REQUIRED_FIELDS = ["快照编号", "特征集", "快照日期"]
    STATUS_ORDER = ["草稿", "创建中", "已完成", "已过期"]
    ACTION_RULES = {"创建快照": "创建中", "确认完成": "已完成", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []
    # 归档与回填是流程动作，不走简单状态流转表。
    DOMAIN_ACTIONS = ("发起归档", "确认归档", "查询归档", "恢复回填", "回查归档来源")

    def list_entries(self, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        """列表输出前补齐分区引用，保证每条快照都带可验证的回填分区。"""
        rows, total = super().list_entries(**kwargs)
        for row in rows:
            archive_flow.ensure_partition_reference(row)
        return rows, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = super().get_entry(entry_id)
        if entry is not None:
            archive_flow.ensure_partition_reference(entry)
        return entry

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记快照时即分配分区引用，后续归档迁移只搬位置、不改引用。"""
        entry, missing = super().create_entry(values)
        if entry is not None:
            archive_flow.ensure_partition_reference(entry)
        return entry, missing

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """工作台上的归档/恢复动作落到同一套归档状态机上。"""
        if action == "发起归档":
            result = self.begin_snapshot_archive(entry_id)
            return store.find(self.MODULE, entry_id) if result["ok"] else None, result["message"]
        if action == "确认归档":
            result = self.confirm_archive(entry_id)
            return store.find(self.MODULE, entry_id) if result["ok"] else None, result["message"]
        if action == "恢复回填":
            result = self.restore_snapshot_to_compute(entry_id)
            entry = store.find(self.MODULE, entry_id) if result["ok"] else None
            return entry, result.get("message", "")
        return super().run_action(entry_id, action)

    # ── 链路1: 特征计算 → 特征快照 ──

    def archive_expired_snapshots(self, commit: bool = False) -> dict[str, Any]:
        """将超过保留天数的快照归档。

        默认只发起迁移：分区引用被冻结、归档位置选定，但迁移停在 50%，
        此时计算任务不能把该快照当作完整输入。传入 commit=True（或在此后
        调用 confirm_archive）才把迁移收尾为“已归档”。
        """
        with store.lock:
            rows = store.rows(self.MODULE)
            started: list[int] = []
            already: list[int] = []
            skipped: list[dict[str, Any]] = []
            today = date.today()
            for snap in rows:
                if not archive_flow.is_expired(snap, today):
                    continue
                archive_flow.ensure_partition_reference(snap)
                sid = int(snap.get("id", 0))
                if snap.get("归档状态") == archive_flow.ARCHIVE_MIGRATING and not commit:
                    skipped.append({"snapshot_id": sid, "reason": "归档迁移已在进行中"})
                    continue
                if snap.get("归档状态") == archive_flow.ARCHIVE_DONE:
                    already.append(sid)
                    continue
                if commit:
                    archive_flow.begin_archive(snap)
                    archive_flow.complete_archive(snap)
                else:
                    archive_flow.begin_archive(snap)
                started.append(sid)
            return {
                "ok": True,
                "archived_count": len(started),
                "expired_ids": started,
                "already_archived": already,
                "skipped": skipped,
                "pending_confirmation": len(started) if not commit else 0,
                "archive_state": archive_flow.ARCHIVE_DONE if commit else archive_flow.ARCHIVE_MIGRATING,
            }

    def begin_snapshot_archive(self, snapshot_id: int, operator: str = "operator") -> dict[str, Any]:
        """把单个快照置为迁移中，模拟“移动到一半”的中间态。"""
        with store.lock:
            snap = store.find(self.MODULE, snapshot_id)
            if snap is None:
                return {"ok": False, "message": f"快照 {snapshot_id} 不存在"}
            result = archive_flow.begin_archive(snap, operator)
            return {"ok": True, **result, "snapshot_id": snapshot_id,
                    "归档状态": snap.get("归档状态"), "归档进度": snap.get("归档进度"),
                    "分区可验证": snap.get("分区可验证"),
                    "message": "归档迁移已发起，迁移完成前该快照不作为完整输入"}

    def confirm_archive(self, snapshot_id: int, operator: str = "operator") -> dict[str, Any]:
        """确认迁移完成：归档状态流转为已归档，分区重新进入可计算范围。"""
        with store.lock:
            snap = store.find(self.MODULE, snapshot_id)
            if snap is None:
                return {"ok": False, "message": f"快照 {snapshot_id} 不存在"}
            result = archive_flow.complete_archive(snap, operator)
            return {"ok": True, **result, "snapshot_id": snapshot_id,
                    "归档状态": snap.get("归档状态"), "归档进度": snap.get("归档进度"),
                    "分区可验证": snap.get("分区可验证"),
                    "released_backfill_tasks": [
                        t.get("id") for t in store.rows("feature_compute")
                        if t.get("回填键") and t.get("输入就绪")
                        and int(t.get("输入快照", 0) or 0) == snapshot_id
                    ],
                    "message": "归档迁移已完成，分区引用可验证"}

    def archive_status(self, snapshot_id: int) -> dict[str, Any]:
        """查询归档迁移进度，以及当前引用是否落在可计算范围。"""
        snap = store.find(self.MODULE, snapshot_id)
        if snap is None:
            return {"ok": False, "message": f"快照 {snapshot_id} 不存在"}
        check = archive_flow.verify_partition_reference(snap)
        tasks = [
            {"compute_id": t.get("id"), "backfill_key": t.get("回填键"),
             "status": t.get("status"), "input_ready": t.get("输入就绪")}
            for t in store.rows("feature_compute")
            if t.get("回填键") and archive_flow.backfill_key(snap) == t.get("回填键")
        ]
        return {
            "ok": True,
            "snapshot_id": snapshot_id,
            "归档状态": snap.get("归档状态"),
            "归档进度": snap.get("归档进度"),
            "归档位置": archive_flow.archive_location(snap),
            "回填分区": archive_flow.partition_reference(snap),
            "分区校验码": snap.get("分区校验码"),
            "partition_readable": check.get("ok"),
            "reason": check.get("reason", ""),
            "backfill_tasks": tasks,
        }

    def snapshot_feature_set(self, feature_set: str, compute_id: int,
                             snapshot_date: str | None = None) -> dict[str, Any]:
        """对指定特征集创建数据快照并存储到snapshots目录。"""
        with store.lock:
            rows = store.rows(self.MODULE)
            next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
            feats = feature_set.split(",") if feature_set else []
            snap_date = snapshot_date or "2026-11-18"
            row = {
                "id": next_id,
                "status": "已完成",
                "pending": False,
                "abnormal": False,
                "快照编号": f"SNAP-C{compute_id:04d}-{next_id:04d}",
                "特征集": feature_set,
                "快照日期": snap_date,
                "特征数量": len(feats),
                "数据行数": str(len(feats) * 10000),
                "存储路径": f"snapshots/feature_set_{next_id}.json",
                "保留天数": "30",
                "快照状态": "已完成",
                "来源计算任务": compute_id,
            }
            archive_flow.ensure_partition_reference(row)
            rows.append(row)
            return {"ok": True, "snapshot_id": next_id, "feature_count": len(feats),
                    "回填分区": row["回填分区"]}

    def restore_snapshot_to_compute(self, snapshot_id: int,
                                    reuses_compute_id: int | None = None) -> dict[str, Any]:
        """从历史快照恢复特征集版本，重新触发计算任务进行回填。

        同一快照、同一原日期范围只生成一笔回填：重复恢复会复用已有任务，
        输入引用和输出分区保持不变，避免失败重试后生成两份互相覆盖的数据。
        """
        with store.lock:
            snap = store.find(self.MODULE, snapshot_id)
            if snap is None:
                return {"ok": False, "message": f"快照 {snapshot_id} 不存在",
                        "restored_from": snapshot_id}
            archive_flow.ensure_partition_reference(snap)

            task = archive_flow.find_backfill_task(snap)
            if reuses_compute_id is not None:
                candidate = store.find("feature_compute", reuses_compute_id)
                if candidate is not None:
                    if task is not None and task.get("id") != candidate.get("id"):
                        return {
                            "ok": False,
                            "message": "该快照已存在进行中的回填任务，输入的版本不一致",
                            "restored_from": snapshot_id,
                            "conflict_compute_id": task.get("id"),
                        }
                    task = candidate

            if task is not None:
                check = archive_flow.verify_partition_reference(snap)
                if not check.get("ok"):
                    # 迁移中途：不新建、不复用成功回执，把任务挂起等待归档完成。
                    task["status"] = "等待中"
                    task["pending"] = True
                    task["abnormal"] = False
                    task["输入就绪"] = False
                    task["归档状态"] = snap.get("归档状态")
                    return {
                        "ok": False,
                        "resumed": True,
                        "compute_id": task.get("id"),
                        "restored_from": snapshot_id,
                        "input_ready": False,
                        "archive_state": snap.get("归档状态"),
                        "input_partition": task.get("输入分区"),
                        "output_partition": task.get("输出分区"),
                        "message": f"归档迁移未完成（{snap.get('归档进度')}%），"
                                   f"回填任务 {task.get('id')} 已挂起，待归档确认后恢复",
                    }
                task["输入就绪"] = True
                task["归档状态"] = snap.get("归档状态")
                task["归档引用"] = check.get("reference")
                task["abnormal"] = False
                return {
                    "ok": True,
                    "resumed": True,
                    "restored_from": snapshot_id,
                    "compute_id": task.get("id"),
                    "input_ready": True,
                    "input_partition": task.get("输入分区"),
                    "output_partition": task.get("输出分区"),
                    "input_version": snap.get("分区校验码"),
                    "archive_state": snap.get("归档状态"),
                    "message": "已复用同一笔回填，输入版本与输出分区保持不变",
                }

            compute_rows = store.rows("feature_compute")
            next_id = max((int(r.get("id", 0)) for r in compute_rows), default=0) + 1
            check = archive_flow.verify_partition_reference(snap)
            task = {
                "id": next_id,
                "status": "草稿" if check.get("ok") else "等待中",
                "pending": True,
                "abnormal": False,
                "任务编号": f"COMP-RST-{next_id:04d}",
                "关联特征": snap.get("特征集", ""),
                "数据切片": snap.get("数据日期范围") or "restored_slice",
                "计算引擎": "spark",
                "输入分区": archive_flow.partition_reference(snap),
                "输出分区": archive_flow.backfill_output_partition(snap),
                "并行度": "4",
                "任务状态": "草稿" if check.get("ok") else "等待中",
                "回填键": archive_flow.backfill_key(snap),
                "输入快照": snapshot_id,
                "来源快照": snapshot_id,
                "归档引用": archive_flow.partition_reference(snap),
                "输入版本": snap.get("分区校验码"),
                "输入就绪": bool(check.get("ok")),
                "归档状态": snap.get("归档状态"),
            }
            compute_rows.append(task)
            result = {
                "ok": bool(check.get("ok")),
                "restored_from": snapshot_id,
                "compute_id": next_id,
                "input_ready": bool(check.get("ok")),
                "input_partition": task["输入分区"],
                "output_partition": task["输出分区"],
                "input_version": snap.get("分区校验码"),
                "archive_state": snap.get("归档状态"),
            }
            if check.get("ok"):
                result["message"] = "快照已恢复并创建回填计算任务"
            else:
                result["message"] = (
                    f"快照归档迁移未完成（{snap.get('归档进度')}%），"
                    f"回填任务 {next_id} 已登记但输入未就绪"
                )
            return result

    def merge_snapshots(self, snapshot_ids: list[int]) -> dict[str, Any]:
        """将多个特征快照归并为一个综合快照，合并特征集并写入新版本。"""
        with store.lock:
            all_features = set()
            sources = []
            for sid in snapshot_ids:
                snap = store.find(self.MODULE, sid)
                if snap is None:
                    continue
                sources.append(sid)
                feats = snap.get("特征集", "")
                for f in feats.split(","):
                    if f.strip():
                        all_features.add(f.strip())
            rows = store.rows(self.MODULE)
            next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
            merged_feats = ",".join(sorted(all_features))
            row = {
                "id": next_id,
                "status": "已完成",
                "pending": False,
                "abnormal": False,
                "快照编号": f"SNAP-MRG-{next_id:04d}",
                "特征集": merged_feats,
                "快照日期": date.today().isoformat(),
                "特征数量": len(all_features),
                "数据行数": str(len(all_features) * 8000),
                "存储路径": f"snapshots/merged_{next_id}.json",
                "保留天数": "60",
                "快照状态": "已完成",
                "来源快照": ",".join(str(s) for s in sources),
            }
            archive_flow.ensure_partition_reference(row)
            rows.append(row)
            return {"ok": True, "merged_id": next_id, "total_features": len(all_features),
                    "sources": snapshot_ids, "回填分区": row["回填分区"]}
