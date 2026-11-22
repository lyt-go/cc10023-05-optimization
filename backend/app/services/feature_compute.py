"""特征计算任务领域服务：批量计算的编排、分区管理与执行追踪。"""
from __future__ import annotations

from datetime import date
from typing import Any

from app.services import archive_flow
from app.services.base import BaseModuleService
from app.store import store


class FeatureComputeService(BaseModuleService):
    MODULE = "feature_compute"
    ENTITY = "计算任务"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["任务编号", "关联特征", "数据切片", "计算引擎", "输入分区", "输出分区", "并行度", "任务状态"]
    REQUIRED_FIELDS = ["任务编号", "关联特征", "数据切片"]
    STATUS_ORDER = ["草稿", "等待中", "计算中", "已完成", "已失败"]
    ACTION_RULES = {"提交执行": "草稿", "停止计算": "计算中", "确认完成": "已完成"}
    NEGATIVE_ACTIONS = []

    # ── 链路1: 特征计算 → 特征快照 ──
    # ── 链路5: 特征计算 → 相关性矩阵 → 特征漂移监控 ──
    def execute_and_snapshot(self, task_id: int) -> dict[str, Any]:
        """执行特征计算任务并在完成后自动生成特征快照存储到snapshots目录。

        执行前按任务的分区引用核对全部输入：只要有一个来源快照还在归档
        迁移中或校验不通过，就返回 ok=False，绝不给出“只有部分输入”的成功回执。
        """
        with store.lock:
            task = store.find(self.MODULE, task_id)
            if task is None:
                return {"ok": False, "message": f"计算任务 {task_id} 不存在"}
            readiness = archive_flow.resolve_task_inputs(task)
            if not readiness["ready"]:
                task["status"] = "已失败" if task.get("回填键") else task.get("status", "草稿")
                task["abnormal"] = True
                task["pending"] = False
                task["输入就绪"] = False
                task["失败原因"] = "输入分区不完整或不可验证"
                if task.get("回填键"):
                    task["任务状态"] = "已失败"
                return {
                    "ok": False,
                    "task_id": task_id,
                    "partial_inputs": readiness["partial_inputs"],
                    "inputs": readiness["inputs"],
                    "message": "输入分区不完整，未进入计算：" + readiness["reason"],
                }

            task["status"] = "已完成"
            task["pending"] = False
            task["abnormal"] = False
            task["输入就绪"] = True
            task["任务状态"] = "已完成"
            snapshot_rows = store.rows("feature_snapshot")
            next_id = max((int(r.get("id", 0)) for r in snapshot_rows), default=0) + 1
            feats = task.get("关联特征", "unknown")
            source_ids = readiness["source_snapshot_ids"]
            source = store.find("feature_snapshot", source_ids[0]) if source_ids else None
            row = {
                "id": next_id,
                "status": "已完成",
                "pending": False,
                "abnormal": False,
                "快照编号": f"SNAP-{next_id:04d}",
                "特征集": feats,
                "快照日期": date.today().isoformat(),
                "特征数量": 12,
                "数据行数": "100000",
                "存储路径": f"snapshots/compute_{task_id}_{next_id}.json",
                "保留天数": "30",
                "快照状态": "已完成",
                "来源计算任务": task_id,
            }
            archive_flow.ensure_partition_reference(row)
            if source is not None:
                row["来源快照"] = ",".join(str(s) for s in source_ids)
                row["归档引用"] = archive_flow.partition_reference(source)
                row["归档状态"] = source.get("归档状态")
                row["输入版本"] = source.get("分区校验码")
            snapshot_rows.append(row)
            if task.get("回填键"):
                task["回填结果快照"] = next_id
            return {
                "ok": True,
                "task_id": task_id,
                "snapshot_id": next_id,
                "source_snapshot_ids": source_ids,
                "archive_reference": row.get("归档引用", ""),
                "message": f"计算任务 {task_id} 已完成，生成特征快照 {next_id}",
            }

    def verify_task_inputs(self, task_id: int) -> dict[str, Any]:
        """核对计算任务的分区引用是否都能读取；供值班人员在发起计算前确认。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"计算任务 {task_id} 不存在"}
        readiness = archive_flow.resolve_task_inputs(task)
        return {
            "ok": readiness["ready"],
            "task_id": task_id,
            "managed": readiness["managed"],
            "source_snapshot_ids": readiness["source_snapshot_ids"],
            "inputs": readiness["inputs"],
            "partial_inputs": readiness["partial_inputs"],
            "message": "" if readiness["ready"] else readiness["reason"],
        }

    def compute_correlation_matrix(self, task_id: int) -> dict[str, Any]:
        """基于特征计算结果计算特征间相关性矩阵并写入存储。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        corr_rows = store.rows("feature_corr")
        next_id = max((int(r.get("id", 0)) for r in corr_rows), default=0) + 1
        feats = task.get("关联特征", "")
        corr_rows.append({
            "id": next_id,
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "矩阵编号": f"CORR-{next_id:04d}",
            "特征集": feats,
            "计算方法": "pearson",
            "阈值筛选": "0.7",
            "可视化配置": "heatmap_full",
            "计算时间": "2026-11-18T10:00:00",
            "矩阵版本": "v1.0.0",
            "结果存储": f"corr_matrix/compute_{task_id}_v1.json",
            # 派生结果回指计算任务，便于从任务回查后续产物。
            "关联计算任务": task_id,
            "来源快照": task.get("输入快照", ""),
            "归档引用": task.get("归档引用", ""),
        })
        return {"ok": True, "corr_id": next_id, "task_id": task_id}

    def trigger_drift_monitoring(self, task_id: int) -> dict[str, Any]:
        """基于特征计算结果触发特征漂移监控，关联基准分布并启动PSI计算。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        drift_rows = store.rows("drift_monitor")
        next_id = max((int(r.get("id", 0)) for r in drift_rows), default=0) + 1
        feats = task.get("关联特征", "")
        drift_rows.append({
            "id": next_id,
            "status": "监控中",
            "pending": False,
            "abnormal": False,
            "监控编号": f"DRIFT-{next_id:04d}",
            "关联特征": feats,
            "基准分布": f"baseline_compute_{task_id}",
            "当前分布": f"current_{task_id}_2026_11_18",
            "PSI值": "0.015",
            "漂移阈值": "0.25",
            "采样窗口": "30d",
            "监控状态": "监控中",
            "关联计算任务": task_id,
            "来源快照": task.get("输入快照", ""),
            "归档引用": task.get("归档引用", ""),
        })
        return {"ok": True, "drift_id": next_id, "task_id": task_id, "psi_value": 0.015}

    def retry_failed_tasks(self, task_ids: list[int]) -> dict[str, Any]:
        """重试失败的计算任务，将状态回退到草稿并重新调度采集。

        回填任务重试前会重新核对输入；归档还没完成就不能回到计算窗口，
        任务保持等待中并记录原因，输入版本与输出分区都不改写。
        """
        retried = []
        failed = []
        waiting = []
        for tid in task_ids:
            task = store.find(self.MODULE, tid)
            if task is None:
                failed.append(tid)
                continue
            if task.get("status") != "已失败":
                failed.append(tid)
                continue
            readiness = archive_flow.resolve_task_inputs(task)
            task["abnormal"] = False
            task["pending"] = True
            task.pop("失败原因", None)
            if not readiness["ready"]:
                task["status"] = "等待中"
                task["输入就绪"] = False
                task["任务状态"] = "等待中"
                waiting.append({"task_id": tid, "reason": readiness["reason"]})
                continue
            task["status"] = "草稿"
            task["输入就绪"] = True
            task["任务状态"] = "草稿"
            retried.append(tid)
        return {
            "ok": len(retried) > 0 or len(waiting) > 0,
            "retried": retried,
            "failed_skipped": failed,
            "waiting_for_archive": waiting,
        }

    def resume_backfill(self, task_id: int) -> dict[str, Any]:
        """归档确认后恢复同一笔回填：沿用原输入版本和原输出分区。"""
        with store.lock:
            task = store.find(self.MODULE, task_id)
            if task is None:
                return {"ok": False, "message": f"计算任务 {task_id} 不存在"}
            if not task.get("回填键"):
                return {"ok": False, "message": "该任务不是回填任务"}
            readiness = archive_flow.resolve_task_inputs(task)
            if not readiness["ready"]:
                task["输入就绪"] = False
                return {"ok": False, "task_id": task_id,
                        "partial_inputs": readiness["partial_inputs"],
                        "message": readiness["reason"]}
            task["status"] = "等待中"
            task["pending"] = True
            task["abnormal"] = False
            task["输入就绪"] = True
            task["任务状态"] = "等待中"
            return {"ok": True, "task_id": task_id, "backfill_key": task.get("回填键"),
                    "input_partition": task.get("输入分区"),
                    "output_partition": task.get("输出分区"),
                    "input_version": task.get("输入版本"),
                    "inputs": readiness["inputs"],
                    "message": "输入分区已可验证，回填任务恢复进入计算窗口"}

    def trace_backfill(self, task_id: int) -> dict[str, Any]:
        """从回填任务回查原快照、归档位置与派生结果。"""
        return archive_flow.trace_task(task_id)

    def query_compute_progress(self, task_id: int) -> dict[str, Any]:
        """查询特征计算任务的执行进度、引擎状态和资源消耗情况。"""
        task = store.find(self.MODULE, task_id)
        if task is None:
            return {"ok": False}
        readiness = archive_flow.resolve_task_inputs(task)
        return {
            "ok": True,
            "task_id": task_id,
            "status": task.get("status"),
            "engine": task.get("计算引擎"),
            "parallelism": task.get("并行度"),
            "input": task.get("输入分区"),
            "output": task.get("输出分区"),
            "input_ready": readiness["ready"],
            "partial_inputs": readiness["partial_inputs"],
            "archive_state": task.get("归档状态"),
            "backfill_key": task.get("回填键"),
        }
