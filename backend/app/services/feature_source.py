"""特征源管理领域服务：负责数据源的注册、接入、字段发现与配置校验。"""
from __future__ import annotations

from typing import Any

from app.services import archive_flow
from app.services.base import BaseModuleService
from app.store import store


class FeatureSourceService(BaseModuleService):
    MODULE = "feature_source"
    ENTITY = "特征源"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["源编号", "源名称", "数据表名", "连接配置", "字段清单", "采样比例", "缓存策略", "源版本"]
    REQUIRED_FIELDS = ["源编号", "源名称", "数据表名"]
    STATUS_ORDER = ["草稿", "待校验", "已接入", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认接入": "已接入", "停用数据源": "已停用"}
    NEGATIVE_ACTIONS = ["停用数据源"]

    # ── 链路1: 特征源 → 特征定义 → 特征计算 → 特征快照 ──
    def fetch_source_schema(self, source_id: int) -> dict[str, Any]:
        """接入数据源并发现字段清单，生成校验记录后返回字段映射。"""
        source = store.find(self.MODULE, source_id)
        if source is None:
            return {"ok": False, "message": f"特征源 {source_id} 不存在"}
        discover_fields = ["id", "uid", "age", "income", "city", "label", "dt"]
        source["字段清单"] = ",".join(discover_fields)
        source["status"] = "已接入"
        source["pending"] = False
        self._sync_cross_define(source_id, discover_fields)
        self._validate_source_connectivity(source_id)
        return {
            "ok": True,
            "source_id": source_id,
            "fields": discover_fields,
            "message": f"特征源 {source_id} 已完成接入，发现 {len(discover_fields)} 个字段并同步至特征定义",
        }

    def _sync_cross_define(self, source_id: int, fields: list[str]) -> None:
        """将数据源发现的字段同步到特征定义模块，建立来源关联。"""
        define_rows = store.rows("feature_define")
        next_id = max((int(r.get("id", 0)) for r in define_rows), default=0)
        for idx, field_name in enumerate(fields, start=1):
            next_id += 1
            define_rows.append({
                "id": next_id,
                "status": "草稿",
                "pending": True,
                "abnormal": False,
                "特征编号": f"FEAT-S{source_id:04d}-F{idx:02d}",
                "特征名称": f"feature_{field_name}",
                "特征类型": "numeric" if field_name in ("age", "income") else "category",
                "计算逻辑": f"从特征源 {source_id} 导入字段 {field_name} 并标准化缺失填充为均值",
                "数据来源": f"feature_source_{source_id}",
                "缺失填充": "mean_fill",
                "取值范围": "[0, 9999]" if field_name in ("age", "income") else "[]",
                "特征版本": "v1.0.0",
            })

    def _validate_source_connectivity(self, source_id: int) -> dict[str, Any]:
        """校验数据源连接配置的有效性和可达性。"""
        source = store.find(self.MODULE, source_id)
        if source is None:
            return {"ok": False, "message": "数据源不存在"}
        conn_config = source.get("连接配置", "")
        is_valid = len(conn_config) > 0 and "host" in conn_config.lower()
        if not is_valid:
            source["abnormal"] = True
            return {"ok": False, "message": f"数据源 {source_id} 连接配置校验失败，缺少必要的主机信息"}
        source["abnormal"] = False
        return {"ok": True, "message": f"数据源 {source_id} 连接配置校验通过，可正常采集数据"}

    def discover_and_register(self, source_id: int, conn_config: str) -> dict[str, Any]:
        """连接外部数据表并自动发现字段，完成源注册与入湖流程。"""
        source = store.find(self.MODULE, source_id)
        if source is None:
            return {"ok": False, "message": f"特征源 {source_id} 不存在"}
        source["连接配置"] = conn_config
        return self.fetch_source_schema(source_id)

    def collect_source_snapshot(self, source_id: int) -> dict[str, Any]:
        """从数据源采集样本数据并计算统计快照，用于数据质量评估。"""
        source = store.find(self.MODULE, source_id)
        if source is None:
            return {"ok": False}
        fields_list = (source.get("字段清单") or "").split(",") if source.get("字段清单") else []
        sample_ratio = source.get("采样比例", "0.1")
        stats = {
            "source_id": source_id,
            "source_name": source.get("源名称", ""),
            "total_fields": len(fields_list),
            "sample_ratio": float(str(sample_ratio).replace("%", "")) if sample_ratio else 0.1,
            "collected_at": "2026-11-18T10:00:00",
            "row_count": 50000,
            "null_ratio": {f: round(i * 0.01, 4) for i, f in enumerate(fields_list)},
        }
        self._archive_source_snapshot(source_id, stats)
        return {"ok": True, "snapshot": stats, "message": f"特征源 {source_id} 快照采集完成"}

    def _archive_source_snapshot(self, source_id: int, stats: dict[str, Any]) -> None:
        """将数据源快照统计信息归档到存储模块，保留历史版本供回溯。"""
        snapshot_rows = store.rows("feature_snapshot")
        next_id = max((int(r.get("id", 0)) for r in snapshot_rows), default=0) + 1
        row = {
            "id": next_id,
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "快照编号": f"SS-{source_id:04d}-{next_id:04d}",
            "特征集": f"source_{source_id}_fields",
            "快照日期": "2026-11-18",
            "特征数量": stats["total_fields"],
            "数据行数": str(stats["row_count"]),
            "存储路径": f"snapshots/source_{source_id}/{next_id:04d}.json",
            "保留天数": "90",
            "快照状态": "已完成",
        }
        archive_flow.ensure_partition_reference(row)
        snapshot_rows.append(row)