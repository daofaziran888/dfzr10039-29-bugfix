"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class BatchStoreService(BaseModuleService):
    MODULE = "batch_store"
    ENTITY = "采集批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "批次编号", "来源作业", "数据行数", "存储路径", "压缩格式", "Schema版本", "校验摘要", "入库时间",
        # 归档回执同步列：存储清单直接展示实际生效的策略、摘要和到期时点。
        "归档策略", "归档凭证", "归档范围", "归档格式", "保留时长", "到期时间", "归档摘要",
    ]
    REQUIRED_FIELDS = ["批次编号", "来源作业", "数据行数"]
    STATUS_ORDER = ["采集中", "已落盘", "校验通过", "校验失败"]
    ACTION_RULES = {"触发校验": "已落盘", "标记通过": "校验通过", "标记失败": "校验失败"}
    NEGATIVE_ACTIONS = []

    def archive_detail(self, batch_id: str) -> dict[str, Any] | None:
        """批次存储详情：从同一份归档范围凭证取数并核对实际到期时点。"""
        from app.services import service_for

        archive = service_for("data_archive")
        return archive.batch_storage_detail(batch_id)

    def cleanup_scope(self, batch_id: str) -> dict[str, Any] | None:
        """清理前读取实际生效的归档格式和范围。"""
        from app.services import service_for

        archive = service_for("data_archive")
        return archive.cleanup_scope(batch_id)
