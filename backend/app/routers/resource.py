"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.models import StorageBatch
from app.schemas import (
    ActionPayload, ActionResult, ArchiveConfirmPayload, EntryPayload, PageResult,
)
from app.services import service_for
from app.store import store

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


def _batch_row_by_entry(entry_id: int) -> tuple[Any, str]:
    service = require_service("batch_store")
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"采集批次 {entry_id} 不存在或已归档")
    return service, str(entry.get("批次编号", ""))


@router.get("/batch_store/{entry_id}/archive", response_model=dict)
def get_batch_archive_detail(entry_id: int) -> dict[str, Any]:
    """批次存储详情：按批次号取当前有效归档范围凭证，核对实际到期时点。"""
    service, batch_id = _batch_row_by_entry(entry_id)
    detail = service.archive_detail(batch_id) if batch_id else None
    if detail is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_id or entry_id} 尚无归档回执")
    return detail


@router.post("/data_archive/confirm", response_model=ActionResult)
def confirm_archive(payload: ArchiveConfirmPayload) -> ActionResult:
    """归档配置入口的确认提交：按实际策略匹配范围并签发归档范围凭证。"""
    service = require_service("data_archive")
    if not payload.batch_id:
        return ActionResult(ok=False, message="缺少批次编号，无法确认归档")

    batch_row = next(
        (row for row in store.rows("batch_store")
         if str(row.get("批次编号", "")) == payload.batch_id),
        None,
    )
    if batch_row is None:
        return ActionResult(ok=False, message=f"批次 {payload.batch_id} 不存在")

    credential = service.confirm_archive(
        StorageBatch.from_seed_row(batch_row),
        archive_format=payload.archive_format,
        retention_days=payload.retention_days,
        archived_at=payload.archived_at,
    )
    return ActionResult(
        ok=True,
        message=f"归档范围凭证已签发：{credential.credential_id}",
        entry=credential.to_row(),
    )


@router.get("/batch_store/{entry_id}/cleanup-scope", response_model=dict)
def get_cleanup_scope(entry_id: int) -> dict[str, Any]:
    """清理动作前置检查：读取实际生效的归档格式和范围。"""
    service, batch_id = _batch_row_by_entry(entry_id)
    scope = service.cleanup_scope(batch_id) if batch_id else None
    if scope is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_id or entry_id} 无有效归档凭证")
    return scope


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.post("/data_archive/{entry_id}/archive", response_model=ActionResult)
def trigger_policy_archive(
    entry_id: int, payload: ArchiveConfirmPayload | None = None,
) -> ActionResult:
    """归档配置入口：从某条策略触发归档，提交后按实际范围签发凭证。"""
    service = require_service("data_archive")
    batch_id = payload.batch_id if payload else None
    entry, message = service.trigger_archive(entry_id, batch_id=batch_id)
    return ActionResult(ok=entry is not None, message=message, entry=entry)
