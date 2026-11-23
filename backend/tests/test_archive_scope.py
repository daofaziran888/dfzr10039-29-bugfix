"""数据归档范围与凭证测试：错配修复、B1/B10 区分、冲突、并发和历史回执。"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

import pytest

from app.integration import archive_batch
from app.models import (
    ArchiveFormat,
    ArchiveScopeCredential,
    StorageBatch,
)
from app.services import service_for
from app.store import store


def _archive_service():
    return service_for("data_archive")


def _resource_router():
    from app.routers import resource

    return resource


@pytest.fixture(autouse=True)
def _reset_archive_state():
    import copy

    batch_snapshot = copy.deepcopy(store.rows("batch_store"))
    store.rows("data_archive").clear()
    store.rows("_archive_credential").clear()
    yield
    store.rows("batch_store")[:] = batch_snapshot
    store.rows("data_archive").clear()
    store.rows("_archive_credential").clear()


def _batch(batch_id: str = "B1", job_id: str = "J1", rows: int = 10) -> StorageBatch:
    return StorageBatch(
        batch_id=batch_id, source_job_id=job_id, row_count=rows,
        compression="gzip", checksum=f"sum-{batch_id}", status="校验通过",
    )


def _policy(policy_id: str, scope: str, fmt: str = "gzip", days: str = "90",
            status: str = "待触发", row_id: int = 1) -> dict:
    return {
        "id": row_id, "策略编号": policy_id, "数据范围": scope,
        "归档周期": "monthly", "归档格式": fmt, "保留时长": days,
        "存储位置": f"/archive/{policy_id}/", "压缩方式": fmt,
        "最后归档": "", "status": status, "pending": False, "abnormal": False,
    }


def _batch_row(batch_id: str, job_id: str = "J1", row_id: int = 1) -> dict:
    return {
        "id": row_id, "批次编号": batch_id, "来源作业": job_id,
        "数据行数": 10, "存储路径": f"/data/{batch_id}/", "压缩格式": "gzip",
        "Schema版本": "v1", "校验摘要": f"sum-{batch_id}", "入库时间": "2026-11-01",
        "status": "校验通过", "pending": False, "abnormal": False,
    }


def test_archive_without_any_policy_does_not_raise() -> None:
    """策略为空：按批次自身范围归档，不再报 TypeError。"""
    credential = _archive_service().confirm_archive(_batch())

    assert credential.policy_id == "ARCH-B1"
    assert credential.scope == "batch:B1"
    assert credential.is_default is True
    assert credential.expires_at, "应按保留时长算出实际到期时点"


def test_archive_ignores_unrelated_policy() -> None:
    """仅有不相关政策：不能套用其格式和保留期。"""
    store.rows("data_archive").append(
        _policy("DATA-OTHER", "OTHER-JOB", fmt="lz4", days="7")
    )

    credential = _archive_service().confirm_archive(_batch())

    assert credential.policy_id == "ARCH-B1"
    assert credential.archive_format == "gzip"
    assert credential.retention_days == 90
    assert credential.is_default is True


def test_archive_batch_policy_beats_job_policy() -> None:
    """批次特定策略与作业级策略同时命中时用批次策略。"""
    store.rows("data_archive").extend([
        _policy("DATA-B1", "B1", fmt="lz4", days="30", row_id=1),
        _policy("DATA-J1", "job:J1", fmt="zstd", days="7", row_id=2),
    ])

    credential = _archive_service().confirm_archive(_batch())

    assert credential.policy_id == "DATA-B1"
    assert credential.archive_format == "lz4"
    assert credential.retention_days == 30
    applied = [item for item in credential.conflicts if item.get("applied")]
    assert [item["policy_id"] for item in applied] == ["DATA-B1"]


def test_conflicting_retention_resolves_to_single_value() -> None:
    """同层策略有效期冲突：只保留一个有效保留期，并在凭证里留冲突明细。"""
    store.rows("data_archive").extend([
        _policy("DATA-J1-A", "job:J1", fmt="gzip", days="7", row_id=1),
        _policy("DATA-J1-B", "job:J1", fmt="lz4", days="30", row_id=2),
    ])

    credential = _archive_service().confirm_archive(_batch())

    assert credential.retention_days == 30, "冲突时取更长的实际保留期"
    assert credential.archive_format == "lz4"
    assert len(credential.conflicts) == 2
    assert sum(1 for item in credential.conflicts if item["applied"]) == 1


def test_b1_policy_does_not_touch_b10() -> None:
    """批次策略 "B1" 不能因为前缀相同把 B10 一起归档。"""
    store.rows("batch_store").extend([
        _batch_row("B1", "J1", 901),
        _batch_row("B10", "J1", 902),
    ])
    store.rows("data_archive").append(_policy("DATA-B1", "B1", fmt="lz4", days="30"))
    service = _archive_service()

    service.confirm_archive(StorageBatch.from_seed_row(store.find("batch_store", 901)))

    assert service.batch_storage_detail("B1")["归档范围"] == "batch:B1"
    b10_detail = service.batch_storage_detail("B10")
    assert b10_detail.get("归档策略") is None
    assert b10_detail.get("到期时间") is None
    assert service.active_credential("B10") is None
    assert service.cleanup_scope("B10") is None, "B10 不应因 B1 的归档产生凭证"


def test_job_policy_covers_both_batches() -> None:
    """作业级策略对 B1、B10 都生效，但各自签发独立凭证。"""
    store.rows("data_archive").append(_policy("DATA-J1", "job:J1", fmt="zstd", days="7"))
    service = _archive_service()

    first = service.confirm_archive(_batch("B1"))
    second = service.confirm_archive(_batch("B10"))

    assert first.policy_id == second.policy_id == "DATA-J1"
    assert first.credential_id != second.credential_id
    assert first.scope == "job:J1" and second.scope == "job:J1"


def test_concurrent_confirm_keeps_one_credential() -> None:
    """并发确认同一批次：只保存一次有效归档结果和起始时间。"""
    store.rows("data_archive").append(_policy("DATA-J1", "job:J1", days="10"))
    service = _archive_service()
    batch = _batch("B1")
    results: list[ArchiveScopeCredential] = []
    barrier = threading.Barrier(6)

    def work() -> None:
        barrier.wait()
        results.append(service.confirm_archive(batch))

    threads = [threading.Thread(target=work) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len({item.credential_id for item in results}) == 1
    assert len({item.archived_at for item in results}) == 1, "起始时间只能有一个"
    stored = service.credentials_for_batch("B1")
    assert len([item for item in stored if item.active]) == 1


def test_confirm_is_idempotent_for_later_calls() -> None:
    """重复确认返回同一份凭证，不覆盖起始时间与到期时点。"""
    service = _archive_service()
    first = service.confirm_archive(_batch(), archived_at="2026-11-01T00:00:00+00:00")
    second = service.confirm_archive(
        _batch(), archived_at="2026-12-01T00:00:00+00:00", retention_days=1,
    )

    assert second.credential_id == first.credential_id
    assert second.archived_at == "2026-11-01T00:00:00+00:00"
    assert second.expires_at == first.expires_at
    assert second.retention_days == first.retention_days


def test_storage_list_and_credential_share_one_source() -> None:
    """批次存储清单展示策略、摘要与到期时点，且与凭证完全一致。"""
    store.rows("batch_store").append(_batch_row("B1"))
    store.rows("data_archive").append(_policy("DATA-B1", "B1", fmt="lz4", days="30"))
    service = _archive_service()

    credential = service.confirm_archive(
        StorageBatch.from_seed_row(store.find("batch_store", 1))
    )
    detail = service.batch_storage_detail("B1")

    assert detail["归档策略"] == "DATA-B1"
    assert detail["归档凭证"] == credential.credential_id
    assert detail["归档格式"] == "lz4"
    assert detail["保留时长"] == 30
    assert detail["到期时间"] == credential.expires_at
    assert detail["归档摘要"] == credential.checksum
    listed = service.list_entries(page=1, size=10)[0]
    assert listed, "策略清单应仍可读"


def test_cleanup_reads_effective_format_and_scope() -> None:
    """清理动作读取实际生效的格式和范围，无凭证不得清理。"""
    store.rows("batch_store").append(_batch_row("B1", "J1", 77))
    service = _archive_service()
    assert service.cleanup_scope("B1") is None

    store.rows("data_archive").append(_policy("DATA-J1", "job:J1", fmt="zstd", days="7"))
    service.confirm_archive(StorageBatch.from_seed_row(store.find("batch_store", 77)))
    scope = service.cleanup_scope("B1")

    assert scope["archive_format"] == "zstd"
    assert scope["scope"] == "job:J1"
    assert scope["expires_at"]
    assert scope["checksum"]


def test_legacy_receipt_row_is_readable() -> None:
    """历史回执（旧字段命名）兼容读取，且只给下游一个有效保留期。"""
    store.rows("batch_store").append(_batch_row("OLD-9", "J9", 88))
    store.rows("_archive_credential").append({
        "id": 5, "策略编号": "DATA-OLD", "数据范围": "OLD-9",
        "归档格式": "ArchiveFormat.LZ4", "保留时长": "15天",
        "最后归档": "2026-01-02T00:00:00+00:00", "status": "已完成",
    })
    service = _archive_service()

    credential = service.active_credential("OLD-9")
    assert credential is not None
    assert credential.archive_format == "lz4"
    assert credential.retention_days == 15
    assert credential.batch_id == "OLD-9"
    assert credential.expires_at == "2026-01-17T00:00:00+00:00"
    scope = service.cleanup_scope("OLD-9")
    assert scope["retention_days"] == 15


def test_legacy_snapshot_without_credential_table() -> None:
    """已写入批次清单但没有凭证行时，历史回执仍可读。"""
    row = _batch_row("OLD-8", "J8", 89)
    row.update({
        "归档策略": "DATA-OLD8", "归档凭证": "ARCH-CRED-OLD-8",
        "归档范围": "batch:OLD-8", "归档格式": "zstd", "保留时长": 20,
        "到期时间": "2026-02-01T00:00:00+00:00", "归档摘要": "legacy-sum",
    })
    store.rows("batch_store").append(row)
    service = _archive_service()

    detail = service.batch_storage_detail("OLD-8")
    assert detail["归档格式"] == "zstd"
    assert detail["到期时间"] == "2026-02-01T00:00:00+00:00"
    assert service.cleanup_scope("OLD-8")["scope"] == "batch:OLD-8"


def test_integration_archive_batch_returns_matched_policy() -> None:
    """流水线调用走同一套匹配逻辑，不再返回无关策略。"""
    store.rows("data_archive").append(_policy("DATA-B1", "B1", fmt="lz4", days="30"))

    policy = archive_batch(_batch())

    assert policy.policy_id == "DATA-B1"
    assert policy.archive_format == ArchiveFormat.LZ4
    assert policy.retention_days == 30


def test_trigger_action_by_batch_policy_targets_only_that_batch() -> None:
    """从归档配置入口触发：批次策略只归档 B1，B10 不产生记录。"""
    store.rows("batch_store").extend([
        _batch_row("B1", "J1", 11), _batch_row("B10", "J1", 12),
    ])
    store.rows("data_archive").append(_policy("DATA-B1", "B1", fmt="lz4", days="30"))
    service = _archive_service()

    entry, message = service.trigger_archive(1)

    assert entry is not None
    assert entry["status"] == "归档中"
    assert "ARCH-CRED-B1" in message
    assert service.active_credential("B1") is not None
    assert service.active_credential("B10") is None
    assert store.find("batch_store", 12).get("归档凭证") is None


def test_trigger_action_job_policy_can_target_explicit_batch() -> None:
    """作业级策略触发时显式指定批次，另一批次不受影响。"""
    store.rows("batch_store").extend([
        _batch_row("B1", "J1", 21), _batch_row("B10", "J1", 22),
    ])
    store.rows("data_archive").append(_policy("DATA-J1", "job:J1", days="7"))
    service = _archive_service()

    entry, message = service.trigger_archive(1, batch_id="B10")

    assert entry is not None
    assert "ARCH-CRED-B10" in message
    assert service.active_credential("B10") is not None
    assert service.active_credential("B1") is None


def test_expiry_uses_archived_at_and_retention() -> None:
    """实际到期时点 = 确认时间 + 实际保留天数。"""
    store.rows("data_archive").append(_policy("DATA-B1", "B1", days="30"))
    credential = _archive_service().confirm_archive(
        _batch(), archived_at="2026-11-01T08:00:00+00:00",
    )

    assert credential.expires_at == "2026-12-01T08:00:00+00:00"
    assert datetime.fromisoformat(credential.expires_at) > datetime(
        2026, 11, 1, tzinfo=timezone.utc,
    )


def test_router_exposes_archive_detail_and_confirm() -> None:
    """路由层提供归档确认与批次存储详情入口。"""
    from app.main import app

    paths = app.openapi()["paths"]
    assert "/api/batch_store/{entry_id}/archive" in paths
    assert "/api/batch_store/{entry_id}/cleanup-scope" in paths
    assert "/api/data_archive/confirm" in paths
    assert "/api/data_archive/{entry_id}/archive" in paths


def test_batch_store_list_fields_include_archive_receipt() -> None:
    """批次存储清单字段包含归档回执列，页面可直接核对。"""
    fields = service_for("batch_store").LIST_FIELDS
    for field in ("归档策略", "归档凭证", "归档范围", "归档格式", "保留时长", "到期时间", "归档摘要"):
        assert field in fields
