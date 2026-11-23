"""数据归档领域逻辑：策略匹配、范围确认、凭证签发和保留期冲突处理。

归档不是"随便挑一条策略"：批次可能同时命中批次特定策略和作业级策略，
两者范围与有效期都可能不同。这里把匹配、优先级、冲突消解和凭证签发
收拢到一处，``batch_store`` 的存储清单和清理动作只认这里签发的凭证。
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
from datetime import datetime, timezone
from typing import Any

from app.models import (
    ArchiveFormat,
    ArchivePolicy,
    ArchiveScopeCredential,
    StorageBatch,
    compute_expiry,
    normalize_archive_format,
    parse_retention,
)
from app.services.base import BaseModuleService
from app.store import store

# 归档范围凭证是内部派生表，不参与业务模块计数和通用资源路由。
CREDENTIAL_MODULE = "_archive_credential"

# 状态越靠前优先级越高：已完成/归档中/待触发的策略都是"有效策略"，
# 归档异常、暂停等状态不参与范围匹配。
POLICY_PRIORITY = {"已完成": 0, "归档中": 1, "待触发": 2}
INACTIVE_POLICY_STATUSES = {"归档异常", "已停用", "已失效", "已删除"}


class DataArchiveService(BaseModuleService):
    MODULE = "data_archive"
    ENTITY = "归档策略"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["策略编号", "数据范围", "归档周期", "归档格式", "保留时长", "存储位置", "压缩方式", "最后归档"]
    REQUIRED_FIELDS = ["策略编号", "数据范围", "归档周期"]
    STATUS_ORDER = ["待触发", "归档中", "已完成", "归档异常"]
    ACTION_RULES = {"触发归档": "归档中", "暂停归档": "已完成", "标记异常": "归档异常"}
    NEGATIVE_ACTIONS = []
    # 同一批次可能被并发确认；确认与凭证落库必须串行，保证只留一份有效结果。
    _confirm_lock = threading.RLock()

    # -- 匹配 ---------------------------------------------------------------

    def _policy_rows(self) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(self.MODULE)
            if str(row.get("status", "")) not in INACTIVE_POLICY_STATUSES
        ]

    @staticmethod
    def _scope_tokens(scope: str) -> list[str]:
        return [
            token.strip()
            for token in re.split(r"[,，;；\s]+", str(scope or ""))
            if token.strip()
        ]

    @classmethod
    def _scope_kind(cls, token: str) -> str:
        lowered = token.casefold()
        if lowered in ("*", "all", "全部", "所有", "global"):
            return "all"
        for prefix in ("job:", "job=", "作业:", "作业="):
            if lowered.startswith(prefix):
                return "job"
        for prefix in ("batch:", "batch=", "批次:", "批次="):
            if lowered.startswith(prefix):
                return "batch"
        return ""

    @classmethod
    def policy_match_level(cls, policy: ArchivePolicy, batch: StorageBatch) -> str:
        """返回匹配层级：``batch`` 最具体、其次 ``job``、最后 ``all``。

        显式前缀（``batch:`` / ``job:`` / ``作业:`` …）只按对应层级匹配，
        裸标识符先当批次号、再当作业号，避免 ``B10`` 被误认成作业范围。
        深层匹配会绑定到具体批次，不会因为前缀相同就把相邻批次一起带上。
        """
        batch_id = str(batch.batch_id or "")
        job_id = str(batch.source_job_id or "")
        levels: set[str] = set()

        def keep(kind: str, bare: str) -> None:
            if not bare or bare in ("*", "all", "全部", "所有", "global"):
                return
            if kind == "batch" and batch_id and bare == batch_id:
                levels.add("batch")
            elif kind == "job" and job_id and bare == job_id:
                levels.add("job")

        for token in cls._scope_tokens(policy.data_scope):
            kind = cls._scope_kind(token)
            if kind == "all":
                levels.add("all")
                continue
            if kind in ("batch", "job"):
                keep(kind, re.split(r"[:=]", token, maxsplit=1)[-1].strip())
                continue
            # 无前缀：既可能是批次号，也可能是作业号，按先后顺序判定。
            lowered = token.casefold()
            if job_id and lowered == job_id.casefold() and not (
                batch_id and lowered == batch_id.casefold()
            ):
                levels.add("job")
            elif batch_id and lowered == batch_id.casefold():
                levels.add("batch")

        job_scope = str(policy.job_scope or "")
        if job_id and job_scope and job_scope == job_id:
            levels.add("job")

        for level in ("batch", "job", "all"):
            if level in levels:
                return level
        return ""

    def matching_policies(
        self,
        batch: StorageBatch,
        *,
        include_global: bool = True,
    ) -> list[tuple[str, ArchivePolicy]]:
        """返回 ``(匹配层级, 策略)`` 列表，按层级、状态和确定性顺序排序。"""
        matched: list[tuple[str, ArchivePolicy]] = []
        for row in self._policy_rows():
            policy = ArchivePolicy.from_seed_row(row)
            level = self.policy_match_level(policy, batch)
            if not level:
                continue
            if level == "all" and not include_global:
                continue
            matched.append((level, policy))

        level_order = {"batch": 0, "job": 1, "all": 2}
        matched.sort(key=lambda item: (
            level_order.get(item[0], 9),
            POLICY_PRIORITY.get(item[1].status, 9),
            item[1].source_row_id,
            item[1].policy_id,
        ))
        return matched

    # -- 冲突与凭证 ---------------------------------------------------------

    @staticmethod
    def _policy_signature(policy: ArchivePolicy) -> dict[str, Any]:
        return {
            "policy_id": policy.policy_id,
            "scope": policy.data_scope,
            "format": normalize_archive_format(policy.archive_format),
            "retention_days": int(policy.retention_days),
            "status": policy.status,
        }

    def resolve_scope(
        self,
        batch: StorageBatch,
    ) -> tuple[ArchivePolicy | None, list[dict[str, Any]], str]:
        """按实际策略匹配归档范围，返回 (采用策略, 冲突记录, 匹配层级)。

        - 批次特定策略优先于作业级策略，作业级优先于全局策略；
        - 同一层级内有效期限（保留时长）冲突时取更长的保留期，并在凭证里
          留下冲突明细，保证下游只收到一个有效保留期；
        - 没有任何有效策略时返回 ``(None, [], "")``，由调用方决定是否回退到
          批次自身的归档范围（不再拿一条不相关的策略凑数，也不再报错）。
        """
        matched = self.matching_policies(batch)
        if not matched:
            return None, [], ""

        level, top = matched[0]
        same_level = [policy for lvl, policy in matched if lvl == level]

        # 有效期限冲突只在同一优先层级内消解：取最长的实际保留期，并留下
        # 明细；最终凭证只写这一个保留期，下游不会同时收到两个有效期。
        effective = {normalize_archive_format(p.archive_format) for p in same_level}
        retained = {int(p.retention_days) for p in same_level}
        conflicts: list[dict[str, Any]] = []
        chosen = top
        if len(same_level) > 1 and (len(retained) > 1 or len(effective) > 1):
            chosen = max(
                same_level,
                key=lambda p: (int(p.retention_days), -POLICY_PRIORITY.get(p.status, 9),
                               -p.source_row_id),
            )
            conflicts = [
                {**self._policy_signature(p), "level": level, "applied": p is chosen}
                for p in same_level
            ]
        elif len(matched) > 1:
            conflicts = [
                {**self._policy_signature(p), "level": lvl, "applied": p is chosen}
                for lvl, p in matched
            ]

        return chosen, conflicts, level

    def _credential_rows(self) -> list[dict[str, Any]]:
        return store.rows(CREDENTIAL_MODULE)

    @staticmethod
    def _row_batch_id(row: dict[str, Any]) -> str:
        return ArchiveScopeCredential.from_row(row).batch_id

    def active_credential(self, batch_id: str) -> ArchiveScopeCredential | None:
        """取该批次当前唯一有效的归档凭证；历史行按签定时间倒序兼容读取。"""
        candidates = [
            row for row in self._credential_rows()
            if self._row_batch_id(row) == batch_id
        ]
        if not candidates:
            return None
        candidates.sort(
            key=lambda row: (
                str(row.get("确认时间") or row.get("归档时间")
                    or row.get("最后归档") or ""),
                int(row.get("id") or 0),
            ),
            reverse=True,
        )
        for row in candidates:
            credential = ArchiveScopeCredential.from_row(row)
            if credential.active and credential.batch_id == batch_id:
                return credential
        return None

    def credentials_for_batch(self, batch_id: str) -> list[ArchiveScopeCredential]:
        return [
            ArchiveScopeCredential.from_row(row)
            for row in self._credential_rows()
            if self._row_batch_id(row) == batch_id
        ]

    def _supersede_credentials(self, batch_id: str, keep_id: str) -> None:
        for row in self._credential_rows():
            if self._row_batch_id(row) != batch_id:
                continue
            credential = ArchiveScopeCredential.from_row(row)
            if credential.credential_id == keep_id:
                continue
            row["凭证状态"] = "已替代"
            row["status"] = "已替代"
            row["pending"] = False

    def _find_credential_row(self, credential_id: str) -> dict[str, Any] | None:
        for row in self._credential_rows():
            if str(row.get("凭证编号") or row.get("credential_id") or "") == credential_id:
                return row
        return None

    def _upsert_credential(
        self, credential: ArchiveScopeCredential
    ) -> tuple[dict[str, Any], bool]:
        """同一批次只保留一份有效凭证；重复确认返回已有结果，不重复落库。"""
        existing = self.active_credential(credential.batch_id)
        if existing is not None:
            return self._find_credential_row(existing.credential_id) or existing.to_row(), False

        self._supersede_credentials(credential.batch_id, credential.credential_id)
        rows = self._credential_rows()
        row = credential.to_row()
        # 历史回执行可能没有 id：取所有行 id 的最大值再 +1，不能假定第一个非空。
        row["id"] = max(
            (int(r.get("id") or 0) for r in rows), default=0
        ) + 1
        rows.append(row)
        return row, True

    @staticmethod
    def _make_checksum(batch: StorageBatch) -> str:
        payload = json.dumps(
            {
                "batch_id": batch.batch_id,
                "job_id": batch.source_job_id,
                "checksum": batch.checksum,
                "rows": batch.row_count,
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def _default_policy(self, batch: StorageBatch, now: str) -> ArchivePolicy:
        """没有匹配策略时按批次自身的实际范围归档，绝不借用无关策略。"""
        return ArchivePolicy(
            policy_id=f"ARCH-{batch.batch_id}",
            data_scope=str(batch.batch_id),
            archive_cycle="on_demand",
            archive_format=ArchiveFormat(normalize_archive_format(batch.compression)),
            retention_days=90,
            storage_location=f"/archive/{batch.batch_id}/",
            compression_method=batch.compression,
            last_archive=now,
            status="已完成",
        )

    def confirm_archive(
        self,
        batch: StorageBatch,
        *,
        archive_format: str | None = None,
        retention_days: int | None = None,
        archived_at: str | None = None,
    ) -> ArchiveScopeCredential:
        """确认一次归档并签发归档范围凭证（幂等，同一批次只留一份有效凭证）。

        已存在的有效凭证直接复用，``批次起始时间``和凭证内容都不会被覆盖；
        显式传入的 ``archive_format`` / ``retention_days`` 只用于首次签发。
        """
        with self._confirm_lock:
            return self._confirm_archive_locked(
                batch,
                archive_format=archive_format,
                retention_days=retention_days,
                archived_at=archived_at,
            )

    def _confirm_archive_locked(
        self,
        batch: StorageBatch,
        *,
        archive_format: str | None = None,
        retention_days: int | None = None,
        archived_at: str | None = None,
    ) -> ArchiveScopeCredential:
        existing = self.active_credential(batch.batch_id)
        if existing is not None:
            self.sync_batch_storage(existing)
            return existing

        now = archived_at or datetime.now(timezone.utc).isoformat()
        policy, conflicts, level = self.resolve_scope(batch)
        is_default = policy is None
        if policy is None:
            policy = self._default_policy(batch, now)

        fmt = normalize_archive_format(
            archive_format if archive_format is not None else policy.archive_format
        )
        retention = parse_retention(policy.retention_days, default=90)
        if retention_days is not None:
            retention = max(0, int(retention_days))

        # 批次特定范围 + 作业范围：凭证里同时记录，便于核对"实际采用的策略"。
        scope_parts = [f"batch:{batch.batch_id}"]
        if batch.source_job_id:
            scope_parts.append(f"job:{batch.source_job_id}")
        credential = ArchiveScopeCredential(
            credential_id=f"ARCH-CRED-{batch.batch_id}",
            batch_id=batch.batch_id,
            job_id=batch.source_job_id,
            policy_id=policy.policy_id,
            policy_scope=policy.data_scope,
            # 凭证范围以实际策略为准：批次策略只管这个批次，作业级策略覆盖
            # 该作业下所有批次，全局策略才落到显式的 batch+job + all 标记。
            # 回退策略只归档当前批次本身，绝不能顺带覆盖同作业的其它批次。
            scope=(
                _credential_scope(level, scope_parts)
                if not is_default else scope_parts[0]
            ),
            archive_format=fmt,
            retention_days=retention,
            archived_at=now,
            expires_at=compute_expiry(now, retention),
            checksum=self._make_checksum(batch),
            batch_checksum=str(batch.checksum or ""),
            status="已完成",
            conflicts=conflicts,
            is_default=is_default,
        )
        row, _ = self._upsert_credential(credential)
        stored = ArchiveScopeCredential.from_row(row)

        # 归档回写策略与批次清单，两处都从同一份凭证取值。
        self._touch_policy(policy, now)
        self.sync_batch_storage(stored)
        return stored

    def _touch_policy(self, policy: ArchivePolicy, now: str) -> None:
        if policy.policy_id.startswith("ARCH-"):
            return  # 默认策略仅体现在凭证上，不污染策略清单
        for row in store.rows(self.MODULE):
            if int(row.get("id", 0) or 0) == policy.source_row_id or (
                not policy.source_row_id and row.get("策略编号") == policy.policy_id
            ):
                row["最后归档"] = now
                row["status"] = "已完成"
                row["pending"] = False
                row["abnormal"] = False
                return

    # -- 与批次存储同步 -----------------------------------------------------

    def sync_batch_storage(self, credential: ArchiveScopeCredential) -> dict[str, Any] | None:
        """把凭证上的实际范围/格式/到期时点写进批次存储清单。"""
        row = self._find_batch_row(credential.batch_id)
        if row is None:
            return None
        row["归档策略"] = credential.policy_id
        row["归档凭证"] = credential.credential_id
        row["归档范围"] = credential.scope
        row["归档格式"] = credential.archive_format
        row["保留时长"] = credential.retention_days
        row["到期时间"] = credential.expires_at
        row["归档摘要"] = credential.checksum
        return row

    def _find_batch_row(self, batch_id: str) -> dict[str, Any] | None:
        for row in store.rows("batch_store"):
            if str(row.get("批次编号", "")) == batch_id:
                return row
        return None

    def batch_storage_detail(self, batch_id: str) -> dict[str, Any] | None:
        """批次存储详情：直接按当前有效凭证取数，不回读过期快照。"""
        row = self._find_batch_row(batch_id)
        if row is None:
            return None
        credential = self.effective_credential(batch_id)
        detail = dict(row)
        if credential is not None:
            detail.update({
                "归档策略": credential.policy_id,
                "归档凭证": credential.credential_id,
                "归档范围": credential.scope,
                "归档格式": credential.archive_format,
                "保留时长": credential.retention_days,
                "到期时间": credential.expires_at,
                "归档摘要": credential.checksum,
            })
        return detail

    def historic_credential(self, batch_id: str) -> ArchiveScopeCredential | None:
        """兼容已写入的历史回执：凭证表缺失时回读批次清单上的归档字段。"""
        row = self._find_batch_row(batch_id)
        if row is None:
            return None
        if not any(row.get(key) for key in ("归档范围", "归档格式", "到期时间", "保留时长")):
            return None
        legacy = dict(row)
        legacy.setdefault("批次编号", batch_id)
        legacy.setdefault("凭证编号", f"ARCH-CRED-{batch_id}")
        legacy.setdefault("归档范围", f"batch:{batch_id}")
        legacy.setdefault("确认时间", str(row.get("入库时间", "")))
        credential = ArchiveScopeCredential.from_row(legacy)
        # 历史回执没有"已替代"这类状态，默认为该批次唯一有效结果。
        if not credential.active:
            return None
        return credential

    def effective_credential(self, batch_id: str) -> ArchiveScopeCredential | None:
        """凭证表优先；凭证表里已有历史行但不是有效状态时，不再退回批次快照。"""
        if any(self._row_batch_id(row) == batch_id for row in self._credential_rows()):
            return self.active_credential(batch_id)
        return self.historic_credential(batch_id)

    def cleanup_scope(self, batch_id: str) -> dict[str, Any] | None:
        """清理动作读取实际生效的格式和范围；无有效凭证时不得清理。"""
        credential = self.effective_credential(batch_id)
        if credential is None:
            return None
        return {
            "batch_id": credential.batch_id,
            "credential_id": credential.credential_id,
            "policy_id": credential.policy_id,
            "scope": credential.scope,
            "archive_format": credential.archive_format,
            "retention_days": credential.retention_days,
            "expires_at": credential.expires_at,
            "expired": _is_expired(credential),
            "checksum": credential.checksum,
        }

    # -- 动作 ---------------------------------------------------------------

    def trigger_archive(
        self, entry_id: int, batch_id: str | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        """从归档配置入口触发：先确认归档范围，再进入"归档中"。

        命中批次特定策略时只归档该批次；作业级/全局策略可以显式指定要确认的
        批次，未指定时按策略范围依次确认其中的批次。相邻批次（如 B1 与 B10）
        不会被同一个批次特定策略带上。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        policy = ArchivePolicy.from_seed_row(entry)
        targets = self._batches_for_policy(policy, batch_id)
        if not targets:
            return None, f"{self.ENTITY} {entry_id} 没有可归档的批次"

        credentials = [self.confirm_archive(batch) for batch in targets]
        # 策略状态按动作规则流转（"触发归档" → 归档中），确认结果由凭证表达；
        # 不强行标成"已完成"，否则策略一旦完成就不再参与后续批次的范围匹配。
        refreshed, message = super().run_action(entry_id, "触发归档")
        return refreshed or entry, (
            f"{message}，签发凭证：" + "、".join(c.credential_id for c in credentials)
        )

    def _batches_for_policy(
        self, policy: ArchivePolicy, batch_id: str | None
    ) -> list[StorageBatch]:
        batches = [StorageBatch.from_seed_row(r) for r in store.rows("batch_store")]
        if batch_id:
            chosen = [b for b in batches if b.batch_id == batch_id]
            if chosen and self.policy_match_level(policy, chosen[0]):
                return chosen
            return []

        levels: dict[str, list[StorageBatch]] = {}
        for batch in batches:
            level = self.policy_match_level(policy, batch)
            if not level:
                continue
            levels.setdefault(level, []).append(batch)
        for level in ("batch", "job", "all"):
            if level in levels:
                return levels[level]
        return []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action == "触发归档":
            return self.trigger_archive(entry_id)
        return super().run_action(entry_id, action)


def _credential_scope(level: str, parts: list[str]) -> str:
    """凭证的归档范围跟着实际命中的策略层级走。

    批次策略的范围只写这个批次；作业级策略覆盖该作业全部批次；全局策略
    才同时带上批次、作业以及 ``all`` 标记。
    """
    if level == "batch":
        return parts[0]
    if level == "job":
        return parts[1] if len(parts) > 1 else parts[0]
    return ",".join(parts) + ",all"


def _is_expired(credential: ArchiveScopeCredential) -> bool:
    if not credential.expires_at:
        return False
    try:
        expires = datetime.fromisoformat(credential.expires_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    return expires <= datetime.now(timezone.utc)
