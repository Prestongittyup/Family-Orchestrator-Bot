from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from apps.api.product_surface.contracts import UIPatch, UIBootstrapState


class UIPatchService:
    """Generates deterministic UI patches and applies them idempotently."""

    def generate_patches(
        self,
        *,
        previous: UIBootstrapState | None,
        current: UIBootstrapState,
    ) -> list[UIPatch]:
        if previous is None:
            return self._full_replace_patches(current)

        patches: list[UIPatch] = []
        source_ts = _parse_iso(current.system_health.last_updated)

        if previous.family.model_dump() != current.family.model_dump():
            patches.append(
                UIPatch(
                    entity_type="family",
                    entity_id=current.family.family_id,
                    change_type="replace",
                    payload=current.family.model_dump(),
                    version=current.snapshot_version,
                    source_timestamp=source_ts,
                )
            )

        patches.extend(
            self._diff_collection(
                entity_type="plan",
                previous={p.plan_id: p.model_dump() for p in previous.active_plans},
                current={p.plan_id: p.model_dump() for p in current.active_plans},
                version=current.snapshot_version,
                source_timestamp=source_ts,
            )
        )

        previous_tasks = {
            row["task_id"]: row
            for row in _task_rows(previous)
        }
        current_tasks = {
            row["task_id"]: row
            for row in _task_rows(current)
        }
        patches.extend(
            self._diff_collection(
                entity_type="task",
                previous=previous_tasks,
                current=current_tasks,
                version=current.snapshot_version,
                source_timestamp=source_ts,
            )
        )

        patches.extend(
            self._diff_collection(
                entity_type="event",
                previous={e.event_id: e.model_dump() for e in previous.calendar.events},
                current={e.event_id: e.model_dump() for e in current.calendar.events},
                version=current.snapshot_version,
                source_timestamp=source_ts,
            )
        )

        patches.extend(
            self._diff_collection(
                entity_type="notification",
                previous={n.notification_id: n.model_dump() for n in previous.notifications},
                current={n.notification_id: n.model_dump() for n in current.notifications},
                version=current.snapshot_version,
                source_timestamp=source_ts,
            )
        )

        patches.sort(key=lambda p: (p.entity_type, p.entity_id, p.change_type))
        return patches

    def apply_patches(
        self,
        *,
        index: dict[str, dict[str, Any]],
        patches: list[UIPatch],
    ) -> dict[str, dict[str, Any]]:
        """Apply patches in an idempotent, replay-safe way."""
        current = {k: dict(v) for k, v in index.items()}
        seen_signatures = set()

        for patch in patches:
            signature = self._patch_signature(patch)
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)

            key = f"{patch.entity_type}:{patch.entity_id}"
            if patch.change_type == "delete":
                current.pop(key, None)
                continue
            current[key] = dict(patch.payload)

        return current

    def _full_replace_patches(self, current: UIBootstrapState) -> list[UIPatch]:
        source_ts = _parse_iso(current.system_health.last_updated)
        patches: list[UIPatch] = [
            UIPatch(
                entity_type="family",
                entity_id=current.family.family_id,
                change_type="replace",
                payload=current.family.model_dump(),
                version=current.snapshot_version,
                source_timestamp=source_ts,
            )
        ]

        for plan in current.active_plans:
            patches.append(
                UIPatch(
                    entity_type="plan",
                    entity_id=plan.plan_id,
                    change_type="create",
                    payload=plan.model_dump(),
                    version=current.snapshot_version,
                    source_timestamp=source_ts,
                )
            )

        for row in _task_rows(current):
            patches.append(
                UIPatch(
                    entity_type="task",
                    entity_id=row["task_id"],
                    change_type="create",
                    payload=row,
                    version=current.snapshot_version,
                    source_timestamp=source_ts,
                )
            )

        for event in current.calendar.events:
            patches.append(
                UIPatch(
                    entity_type="event",
                    entity_id=event.event_id,
                    change_type="create",
                    payload=event.model_dump(),
                    version=current.snapshot_version,
                    source_timestamp=source_ts,
                )
            )

        for row in current.notifications:
            patches.append(
                UIPatch(
                    entity_type="notification",
                    entity_id=row.notification_id,
                    change_type="create",
                    payload=row.model_dump(),
                    version=current.snapshot_version,
                    source_timestamp=source_ts,
                )
            )

        patches.sort(key=lambda p: (p.entity_type, p.entity_id, p.change_type))
        return patches

    def _diff_collection(
        self,
        *,
        entity_type: str,
        previous: dict[str, dict[str, Any]],
        current: dict[str, dict[str, Any]],
        version: int,
        source_timestamp: datetime,
    ) -> list[UIPatch]:
        patches: list[UIPatch] = []
        previous_ids = set(previous)
        current_ids = set(current)

        for entity_id in sorted(current_ids - previous_ids):
            patches.append(
                UIPatch(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    change_type="create",
                    payload=current[entity_id],
                    version=version,
                    source_timestamp=source_timestamp,
                )
            )

        for entity_id in sorted(previous_ids - current_ids):
            patches.append(
                UIPatch(
                    entity_type=entity_type,
                    entity_id=entity_id,
                    change_type="delete",
                    payload={},
                    version=version,
                    source_timestamp=source_timestamp,
                )
            )

        for entity_id in sorted(previous_ids.intersection(current_ids)):
            if previous[entity_id] != current[entity_id]:
                patches.append(
                    UIPatch(
                        entity_type=entity_type,
                        entity_id=entity_id,
                        change_type="update",
                        payload=current[entity_id],
                        version=version,
                        source_timestamp=source_timestamp,
                    )
                )

        return patches

    @staticmethod
    def _patch_signature(patch: UIPatch) -> str:
        canonical = json.dumps(
            {
                "entity_type": patch.entity_type,
                "entity_id": patch.entity_id,
                "change_type": patch.change_type,
                "payload": patch.payload,
                "version": patch.version,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _task_rows(state: UIBootstrapState) -> list[dict[str, Any]]:
    rows = []
    for section in (state.task_board.pending, state.task_board.in_progress, state.task_board.completed, state.task_board.failed):
        rows.extend(item.model_dump() for item in section)
    rows.sort(key=lambda row: row["task_id"])
    return rows


def _parse_iso(value: str) -> datetime:
    fallback = "1970-01-01T00:00:00Z"
    raw = (value or fallback).replace("Z", "+00:00")
    return datetime.fromisoformat(raw)
