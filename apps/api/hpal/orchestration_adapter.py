from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from household_os.runtime.orchestrator import HouseholdOSOrchestrator


@dataclass(frozen=True)
class OrchestrationSubmission:
    request_id: str | None
    action_id: str | None
    command_id: str


class OrchestrationAdapter:
    """Single HPAL write bridge into orchestration runtime."""

    def __init__(self, orchestrator: HouseholdOSOrchestrator | None = None) -> None:
        self.orchestrator = orchestrator or HouseholdOSOrchestrator()

    def submit_command(
        self,
        *,
        family_id: str,
        command_type: str,
        intent_text: str,
        idempotency_key: str,
        payload: dict[str, Any],
    ) -> OrchestrationSubmission:
        self._iel_precheck(family_id=family_id, payload=payload)
        result = self.orchestrator.tick(
            household_id=family_id,
            user_input=intent_text,
        )
        command_id = self._command_id(family_id=family_id, idempotency_key=idempotency_key, payload=payload)
        graph = self.orchestrator.state_store.load_graph(family_id)
        hpal = graph.setdefault("hpal", {})
        command_log = hpal.setdefault("command_log", [])
        command_log.append(
            {
                "command_id": command_id,
                "command_type": command_type,
                "idempotency_key": idempotency_key,
                "payload_hash": self._hash_payload(payload),
                "recorded_at": self._now_iso(),
                "request_id": result.response.request_id if result.response else None,
                "action_id": result.action_record.action_id if result.action_record else None,
            }
        )
        self.orchestrator.state_store.save_graph(graph)
        return OrchestrationSubmission(
            request_id=result.response.request_id if result.response else None,
            action_id=result.action_record.action_id if result.action_record else None,
            command_id=command_id,
        )

    def save_hpal_state(
        self,
        *,
        family_id: str,
        graph: dict[str, Any],
        expected_state_version: int | None = None,
    ) -> dict[str, Any]:
        self._iel_precheck(family_id=family_id, payload=graph.get("hpal", {}))
        current = self.orchestrator.state_store.load_graph(family_id)
        current_version = int(current.get("state_version", 0))
        if expected_state_version is not None and current_version != expected_state_version:
            raise ValueError("concurrent state update detected")

        out = dict(graph)
        out["state_version"] = current_version + 1
        return self.orchestrator.state_store.save_graph(out)

    def load_graph(self, family_id: str) -> dict[str, Any]:
        return self.orchestrator.state_store.load_graph(family_id)

    def _iel_precheck(self, *, family_id: str, payload: dict[str, Any]) -> None:
        if not family_id.strip():
            raise ValueError("family_id is required")
        found_family_ids = self._extract_family_ids(payload)
        if len(found_family_ids) > 1:
            raise ValueError("mixed-family mutation is not allowed")
        if found_family_ids and family_id not in found_family_ids:
            raise ValueError("cross-family mutation is not allowed")

    def _extract_family_ids(self, payload: Any) -> set[str]:
        found: set[str] = set()
        if isinstance(payload, dict):
            for key, value in payload.items():
                if key == "family_id" and value is not None:
                    found.add(str(value))
                else:
                    found.update(self._extract_family_ids(value))
            return found
        if isinstance(payload, list):
            for item in payload:
                found.update(self._extract_family_ids(item))
        return found

    def _hash_payload(self, payload: dict[str, Any]) -> str:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _command_id(self, *, family_id: str, idempotency_key: str, payload: dict[str, Any]) -> str:
        digest = self._hash_payload({"family_id": family_id, "idempotency_key": idempotency_key, "payload": payload})
        return f"cmd-{digest[:16]}"

    def _now_iso(self) -> str:
        return datetime.utcnow().isoformat() + "Z"
