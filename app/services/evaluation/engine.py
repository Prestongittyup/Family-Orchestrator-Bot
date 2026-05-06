from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from typing import Any, AsyncIterator
from uuid import uuid4

from app.schemas.intelligence import EvaluationEmailInput
from app.services.evaluation.comparator import (
    compare_email_outputs,
    compute_dls,
    normalize_email_output,
    outputs_disagree,
)
from app.services.evaluation.store import EvaluationStore
from app.services.llm_gateway import LLMGatewayService
from app.services.rules_engine import RulesEngine


@dataclass(frozen=True)
class EmailEvaluationResult:
    evaluation_id: str
    user_id: str
    email_id: str
    rule_output: dict[str, Any]
    llm_output: dict[str, Any]
    comparison: dict[str, Any]
    dls: float
    latency_ms: float
    tokens_in: int
    tokens_out: int
    estimated_cost: float
    llm_used: bool
    timestamp: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "evaluation_id": self.evaluation_id,
            "user_id": self.user_id,
            "email_id": self.email_id,
            "rule_output": self.rule_output,
            "llm_output": self.llm_output,
            "comparison": self.comparison,
            "dls": self.dls,
            "delta_score": self.dls,
            "latency_ms": self.latency_ms,
            "tokens_in": self.tokens_in,
            "tokens_out": self.tokens_out,
            "estimated_cost": self.estimated_cost,
            "llm_used": self.llm_used,
            "timestamp": self.timestamp,
        }


class EvaluationEngine:
    def __init__(
        self,
        *,
        rules_engine: RulesEngine,
        llm_gateway: LLMGatewayService,
        evaluation_store: EvaluationStore,
        max_parallel_batch: int = 5,
    ) -> None:
        self._rules = rules_engine
        self._llm = llm_gateway
        self._store = evaluation_store
        self._max_parallel_batch = max(1, max_parallel_batch)

    async def evaluate_email(self, *, user_id: str, email: EvaluationEmailInput) -> EmailEvaluationResult:
        started = perf_counter()
        messages = [email.body, *[row for row in email.thread if row and row.strip()]]
        rule_result, llm_metrics = await asyncio.gather(
            asyncio.to_thread(
                self._rules.analyze_email,
                sender=email.sender,
                subject=email.subject,
                messages=messages,
                to_me=True,
                cc_me=False,
            ),
            self._llm.analyze_email_with_metrics(
                user_id=user_id,
                sender=email.sender,
                subject=email.subject,
                to_me=True,
                cc_me=False,
                messages=messages,
                fallback_payload={
                    "priority": "medium",
                    "needs_attention": False,
                    "actions": [],
                    "state_summary": "LLM unavailable for evaluation",
                    "reason": "llm_unavailable",
                },
            ),
        )

        rule_output = normalize_email_output(rule_result.to_payload())
        llm_output = normalize_email_output(llm_metrics.payload)

        comparison = compare_email_outputs(rule_output, llm_output)
        dls = compute_dls(rule_output, llm_output)

        outcome = EmailEvaluationResult(
            evaluation_id=str(uuid4()),
            user_id=user_id,
            email_id=_compute_email_id(email.sender, email.subject, email.body, email.thread),
            rule_output=rule_output,
            llm_output=llm_output,
            comparison=comparison,
            dls=dls,
            latency_ms=round((perf_counter() - started) * 1000.0, 3),
            tokens_in=llm_metrics.tokens_in,
            tokens_out=llm_metrics.tokens_out,
            estimated_cost=llm_metrics.estimated_cost,
            llm_used=llm_metrics.llm_used,
            timestamp=datetime.now(UTC).isoformat(),
        )

        await self._store.save_email_comparison(outcome.to_payload())
        return outcome

    async def evaluate_batch(self, *, user_id: str, emails: list[EvaluationEmailInput]) -> dict[str, Any]:
        semaphore = asyncio.Semaphore(self._max_parallel_batch)

        async def _run_one(entry: EvaluationEmailInput) -> EmailEvaluationResult:
            async with semaphore:
                return await self.evaluate_email(user_id=user_id, email=entry)

        evaluations = await asyncio.gather(*[_run_one(item) for item in emails])
        payloads = [row.to_payload() for row in evaluations]

        total = len(payloads)
        avg_dls = round(sum(float(row["dls"]) for row in payloads) / max(total, 1), 4)
        disagreements = sum(
            1
            for row in payloads
            if outputs_disagree(
                row["rule_output"],
                row["llm_output"],
            )
        )
        llm_only = sum(1 for row in payloads if bool(row["comparison"].get("missed_intent_detection")))
        llm_new_actions = sum(
            1
            for row in payloads
            if len(row["llm_output"].get("actions", [])) > len(row["rule_output"].get("actions", []))
        )
        priority_changes = sum(1 for row in payloads if bool(row["comparison"].get("priority_delta")))
        positive_lift_count = sum(1 for row in payloads if float(row["dls"]) > 0.0)
        negative_lift_count = sum(1 for row in payloads if float(row["dls"]) < 0.0)
        llm_usage_count = sum(
            1
            for row in payloads
            if bool(row.get("llm_used"))
            or int(row.get("tokens_in", 0)) > 0
            or int(row.get("tokens_out", 0)) > 0
        )
        llm_cost_total = sum(float(row.get("estimated_cost", 0.0)) for row in payloads)

        aggregate = {
            "total_emails": total,
            "avg_dls": avg_dls,
            "avg_delta_score": avg_dls,
            "disagreement_percent": _pct(disagreements, total),
            "llm_only_detections_percent": _pct(llm_only, total),
            "llm_new_actions_percent": _pct(llm_new_actions, total),
            "meaningful_priority_change_percent": _pct(priority_changes, total),
            "positive_lift_ratio": _ratio(positive_lift_count, total),
            "negative_lift_ratio": _ratio(negative_lift_count, total),
            "llm_cost_per_email": round(llm_cost_total / max(total, 1), 6),
            "llm_usage_percentage": _ratio(llm_usage_count, total),
        }

        batch_payload = {
            "batch_run_id": str(uuid4()),
            "timestamp": datetime.now(UTC).isoformat(),
            "aggregate": aggregate,
            "count": total,
        }
        await self._store.save_batch_summary(batch_payload)

        return {
            "aggregate": aggregate,
            "evaluations": payloads,
        }

    async def stream_single_evaluation(
        self,
        *,
        user_id: str,
        email: EvaluationEmailInput,
    ) -> AsyncIterator[dict[str, Any]]:
        started = perf_counter()
        messages = [email.body, *[row for row in email.thread if row and row.strip()]]

        rule_result = await asyncio.to_thread(
            self._rules.analyze_email,
            sender=email.sender,
            subject=email.subject,
            messages=messages,
            to_me=True,
            cc_me=False,
        )
        rule_output = normalize_email_output(rule_result.to_payload())
        yield {"type": "rule_output", "data": rule_output}

        llm_metrics = await self._llm.analyze_email_with_metrics(
            user_id=user_id,
            sender=email.sender,
            subject=email.subject,
            to_me=True,
            cc_me=False,
            messages=messages,
            fallback_payload={
                "priority": "medium",
                "needs_attention": False,
                "actions": [],
                "state_summary": "LLM unavailable for evaluation",
                "reason": "llm_unavailable",
            },
        )
        llm_output = normalize_email_output(llm_metrics.payload)
        yield {"type": "llm_output", "data": llm_output}

        comparison = compare_email_outputs(rule_output, llm_output)
        dls = compute_dls(rule_output, llm_output)

        result = EmailEvaluationResult(
            evaluation_id=str(uuid4()),
            user_id=user_id,
            email_id=_compute_email_id(email.sender, email.subject, email.body, email.thread),
            rule_output=rule_output,
            llm_output=llm_output,
            comparison=comparison,
            dls=dls,
            latency_ms=round((perf_counter() - started) * 1000.0, 3),
            tokens_in=llm_metrics.tokens_in,
            tokens_out=llm_metrics.tokens_out,
            estimated_cost=llm_metrics.estimated_cost,
            llm_used=llm_metrics.llm_used,
            timestamp=datetime.now(UTC).isoformat(),
        )
        await self._store.save_email_comparison(result.to_payload())

        yield {"type": "comparison", "data": result.to_payload()}


def _compute_email_id(sender: str, subject: str, body: str, thread: list[str]) -> str:
    digest_source = "\n".join(
        [sender.strip().lower(), subject.strip().lower(), body.strip(), *[item.strip() for item in thread]]
    )
    return hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:20]


def _pct(value: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((value / total) * 100.0, 2)


def _ratio(value: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(value / total, 4)
