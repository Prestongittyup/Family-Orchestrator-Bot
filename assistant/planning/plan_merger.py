from __future__ import annotations

from datetime import datetime

from assistant.contracts.assistant_plan import AssistantProposal, MergedConflict, RankedPlanItem


def _parse_time_block(value: str) -> tuple[datetime, datetime] | None:
    if not value or len(value) < 22:
        return None
    start_raw = value[:16].strip()
    end_raw = value[-5:].strip()
    if value[16] != "-" or len(end_raw) != 5:
        return None
    date_part = start_raw[:10]
    try:
        start_dt = datetime.fromisoformat(start_raw)
        end_dt = datetime.fromisoformat(f"{date_part} {end_raw}")
    except ValueError:
        return None
    return start_dt, end_dt


class PlanMerger:
    def detect_conflicts(self, proposals: list[AssistantProposal]) -> list[MergedConflict]:
        conflicts: list[MergedConflict] = []

        for index, left in enumerate(proposals):
            for right in proposals[index + 1 :]:
                for left_block in left.time_blocks:
                    left_range = _parse_time_block(left_block)
                    if left_range is None:
                        continue
                    for right_block in right.time_blocks:
                        right_range = _parse_time_block(right_block)
                        if right_range is None:
                            continue
                        if left_range[0] < right_range[1] and right_range[0] < left_range[1]:
                            conflicts.append(
                                MergedConflict(
                                    conflict_type="cross_domain_overlap",
                                    severity="medium",
                                    description=(
                                        f"{left.domain} proposal '{left.title}' overlaps with "
                                        f"{right.domain} proposal '{right.title}'."
                                    ),
                                    impacted_proposals=[left.proposal_id, right.proposal_id],
                                )
                            )
                            break

        return conflicts

    def rank(self, proposals: list[AssistantProposal], conflicts: list[MergedConflict], primary_domains: list[str]) -> list[RankedPlanItem]:
        conflict_counts = {
            proposal.proposal_id: 0
            for proposal in proposals
        }
        for conflict in conflicts:
            for proposal_id in conflict.impacted_proposals:
                conflict_counts[proposal_id] = conflict_counts.get(proposal_id, 0) + 1

        domain_priority = {domain: index for index, domain in enumerate(primary_domains)}

        scored = []
        for proposal in proposals:
            priority_bonus = max(0, 4 - domain_priority.get(proposal.domain, len(primary_domains) + 1)) * 0.03
            score = round(proposal.confidence + priority_bonus - (conflict_counts.get(proposal.proposal_id, 0) * 0.12), 4)
            scored.append((score, proposal))

        ordered = sorted(
            scored,
            key=lambda item: (-item[0], domain_priority.get(item[1].domain, 999), item[1].proposal_id),
        )
        return [
            RankedPlanItem(
                rank=index + 1,
                proposal_id=proposal.proposal_id,
                domain=proposal.domain,
                title=proposal.title,
                confidence=proposal.confidence,
                rationale=proposal.rationale,
            )
            for index, (_score, proposal) in enumerate(ordered)
        ]

    def merge(
        self,
        proposals: list[AssistantProposal],
        *,
        existing_conflicts: list[MergedConflict],
        primary_domains: list[str],
    ) -> tuple[list[MergedConflict], list[RankedPlanItem]]:
        cross_domain_conflicts = self.detect_conflicts(proposals)
        all_conflicts = [*existing_conflicts, *cross_domain_conflicts]
        ranked = self.rank(proposals, all_conflicts, primary_domains)
        return all_conflicts, ranked