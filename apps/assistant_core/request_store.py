from __future__ import annotations

from copy import deepcopy

from apps.assistant_core.contracts import AssistantResponse, ProposedAction


class AssistantRequestStore:
    def __init__(self) -> None:
        self._responses: dict[str, AssistantResponse] = {}

    def save(self, response: AssistantResponse) -> AssistantResponse:
        self._responses[response.request_id] = response
        return response

    def get(self, request_id: str) -> AssistantResponse | None:
        response = self._responses.get(request_id)
        return None if response is None else AssistantResponse.model_validate(deepcopy(response.model_dump()))

    def approve(self, request_id: str, action_ids: list[str]) -> AssistantResponse | None:
        response = self.get(request_id)
        if response is None:
            return None

        approved_actions: list[ProposedAction] = []
        approved_count = 0
        requested = set(action_ids)
        for action in response.proposed_actions:
            if action.action_id in requested:
                approved_actions.append(action.model_copy(update={"approval_status": "approved"}))
                approved_count += 1
            else:
                approved_actions.append(action)

        reasoning_trace = list(response.reasoning_trace)
        reasoning_trace.append(
            f"Approval gate recorded {approved_count} action(s); no downstream calendar or state mutation was executed."
        )
        updated = response.model_copy(update={"proposed_actions": approved_actions, "reasoning_trace": reasoning_trace})
        self._responses[request_id] = updated
        return self.get(request_id)


request_store = AssistantRequestStore()