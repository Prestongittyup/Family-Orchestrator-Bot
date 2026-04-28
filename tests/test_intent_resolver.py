from __future__ import annotations

from archive.apps.api.intent_contract.schema import IntentType
from archive.apps.api.llm.intent_resolver import _coerce_intent_type


def test_coerce_intent_type_accepts_enum_value() -> None:
    assert _coerce_intent_type("create_task") == IntentType.CREATE_TASK


def test_coerce_intent_type_accepts_enum_name() -> None:
    assert _coerce_intent_type("CREATE_TASK") == IntentType.CREATE_TASK


def test_coerce_intent_type_unknown_value_returns_none() -> None:
    assert _coerce_intent_type("appointment") is None
