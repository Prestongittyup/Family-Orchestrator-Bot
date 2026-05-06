from __future__ import annotations
import pytest

from archive.apps.api.intent_contract.schema import IntentType
from archive.apps.api.llm.intent_resolver import _coerce_intent_type

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



@pytest.mark.unit
@pytest.mark.legacy
def test_coerce_intent_type_accepts_enum_value() -> None:
    assert _coerce_intent_type("create_task") == IntentType.CREATE_TASK


@pytest.mark.unit
@pytest.mark.legacy
def test_coerce_intent_type_accepts_enum_name() -> None:
    assert _coerce_intent_type("CREATE_TASK") == IntentType.CREATE_TASK


@pytest.mark.unit
@pytest.mark.legacy
def test_coerce_intent_type_unknown_value_returns_none() -> None:
    assert _coerce_intent_type("appointment") is None