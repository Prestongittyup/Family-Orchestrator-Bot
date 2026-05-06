import pytest
from archive.apps.api.core.backpressure_middleware import _classify

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



@pytest.mark.integration
@pytest.mark.legacy
def test_identity_paths_are_classified_short() -> None:
    assert _classify("/v1/identity/household/create") == "SHORT"
    assert _classify("/v1/identity/bootstrap") == "SHORT"


@pytest.mark.integration
@pytest.mark.legacy
def test_non_identity_paths_keep_existing_classification() -> None:
    assert _classify("/v1/auth/token/refresh") == "SHORT"
    assert _classify("/v1/families") == "LONG"


@pytest.mark.integration
@pytest.mark.legacy
def test_api_prefixed_paths_are_normalized_for_classification() -> None:
    assert _classify("/api/v1/identity/household/create") == "SHORT"
    assert _classify("/api/v1/auth/token/refresh/") == "SHORT"
    assert _classify("/api/v1/families") == "LONG"