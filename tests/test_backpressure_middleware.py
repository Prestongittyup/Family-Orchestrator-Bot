from archive.apps.api.core.backpressure_middleware import _classify


def test_identity_paths_are_classified_short() -> None:
    assert _classify("/v1/identity/household/create") == "SHORT"
    assert _classify("/v1/identity/bootstrap") == "SHORT"


def test_non_identity_paths_keep_existing_classification() -> None:
    assert _classify("/v1/system/health") == "SHORT"
    assert _classify("/v1/families") == "LONG"


def test_api_prefixed_paths_are_normalized_for_classification() -> None:
    assert _classify("/api/v1/identity/household/create") == "SHORT"
    assert _classify("/api/v1/system/health/") == "SHORT"
    assert _classify("/api/v1/families") == "LONG"