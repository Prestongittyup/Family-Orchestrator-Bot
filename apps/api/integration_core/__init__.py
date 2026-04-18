from apps.api.integration_core.architecture_guard import (
    FORBIDDEN_IMPORT_PREFIXES,
    IntegrationCoreBoundaryViolation,
    assert_allowed_import,
    guarded_import,
    validate_loaded_module_boundaries,
)
from apps.api.integration_core.credentials import (
    CredentialStore,
    CredentialCipher,
    InMemoryOAuthCredentialStore,
    NoopCredentialCipher,
    OAuthCredential,
    OAuthCredentialRecord,
    OAuthCredentialStore,
    OAuthToken,
)
from apps.api.integration_core.identity import Household, User, UserIdentity
from apps.api.integration_core.identity_service import IdentityService
from apps.api.integration_core.orchestrator import (
    ExternalEvent,
    IntegrationOrchestrator,
    Orchestrator,
    create_orchestrator,
)
from apps.api.integration_core.brief_builder import BriefBuilder
from apps.api.integration_core.models.household_state import (
    CalendarEvent,
    HouseholdState,
    IntegrationHealth,
    WindowedCalendar,
)
from apps.api.integration_core.state_builder import StateBuilder
from apps.api.integration_core.event_adapter import adapt_external_events, external_event_to_os1_payload
from apps.api.integration_core.feature_flags import (
    INTEGRATION_CORE_INGESTION_ENABLED,
    flag_default,
    is_enabled,
)
from apps.api.integration_core.os1_bridge import _IdempotencyStore, get_idempotency_store, ingest_external_events
from apps.api.integration_core.providers import (
    GmailProviderMock,
    GmailPlaceholderProvider,
    GoogleCalendarProviderMock,
    GoogleCalendarPlaceholderProvider,
    IntegrationProvider,
    Provider,
)
from apps.api.integration_core.repository import IdentityRepository, InMemoryIdentityRepository
from apps.api.integration_core.registry import ProviderRegistry, build_default_provider_registry
from apps.api.integration_core.google_calendar_provider import (
    GoogleCalendarRealProvider,
    PROVIDER_NAME as GOOGLE_CALENDAR_REAL_PROVIDER_NAME,
    map_google_event_to_raw,
    build_event_id_debug,
)
from apps.api.integration_core.google_calendar_sandbox_runner import (
    GoogleCalendarSandboxRunner,
    SandboxRunResult,
)
from apps.api.integration_core.decision_engine import (
    DecisionEngine,
    DecisionContext,
)

__all__ = [
    "FORBIDDEN_IMPORT_PREFIXES",
    "IntegrationCoreBoundaryViolation",
    "assert_allowed_import",
    "guarded_import",
    "validate_loaded_module_boundaries",
    "CredentialCipher",
    "CredentialStore",
    "OAuthCredentialStore",
    "OAuthCredential",
    "OAuthToken",
    "OAuthCredentialRecord",
    "NoopCredentialCipher",
    "InMemoryOAuthCredentialStore",
    "User",
    "Household",
    "UserIdentity",
    "IdentityRepository",
    "InMemoryIdentityRepository",
    "IdentityService",
    "ExternalEvent",
    "BriefBuilder",
    "Orchestrator",
    "create_orchestrator",
    "IntegrationOrchestrator",
    "ingest_external_events",
    "get_idempotency_store",
    "_IdempotencyStore",
    "INTEGRATION_CORE_INGESTION_ENABLED",
    "is_enabled",
    "flag_default",
    "adapt_external_events",
    "external_event_to_os1_payload",
    "Provider",
    "IntegrationProvider",
    "GmailProviderMock",
    "GmailPlaceholderProvider",
    "GoogleCalendarProviderMock",
    "GoogleCalendarPlaceholderProvider",
    "ProviderRegistry",
    "build_default_provider_registry",
    "GoogleCalendarRealProvider",
    "GOOGLE_CALENDAR_REAL_PROVIDER_NAME",
    "map_google_event_to_raw",
    "build_event_id_debug",
    "GoogleCalendarSandboxRunner",
    "SandboxRunResult",
    "DecisionEngine",
    "DecisionContext",
]
