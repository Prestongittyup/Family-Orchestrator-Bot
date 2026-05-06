from __future__ import annotations
import pytest

from pathlib import Path
import re

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]



REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_SCOPES = (
    REPO_ROOT / "app",
    REPO_ROOT / "household_os",
    REPO_ROOT / "household_state",
)
API_SCOPE = REPO_ROOT / "app" / "api"
ARCH_DOC_ROOT = REPO_ROOT / "docs" / "architecture"
RFC_FILE = ARCH_DOC_ROOT / "RFC-001.md"
RFC_REFERENCE_DOCS = (
    ARCH_DOC_ROOT / "Architecture Implementation Plan.md",
    ARCH_DOC_ROOT / "Runtime Flow Spec.md",
    ARCH_DOC_ROOT / "Enforcement Checklist.md",
    ARCH_DOC_ROOT / "LAYER_MAP.md",
)

LLM_ALLOWLIST_PREFIXES = (
    REPO_ROOT / "app" / "adapters" / "llm" / "providers",
    REPO_ROOT / "app" / "adapters" / "llm" / "gateway.py",
)
COMMAND_RUNTIME_ALLOWLIST = {
    REPO_ROOT / "app" / "api" / "command.py",
    REPO_ROOT / "app" / "api" / "assistant_runtime.py",
    REPO_ROOT / "app" / "api" / "email.py",
    REPO_ROOT / "app" / "api" / "tasks.py",
    REPO_ROOT / "app" / "api" / "schedule.py",
    REPO_ROOT / "app" / "api" / "reminders.py",
    REPO_ROOT / "app" / "api" / "notifications.py",
}
SENSITIVE_STATE_WRITE_ALLOWLIST = {
    REPO_ROOT / "household_os" / "runtime" / "orchestrator.py",
}
EVENT_LOG_REPOSITORY_IMPORT_ALLOWLIST = {
    REPO_ROOT / "app" / "adapters" / "db" / "__init__.py",
    REPO_ROOT / "app" / "services" / "events" / "event_log_service.py",
    REPO_ROOT / "app" / "services" / "events" / "canonical_router_service.py",
}
ACTION_PIPELINE_DIRECT_CALL_ALLOWLIST = {
    REPO_ROOT / "household_os" / "runtime" / "orchestrator.py",
    REPO_ROOT / "household_os" / "runtime" / "action_pipeline.py",
}

FORBIDDEN_ARCHIVE_IMPORT = re.compile(r"from\s+archive\.|import\s+archive\.")
FORBIDDEN_LLM_PATTERNS = (
    re.compile(r"(^|\s)import\s+openai\b"),
    re.compile(r"from\s+openai\b"),
    re.compile(r"generativelanguage\.googleapis\.com"),
    re.compile(r"streamGenerateContent\?alt=sse"),
)
FORBIDDEN_STATE_PATTERNS = (
    re.compile(r"household_os_state_graph\.json"),
    re.compile(r"household_state_graph\.json"),
    re.compile(r"life_state\.json"),
    re.compile(r"write_text\(json\.dumps\("),
    re.compile(r"json\.dump\("),
)
SENSITIVE_STATE_WRITE_PATTERNS = (
    re.compile(r"RequestActionType\.WRITE_SENSITIVE_STATE"),
    re.compile(r"_write_sensitive_state\("),
)
EVENT_LOG_REPOSITORY_IMPORT_PATTERNS = (
    re.compile(r"from\s+app\.adapters\.db\.event_log_repository\s+import"),
    re.compile(r"import\s+app\.adapters\.db\.event_log_repository"),
)


def _iter_runtime_files() -> list[Path]:
    files: list[Path] = []
    for scope in RUNTIME_SCOPES:
        if not scope.exists():
            continue
        files.extend(path for path in scope.rglob("*.py") if path.is_file())
    return files


def _iter_api_files() -> list[Path]:
    if not API_SCOPE.exists():
        return []
    return [path for path in API_SCOPE.rglob("*.py") if path.is_file()]


def _is_llm_allowlisted(path: Path) -> bool:
    return any(path == prefix or prefix in path.parents for prefix in LLM_ALLOWLIST_PREFIXES)


@pytest.mark.system
def test_rfc_root_of_truth_is_present_and_referenced() -> None:
    assert RFC_FILE.exists(), "RFC-001.md must exist as canonical architecture artifact"

    rfc_text = RFC_FILE.read_text(encoding="utf-8")
    assert "IMMUTABLE CONTRACT" in rfc_text

    for doc in RFC_REFERENCE_DOCS:
        assert doc.exists(), f"Required architecture doc missing: {doc.relative_to(REPO_ROOT)}"
        source = doc.read_text(encoding="utf-8")
        assert "RFC-001.md" in source, f"Missing RFC-001 reference in {doc.relative_to(REPO_ROOT)}"
        assert "source of truth" in source.lower(), (
            f"Missing source-of-truth declaration in {doc.relative_to(REPO_ROOT)}"
        )


@pytest.mark.system
def test_archive_guard_runtime_modules_have_no_archive_imports() -> None:
    violations: list[str] = []
    for path in _iter_runtime_files():
        source = path.read_text(encoding="utf-8")
        if FORBIDDEN_ARCHIVE_IMPORT.search(source):
            violations.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))

    assert not violations, f"Runtime modules import archive namespace: {violations}"


@pytest.mark.system
def test_llm_guard_runtime_modules_only_use_gateway() -> None:
    violations: list[str] = []
    for path in _iter_runtime_files():
        if _is_llm_allowlisted(path):
            continue
        source = path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_LLM_PATTERNS:
            if pattern.search(source):
                violations.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))
                break

    assert not violations, f"Forbidden direct LLM provider usage detected: {violations}"


@pytest.mark.system
def test_state_guard_no_file_based_runtime_state_authority() -> None:
    violations: list[str] = []
    for path in _iter_runtime_files():
        source = path.read_text(encoding="utf-8")
        for pattern in FORBIDDEN_STATE_PATTERNS:
            if pattern.search(source):
                violations.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))
                break

    assert not violations, f"File-backed state persistence markers detected: {violations}"


@pytest.mark.system
def test_command_guard_only_allowlisted_api_modules_use_command_runtime() -> None:
    violations: list[str] = []
    for path in _iter_api_files():
        source = path.read_text(encoding="utf-8")
        uses_command_runtime = (
            "get_command_runtime_service(" in source or "handle_command(" in source
        )
        if uses_command_runtime and path not in COMMAND_RUNTIME_ALLOWLIST:
            violations.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))

    assert not violations, f"Command runtime usage leaked outside allowlist: {violations}"


@pytest.mark.system
def test_command_guard_has_single_command_post_entrypoint() -> None:
    command_api = REPO_ROOT / "app" / "api" / "command.py"
    source = command_api.read_text(encoding="utf-8")

    assert 'APIRouter(prefix="/command"' in source
    assert source.count("@router.post(") == 1


@pytest.mark.system
def test_sensitive_state_write_guard_single_entrypoint() -> None:
    violations: list[str] = []
    for path in _iter_runtime_files():
        source = path.read_text(encoding="utf-8")
        if any(pattern.search(source) for pattern in SENSITIVE_STATE_WRITE_PATTERNS):
            if path not in SENSITIVE_STATE_WRITE_ALLOWLIST:
                violations.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))

    assert not violations, f"Sensitive state write path leaked outside orchestrator: {violations}"


@pytest.mark.system
def test_event_log_repository_import_guard_only_allowlisted_modules() -> None:
    violations: list[str] = []
    for path in (REPO_ROOT / "app").rglob("*.py"):
        if not path.is_file():
            continue
        source = path.read_text(encoding="utf-8")
        if any(pattern.search(source) for pattern in EVENT_LOG_REPOSITORY_IMPORT_PATTERNS):
            if path not in EVENT_LOG_REPOSITORY_IMPORT_ALLOWLIST:
                violations.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))

    assert not violations, f"Event-log repository imports leaked outside event services: {violations}"


@pytest.mark.system
def test_action_pipeline_direct_invocation_guard_only_orchestrator() -> None:
    violations: list[str] = []
    for path in (REPO_ROOT / "household_os").rglob("*.py"):
        if not path.is_file():
            continue
        source = path.read_text(encoding="utf-8")
        direct_call = ".execute_approved_actions(" in source or ".reject_actions(" in source
        if direct_call and path not in ACTION_PIPELINE_DIRECT_CALL_ALLOWLIST:
            violations.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))

    assert not violations, f"Direct action pipeline calls leaked outside orchestrator: {violations}"