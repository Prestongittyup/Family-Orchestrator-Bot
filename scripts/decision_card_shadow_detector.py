from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]

SOURCE_SUFFIXES = {".py", ".ts", ".tsx", ".js", ".jsx"}
EXCLUDED_DIRECTORY_NAMES = {
    ".git",
    ".venv",
    "archive",
    "dist",
    "build",
    "node_modules",
    "venv",
    "legacy",
    "tests",
    "__pycache__",
}

CANONICAL_DECISION_SUBSYSTEM_ROOT = REPO_ROOT / "decision_card_system"
CANONICAL_DECISION_CARD_REGISTRY = CANONICAL_DECISION_SUBSYSTEM_ROOT / "registry.py"
CANONICAL_DECISION_SUBSYSTEM_INIT = CANONICAL_DECISION_SUBSYSTEM_ROOT / "__init__.py"
CANONICAL_DECISION_CONTRACT_TS = REPO_ROOT / "hpal-frontend" / "src" / "runtime" / "decision_card_contract.ts"
LEGACY_DECISION_CARD_SHIM = REPO_ROOT / "core" / "replay" / "decision_card_registry.py"
CANONICAL_DECISION_REPLAY = REPO_ROOT / "core" / "replay" / "event_replay_engine.py"
CANONICAL_DECISION_RUNTIME = REPO_ROOT / "app" / "services" / "commands" / "runtime.py"
CANONICAL_DECISION_RULES = REPO_ROOT / "app" / "services" / "rules_engine" / "command_rules.py"
CANONICAL_DECISION_DEPENDENCY_GUARD = REPO_ROOT / "scripts" / "decision_card_dependency_graph_guard.py"

ALLOWED_DECISION_CARD_SYMBOL_PATHS = {
    CANONICAL_DECISION_CARD_REGISTRY,
    CANONICAL_DECISION_SUBSYSTEM_INIT,
    LEGACY_DECISION_CARD_SHIM,
    CANONICAL_DECISION_REPLAY,
    CANONICAL_DECISION_RUNTIME,
    CANONICAL_DECISION_RULES,
    CANONICAL_DECISION_DEPENDENCY_GUARD,
}

ALLOWED_DECISION_ENGINE_PATHS = {
    REPO_ROOT / "household_os" / "core" / "decision_engine.py",
    REPO_ROOT / "household_state" / "decision_engine.py",
    REPO_ROOT / "apps" / "api" / "integration_core" / "decision_engine.py",
}

AGENT_OR_FRONTEND_PREFIXES = (
    REPO_ROOT / "assistant",
    REPO_ROOT / "app" / "services" / "agents",
    REPO_ROOT / "apps" / "api" / "assistant_runtime_router.py",
    REPO_ROOT / "hpal-frontend" / "src",
)

DECISION_CARD_EVENT_TOKENS = (
    "DecisionCardGenerated",
    "DecisionCardSurfaced",
    "DecisionCardAcknowledged",
    "DecisionCardResolved",
    "DecisionCardApplied",
    "decision.card.",
)

FORBIDDEN_INLINE_BRANCH_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "inline_conflict_branch",
        re.compile(r"\bif\s+conflict\b", re.IGNORECASE),
    ),
    (
        "inline_decision_fallback",
        re.compile(r"\b(fallback\s+decision|decision\s+fallback)\b", re.IGNORECASE),
    ),
    (
        "choose_between_branch",
        re.compile(r"\bchoose\s+between\b", re.IGNORECASE),
    ),
    (
        "resolve_conflict_phrase",
        re.compile(r"\bresolve\s+conflict\b", re.IGNORECASE),
    ),
)

FORBIDDEN_SHADOW_CLASS_PATTERN = re.compile(
    r"^\s*class\s+\w*(DecisionRegistry|ConflictResolver|DecisionResolver)\w*\s*[:(]",
    re.IGNORECASE | re.MULTILINE,
)

CREATE_DECISION_CARD_DEFINITION_PATTERN = re.compile(
    r"(^\s*def\s+create_decision_card\s*\(|^\s*def\s+createDecisionCard\s*\()",
    re.MULTILINE,
)

CREATE_DECISION_CARD_USAGE_PATTERN = re.compile(
    r"\b(createDecisionCard|create_decision_card|reduce_decision_card_projection)\s*\(",
)

DECISION_CARD_IMPORT_PATTERN = re.compile(
    r"(^\s*from\s+(?:decision_card_system\.registry|core\.replay\.decision_card_registry)\s+import\s+.+$|^\s*import\s+(?:decision_card_system\.registry|core\.replay\.decision_card_registry)\b)",
    re.MULTILINE,
)

DECISION_ENGINE_CLASS_PATTERN = re.compile(
    r"^\s*class\s+\w*DecisionEngine\w*\s*[:(]",
    re.MULTILINE,
)


@dataclass(frozen=True)
class ShadowViolation:
    code: str
    path: Path
    message: str
    line: int | None = None

    def render(self, root: Path) -> str:
        rel = self.path.relative_to(root).as_posix()
        if self.line is not None:
            return f"[{self.code}] {rel}:{self.line} {self.message}"
        return f"[{self.code}] {rel} {self.message}"


def _iter_source_files(root: Path) -> Iterable[Path]:
    for file_path in root.rglob("*"):
        if not file_path.is_file():
            continue
        if file_path.suffix.lower() not in SOURCE_SUFFIXES:
            continue
        if any(part in EXCLUDED_DIRECTORY_NAMES for part in file_path.parts):
            continue
        yield file_path


def _line_number(text: str, index: int) -> int:
    return text.count("\n", 0, index) + 1


def _is_agent_or_frontend_file(path: Path) -> bool:
    for prefix in AGENT_OR_FRONTEND_PREFIXES:
        if prefix.is_dir() and prefix in path.parents:
            return True
        if prefix.is_file() and path == prefix:
            return True
    return False


def collect_decision_shadow_violations(root: Path | None = None) -> list[ShadowViolation]:
    repo_root = (root or REPO_ROOT).resolve()
    violations: list[ShadowViolation] = []

    for file_path in _iter_source_files(repo_root):
        text = file_path.read_text(encoding="utf-8", errors="ignore")

        if file_path.name == "decision_card_registry.py" and file_path.resolve() != LEGACY_DECISION_CARD_SHIM:
            violations.append(
                ShadowViolation(
                    code="parallel_registry_file",
                    path=file_path,
                    message="only core/replay/decision_card_registry.py may exist as the legacy decision-card shim",
                )
            )

        if file_path.name == "registry.py" and "decision_card" in file_path.as_posix() and file_path.resolve() != CANONICAL_DECISION_CARD_REGISTRY:
            violations.append(
                ShadowViolation(
                    code="parallel_registry_file",
                    path=file_path,
                    message="only decision_card_system/registry.py may define decision-card registry behavior",
                )
            )

        if file_path.resolve() not in {CANONICAL_DECISION_CARD_REGISTRY}:
            match = CREATE_DECISION_CARD_DEFINITION_PATTERN.search(text)
            if match:
                violations.append(
                    ShadowViolation(
                        code="shadow_create_api",
                        path=file_path,
                        line=_line_number(text, match.start()),
                        message="shadow decision-card creator detected outside canonical registry",
                    )
                )

        if FORBIDDEN_SHADOW_CLASS_PATTERN.search(text):
            match = FORBIDDEN_SHADOW_CLASS_PATTERN.search(text)
            violations.append(
                ShadowViolation(
                    code="shadow_class",
                    path=file_path,
                    line=_line_number(text, match.start()) if match else None,
                    message="registry/resolver-like class found outside canonical decision-card authority",
                )
            )

        if file_path.resolve() not in ALLOWED_DECISION_ENGINE_PATHS:
            match = DECISION_ENGINE_CLASS_PATTERN.search(text)
            if match:
                violations.append(
                    ShadowViolation(
                        code="duplicate_decision_engine",
                        path=file_path,
                        line=_line_number(text, match.start()),
                        message="DecisionEngine class outside approved decision engine modules",
                    )
                )

        if file_path.resolve() not in ALLOWED_DECISION_CARD_SYMBOL_PATHS:
            import_match = DECISION_CARD_IMPORT_PATTERN.search(text)
            if import_match:
                violations.append(
                    ShadowViolation(
                        code="unauthorized_registry_import",
                        path=file_path,
                        line=_line_number(text, import_match.start()),
                        message="decision-card registry imports are only allowed in canonical runtime/rules/replay modules",
                    )
                )

            usage_match = CREATE_DECISION_CARD_USAGE_PATTERN.search(text)
            if usage_match:
                violations.append(
                    ShadowViolation(
                        code="unauthorized_registry_usage",
                        path=file_path,
                        line=_line_number(text, usage_match.start()),
                        message="decision-card registry function usage outside canonical authority path",
                    )
                )

        if (
            file_path.resolve() not in ALLOWED_DECISION_CARD_SYMBOL_PATHS
            and file_path.resolve() != CANONICAL_DECISION_CONTRACT_TS
            and _is_agent_or_frontend_file(file_path)
        ):
            for token in DECISION_CARD_EVENT_TOKENS:
                token_index = text.find(token)
                if token_index == -1:
                    continue
                violations.append(
                    ShadowViolation(
                        code="agent_frontend_decision_bypass",
                        path=file_path,
                        line=_line_number(text, token_index),
                        message=f"agent/frontend layer references decision-card token '{token}'",
                    )
                )

        if file_path.resolve() not in ALLOWED_DECISION_CARD_SYMBOL_PATHS:
            for code, pattern in FORBIDDEN_INLINE_BRANCH_PATTERNS:
                match = pattern.search(text)
                if not match:
                    continue
                violations.append(
                    ShadowViolation(
                        code=code,
                        path=file_path,
                        line=_line_number(text, match.start()),
                        message="inline decision/conflict branching detected outside canonical decision-card runtime",
                    )
                )

    deduped: dict[tuple[str, Path, int | None, str], ShadowViolation] = {}
    for violation in violations:
        key = (violation.code, violation.path, violation.line, violation.message)
        deduped[key] = violation

    return sorted(deduped.values(), key=lambda item: (item.path.as_posix(), item.line or 0, item.code))


def run_decision_shadow_detector(root: Path | None = None) -> int:
    repo_root = (root or REPO_ROOT).resolve()
    violations = collect_decision_shadow_violations(repo_root)
    if not violations:
        print("decision-card shadow detector: PASS")
        return 0

    print("decision-card shadow detector: FAIL")
    for violation in violations:
        print(violation.render(repo_root))
    return 1


if __name__ == "__main__":
    raise SystemExit(run_decision_shadow_detector())
