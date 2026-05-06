from __future__ import annotations

from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
_CANONICAL_WRITE_DIR = _REPO_ROOT / "verification_reports" / "root_artifacts" / "reports"
_FALLBACK_READ_DIRS: tuple[Path, ...] = (
    _CANONICAL_WRITE_DIR,
    _REPO_ROOT,
    _REPO_ROOT / "verification_reports" / "root_artifacts" / "reports",
    _REPO_ROOT / "verification_reports",
)


def _normalize_file_name(file_name: str | Path) -> Path:
    candidate = Path(file_name)
    if candidate.is_absolute():
        return candidate
    return Path(str(candidate).lstrip("\\/"))


def artifact_write_path(file_name: str | Path) -> Path:
    """Return canonical write target for generated artifacts under verification_reports/root_artifacts/reports."""
    normalized = _normalize_file_name(file_name)
    if normalized.is_absolute():
        normalized.parent.mkdir(parents=True, exist_ok=True)
        return normalized

    target = _CANONICAL_WRITE_DIR / normalized
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def artifact_read_path(file_name: str | Path) -> Path:
    """
    Resolve an artifact for reading.

    Search order:
    1) repo root artifacts
    2) verification report snapshots
    3) fallback repo-root path (may not exist)
    """
    normalized = _normalize_file_name(file_name)
    if normalized.is_absolute():
        return normalized

    for directory in _FALLBACK_READ_DIRS:
        candidate = directory / normalized
        if candidate.exists():
            return candidate

    return _REPO_ROOT / normalized
