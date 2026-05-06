from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, Mapping

_LAYER_HEADING_PATTERN = re.compile(r"^###\s+\d+\.\s+(?P<name>.+)$")
_BULLET_PATTERN = re.compile(r"^-\s+(?P<item>.+)$")
_SECTION_HEADING_PATTERN = re.compile(r"^##\s+")
_SERVICE_BOUNDARY_PATTERN = re.compile(r"^(?P<name>[a-z_][a-z0-9_]*)/$")

_PACKAGE_ROOT_ALLOWLIST = {
    "app",
    "app.api",
    "app.services",
    "apps",
    "apps.api",
    "household_os",
    "household_os.runtime",
    "household_os.security",
    "tests",
    "scripts",
    "ci",
    "core",
    "core.architecture",
}


class ArchitectureContractError(RuntimeError):
    """Raised when architecture contract artifacts are missing or unreadable."""


@dataclass(frozen=True)
class ArchitectureContract:
    rfc_path: Path
    layer_map_path: Path
    allowed_layers: Mapping[str, tuple[str, ...]]
    allowed_module_prefixes: tuple[str, ...]
    allowed_service_boundaries: tuple[str, ...]
    forbidden_patterns: tuple[str, ...]
    layer_map_version: str = "RFC-001-derived"
    _layer_index: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def get_allowed_layers(self) -> dict[str, tuple[str, ...]]:
        return {layer: tuple(prefixes) for layer, prefixes in self.allowed_layers.items()}

    def get_forbidden_patterns(self) -> list[str]:
        return list(self.forbidden_patterns)

    def validate_module_path(self, path: str) -> bool:
        module = _normalize_module_path(path)
        if not module:
            return False
        if module in _PACKAGE_ROOT_ALLOWLIST:
            return True

        if module.startswith("app.services."):
            return any(
                module == boundary or module.startswith(f"{boundary}.")
                for boundary in self.allowed_service_boundaries
            )

        return any(
            module == prefix or module.startswith(f"{prefix}.")
            for prefix in self.allowed_module_prefixes
        )

    def validate_layer_compliance(self, layer: str, module: str) -> bool:
        layer_name = str(layer or "").strip()
        module_name = _normalize_module_path(module)
        if not layer_name or not module_name:
            return False
        prefixes = self._layer_index.get(layer_name)
        if not prefixes:
            return False
        return any(module_name == prefix or module_name.startswith(f"{prefix}.") for prefix in prefixes)

    def find_layer(self, module: str) -> str | None:
        module_name = _normalize_module_path(module)
        if not module_name:
            return None
        for layer_name, prefixes in self._layer_index.items():
            if any(module_name == prefix or module_name.startswith(f"{prefix}.") for prefix in prefixes):
                return layer_name
        return None


def _normalize_module_path(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("\\", "/")
    if text.endswith(".py"):
        text = text[:-3]
    text = text.strip("/")
    if text.startswith("./"):
        text = text[2:]
    text = text.replace("/", ".")
    if text.endswith(".__init__"):
        text = text[: -len(".__init__")]
    return text.strip(".")


def _path_item_to_module_prefix(path_item: str) -> str:
    raw = str(path_item or "").strip().strip("`")
    if not raw:
        return ""
    raw = raw.replace("\\", "/")
    if raw.endswith("/*"):
        raw = raw[:-2]
    if raw.endswith("*"):
        raw = raw[:-1]
    raw = raw.rstrip("/")
    if not raw:
        return ""
    return raw.replace("/", ".")


def _parse_layer_map(layer_map_source: str) -> tuple[dict[str, tuple[str, ...]], tuple[str, ...]]:
    layers: dict[str, list[str]] = {}
    forbidden_patterns: list[str] = []

    current_layer: str | None = None
    collecting_paths = False
    collecting_forbidden = False

    for line in layer_map_source.splitlines():
        stripped = line.strip()

        heading_match = _LAYER_HEADING_PATTERN.match(stripped)
        if heading_match:
            current_layer = heading_match.group("name").strip()
            layers.setdefault(current_layer, [])
            collecting_paths = False
            collecting_forbidden = False
            continue

        if current_layer is None:
            continue

        if stripped == "Path:":
            collecting_paths = True
            collecting_forbidden = False
            continue

        if stripped == "Forbidden:":
            collecting_paths = False
            collecting_forbidden = True
            continue

        bullet_match = _BULLET_PATTERN.match(stripped)
        if collecting_paths:
            if bullet_match:
                prefix = _path_item_to_module_prefix(bullet_match.group("item"))
                if prefix:
                    layers[current_layer].append(prefix)
                continue
            if stripped == "":
                continue
            if stripped.endswith(":") or _SECTION_HEADING_PATTERN.match(stripped):
                collecting_paths = False
                continue
            collecting_paths = False

        if collecting_forbidden:
            if bullet_match:
                forbidden_item = bullet_match.group("item").strip().rstrip(".")
                if forbidden_item:
                    forbidden_patterns.append(forbidden_item)
                continue
            if stripped == "":
                continue
            if stripped.endswith(":") or _SECTION_HEADING_PATTERN.match(stripped):
                collecting_forbidden = False
                continue
            collecting_forbidden = False

    normalized_layers = {
        layer: tuple(dict.fromkeys(prefixes))
        for layer, prefixes in layers.items()
        if prefixes
    }

    normalized_forbidden = tuple(dict.fromkeys(forbidden_patterns))
    return normalized_layers, normalized_forbidden


def _parse_rfc_forbidden_patterns(rfc_source: str) -> tuple[str, ...]:
    in_forbidden_section = False
    patterns: list[str] = []

    for line in rfc_source.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            in_forbidden_section = stripped == "## 5. Forbidden Anti-Patterns"
            continue
        if not in_forbidden_section:
            continue
        bullet_match = _BULLET_PATTERN.match(stripped)
        if bullet_match:
            pattern = bullet_match.group("item").strip().rstrip(".")
            if pattern:
                patterns.append(pattern)
        elif stripped == "":
            continue
        elif stripped.startswith("## "):
            break

    return tuple(dict.fromkeys(patterns))


def _parse_rfc_service_boundaries(rfc_source: str) -> tuple[str, ...]:
    boundaries: list[str] = []
    collecting = False

    for line in rfc_source.splitlines():
        stripped = line.strip()
        if not collecting:
            if stripped == "app/services/":
                collecting = True
            continue

        if stripped.startswith("```"):
            if boundaries:
                break
            continue

        if stripped == "":
            if boundaries:
                continue
            continue

        match = _SERVICE_BOUNDARY_PATTERN.match(stripped)
        if match:
            boundaries.append(f"app.services.{match.group('name')}")
            continue

        if boundaries and not line.startswith("    ") and not line.startswith("\t"):
            break

    return tuple(dict.fromkeys(boundaries))


def _resolve_repo_root(repo_root: Path | None) -> Path:
    if repo_root is not None:
        return Path(repo_root).resolve()
    return Path(__file__).resolve().parents[2]


@lru_cache(maxsize=1)
def _load_cached_contract(repo_root_text: str) -> ArchitectureContract:
    repo_root = Path(repo_root_text)
    rfc_path = repo_root / "docs" / "architecture" / "RFC-001.md"
    layer_map_path = repo_root / "docs" / "architecture" / "LAYER_MAP.md"

    if not rfc_path.exists():
        raise ArchitectureContractError(f"Missing architecture contract: {rfc_path}")
    if not layer_map_path.exists():
        raise ArchitectureContractError(f"Missing layer map contract: {layer_map_path}")

    rfc_source = rfc_path.read_text(encoding="utf-8")
    layer_map_source = layer_map_path.read_text(encoding="utf-8")

    layer_map_layers, layer_map_forbidden = _parse_layer_map(layer_map_source)
    rfc_forbidden = _parse_rfc_forbidden_patterns(rfc_source)
    rfc_service_boundaries = _parse_rfc_service_boundaries(rfc_source)

    inferred_service_boundaries = tuple(
        prefix
        for prefixes in layer_map_layers.values()
        for prefix in prefixes
        if prefix.startswith("app.services.")
    )

    allowed_service_boundaries = tuple(
        sorted(dict.fromkeys((*rfc_service_boundaries, *inferred_service_boundaries)))
    )

    all_layer_prefixes = tuple(
        sorted(
            dict.fromkeys(
                prefix
                for prefixes in layer_map_layers.values()
                for prefix in prefixes
            )
        )
    )

    allowed_module_prefixes = tuple(
        sorted(dict.fromkeys((*all_layer_prefixes, *allowed_service_boundaries)))
    )

    forbidden_patterns = tuple(dict.fromkeys((*layer_map_forbidden, *rfc_forbidden)))

    layer_index: dict[str, tuple[str, ...]] = {
        layer_name: tuple(prefixes)
        for layer_name, prefixes in layer_map_layers.items()
    }

    return ArchitectureContract(
        rfc_path=rfc_path,
        layer_map_path=layer_map_path,
        allowed_layers=MappingProxyType(layer_index),
        allowed_module_prefixes=allowed_module_prefixes,
        allowed_service_boundaries=allowed_service_boundaries,
        forbidden_patterns=forbidden_patterns,
        layer_map_version="RFC-001-derived",
        _layer_index=MappingProxyType(layer_index),
    )


def load_architecture_contract(repo_root: Path | None = None) -> ArchitectureContract:
    resolved_root = _resolve_repo_root(repo_root)
    return _load_cached_contract(str(resolved_root))


__all__ = [
    "ArchitectureContract",
    "ArchitectureContractError",
    "load_architecture_contract",
]
