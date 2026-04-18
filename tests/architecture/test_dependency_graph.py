import importlib
import pkgutil

from .dependency_graph import ALLOWED_DEPENDENCIES


GRAPH_NODES = set(ALLOWED_DEPENDENCIES.keys())


def _canonical(module_name: str) -> str:
    if module_name.startswith("apps.api."):
        return module_name.replace("apps.api.", "", 1)
    return module_name


def get_module_imports(module_name: str) -> set[str]:
    """
    Resolve imports using live module inspection (no source-string scanning).
    Returns only dependencies that are part of the canonical graph namespace.
    """
    module = importlib.import_module(module_name)
    imports: set[str] = set()

    for attr_name in dir(module):
        try:
            attr = getattr(module, attr_name)
        except Exception:
            continue

        mod_name = getattr(attr, "__module__", None)
        if isinstance(mod_name, str):
            canonical = _canonical(mod_name)
            if canonical in GRAPH_NODES:
                imports.add(canonical)

    return imports


def _expand_endpoints_dependencies() -> set[str]:
    """Aggregate dependencies across endpoint modules under apps.api.endpoints."""
    deps: set[str] = set()
    package = importlib.import_module("apps.api.endpoints")

    for module_info in pkgutil.iter_modules(package.__path__, package.__name__ + "."):
        submodule_name = module_info.name
        deps.update(get_module_imports(submodule_name))

    return deps


def test_dependency_graph_is_valid():
    """
    Enforce strict layer boundaries using resolved imports.
    """
    for module, allowed in ALLOWED_DEPENDENCIES.items():
        try:
            if module == "apps.api.endpoints":
                actual = _expand_endpoints_dependencies()
            else:
                actual = get_module_imports(module)
        except Exception:
            continue

        disallowed = {dep for dep in actual if dep not in allowed and dep != module}
        assert not disallowed, f"Illegal dependency: {module} -> {sorted(disallowed)}"
