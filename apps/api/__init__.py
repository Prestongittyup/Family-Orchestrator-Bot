"""Canonical API compatibility namespace used by architecture tests."""

from core.architecture.architecture_guard import enforce_import_boundary

enforce_import_boundary(module_path=__name__, module_file=__file__)
