"""
Compiler — Intent parsing and context resolution.

The workflow compilation layer that previously emitted DAG plans for OS-3 has
been removed. This package now exposes only the parser and context resolver
used by non-execution features.
"""

from legacy.compiler.intent_parser import Intent, IntentParser
from legacy.compiler.context_resolver import (
    ContextStore,
    InMemoryContextStore,
    ContextResolver,
    EnrichedIntent,
    HouseholdContext,
    UserContext,
    SystemContext,
)
__all__ = [
    "Intent",
    "IntentParser",
    "ContextStore",
    "InMemoryContextStore",
    "ContextResolver",
    "EnrichedIntent",
    "HouseholdContext",
    "UserContext",
    "SystemContext",
]
