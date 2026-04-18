from __future__ import annotations

import re

from apps.api.core.database import SessionLocal
from apps.api.models.task import Task
from modules.core.models.module_output import ModuleOutput, Proposal, Signal


_HIGH_EFFORT_KEYWORDS = {
    "install", "assemble", "repair", "renovate", "deep clean", "replace", "migrate", "refactor", "audit"
}
_LOW_EFFORT_KEYWORDS = {
    "call", "email", "text", "ping", "check", "review", "confirm", "remind"
}


def _tokenize(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def _infer_category(title: str, description: str) -> str:
    token_space = f"{title} {description}".lower()
    tokens = _tokenize(token_space)

    if {"health", "doctor", "med", "medicine", "workout", "exercise", "checkup"} & tokens:
        return "health"
    if {"maintain", "maintenance", "repair", "fix", "clean", "replace"} & tokens:
        return "maintenance"
    if {"event", "meeting", "appointment", "trip"} & tokens:
        return "event_prep"
    if {"task", "todo", "followup", "follow", "plan", "planning"} & tokens:
        return "task"
    return "other"


def _infer_effort(title: str, description: str) -> str:
    token_space = f"{title} {description}".lower()
    if any(phrase in token_space for phrase in _HIGH_EFFORT_KEYWORDS):
        return "high"
    if any(phrase in token_space for phrase in _LOW_EFFORT_KEYWORDS):
        return "low"
    return "medium"


def _infer_duration_units(title: str, description: str, category: str) -> int:
    token_space = f"{title} {description}".lower()
    if category == "health":
        return 2
    if category == "maintenance":
        return 2
    if "urgent" in token_space or "asap" in token_space:
        return 1
    if any(word in token_space for word in ("clean", "repair", "assemble", "install", "migrate", "refactor")):
        return 2
    return 1


def task_module(household_id: str) -> ModuleOutput:
    """
    Generate task proposals for a household by querying actual task data from SQLite.
    
    Queries all tasks for the household where status is 'queued' or 'running'.
    Converts Task objects to Proposal objects for the brief.
    """
    session = SessionLocal()
    try:
        # Query actual tasks from database
        active_tasks = session.query(Task).filter(
            Task.household_id == household_id,
            Task.status.in_(["queued", "running", "pending"])
        ).all()
        
        proposals = []
        signals = []
        
        # Convert Task objects to Proposals
        for task in active_tasks:
            title = str(task.title or "")
            description = str(task.description or "")
            category = _infer_category(title, description)
            effort = _infer_effort(title, description)
            duration = _infer_duration_units(title, description, category)

            proposals.append(
                Proposal(
                    id=task.id,
                    type="task_action",
                    title=title,
                    description=description or f"Priority: {task.priority}",
                    priority=_priority_to_numeric(task.priority),
                    source_module="task_module",
                    duration=duration,
                    effort=effort,
                    category=category,
                )
            )
            
            # Add signal for overdue tasks
            if task.status == "queued" and task.priority.lower() == "high":
                signals.append(
                    Signal(
                        id=f"{task.id}_overdue_signal",
                        type="overdue_task",
                        message=f"High priority task '{task.title}' is pending.",
                        severity="high",
                        source_module="task_module",
                    )
                )
        
        # Compute confidence based on data freshness
        confidence = 0.95 if active_tasks else 0.70
        
        return ModuleOutput(
            module="task_module",
            proposals=proposals,
            signals=signals,
            confidence=confidence,
            metadata={
                "household_id": household_id,
                "task_count": len(active_tasks),
                "source": "sqlite",
            },
        )
    except Exception as e:
        # Fallback: return empty but valid ModuleOutput if database fails
        return ModuleOutput(
            module="task_module",
            proposals=[],
            signals=[],
            confidence=0.0,
            metadata={
                "household_id": household_id,
                "error": str(e),
                "source": "sqlite_error",
            },
        )
    finally:
        session.close()


def _priority_to_numeric(priority_str: str) -> int:
    """Convert string priority to numeric (1-5 scale)."""
    mapping = {
        "low": 1,
        "medium": 3,
        "high": 5,
    }
    return mapping.get(priority_str.lower(), 3)

