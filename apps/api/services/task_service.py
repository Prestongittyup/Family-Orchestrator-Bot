from __future__ import annotations

from uuid import uuid4
from datetime import UTC, datetime

from apps.api.core.database import SessionLocal
from apps.api.models.task import Task
from apps.api.schemas.event import SystemEvent
from apps.api.services.canonical_event_adapter import CanonicalEventAdapter
from apps.api.services.canonical_event_router import canonical_event_router


class _TaskServiceRouter:
    @staticmethod
    def emit(event: SystemEvent) -> None:
        canonical_event_router.route(
            CanonicalEventAdapter.to_envelope(event),
            persist=False,
            dispatch=False,
        )


router = _TaskServiceRouter()


def create_task(household_id: str, title: str) -> Task:
    session = SessionLocal()
    try:
        task = Task(
            id=str(uuid4()),
            household_id=household_id,
            title=title,
            description=None,
            status="pending",
            priority="medium",
        )

        session.add(task)
        session.flush()   # ensures DB assigns lifecycle hooks properly
        session.commit()
        session.refresh(task)

        task_created_payload = {
            "task_id": task.id,
            "household_id": task.household_id,
            "title": task.title,
            "status": task.status,
            "priority": task.priority,
        }

        try:
            router.emit(
                SystemEvent.task_created(
                    household_id=household_id,
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload=task_created_payload,
                )
            )
        except AttributeError:
            router.emit(
                SystemEvent(
                    household_id=household_id,
                    type="task_created",
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload=task_created_payload,
                )
            )

        return task

    except ValueError as e:
        try:
            router.emit(
                SystemEvent.task_creation_failed(
                    household_id=household_id,
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload={
                        "reason": "validation_error",
                        "error_message": str(e),
                        "input": {
                            "household_id": household_id,
                            "title": title,
                        },
                    },
                )
            )
        except AttributeError:
            router.emit(
                SystemEvent(
                    household_id=household_id,
                    type="task_creation_failed",
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload={
                        "reason": "validation_error",
                        "error_message": str(e),
                        "input": {
                            "household_id": household_id,
                            "title": title,
                        },
                    },
                )
            )
        raise
    except Exception as e:
        try:
            router.emit(
                SystemEvent.task_creation_failed(
                    household_id=household_id,
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload={
                        "reason": "internal_error",
                        "error_message": str(e),
                        "input": {
                            "household_id": household_id,
                            "title": title,
                        },
                    },
                )
            )
        except AttributeError:
            router.emit(
                SystemEvent(
                    household_id=household_id,
                    type="task_creation_failed",
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload={
                        "reason": "internal_error",
                        "error_message": str(e),
                        "input": {
                            "household_id": household_id,
                            "title": title,
                        },
                    },
                )
            )
        raise

    finally:
        session.close()


def update_task_metadata(task_id: str, priority: str, category: str | None = None) -> None:
    """Update priority and metadata category on an existing task."""
    session = SessionLocal()
    try:
        task = session.get(Task, task_id)
        if task is None:
            return
        
        # Capture old metadata before mutation
        old_metadata = {
            "priority": task.priority,
            "description": task.description,
        }
        
        task.priority = priority
        if category is not None:
            task.description = category
        session.commit()
        
        # Emit canonical event after successful commit
        new_metadata = {
            "priority": task.priority,
            "description": task.description,
        }
        
        changed_fields: list[str] = []
        if old_metadata["priority"] != new_metadata["priority"]:
            changed_fields.append("priority")
        if old_metadata["description"] != new_metadata["description"]:
            changed_fields.append("description")

        task_metadata_updated_payload = {
            "task_id": task_id,
            "changed_fields": changed_fields,
            "old_metadata": old_metadata,
            "new_metadata": new_metadata,
        }

        try:
            router.emit(
                SystemEvent.task_updated(
                    household_id=task.household_id,
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload=task_metadata_updated_payload,
                )
            )
        except AttributeError:
            router.emit(
                SystemEvent(
                    household_id=task.household_id,
                    type="task_updated",
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload=task_metadata_updated_payload,
                )
            )
    except ValueError as e:
        household_id_for_failure = task.household_id if "task" in locals() and task is not None else "unknown"
        try:
            router.emit(
                SystemEvent.task_update_failed(
                    household_id=household_id_for_failure,
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload={
                        "reason": "validation_error",
                        "error_message": str(e),
                        "input": {
                            "task_id": task_id,
                            "priority": priority,
                            "category": category,
                        },
                    },
                )
            )
        except AttributeError:
            router.emit(
                SystemEvent(
                    household_id=household_id_for_failure,
                    type="task_update_failed",
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload={
                        "reason": "validation_error",
                        "error_message": str(e),
                        "input": {
                            "task_id": task_id,
                            "priority": priority,
                            "category": category,
                        },
                    },
                )
            )
        raise
    except Exception as e:
        household_id_for_failure = task.household_id if "task" in locals() and task is not None else "unknown"
        try:
            router.emit(
                SystemEvent.task_update_failed(
                    household_id=household_id_for_failure,
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload={
                        "reason": "internal_error",
                        "error_message": str(e),
                        "input": {
                            "task_id": task_id,
                            "priority": priority,
                            "category": category,
                        },
                    },
                )
            )
        except AttributeError:
            router.emit(
                SystemEvent(
                    household_id=household_id_for_failure,
                    type="task_update_failed",
                    timestamp=datetime.now(UTC),
                    source="task_service",
                    payload={
                        "reason": "internal_error",
                        "error_message": str(e),
                        "input": {
                            "task_id": task_id,
                            "priority": priority,
                            "category": category,
                        },
                    },
                )
            )
        raise
    finally:
        session.close()

