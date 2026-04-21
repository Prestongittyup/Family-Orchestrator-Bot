from __future__ import annotations

from uuid import uuid4

from apps.api.core.database import SessionLocal
from apps.api.models.task import Task
from apps.api.realtime.broadcaster import broadcaster


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

        broadcaster.publish_sync(
            household_id=household_id,
            event_type="task_created",
            payload={
                "task_id": task.id,
                "household_id": task.household_id,
                "title": task.title,
                "status": task.status,
                "priority": task.priority,
            },
        )

        return task

    finally:
        session.close()


def update_task_metadata(task_id: str, priority: str, category: str | None = None) -> None:
    """Update priority and metadata category on an existing task."""
    session = SessionLocal()
    try:
        task = session.get(Task, task_id)
        if task is None:
            return
        task.priority = priority
        if category is not None:
            task.description = category
        session.commit()
    finally:
        session.close()

