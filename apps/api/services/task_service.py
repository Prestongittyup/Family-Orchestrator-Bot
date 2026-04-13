from __future__ import annotations

from uuid import uuid4

from apps.api.core.database import SessionLocal
from apps.api.models.task import Task


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

        return task

    finally:
        session.close()
