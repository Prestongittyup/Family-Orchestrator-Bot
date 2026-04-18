from __future__ import annotations

from pydantic import BaseModel


class TaskCreatedEvent(BaseModel):
    title: str
