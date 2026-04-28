"""
Intent Contract Layer - Schema Definitions
===========================================

Defines all intent types and their validation schemas.

Design principles:
  - Closed enum of intents (no free-form execution)
  - Pydantic models for strict validation
  - All required fields explicitly declared
  - Optional fields have clear defaults
  - No implicit coercion or guessing
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, validator


# ---------------------------------------------------------------------------
# INTENT ENUM — closed set of all supported intents
# ---------------------------------------------------------------------------


class IntentType(str, Enum):
    """Enumeration of all supported user intents."""

    # Task lifecycle
    CREATE_TASK = "create_task"
    COMPLETE_TASK = "complete_task"
    RESCHEDULE_TASK = "reschedule_task"

    # Event lifecycle
    CREATE_EVENT = "create_event"
    UPDATE_EVENT = "update_event"
    DELETE_EVENT = "delete_event"

    # Plan lifecycle
    CREATE_PLAN = "create_plan"
    UPDATE_PLAN = "update_plan"
    RECOMPUTE_PLAN = "recompute_plan"


# ---------------------------------------------------------------------------
# INTENT SCHEMAS — strict validation for each intent type
# ---------------------------------------------------------------------------


class CreateTaskIntent(BaseModel):
    """
    CREATE_TASK intent schema.

    Fields:
      task_name: required, non-empty string
      due_time: optional, ISO 8601 datetime
      plan_id: optional, reference to parent plan
    """

    task_name: str = Field(..., min_length=1, max_length=256)
    due_time: Optional[datetime] = Field(None, description="Optional due time")
    plan_id: Optional[str] = Field(None, description="Optional parent plan ID")

    class Config:
        frozen = True
        schema_extra = {
            "example": {
                "task_name": "Prepare dinner",
                "due_time": "2026-04-20T18:00:00",
                "plan_id": "plan-abc123",
            }
        }


class CompleteTaskIntent(BaseModel):
    """
    COMPLETE_TASK intent schema.

    Fields:
      task_id: required, non-empty string (must exist)
    """

    task_id: str = Field(..., min_length=1, max_length=256)

    class Config:
        frozen = True
        schema_extra = {"example": {"task_id": "task-xyz789"}}


class RescheduleTaskIntent(BaseModel):
    """
    RESCHEDULE_TASK intent schema.

    Fields:
      task_id: required, non-empty string (must exist)
      new_time: required, ISO 8601 datetime
      reason: optional, human-readable reason for reschedule
    """

    task_id: str = Field(..., min_length=1, max_length=256)
    new_time: datetime = Field(...)
    reason: Optional[str] = Field(None, max_length=512)

    class Config:
        frozen = True
        schema_extra = {
            "example": {
                "task_id": "task-xyz789",
                "new_time": "2026-04-20T19:30:00",
                "reason": "event_conflict",
            }
        }


class CreateEventIntent(BaseModel):
    """
    CREATE_EVENT intent schema.

    Fields:
      event_name: required, non-empty string
      start_time: required, ISO 8601 datetime
      end_time: optional, ISO 8601 datetime
      description: optional, event description
    """

    event_name: str = Field(..., min_length=1, max_length=256)
    start_time: datetime = Field(...)
    end_time: Optional[datetime] = Field(None)
    description: Optional[str] = Field(None, max_length=1024)

    @validator("end_time")
    def end_after_start(cls, v, values):
        if v is not None and "start_time" in values and v <= values["start_time"]:
            raise ValueError("end_time must be after start_time")
        return v

    class Config:
        frozen = True
        schema_extra = {
            "example": {
                "event_name": "School pickup",
                "start_time": "2026-04-20T15:00:00",
                "end_time": "2026-04-20T15:30:00",
            }
        }


class UpdateEventIntent(BaseModel):
    """
    UPDATE_EVENT intent schema.

    Fields:
      event_id: required, non-empty string (must exist)
      event_name: optional, updated name
      start_time: optional, updated start time
      end_time: optional, updated end time
      description: optional, updated description
    """

    event_id: str = Field(..., min_length=1, max_length=256)
    event_name: Optional[str] = Field(None, min_length=1, max_length=256)
    start_time: Optional[datetime] = Field(None)
    end_time: Optional[datetime] = Field(None)
    description: Optional[str] = Field(None, max_length=1024)

    @validator("end_time")
    def end_after_start(cls, v, values):
        start_time = values.get("start_time")
        if v is not None and start_time is not None and v <= start_time:
            raise ValueError("end_time must be after start_time")
        return v

    class Config:
        frozen = True
        schema_extra = {
            "example": {
                "event_id": "event-abc123",
                "event_name": "Updated event name",
                "start_time": "2026-04-20T15:30:00",
            }
        }


class DeleteEventIntent(BaseModel):
    """
    DELETE_EVENT intent schema.

    Fields:
      event_id: required, non-empty string (must exist)
      reason: optional, reason for deletion
    """

    event_id: str = Field(..., min_length=1, max_length=256)
    reason: Optional[str] = Field(None, max_length=512)

    class Config:
        frozen = True
        schema_extra = {
            "example": {
                "event_id": "event-abc123",
                "reason": "cancelled_by_user",
            }
        }


class CreatePlanIntent(BaseModel):
    """
    CREATE_PLAN intent schema.

    Fields:
      plan_name: required, non-empty string
      description: optional, plan description
      start_date: optional, ISO 8601 datetime
      end_date: optional, ISO 8601 datetime
    """

    plan_name: str = Field(..., min_length=1, max_length=256)
    description: Optional[str] = Field(None, max_length=1024)
    start_date: Optional[datetime] = Field(None)
    end_date: Optional[datetime] = Field(None)

    @validator("end_date")
    def end_after_start(cls, v, values):
        start_date = values.get("start_date")
        if v is not None and start_date is not None and v <= start_date:
            raise ValueError("end_date must be after start_date")
        return v

    class Config:
        frozen = True
        schema_extra = {
            "example": {
                "plan_name": "Weekend activities",
                "start_date": "2026-04-25T00:00:00",
                "end_date": "2026-04-26T23:59:59",
            }
        }


class UpdatePlanIntent(BaseModel):
    """
    UPDATE_PLAN intent schema.

    Fields:
      plan_id: required, non-empty string (must exist)
      plan_name: optional, updated name
      description: optional, updated description
      start_date: optional, updated start date
      end_date: optional, updated end date
    """

    plan_id: str = Field(..., min_length=1, max_length=256)
    plan_name: Optional[str] = Field(None, min_length=1, max_length=256)
    description: Optional[str] = Field(None, max_length=1024)
    start_date: Optional[datetime] = Field(None)
    end_date: Optional[datetime] = Field(None)

    @validator("end_date")
    def end_after_start(cls, v, values):
        start_date = values.get("start_date")
        if v is not None and start_date is not None and v <= start_date:
            raise ValueError("end_date must be after start_date")
        return v

    class Config:
        frozen = True
        schema_extra = {
            "example": {
                "plan_id": "plan-abc123",
                "plan_name": "Updated plan name",
            }
        }


class RecomputePlanIntent(BaseModel):
    """
    RECOMPUTE_PLAN intent schema.

    Fields:
      plan_id: required, non-empty string (must exist)
      reason: optional, reason for recomputation
    """

    plan_id: str = Field(..., min_length=1, max_length=256)
    reason: Optional[str] = Field(None, max_length=512)

    class Config:
        frozen = True
        schema_extra = {
            "example": {
                "plan_id": "plan-abc123",
                "reason": "schedule_changed",
            }
        }


# ---------------------------------------------------------------------------
# INTENT SCHEMA MAPPING — map intent type to its schema class
# ---------------------------------------------------------------------------


INTENT_SCHEMA_MAP = {
    IntentType.CREATE_TASK: CreateTaskIntent,
    IntentType.COMPLETE_TASK: CompleteTaskIntent,
    IntentType.RESCHEDULE_TASK: RescheduleTaskIntent,
    IntentType.CREATE_EVENT: CreateEventIntent,
    IntentType.UPDATE_EVENT: UpdateEventIntent,
    IntentType.DELETE_EVENT: DeleteEventIntent,
    IntentType.CREATE_PLAN: CreatePlanIntent,
    IntentType.UPDATE_PLAN: UpdatePlanIntent,
    IntentType.RECOMPUTE_PLAN: RecomputePlanIntent,
}


# ---------------------------------------------------------------------------
# EXTRACTED FIELDS CONTAINER
# ---------------------------------------------------------------------------


class ExtractedFields(BaseModel):
    """
    Container for fields extracted from raw user input.

    Stores raw extracted data before type conversion and validation.
    """

    data: dict = Field(default_factory=dict)

    def get(self, key: str, default=None):
        return self.data.get(key, default)

    def set(self, key: str, value):
        self.data[key] = value

    class Config:
        schema_extra = {
            "example": {
                "data": {
                    "task_name": "Buy groceries",
                    "due_time": "2026-04-20T18:00:00",
                }
            }
        }
