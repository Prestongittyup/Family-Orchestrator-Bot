from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from pydantic import BaseModel, Field


class SystemEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    household_id: str
    type: str
    source: str
    payload: dict
    severity: str = "info"
    timestamp: datetime | None = None
    idempotency_key: str | None = None
    actor_type: str | None = None
    watermark: int | None = None
    signature: str | None = None


class CalendarEventCreated(SystemEvent):
    def __init__(self, *, household_id: str, event_id: str, changes: dict, source: str = "calendar_service"):
        super().__init__(
            household_id=household_id,
            type="calendar_event_created",
            source=source,
            payload={"event_id": event_id, "changes": changes},
        )


class CalendarEventUpdated(SystemEvent):
    def __init__(self, *, household_id: str, event_id: str, changes: dict, source: str = "calendar_service"):
        super().__init__(
            household_id=household_id,
            type="calendar_event_updated",
            source=source,
            payload={"event_id": event_id, "changes": changes},
        )


class CalendarEventDeleted(SystemEvent):
    def __init__(self, *, household_id: str, event_id: str, changes: dict, source: str = "calendar_service"):
        super().__init__(
            household_id=household_id,
            type="calendar_event_deleted",
            source=source,
            payload={"event_id": event_id, "changes": changes},
        )


class CalendarEventUpdateFailed(SystemEvent):
    def __init__(
        self,
        *,
        household_id: str,
        reason: str,
        error_message: str,
        input: dict,
        source: str = "calendar_service",
    ):
        super().__init__(
            household_id=household_id,
            type="calendar_event_update_failed",
            source=source,
            payload={
                "reason": reason,
                "error_message": error_message,
                "input": input,
            },
        )


class CalendarEventCreationFailed(SystemEvent):
    def __init__(
        self,
        *,
        household_id: str,
        reason: str,
        error_message: str,
        input: dict,
        source: str = "calendar_service",
    ):
        super().__init__(
            household_id=household_id,
            type="calendar_event_creation_failed",
            source=source,
            payload={
                "reason": reason,
                "error_message": error_message,
                "input": input,
            },
        )


SystemEvent.CalendarEventCreated = CalendarEventCreated
SystemEvent.CalendarEventUpdated = CalendarEventUpdated
SystemEvent.CalendarEventDeleted = CalendarEventDeleted
SystemEvent.CalendarEventUpdateFailed = CalendarEventUpdateFailed
SystemEvent.CalendarEventCreationFailed = CalendarEventCreationFailed


class ChatMessageSent(SystemEvent):
    def __init__(
        self,
        *,
        household_id: str,
        message_id: str,
        user_id: str,
        content: str,
        source: str = "chat_gateway_service",
    ):
        super().__init__(
            household_id=household_id,
            type="chat_message_sent",
            source=source,
            payload={
                "message_id": message_id,
                "user_id": user_id,
                "content": content,
            },
        )


class ChatMessageFailed(SystemEvent):
    def __init__(
        self,
        *,
        household_id: str,
        reason: str,
        error_message: str,
        input: dict,
        source: str = "chat_gateway_service",
    ):
        super().__init__(
            household_id=household_id,
            type="chat_message_failed",
            source=source,
            payload={
                "reason": reason,
                "error_message": error_message,
                "input": input,
            },
        )


SystemEvent.ChatMessageSent = ChatMessageSent
SystemEvent.ChatMessageFailed = ChatMessageFailed


class EmailParsed(SystemEvent):
    def __init__(
        self,
        *,
        household_id: str,
        email_id: str,
        source: str,
        parsed_fields: dict,
    ):
        super().__init__(
            household_id=household_id,
            type="email_parsed",
            source=source,
            payload={
                "email_id": email_id,
                "parsed_fields": parsed_fields,
            },
        )


class EmailParseFailed(SystemEvent):
    def __init__(
        self,
        *,
        household_id: str,
        reason: str,
        error_message: str,
        raw_input: dict,
        source: str = "ingestion",
    ):
        super().__init__(
            household_id=household_id,
            type="email_parse_failed",
            source=source,
            payload={
                "reason": reason,
                "error_message": error_message,
                "raw_input": raw_input,
            },
        )


SystemEvent.EmailParsed = EmailParsed
SystemEvent.EmailParseFailed = EmailParseFailed


class HouseholdCreated(SystemEvent):
    def __init__(self, *, household_id: str, name: str, timezone: str, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="household_created",
            source=source,
            payload={"household_id": household_id, "name": name, "timezone": timezone},
        )


class HouseholdUpdated(SystemEvent):
    def __init__(self, *, household_id: str, changes: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="household_updated",
            source=source,
            payload={"household_id": household_id, "changes": changes},
        )


class HouseholdCreationFailed(SystemEvent):
    def __init__(self, *, household_id: str, reason: str, error_message: str, input: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="household_creation_failed",
            source=source,
            payload={"reason": reason, "error_message": error_message, "input": input},
        )


class HouseholdUpdateFailed(SystemEvent):
    def __init__(self, *, household_id: str, reason: str, error_message: str, input: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="household_update_failed",
            source=source,
            payload={"reason": reason, "error_message": error_message, "input": input},
        )


class UserCreated(SystemEvent):
    def __init__(self, *, household_id: str, user_id: str, email: str | None, role: str, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="user_created",
            source=source,
            payload={"user_id": user_id, "email": email, "role": role},
        )


class UserUpdated(SystemEvent):
    def __init__(self, *, household_id: str, user_id: str, changes: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="user_updated",
            source=source,
            payload={"user_id": user_id, "changes": changes},
        )


class UserCreationFailed(SystemEvent):
    def __init__(self, *, household_id: str, reason: str, error_message: str, input: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="user_creation_failed",
            source=source,
            payload={"reason": reason, "error_message": error_message, "input": input},
        )


class UserUpdateFailed(SystemEvent):
    def __init__(self, *, household_id: str, reason: str, error_message: str, input: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="user_update_failed",
            source=source,
            payload={"reason": reason, "error_message": error_message, "input": input},
        )


class DeviceCreated(SystemEvent):
    def __init__(self, *, household_id: str, device_id: str, user_id: str, device_name: str, platform: str, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="device_created",
            source=source,
            payload={"device_id": device_id, "user_id": user_id, "device_name": device_name, "platform": platform},
        )


class DeviceUpdated(SystemEvent):
    def __init__(self, *, household_id: str, device_id: str, changes: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="device_updated",
            source=source,
            payload={"device_id": device_id, "changes": changes},
        )


class DeviceCreationFailed(SystemEvent):
    def __init__(self, *, household_id: str, reason: str, error_message: str, input: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="device_creation_failed",
            source=source,
            payload={"reason": reason, "error_message": error_message, "input": input},
        )


class DeviceUpdateFailed(SystemEvent):
    def __init__(self, *, household_id: str, reason: str, error_message: str, input: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="device_update_failed",
            source=source,
            payload={"reason": reason, "error_message": error_message, "input": input},
        )


class MembershipCreated(SystemEvent):
    def __init__(self, *, household_id: str, membership_id: str, user_id: str, role: str, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="membership_created",
            source=source,
            payload={"membership_id": membership_id, "user_id": user_id, "role": role},
        )


class MembershipUpdated(SystemEvent):
    def __init__(self, *, household_id: str, membership_id: str, changes: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="membership_updated",
            source=source,
            payload={"membership_id": membership_id, "changes": changes},
        )


class MembershipAccepted(SystemEvent):
    def __init__(self, *, household_id: str, membership_id: str, user_id: str, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="membership_accepted",
            source=source,
            payload={"membership_id": membership_id, "user_id": user_id},
        )


class MembershipCreationFailed(SystemEvent):
    def __init__(self, *, household_id: str, reason: str, error_message: str, input: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="membership_creation_failed",
            source=source,
            payload={"reason": reason, "error_message": error_message, "input": input},
        )


class MembershipUpdateFailed(SystemEvent):
    def __init__(self, *, household_id: str, reason: str, error_message: str, input: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="membership_update_failed",
            source=source,
            payload={"reason": reason, "error_message": error_message, "input": input},
        )


class MembershipAcceptFailed(SystemEvent):
    def __init__(self, *, household_id: str, reason: str, error_message: str, input: dict, source: str = "identity_repository"):
        super().__init__(
            household_id=household_id,
            type="membership_accept_failed",
            source=source,
            payload={"reason": reason, "error_message": error_message, "input": input},
        )


SystemEvent.HouseholdCreated = HouseholdCreated
SystemEvent.HouseholdUpdated = HouseholdUpdated
SystemEvent.HouseholdCreationFailed = HouseholdCreationFailed
SystemEvent.HouseholdUpdateFailed = HouseholdUpdateFailed
SystemEvent.UserCreated = UserCreated
SystemEvent.UserUpdated = UserUpdated
SystemEvent.UserCreationFailed = UserCreationFailed
SystemEvent.UserUpdateFailed = UserUpdateFailed
SystemEvent.DeviceCreated = DeviceCreated
SystemEvent.DeviceUpdated = DeviceUpdated
SystemEvent.DeviceCreationFailed = DeviceCreationFailed
SystemEvent.DeviceUpdateFailed = DeviceUpdateFailed
SystemEvent.MembershipCreated = MembershipCreated
SystemEvent.MembershipUpdated = MembershipUpdated
SystemEvent.MembershipAccepted = MembershipAccepted
SystemEvent.MembershipCreationFailed = MembershipCreationFailed
SystemEvent.MembershipUpdateFailed = MembershipUpdateFailed
SystemEvent.MembershipAcceptFailed = MembershipAcceptFailed
