from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class User:
    user_id: UUID
    email: str
    display_name: str | None
    household_id: str


@dataclass(frozen=True)
class Household:
    household_id: str
    name: str
    member_user_ids: tuple[UUID, ...] = ()


@dataclass(frozen=True)
class UserIdentity:
    user_id: str
    household_id: str
    email: str
    display_name: str | None = None
    timezone: str = "UTC"
    is_active: bool = True

