from __future__ import annotations

from dataclasses import replace
from uuid import UUID, uuid4

from apps.api.integration_core.identity import Household, User
from apps.api.integration_core.repository import IdentityRepository


class IdentityService:
    def __init__(self, repository: IdentityRepository) -> None:
        self._repository = repository

    def create_user(
        self,
        *,
        email: str,
        display_name: str | None,
        household_id: str,
        user_id: UUID | None = None,
    ) -> User:
        created = User(
            user_id=user_id or uuid4(),
            email=str(email),
            display_name=display_name,
            household_id=str(household_id),
        )
        return self._repository.create_user(created)

    def get_user(self, user_id: UUID) -> User | None:
        return self._repository.get_user(user_id)

    def update_user(
        self,
        *,
        user_id: UUID,
        email: str | None = None,
        display_name: str | None = None,
        household_id: str | None = None,
    ) -> User:
        existing = self._repository.get_user(user_id)
        if existing is None:
            raise KeyError(f"user not found: {user_id}")

        updated = replace(
            existing,
            email=existing.email if email is None else str(email),
            display_name=existing.display_name if display_name is None else display_name,
            household_id=existing.household_id if household_id is None else str(household_id),
        )
        return self._repository.update_user(updated)

    def delete_user(self, user_id: UUID) -> bool:
        return self._repository.delete_user(user_id)

    def create_household(
        self,
        *,
        household_id: str,
        name: str,
        member_user_ids: tuple[UUID, ...] = (),
    ) -> Household:
        household = Household(
            household_id=str(household_id),
            name=str(name),
            member_user_ids=tuple(member_user_ids),
        )
        return self._repository.create_household(household)

    def get_household(self, household_id: str) -> Household | None:
        return self._repository.get_household(str(household_id))

    def update_household(self, *, household_id: str, name: str | None = None) -> Household:
        existing = self._repository.get_household(str(household_id))
        if existing is None:
            raise KeyError(f"household not found: {household_id}")

        updated = replace(
            existing,
            name=existing.name if name is None else str(name),
        )
        return self._repository.update_household(updated)

    def delete_household(self, household_id: str) -> bool:
        return self._repository.delete_household(str(household_id))

    def assign_user_to_household(self, *, user_id: UUID, household_id: str) -> tuple[User, Household]:
        return self._repository.assign_user_to_household(user_id=user_id, household_id=str(household_id))
