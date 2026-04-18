from __future__ import annotations

from dataclasses import replace
from threading import RLock
from typing import Protocol
from uuid import UUID

from apps.api.integration_core.identity import Household, User


class IdentityRepository(Protocol):
    def create_user(self, user: User) -> User:
        ...

    def get_user(self, user_id: UUID) -> User | None:
        ...

    def update_user(self, user: User) -> User:
        ...

    def delete_user(self, user_id: UUID) -> bool:
        ...

    def create_household(self, household: Household) -> Household:
        ...

    def get_household(self, household_id: str) -> Household | None:
        ...

    def update_household(self, household: Household) -> Household:
        ...

    def delete_household(self, household_id: str) -> bool:
        ...

    def assign_user_to_household(self, user_id: UUID, household_id: str) -> tuple[User, Household]:
        ...


class InMemoryIdentityRepository:
    def __init__(self) -> None:
        self._users: dict[UUID, User] = {}
        self._households: dict[str, Household] = {}
        self._lock = RLock()

    def create_user(self, user: User) -> User:
        with self._lock:
            if user.user_id in self._users:
                raise ValueError(f"user already exists: {user.user_id}")
            self._users[user.user_id] = user
            return user

    def get_user(self, user_id: UUID) -> User | None:
        with self._lock:
            return self._users.get(user_id)

    def update_user(self, user: User) -> User:
        with self._lock:
            if user.user_id not in self._users:
                raise KeyError(f"user not found: {user.user_id}")
            self._users[user.user_id] = user
            return user

    def delete_user(self, user_id: UUID) -> bool:
        with self._lock:
            user = self._users.pop(user_id, None)
            if user is None:
                return False

            household = self._households.get(user.household_id)
            if household is not None:
                members = tuple(uid for uid in household.member_user_ids if uid != user_id)
                self._households[user.household_id] = replace(household, member_user_ids=members)
            return True

    def create_household(self, household: Household) -> Household:
        with self._lock:
            if household.household_id in self._households:
                raise ValueError(f"household already exists: {household.household_id}")
            self._households[household.household_id] = household
            return household

    def get_household(self, household_id: str) -> Household | None:
        with self._lock:
            return self._households.get(household_id)

    def update_household(self, household: Household) -> Household:
        with self._lock:
            if household.household_id not in self._households:
                raise KeyError(f"household not found: {household.household_id}")
            self._households[household.household_id] = household
            return household

    def delete_household(self, household_id: str) -> bool:
        with self._lock:
            household = self._households.pop(household_id, None)
            if household is None:
                return False

            for user_id in household.member_user_ids:
                user = self._users.get(user_id)
                if user is not None:
                    self._users[user_id] = replace(user, household_id="")
            return True

    def assign_user_to_household(self, user_id: UUID, household_id: str) -> tuple[User, Household]:
        with self._lock:
            user = self._users.get(user_id)
            if user is None:
                raise KeyError(f"user not found: {user_id}")

            household = self._households.get(household_id)
            if household is None:
                raise KeyError(f"household not found: {household_id}")

            # Remove membership from prior household if needed.
            if user.household_id and user.household_id != household_id:
                old_household = self._households.get(user.household_id)
                if old_household is not None:
                    old_members = tuple(uid for uid in old_household.member_user_ids if uid != user_id)
                    self._households[user.household_id] = replace(old_household, member_user_ids=old_members)

            updated_user = replace(user, household_id=household_id)
            self._users[user_id] = updated_user

            members = set(household.member_user_ids)
            members.add(user_id)
            ordered_members = tuple(sorted(members, key=lambda uid: str(uid)))
            updated_household = replace(household, member_user_ids=ordered_members)
            self._households[household_id] = updated_household

            return updated_user, updated_household

    def clear(self) -> None:
        """Reset all in-memory users and households (test/reset helper)."""
        with self._lock:
            self._users.clear()
            self._households.clear()
