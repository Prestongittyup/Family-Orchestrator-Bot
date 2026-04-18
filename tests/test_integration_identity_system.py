from __future__ import annotations

from uuid import UUID

from apps.api.integration_core import IdentityService, InMemoryIdentityRepository


def test_user_creation() -> None:
    service = IdentityService(InMemoryIdentityRepository())
    service.create_household(household_id="hh-1", name="Family Home")

    user = service.create_user(
        email="alice@example.com",
        display_name="Alice",
        household_id="hh-1",
    )

    assert isinstance(user.user_id, UUID)
    assert user.email == "alice@example.com"
    assert user.display_name == "Alice"
    assert user.household_id == "hh-1"


def test_household_membership_assignment() -> None:
    service = IdentityService(InMemoryIdentityRepository())
    service.create_household(household_id="hh-1", name="Family Home")
    user = service.create_user(
        email="bob@example.com",
        display_name="Bob",
        household_id="",
    )

    updated_user, updated_household = service.assign_user_to_household(
        user_id=user.user_id,
        household_id="hh-1",
    )

    assert updated_user.household_id == "hh-1"
    assert user.user_id in updated_household.member_user_ids


def test_lookup_by_user_id_and_household_id() -> None:
    service = IdentityService(InMemoryIdentityRepository())
    created_household = service.create_household(household_id="hh-lookup", name="Lookup Home")
    created_user = service.create_user(
        email="carol@example.com",
        display_name="Carol",
        household_id="hh-lookup",
    )

    found_user = service.get_user(created_user.user_id)
    found_household = service.get_household(created_household.household_id)

    assert found_user is not None
    assert found_household is not None
    assert found_user.user_id == created_user.user_id
    assert found_household.household_id == created_household.household_id
