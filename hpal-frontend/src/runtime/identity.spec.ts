import {
  HouseholdRole,
  buildDeviceId,
  permissionsForRole,
  resolveIdentity,
  type IdentityContext,
} from "./identity";
import { createSessionToken, parseSessionToken } from "./sessionAuth";

function assert(condition: boolean, message: string): void {
  if (!condition) {
    throw new Error(`assertion_failed:${message}`);
  }
}

function assertEqual<T>(actual: T, expected: T, message: string): void {
  if (actual !== expected) {
    throw new Error(`assertion_failed:${message}; actual=${String(actual)} expected=${String(expected)}`);
  }
}

function testMultiUserIsolation(): void {
  const contextA = createContext({
    householdId: "family-1",
    userId: "user-a",
    role: HouseholdRole.ADULT,
    userAgent: "agent-a",
  });
  const contextB = createContext({
    householdId: "family-1",
    userId: "user-b",
    role: HouseholdRole.ADULT,
    userAgent: "agent-b",
  });

  const identityA = resolveIdentity(contextA);
  const identityB = resolveIdentity(contextB);

  assertEqual(identityA.household_id, identityB.household_id, "same household must remain shared");
  assert(identityA.user_id !== identityB.user_id, "user isolation must hold");
  assert(identityA.device_id !== identityB.device_id, "device isolation must hold");
}

function testPermissionEnforcementMatrix(): void {
  const admin = permissionsForRole(HouseholdRole.ADMIN);
  const adult = permissionsForRole(HouseholdRole.ADULT);
  const child = permissionsForRole(HouseholdRole.CHILD);
  const viewOnly = permissionsForRole(HouseholdRole.VIEW_ONLY);

  assert(admin.can_chat && admin.can_execute_actions && admin.can_override_conflicts, "admin should have full controls");
  assert(adult.can_chat && adult.can_execute_actions && !adult.can_override_conflicts, "adult should execute but not override");
  assert(child.can_chat && !child.can_execute_actions, "child should be chat-limited");
  assert(!viewOnly.can_chat && !viewOnly.can_execute_actions, "view only should be restricted");
}

function testDeviceConsistencyDeterminism(): void {
  const first = buildDeviceId({ userId: "user-a", userAgent: "same-agent", platform: "web" });
  const second = buildDeviceId({ userId: "user-a", userAgent: "same-agent", platform: "web" });
  const third = buildDeviceId({ userId: "user-a", userAgent: "different-agent", platform: "web" });

  assertEqual(first, second, "same deterministic input must produce same device id");
  assert(first !== third, "different agent must produce different device id");
}

function testSessionHydrationDeterminism(): void {
  const token = createSessionToken({
    household_id: "family-9",
    user_id: "user-z",
    role: HouseholdRole.CHILD,
  });

  const first = parseSessionToken(token);
  const second = parseSessionToken(token);

  assertEqual(first.household_id, second.household_id, "household must hydrate deterministically");
  assertEqual(first.user_id, second.user_id, "user must hydrate deterministically");
  assertEqual(first.role, second.role, "role must hydrate deterministically");
  assertEqual(first.issued_at_epoch_ms, 0, "mock token must be deterministic with fixed epoch");
}

export function runIdentityContractTests(): void {
  testMultiUserIsolation();
  testPermissionEnforcementMatrix();
  testDeviceConsistencyDeterminism();
  testSessionHydrationDeterminism();
}

function createContext(input: {
  householdId: string;
  userId: string;
  role: HouseholdRole;
  userAgent: string;
}): IdentityContext {
  return {
    household: {
      household_id: input.householdId,
      name: input.householdId,
      timezone: "UTC",
    },
    user: {
      user_id: input.userId,
      display_name: input.userId,
    },
    device: {
      device_id: buildDeviceId({
        userId: input.userId,
        userAgent: input.userAgent,
        platform: "web",
      }),
      platform: "web",
      label: `web-${input.userId}`,
    },
    membership: {
      household_id: input.householdId,
      user_id: input.userId,
      role: input.role,
      is_active: true,
    },
    permission_flags: permissionsForRole(input.role),
    session_token: createSessionToken({
      household_id: input.householdId,
      user_id: input.userId,
      role: input.role,
    }),
  };
}
