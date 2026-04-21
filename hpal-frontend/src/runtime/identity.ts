export enum HouseholdRole {
  ADMIN = "ADMIN",
  ADULT = "ADULT",
  CHILD = "CHILD",
  VIEW_ONLY = "VIEW_ONLY",
}

export interface Household {
  household_id: string;
  name: string;
  timezone: string;
}

export interface UserPerson {
  user_id: string;
  display_name: string;
}

export interface Device {
  device_id: string;
  platform: "web" | "ios" | "android";
  label: string;
}

export interface Membership {
  household_id: string;
  user_id: string;
  role: HouseholdRole;
  is_active: boolean;
}

export interface PermissionFlags {
  can_chat: boolean;
  can_execute_actions: boolean;
  can_override_conflicts: boolean;
  can_view_sensitive_cards: boolean;
}

export interface IdentityContext {
  household: Household;
  user: UserPerson;
  device: Device;
  membership: Membership;
  permission_flags: PermissionFlags;
  session_token: string;
}

export interface DeterministicIdentity {
  household_id: string;
  user_id: string;
  device_id: string;
}

export interface SessionClaims {
  token: string;
  household_id: string;
  user_id: string;
  role: HouseholdRole;
  issued_at_epoch_ms: number;
}

export function permissionsForRole(role: HouseholdRole): PermissionFlags {
  if (role === HouseholdRole.ADMIN) {
    return {
      can_chat: true,
      can_execute_actions: true,
      can_override_conflicts: true,
      can_view_sensitive_cards: true,
    };
  }

  if (role === HouseholdRole.ADULT) {
    return {
      can_chat: true,
      can_execute_actions: true,
      can_override_conflicts: false,
      can_view_sensitive_cards: true,
    };
  }

  if (role === HouseholdRole.CHILD) {
    return {
      can_chat: true,
      can_execute_actions: false,
      can_override_conflicts: false,
      can_view_sensitive_cards: false,
    };
  }

  return {
    can_chat: false,
    can_execute_actions: false,
    can_override_conflicts: false,
    can_view_sensitive_cards: false,
  };
}

export function buildDeviceId(input: { userId: string; userAgent: string | undefined; platform: string }): string {
  const canonical = `${input.userId}|${input.platform}|${input.userAgent ?? "unknown-agent"}`;
  return `dev-${simpleHash(canonical)}`;
}

export function resolveIdentity(context: IdentityContext): DeterministicIdentity {
  return {
    household_id: context.household.household_id,
    user_id: context.user.user_id,
    device_id: context.device.device_id,
  };
}

export function inferPlatform(userAgent: string | undefined): Device["platform"] {
  const ua = (userAgent ?? "").toLowerCase();
  if (ua.includes("iphone") || ua.includes("ipad") || ua.includes("ios")) {
    return "ios";
  }
  if (ua.includes("android")) {
    return "android";
  }
  return "web";
}

function simpleHash(value: string): string {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash).toString(16);
}
