import { HouseholdRole, inferPlatform, permissionsForRole, type IdentityContext } from "./identity";

const BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "http://localhost:8000";

const STORAGE_KEYS = {
  householdId: "hpal-household-id",
  userId: "hpal-user-id",
  deviceId: "hpal-device-id",
  role: "hpal-role",
  token: "hpal.session.token",
  email: "hpal-auth-email",
  name: "hpal-auth-name",
} as const;

export interface AuthProvider {
  ensureAuthenticated: () => Promise<IdentityContext>;
  validateToken: (token: string) => Promise<{ valid: boolean; refreshedToken?: string; role?: HouseholdRole }>;
}

interface SessionValidationResponse {
  is_valid: boolean;
  identity_context?: {
    household_id: string;
    user_id: string;
    device_id: string;
    user_role: "ADMIN" | "ADULT" | "CHILD" | "VIEW_ONLY";
    can_chat: boolean;
    can_execute_actions: boolean;
    can_override_conflicts: boolean;
    can_view_sensitive_cards: boolean;
  } | null;
  refreshed_token?: string | null;
}

interface OAuthStubResponse {
  household: { household_id: string; name: string; timezone: string };
  user: { user_id: string; name: string; role: "ADMIN" | "ADULT" | "CHILD" | "VIEW_ONLY" };
  device: { device_id: string; device_name: string; platform: "iOS" | "Android" | "Web" };
  identity_context: {
    household_id: string;
    user_id: string;
    device_id: string;
    user_role: "ADMIN" | "ADULT" | "CHILD" | "VIEW_ONLY";
    can_chat: boolean;
    can_execute_actions: boolean;
    can_override_conflicts: boolean;
    can_view_sensitive_cards: boolean;
  };
  session_token: string;
}

export class ServerAuthProvider implements AuthProvider {
  async ensureAuthenticated(): Promise<IdentityContext> {
    const token = localStorage.getItem(STORAGE_KEYS.token);
    if (token) {
      const validated = await this.validateToken(token);
      if (validated.valid) {
        if (validated.refreshedToken) {
          localStorage.setItem(STORAGE_KEYS.token, validated.refreshedToken);
        }
        const context = this.buildFromStorage(validated.refreshedToken || token, validated.role);
        if (context) {
          return context;
        }
      }
    }

    // Fallback auth path: OAuth stub for closed beta.
    return this.oauthStubSignIn();
  }

  async validateToken(token: string): Promise<{ valid: boolean; refreshedToken?: string; role?: HouseholdRole }> {
    const response = await fetch(`${BASE_URL}/v1/identity/session/validate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_token: token }),
    });

    if (!response.ok) {
      return { valid: false };
    }

    const data = (await response.json()) as SessionValidationResponse;
    if (!data.is_valid) {
      return { valid: false };
    }

    const role = data.identity_context?.user_role as HouseholdRole | undefined;
    if (data.identity_context) {
      localStorage.setItem(STORAGE_KEYS.householdId, data.identity_context.household_id);
      localStorage.setItem(STORAGE_KEYS.userId, data.identity_context.user_id);
      localStorage.setItem(STORAGE_KEYS.deviceId, data.identity_context.device_id);
      if (role) {
        localStorage.setItem(STORAGE_KEYS.role, role);
      }
    }

    return {
      valid: true,
      refreshedToken: data.refreshed_token || undefined,
      role,
    };
  }

  private async oauthStubSignIn(): Promise<IdentityContext> {
    const householdId = localStorage.getItem(STORAGE_KEYS.householdId) || "family-1";
    const email = localStorage.getItem(STORAGE_KEYS.email) || "beta.user@hpal.local";
    const displayName = localStorage.getItem(STORAGE_KEYS.name) || "Beta User";
    const ua = typeof navigator !== "undefined" ? navigator.userAgent : "unknown-agent";
    const platform = inferPlatform(ua);

    const response = await fetch(`${BASE_URL}/v1/auth/oauth/google/stub`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        household_id: householdId,
        email,
        display_name: displayName,
        role: localStorage.getItem(STORAGE_KEYS.role) || "ADULT",
        device_name: `${platform}-device`,
        platform: mapPlatformForBackend(platform),
        user_agent: ua,
      }),
    });

    if (!response.ok) {
      throw new Error(`auth_failed:${response.status}`);
    }

    const data = (await response.json()) as OAuthStubResponse;

    localStorage.setItem(STORAGE_KEYS.householdId, data.identity_context.household_id);
    localStorage.setItem(STORAGE_KEYS.userId, data.identity_context.user_id);
    localStorage.setItem(STORAGE_KEYS.deviceId, data.identity_context.device_id);
    localStorage.setItem(STORAGE_KEYS.role, data.identity_context.user_role);
    localStorage.setItem(STORAGE_KEYS.token, data.session_token);

    return {
      household: {
        household_id: data.household.household_id,
        name: data.household.name,
        timezone: data.household.timezone,
      },
      user: {
        user_id: data.user.user_id,
        display_name: data.user.name,
      },
      device: {
        device_id: data.device.device_id,
        platform,
        label: data.device.device_name,
      },
      membership: {
        household_id: data.identity_context.household_id,
        user_id: data.identity_context.user_id,
        role: data.identity_context.user_role as HouseholdRole,
        is_active: true,
      },
      permission_flags: {
        can_chat: data.identity_context.can_chat,
        can_execute_actions: data.identity_context.can_execute_actions,
        can_override_conflicts: data.identity_context.can_override_conflicts,
        can_view_sensitive_cards: data.identity_context.can_view_sensitive_cards,
      },
      session_token: data.session_token,
    };
  }

  private buildFromStorage(token: string, roleOverride?: HouseholdRole): IdentityContext | null {
    const householdId = localStorage.getItem(STORAGE_KEYS.householdId);
    const userId = localStorage.getItem(STORAGE_KEYS.userId);
    const deviceId = localStorage.getItem(STORAGE_KEYS.deviceId);
    const role = roleOverride || (localStorage.getItem(STORAGE_KEYS.role) as HouseholdRole | null) || HouseholdRole.ADULT;

    if (!householdId || !userId || !deviceId) {
      return null;
    }

    const platform = inferPlatform(typeof navigator !== "undefined" ? navigator.userAgent : "");
    return {
      household: {
        household_id: householdId,
        name: `Household ${householdId}`,
        timezone: "UTC",
      },
      user: {
        user_id: userId,
        display_name: localStorage.getItem(STORAGE_KEYS.name) || userId,
      },
      device: {
        device_id: deviceId,
        platform,
        label: `${platform}-${userId}`,
      },
      membership: {
        household_id: householdId,
        user_id: userId,
        role,
        is_active: true,
      },
      permission_flags: permissionsForRole(role),
      session_token: token,
    };
  }
}

export const authProvider = new ServerAuthProvider();

function mapPlatformForBackend(platform: "web" | "ios" | "android"): "Web" | "iOS" | "Android" {
  if (platform === "ios") return "iOS";
  if (platform === "android") return "Android";
  return "Web";
}
