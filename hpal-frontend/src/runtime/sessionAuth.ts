import {
  buildDeviceId,
  HouseholdRole,
  inferPlatform,
  permissionsForRole,
  type IdentityContext,
  type SessionClaims,
} from "./identity";

const SESSION_STORAGE_KEY = "hpal.session.token";

export class MockSessionAuth {
  hydrateSession(): IdentityContext {
    const claims = this.readOrCreateClaims();
    const userAgent = typeof navigator !== "undefined" ? navigator.userAgent : "unknown-agent";
    const platform = inferPlatform(userAgent);

    const deviceId = buildDeviceId({
      userId: claims.user_id,
      userAgent,
      platform,
    });

    return {
      household: {
        household_id: claims.household_id,
        name: `Household ${claims.household_id}`,
        timezone: "UTC",
      },
      user: {
        user_id: claims.user_id,
        display_name: claims.user_id,
      },
      device: {
        device_id: deviceId,
        platform,
        label: `${platform}-${claims.user_id}`,
      },
      membership: {
        household_id: claims.household_id,
        user_id: claims.user_id,
        role: claims.role,
        is_active: true,
      },
      permission_flags: permissionsForRole(claims.role),
      session_token: claims.token,
    };
  }

  private readOrCreateClaims(): SessionClaims {
    const existing = this.readToken();
    if (existing) {
      return parseSessionToken(existing);
    }

    const defaultClaims: SessionClaims = {
      token: createSessionToken({
        household_id: "family-1",
        user_id: "user-admin",
        role: HouseholdRole.ADMIN,
      }),
      household_id: "family-1",
      user_id: "user-admin",
      role: HouseholdRole.ADMIN,
      issued_at_epoch_ms: 0,
    };

    this.writeToken(defaultClaims.token);
    return parseSessionToken(defaultClaims.token);
  }

  private readToken(): string | null {
    if (typeof window === "undefined") {
      return null;
    }
    try {
      return window.localStorage.getItem(SESSION_STORAGE_KEY);
    } catch {
      return null;
    }
  }

  private writeToken(token: string): void {
    if (typeof window === "undefined") {
      return;
    }
    try {
      window.localStorage.setItem(SESSION_STORAGE_KEY, token);
    } catch {
      // Ignore storage errors; deterministic fallback still works in-memory.
    }
  }
}

export function createSessionToken(input: {
  household_id: string;
  user_id: string;
  role: HouseholdRole;
}): string {
  const payload = {
    household_id: input.household_id,
    user_id: input.user_id,
    role: input.role,
    issued_at_epoch_ms: 0,
  };
  return encodePayload(payload);
}

export function parseSessionToken(token: string): SessionClaims {
  const parsed = decodePayload(token) as Partial<SessionClaims>;

  const role = Object.values(HouseholdRole).includes(parsed.role as HouseholdRole)
    ? (parsed.role as HouseholdRole)
    : HouseholdRole.VIEW_ONLY;

  return {
    token,
    household_id: String(parsed.household_id ?? "family-1"),
    user_id: String(parsed.user_id ?? "user-view"),
    role,
    issued_at_epoch_ms: Number(parsed.issued_at_epoch_ms ?? 0),
  };
}

function encodePayload(value: Record<string, unknown>): string {
  const json = JSON.stringify(value);
  return `mock.${encodeURIComponent(json)}`;
}

function decodePayload(token: string): Record<string, unknown> {
  const payload = token.startsWith("mock.") ? token.slice(5) : token;
  const json = decodeURIComponent(payload);
  return JSON.parse(json) as Record<string, unknown>;
}
