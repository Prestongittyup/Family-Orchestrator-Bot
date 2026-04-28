import { HouseholdRole, inferPlatform, permissionsForRole, type IdentityContext } from "./identity";
import { fetchWithApiFallback } from "../api/network";

const STORAGE_KEYS = {
  householdId: "hpal-household-id",
  householdName: "hpal-household-name",
  googleHouseholdId: "hpal-google-household-id",
  onboardingState: "hpal.onboarding.v1",
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

interface OAuthStubErrorResponse {
  detail?: string;
}

interface SessionTokenClaims {
  household_id?: unknown;
  user_id?: unknown;
  device_id?: unknown;
  user_role?: unknown;
}

export class ServerAuthProvider implements AuthProvider {
  async ensureAuthenticated(): Promise<IdentityContext> {
    const token = localStorage.getItem(STORAGE_KEYS.token);
    if (token) {
      this.seedIdentityFromToken(token);
      const cachedContext = this.buildFromStorage(token);
      let validationRequestFailed = false;

      try {
        const validated = await this.validateToken(token);
        if (validated.valid) {
          const nextToken = validated.refreshedToken || token;
          if (validated.refreshedToken) {
            localStorage.setItem(STORAGE_KEYS.token, validated.refreshedToken);
          }
          this.seedIdentityFromToken(nextToken);
          const context = this.buildFromStorage(nextToken, validated.role);
          if (context) {
            return context;
          }
        } else {
          localStorage.removeItem(STORAGE_KEYS.token);
        }
      } catch (error) {
        console.warn("Session validation request failed; using cached identity context", error);
        validationRequestFailed = true;
      }

      if (validationRequestFailed && cachedContext) {
        return cachedContext;
      }
    }

    return await this.oauthStubSignIn();
  }

  async validateToken(token: string): Promise<{ valid: boolean; refreshedToken?: string; role?: HouseholdRole }> {
    const response = await fetchWithApiFallback("/v1/identity/session/validate", {
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
    const persistedToken = localStorage.getItem(STORAGE_KEYS.token);
    this.seedIdentityFromToken(persistedToken);

    const tokenClaims = parseSessionClaimsFromToken(persistedToken);
    const householdId =
      readNonEmptyStorage(STORAGE_KEYS.householdId)
      || readNonEmptyStorage(STORAGE_KEYS.googleHouseholdId)
      || readHouseholdIdFromOnboardingState()
      || normalizeString(tokenClaims?.household_id)
      || "family-1";

    if (!readNonEmptyStorage(STORAGE_KEYS.householdId)) {
      localStorage.setItem(STORAGE_KEYS.householdId, householdId);
    }

    const householdScopedFallbackEmail = buildHouseholdScopedFallbackEmail(householdId);
    const email = localStorage.getItem(STORAGE_KEYS.email) || householdScopedFallbackEmail;
    const displayName = localStorage.getItem(STORAGE_KEYS.name) || "Beta User";
    const ua = typeof navigator !== "undefined" ? navigator.userAgent : "unknown-agent";
    const platform = inferPlatform(ua);
    const role = localStorage.getItem(STORAGE_KEYS.role) || "ADULT";

    const requestBody = (requestEmail: string) => ({
      household_id: householdId,
      email: requestEmail,
      display_name: displayName,
      role,
      device_name: `${platform}-device`,
      platform: mapPlatformForBackend(platform),
      user_agent: ua,
    });

    let response = await fetchWithApiFallback("/v1/auth/oauth/google/stub", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(requestBody(email)),
    });

    if (!response.ok) {
      const initialError = await parseAuthErrorDetail(response);
      const shouldRetryWithScopedEmail =
        response.status === 400
        && initialError.includes("email already belongs to a different household")
        && email !== householdScopedFallbackEmail;

      if (shouldRetryWithScopedEmail) {
        response = await fetchWithApiFallback("/v1/auth/oauth/google/stub", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(requestBody(householdScopedFallbackEmail)),
        });
        if (response.ok) {
          localStorage.setItem(STORAGE_KEYS.email, householdScopedFallbackEmail);
        } else {
          const retryError = await parseAuthErrorDetail(response);
          throw new Error(`auth_failed:${response.status}:${retryError || "unknown"}`);
        }
      } else {
        throw new Error(`auth_failed:${response.status}:${initialError || "unknown"}`);
      }
    }

    const data = (await response.json()) as OAuthStubResponse;

    localStorage.setItem(STORAGE_KEYS.householdId, data.identity_context.household_id);
    localStorage.setItem(STORAGE_KEYS.householdName, data.household.name || data.identity_context.household_id);
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
    const claims = parseSessionClaimsFromToken(token);
    const householdId =
      readNonEmptyStorage(STORAGE_KEYS.householdId)
      || normalizeString(claims?.household_id)
      || null;
    const userId =
      readNonEmptyStorage(STORAGE_KEYS.userId)
      || normalizeString(claims?.user_id)
      || null;
    const deviceId =
      readNonEmptyStorage(STORAGE_KEYS.deviceId)
      || normalizeString(claims?.device_id)
      || null;
    const role =
      roleOverride
      || toHouseholdRole(readNonEmptyStorage(STORAGE_KEYS.role))
      || toHouseholdRole(claims?.user_role)
      || HouseholdRole.ADULT;
    const householdName =
      readNonEmptyStorage(STORAGE_KEYS.householdName)
      || readHouseholdNameFromOnboardingState()
      || (householdId ? `Household ${householdId}` : null);

    if (!householdId || !userId || !deviceId) {
      return null;
    }

    const platform = inferPlatform(typeof navigator !== "undefined" ? navigator.userAgent : "");
    return {
      household: {
        household_id: householdId,
        name: householdName || `Household ${householdId}`,
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

  private seedIdentityFromToken(token: string | null): void {
    const claims = parseSessionClaimsFromToken(token);
    if (!claims) {
      return;
    }

    const householdId = normalizeString(claims.household_id);
    const userId = normalizeString(claims.user_id);
    const deviceId = normalizeString(claims.device_id);
    const role = toHouseholdRole(claims.user_role);

    if (householdId && !readNonEmptyStorage(STORAGE_KEYS.householdId)) {
      localStorage.setItem(STORAGE_KEYS.householdId, householdId);
    }
    if (userId && !readNonEmptyStorage(STORAGE_KEYS.userId)) {
      localStorage.setItem(STORAGE_KEYS.userId, userId);
    }
    if (deviceId && !readNonEmptyStorage(STORAGE_KEYS.deviceId)) {
      localStorage.setItem(STORAGE_KEYS.deviceId, deviceId);
    }
    if (role && !readNonEmptyStorage(STORAGE_KEYS.role)) {
      localStorage.setItem(STORAGE_KEYS.role, role);
    }
  }
}

export const authProvider = new ServerAuthProvider();

function mapPlatformForBackend(platform: "web" | "ios" | "android"): "Web" | "iOS" | "Android" {
  if (platform === "ios") return "iOS";
  if (platform === "android") return "Android";
  return "Web";
}

function sanitizeHouseholdForAlias(householdId: string): string {
  const cleaned = householdId.trim().toLowerCase().replace(/[^a-z0-9_-]/g, "-");
  return cleaned || "default";
}

function buildHouseholdScopedFallbackEmail(householdId: string): string {
  return `beta+${sanitizeHouseholdForAlias(householdId)}@hpal.local`;
}

function decodeBase64Url(input: string): string | null {
  try {
    const normalized = input.replace(/-/g, "+").replace(/_/g, "/");
    const padding = normalized.length % 4 === 0 ? "" : "=".repeat(4 - (normalized.length % 4));
    return atob(`${normalized}${padding}`);
  } catch {
    return null;
  }
}

function decodeBase64(input: string): string | null {
  try {
    return atob(input);
  } catch {
    return null;
  }
}

function normalizeString(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.trim();
  return normalized ? normalized : null;
}

function toHouseholdRole(value: unknown): HouseholdRole | null {
  if (typeof value !== "string") {
    return null;
  }

  if (value === HouseholdRole.ADMIN || value === HouseholdRole.ADULT || value === HouseholdRole.CHILD || value === HouseholdRole.VIEW_ONLY) {
    return value;
  }

  return null;
}

function readNonEmptyStorage(key: string): string | null {
  return normalizeString(localStorage.getItem(key));
}

function readHouseholdIdFromOnboardingState(): string | null {
  const raw = localStorage.getItem(STORAGE_KEYS.onboardingState);
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as { householdId?: unknown };
    return normalizeString(parsed.householdId);
  } catch {
    return null;
  }
}

function readHouseholdNameFromOnboardingState(): string | null {
  const raw = localStorage.getItem(STORAGE_KEYS.onboardingState);
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as { householdName?: unknown };
    return normalizeString(parsed.householdName);
  } catch {
    return null;
  }
}

function parseSessionClaimsFromToken(token: string | null): SessionTokenClaims | null {
  if (!token) {
    return null;
  }

  try {
    if (token.startsWith("mock.")) {
      const decoded = decodeURIComponent(token.slice(5));
      return JSON.parse(decoded) as SessionTokenClaims;
    }
  } catch {
    // Fall through to other token formats.
  }

  const jwtParts = token.split(".");
  if (jwtParts.length === 3) {
    const payloadJson = decodeBase64Url(jwtParts[1]);
    if (payloadJson) {
      try {
        return JSON.parse(payloadJson) as SessionTokenClaims;
      } catch {
        // Fall through to raw base64 token parse.
      }
    }
  }

  const rawJson = decodeBase64(token) || decodeBase64Url(token);
  if (!rawJson) {
    return null;
  }

  try {
    return JSON.parse(rawJson) as SessionTokenClaims;
  } catch {
    return null;
  }
}

function extractHouseholdIdFromToken(token: string | null): string | null {
  const claims = parseSessionClaimsFromToken(token);
  return normalizeString(claims?.household_id);
}

async function parseAuthErrorDetail(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as OAuthStubErrorResponse;
    return typeof body.detail === "string" ? body.detail : "";
  } catch {
    return "";
  }
}
