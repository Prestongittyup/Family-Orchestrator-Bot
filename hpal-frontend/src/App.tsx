import React, { useEffect, useState } from "react";
import { BrowserRouter as Router, Routes, Route, Navigate } from "react-router-dom";
import { AppShell } from "./ui/components/AppShell";
import { AnalyticsScreen } from "./ui/screens/AnalyticsScreen";
import { CalendarScreen } from "./ui/screens/CalendarScreen";
import { ChatScreen } from "./ui/screens/ChatScreen";
import { DashboardScreen } from "./ui/screens/DashboardScreen";
import { InboxScreen } from "./ui/screens/InboxScreen";
import { PantryScreen } from "./ui/screens/PantryScreen";
import { TasksScreen } from "./ui/screens/TasksScreen";
import { useRuntimeStore } from "./runtime/store";
import { OnboardingContainer } from "./screens/onboarding/OnboardingContainer";

type OAuthCallbackContext = {
  status: string | null;
  familyId: string | null;
  userId: string | null;
};

const GOOGLE_CONNECTED_USER_STORAGE_KEY = "hpal-google-user-id";
const GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY = "hpal-google-household-id";
const ONBOARDING_STORAGE_KEY = "hpal.onboarding.v1";

type StoredSessionClaims = {
  household_id?: unknown;
};

const INVALID_HOUSEHOLD_IDS = new Set(["null", "undefined", "none"]);

const normalizeHouseholdId = (value: unknown): string | null => {
  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.trim();
  if (!normalized) {
    return null;
  }

  if (INVALID_HOUSEHOLD_IDS.has(normalized.toLowerCase())) {
    return null;
  }

  return normalized;
};

const decodeBase64Url = (input: string): string | null => {
  try {
    const normalized = input.replace(/-/g, "+").replace(/_/g, "/");
    const padding = normalized.length % 4 === 0 ? "" : "=".repeat(4 - (normalized.length % 4));
    return atob(`${normalized}${padding}`);
  } catch {
    return null;
  }
};

const decodeBase64 = (input: string): string | null => {
  try {
    return atob(input);
  } catch {
    return null;
  }
};

const parseStoredSessionClaims = (token: string | null): StoredSessionClaims | null => {
  if (!token) {
    return null;
  }

  try {
    if (token.startsWith("mock.")) {
      const decoded = decodeURIComponent(token.slice(5));
      return JSON.parse(decoded) as StoredSessionClaims;
    }
  } catch {
    // Fall through to other token formats.
  }

  const parts = token.split(".");
  if (parts.length === 3) {
    const payloadJson = decodeBase64Url(parts[1]);
    if (payloadJson) {
      try {
        return JSON.parse(payloadJson) as StoredSessionClaims;
      } catch {
        // Fall through to raw base64 JSON token.
      }
    }
  }

  const rawDecoded = decodeBase64(token) || decodeBase64Url(token);
  if (!rawDecoded) {
    return null;
  }

  try {
    return JSON.parse(rawDecoded) as StoredSessionClaims;
  } catch {
    return null;
  }
};

const resolveStoredSessionHouseholdId = (token: string | null): string | null => {
  const claims = parseStoredSessionClaims(token);
  return normalizeHouseholdId(claims?.household_id);
};

function resolveOAuthCallbackContext(): OAuthCallbackContext {
  const params = new URLSearchParams(window.location.search);
  return {
    status: params.get("status"),
    familyId: normalizeHouseholdId(params.get("familyId")),
    userId: params.get("user_id"),
  };
}

function hydrateOAuthCallbackIdentity(context: OAuthCallbackContext): void {
  if (context.status !== "integration_successful") {
    return;
  }

  try {
    const callbackHouseholdId = normalizeHouseholdId(context.familyId);
    if (callbackHouseholdId) {
      localStorage.setItem("hpal-household-id", callbackHouseholdId);
      localStorage.setItem(GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY, callbackHouseholdId);
    }
    if (context.userId) {
      localStorage.setItem(GOOGLE_CONNECTED_USER_STORAGE_KEY, context.userId);
    }
  } catch {
    // Ignore storage failures and continue with in-memory state.
  }
}

/** Retrieve stored household id without requiring URL params. */
function resolveStoredHouseholdId(): string | null {
  try {
    return normalizeHouseholdId(localStorage.getItem("hpal-household-id"));
  } catch {
    return null;
  }
}

function resolveStoredGoogleHouseholdId(): string | null {
  try {
    return normalizeHouseholdId(localStorage.getItem(GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY));
  } catch {
    return null;
  }
}

function resolveStoredOnboardingHouseholdId(): string | null {
  try {
    const raw = localStorage.getItem(ONBOARDING_STORAGE_KEY);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw) as { householdId?: unknown };
    return normalizeHouseholdId(parsed.householdId);
  } catch {
    return null;
  }
}

function resolveStoredSessionToken(): string | null {
  try {
    return localStorage.getItem("hpal.session.token");
  } catch {
    return null;
  }
}

function resolveRecoverableHouseholdId(sessionToken: string | null): string | null {
  const storedHouseholdId = resolveStoredHouseholdId();
  if (storedHouseholdId) {
    return storedHouseholdId;
  }

  const recovered =
    resolveStoredGoogleHouseholdId()
    || resolveStoredOnboardingHouseholdId()
    || resolveStoredSessionHouseholdId(sessionToken);

  if (!recovered) {
    return null;
  }

  try {
    localStorage.setItem("hpal-household-id", recovered);
  } catch {
    // Ignore storage failures and continue.
  }

  return recovered;
}

const App: React.FC = () => {
  const oauthCallbackContext = resolveOAuthCallbackContext();
  hydrateOAuthCallbackIdentity(oauthCallbackContext);

  // Prioritise URL param (dev override) then localStorage then null (needs onboarding)
  const urlFamilyId = normalizeHouseholdId(oauthCallbackContext.familyId);
  const storedSessionToken = resolveStoredSessionToken();
  const recoveredFamilyId = resolveRecoverableHouseholdId(storedSessionToken);
  const initialFamilyId = urlFamilyId || recoveredFamilyId || null;

  const initialize = useRuntimeStore((state) => state.initialize);
  const stopSyncLoop = useRuntimeStore((state) => state.stopSyncLoop);

  // Track whether onboarding has been completed:
  // true  → show main app
  // false → show onboarding screens
  const [onboardingDone, setOnboardingDone] = useState<boolean>(
    () => initialFamilyId !== null
  );

  // If familyId exists, init is safe; otherwise wait for onboarding to complete.
  const [familyId, setFamilyId] = useState<string>(initialFamilyId || "");

  useEffect(() => {
    if (onboardingDone && familyId) {
      initialize(familyId);
    }
    return () => {
      stopSyncLoop();
    };
  }, [onboardingDone, familyId, initialize, stopSyncLoop]);

  useEffect(() => {
    if (oauthCallbackContext.status !== "integration_successful") {
      return;
    }

    const params = new URLSearchParams(window.location.search);
    params.delete("status");
    params.delete("user_id");
    params.delete("familyId");
    const nextSearch = params.toString();
    const nextUrl = `${window.location.pathname}${nextSearch ? `?${nextSearch}` : ""}${window.location.hash}`;
    window.history.replaceState({}, document.title, nextUrl);
  }, [oauthCallbackContext.status]);

  const handleOnboardingComplete = () => {
    const newHouseholdId = resolveRecoverableHouseholdId(resolveStoredSessionToken());
    if (!newHouseholdId) {
      return;
    }
    setFamilyId(newHouseholdId);
    setOnboardingDone(true);
  };

  if (!onboardingDone) {
    return <OnboardingContainer onComplete={handleOnboardingComplete} />;
  }

  return (
    <Router>
      <AppShell>
        <Routes>
          <Route path="/" element={<DashboardScreen />} />
          <Route path="/tasks" element={<TasksScreen />} />
          <Route path="/calendar" element={<CalendarScreen />} />
          <Route path="/analytics" element={<AnalyticsScreen />} />
          <Route path="/inbox" element={<InboxScreen />} />
          <Route path="/pantry" element={<PantryScreen />} />
          <Route path="/chat" element={<ChatScreen />} />
          <Route path="/chat/*" element={<ChatScreen />} />
          <Route path="/assistant" element={<ChatScreen />} />
          <Route path="/assistant/*" element={<ChatScreen />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </AppShell>
    </Router>
  );
};

export default App;
