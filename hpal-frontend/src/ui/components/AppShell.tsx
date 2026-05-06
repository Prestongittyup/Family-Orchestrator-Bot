import React, { useEffect, useMemo, useRef, useState } from "react";
import { NavLink } from "react-router-dom";
import { useRuntimeStore } from "../../runtime/store";
import { buildApiUrl, fetchWithApiFallback } from "../../api/network";

type GoogleCalendarConnectionStatus = "checking" | "connected" | "not_connected" | "error";

type GoogleCalendarStatusResponse = {
  user_id: string;
  provider_name: string;
  connected: boolean;
  expires_at: string | null;
  scopes: string[];
};

const EMAIL_SYNC_STATUS_STORAGE_KEY = "hpal-email-sync-status";
const GOOGLE_CONNECTED_USER_STORAGE_KEY = "hpal-google-user-id";
const GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY = "hpal-google-household-id";
const HOUSEHOLD_NAME_STORAGE_KEY = "hpal-household-name";
const INBOX_SYNC_MAX_RESULTS = 100;
const SHADOW_INTEGRATION_ENDPOINTS_ENABLED = false;

type InboxSyncStatusPayload = {
  status: "syncing" | "success" | "failed";
  attempted_at: string;
  last_success_at: string | null;
  max_results: number;
  processed_count: number;
  failed_count: number;
  message: string;
};

const persistInboxSyncStatus = (payload: InboxSyncStatusPayload): void => {
  const existing = (() => {
    try {
      const raw = localStorage.getItem(EMAIL_SYNC_STATUS_STORAGE_KEY);
      if (!raw) {
        return null;
      }
      return JSON.parse(raw) as Partial<InboxSyncStatusPayload>;
    } catch {
      return null;
    }
  })();

  const normalized: InboxSyncStatusPayload = {
    ...payload,
    last_success_at: payload.last_success_at ?? (existing?.last_success_at ? String(existing.last_success_at) : null),
  };

  try {
    localStorage.setItem(EMAIL_SYNC_STATUS_STORAGE_KEY, JSON.stringify(normalized));
  } catch {
    // best-effort persistence only
  }

  try {
    window.dispatchEvent(new CustomEvent<InboxSyncStatusPayload>("hpal:email-sync-status", { detail: normalized }));
  } catch {
    // ignore dispatch failures in restrictive environments
  }
};

const resolveStoredGoogleUserId = (): string => {
  try {
    return localStorage.getItem(GOOGLE_CONNECTED_USER_STORAGE_KEY) || "";
  } catch {
    return "";
  }
};

const resolveStoredGoogleHouseholdId = (): string => {
  try {
    return localStorage.getItem(GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY) || "";
  } catch {
    return "";
  }
};

const resolveStoredHouseholdId = (): string => {
  try {
    return localStorage.getItem("hpal-household-id") || "";
  } catch {
    return "";
  }
};

const resolveStoredHouseholdName = (): string => {
  try {
    return localStorage.getItem(HOUSEHOLD_NAME_STORAGE_KEY) || "";
  } catch {
    return "";
  }
};

const readOnboardingHouseholdName = (): string => {
  try {
    const raw = localStorage.getItem("hpal.onboarding.v1");
    if (!raw) {
      return "";
    }
    const parsed = JSON.parse(raw) as { householdName?: unknown };
    return typeof parsed.householdName === "string" ? parsed.householdName : "";
  } catch {
    return "";
  }
};

const isUuidLike = (value: string): boolean =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);

const isSyntheticHouseholdLabel = (label: string, householdId: string): boolean => {
  const normalizedLabel = label.trim().toLowerCase();
  const normalizedHouseholdId = householdId.trim().toLowerCase();
  if (!normalizedLabel) {
    return true;
  }
  if (isUuidLike(normalizedLabel)) {
    return true;
  }
  if (normalizedHouseholdId && normalizedLabel === normalizedHouseholdId) {
    return true;
  }
  if (normalizedHouseholdId && normalizedLabel === `household ${normalizedHouseholdId}`) {
    return true;
  }
  return false;
};

interface AppShellProps {
  children: React.ReactNode;
}

export const AppShell: React.FC<AppShellProps> = ({ children }) => {
  const activeUser = useRuntimeStore((state) => state.active_user);
  const activeHousehold = useRuntimeStore((state) => state.active_household);
  const forceReconcile = useRuntimeStore((state) => state.forceReconcile);
  const [googleStatus, setGoogleStatus] = useState<GoogleCalendarConnectionStatus>("checking");
  const [postConnectSyncLoading, setPostConnectSyncLoading] = useState(false);
  const [postConnectSyncMessage, setPostConnectSyncMessage] = useState<string | null>(null);
  const [postConnectSyncError, setPostConnectSyncError] = useState<string | null>(null);
  const hasTriggeredPostConnectSyncRef = useRef(false);
  const hasTriggeredConnectedReconcileRef = useRef(false);
  const callbackContext = useMemo(() => {
    const params = new URLSearchParams(window.location.search);
    return {
      status: params.get("status"),
      userId: params.get("user_id") || "",
      familyId: params.get("familyId") || "",
    };
  }, []);
  const callbackStatus = callbackContext.status;
  const callbackUserId = callbackContext.userId;
  const callbackFamilyId = callbackContext.familyId;

  const activeUserId = (activeUser?.user_id || "").trim();
  const linkedGoogleUserId = resolveStoredGoogleUserId().trim();
  const linkedGoogleHouseholdId = resolveStoredGoogleHouseholdId().trim();
  const resolvedHouseholdId = activeHousehold?.household_id || resolveStoredHouseholdId();
  const storedHouseholdName = resolveStoredHouseholdName().trim();
  const onboardingHouseholdName = readOnboardingHouseholdName().trim();
  const activeHouseholdName = (activeHousehold?.name || "").trim();
  const linkedGoogleUserForHousehold =
    linkedGoogleUserId && linkedGoogleHouseholdId && resolvedHouseholdId && linkedGoogleHouseholdId !== resolvedHouseholdId
      ? ""
      : linkedGoogleUserId;
  const connectUserIdCandidate = activeUserId || linkedGoogleUserForHousehold || callbackUserId;

  const statusCandidateUserIds = useMemo(() => {
    const candidates = callbackStatus === "integration_successful"
      ? [callbackUserId, activeUserId, linkedGoogleUserForHousehold]
      : [activeUserId, linkedGoogleUserForHousehold];

    return Array.from(
      new Set(
        candidates
          .map((value) => value.trim())
          .filter((value) => value.length > 0),
      ),
    );
  }, [callbackStatus, callbackUserId, activeUserId, linkedGoogleUserForHousehold]);
  const statusCandidateKey = statusCandidateUserIds.join("|");
  const displayHouseholdName = (() => {
    if (activeHouseholdName && !isSyntheticHouseholdLabel(activeHouseholdName, resolvedHouseholdId)) {
      return activeHouseholdName;
    }
    if (storedHouseholdName && !isSyntheticHouseholdLabel(storedHouseholdName, resolvedHouseholdId)) {
      return storedHouseholdName;
    }
    if (onboardingHouseholdName && !isSyntheticHouseholdLabel(onboardingHouseholdName, resolvedHouseholdId)) {
      return onboardingHouseholdName;
    }
    if (resolvedHouseholdId && !isUuidLike(resolvedHouseholdId)) {
      return resolvedHouseholdId;
    }
    return "Your household";
  })();

  const sleep = (ms: number): Promise<void> =>
    new Promise((resolve) => window.setTimeout(resolve, ms));

  useEffect(() => {
    if (!activeHouseholdName || isSyntheticHouseholdLabel(activeHouseholdName, resolvedHouseholdId)) {
      return;
    }

    try {
      localStorage.setItem(HOUSEHOLD_NAME_STORAGE_KEY, activeHouseholdName);
    } catch {
      // best-effort persistence only
    }
  }, [activeHouseholdName, resolvedHouseholdId]);

  useEffect(() => {
    if (!SHADOW_INTEGRATION_ENDPOINTS_ENABLED) {
      setGoogleStatus("not_connected");
      hasTriggeredConnectedReconcileRef.current = false;
      return;
    }

    let isCancelled = false;

    const fetchGoogleStatus = async () => {
      if (statusCandidateUserIds.length === 0) {
        setGoogleStatus("not_connected");
        hasTriggeredConnectedReconcileRef.current = false;
        return;
      }

      setGoogleStatus("checking");
      try {
        let hadSuccessfulLookup = false;

        for (const candidateUserId of statusCandidateUserIds) {
          let payload: GoogleCalendarStatusResponse | null = null;
          for (let attempt = 0; attempt < 3; attempt += 1) {
            // TODO: REMOVE_SHADOW_ENDPOINT
            const response = await fetchWithApiFallback(
              `/integrations/google-calendar/status/${encodeURIComponent(candidateUserId)}`,
              { method: "GET" },
            );

            if (response.status === 429 && attempt < 2) {
              await sleep(200 * (attempt + 1));
              continue;
            }

            if (!response.ok) {
              break;
            }

            payload = (await response.json()) as GoogleCalendarStatusResponse;
            hadSuccessfulLookup = true;
            break;
          }

          if (!payload) {
            continue;
          }

          if (payload.connected) {
            if (!isCancelled) {
              setGoogleStatus("connected");
              localStorage.setItem(GOOGLE_CONNECTED_USER_STORAGE_KEY, candidateUserId);
              if (resolvedHouseholdId) {
                localStorage.setItem(GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY, resolvedHouseholdId);
              }
              if (!hasTriggeredConnectedReconcileRef.current) {
                hasTriggeredConnectedReconcileRef.current = true;
                void forceReconcile();
              }
            }
            return;
          }
        }

        if (isCancelled) {
          return;
        }
        hasTriggeredConnectedReconcileRef.current = false;
        setGoogleStatus(hadSuccessfulLookup ? "not_connected" : "error");
      } catch {
        if (isCancelled) {
          return;
        }
        hasTriggeredConnectedReconcileRef.current = false;
        setGoogleStatus("error");
      }
    };

    void fetchGoogleStatus();
    return () => {
      isCancelled = true;
    };
  }, [statusCandidateKey, resolvedHouseholdId, forceReconcile]);

  const onConnectGoogle = () => {
    if (!SHADOW_INTEGRATION_ENDPOINTS_ENABLED) {
      setPostConnectSyncError("Google connect is disabled in canonical mode.");
      return;
    }

    if (!connectUserIdCandidate) {
      setPostConnectSyncError("Unable to resolve your account identity yet. Refresh once and try again.");
      return;
    }

    const encodedUserId = encodeURIComponent(connectUserIdCandidate);
    const params = new URLSearchParams();
    params.set("return_base", window.location.origin);
    if (resolvedHouseholdId) {
      params.set("household_id", resolvedHouseholdId);
    }
    const query = params.toString();
    // TODO: REMOVE_SHADOW_ENDPOINT
    window.location.href = buildApiUrl(
      `/integrations/google-calendar/connect/${encodedUserId}${query ? `?${query}` : ""}`,
    );
  };

  useEffect(() => {
    if (!SHADOW_INTEGRATION_ENDPOINTS_ENABLED) {
      hasTriggeredPostConnectSyncRef.current = false;
      if (callbackStatus === "integration_successful") {
        setPostConnectSyncError("Google post-connect sync is disabled in canonical mode.");
      }
      return;
    }

    const syncCandidateUserIds = Array.from(
      new Set([callbackUserId, activeUserId, linkedGoogleUserForHousehold].filter((value): value is string => Boolean(value))),
    );
    const syncHouseholdId = callbackFamilyId || resolvedHouseholdId;

    if (callbackStatus !== "integration_successful") {
      hasTriggeredPostConnectSyncRef.current = false;
      return;
    }
    if (hasTriggeredPostConnectSyncRef.current) {
      return;
    }
    if (syncCandidateUserIds.length === 0 || !syncHouseholdId) {
      setPostConnectSyncError("Google connected, but household identity is incomplete. Open Inventory and try Sync Inbox.");
      return;
    }

    hasTriggeredPostConnectSyncRef.current = true;

    let isCancelled = false;
    const runPostConnectInboxSync = async () => {
      setPostConnectSyncLoading(true);
      setPostConnectSyncMessage(null);
      setPostConnectSyncError(null);
      const attemptedAt = new Date().toISOString();

      persistInboxSyncStatus({
        status: "syncing",
        attempted_at: attemptedAt,
        last_success_at: null,
        max_results: INBOX_SYNC_MAX_RESULTS,
        processed_count: 0,
        failed_count: 0,
        message: `Syncing latest ${INBOX_SYNC_MAX_RESULTS} inbox emails...`,
      });

      try {
        const requestWith429Retry = async (path: string, init: RequestInit): Promise<Response> => {
          for (let attempt = 0; attempt < 3; attempt += 1) {
            const response = await fetchWithApiFallback(path, init);
            if (response.status === 429 && attempt < 2) {
              await sleep(200 * (attempt + 1));
              continue;
            }
            return response;
          }
          return fetchWithApiFallback(path, init);
        };

        let connectedUserId: string | null = null;
        for (const candidateUserId of syncCandidateUserIds) {
          // TODO: REMOVE_SHADOW_ENDPOINT
          const statusResponse = await requestWith429Retry(
            `/integrations/google-calendar/status/${encodeURIComponent(candidateUserId)}`,
            { method: "GET" },
          );
          if (!statusResponse.ok) {
            continue;
          }

          const statusPayload = (await statusResponse.json()) as GoogleCalendarStatusResponse;
          if (!statusPayload.connected) {
            continue;
          }

          connectedUserId = candidateUserId;
          if (!isCancelled) {
            setGoogleStatus("connected");
            localStorage.setItem(GOOGLE_CONNECTED_USER_STORAGE_KEY, connectedUserId);
            localStorage.setItem(GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY, syncHouseholdId);
          }
          break;
        }

        if (!connectedUserId) {
          throw new Error("Google sign-in returned, but inbox integration is not connected for this user. Reconnect Google and try again.");
        }

        const params = new URLSearchParams({
          household_id: syncHouseholdId,
          max_results: String(INBOX_SYNC_MAX_RESULTS),
        });
        // TODO: REMOVE_SHADOW_ENDPOINT
        const response = await requestWith429Retry(
          `/integrations/google-email/sync/${encodeURIComponent(connectedUserId)}?${params.toString()}`,
          { method: "POST" },
        );

        if (!response.ok) {
          if (response.status === 412) {
            throw new Error("Google inbox permission is missing. Reconnect Google to grant Gmail read access.");
          }
          if (response.status === 404) {
            throw new Error("Google integration is not connected for this user yet. Try Connect Google again.");
          }
          if (response.status === 422) {
            throw new Error("Household identity is missing for inbox sync.");
          }
          throw new Error("Unable to run inbox sync after Google connect.");
        }

        const payload = (await response.json()) as {
          processed_count?: number;
          failed_count?: number;
        };
        const processed = Number(payload.processed_count || 0);
        const failed = Number(payload.failed_count || 0);
        if (!isCancelled) {
          const successMessage = processed > 0
            ? `Google sign-in complete. Synced ${processed} inbox message(s)${failed > 0 ? ` (${failed} failed)` : ""}.`
            : `Google sign-in complete. Inbox sync finished with no eligible messages in the latest ${INBOX_SYNC_MAX_RESULTS} emails.`;

          if (processed > 0) {
            const failedSuffix = failed > 0 ? ` (${failed} failed)` : "";
            setPostConnectSyncMessage(`Google sign-in complete. Synced ${processed} inbox message(s)${failedSuffix}.`);
          } else {
            setPostConnectSyncMessage(
              `Google sign-in complete. Inbox sync finished with no eligible messages in the latest ${INBOX_SYNC_MAX_RESULTS} emails.`,
            );
          }

          persistInboxSyncStatus({
            status: "success",
            attempted_at: attemptedAt,
            last_success_at: new Date().toISOString(),
            max_results: INBOX_SYNC_MAX_RESULTS,
            processed_count: processed,
            failed_count: failed,
            message: successMessage,
          });
        }

        await forceReconcile();
      } catch (error) {
        if (!isCancelled) {
          const text = String(error || "post_connect_email_sync_failed");
          if (text.includes("inbox integration is not connected")) {
            setGoogleStatus("not_connected");
          }
          setPostConnectSyncError(text);
          persistInboxSyncStatus({
            status: "failed",
            attempted_at: attemptedAt,
            last_success_at: null,
            max_results: INBOX_SYNC_MAX_RESULTS,
            processed_count: 0,
            failed_count: 0,
            message: text,
          });
        }
      } finally {
        if (!isCancelled) {
          setPostConnectSyncLoading(false);
        }
      }
    };

    void runPostConnectInboxSync();

    return () => {
      isCancelled = true;
    };
  }, [
    callbackFamilyId,
    callbackStatus,
    callbackUserId,
    forceReconcile,
    activeUserId,
    linkedGoogleUserForHousehold,
    resolvedHouseholdId,
  ]);

  const googleStatusLabel = (() => {
    if (googleStatus === "connected") {
      return "Google Calendar connected";
    }
    if (googleStatus === "not_connected") {
      return "Google Calendar not connected";
    }
    if (googleStatus === "error") {
      return "Google Calendar status unavailable";
    }
    return "Checking Google Calendar status...";
  })();

  const connectButtonDisabled = !connectUserIdCandidate;
  const connectButtonLabel = connectButtonDisabled
    ? "Loading account before connect"
    : googleStatus === "connected"
    ? "Reconnect Google (Calendar + Gmail)"
    : "Connect Google (Calendar + Gmail)";

  const callbackMessage = callbackStatus === "integration_successful"
    ? postConnectSyncLoading
      ? `Google sign-in complete. Syncing latest ${INBOX_SYNC_MAX_RESULTS} inbox emails now...`
      : postConnectSyncMessage || (googleStatus === "connected"
        ? "Google sign-in complete. Calendar sync is now enabled for this household."
        : null)
    : null;

  const onSwitchHousehold = () => {
    const confirmed = window.confirm("Switch household and return to onboarding?");
    if (!confirmed) {
      return;
    }

    const keys = [
      "hpal-household-id",
      HOUSEHOLD_NAME_STORAGE_KEY,
      "hpal-user-id",
      "hpal-device-id",
      "hpal-role",
      "hpal.session.token",
      GOOGLE_CONNECTED_USER_STORAGE_KEY,
      GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY,
      "hpal-auth-email",
      "hpal-auth-name",
      "hpal.onboarding.v1",
    ];
    for (const key of keys) {
      localStorage.removeItem(key);
    }

    window.location.href = "/";
  };

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <h1 className="brand">Home Planner</h1>
        <p className="brand-subtitle">Your family's day, in one place</p>
        <nav className="nav-links" aria-label="Primary navigation">
          <NavLink to="/" end className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
            Home
          </NavLink>
          <NavLink to="/tasks" className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
            To-Dos
          </NavLink>
          <NavLink to="/calendar" className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
            Calendar
          </NavLink>
          <NavLink to="/analytics" className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
            Analytics
          </NavLink>
          <NavLink to="/inbox" className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
            Inbox
          </NavLink>
          <NavLink to="/pantry" className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
            Inventory
          </NavLink>
          <NavLink to="/chat" className={({ isActive }) => `nav-link ${isActive ? "active" : ""}`}>
            Assistant
          </NavLink>
        </nav>

        <section className="sidebar-account" aria-label="Account and integrations">
          <h2>Account</h2>
          <p>
            {activeUser?.display_name || "Guest"}
            {" · "}
            {displayHouseholdName}
          </p>
          <p
            className={`sidebar-integration-status ${
              googleStatus === "connected" ? "sidebar-integration-status-connected" : ""
            }`}
          >
            {googleStatusLabel}
          </p>
          {callbackMessage ? <p className="sidebar-flash-success">{callbackMessage}</p> : null}
          {postConnectSyncError ? <p className="error-text">{postConnectSyncError}</p> : null}
          <button type="button" className="sidebar-action" onClick={onConnectGoogle} disabled={connectButtonDisabled}>
            {connectButtonLabel}
          </button>
          <button type="button" className="sidebar-action sidebar-action-muted" onClick={onSwitchHousehold}>
            Switch Household
          </button>
        </section>
      </aside>
      <main className="main-panel">{children}</main>
    </div>
  );
};
