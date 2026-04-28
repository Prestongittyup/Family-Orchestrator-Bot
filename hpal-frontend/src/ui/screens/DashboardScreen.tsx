import React from "react";
import { useNavigate } from "react-router-dom";
import type { EmailDetail, Notification, RequestIdentityContext } from "../../api/contracts";
import { productSurfaceClient } from "../../api/productSurfaceClient";
import { useRuntimeStore } from "../../runtime/store";
import { selectNotifications, selectTaskCounts } from "../../runtime/selectors";
import {
  buildDismissedEmailKey,
  readDismissedEmailMap,
  writeDismissedEmailMap,
  type DismissedEmailMap,
} from "../utils/emailDismissals";
import { SyncStatusPill } from "../components/SyncStatusPill";

const deriveGreeting = (): string => {
  const hour = new Date().getHours();
  if (hour < 12) {
    return "Good morning";
  }
  if (hour < 18) {
    return "Good afternoon";
  }
  return "Good evening";
};

const firstName = (displayName: string | undefined): string => {
  if (!displayName) {
    return "there";
  }
  return displayName.trim().split(/\s+/)[0] || "there";
};

const prettyHouseholdName = (value: string | undefined): string => {
  if (!value) {
    return "Your household";
  }

  // UUID-like IDs are not meaningful to end users.
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) {
    return "Your household";
  }

  const normalized = value.replace(/[-_]+/g, " ").trim();
  if (!normalized) {
    return "Your household";
  }

  return normalized.replace(/\b\w/g, (match) => match.toUpperCase());
};

const formatMemberCount = (count: number): string =>
  `${count} ${count === 1 ? "member" : "members"}`;

const formatDateTime = (value: string | undefined): string => {
  const raw = (value || "").trim();
  if (!raw) {
    return "Unknown";
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return raw;
  }

  return parsed.toLocaleString();
};

const readLocalStorageValue = (key: string): string => {
  try {
    return localStorage.getItem(key) || "";
  } catch {
    return "";
  }
};

const EMAIL_SYNC_STATUS_STORAGE_KEY = "hpal-email-sync-status";
const EMAIL_SYNC_STATUS_EVENT = "hpal:email-sync-status";
const GOOGLE_CONNECTED_USER_STORAGE_KEY = "hpal-google-user-id";
const GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY = "hpal-google-household-id";
const INBOX_SYNC_MAX_RESULTS = 100;
const EMAIL_NOTIFICATION_PREFIX = "notif:email_summary:";

type InboxSyncStatusPayload = {
  status: "syncing" | "success" | "failed";
  attempted_at: string;
  last_success_at: string | null;
  max_results: number;
  processed_count: number;
  ignored_count?: number;
  failed_count: number;
  message: string;
};

const readInboxSyncStatus = (): InboxSyncStatusPayload | null => {
  try {
    const raw = localStorage.getItem(EMAIL_SYNC_STATUS_STORAGE_KEY);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw) as Partial<InboxSyncStatusPayload>;
    if (!parsed || typeof parsed !== "object") {
      return null;
    }

    const status = parsed.status;
    if (status !== "syncing" && status !== "success" && status !== "failed") {
      return null;
    }

    return {
      status,
      attempted_at: String(parsed.attempted_at || ""),
      last_success_at: parsed.last_success_at ? String(parsed.last_success_at) : null,
      max_results: Number(parsed.max_results || INBOX_SYNC_MAX_RESULTS),
      processed_count: Number(parsed.processed_count || 0),
      ignored_count: Number(parsed.ignored_count || 0),
      failed_count: Number(parsed.failed_count || 0),
      message: String(parsed.message || ""),
    };
  } catch {
    return null;
  }
};

const persistInboxSyncStatus = (payload: InboxSyncStatusPayload): InboxSyncStatusPayload => {
  const existing = readInboxSyncStatus();
  const normalized: InboxSyncStatusPayload = {
    ...payload,
    last_success_at: payload.last_success_at ?? existing?.last_success_at ?? null,
  };

  try {
    localStorage.setItem(EMAIL_SYNC_STATUS_STORAGE_KEY, JSON.stringify(normalized));
  } catch {
    // best-effort persistence only
  }

  try {
    window.dispatchEvent(new CustomEvent<InboxSyncStatusPayload>(EMAIL_SYNC_STATUS_EVENT, { detail: normalized }));
  } catch {
    // ignore dispatch failures in restrictive environments
  }

  return normalized;
};

const describeInboxSyncStatus = (status: InboxSyncStatusPayload | null): string => {
  if (!status) {
    return "No inbox sync has been run yet.";
  }

  if (status.status === "syncing") {
    return status.message || `Sync in progress for latest ${status.max_results} inbox emails.`;
  }

  if (status.status === "failed") {
    return status.message || "Last inbox sync failed.";
  }

  const ignoredSuffix = (status.ignored_count || 0) > 0 ? ` (${status.ignored_count} ignored)` : "";
  const failedSuffix = status.failed_count > 0 ? ` (${status.failed_count} failed)` : "";
  return status.message || `Last sync processed ${status.processed_count} message(s)${ignoredSuffix}${failedSuffix}.`;
};

const extractEmailId = (notification: Notification): string | null => {
  const notificationId = typeof notification.notification_id === "string"
    ? notification.notification_id
    : "";
  if (notificationId.startsWith(EMAIL_NOTIFICATION_PREFIX)) {
    const derived = notificationId.slice(EMAIL_NOTIFICATION_PREFIX.length).trim();
    return derived || null;
  }

  const title = typeof notification.title === "string" ? notification.title.toLowerCase() : "";
  const relatedEntity = typeof notification.related_entity === "string"
    ? notification.related_entity.trim()
    : "";
  if (title.startsWith("email:") && relatedEntity) {
    return relatedEntity;
  }

  return null;
};

const isEmailDebriefNotification = (notification: Notification): boolean => {
  const notificationId = typeof notification.notification_id === "string"
    ? notification.notification_id
    : "";
  if (notificationId.startsWith(EMAIL_NOTIFICATION_PREFIX)) {
    return true;
  }

  if (notification.title.toLowerCase().startsWith("email:")) {
    return true;
  }

  return false;
};

const debriefItemIdentifier = (notification: Notification, emailId: string | null): string =>
  (emailId || notification.notification_id || "").trim().toLowerCase();

const deriveDashboardDismissTag = (notification: Notification): string => {
  const text = `${notification.title} ${notification.message}`.toLowerCase();
  if (text.includes("action item") || text.includes("action required")) {
    return "actionable";
  }
  if (text.includes("calendar")) {
    return "calendar";
  }
  if (text.includes("invoice") || text.includes("payment") || text.includes("receipt")) {
    return "finance";
  }
  if (text.includes("promo") || text.includes("sale") || text.includes("offer")) {
    return "promotions";
  }
  return "general";
};

export const DashboardScreen: React.FC = () => {
  const navigate = useNavigate();
  const initialize = useRuntimeStore((state) => state.initialize);
  const familyId = useRuntimeStore((state) => state.familyId);
  const runtimeState = useRuntimeStore((state) => state.runtimeState);
  const isLoading = useRuntimeStore((state) => state.isLoading);
  const error = useRuntimeStore((state) => state.error);
  const activeUser = useRuntimeStore((state) => state.active_user);
  const activeHousehold = useRuntimeStore((state) => state.active_household);
  const deviceContext = useRuntimeStore((state) => state.device_context);
  const sessionToken = useRuntimeStore((state) => state.sessionToken);
  const forceReconcile = useRuntimeStore((state) => state.forceReconcile);
  const [selectedEmailId, setSelectedEmailId] = React.useState<string | null>(null);
  const [selectedEmailDetail, setSelectedEmailDetail] = React.useState<EmailDetail | null>(null);
  const [detailLoadingId, setDetailLoadingId] = React.useState<string | null>(null);
  const [emailDetailError, setEmailDetailError] = React.useState<string | null>(null);
  const [emailSyncLoading, setEmailSyncLoading] = React.useState(false);
  const [emailSyncMessage, setEmailSyncMessage] = React.useState<string | null>(null);
  const [emailSyncError, setEmailSyncError] = React.useState<string | null>(null);
  const [emailSyncStatus, setEmailSyncStatus] = React.useState<InboxSyncStatusPayload | null>(() =>
    readInboxSyncStatus(),
  );
  const [dismissedEmailMap, setDismissedEmailMap] = React.useState<DismissedEmailMap>(() => readDismissedEmailMap());

  React.useEffect(() => {
    writeDismissedEmailMap(dismissedEmailMap);
  }, [dismissedEmailMap]);

  const onRetryDashboardLoad = React.useCallback(() => {
    if (!familyId) {
      return;
    }

    void initialize(familyId);
  }, [familyId, initialize]);

  React.useEffect(() => {
    const onStorage = (event: StorageEvent) => {
      if (event.key === EMAIL_SYNC_STATUS_STORAGE_KEY) {
        setEmailSyncStatus(readInboxSyncStatus());
      }
    };

    const onSyncStatus = (event: Event) => {
      const customEvent = event as CustomEvent<InboxSyncStatusPayload>;
      if (customEvent.detail) {
        setEmailSyncStatus(customEvent.detail);
        return;
      }
      setEmailSyncStatus(readInboxSyncStatus());
    };

    window.addEventListener("storage", onStorage);
    window.addEventListener(EMAIL_SYNC_STATUS_EVENT, onSyncStatus as EventListener);

    return () => {
      window.removeEventListener("storage", onStorage);
      window.removeEventListener(EMAIL_SYNC_STATUS_EVENT, onSyncStatus as EventListener);
    };
  }, []);

  if (!runtimeState) {
    if (isLoading) {
      return <section className="screen-panel">Loading dashboard...</section>;
    }

    return (
      <section className="screen-panel">
        <p className="error-text">{error || "No data available."}</p>
        <p className="empty-text">Try reloading your household state.</p>
        <button
          type="button"
          className="dashboard-detail-button"
          onClick={onRetryDashboardLoad}
          disabled={!familyId}
        >
          Retry load
        </button>
      </section>
    );
  }

  const counts = selectTaskCounts(runtimeState);
  const notifications = selectNotifications(runtimeState);
  const emailDebriefNotifications = notifications.filter(isEmailDebriefNotification);
  const householdId = runtimeState.snapshot.family.family_id;
  const emailDebriefItems = emailDebriefNotifications.map((notification) => {
    const emailId = extractEmailId(notification);
    const itemKey = debriefItemIdentifier(notification, emailId);
    const dismissalKey = buildDismissedEmailKey(householdId, itemKey);
    const dismissed = dismissedEmailMap[dismissalKey];

    return {
      notification,
      emailId,
      dismissalKey,
      dismissedAt: dismissed ? dismissed.dismissed_at : null,
    };
  });
  const dismissedEmailDebriefItems = emailDebriefItems.filter((item) => item.dismissedAt !== null);
  const activeEmailDebriefItems = emailDebriefItems.filter((item) => item.dismissedAt === null);
  const prioritizedEmailDebriefItems = activeEmailDebriefItems.filter((item) => item.notification.level !== "info");
  const hiddenLowPriorityEmailCount = activeEmailDebriefItems.length - prioritizedEmailDebriefItems.length;
  const dismissedOnHomeCount = dismissedEmailDebriefItems.length;
  const actionableEmailDebriefItems = prioritizedEmailDebriefItems.filter(
    (item): item is { notification: Notification; emailId: string; dismissalKey: string; dismissedAt: string | null } =>
      typeof item.emailId === "string" && item.emailId.length > 0,
  );
  const summaryOnlyEmailDebriefItems = prioritizedEmailDebriefItems.filter((item) => !item.emailId);
  const recentAlerts = notifications.filter((notification) => !isEmailDebriefNotification(notification));
  const overview = runtimeState.snapshot.today_overview;
  const lowStockCount = runtimeState.snapshot.pantry?.low_stock_count ?? 0;
  const completedTotal = counts.pending + counts.inProgress + counts.completed;
  const completionRate = completedTotal > 0 ? Math.round((counts.completed / completedTotal) * 100) : 100;
  const nextBestStep =
    overview.open_task_count > 0
      ? "You have tasks waiting. Start with the highest-priority item."
      : overview.notification_count > 0
      ? "Review your latest alerts to stay ahead of schedule changes."
      : "Everything looks calm right now. Great job keeping things on track.";

  const onOpenAlerts = () => {
    const target = document.getElementById("dashboard-recent-alerts");
    if (!target) {
      return;
    }
    target.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  const detailIdentity: RequestIdentityContext | undefined =
    activeHousehold && activeUser && deviceContext && sessionToken
      ? {
          household_id: activeHousehold.household_id,
          user_id: activeUser.user_id,
          device_id: deviceContext.device_id,
          session_token: sessionToken,
        }
      : undefined;

  const onToggleEmailDetail = async (emailId: string) => {
    if (!emailId) {
      return;
    }

    if (selectedEmailId === emailId && detailLoadingId === null) {
      setSelectedEmailId(null);
      setSelectedEmailDetail(null);
      setEmailDetailError(null);
      return;
    }

    setSelectedEmailId(emailId);
    setEmailDetailError(null);
    setDetailLoadingId(emailId);
    try {
      const detail = await productSurfaceClient.fetchEmailDetail(
        runtimeState.snapshot.family.family_id,
        emailId,
        detailIdentity,
      );
      setSelectedEmailDetail(detail);
    } catch (_error) {
      setSelectedEmailDetail(null);
      setEmailDetailError("Unable to load email details right now.");
    } finally {
      setDetailLoadingId(null);
    }
  };

  const onDismissEmailDebriefItem = (notification: Notification, emailId: string | null, dismissalKey: string) => {
    const itemKey = debriefItemIdentifier(notification, emailId);
    if (!itemKey) {
      return;
    }

    setDismissedEmailMap((current) => ({
      ...current,
      [dismissalKey]: {
        tag: deriveDashboardDismissTag(notification),
        dismissed_at: new Date().toISOString(),
        household_id: householdId,
        item_key: itemKey,
        notification_id: notification.notification_id,
      },
    }));

    if (selectedEmailId && emailId && selectedEmailId === emailId) {
      setSelectedEmailId(null);
      setSelectedEmailDetail(null);
      setEmailDetailError(null);
    }
  };

  const onSyncInbox = async () => {
    const activeUserId = (activeUser?.user_id || "").trim();
    const linkedGoogleUserId = readLocalStorageValue(GOOGLE_CONNECTED_USER_STORAGE_KEY).trim();
    const linkedGoogleHouseholdId = readLocalStorageValue(GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY).trim();
    const resolvedHouseholdId = (
      activeHousehold?.household_id ||
      runtimeState.snapshot.family.family_id ||
      readLocalStorageValue("hpal-household-id")
    ).trim();
    const scopedLinkedGoogleUserId =
      linkedGoogleUserId && linkedGoogleHouseholdId && resolvedHouseholdId && linkedGoogleHouseholdId !== resolvedHouseholdId
        ? ""
        : linkedGoogleUserId;
    const syncCandidateUserIds = Array.from(
      new Set(
        [scopedLinkedGoogleUserId, activeUserId]
          .map((candidate) => candidate.trim())
          .filter((candidate) => candidate.length > 0),
      ),
    );
    const attemptedAt = new Date().toISOString();

    if (syncCandidateUserIds.length === 0) {
      const message = "Missing user identity. Complete onboarding and reconnect Google.";
      setEmailSyncError(message);
      setEmailSyncStatus(
        persistInboxSyncStatus({
          status: "failed",
          attempted_at: attemptedAt,
          last_success_at: emailSyncStatus?.last_success_at ?? null,
          max_results: INBOX_SYNC_MAX_RESULTS,
          processed_count: 0,
          failed_count: 0,
          message,
        }),
      );
      return;
    }

    if (!resolvedHouseholdId) {
      const message = "Missing household identity. Complete onboarding and try again.";
      setEmailSyncError(message);
      setEmailSyncStatus(
        persistInboxSyncStatus({
          status: "failed",
          attempted_at: attemptedAt,
          last_success_at: emailSyncStatus?.last_success_at ?? null,
          max_results: INBOX_SYNC_MAX_RESULTS,
          processed_count: 0,
          failed_count: 0,
          message,
        }),
      );
      return;
    }

    setEmailSyncStatus(
      persistInboxSyncStatus({
        status: "syncing",
        attempted_at: attemptedAt,
        last_success_at: emailSyncStatus?.last_success_at ?? null,
        max_results: INBOX_SYNC_MAX_RESULTS,
        processed_count: 0,
        failed_count: 0,
        message: `Syncing latest ${INBOX_SYNC_MAX_RESULTS} inbox emails...`,
      }),
    );

    setEmailSyncLoading(true);
    setEmailSyncMessage(null);
    setEmailSyncError(null);

    try {
      const runSyncWithRetries = async (candidateUserId: string) => {
        let lastError: unknown = null;
        for (let attempt = 0; attempt < 3; attempt += 1) {
          try {
            return await productSurfaceClient.syncGoogleInbox(
              candidateUserId,
              resolvedHouseholdId,
              detailIdentity,
              INBOX_SYNC_MAX_RESULTS,
            );
          } catch (error) {
            lastError = error;
            const text = String(error || "email sync failed");
            if (text.includes("email_sync_failed:429") && attempt < 2) {
              await new Promise((resolve) => window.setTimeout(resolve, 200 * (attempt + 1)));
              continue;
            }
            throw error;
          }
        }
        throw lastError || new Error("email sync failed");
      };

      let result: Awaited<ReturnType<typeof productSurfaceClient.syncGoogleInbox>> | null = null;
      let syncFailure: unknown = null;
      for (const candidateUserId of syncCandidateUserIds) {
        try {
          result = await runSyncWithRetries(candidateUserId);
          localStorage.setItem(GOOGLE_CONNECTED_USER_STORAGE_KEY, candidateUserId);
          localStorage.setItem(GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY, resolvedHouseholdId);
          break;
        } catch (error) {
          syncFailure = error;
          const text = String(error || "email sync failed");
          if (text.includes("email_sync_failed:404")) {
            continue;
          }
          throw error;
        }
      }

      if (!result) {
        throw syncFailure || new Error("email sync failed");
      }

      const successMessage = result.processed_count > 0
        ? `Synced ${result.processed_count} inbox message(s) from the latest ${INBOX_SYNC_MAX_RESULTS}${result.failed_count > 0 ? ` (${result.failed_count} failed)` : ""}.`
        : `Inbox sync completed for latest ${INBOX_SYNC_MAX_RESULTS} emails, but no eligible messages were returned.`;

      if (result.processed_count > 0) {
        const failedSuffix = result.failed_count > 0 ? ` (${result.failed_count} failed)` : "";
        setEmailSyncMessage(
          `Synced ${result.processed_count} inbox message(s) from the latest ${INBOX_SYNC_MAX_RESULTS}${failedSuffix}.`,
        );
      } else {
        setEmailSyncMessage(
          `Inbox sync completed for latest ${INBOX_SYNC_MAX_RESULTS} emails, but no eligible messages were returned.`,
        );
      }

      setEmailSyncStatus(
        persistInboxSyncStatus({
          status: "success",
          attempted_at: attemptedAt,
          last_success_at: new Date().toISOString(),
          max_results: INBOX_SYNC_MAX_RESULTS,
          processed_count: Number(result.processed_count || 0),
          failed_count: Number(result.failed_count || 0),
          message: successMessage,
        }),
      );

      await forceReconcile();
    } catch (error) {
      const text = String(error || "email sync failed");
      let failureMessage = "Unable to sync inbox right now.";

      if (text.includes("email_sync_failed:412")) {
        failureMessage = "Google inbox permission is missing. Reconnect Google to grant Gmail read access.";
      } else if (text.includes("email_sync_failed:404")) {
        failureMessage = "Google inbox integration is not connected for this account. Reconnect Google and try again.";
      } else if (text.includes("email_sync_failed:422")) {
        failureMessage = "Household identity is missing for inbox sync.";
      }

      setEmailSyncError(failureMessage);
      setEmailSyncStatus(
        persistInboxSyncStatus({
          status: "failed",
          attempted_at: attemptedAt,
          last_success_at: emailSyncStatus?.last_success_at ?? null,
          max_results: INBOX_SYNC_MAX_RESULTS,
          processed_count: 0,
          failed_count: 0,
          message: failureMessage,
        }),
      );
    } finally {
      setEmailSyncLoading(false);
    }
  };

  const syncSummary = describeInboxSyncStatus(emailSyncStatus);
  const lastSuccessLabel = emailSyncStatus?.last_success_at
    ? formatDateTime(emailSyncStatus.last_success_at)
    : "Not yet";
  const lastAttemptLabel = emailSyncStatus?.attempted_at
    ? formatDateTime(emailSyncStatus.attempted_at)
    : "Not yet";

  return (
    <section className="screen-panel dashboard-panel">
      <header className="dashboard-hero">
        <div className="dashboard-hero-main">
          <p className="dashboard-eyebrow">{prettyHouseholdName(runtimeState.snapshot.family.family_id)}</p>
          <h2 className="dashboard-title">
            {deriveGreeting()}, {firstName(activeUser?.display_name)}
          </h2>
          <p className="dashboard-subtitle">
            Here is what your household looks like today.
          </p>
          <p className="dashboard-meta">{formatMemberCount(runtimeState.snapshot.family.member_count)}</p>
        </div>
        <div className="dashboard-hero-side">
          <SyncStatusPill status={runtimeState.sync_status} />
        </div>
      </header>

      {error ? <p className="error-text">{error}</p> : null}

      <div className="metric-grid dashboard-metric-grid">
        <button type="button" className="metric-card dashboard-metric-card dashboard-metric-button" onClick={() => navigate("/tasks")}>
          <p className="metric-kicker">Tasks</p>
          <h3>Needs attention</h3>
          <p>{overview.open_task_count}</p>
        </button>
        <button type="button" className="metric-card dashboard-metric-card dashboard-metric-button" onClick={() => navigate("/calendar")}>
          <p className="metric-kicker">Calendar</p>
          <h3>Events today</h3>
          <p>{overview.scheduled_event_count}</p>
        </button>
        <button type="button" className="metric-card dashboard-metric-card dashboard-metric-button" onClick={() => navigate("/assistant")}>
          <p className="metric-kicker">Plans</p>
          <h3>In motion</h3>
          <p>{overview.active_plan_count}</p>
        </button>
        <button type="button" className="metric-card dashboard-metric-card dashboard-metric-button" onClick={onOpenAlerts}>
          <p className="metric-kicker">Alerts</p>
          <h3>Unread</h3>
          <p>{overview.notification_count}</p>
        </button>
        <button type="button" className="metric-card dashboard-metric-card dashboard-metric-button" onClick={() => navigate("/pantry")}>
          <p className="metric-kicker">Pantry</p>
          <h3>Low stock</h3>
          <p>{lowStockCount}</p>
        </button>
      </div>

      <section className="dashboard-section">
        <div className="dashboard-section-header">
          <h3>Progress Snapshot</h3>
          <span className="dashboard-highlight">{completionRate}% complete</span>
        </div>
        <div className="metric-grid dashboard-metric-grid compact">
          <article className="metric-card dashboard-metric-card small">
            <h3>Pending</h3>
            <p>{counts.pending}</p>
          </article>
          <article className="metric-card dashboard-metric-card small">
            <h3>In progress</h3>
            <p>{counts.inProgress}</p>
          </article>
          <article className="metric-card dashboard-metric-card small">
            <h3>Completed</h3>
            <p>{counts.completed}</p>
          </article>
          <article className="metric-card dashboard-metric-card small">
            <h3>Needs retry</h3>
            <p>{counts.failed}</p>
          </article>
        </div>
      </section>

      <section className="dashboard-section dashboard-next-step">
        <h3>Next Best Step</h3>
        <p>{nextBestStep}</p>
      </section>

      <section className="dashboard-section" id="dashboard-email-debriefing">
        <div className="dashboard-section-header">
          <h3>Email Debriefing</h3>
          <span className="dashboard-highlight">{prioritizedEmailDebriefItems.length} item(s)</span>
        </div>

        <div className="dashboard-sync-status-panel" aria-live="polite">
          <p className="dashboard-sync-status-summary">{syncSummary}</p>
          <p className="dashboard-sync-status-meta">
            <strong>Last successful sync:</strong> {lastSuccessLabel}
          </p>
          <p className="dashboard-sync-status-meta">
            <strong>Last attempt:</strong> {lastAttemptLabel}
          </p>
          <p className="dashboard-sync-status-meta">
            <strong>Sync window:</strong> Latest {INBOX_SYNC_MAX_RESULTS} inbox emails
          </p>
        </div>

        <div className="dashboard-list-controls">
          <button
            type="button"
            className="dashboard-detail-button"
            onClick={() => void onSyncInbox()}
            disabled={emailSyncLoading}
          >
            {emailSyncLoading ? "Syncing inbox..." : "Sync Inbox"}
          </button>
          <button
            type="button"
            className="dashboard-detail-button"
            onClick={() => navigate("/inbox")}
          >
            Open Inbox Tab
          </button>
        </div>
        {emailSyncMessage ? <p>{emailSyncMessage}</p> : null}
        {emailSyncError ? <p className="error-text">{emailSyncError}</p> : null}
        {hiddenLowPriorityEmailCount > 0 ? (
          <p className="task-meta">
            {hiddenLowPriorityEmailCount} low-priority email(s) hidden on Home. Open Inbox Tab to review everything.
          </p>
        ) : null}
        {dismissedOnHomeCount > 0 ? (
          <p className="task-meta">
            {dismissedOnHomeCount} dismissed email alert(s) hidden on Home.
          </p>
        ) : null}

        {prioritizedEmailDebriefItems.length === 0 ? (
          <p className="empty-text">
            {emailDebriefNotifications.length === 0
              ? `No email debriefing items yet. Sync your inbox to pull the latest ${INBOX_SYNC_MAX_RESULTS} messages.`
              : "No high-priority email debriefing items right now. Open Inbox Tab to review all synced emails."}
          </p>
        ) : (
          <ul className="list-panel dashboard-list-panel">
            {actionableEmailDebriefItems.map(({ notification, emailId, dismissalKey }) => {
              return (
                <li key={notification.notification_id}>
                  <div className="dashboard-list-title-row">
                    <strong>{notification.title}</strong>
                    <span className={`level-pill level-${notification.level}`}>{notification.level}</span>
                  </div>
                  <p>{notification.message}</p>
                  <div className="dashboard-list-controls">
                    <button
                      type="button"
                      className="dashboard-detail-button"
                      onClick={() => void onToggleEmailDetail(emailId)}
                    >
                      {selectedEmailId === emailId ? "Hide details" : "View details"}
                    </button>
                    <button
                      type="button"
                      className="dashboard-detail-button"
                      onClick={() => onDismissEmailDebriefItem(notification, emailId, dismissalKey)}
                    >
                      Dismiss
                    </button>
                  </div>
                  {selectedEmailId === emailId ? (
                    <div className="email-detail-panel">
                      {detailLoadingId === emailId ? (
                        <p>Loading email details...</p>
                      ) : emailDetailError ? (
                        <p className="error-text">{emailDetailError}</p>
                      ) : selectedEmailDetail && selectedEmailDetail.email_id === emailId ? (
                        <>
                          <p>{selectedEmailDetail.summary}</p>
                          <div className="email-detail-meta">
                            <span><strong>Sender:</strong> {selectedEmailDetail.sender}</span>
                            <span><strong>Received:</strong> {formatDateTime(selectedEmailDetail.received_at)}</span>
                            <span><strong>Triage:</strong> {selectedEmailDetail.triage_decision}</span>
                            <span><strong>Priority:</strong> {selectedEmailDetail.importance_bucket}</span>
                          </div>

                          {selectedEmailDetail.action_items.length > 0 ? (
                            <div>
                              <strong>Action Items</strong>
                              <ul className="email-detail-list">
                                {selectedEmailDetail.action_items.map((item, index) => (
                                  <li key={`${selectedEmailDetail.email_id}-action-${index}`}>
                                    {item.title}
                                    {item.due_hint_local ? ` (${item.due_hint_local})` : ""}
                                  </li>
                                ))}
                              </ul>
                            </div>
                          ) : null}

                          {selectedEmailDetail.calendar_candidates.length > 0 ? (
                            <div>
                              <strong>Calendar Candidates</strong>
                              <ul className="email-detail-list">
                                {selectedEmailDetail.calendar_candidates.map((item, index) => (
                                  <li key={`${selectedEmailDetail.email_id}-calendar-${index}`}>
                                    {item.title}
                                    {item.time_hint_local ? ` (${item.time_hint_local})` : ""}
                                  </li>
                                ))}
                              </ul>
                            </div>
                          ) : null}

                          {selectedEmailDetail.informational_items.length > 0 ? (
                            <div>
                              <strong>Informational Highlights</strong>
                              <ul className="email-detail-list">
                                {selectedEmailDetail.informational_items.map((item, index) => (
                                  <li key={`${selectedEmailDetail.email_id}-info-${index}`}>
                                    {item.title}
                                  </li>
                                ))}
                              </ul>
                            </div>
                          ) : null}

                          {selectedEmailDetail.body ? (
                            <div className="email-detail-body">
                              <strong>Email Body</strong>
                              <p>{selectedEmailDetail.body}</p>
                            </div>
                          ) : null}
                        </>
                      ) : null}
                    </div>
                  ) : null}
                </li>
              );
            })}

            {summaryOnlyEmailDebriefItems.map(({ notification }) => (
              <li key={`${notification.notification_id}-summary-only`}>
                <div className="dashboard-list-title-row">
                  <strong>{notification.title}</strong>
                  <span className={`level-pill level-${notification.level}`}>{notification.level}</span>
                </div>
                <p>{notification.message}</p>
                <p className="empty-text">Detailed view unavailable for this message. Sync inbox to refresh detail links.</p>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="dashboard-section" id="dashboard-recent-alerts">
        <div className="dashboard-section-header">
          <h3>Recent Alerts</h3>
        </div>
        {recentAlerts.length === 0 ? (
          <p className="empty-text">No alerts right now. You are all caught up.</p>
        ) : (
          <ul className="list-panel dashboard-list-panel">
            {recentAlerts.map((notification) => (
              <li key={notification.notification_id}>
                <div className="dashboard-list-title-row">
                  <strong>{notification.title}</strong>
                  <span className={`level-pill level-${notification.level}`}>{notification.level}</span>
                </div>
                <p>{notification.message}</p>
              </li>
            ))}
          </ul>
        )}
      </section>
    </section>
  );
};
