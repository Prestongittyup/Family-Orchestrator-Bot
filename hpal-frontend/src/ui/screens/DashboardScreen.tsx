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
import {
  interpretHomeUxSurfaceContract,
  type HomePriorityCard,
  type HomeUxSurfaceModel,
} from "../contracts/homeUxSurfaceContract";
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

const formatDecisionBlockingLabel = (count: number): string => {
  if (count <= 0) {
    return "No decisions blocking you";
  }

  return `${count} decision${count === 1 ? "" : "s"} blocking your day`;
};

const decisionQueueIndexLabel = (index: number): string => {
  if (index <= 0) {
    return "Now";
  }
  return `Next ${index}`;
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
  const [homeUxSurface, setHomeUxSurface] = React.useState<HomeUxSurfaceModel | null>(null);
  const [homePriorityLoading, setHomePriorityLoading] = React.useState(false);
  const [homePriorityError, setHomePriorityError] = React.useState<string | null>(null);
  const [homePriorityExpanded, setHomePriorityExpanded] = React.useState(false);
  const [activeDecisionId, setActiveDecisionId] = React.useState<string | null>(null);
  const [decisionMutationInFlightId, setDecisionMutationInFlightId] = React.useState<string | null>(null);
  const [decisionFeedbackMessage, setDecisionFeedbackMessage] = React.useState<string | null>(null);

  const detailIdentity: RequestIdentityContext | undefined = React.useMemo(() => {
    if (!activeHousehold || !activeUser || !deviceContext || !sessionToken) {
      return undefined;
    }

    return {
      household_id: activeHousehold.household_id,
      user_id: activeUser.user_id,
      device_id: deviceContext.device_id,
      session_token: sessionToken,
    };
  }, [
    activeHousehold?.household_id,
    activeUser?.user_id,
    deviceContext?.device_id,
    sessionToken,
  ]);

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

  React.useEffect(() => {
    const householdId = (runtimeState?.snapshot.family.family_id || "").trim();
    if (!householdId) {
      setHomeUxSurface(null);
      setHomePriorityError(null);
      setHomePriorityLoading(false);
      return;
    }

    let cancelled = false;
    setHomePriorityLoading(true);
    setHomePriorityError(null);

    void productSurfaceClient.fetchHomeV0(householdId, detailIdentity)
      .then((homeContract) => {
        if (cancelled) {
          return;
        }
        setHomeUxSurface(interpretHomeUxSurfaceContract(homeContract));
        setHomePriorityExpanded(false);
      })
      .catch(() => {
        if (cancelled) {
          return;
        }
        setHomeUxSurface(null);
        setHomePriorityError("Unable to load Home focus right now.");
      })
      .finally(() => {
        if (cancelled) {
          return;
        }
        setHomePriorityLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [detailIdentity, runtimeState?.snapshot.family.family_id]);

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

  const orderedHomeCards = homeUxSurface?.ordered_cards ?? [];
  const decisionCards = orderedHomeCards.filter((card) => card.kind === "decision");
  const actionCards = orderedHomeCards.filter((card) => card.kind === "action");
  const calendarCards = orderedHomeCards.filter((card) => card.kind === "calendar");

  const pendingDecisionCards = decisionCards;

  const activeDecisionCard =
    pendingDecisionCards.find((card) => card.source_id === activeDecisionId)
    || pendingDecisionCards[0]
    || null;

  const hasBlockingDecisions = pendingDecisionCards.length > 0;
  const decisionBlockingLabel = formatDecisionBlockingLabel(pendingDecisionCards.length);
  const topCalendarCard = calendarCards[0] ?? null;
  const decisionSignature = decisionCards.map((card) => card.source_id).join("|");

  React.useEffect(() => {
    setActiveDecisionId(null);
    setDecisionFeedbackMessage(null);
    setHomePriorityExpanded(false);
  }, [decisionSignature]);

  React.useEffect(() => {
    if (!activeDecisionCard) {
      setActiveDecisionId(null);
      return;
    }

    if (activeDecisionId !== activeDecisionCard.source_id) {
      setActiveDecisionId(activeDecisionCard.source_id);
    }
  }, [activeDecisionCard, activeDecisionId]);

  React.useEffect(() => {
    if (!hasBlockingDecisions) {
      setHomePriorityExpanded(true);
    }
  }, [hasBlockingDecisions]);

  const onResolveDecisionOption = React.useCallback(
    async (card: HomePriorityCard, option: string) => {
      const householdId = (runtimeState.snapshot.family.family_id || "").trim();
      if (!householdId) {
        setHomePriorityError("Unable to resolve decision: missing household context.");
        return;
      }

      if (decisionMutationInFlightId === card.source_id) {
        return;
      }

      setDecisionMutationInFlightId(card.source_id);
      setDecisionFeedbackMessage(null);
      setHomePriorityError(null);

      try {
        const normalizedOption = option.trim().toLowerCase();
        let feedbackPrefix = "Decision recorded";

        if (/\b(ignore|dismiss|skip)\b/.test(normalizedOption)) {
          await productSurfaceClient.ignoreDecision(householdId, card.source_id, detailIdentity);
          feedbackPrefix = "Decision ignored";
        } else if (/\b(defer|later|tomorrow|next\s+week)\b/.test(normalizedOption)) {
          const deferDate = new Date();
          deferDate.setDate(deferDate.getDate() + 1);
          const deferToDate = deferDate.toISOString().slice(0, 10);
          await productSurfaceClient.deferDecision(
            householdId,
            card.source_id,
            deferToDate,
            detailIdentity,
          );
          feedbackPrefix = `Decision deferred to ${deferToDate}`;
        } else {
          await productSurfaceClient.completeDecision(householdId, card.source_id, detailIdentity);
          feedbackPrefix = "Decision completed";
        }

        const refreshedHome = await productSurfaceClient.fetchHomeV0(householdId, detailIdentity);
        setHomeUxSurface(interpretHomeUxSurfaceContract(refreshedHome));
        setDecisionFeedbackMessage(`${feedbackPrefix}: ${option}`);
      } catch {
        setHomePriorityError("Unable to persist decision right now.");
      } finally {
        setDecisionMutationInFlightId(null);
      }
    },
    [decisionMutationInFlightId, detailIdentity, runtimeState.snapshot.family.family_id],
  );

  const onOpenAlerts = () => {
    const target = document.getElementById("dashboard-recent-alerts");
    if (!target) {
      return;
    }
    target.scrollIntoView({ behavior: "smooth", block: "start" });
  };

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
    <section className={`screen-panel dashboard-panel ${hasBlockingDecisions ? "decision-priority-active" : ""}`}>
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

      <div className={`metric-grid dashboard-metric-grid ${hasBlockingDecisions ? "dashboard-secondary-muted" : ""}`}>
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

      <section className={`dashboard-section ${hasBlockingDecisions ? "dashboard-secondary-muted" : ""}`}>
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

      <section className={`dashboard-section dashboard-next-step ${hasBlockingDecisions ? "dashboard-secondary-muted" : ""}`}>
        <h3>Next Best Step</h3>
        <p>{nextBestStep}</p>
      </section>

      <section className="dashboard-section dashboard-home-focus" id="dashboard-home-focus">
        <div className="dashboard-section-header">
          <h3>{hasBlockingDecisions ? "Decision Gate" : "Home Focus"}</h3>
          <span className="dashboard-highlight">{decisionBlockingLabel}</span>
        </div>

        {homePriorityLoading ? (
          <p className="empty-text">Building decision-first flow...</p>
        ) : null}

        {!homePriorityLoading && homePriorityError ? (
          <p className="error-text">{homePriorityError}</p>
        ) : null}

        {!homePriorityLoading && !homePriorityError && homeUxSurface ? (
          <>
            <div className="home-focus-hero" aria-live="polite">
              <p className="home-focus-eyebrow">
                {hasBlockingDecisions ? "Decisions first" : "Ready to execute"}
              </p>
              <h4>{hasBlockingDecisions ? "This needs your decision" : "No decisions blocking you"}</h4>
              <p>
                {hasBlockingDecisions
                  ? "Pick what should happen. We can't proceed until this is decided."
                  : "No decisions are blocking your day. Move straight into execution."}
              </p>
              <p className="task-meta">
                {hasBlockingDecisions
                  ? `${decisionBlockingLabel}. ${actionCards.length} action${actionCards.length === 1 ? "" : "s"} up next.`
                  : "No decisions blocking you. You're clear to execute."}
              </p>
              {decisionFeedbackMessage ? (
                <p className="task-meta home-decision-feedback">{decisionFeedbackMessage}</p>
              ) : null}
            </div>

            {pendingDecisionCards.length === 0 ? (
              <p className="empty-text">All decisions in this view are handled. Continue with actions below.</p>
            ) : (
              <ul className="list-panel dashboard-list-panel home-priority-list home-decision-list">
                {pendingDecisionCards.map((card, index) => {
                  const isExpanded = activeDecisionCard?.card_id === card.card_id;
                  const options = card.decision_options && card.decision_options.length > 0
                    ? card.decision_options
                    : ["Confirm this decision"];

                  return (
                    <li
                      key={card.card_id}
                      className={`home-priority-card home-decision-card ${isExpanded ? "home-decision-card-expanded" : "home-decision-card-collapsed"}`}
                    >
                      <div className="dashboard-list-title-row">
                        <strong>{index === 0 ? "This needs your decision" : "Decision queued next"}</strong>
                        <span className="level-pill home-priority-tier-pill home-decision-queue-pill">
                          {decisionQueueIndexLabel(index)}
                        </span>
                      </div>
                      <p className="home-decision-question">{card.title}</p>
                      {isExpanded ? (
                        <div className="home-decision-options" key={card.card_id}>
                          {options.map((option, optionIndex) => (
                            <button
                              key={`${card.card_id}:option:${optionIndex}`}
                              type="button"
                              className={`dashboard-detail-button home-decision-option-button ${optionIndex === 0 ? "home-decision-option-recommended" : ""}`}
                              onClick={() => onResolveDecisionOption(card, option)}
                              disabled={decisionMutationInFlightId === card.source_id}
                            >
                              <span>{option}</span>
                              {optionIndex === 0 ? (
                                <span className="home-decision-recommendation">Recommended - Best fit</span>
                              ) : null}
                            </button>
                          ))}
                        </div>
                      ) : (
                        <p className="task-meta">Pick what should happen, then continue.</p>
                      )}
                    </li>
                  );
                })}
              </ul>
            )}

            <div className={`home-up-next ${hasBlockingDecisions ? "home-up-next-collapsed" : ""}`}>
              <div className="dashboard-section-header home-up-next-header">
                <h4>{hasBlockingDecisions ? "Up next" : "Actions"}</h4>
                <span className="dashboard-highlight">
                  {actionCards.length} action{actionCards.length === 1 ? "" : "s"}
                </span>
              </div>

              {actionCards.length > 0 ? (
                <>
                  {hasBlockingDecisions ? (
                    <button
                      type="button"
                      className="dashboard-detail-button"
                      onClick={() => setHomePriorityExpanded((current) => !current)}
                    >
                      {homePriorityExpanded
                        ? "Hide up next actions"
                        : `Show ${Math.min(actionCards.length, 3)} up next action(s)`}
                    </button>
                  ) : null}

                  {(homePriorityExpanded || !hasBlockingDecisions) ? (
                    <ul className="list-panel dashboard-list-panel home-priority-list collapsed">
                      {actionCards.map((card, index) => (
                        <li
                          key={card.card_id}
                          className={`home-priority-card ${!hasBlockingDecisions && index === 0 ? "home-action-primary-card" : ""}`}
                        >
                          <div className="dashboard-list-title-row">
                            <strong>{card.title}</strong>
                            {!hasBlockingDecisions && index === 0 ? (
                              <span className="level-pill home-priority-tier-pill home-action-primary-pill">
                                Start now
                              </span>
                            ) : null}
                          </div>
                          <p>{card.detail}</p>
                          <div className="dashboard-list-controls">
                            <button
                              type="button"
                              className={`dashboard-detail-button ${!hasBlockingDecisions && index === 0 ? "home-action-primary-button" : ""}`}
                              onClick={() => navigate(card.cta_route)}
                            >
                              {!hasBlockingDecisions && index === 0 ? "Do this next" : card.cta_label}
                            </button>
                          </div>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="task-meta">
                      Actions are intentionally collapsed while decisions are unresolved.
                    </p>
                  )}
                </>
              ) : (
                <p className="empty-text">No actions queued right now.</p>
              )}
            </div>

            <div className={`home-calendar-strip ${hasBlockingDecisions ? "home-calendar-strip-minimal" : ""}`}>
              <p className="home-calendar-strip-label">Calendar context</p>
              {topCalendarCard ? (
                <p>
                  {topCalendarCard.title} ({topCalendarCard.detail})
                  {calendarCards.length > 1 ? ` - +${calendarCards.length - 1} more` : ""}
                </p>
              ) : (
                <p>No schedule pressure right now.</p>
              )}
            </div>
          </>
        ) : null}
      </section>

      <section className={`dashboard-section ${hasBlockingDecisions ? "dashboard-secondary-muted" : ""}`} id="dashboard-email-debriefing">
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

      <section className={`dashboard-section ${hasBlockingDecisions ? "dashboard-secondary-muted" : ""}`} id="dashboard-recent-alerts">
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
