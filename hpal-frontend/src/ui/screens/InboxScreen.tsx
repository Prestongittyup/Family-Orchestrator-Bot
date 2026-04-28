import React from "react";
import { useNavigate } from "react-router-dom";
import type { CalendarEventSummary, EmailDetail, Notification, RequestIdentityContext } from "../../api/contracts";
import { productSurfaceClient } from "../../api/productSurfaceClient";
import { useRuntimeStore } from "../../runtime/store";
import { selectNotifications } from "../../runtime/selectors";
import {
  buildDismissedEmailKey,
  readDismissedEmailMap,
  writeDismissedEmailMap,
  type DismissedEmailMap,
} from "../utils/emailDismissals";
import { SyncStatusPill } from "../components/SyncStatusPill";

const EMAIL_SYNC_STATUS_STORAGE_KEY = "hpal-email-sync-status";
const EMAIL_SYNC_STATUS_EVENT = "hpal:email-sync-status";
const GOOGLE_CONNECTED_USER_STORAGE_KEY = "hpal-google-user-id";
const GOOGLE_CONNECTED_HOUSEHOLD_STORAGE_KEY = "hpal-google-household-id";
const ASSISTANT_PENDING_PROMPT_KEY = "hpal.assistant.pending_prompt";
const ASSISTANT_PENDING_AUTOSEND_KEY = "hpal.assistant.pending_autosend";
const INBOX_SYNC_MAX_RESULTS = 100;
const EMAIL_NOTIFICATION_PREFIX = "notif:email_summary:";

type InboxSyncStatusPayload = {
  status: "syncing" | "success" | "failed";
  attempted_at: string;
  last_success_at: string | null;
  max_results: number;
  processed_count: number;
  ignored_count: number;
  failed_count: number;
  message: string;
};

type InboxFilterMode = "smart_focus" | "all" | "actionable" | "calendar" | "finance" | "promotions" | "dismissed";

type InboxSignalTag = "actionable" | "calendar" | "finance" | "promotions";

type EnrichedEmailDebriefItem = {
  notification: Notification;
  emailId: string | null;
  tags: InboxSignalTag[];
  focusScore: number;
  reason: string;
};

type InboxDebriefRow = EnrichedEmailDebriefItem & {
  dismissalKey: string;
  dismissedTag: string | null;
  dismissedAt: string | null;
};

const EMAIL_SIGNAL_KEYWORDS: Record<InboxSignalTag, string[]> = {
  actionable: [
    "action required",
    "respond",
    "reply needed",
    "follow up",
    "approval",
    "confirm",
    "deadline",
    "due",
    "urgent",
    "asap",
  ],
  calendar: [
    "calendar",
    "meeting",
    "appointment",
    "schedule",
    "reschedule",
    "invite",
    "event",
    "time",
    "tomorrow",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
  ],
  finance: [
    "invoice",
    "bill",
    "payment",
    "receipt",
    "statement",
    "balance",
    "renewal",
    "subscription",
    "charge",
    "refund",
    "tax",
  ],
  promotions: [
    "sale",
    "promo",
    "discount",
    "offer",
    "deal",
    "newsletter",
    "unsubscribe",
    "marketing",
    "limited time",
    "special offer",
  ],
};

const tagLabel = (tag: InboxSignalTag): string => {
  if (tag === "actionable") {
    return "Action";
  }
  if (tag === "calendar") {
    return "Calendar";
  }
  if (tag === "finance") {
    return "Finance";
  }
  return "Promotion";
};

const parseSignalCount = (message: string, label: "action item" | "calendar candidate"): number => {
  const matcher = new RegExp(`(\\d+)\\s+${label}s?`, "i");
  const match = message.match(matcher);
  if (!match) {
    return 0;
  }
  const parsed = Number.parseInt(match[1], 10);
  return Number.isFinite(parsed) ? parsed : 0;
};

const classifyEmailDebriefItem = (notification: Notification): EnrichedEmailDebriefItem => {
  const emailId = extractEmailId(notification);
  const fullText = `${notification.title} ${notification.message}`.toLowerCase();
  const actionItemCount = parseSignalCount(notification.message, "action item");
  const calendarCandidateCount = parseSignalCount(notification.message, "calendar candidate");

  let score = notification.level === "critical" ? 70 : notification.level === "warning" ? 45 : 15;
  const reasons: string[] = [];
  const tags = new Set<InboxSignalTag>();

  if (actionItemCount > 0) {
    tags.add("actionable");
    const suffix = actionItemCount === 1 ? "" : "s";
    reasons.push(`${actionItemCount} action item${suffix}`);
    score += actionItemCount * 24;
  }

  if (calendarCandidateCount > 0) {
    tags.add("calendar");
    const suffix = calendarCandidateCount === 1 ? "" : "s";
    reasons.push(`${calendarCandidateCount} calendar candidate${suffix}`);
    score += calendarCandidateCount * 22;
  }

  const keywordWeight: Record<InboxSignalTag, number> = {
    actionable: 18,
    calendar: 14,
    finance: 13,
    promotions: -20,
  };

  for (const [tag, keywords] of Object.entries(EMAIL_SIGNAL_KEYWORDS) as Array<[InboxSignalTag, string[]]>) {
    const hitCount = keywords.reduce((count, keyword) => (fullText.includes(keyword) ? count + 1 : count), 0);
    if (hitCount === 0) {
      continue;
    }

    tags.add(tag);
    score += keywordWeight[tag] * hitCount;
    reasons.push(`${hitCount} ${tag} signal${hitCount === 1 ? "" : "s"}`);
  }

  if (tags.has("promotions") && !tags.has("actionable") && !tags.has("calendar") && !tags.has("finance")) {
    score -= 20;
  }

  const normalizedScore = Math.max(0, Math.min(100, score));
  const fallbackReason = notification.level === "info" ? "general update" : "high-priority signal";

  return {
    notification,
    emailId,
    tags: Array.from(tags).sort(),
    focusScore: normalizedScore,
    reason: reasons.length > 0 ? reasons.slice(0, 3).join(", ") : fallbackReason,
  };
};

const deriveDismissTag = (item: EnrichedEmailDebriefItem): string => {
  if (item.tags.includes("actionable")) {
    return "actionable";
  }
  if (item.tags.includes("calendar")) {
    return "calendar";
  }
  if (item.tags.includes("finance")) {
    return "finance";
  }
  if (item.tags.includes("promotions")) {
    return "promotions";
  }
  if (item.focusScore >= 45) {
    return "priority";
  }
  return "general";
};

const filterMatches = (item: EnrichedEmailDebriefItem, filter: InboxFilterMode): boolean => {
  if (filter === "dismissed") {
    return false;
  }
  if (filter === "all") {
    return true;
  }
  if (filter === "smart_focus") {
    return item.focusScore >= 45 && !item.tags.includes("promotions");
  }
  if (filter === "actionable") {
    return item.tags.includes("actionable");
  }
  if (filter === "calendar") {
    return item.tags.includes("calendar");
  }
  if (filter === "finance") {
    return item.tags.includes("finance");
  }
  return item.tags.includes("promotions");
};

const debriefItemIdentifier = (item: EnrichedEmailDebriefItem): string =>
  (item.emailId || item.notification.notification_id || "").trim().toLowerCase();

const describeCalendarAddOutcome = (addedCount: number, skippedCount: number): string => {
  const parts: string[] = [];
  if (addedCount > 0) {
    parts.push(`${addedCount} calendar event${addedCount === 1 ? "" : "s"} added`);
  }
  if (skippedCount > 0) {
    parts.push(`${skippedCount} skipped`);
  }

  return parts.length > 0 ? `${parts.join(", ")}.` : "No calendar events were added.";
};

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

const formatOptionalDateTime = (value: string | null | undefined): string => {
  const raw = (value || "").trim();
  if (!raw) {
    return "";
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return raw;
  }

  return parsed.toLocaleString();
};

const normalizeComparableText = (value: string): string =>
  value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const toDateKey = (value: string | null | undefined): string | null => {
  const raw = (value || "").trim();
  if (!raw) {
    return null;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  const year = parsed.getFullYear();
  const month = `${parsed.getMonth() + 1}`.padStart(2, "0");
  const day = `${parsed.getDate()}`.padStart(2, "0");
  return `${year}-${month}-${day}`;
};

const findMatchingCalendarEventId = (
  candidate: EmailDetail["calendar_candidates"][number],
  events: CalendarEventSummary[],
): string | null => {
  const candidateTitle = normalizeComparableText(candidate.title || "");
  if (!candidateTitle) {
    return null;
  }

  const candidateDateKey = toDateKey(candidate.time_hint_local || candidate.time_hint || null);

  for (const event of events) {
    const eventTitle = normalizeComparableText(event.title || "");
    if (!eventTitle || eventTitle !== candidateTitle) {
      continue;
    }

    if (candidateDateKey) {
      const eventDateKey = toDateKey(event.start);
      if (eventDateKey && eventDateKey !== candidateDateKey) {
        continue;
      }
    }

    return event.event_id;
  }

  return null;
};

const toCalendarStartTime = (candidate: EmailDetail["calendar_candidates"][number]): string | null => {
  const raw = (candidate.time_hint_local || candidate.time_hint || "").trim();
  if (!raw) {
    return null;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return `${raw}T09:00:00`;
  }

  if (raw.includes("T")) {
    return raw;
  }

  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return parsed.toISOString();
};

const readLocalStorageValue = (key: string): string => {
  try {
    return localStorage.getItem(key) || "";
  } catch {
    return "";
  }
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

  const ignoredSuffix = status.ignored_count > 0 ? ` (${status.ignored_count} ignored)` : "";
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

export const InboxScreen: React.FC = () => {
  const navigate = useNavigate();
  const runtimeState = useRuntimeStore((state) => state.runtimeState);
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
  const [inboxFilter, setInboxFilter] = React.useState<InboxFilterMode>("all");
  const [emailSyncStatus, setEmailSyncStatus] = React.useState<InboxSyncStatusPayload | null>(() =>
    readInboxSyncStatus(),
  );
  const [selectedCalendarCandidateIndexes, setSelectedCalendarCandidateIndexes] = React.useState<number[]>([]);
  const [calendarActionLoading, setCalendarActionLoading] = React.useState(false);
  const [calendarActionMessage, setCalendarActionMessage] = React.useState<string | null>(null);
  const [calendarActionError, setCalendarActionError] = React.useState<string | null>(null);
  const [manuallyAddedCandidateKeys, setManuallyAddedCandidateKeys] = React.useState<string[]>([]);
  const [dismissedEmailMap, setDismissedEmailMap] = React.useState<DismissedEmailMap>(() => readDismissedEmailMap());

  React.useEffect(() => {
    writeDismissedEmailMap(dismissedEmailMap);
  }, [dismissedEmailMap]);

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
    return <section className="screen-panel">Loading inbox...</section>;
  }

  const detailIdentity: RequestIdentityContext | undefined =
    activeHousehold && activeUser && deviceContext && sessionToken
      ? {
          household_id: activeHousehold.household_id,
          user_id: activeUser.user_id,
          device_id: deviceContext.device_id,
          session_token: sessionToken,
        }
      : undefined;

  const householdId = runtimeState.snapshot.family.family_id;
  const notifications = selectNotifications(runtimeState);
  const emailDebriefNotifications = notifications.filter(isEmailDebriefNotification);
  const emailDebriefItems = emailDebriefNotifications.map(classifyEmailDebriefItem);
  const emailDebriefRows: InboxDebriefRow[] = emailDebriefItems.map((item) => {
    const itemKey = debriefItemIdentifier(item);
    const dismissalKey = buildDismissedEmailKey(householdId, itemKey);
    const dismissed = dismissedEmailMap[dismissalKey];

    return {
      ...item,
      dismissalKey,
      dismissedTag: dismissed ? dismissed.tag : null,
      dismissedAt: dismissed ? dismissed.dismissed_at : null,
    };
  });

  const dismissedEmailDebriefItems = emailDebriefRows.filter((item) => item.dismissedAt !== null);
  const activeEmailDebriefItems = emailDebriefRows.filter((item) => item.dismissedAt === null);
  const visibleEmailDebriefItems = (
    inboxFilter === "dismissed"
      ? dismissedEmailDebriefItems
      : activeEmailDebriefItems.filter((item) => filterMatches(item, inboxFilter))
  )
    .sort((left, right) => {
      if (right.focusScore !== left.focusScore) {
        return right.focusScore - left.focusScore;
      }
      return left.notification.notification_id.localeCompare(right.notification.notification_id);
    });
  const smartFocusCount = activeEmailDebriefItems.filter((item) => filterMatches(item, "smart_focus")).length;
  const actionableCount = activeEmailDebriefItems.filter((item) => item.tags.includes("actionable")).length;
  const calendarCount = activeEmailDebriefItems.filter((item) => item.tags.includes("calendar")).length;
  const financeCount = activeEmailDebriefItems.filter((item) => item.tags.includes("finance")).length;
  const promotionsCount = activeEmailDebriefItems.filter((item) => item.tags.includes("promotions")).length;
  const dismissedCount = dismissedEmailDebriefItems.length;

  const calendarCandidateStatusByIndex = React.useMemo(() => {
    const status = new Map<number, { alreadyAdded: boolean; eventId: string | null }>();
    if (!selectedEmailDetail) {
      return status;
    }

    const manualSet = new Set(manuallyAddedCandidateKeys);
    selectedEmailDetail.calendar_candidates.forEach((candidate, index) => {
      const candidateKey = `${selectedEmailDetail.email_id}:${index}`;
      if (manualSet.has(candidateKey)) {
        status.set(index, { alreadyAdded: true, eventId: null });
        return;
      }

      if (selectedEmailDetail.calendar_event_id && index === 0) {
        status.set(index, { alreadyAdded: true, eventId: selectedEmailDetail.calendar_event_id });
        return;
      }

      const matchedEventId = findMatchingCalendarEventId(candidate, runtimeState.snapshot.calendar.events);
      if (matchedEventId) {
        status.set(index, { alreadyAdded: true, eventId: matchedEventId });
        return;
      }

      status.set(index, { alreadyAdded: false, eventId: null });
    });

    return status;
  }, [manuallyAddedCandidateKeys, runtimeState.snapshot.calendar.events, selectedEmailDetail]);

  const openAssistantWorkflow = (prompt: string) => {
    try {
      localStorage.setItem(ASSISTANT_PENDING_PROMPT_KEY, prompt);
      localStorage.setItem(ASSISTANT_PENDING_AUTOSEND_KEY, "1");
    } catch {
      // Best-effort fallback for assistant quick links.
    }

    const params = new URLSearchParams({
      prompt,
      autosend: "1",
    });
    navigate(`/assistant?${params.toString()}`);
  };

  const onDismissEmailItem = (item: InboxDebriefRow) => {
    const itemKey = debriefItemIdentifier(item);
    if (!itemKey) {
      return;
    }

    const nextRecord = {
      tag: deriveDismissTag(item),
      dismissed_at: new Date().toISOString(),
      household_id: householdId,
      item_key: itemKey,
      notification_id: item.notification.notification_id,
    };

    setDismissedEmailMap((current) => ({
      ...current,
      [item.dismissalKey]: nextRecord,
    }));

    if (selectedEmailId && item.emailId && selectedEmailId === item.emailId) {
      setSelectedEmailId(null);
      setSelectedEmailDetail(null);
      setEmailDetailError(null);
      setSelectedCalendarCandidateIndexes([]);
      setCalendarActionMessage(null);
      setCalendarActionError(null);
      setManuallyAddedCandidateKeys([]);
    }
  };

  const onRestoreDismissedEmailItem = (item: InboxDebriefRow) => {
    setDismissedEmailMap((current) => {
      if (!current[item.dismissalKey]) {
        return current;
      }

      const next = { ...current };
      delete next[item.dismissalKey];
      return next;
    });
  };

  const onClearDismissedEmails = () => {
    setDismissedEmailMap({});
    if (inboxFilter === "dismissed") {
      setInboxFilter("all");
    }
  };

  const onToggleEmailDetail = async (emailId: string) => {
    if (!emailId) {
      return;
    }

    if (selectedEmailId === emailId && detailLoadingId === null) {
      setSelectedEmailId(null);
      setSelectedEmailDetail(null);
      setEmailDetailError(null);
      setSelectedCalendarCandidateIndexes([]);
      setCalendarActionMessage(null);
      setCalendarActionError(null);
      setManuallyAddedCandidateKeys([]);
      return;
    }

    setSelectedEmailId(emailId);
    setEmailDetailError(null);
    setCalendarActionMessage(null);
    setCalendarActionError(null);
    setSelectedCalendarCandidateIndexes([]);
    setManuallyAddedCandidateKeys([]);
    setDetailLoadingId(emailId);
    try {
      const detail = await productSurfaceClient.fetchEmailDetail(
        runtimeState.snapshot.family.family_id,
        emailId,
        detailIdentity,
      );
      setSelectedEmailDetail(detail);
      setSelectedCalendarCandidateIndexes(detail.calendar_candidates.map((_, index) => index));
    } catch {
      setSelectedEmailDetail(null);
      setEmailDetailError("Unable to load email details right now.");
      setSelectedCalendarCandidateIndexes([]);
    } finally {
      setDetailLoadingId(null);
    }
  };

  const onToggleCalendarCandidate = (index: number) => {
    setSelectedCalendarCandidateIndexes((current) => {
      if (current.includes(index)) {
        return current.filter((value) => value !== index);
      }
      return [...current, index].sort((left, right) => left - right);
    });
  };

  type CalendarActionContext = {
    householdId: string;
    userId: string;
    identity: RequestIdentityContext;
  };

  const resolveCalendarActionContext = (): CalendarActionContext | null => {
    if (!detailIdentity) {
      setCalendarActionMessage(null);
      setCalendarActionError("Unable to confirm your identity for calendar updates. Reconnect and try again.");
      return null;
    }

    const calendarHouseholdId = (activeHousehold?.household_id || runtimeState.snapshot.family.family_id || "").trim();
    if (!calendarHouseholdId) {
      setCalendarActionMessage(null);
      setCalendarActionError("Missing household context. Refresh and try again.");
      return null;
    }

    return {
      householdId: calendarHouseholdId,
      userId: (activeUser?.user_id || "user-admin").trim() || "user-admin",
      identity: detailIdentity,
    };
  };

  const addCalendarCandidatesForDetail = async (
    detail: EmailDetail,
    targetIndexes: number[],
    context: CalendarActionContext,
  ): Promise<{ addedCount: number; skippedCount: number; newlyAddedKeys: string[] }> => {
    const manualSet = new Set(manuallyAddedCandidateKeys);
    let addedCount = 0;
    let skippedCount = 0;
    const newlyAddedKeys: string[] = [];

    for (const index of targetIndexes.sort((left, right) => left - right)) {
      const candidate = detail.calendar_candidates[index];
      if (!candidate) {
        skippedCount += 1;
        continue;
      }

      const candidateKey = `${detail.email_id}:${index}`;
      const alreadyAdded =
        manualSet.has(candidateKey)
        || (Boolean((detail.calendar_event_id || "").trim()) && index === 0)
        || findMatchingCalendarEventId(candidate, runtimeState.snapshot.calendar.events) !== null;
      if (alreadyAdded) {
        skippedCount += 1;
        continue;
      }

      const title = (candidate.title || "").trim();
      if (!title) {
        skippedCount += 1;
        continue;
      }

      await productSurfaceClient.createCalendarEvent(
        context.householdId,
        {
          user_id: context.userId,
          title,
          description: `Added from email: ${detail.subject}`,
          start_time: toCalendarStartTime(candidate),
          duration_minutes: 60,
          recurrence: "none",
        },
        context.identity,
      );

      addedCount += 1;
      newlyAddedKeys.push(candidateKey);
    }

    return {
      addedCount,
      skippedCount,
      newlyAddedKeys,
    };
  };

  const onAddCalendarCandidates = async (mode: "all" | "selected") => {
    if (!selectedEmailDetail) {
      return;
    }

    const context = resolveCalendarActionContext();
    if (!context) {
      return;
    }

    const selectedSet = new Set(selectedCalendarCandidateIndexes);
    const targetIndexes = selectedEmailDetail.calendar_candidates
      .map((_, index) => index)
      .filter((index) => (mode === "all" ? true : selectedSet.has(index)));

    if (targetIndexes.length === 0) {
      setCalendarActionMessage(null);
      setCalendarActionError("Select at least one calendar candidate first.");
      return;
    }

    setCalendarActionLoading(true);
    setCalendarActionMessage(null);
    setCalendarActionError(null);

    try {
      const result = await addCalendarCandidatesForDetail(
        selectedEmailDetail,
        targetIndexes,
        context,
      );

      if (result.newlyAddedKeys.length > 0) {
        setManuallyAddedCandidateKeys((current) =>
          Array.from(new Set([...current, ...result.newlyAddedKeys])),
        );
      }

      if (result.addedCount > 0) {
        await forceReconcile();
      }

      setCalendarActionMessage(describeCalendarAddOutcome(result.addedCount, result.skippedCount));
      setCalendarActionError(null);
    } catch (error) {
      const raw = String(error || "");
      let message = "Unable to add one or more calendar events right now.";
      if (raw.includes("calendar_create_failed:401") || raw.includes("calendar_create_failed:403")) {
        message = "Calendar permissions are missing. Reconnect your account and try again.";
      }
      setCalendarActionMessage(null);
      setCalendarActionError(message);
    } finally {
      setCalendarActionLoading(false);
    }
  };

  const onQuickAddAllToCalendar = async (emailId: string) => {
    if (!emailId) {
      return;
    }

    const context = resolveCalendarActionContext();
    if (!context) {
      return;
    }

    setCalendarActionLoading(true);
    setCalendarActionMessage(null);
    setCalendarActionError(null);
    setDetailLoadingId(emailId);

    try {
      const detail = selectedEmailDetail && selectedEmailDetail.email_id === emailId
        ? selectedEmailDetail
        : await productSurfaceClient.fetchEmailDetail(
            runtimeState.snapshot.family.family_id,
            emailId,
            detailIdentity,
          );

      const targetIndexes = detail.calendar_candidates.map((_, index) => index);
      setSelectedEmailId(emailId);
      setSelectedEmailDetail(detail);
      setSelectedCalendarCandidateIndexes(targetIndexes);

      if (targetIndexes.length === 0) {
        setCalendarActionMessage(null);
        setCalendarActionError("No calendar candidates were extracted for this email.");
        return;
      }

      const result = await addCalendarCandidatesForDetail(detail, targetIndexes, context);
      if (result.newlyAddedKeys.length > 0) {
        setManuallyAddedCandidateKeys((current) =>
          Array.from(new Set([...current, ...result.newlyAddedKeys])),
        );
      }
      if (result.addedCount > 0) {
        await forceReconcile();
      }

      setCalendarActionMessage(describeCalendarAddOutcome(result.addedCount, result.skippedCount));
      setCalendarActionError(null);
    } catch (error) {
      const raw = String(error || "");
      let message = "Unable to add one or more calendar events right now.";
      if (raw.includes("calendar_create_failed:401") || raw.includes("calendar_create_failed:403")) {
        message = "Calendar permissions are missing. Reconnect your account and try again.";
      }
      if (raw.includes("email_detail_failed")) {
        message = "Unable to load email details for calendar conversion right now.";
      }
      setCalendarActionMessage(null);
      setCalendarActionError(message);
    } finally {
      setDetailLoadingId(null);
      setCalendarActionLoading(false);
    }
  };

  const onAskCalendarSelectionWorkflow = () => {
    if (!selectedEmailDetail) {
      return;
    }

    if (selectedEmailDetail.calendar_candidates.length === 0) {
      openAssistantWorkflow(
        `This email (subject: ${selectedEmailDetail.subject}) has no extracted calendar candidates. Please summarize whether I should still create calendar events manually and suggest options.`,
      );
      return;
    }

    const candidateLines = selectedEmailDetail.calendar_candidates
      .map((candidate, index) => {
        const hint = formatOptionalDateTime(candidate.time_hint_local || candidate.time_hint || null);
        const confidence = typeof candidate.confidence === "number"
          ? `, confidence ${(candidate.confidence * 100).toFixed(0)}%`
          : "";
        return `${index + 1}. ${candidate.title}${hint ? ` (${hint})` : ""}${confidence}`;
      })
      .join("\n");

    openAssistantWorkflow(
      `Email subject: ${selectedEmailDetail.subject}\n` +
      `Summary: ${selectedEmailDetail.summary}\n\n` +
      `Calendar candidates:\n${candidateLines}\n\n` +
      `Ask me this exact question: \"Do you want me to add all of this to your calendar or only some of it?\" ` +
      `Wait for my choice, then confirm what was added and what was skipped.`,
    );
  };

  const onGenerateTaskWorkflow = () => {
    if (!selectedEmailDetail) {
      return;
    }

    const actionLines = selectedEmailDetail.action_items.length > 0
      ? selectedEmailDetail.action_items
          .map((item, index) => {
            const due = formatOptionalDateTime(item.due_hint_local || item.due_hint || null);
            return `${index + 1}. ${item.title}${due ? ` (due hint: ${due})` : ""}`;
          })
          .join("\n")
      : "No explicit action items extracted.";

    openAssistantWorkflow(
      `Use this email debrief to create tasks and reminders.\n` +
      `Subject: ${selectedEmailDetail.subject}\n` +
      `Summary: ${selectedEmailDetail.summary}\n` +
      `Action items:\n${actionLines}\n\n` +
      `Please: (1) turn required items into tasks, (2) suggest reminder timing, (3) ask if Friday/weekend purchase reminders should be created, ` +
      `and (4) ask whether purchase tasks should be placed in free calendar windows.`,
    );
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
          ignored_count: 0,
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
          ignored_count: 0,
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
        ignored_count: 0,
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

      const processedCount = Number(result.processed_count || 0);
      const ignoredCount = Number(result.ignored_count || 0);
      const failedCount = Number(result.failed_count || 0);
      let successMessage = `Inbox sync complete: ${processedCount} actionable message(s) ingested`;
      if (ignoredCount > 0) {
        successMessage += `, ${ignoredCount} low-priority message(s) ignored`;
      }
      if (failedCount > 0) {
        successMessage += `, ${failedCount} failed`;
      }
      successMessage += ".";

      if (processedCount === 0 && ignoredCount === 0 && failedCount === 0) {
        successMessage = `Inbox sync completed for latest ${INBOX_SYNC_MAX_RESULTS} emails, but no eligible messages were returned.`;
      }

      setEmailSyncMessage(successMessage);
      setEmailSyncStatus(
        persistInboxSyncStatus({
          status: "success",
          attempted_at: attemptedAt,
          last_success_at: new Date().toISOString(),
          max_results: INBOX_SYNC_MAX_RESULTS,
          processed_count: processedCount,
          ignored_count: ignoredCount,
          failed_count: failedCount,
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
          ignored_count: 0,
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
      <header className="screen-header">
        <div>
          <h2>Inbox</h2>
          <p>Review synced email summaries and open extracted details.</p>
        </div>
        <SyncStatusPill status={runtimeState.sync_status} />
      </header>

      <section className="dashboard-section" aria-label="Email sync status">
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
        </div>
        {emailSyncMessage ? <p>{emailSyncMessage}</p> : null}
        {emailSyncError ? <p className="error-text">{emailSyncError}</p> : null}
      </section>

      <section className="dashboard-section" aria-label="Email debrief items">
        <div className="dashboard-section-header">
          <h3>Inbox Intelligence</h3>
          <span className="dashboard-highlight">{visibleEmailDebriefItems.length} / {activeEmailDebriefItems.length} active item(s)</span>
        </div>
        <p>Extracted inbox signals for prioritization, follow-ups, and calendar conversion.</p>

        <div className="inbox-filter-tabs" role="tablist" aria-label="Inbox filters">
          <button
            type="button"
            role="tab"
            aria-selected={inboxFilter === "smart_focus"}
            className={`dashboard-detail-button inbox-filter-tab${inboxFilter === "smart_focus" ? " inbox-filter-tab-active" : ""}`}
            onClick={() => setInboxFilter("smart_focus")}
          >
            Smart Focus ({smartFocusCount})
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={inboxFilter === "actionable"}
            className={`dashboard-detail-button inbox-filter-tab${inboxFilter === "actionable" ? " inbox-filter-tab-active" : ""}`}
            onClick={() => setInboxFilter("actionable")}
          >
            Action Required ({actionableCount})
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={inboxFilter === "calendar"}
            className={`dashboard-detail-button inbox-filter-tab${inboxFilter === "calendar" ? " inbox-filter-tab-active" : ""}`}
            onClick={() => setInboxFilter("calendar")}
          >
            Calendar ({calendarCount})
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={inboxFilter === "finance"}
            className={`dashboard-detail-button inbox-filter-tab${inboxFilter === "finance" ? " inbox-filter-tab-active" : ""}`}
            onClick={() => setInboxFilter("finance")}
          >
            Finance ({financeCount})
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={inboxFilter === "promotions"}
            className={`dashboard-detail-button inbox-filter-tab${inboxFilter === "promotions" ? " inbox-filter-tab-active" : ""}`}
            onClick={() => setInboxFilter("promotions")}
          >
            Promotions ({promotionsCount})
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={inboxFilter === "all"}
            className={`dashboard-detail-button inbox-filter-tab${inboxFilter === "all" ? " inbox-filter-tab-active" : ""}`}
            onClick={() => setInboxFilter("all")}
          >
            All ({activeEmailDebriefItems.length})
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={inboxFilter === "dismissed"}
            className={`dashboard-detail-button inbox-filter-tab${inboxFilter === "dismissed" ? " inbox-filter-tab-active" : ""}`}
            onClick={() => setInboxFilter("dismissed")}
          >
            Dismissed ({dismissedCount})
          </button>
        </div>
        {dismissedCount > 0 ? (
          <div className="dashboard-list-controls inbox-filter-clear">
            <button
              type="button"
              className="dashboard-detail-button"
              onClick={onClearDismissedEmails}
            >
              Clear dismissed
            </button>
          </div>
        ) : null}

        {visibleEmailDebriefItems.length === 0 ? (
          <p className="empty-text">
            {emailDebriefItems.length === 0
              ? `No email debriefing items yet. Sync your inbox to pull the latest ${INBOX_SYNC_MAX_RESULTS} messages.`
              : inboxFilter === "dismissed"
              ? "No dismissed email items yet."
              : "No email items match the selected filter right now."}
          </p>
        ) : (
          <ul className="list-panel dashboard-list-panel">
            {visibleEmailDebriefItems.map((item) => {
              const { notification, emailId, tags, focusScore, reason } = item;
              const isDismissed = item.dismissedAt !== null;

              return (
                <li key={notification.notification_id}>
                  <div className="inbox-item-layout">
                    <div className="inbox-item-content">
                      <div className="dashboard-list-title-row">
                        <strong>{notification.title}</strong>
                        <span className={`level-pill level-${notification.level}`}>{notification.level}</span>
                      </div>
                      <div className="inbox-signal-row">
                        <span className="inbox-focus-score">Focus score {focusScore}</span>
                        {tags.map((tag) => (
                          <span key={`${notification.notification_id}:${tag}`} className="inbox-signal-chip">
                            {tagLabel(tag)}
                          </span>
                        ))}
                        {isDismissed ? (
                          <span className="inbox-dismissed-chip">
                            Dismissed: {item.dismissedTag || "general"}
                          </span>
                        ) : null}
                      </div>
                      <p className="task-meta inbox-signal-reason">Signals: {reason}</p>
                      <p>{notification.message}</p>
                      {isDismissed && item.dismissedAt ? (
                        <p className="task-meta">Dismissed on {formatDateTime(item.dismissedAt)}</p>
                      ) : null}

                      {!isDismissed && selectedEmailId === emailId && emailId ? (
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
                              {selectedEmailDetail.task_title || selectedEmailDetail.calendar_event_id ? (
                                <p className="task-meta">
                                  {selectedEmailDetail.task_title
                                    ? `Task created: ${selectedEmailDetail.task_title}. `
                                    : ""}
                                  {selectedEmailDetail.calendar_event_id
                                    ? "This email already added an event to your calendar."
                                    : ""}
                                </p>
                              ) : null}

                              {selectedEmailDetail.action_items.length > 0 ? (
                                <div>
                                  <strong>Action items</strong>
                                  <ul className="email-detail-list">
                                    {selectedEmailDetail.action_items.map((actionItem, index) => {
                                      const dueLabel = formatOptionalDateTime(
                                        actionItem.due_hint_local || actionItem.due_hint || null,
                                      );
                                      return (
                                        <li key={`${selectedEmailDetail.email_id}:action:${index}`}>
                                          {actionItem.title}
                                          {dueLabel ? ` (due hint: ${dueLabel})` : ""}
                                        </li>
                                      );
                                    })}
                                  </ul>
                                </div>
                              ) : null}

                              {selectedEmailDetail.calendar_candidates.length > 0 ? (
                                <div className="email-detail-candidates">
                                  <strong>Calendar candidates</strong>
                                  <div className="email-detail-candidate-actions">
                                    <button
                                      type="button"
                                      className="dashboard-detail-button"
                                      disabled={calendarActionLoading}
                                      onClick={() => void onAddCalendarCandidates("all")}
                                    >
                                      {calendarActionLoading ? "Adding..." : "Add all to calendar"}
                                    </button>
                                    <button
                                      type="button"
                                      className="dashboard-detail-button"
                                      disabled={calendarActionLoading || selectedCalendarCandidateIndexes.length === 0}
                                      onClick={() => void onAddCalendarCandidates("selected")}
                                    >
                                      Add selected
                                    </button>
                                    <button
                                      type="button"
                                      className="dashboard-detail-button"
                                      onClick={onAskCalendarSelectionWorkflow}
                                    >
                                      Ask all-or-some question
                                    </button>
                                  </div>

                                  {calendarActionMessage ? <p className="task-meta">{calendarActionMessage}</p> : null}
                                  {calendarActionError ? <p className="error-text">{calendarActionError}</p> : null}

                                  <ul className="email-detail-list email-detail-candidate-list">
                                    {selectedEmailDetail.calendar_candidates.map((candidate, index) => {
                                      const status = calendarCandidateStatusByIndex.get(index) || {
                                        alreadyAdded: false,
                                        eventId: null,
                                      };
                                      const hintLabel = formatOptionalDateTime(candidate.time_hint_local || candidate.time_hint || null);
                                      const confidenceLabel = typeof candidate.confidence === "number"
                                        ? `${(candidate.confidence * 100).toFixed(0)}% confidence`
                                        : "";

                                      return (
                                        <li key={`${selectedEmailDetail.email_id}:calendar:${index}`} className="email-detail-candidate-item">
                                          <label className="email-detail-candidate-select">
                                            <input
                                              type="checkbox"
                                              checked={selectedCalendarCandidateIndexes.includes(index)}
                                              disabled={calendarActionLoading || status.alreadyAdded}
                                              onChange={() => onToggleCalendarCandidate(index)}
                                            />
                                            <span>{candidate.title || `Candidate ${index + 1}`}</span>
                                          </label>
                                          <span className={status.alreadyAdded ? "email-candidate-pill email-candidate-pill-added" : "email-candidate-pill email-candidate-pill-pending"}>
                                            {status.alreadyAdded ? "Already on calendar" : "Ready to add"}
                                          </span>
                                          {hintLabel ? <p className="task-meta">Time hint: {hintLabel}</p> : null}
                                          {confidenceLabel ? <p className="task-meta">{confidenceLabel}</p> : null}
                                          {status.eventId ? <p className="task-meta">Event ID: {status.eventId}</p> : null}
                                        </li>
                                      );
                                    })}
                                  </ul>
                                </div>
                              ) : (
                                <p className="task-meta">No calendar candidates were extracted from this email.</p>
                              )}

                              <div className="dashboard-list-controls">
                                <button
                                  type="button"
                                  className="dashboard-detail-button"
                                  onClick={onGenerateTaskWorkflow}
                                >
                                  Generate Tasks + Reminders
                                </button>
                              </div>
                              {selectedEmailDetail.body || selectedEmailDetail.body_excerpt ? (
                                <div className="email-detail-body">
                                  <strong>Email body</strong>
                                  <p>{selectedEmailDetail.body || selectedEmailDetail.body_excerpt}</p>
                                </div>
                              ) : null}
                            </>
                          ) : (
                            <p className="empty-text">No detailed payload found for this email.</p>
                          )}
                        </div>
                      ) : null}

                      {!isDismissed && !emailId ? (
                        <p className="task-meta">Detailed view is not available for this debrief item.</p>
                      ) : null}
                    </div>

                    <aside className="inbox-item-actions" aria-label="Email item actions">
                      {!isDismissed ? (
                        <>
                          {emailId ? (
                            <>
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
                                disabled={calendarActionLoading}
                                onClick={() => void onQuickAddAllToCalendar(emailId)}
                              >
                                {calendarActionLoading && selectedEmailId === emailId ? "Adding..." : "Add all to calendar"}
                              </button>
                            </>
                          ) : null}
                          <button
                            type="button"
                            className="dashboard-detail-button"
                            onClick={() => onDismissEmailItem(item)}
                          >
                            Dismiss
                          </button>
                        </>
                      ) : (
                        <button
                          type="button"
                          className="dashboard-detail-button"
                          onClick={() => onRestoreDismissedEmailItem(item)}
                        >
                          Restore
                        </button>
                      )}
                    </aside>
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </section>
    </section>
  );
};
