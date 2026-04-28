const DISMISSED_EMAIL_STORAGE_KEY = "hpal-dismissed-email-debrief-v1";

export type DismissedEmailRecord = {
  tag: string;
  dismissed_at: string;
  household_id: string;
  item_key: string;
  notification_id: string;
};

export type DismissedEmailMap = Record<string, DismissedEmailRecord>;

const normalizeKeyPart = (value: string): string => value.trim().toLowerCase();

export const buildDismissedEmailKey = (householdId: string, itemKey: string): string =>
  `${normalizeKeyPart(householdId)}::${normalizeKeyPart(itemKey)}`;

export const readDismissedEmailMap = (): DismissedEmailMap => {
  try {
    const raw = localStorage.getItem(DISMISSED_EMAIL_STORAGE_KEY);
    if (!raw) {
      return {};
    }

    const parsed = JSON.parse(raw) as Record<string, unknown>;
    if (!parsed || typeof parsed !== "object") {
      return {};
    }

    const normalized: DismissedEmailMap = {};
    for (const [key, value] of Object.entries(parsed)) {
      if (!value || typeof value !== "object") {
        continue;
      }

      const record = value as Partial<DismissedEmailRecord>;
      const householdId = String(record.household_id || "").trim();
      const itemKey = String(record.item_key || "").trim();
      const notificationId = String(record.notification_id || "").trim();
      const dismissedAt = String(record.dismissed_at || "").trim();
      const tag = String(record.tag || "general").trim() || "general";

      if (!householdId || !itemKey || !notificationId || !dismissedAt) {
        continue;
      }

      normalized[key] = {
        tag,
        dismissed_at: dismissedAt,
        household_id: householdId,
        item_key: itemKey,
        notification_id: notificationId,
      };
    }

    return normalized;
  } catch {
    return {};
  }
};

export const writeDismissedEmailMap = (value: DismissedEmailMap): void => {
  try {
    localStorage.setItem(DISMISSED_EMAIL_STORAGE_KEY, JSON.stringify(value));
  } catch {
    // Ignore storage failures in restricted environments.
  }
};
