const HOUSEHOLD_ID = "test-household-1";
const API_BASE = "http://localhost:8000";

const POLL_INTERVAL_MS = 5000;

const summaryTextEl = document.getElementById("summaryText");
const decisionsListEl = document.getElementById("decisionsList");
const actionsListEl = document.getElementById("actionsList");
const calendarListEl = document.getElementById("calendarList");
const refreshButtonEl = document.getElementById("refreshButton");
const lastUpdatedEl = document.getElementById("lastUpdated");
const updateStatusEl = document.getElementById("updateStatus");

let previousFingerprint = "";
let lastUpdatedAt = 0;
let isFetching = false;

function buildHomeUrl() {
  return `${API_BASE.replace(/\/$/, "")}/home?household_id=${encodeURIComponent(HOUSEHOLD_ID)}`;
}

function stableStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry)).join(",")}]`;
  }

  const keys = Object.keys(value).sort();
  const pairs = keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`);
  return `{${pairs.join(",")}}`;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function setStatus(text) {
  updateStatusEl.textContent = text;
}

function updateLastUpdatedLabel() {
  if (!lastUpdatedAt) {
    lastUpdatedEl.textContent = "Last updated: never";
    return;
  }

  const seconds = Math.max(0, Math.floor((Date.now() - lastUpdatedAt) / 1000));
  const unit = seconds === 1 ? "second" : "seconds";
  lastUpdatedEl.textContent = `Last updated: ${seconds} ${unit} ago`;
}

function clampTextToLines(text, maxLines) {
  const normalized = String(text || "").replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "No summary available.";
  }

  const maxChars = maxLines * 95;
  if (normalized.length <= maxChars) {
    return normalized;
  }

  return `${normalized.slice(0, maxChars - 1).trimEnd()}...`;
}

function renderSummary(data) {
  summaryTextEl.textContent = clampTextToLines(data.summary, 2);
}

function renderPlaceholder(listEl, text) {
  listEl.innerHTML = "";
  const item = document.createElement("li");
  const paragraph = document.createElement("p");
  paragraph.className = "placeholder-text";
  paragraph.textContent = text;
  item.appendChild(paragraph);
  listEl.appendChild(item);
}

function renderDecisions(data) {
  const decisions = safeArray(data.needs_decision).slice(0, 3);
  decisionsListEl.innerHTML = "";

  if (decisions.length === 0) {
    renderPlaceholder(decisionsListEl, "No decisions blocking you.");
    return;
  }

  decisions.forEach((decision, index) => {
    const item = document.createElement("li");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "decision-button";
    button.textContent = String(decision.question || `Decision ${index + 1}`).trim() || `Decision ${index + 1}`;
    button.addEventListener("click", () => {
      console.log("Decision clicked", decision);
    });
    item.appendChild(button);
    decisionsListEl.appendChild(item);
  });
}

function renderActions(data) {
  const actions = safeArray(data.actions).slice(0, 5);
  actionsListEl.innerHTML = "";

  if (actions.length === 0) {
    renderPlaceholder(actionsListEl, "No actions right now.");
    return;
  }

  actions.forEach((action, index) => {
    const item = document.createElement("li");
    const paragraph = document.createElement("p");
    paragraph.className = "action-text";
    paragraph.textContent = String(action.title || `Action ${index + 1}`).trim() || `Action ${index + 1}`;
    item.appendChild(paragraph);
    actionsListEl.appendChild(item);
  });
}

function parseDate(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }

  const date = new Date(raw);
  return Number.isNaN(date.getTime()) ? null : date;
}

function formatTime(value) {
  const date = parseDate(value);
  if (!date) {
    return "Time unknown";
  }

  return date.toLocaleTimeString([], {
    hour: "numeric",
    minute: "2-digit",
  });
}

function selectCalendarRows(calendarRows) {
  const now = new Date();
  const todayStart = new Date(now);
  todayStart.setHours(0, 0, 0, 0);

  const tomorrowStart = new Date(todayStart);
  tomorrowStart.setDate(todayStart.getDate() + 1);

  const urgentWindowEnd = new Date(now);
  urgentWindowEnd.setHours(now.getHours() + 12);

  const sortedRows = calendarRows
    .map((row) => ({ row, start: parseDate(row.start) }))
    .filter((entry) => entry.start)
    .sort((a, b) => a.start.getTime() - b.start.getTime());

  const todayRows = sortedRows.filter((entry) => entry.start >= todayStart && entry.start < tomorrowStart);
  const urgentUpcomingRows = sortedRows.filter((entry) => entry.start >= now && entry.start <= urgentWindowEnd);

  const combined = [];
  const seenIds = new Set();

  [...todayRows, ...urgentUpcomingRows].forEach((entry) => {
    const rowId = String(entry.row.id || "").trim() || `${entry.row.title || "event"}-${entry.start.getTime()}`;
    if (seenIds.has(rowId)) {
      return;
    }
    seenIds.add(rowId);
    combined.push(entry.row);
  });

  if (combined.length > 0) {
    return combined.slice(0, 5);
  }

  return sortedRows.map((entry) => entry.row).slice(0, 5);
}

function renderCalendar(data) {
  const calendarRows = safeArray(data.calendar);
  const selectedRows = selectCalendarRows(calendarRows);
  calendarListEl.innerHTML = "";

  if (selectedRows.length === 0) {
    renderPlaceholder(calendarListEl, "No urgent upcoming events.");
    return;
  }

  selectedRows.forEach((entry, index) => {
    const item = document.createElement("li");
    const paragraph = document.createElement("p");
    paragraph.className = "calendar-text";
    const title = String(entry.title || `Event ${index + 1}`).trim() || `Event ${index + 1}`;
    paragraph.textContent = `${formatTime(entry.start)} - ${title}`;
    item.appendChild(paragraph);
    calendarListEl.appendChild(item);
  });
}

function render(data) {
  renderSummary(data);
  renderDecisions(data);
  renderActions(data);
  renderCalendar(data);
}

async function fetchHome({ manual = false } = {}) {
  if (isFetching) {
    return;
  }

  isFetching = true;
  setStatus(manual ? "Refreshing..." : "Checking for updates...");

  try {
    const response = await fetch(buildHomeUrl(), {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
      cache: "no-store",
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    const fingerprint = stableStringify(data);
    const changed = fingerprint !== previousFingerprint;

    if (changed) {
      render(data);
      previousFingerprint = fingerprint;
      setStatus(manual ? "Updated." : "New update received.");
    } else {
      setStatus("No new updates");
    }

    lastUpdatedAt = Date.now();
    updateLastUpdatedLabel();
  } catch (error) {
    setStatus(`Update failed: ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    isFetching = false;
  }
}

refreshButtonEl.addEventListener("click", () => {
  void fetchHome({ manual: true });
});

setInterval(() => {
  void fetchHome();
}, POLL_INTERVAL_MS);

setInterval(() => {
  updateLastUpdatedLabel();
}, 1000);

void fetchHome();
