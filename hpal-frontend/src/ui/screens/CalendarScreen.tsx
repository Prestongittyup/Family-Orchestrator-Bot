import React from "react";
import { useRuntimeStore } from "../../runtime/store";
import { selectCalendarEvents } from "../../runtime/selectors";
import { SyncStatusPill } from "../components/SyncStatusPill";

type CalendarMonthCell = {
  key: string;
  dayNumber: number;
  inMonth: boolean;
};

const formatLocalDateTime = (raw: string): string => {
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) {
    return raw;
  }
  return parsed.toLocaleString([], {
    dateStyle: "medium",
    timeStyle: "short",
  });
};

const formatLocalTimeRange = (startRaw: string, endRaw: string): string => {
  const start = new Date(startRaw);
  const end = new Date(endRaw);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    return `${startRaw} to ${endRaw}`;
  }

  return `${start.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })} - ${end.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}`;
};

const buildLocalDayKey = (date: Date): string => {
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, "0");
  const day = `${date.getDate()}`.padStart(2, "0");
  return `${year}-${month}-${day}`;
};

const toLocalDayLabel = (date: Date): string =>
  date.toLocaleDateString([], {
    weekday: "long",
    month: "short",
    day: "numeric",
  });

const buildMonthGrid = (anchor: Date): CalendarMonthCell[] => {
  const firstOfMonth = new Date(anchor.getFullYear(), anchor.getMonth(), 1);
  const startOffset = (firstOfMonth.getDay() + 6) % 7;
  const firstCellDate = new Date(firstOfMonth);
  firstCellDate.setDate(firstOfMonth.getDate() - startOffset);

  const cells: CalendarMonthCell[] = [];
  for (let index = 0; index < 42; index += 1) {
    const cellDate = new Date(firstCellDate);
    cellDate.setDate(firstCellDate.getDate() + index);
    cells.push({
      key: buildLocalDayKey(cellDate),
      dayNumber: cellDate.getDate(),
      inMonth: cellDate.getMonth() === anchor.getMonth(),
    });
  }
  return cells;
};

export const CalendarScreen: React.FC = () => {
  const runtimeState = useRuntimeStore((state) => state.runtimeState);
  const createCalendarEvent = useRuntimeStore((state) => state.createCalendarEvent);
  const updateCalendarEvent = useRuntimeStore((state) => state.updateCalendarEvent);
  const deleteCalendarEvent = useRuntimeStore((state) => state.deleteCalendarEvent);
  const activeUser = useRuntimeStore((state) => state.active_user);

  const onAddEvent = async () => {
    const title = window.prompt("Event title");
    if (!title) return;
    const recurrenceRaw = window.prompt("Recurrence (none/daily/weekly/monthly)", "none") || "none";
    const recurrence = ["none", "daily", "weekly", "monthly"].includes(recurrenceRaw)
      ? (recurrenceRaw as "none" | "daily" | "weekly" | "monthly")
      : "none";

    await createCalendarEvent({
      user_id: activeUser?.user_id || "user-admin",
      title,
      recurrence,
      duration_minutes: 30,
    });
  };

  const onEditEvent = async (eventId: string, currentTitle: string) => {
    const title = window.prompt("Edit title", currentTitle);
    if (!title) return;
    await updateCalendarEvent(eventId, { title });
  };

  const onDeleteEvent = async (eventId: string) => {
    const confirmed = window.confirm("Delete this event?");
    if (!confirmed) return;
    await deleteCalendarEvent(eventId);
  };

  if (!runtimeState) {
    return <section className="screen-panel">Loading calendar...</section>;
  }

  const events = selectCalendarEvents(runtimeState);
  const windowStart = new Date(runtimeState.snapshot.calendar.window_start);
  const monthLabel = Number.isNaN(windowStart.getTime())
    ? "Current Month"
    : windowStart.toLocaleDateString([], { month: "long", year: "numeric" });

  const monthCells = Number.isNaN(windowStart.getTime()) ? [] : buildMonthGrid(windowStart);

  const groupedEvents = React.useMemo(() => {
    const groups = new Map<string, { label: string; events: typeof events }>();

    for (const event of events) {
      const parsed = new Date(event.start);
      const key = Number.isNaN(parsed.getTime()) ? event.start.slice(0, 10) : buildLocalDayKey(parsed);
      const label = Number.isNaN(parsed.getTime()) ? key : toLocalDayLabel(parsed);

      const existing = groups.get(key);
      if (existing) {
        existing.events.push(event);
      } else {
        groups.set(key, {
          label,
          events: [event],
        });
      }
    }

    return [...groups.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, value]) => ({
        key,
        label: value.label,
        events: value.events.sort((left, right) => left.start.localeCompare(right.start)),
      }));
  }, [events]);

  const eventDayKeys = new Set(groupedEvents.map((group) => group.key));

  return (
    <section className="screen-panel calendar-panel">
      <header className="screen-header">
        <div>
          <h2>Calendar</h2>
          <p>Window: {formatLocalDateTime(runtimeState.snapshot.calendar.window_start)} to {formatLocalDateTime(runtimeState.snapshot.calendar.window_end)}</p>
        </div>
        <div className="calendar-header-actions">
          <button type="button" onClick={onAddEvent}>Add Event</button>
          <SyncStatusPill status={runtimeState.sync_status} />
        </div>
      </header>

      <div className="calendar-layout">
        <section className="calendar-month-card" aria-label="Month overview">
          <div className="calendar-month-header">
            <h3>{monthLabel}</h3>
            <p>Outlook-style month snapshot</p>
          </div>
          <div className="calendar-weekdays" aria-hidden="true">
            <span>Mon</span>
            <span>Tue</span>
            <span>Wed</span>
            <span>Thu</span>
            <span>Fri</span>
            <span>Sat</span>
            <span>Sun</span>
          </div>
          <div className="calendar-month-grid">
            {monthCells.map((cell) => (
              <div
                key={cell.key}
                className={`calendar-day-cell ${cell.inMonth ? "" : "calendar-day-cell-muted"} ${eventDayKeys.has(cell.key) ? "calendar-day-cell-has-event" : ""}`.trim()}
              >
                {cell.dayNumber}
              </div>
            ))}
          </div>
        </section>

        <section className="calendar-agenda" aria-label="Agenda">
          {groupedEvents.length === 0 ? <p className="empty-text">No events in this window.</p> : null}
          {groupedEvents.map((group) => (
            <article key={group.key} className="calendar-day-group">
              <header className="calendar-day-group-header">
                <h3>{group.label}</h3>
                <span>{group.events.length} {group.events.length === 1 ? "event" : "events"}</span>
              </header>
              <ul className="calendar-event-list">
                {group.events.map((event) => (
                  <li key={event.event_id} className="calendar-event-card">
                    <div className="calendar-event-time">{formatLocalTimeRange(event.start, event.end)}</div>
                    <div className="calendar-event-details">
                      <strong>{event.title}</strong>
                      <p>{formatLocalDateTime(event.start)} to {formatLocalDateTime(event.end)}</p>
                      <p className="task-meta">Participants: {event.participants.length > 0 ? event.participants.join(", ") : "No participants yet"}</p>
                    </div>
                    <div className="calendar-event-actions">
                      <button type="button" onClick={() => onEditEvent(event.event_id, event.title)}>Edit</button>
                      <button type="button" onClick={() => onDeleteEvent(event.event_id)}>Delete</button>
                    </div>
                  </li>
                ))}
              </ul>
            </article>
          ))}
        </section>
      </div>
    </section>
  );
};
