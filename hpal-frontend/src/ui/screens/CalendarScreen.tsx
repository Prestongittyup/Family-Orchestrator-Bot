import React from "react";
import { useRuntimeStore } from "../../runtime/store";
import { selectCalendarEvents } from "../../runtime/selectors";
import { SyncStatusPill } from "../components/SyncStatusPill";

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

  return (
    <section className="screen-panel">
      <header className="screen-header">
        <div>
          <h2>Calendar</h2>
          <p>
            Window: {runtimeState.snapshot.calendar.window_start} to {runtimeState.snapshot.calendar.window_end}
          </p>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
          <button type="button" onClick={onAddEvent}>Add Event</button>
          <SyncStatusPill status={runtimeState.sync_status} />
        </div>
      </header>

      {events.length === 0 ? <p className="empty-text">No events in this window.</p> : null}
      <ul className="list-panel">
        {events.map((event) => (
          <li key={event.event_id}>
            <strong>{event.title}</strong>
            <p>
              {event.start} to {event.end}
            </p>
            <p className="task-meta">Participants: {event.participants.join(", ")}</p>
            <div style={{ display: "flex", gap: "0.5rem" }}>
              <button type="button" onClick={() => onEditEvent(event.event_id, event.title)}>Edit</button>
              <button type="button" onClick={() => onDeleteEvent(event.event_id)}>Delete</button>
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
};
