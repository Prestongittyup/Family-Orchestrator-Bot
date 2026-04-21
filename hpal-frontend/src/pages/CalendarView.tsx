/**
 * Calendar View Page
 *
 * Events grouped by time window with linked plan indicators.
 * Displays event source (manual vs system_generated).
 * All data read-only from projection.
 */

import React, { useState } from "react";
import { useHPALStore } from "../store/hpal-store";
import { useSyncProjection } from "../hooks/useSyncProjection";
import { LoadingState, EventBadge } from "../components/index";
import { Event } from "../types/index";

interface CalendarViewProps {
  familyId: string;
}

interface TimeBlock {
  label: string;
  start: number;
  end: number;
}

export const CalendarView: React.FC<CalendarViewProps> = ({ familyId }) => {
  const { events, plans, error, loading } = useHPALStore();

  const [selectedDate, setSelectedDate] = useState(
    new Date().toISOString().split("T")[0]
  );

  useSyncProjection({
    familyId,
    enabled: true,
    pollInterval: 20000,
  });

  // Time blocks for the day
  const timeBlocks: TimeBlock[] = [
    { label: "00:00 - 06:00", start: 0, end: 6 },
    { label: "06:00 - 12:00", start: 6, end: 12 },
    { label: "12:00 - 18:00", start: 12, end: 18 },
    { label: "18:00 - 24:00", start: 18, end: 24 },
  ];

  // Filter events for selected date
  const selectedDayEvents = events.filter((e) => {
    const eventDate = new Date(e.time_window.start).toISOString().split("T")[0];
    return eventDate === selectedDate;
  });

  // Group events by time block
  const eventsByBlock: Record<string, Event[]> = {};
  timeBlocks.forEach((block) => {
    eventsByBlock[block.label] = selectedDayEvents.filter((e) => {
      const hour = new Date(e.time_window.start).getHours();
      return hour >= block.start && hour < block.end;
    });
  });

  // Get linked plan for event
  const getLinkedPlans = (event: Event) => {
    return plans.filter((p) => event.linked_plans.includes(p.plan_id));
  };

  // Navigate dates
  const prevDate = () => {
    const prev = new Date(selectedDate);
    prev.setDate(prev.getDate() - 1);
    setSelectedDate(prev.toISOString().split("T")[0]);
  };

  const nextDate = () => {
    const next = new Date(selectedDate);
    next.setDate(next.getDate() + 1);
    setSelectedDate(next.toISOString().split("T")[0]);
  };

  const goToToday = () => {
    setSelectedDate(new Date().toISOString().split("T")[0]);
  };

  return (
    <div className="page calendar-view">
      <header className="page-header">
        <h1>📅 Calendar</h1>
      </header>

      {/* Date Navigation */}
      <section className="date-nav">
        <button className="btn btn-ghost" onClick={prevDate}>
          ← Previous
        </button>

        <div className="date-display">
          <h2>{new Date(selectedDate).toLocaleDateString()}</h2>
          <input
            type="date"
            value={selectedDate}
            onChange={(e) => setSelectedDate(e.target.value)}
            className="date-input"
          />
        </div>

        <button className="btn btn-ghost" onClick={nextDate}>
          Next →
        </button>

        <button className="btn btn-secondary" onClick={goToToday}>
          Today
        </button>
      </section>

      <LoadingState loading={loading && events.length === 0} error={error}>
        {selectedDayEvents.length === 0 ? (
          <div className="empty-calendar">
            <p className="empty-state">No events scheduled for this day</p>
          </div>
        ) : (
          <div className="calendar-grid">
            {timeBlocks.map((block) => {
              const blockEvents = eventsByBlock[block.label];

              return (
                <div key={block.label} className="time-block">
                  <div className="block-header">
                    <h3 className="block-time">{block.label}</h3>
                    {blockEvents.length > 0 && (
                      <span className="event-count">{blockEvents.length}</span>
                    )}
                  </div>

                  <div className="block-content">
                    {blockEvents.length === 0 ? (
                      <p className="empty-block">No events</p>
                    ) : (
                      <div className="events-list">
                        {blockEvents.map((event) => {
                          const linkedPlans = getLinkedPlans(event);

                          return (
                            <div key={event.event_id} className="event-item">
                              <div className="event-header">
                                <div className="event-title-row">
                                  <span className="source-indicator">
                                    {event.source === "manual" ? "👤" : "🤖"}
                                  </span>
                                  <span className="event-title">
                                    {event.title}
                                  </span>
                                </div>
                                <span
                                  className={`source-label source-${event.source}`}
                                >
                                  {event.source === "manual"
                                    ? "Manual"
                                    : "System"}
                                </span>
                              </div>

                              {event.participants.length > 0 && (
                                <div className="event-participants">
                                  <span className="participants-label">
                                    Participants:
                                  </span>
                                  <span className="participants-list">
                                    {event.participants.join(", ")}
                                  </span>
                                </div>
                              )}

                              {linkedPlans.length > 0 && (
                                <div className="linked-plans">
                                  <span className="plans-label">
                                    Linked to:
                                  </span>
                                  <div className="plans-badges">
                                    {linkedPlans.map((p) => (
                                      <span
                                        key={p.plan_id}
                                        className="plan-badge"
                                      >
                                        {p.title}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              )}

                              <div className="event-meta">
                                <span className="meta-time">
                                  {new Date(
                                    event.time_window.start
                                  ).toLocaleTimeString([], {
                                    hour: "2-digit",
                                    minute: "2-digit",
                                  })}
                                  {" - "}
                                  {new Date(
                                    event.time_window.end
                                  ).toLocaleTimeString([], {
                                    hour: "2-digit",
                                    minute: "2-digit",
                                  })}
                                </span>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </LoadingState>

      {/* Legend */}
      <section className="calendar-legend">
        <h3>Legend</h3>
        <div className="legend-items">
          <div className="legend-item">
            <span className="legend-icon">👤</span>
            <span>Manual Event (user-created)</span>
          </div>
          <div className="legend-item">
            <span className="legend-icon">🤖</span>
            <span>System Event (auto-generated)</span>
          </div>
        </div>
      </section>

      {error && (
        <div className="error-banner">
          <strong>Error:</strong> {error}
        </div>
      )}
    </div>
  );
};
