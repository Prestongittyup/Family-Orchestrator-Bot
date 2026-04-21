/**
 * Family Dashboard Page
 *
 * Overview view showing system health, active plans, today's summary,
 * and conflict indicators. All data is read-only from projection.
 */

import React from "react";
import { useHPALStore } from "../store/hpal-store";
import { useSyncProjection } from "../hooks/useSyncProjection";
import {
  SystemHealthIndicator,
  PlanCard,
  TaskCard,
  ChangeIndicator,
  LoadingState,
} from "../components/index";

interface FamilyDashboardProps {
  familyId: string;
}

export const FamilyDashboard: React.FC<FamilyDashboardProps> = ({ familyId }) => {
  const {
    family,
    plans,
    tasks,
    events,
    error,
    loading,
    last_change,
  } = useHPALStore();

  const { sync } = useSyncProjection({
    familyId,
    enabled: true,
    pollInterval: 20000,
  });

  if (!family) {
    return (
      <LoadingState loading={loading} error={error}>
        <div>No family data</div>
      </LoadingState>
    );
  }

  const pendingTasks = tasks.filter((t) => t.status === "pending");
  const inProgressTasks = tasks.filter((t) => t.status === "in_progress");
  const staleTasks = tasks.filter((t) => t.status === "stale_projection");

  const todayEvents = events.filter((e) => {
    const eventDate = new Date(e.time_window.start).toDateString();
    const today = new Date().toDateString();
    return eventDate === today;
  });

  return (
    <div className="page family-dashboard">
      <header className="page-header">
        <h1>📊 Household Control Center</h1>
        <p className="subtitle">Family: {family.members.map((m) => m.name).join(", ")}</p>
      </header>

      <div className="dashboard-grid">
        {/* System Health Section */}
        <section className="card health-section">
          <h2>System Health</h2>
          <SystemHealthIndicator summary={family.system_state_summary} />
          <button
            className="btn btn-secondary"
            onClick={sync}
            disabled={loading}
          >
            {loading ? "Syncing..." : "Refresh Now"}
          </button>
        </section>

        {/* Active Plans Summary */}
        <section className="card plans-section">
          <h2>
            📋 Active Plans <span className="badge">{plans.length}</span>
          </h2>
          <div className="plan-list">
            {plans.length === 0 ? (
              <p className="empty-state">No active plans</p>
            ) : (
              plans.slice(0, 3).map((plan) => (
                <PlanCard key={plan.plan_id} plan={plan} />
              ))
            )}
          </div>
          {plans.length > 3 && (
            <p className="text-muted">... and {plans.length - 3} more</p>
          )}
        </section>

        {/* Task Summary */}
        <section className="card tasks-section">
          <h2>✓ Task Summary</h2>
          <div className="task-summary">
            <div className="summary-item">
              <span className="summary-label">Pending</span>
              <span className="summary-value pending">{pendingTasks.length}</span>
            </div>
            <div className="summary-item">
              <span className="summary-label">In Progress</span>
              <span className="summary-value in-progress">
                {inProgressTasks.length}
              </span>
            </div>
            <div className="summary-item">
              <span className="summary-label">Stale Projection</span>
              <span className="summary-value warning">{staleTasks.length}</span>
            </div>
          </div>
          {staleTasks.length > 0 && (
            <div className="warning-box">
              ⚠️ {staleTasks.length} task(s) have unknown lifecycle state.
              System will reconcile.
            </div>
          )}
        </section>

        {/* Today's Events */}
        <section className="card events-section">
          <h2>📅 Today's Events</h2>
          {todayEvents.length === 0 ? (
            <p className="empty-state">No events today</p>
          ) : (
            <div className="events-list">
              {todayEvents.map((event) => (
                <div key={event.event_id} className="event-row">
                  <span className="event-time">
                    {new Date(event.time_window.start).toLocaleTimeString()}
                  </span>
                  <span className="event-title">{event.title}</span>
                  <span
                    className={`event-source event-source-${event.source}`}
                  >
                    {event.source === "manual" ? "👤" : "🤖"}
                  </span>
                </div>
              ))}
            </div>
          )}
        </section>

        {/* Conflict/Reconciliation Indicators */}
        <section className="card conflicts-section">
          <h2>⚡ System Status</h2>
          <div className="conflicts-list">
            {family.system_state_summary.stale_projection ? (
              <div className="conflict-item warning">
                <span className="icon">⚠️</span>
                <span className="text">
                  Projection is stale — waiting for reconciliation
                </span>
              </div>
            ) : (
              <div className="conflict-item success">
                <span className="icon">✓</span>
                <span className="text">All projections current</span>
              </div>
            )}
            {family.system_state_summary.pending_actions > 0 && (
              <div className="conflict-item info">
                <span className="icon">ℹ️</span>
                <span className="text">
                  {family.system_state_summary.pending_actions} pending
                  orchestration actions
                </span>
              </div>
            )}
          </div>
        </section>

        {/* Recent Change */}
        <section className="card change-section">
          <h2>📝 Latest Change</h2>
          {last_change ? (
            <ChangeIndicator change={last_change} />
          ) : (
            <p className="empty-state">No recent changes</p>
          )}
        </section>
      </div>

      {error && (
        <div className="error-banner">
          <strong>Error:</strong> {error}
        </div>
      )}
    </div>
  );
};
