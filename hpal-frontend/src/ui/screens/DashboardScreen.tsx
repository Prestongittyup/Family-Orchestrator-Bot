import React from "react";
import { useRuntimeStore } from "../../runtime/store";
import { selectNotifications, selectTaskCounts } from "../../runtime/selectors";
import { SyncStatusPill } from "../components/SyncStatusPill";

export const DashboardScreen: React.FC = () => {
  const runtimeState = useRuntimeStore((state) => state.runtimeState);
  const isLoading = useRuntimeStore((state) => state.isLoading);
  const error = useRuntimeStore((state) => state.error);

  if (!runtimeState) {
    return <section className="screen-panel">{isLoading ? "Loading dashboard..." : "No data available."}</section>;
  }

  const counts = selectTaskCounts(runtimeState);
  const notifications = selectNotifications(runtimeState);

  return (
    <section className="screen-panel">
      <header className="screen-header">
        <div>
          <h2>Family Dashboard</h2>
          <p>
            {runtimeState.snapshot.family.family_id} | {runtimeState.snapshot.family.member_count} members
          </p>
        </div>
        <SyncStatusPill status={runtimeState.sync_status} />
      </header>

      {error ? <p className="error-text">{error}</p> : null}

      <div className="metric-grid">
        <article className="metric-card">
          <h3>Open Tasks</h3>
          <p>{runtimeState.snapshot.today_overview.open_task_count}</p>
        </article>
        <article className="metric-card">
          <h3>Events Today</h3>
          <p>{runtimeState.snapshot.today_overview.scheduled_event_count}</p>
        </article>
        <article className="metric-card">
          <h3>Active Plans</h3>
          <p>{runtimeState.snapshot.today_overview.active_plan_count}</p>
        </article>
        <article className="metric-card">
          <h3>Notifications</h3>
          <p>{runtimeState.snapshot.today_overview.notification_count}</p>
        </article>
      </div>

      <div className="metric-grid">
        <article className="metric-card">
          <h3>Pending</h3>
          <p>{counts.pending}</p>
        </article>
        <article className="metric-card">
          <h3>In Progress</h3>
          <p>{counts.inProgress}</p>
        </article>
        <article className="metric-card">
          <h3>Completed</h3>
          <p>{counts.completed}</p>
        </article>
        <article className="metric-card">
          <h3>Failed</h3>
          <p>{counts.failed}</p>
        </article>
      </div>

      <section>
        <h3>Notifications</h3>
        {notifications.length === 0 ? <p className="empty-text">No active notifications.</p> : null}
        <ul className="list-panel">
          {notifications.map((notification) => (
            <li key={notification.notification_id}>
              <strong>{notification.title}</strong>
              <p>{notification.message}</p>
              <span className={`level-pill level-${notification.level}`}>{notification.level}</span>
            </li>
          ))}
        </ul>
      </section>
    </section>
  );
};
