/**
 * Task Board Page
 *
 * Kanban-style board grouping tasks by status.
 * Filters by plan_id and person_id.
 * Read-only UI; mutations via HPAL internal endpoints.
 */

import React, { useState } from "react";
import { useHPALStore } from "../store/hpal-store";
import { useSyncProjection, useTaskSync } from "../hooks/useSyncProjection";
import { TaskCard, LoadingState } from "../components/index";

interface TaskBoardProps {
  familyId: string;
}

export const TaskBoard: React.FC<TaskBoardProps> = ({ familyId }) => {
  const {
    family,
    tasks,
    plans,
    error,
    loading,
    selected_plan_id,
    selected_person_id,
    selectPlan,
    selectPerson,
  } = useHPALStore();

  const [filterStatus, setFilterStatus] = useState<string | null>(null);

  useSyncProjection({
    familyId,
    enabled: true,
    pollInterval: 15000,
  });

  const { refetch: refetchTasks } = useTaskSync({
    familyId,
    enabled: true,
  });

  // Apply filters
  let filtered = tasks;

  if (selected_plan_id) {
    filtered = filtered.filter((t) => t.plan_id === selected_plan_id);
  }

  if (selected_person_id) {
    filtered = filtered.filter((t) => t.assigned_to === selected_person_id);
  }

  if (filterStatus) {
    filtered = filtered.filter((t) => t.status === filterStatus);
  }

  // Group by status
  const tasksByStatus = {
    pending: filtered.filter((t) => t.status === "pending"),
    in_progress: filtered.filter((t) => t.status === "in_progress"),
    completed: filtered.filter((t) => t.status === "completed"),
    failed: filtered.filter((t) => t.status === "failed"),
    stale_projection: filtered.filter((t) => t.status === "stale_projection"),
  };

  return (
    <div className="page task-board">
      <header className="page-header">
        <h1>✓ Task Board</h1>
      </header>

      {/* Filters */}
      <section className="filters-section">
        <div className="filter-row">
          <div className="filter-group">
            <label>Plan:</label>
            <select
              value={selected_plan_id || ""}
              onChange={(e) => selectPlan(e.target.value || null)}
            >
              <option value="">All Plans</option>
              {plans.map((p) => (
                <option key={p.plan_id} value={p.plan_id}>
                  {p.title} (rev {p.revision})
                </option>
              ))}
            </select>
          </div>

          <div className="filter-group">
            <label>Person:</label>
            <select
              value={selected_person_id || ""}
              onChange={(e) => selectPerson(e.target.value || null)}
            >
              <option value="">All People</option>
              {family?.members.map((m) => (
                <option key={m.person_id} value={m.person_id}>
                  {m.name}
                </option>
              ))}
            </select>
          </div>

          <div className="filter-group">
            <label>Status:</label>
            <select
              value={filterStatus || ""}
              onChange={(e) => setFilterStatus(e.target.value || null)}
            >
              <option value="">All Statuses</option>
              <option value="pending">Pending</option>
              <option value="in_progress">In Progress</option>
              <option value="completed">Completed</option>
              <option value="failed">Failed</option>
              <option value="stale_projection">Stale Projection</option>
            </select>
          </div>

          <button className="btn btn-secondary" onClick={() => refetchTasks()}>
            🔄 Refresh
          </button>
        </div>

        <p className="filter-info">
          Showing {filtered.length} of {tasks.length} total tasks
        </p>
      </section>

      <LoadingState loading={loading && tasks.length === 0} error={error}>
        {/* Kanban Board */}
        <div className="kanban-board">
          {Object.entries(tasksByStatus).map(([status, statusTasks]) => {
            const statusLabels: Record<string, string> = {
              pending: "📋 Pending",
              in_progress: "⏳ In Progress",
              completed: "✅ Completed",
              failed: "❌ Failed",
              stale_projection: "⚠️ Stale Projection",
            };

            return (
              <div key={status} className={`kanban-column column-${status}`}>
                <div className="column-header">
                  <h3>{statusLabels[status]}</h3>
                  <span className="column-count">{statusTasks.length}</span>
                </div>

                <div className="column-tasks">
                  {statusTasks.length === 0 ? (
                    <p className="column-empty">No tasks</p>
                  ) : (
                    statusTasks.map((task) => (
                      <TaskCard key={task.task_id} task={task} />
                    ))
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </LoadingState>

      {/* Notes */}
      <section className="info-section">
        <p className="info-text">
          💡 <strong>Note:</strong> Task status can only be changed by the
          system via recomputation or by authorized internal endpoints. Frontend
          display is read-only to ensure consistency with backend state.
        </p>
      </section>

      {error && (
        <div className="error-banner">
          <strong>Error:</strong> {error}
        </div>
      )}
    </div>
  );
};
