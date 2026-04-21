/**
 * Plan Detail View
 *
 * Shows plan metadata, linked tasks, recompute status, and revision history.
 * All data is read-only; mutations go through HPAL command gateway.
 */

import React, { useState } from "react";
import { useHPALStore } from "../store/hpal-store";
import { useSyncProjection } from "../hooks/useSyncProjection";
import { hpalClient } from "../api/hpal-client";
import { ChangeIndicator, LoadingState, TaskCard } from "../components/index";
import { v4 as uuidv4 } from "uuid";

interface PlanDetailProps {
  familyId: string;
  planId: string;
  onClose?: () => void;
}

export const PlanDetail: React.FC<PlanDetailProps> = ({
  familyId,
  planId,
  onClose,
}) => {
  const { plans, tasks, error, loading, last_change, recordChange, setError } =
    useHPALStore();

  const [recomputeReason, setRecomputeReason] = useState("");
  const [isRecomputing, setIsRecomputing] = useState(false);

  useSyncProjection({
    familyId,
    enabled: true,
    pollInterval: 15000,
  });

  const plan = plans.find((p) => p.plan_id === planId);

  if (!plan && !loading) {
    return (
      <div className="page plan-detail">
        <div className="error-state">Plan not found</div>
        {onClose && <button onClick={onClose}>Close</button>}
      </div>
    );
  }

  const planTasks = tasks.filter((t) => t.plan_id === planId);
  const tasksByStatus = {
    pending: planTasks.filter((t) => t.status === "pending"),
    in_progress: planTasks.filter((t) => t.status === "in_progress"),
    completed: planTasks.filter((t) => t.status === "completed"),
    failed: planTasks.filter((t) => t.status === "failed"),
    stale_projection: planTasks.filter((t) => t.status === "stale_projection"),
  };

  const handleRecompute = async () => {
    if (!plan || !recomputeReason.trim()) {
      setError("Please provide a reason for recomputation");
      return;
    }

    setIsRecomputing(true);
    try {
      await hpalClient.recomputePlan(familyId, planId, {
        reason: recomputeReason,
        idempotency_key: `recompute:${planId}:${uuidv4()}`,
      });

      recordChange({
        entity_type: "plan",
        entity_id: planId,
        reason_code: "manual_recompute",
        initiated_by: "user",
        recompute_trigger: recomputeReason,
        watermark: null,
        timestamp: new Date().toISOString(),
      });

      setRecomputeReason("");
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      setError(`Failed to recompute: ${error}`);
    } finally {
      setIsRecomputing(false);
    }
  };

  return (
    <div className="page plan-detail">
      <header className="page-header">
        <div className="header-top">
          {onClose && (
            <button className="btn btn-ghost" onClick={onClose}>
              ← Back
            </button>
          )}
          <h1>{plan?.title || "Loading..."}</h1>
        </div>
      </header>

      <LoadingState loading={loading && !plan} error={error}>
        {plan && (
          <div className="detail-grid">
            {/* Plan Metadata */}
            <section className="card metadata-section">
              <h2>Plan Info</h2>
              <div className="metadata-grid">
                <div className="metadata-row">
                  <span className="label">Status:</span>
                  <span className={`value status-${plan.status}`}>
                    {plan.status}
                  </span>
                </div>
                <div className="metadata-row">
                  <span className="label">Stability:</span>
                  <span
                    className={`value stability-${plan.stability_state}`}
                  >
                    {plan.stability_state}
                  </span>
                </div>
                <div className="metadata-row">
                  <span className="label">Revision:</span>
                  <span className="value">v{plan.revision}</span>
                </div>
                <div className="metadata-row">
                  <span className="label">Intent:</span>
                  <span className="value text-muted">
                    {plan.intent_origin}
                  </span>
                </div>
                <div className="metadata-row">
                  <span className="label">Time Window:</span>
                  <span className="value text-muted">
                    {new Date(plan.schedule_window.start).toLocaleDateString()}
                    {" — "}
                    {new Date(plan.schedule_window.end).toLocaleDateString()}
                  </span>
                </div>
                {plan.last_recomputed_at && (
                  <div className="metadata-row">
                    <span className="label">Last Recomputed:</span>
                    <span className="value text-muted">
                      {new Date(
                        plan.last_recomputed_at
                      ).toLocaleString()}
                    </span>
                  </div>
                )}
              </div>
            </section>

            {/* Recompute Control */}
            <section className="card recompute-section">
              <h2>🔄 Recompute Plan</h2>
              <div className="recompute-form">
                <textarea
                  value={recomputeReason}
                  onChange={(e) => setRecomputeReason(e.target.value)}
                  placeholder="Reason for recomputation (e.g., schedule conflict, new event)"
                  disabled={isRecomputing}
                  rows={3}
                />
                <button
                  className="btn btn-primary"
                  onClick={handleRecompute}
                  disabled={
                    isRecomputing ||
                    !recomputeReason.trim() ||
                    plan.stability_state === "blocked"
                  }
                >
                  {isRecomputing ? "Recomputing..." : "Request Recompute"}
                </button>
                {plan.stability_state === "blocked" && (
                  <p className="warning-text">
                    ⚠️ Plan is blocked. Fix the issue before recomputing.
                  </p>
                )}
              </div>
            </section>

            {/* Linked Tasks */}
            <section className="card tasks-section">
              <h2>
                ✓ Linked Tasks <span className="badge">{planTasks.length}</span>
              </h2>

              {planTasks.length === 0 ? (
                <p className="empty-state">No tasks linked to this plan</p>
              ) : (
                <div className="tasks-by-status">
                  {Object.entries(tasksByStatus).map(([status, statusTasks]) => {
                    if (statusTasks.length === 0) return null;
                    return (
                      <div key={status} className="task-group">
                        <h3 className={`group-title status-${status}`}>
                          {status.replace(/_/g, " ")} ({statusTasks.length})
                        </h3>
                        <div className="task-list">
                          {statusTasks.map((task) => (
                            <TaskCard key={task.task_id} task={task} />
                          ))}
                        </div>
                      </div>
                    );
                  })}

                  {tasksByStatus.stale_projection.length > 0 && (
                    <div className="warning-box">
                      ⚠️ {tasksByStatus.stale_projection.length} task(s) have
                      stale projection. System will reconcile automatically.
                    </div>
                  )}
                </div>
              )}
            </section>

            {/* Revision History (Read-only) */}
            <section className="card history-section">
              <h2>📜 Revision History</h2>
              <div className="revision-timeline">
                <div className="timeline-item current">
                  <div className="timeline-marker">●</div>
                  <div className="timeline-content">
                    <p className="revision-label">Current Revision {plan.revision}</p>
                    <p className="revision-meta">
                      Status: {plan.status} | Stability: {plan.stability_state}
                    </p>
                  </div>
                </div>
                {plan.revision > 1 && (
                  <div className="timeline-info">
                    <p className="text-muted">
                      Prior revisions available via audit trail
                    </p>
                  </div>
                )}
              </div>
            </section>

            {/* Recent Change */}
            {last_change && last_change.entity_id === planId && (
              <section className="card change-section">
                <h2>📝 Latest Change</h2>
                <ChangeIndicator change={last_change} />
              </section>
            )}
          </div>
        )}
      </LoadingState>

      {error && (
        <div className="error-banner">
          <strong>Error:</strong> {error}
        </div>
      )}
    </div>
  );
};
