/**
 * Common UI Components for HPAL Frontend
 */

import React from "react";
import { ProjectionWatermark, SystemStateSummary } from "../types/index";

export interface SystemHealthIndicatorProps {
  summary: SystemStateSummary;
}

export const SystemHealthIndicator: React.FC<SystemHealthIndicatorProps> = ({
  summary,
}) => {
  const stability = summary.stale_projection ? "⚠️ Stale" : "✓ Current";
  const pendingLabel = summary.pending_actions > 0 ? "⚠️" : "✓";

  return (
    <div className="health-indicator">
      <div className="health-row">
        <span className="label">Projection Status:</span>
        <span className={`value ${summary.stale_projection ? "stale" : "current"}`}>
          {stability}
        </span>
      </div>
      <div className="health-row">
        <span className="label">Pending Actions:</span>
        <span className="value">
          {pendingLabel} {summary.pending_actions}
        </span>
      </div>
      <div className="health-row">
        <span className="label">Last Update:</span>
        <span className="value text-muted">
          {new Date(summary.last_projection_at).toLocaleTimeString()}
        </span>
      </div>
      <div className="health-row">
        <span className="label">State Version:</span>
        <span className="value text-muted">v{summary.state_version}</span>
      </div>
    </div>
  );
};

export interface PlanCardProps {
  plan: {
    plan_id: string;
    title: string;
    status: string;
    stability_state: string;
    revision: number;
    linked_tasks: string[];
  };
  onSelect?: (planId: string) => void;
}

export const PlanCard: React.FC<PlanCardProps> = ({ plan, onSelect }) => {
  const stabilityColor = {
    stable: "#22c55e",
    adjusting: "#eab308",
    blocked: "#ef4444",
  }[plan.stability_state as keyof typeof stabilityColor] || "#64748b";

  return (
    <div
      className="plan-card"
      onClick={() => onSelect?.(plan.plan_id)}
      style={{ borderLeftColor: stabilityColor }}
    >
      <div className="plan-header">
        <h3>{plan.title}</h3>
        <span className={`status-badge status-${plan.status}`}>{plan.status}</span>
      </div>
      <div className="plan-meta">
        <span className="meta-tag">rev {plan.revision}</span>
        <span className={`stability-tag stability-${plan.stability_state}`}>
          {plan.stability_state}
        </span>
        <span className="meta-tag">{plan.linked_tasks.length} tasks</span>
      </div>
    </div>
  );
};

export interface TaskCardProps {
  task: {
    task_id: string;
    title: string;
    status: string;
    assigned_to: string;
    priority: string;
    due_time: string | null;
  };
}

export const TaskCard: React.FC<TaskCardProps> = ({ task }) => {
  const statusColor = {
    pending: "#3b82f6",
    in_progress: "#eab308",
    completed: "#22c55e",
    failed: "#ef4444",
    stale_projection: "#64748b",
  }[task.status as keyof object] || "#64748b";

  return (
    <div className="task-card" style={{ borderTopColor: statusColor }}>
      <div className="task-header">
        <h4>{task.title}</h4>
        <span className={`status-badge status-${task.status}`}>{task.status}</span>
      </div>
      <div className="task-meta">
        <span className="meta-tag">👤 {task.assigned_to}</span>
        <span className={`priority-tag priority-${task.priority}`}>{task.priority}</span>
      </div>
      {task.due_time && (
        <div className="task-time">
          <span className="time-label">Due:</span>
          <span className="time-value">
            {new Date(task.due_time).toLocaleString()}
          </span>
        </div>
      )}
    </div>
  );
};

export interface EventBadgeProps {
  event: {
    event_id: string;
    title: string;
    source: "manual" | "system_generated";
  };
}

export const EventBadge: React.FC<EventBadgeProps> = ({ event }) => {
  const sourceIndicator = event.source === "manual" ? "👤" : "🤖";

  return (
    <span className={`event-badge event-source-${event.source}`}>
      {sourceIndicator} {event.title}
    </span>
  );
};

export interface WatermarkDisplayProps {
  watermark: ProjectionWatermark | null;
}

export const WatermarkDisplay: React.FC<WatermarkDisplayProps> = ({ watermark }) => {
  if (!watermark) {
    return <div className="watermark-display">No watermark</div>;
  }

  return (
    <div className="watermark-display">
      <div className="watermark-row">
        <span>Epoch:</span>
        <code>{watermark.projection_epoch}</code>
      </div>
      <div className="watermark-row">
        <span>Hash:</span>
        <code className="hash-short">{watermark.snapshot_hash.substring(0, 12)}...</code>
      </div>
      <div className="watermark-row">
        <span>Events:</span>
        <span>{watermark.event_count}</span>
      </div>
    </div>
  );
};

export interface LoadingStateProps {
  loading: boolean;
  error: string | null;
}

export const LoadingState: React.FC<
  LoadingStateProps & { children: React.ReactNode }
> = ({ loading, error, children }) => {
  if (error) {
    return <div className="error-state">❌ Error: {error}</div>;
  }

  if (loading) {
    return <div className="loading-state">⏳ Loading...</div>;
  }

  return <>{children}</>;
};

export interface ChangeIndicatorProps {
  change: {
    entity_type: string;
    reason_code: string;
    initiated_by: "user" | "system";
    timestamp: string;
  } | null;
}

export const ChangeIndicator: React.FC<ChangeIndicatorProps> = ({ change }) => {
  if (!change) {
    return null;
  }

  const initiator = change.initiated_by === "user" ? "👤 User" : "🤖 System";

  return (
    <div className="change-indicator">
      <span className="indicator-label">Recent Change:</span>
      <span className="indicator-entity">{change.entity_type}</span>
      <span className="indicator-reason">{change.reason_code}</span>
      <span className="indicator-initiator">{initiator}</span>
      <span className="indicator-time">
        {new Date(change.timestamp).toLocaleTimeString()}
      </span>
    </div>
  );
};

// Alias for WatermarkDisplay
export const WatermarkIndicator = WatermarkDisplay;

export interface ConflictBadgeProps {
  severity: "high" | "medium" | "low";
  count?: number;
}

export const ConflictBadge: React.FC<ConflictBadgeProps> = ({ severity, count = 1 }) => {
  const severityEmoji = {
    high: "🔴",
    medium: "🟡",
    low: "🟢",
  }[severity];

  return (
    <span className={`conflict-badge severity-${severity}`}>
      {severityEmoji} {count} conflict{count !== 1 ? "s" : ""}
    </span>
  );
};

export interface ExplainPanelProps {
  reason?: string;
  isSystemInitiated?: boolean;
  recomputeTrigger?: string;
  watermarkVersion?: string;
  lastUpdateTime?: string;
}

export const ExplainPanel: React.FC<ExplainPanelProps> = ({
  reason,
  isSystemInitiated,
  recomputeTrigger,
  watermarkVersion,
  lastUpdateTime,
}) => {
  if (
    !reason &&
    !isSystemInitiated &&
    !recomputeTrigger &&
    !watermarkVersion &&
    !lastUpdateTime
  ) {
    return null;
  }

  return (
    <div className="explain-panel">
      <div className="explain-header">
        <span className="explain-title">💡 Why did this change?</span>
      </div>
      <div className="explain-content">
        {reason && (
          <div className="explain-item">
            <span className="explain-label">Reason:</span>
            <span className="explain-value">{reason}</span>
          </div>
        )}
        {isSystemInitiated !== undefined && (
          <div className="explain-item">
            <span className="explain-label">Initiated:</span>
            <span className="explain-value">
              {isSystemInitiated ? "🤖 System" : "👤 User"}
            </span>
          </div>
        )}
        {recomputeTrigger && (
          <div className="explain-item">
            <span className="explain-label">Recompute Trigger:</span>
            <span className="explain-value">{recomputeTrigger}</span>
          </div>
        )}
        {watermarkVersion && (
          <div className="explain-item">
            <span className="explain-label">Version:</span>
            <code className="version-code">{watermarkVersion.substring(0, 12)}</code>
          </div>
        )}
        {lastUpdateTime && (
          <div className="explain-item">
            <span className="explain-label">Updated:</span>
            <span className="explain-value">
              {new Date(lastUpdateTime).toLocaleTimeString()}
            </span>
          </div>
        )}
      </div>
    </div>
  );
};
