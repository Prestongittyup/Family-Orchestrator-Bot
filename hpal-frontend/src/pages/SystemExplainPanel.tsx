/**
 * System Explain Panel Page
 *
 * Displays projection metadata, watermark info, and system state information.
 * Helps users understand the state of the orchestration system.
 */

import React from "react";
import { useHPALStore } from "../store/hpal-store";
import { useSyncProjection } from "../hooks/useSyncProjection";
import { LoadingState, SystemHealthIndicator, WatermarkIndicator } from "../components/index";

interface SystemExplainPanelProps {
  familyId: string;
}

export const SystemExplainPanel: React.FC<SystemExplainPanelProps> = ({
  familyId,
}) => {
  const { systemStateSummary, watermark, error, loading } = useHPALStore();

  useSyncProjection({
    familyId,
    enabled: true,
    pollInterval: 30000,
  });

  return (
    <div className="page system-explain">
      <header className="page-header">
        <h1>🔍 System Status & Diagnostics</h1>
        <p className="subtitle">Projection health and orchestration metadata</p>
      </header>

      <LoadingState
        loading={loading && !watermark}
        error={error}
      >
        <div className="system-grid">
          {/* Health Overview */}
          <section className="system-card">
            <h2>System Health</h2>
            {systemStateSummary && (
              <SystemHealthIndicator summary={systemStateSummary} />
            )}
          </section>

          {/* Watermark Information */}
          <section className="system-card">
            <h2>Projection Watermark</h2>
            {watermark && <WatermarkIndicator watermark={watermark} />}
          </section>

          {/* System State Summary */}
          {systemStateSummary && (
            <section className="system-card">
              <h2>Orchestration Metrics</h2>
              <div className="metrics-grid">
                <div className="metric">
                  <span className="metric-label">Plans Managed</span>
                  <span className="metric-value">
                    {systemStateSummary.plans_count}
                  </span>
                </div>
                <div className="metric">
                  <span className="metric-label">Active Tasks</span>
                  <span className="metric-value">
                    {systemStateSummary.tasks_count}
                  </span>
                </div>
                <div className="metric">
                  <span className="metric-label">Conflict Rate</span>
                  <span className="metric-value">
                    {(systemStateSummary.conflict_rate * 100).toFixed(1)}%
                  </span>
                </div>
                <div className="metric">
                  <span className="metric-label">System Uptime</span>
                  <span className="metric-value">
                    {systemStateSummary.system_uptime_hours}h
                  </span>
                </div>
              </div>
            </section>
          )}

          {/* Stability Information */}
          {systemStateSummary && (
            <section className="system-card">
              <h2>Stability Assessment</h2>
              <div className="stability-info">
                <div className="stability-score">
                  <span className="label">Overall Stability</span>
                  <div className="progress-bar">
                    <div
                      className={`progress-fill stability-${
                        systemStateSummary.priority_accuracy > 0.8
                          ? "high"
                          : systemStateSummary.priority_accuracy > 0.5
                          ? "medium"
                          : "low"
                      }`}
                      style={{
                        width: `${systemStateSummary.priority_accuracy * 100}%`,
                      }}
                    />
                  </div>
                  <span className="value">
                    {(systemStateSummary.priority_accuracy * 100).toFixed(1)}%
                  </span>
                </div>

                <div className="stability-info-item">
                  <strong>Prediction Accuracy:</strong>
                  <p>
                    {(systemStateSummary.priority_accuracy * 100).toFixed(1)}%
                    accuracy on task priority predictions
                  </p>
                </div>

                <div className="stability-info-item">
                  <strong>Conflict Incidents:</strong>
                  <p>
                    {(systemStateSummary.conflict_rate * 100).toFixed(2)}% of
                    orchestration events involve conflicts requiring manual
                    reconciliation
                  </p>
                </div>

                <div className="stability-info-item">
                  <strong>System Responsiveness:</strong>
                  <p>
                    Orchestration state updates are propagated with
                    {watermark ? ` ~${watermark.projection_lag_ms}ms latency` : " --"}
                  </p>
                </div>
              </div>
            </section>
          )}

          {/* Projection Staleness */}
          {watermark && (
            <section className="system-card">
              <h2>Projection Currency</h2>
              <div className="staleness-info">
                <div
                  className={`staleness-badge ${
                    watermark.stale_projection ? "stale" : "current"
                  }`}
                >
                  {watermark.stale_projection ? "⚠️ Stale" : "✅ Current"}
                </div>

                <div className="staleness-detail">
                  <p>
                    <strong>Last Update:</strong>
                    {new Date(
                      watermark.last_projection_at
                    ).toLocaleString()}
                  </p>
                  <p>
                    <strong>Projection Epoch:</strong> {watermark.projection_epoch}
                  </p>
                  <p>
                    <strong>Version:</strong> {watermark.projection_version}
                  </p>
                  <p>
                    <strong>Lag:</strong> {watermark.projection_lag_ms}ms
                  </p>
                </div>

                {watermark.stale_projection && (
                  <div className="stale-warning">
                    <p>
                      The projection is stale and may not reflect the latest
                      orchestration state. Consider refreshing or waiting for
                      the next sync cycle.
                    </p>
                  </div>
                )}
              </div>
            </section>
          )}

          {/* Help & Guidance */}
          <section className="system-card">
            <h2>Understanding the System</h2>
            <div className="help-content">
              <div className="help-item">
                <strong>🤖 System Stability</strong>
                <p>
                  Indicates how predictable and conflict-free orchestration
                  decisions are. Higher values mean fewer unexpected changes.
                </p>
              </div>

              <div className="help-item">
                <strong>📊 Conflict Rate</strong>
                <p>
                  Percentage of events where the system detected conflicting
                  constraints or user intentions requiring reconciliation.
                </p>
              </div>

              <div className="help-item">
                <strong>⏱️ Projection Watermark</strong>
                <p>
                  Tracks the version and timeliness of the frontend's view of
                  backend state. A stale projection means the UI view may be
                  behind reality.
                </p>
              </div>

              <div className="help-item">
                <strong>🔄 Staleness</strong>
                <p>
                  The frontend periodically syncs with the backend using
                  projection watermarks to ensure consistency. If marked stale,
                  refresh to get the latest state.
                </p>
              </div>
            </div>
          </section>
        </div>

        {error && (
          <div className="error-banner">
            <strong>Error:</strong> {error}
          </div>
        )}
      </LoadingState>
    </div>
  );
};
