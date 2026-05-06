import React from "react";
import type { DLSAnalyticsSnapshot, DLSRealtimeEvent, IntelligenceSystemMode } from "../../api/contracts";
import { productSurfaceClient } from "../../api/productSurfaceClient";
import { useRuntimeStore } from "../../runtime/store";

const CHART_WIDTH = 520;
const CHART_HEIGHT = 210;
const CHART_PADDING = 20;
const CANONICAL_ANALYTICS_ENABLED = false;
const CANONICAL_ANALYTICS_DISABLED_MESSAGE =
  "Analytics is disabled in canonical mode until a backend-supported analytics surface is available.";

type StreamStatus = "connecting" | "live" | "disconnected" | "error";

type TrendPoint = {
  timestamp: string;
  avg_dls: number;
  cost_per_hour: number;
  llm_calls_per_minute: number;
};

const emptySnapshot: DLSAnalyticsSnapshot = {
  avg_dls: 0,
  positive_lift_ratio: 0,
  negative_lift_ratio: 0,
  llm_cost_per_email: 0,
  rule_only_cost: 0,
  llm_usage_percentage: 0,
  total_emails_processed: 0,
  rolling_avg_dls: 0,
  rolling_cost_per_email: 0,
  lift_to_cost: 0,
  efficiency_score: 0,
  action_discovery_rate: 0,
  system_mode: "RULE_ONLY",
  alerts: [],
};

const modeLabel = (mode: IntelligenceSystemMode): string => {
  if (mode === "LLM_CORE") {
    return "LLM Core";
  }
  if (mode === "HYBRID") {
    return "Hybrid";
  }
  return "Rule Only";
};

const modeClass = (mode: IntelligenceSystemMode): string => {
  if (mode === "LLM_CORE") {
    return "analytics-mode-indicator-core";
  }
  if (mode === "HYBRID") {
    return "analytics-mode-indicator-hybrid";
  }
  return "analytics-mode-indicator-rules";
};

const toPercent = (value: number): string => `${(Math.max(0, value) * 100).toFixed(1)}%`;

const toMoney = (value: number): string => `$${Math.max(0, value).toFixed(4)}`;

const toSeriesPath = (
  values: number[],
  width: number,
  height: number,
  minValue?: number,
  maxValue?: number,
): string => {
  if (values.length === 0) {
    return "";
  }

  const low = minValue ?? Math.min(...values);
  const high = maxValue ?? Math.max(...values);
  const span = Math.max(0.0001, high - low);
  const usableWidth = width - CHART_PADDING * 2;
  const usableHeight = height - CHART_PADDING * 2;

  return values
    .map((value, index) => {
      const x = CHART_PADDING + (index / Math.max(values.length - 1, 1)) * usableWidth;
      const y = height - CHART_PADDING - ((value - low) / span) * usableHeight;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
};

const toDlsGainPerHourSeries = (points: TrendPoint[]): number[] => {
  if (points.length === 0) {
    return [];
  }

  return points.map((point, index) => {
    if (index === 0) {
      return 0;
    }

    const previous = points[index - 1];
    const previousTime = new Date(previous.timestamp).getTime();
    const currentTime = new Date(point.timestamp).getTime();
    const deltaHours = Math.max((currentTime - previousTime) / (1000 * 60 * 60), 1 / 3600);
    return (point.avg_dls - previous.avg_dls) / deltaHours;
  });
};

const normalizeRealtimeEvent = (payload: unknown): DLSRealtimeEvent | null => {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  const candidate = payload as Partial<DLSRealtimeEvent>;
  if (!candidate.timestamp || !candidate.system_mode) {
    return null;
  }

  return {
    timestamp: String(candidate.timestamp),
    avg_dls: Number(candidate.avg_dls || 0),
    cost_per_hour: Number(candidate.cost_per_hour || 0),
    llm_calls_per_minute: Number(candidate.llm_calls_per_minute || 0),
    system_mode: candidate.system_mode,
    efficiency_score: Number(candidate.efficiency_score || 0),
    alerts: Array.isArray(candidate.alerts) ? candidate.alerts.map((item) => String(item)) : [],
  };
};

export const AnalyticsScreen: React.FC = () => {
  const activeUser = useRuntimeStore((state) => state.active_user);
  const userId = (activeUser?.user_id || "").trim() || undefined;

  const [snapshot, setSnapshot] = React.useState<DLSAnalyticsSnapshot>(emptySnapshot);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);
  const [streamStatus, setStreamStatus] = React.useState<StreamStatus>("connecting");
  const [trend, setTrend] = React.useState<TrendPoint[]>([]);

  React.useEffect(() => {
    if (!CANONICAL_ANALYTICS_ENABLED) {
      setSnapshot(emptySnapshot);
      setTrend([]);
      setStreamStatus("disconnected");
      setError(CANONICAL_ANALYTICS_DISABLED_MESSAGE);
      setLoading(false);
      return;
    }

    let closed = false;
    const socket = productSurfaceClient.createDLSAnalyticsSocket(userId, 500, 2);

    const bootstrap = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await productSurfaceClient.fetchDLSAnalytics(userId, 500);
        if (closed) {
          return;
        }
        setSnapshot(data);
      } catch (fetchError) {
        if (closed) {
          return;
        }
        setError(String(fetchError || "analytics_fetch_failed"));
      } finally {
        if (!closed) {
          setLoading(false);
        }
      }
    };

    socket.onopen = () => {
      if (!closed) {
        setStreamStatus("live");
      }
    };

    socket.onmessage = (event) => {
      let payload: unknown;
      try {
        payload = JSON.parse(String(event.data || "{}"));
      } catch {
        return;
      }

      const parsed = normalizeRealtimeEvent(payload);
      if (!parsed || closed) {
        return;
      }

      setStreamStatus("live");
      setTrend((current) => [
        ...current,
        {
          timestamp: parsed.timestamp,
          avg_dls: parsed.avg_dls,
          cost_per_hour: parsed.cost_per_hour,
          llm_calls_per_minute: parsed.llm_calls_per_minute,
        },
      ].slice(-60));

      setSnapshot((current) => ({
        ...current,
        avg_dls: parsed.avg_dls,
        efficiency_score: parsed.efficiency_score,
        system_mode: parsed.system_mode,
        alerts: parsed.alerts,
      }));
    };

    socket.onerror = () => {
      if (!closed) {
        setStreamStatus("error");
      }
    };

    socket.onclose = () => {
      if (!closed) {
        setStreamStatus("disconnected");
      }
    };

    void bootstrap();

    return () => {
      closed = true;
      socket.close();
    };
  }, [userId]);

  const dlsSeries = trend.map((point) => point.avg_dls);
  const costSeries = trend.map((point) => point.cost_per_hour);
  const dlsGainSeries = toDlsGainPerHourSeries(trend);

  const dlsPath = toSeriesPath(dlsSeries, CHART_WIDTH, CHART_HEIGHT, -1.5, 4.0);
  const costPath = toSeriesPath(costSeries, CHART_WIDTH, CHART_HEIGHT);
  const dlsGainPath = toSeriesPath(dlsGainSeries, CHART_WIDTH, CHART_HEIGHT);
  const latestLlmCallsPerMinute = trend.length > 0 ? trend[trend.length - 1].llm_calls_per_minute : 0;

  const streamStatusLabel = (() => {
    if (streamStatus === "live") {
      return "Live";
    }
    if (streamStatus === "connecting") {
      return "Connecting";
    }
    if (streamStatus === "disconnected") {
      return "Disconnected";
    }
    return "Stream error";
  })();

  return (
    <section className="screen-panel analytics-screen-panel">
      <header className="screen-header analytics-screen-header">
        <div>
          <h2>Intelligence Analytics</h2>
          <p>Are we getting smarter or just more expensive?</p>
        </div>
        <div className="analytics-stream-pill" data-status={streamStatus}>
          Stream: {streamStatusLabel}
        </div>
      </header>

      {error ? <p className="error-text">{error}</p> : null}

      {loading ? (
        <p className="empty-text">Loading analytics...</p>
      ) : (
        <>
          <div className="analytics-kpi-grid">
            <article className="metric-card">
              <h3>Average DLS</h3>
              <p className="analytics-metric-value">{snapshot.avg_dls.toFixed(3)}</p>
            </article>
            <article className="metric-card">
              <h3>Efficiency Score</h3>
              <p className="analytics-metric-value">{snapshot.efficiency_score.toFixed(3)}</p>
            </article>
            <article className="metric-card">
              <h3>LLM Cost / Email</h3>
              <p className="analytics-metric-value">{toMoney(snapshot.llm_cost_per_email)}</p>
            </article>
            <article className="metric-card">
              <h3>LLM Usage</h3>
              <p className="analytics-metric-value">{toPercent(snapshot.llm_usage_percentage)}</p>
            </article>
            <article className="metric-card">
              <h3>LLM Calls / Min</h3>
              <p className="analytics-metric-value">{latestLlmCallsPerMinute.toFixed(2)}</p>
            </article>
          </div>

          <div className={`analytics-mode-indicator ${modeClass(snapshot.system_mode)}`}>
            <span>System mode</span>
            <strong>{modeLabel(snapshot.system_mode)}</strong>
          </div>

          <section className="analytics-grid">
            <article className="metric-card analytics-card">
              <h3>DLS Trend Line</h3>
              <svg viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`} className="analytics-chart" role="img" aria-label="DLS trend line">
                <polyline className="analytics-line analytics-line-dls" points={dlsPath} />
              </svg>
              <p className="empty-text">Rolling average DLS (last 50 events): {snapshot.rolling_avg_dls.toFixed(3)}</p>
            </article>

            <article className="metric-card analytics-card">
              <h3>Cost vs Value</h3>
              <svg viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`} className="analytics-chart" role="img" aria-label="Cost versus value trend">
                <polyline className="analytics-line analytics-line-cost" points={costPath} />
                <polyline className="analytics-line analytics-line-gain" points={dlsGainPath} />
              </svg>
              <p className="empty-text">
                Cost / hour vs DLS gain / hour. Lift-to-cost ratio: {snapshot.lift_to_cost.toFixed(3)}
              </p>
            </article>

            <article className="metric-card analytics-card">
              <h3>Action Discovery Rate</h3>
              <div className="analytics-progress-shell" role="progressbar" aria-valuenow={Math.round(snapshot.action_discovery_rate * 100)} aria-valuemin={0} aria-valuemax={100}>
                <div
                  className="analytics-progress-fill"
                  style={{ width: `${Math.max(2, Math.min(100, snapshot.action_discovery_rate * 100))}%` }}
                />
              </div>
              <p className="analytics-metric-value">{toPercent(snapshot.action_discovery_rate)}</p>
              <p className="empty-text">Positive lift: {toPercent(snapshot.positive_lift_ratio)} · Negative lift: {toPercent(snapshot.negative_lift_ratio)}</p>
            </article>

            <article className="metric-card analytics-card">
              <h3>Business Threshold Alerts</h3>
              <ul className="analytics-alert-list">
                {snapshot.alerts.length > 0 ? snapshot.alerts.map((item) => <li key={item}>{item}</li>) : <li>No alerts</li>}
              </ul>
              <p className="empty-text">Total emails processed: {snapshot.total_emails_processed}</p>
            </article>
          </section>
        </>
      )}
    </section>
  );
};
