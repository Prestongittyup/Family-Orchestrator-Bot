function Metric({ label, value }) {
  return (
    <div className="sim-metric-card">
      <p className="sim-metric-label">{label}</p>
      <p className="sim-metric-value">{value}</p>
    </div>
  )
}

export default function SimulationMetricsPanel({ result }) {
  const drift = result?.decision_drift_metrics ?? {}
  const stability = result?.stability_scores ?? {}
  const assertions = result?.assertions ?? {}

  return (
    <div className="card simulation-metrics-panel">
      <div className="card-header">
        <h2>Simulation Metrics</h2>
      </div>
      <div className="sim-metrics-grid">
        <Metric label="Stability Score" value={stability.stability_score ?? 0} />
        <Metric label="Decision Drift" value={drift.decision_drift_score ?? 0} />
        <Metric label="Priority Flip Rate" value={drift.priority_flip_rate ?? 0} />
        <Metric label="Brief Instability" value={drift.brief_instability_index ?? 0} />
        <Metric
          label="Conflict Resolution Success"
          value={assertions.correct_conflict_resolution_behavior ? 'yes' : 'no'}
        />
        <Metric
          label="No Stale Events"
          value={assertions.no_stale_event_persistence ? 'yes' : 'no'}
        />
      </div>
    </div>
  )
}
