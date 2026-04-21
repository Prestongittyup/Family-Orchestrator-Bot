const METRIC_LABELS = {
  avg_priority: 'Avg Priority',
  avg_relevance: 'Avg Relevance',
  avg_completeness: 'Avg Completeness',
  avg_clarity: 'Avg Clarity',
  avg_priority_correctness: 'Priority Correctness',
  avg_conflict_handling: 'Conflict Handling',
  avg_omission: 'Omission Score',
  avg_noise_penalty: 'Noise Penalty',
}

function MetricBar({ label, value }) {
  const v = Number(value)
  const pct = (v / 10) * 100
  const color = v >= 8 ? 'var(--green)' : v >= 5 ? 'var(--yellow)' : 'var(--red)'

  return (
    <div className="metric-bar-row">
      <div className="metric-bar-top">
        <span className="metric-label">{label}</span>
        <span className="metric-value" style={{ color }}>{v.toFixed(1)}<span className="metric-max"> / 10</span></span>
      </div>
      <div className="score-bar-track">
        <div className="score-bar-fill" style={{ width: `${pct}%`, background: color }} />
      </div>
    </div>
  )
}

export default function MetricsPanel({ aggregate }) {
  const entries = Object.entries(METRIC_LABELS)
  if (!entries.length) return <p className="empty-state">No aggregate data.</p>

  return (
    <div className="metrics-panel">
      {entries.map(([key, label]) => (
        <MetricBar key={key} label={label} value={aggregate[key] ?? 0} />
      ))}
    </div>
  )
}
