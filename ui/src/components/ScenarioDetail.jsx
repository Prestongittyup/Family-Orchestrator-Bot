const SCORE_LABELS = {
  priority_score: 'Priority Score',
  relevance_score: 'Relevance',
  completeness_score: 'Completeness',
  clarity_score: 'Clarity',
  priority_correctness: 'Priority Correctness',
  conflict_handling_score: 'Conflict Handling',
  omission_score: 'Omission Score',
  noise_penalty: 'Noise Penalty',
}

function ScoreBar({ label, value }) {
  const pct = (Number(value) / 10) * 100
  const color = value >= 8 ? 'var(--green)' : value >= 5 ? 'var(--yellow)' : 'var(--red)'

  return (
    <div className="score-bar-row">
      <span className="score-bar-label">{label}</span>
      <div className="score-bar-track">
        <div className="score-bar-fill" style={{ width: `${pct}%`, background: color }} />
      </div>
      <span className="score-bar-value" style={{ color }}>{value}</span>
    </div>
  )
}

export default function ScenarioDetail({ scenario }) {
  if (!scenario) {
    return (
      <div className="empty-state">
        <p>Click a scenario row to inspect its full breakdown.</p>
      </div>
    )
  }

  const { scenario_id, description, scores = {}, issues = [] } = scenario

  return (
    <div className="scenario-detail">
      <div className="detail-header">
        <code>{scenario_id}</code>
        <p className="detail-desc">{description}</p>
      </div>

      <h3 className="section-label">Score Breakdown</h3>
      <div className="score-bars">
        {Object.entries(SCORE_LABELS).map(([key, label]) => (
          <ScoreBar key={key} label={label} value={Number(scores[key] ?? 0)} />
        ))}
      </div>

      <h3 className="section-label">Issues {issues.length > 0 && <span className="badge badge-red">{issues.length}</span>}</h3>
      {issues.length === 0
        ? <p className="no-issues">✓ No issues detected</p>
        : (
          <ul className="issue-list">
            {issues.map((issue, i) => (
              <li key={i} className="issue-item">
                <span className="issue-bullet">⚠</span>
                {issue}
              </li>
            ))}
          </ul>
        )}
    </div>
  )
}
