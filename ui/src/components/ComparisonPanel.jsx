const METRIC_LABELS = {
  priority_score: 'Priority Score',
  relevance_score: 'Relevance',
  completeness_score: 'Completeness',
  clarity_score: 'Clarity',
  priority_correctness: 'Priority Correctness',
  conflict_handling_score: 'Conflict Handling',
  omission_score: 'Omission Score',
  noise_penalty: 'Noise Penalty',
}

function deltaArrow(delta) {
  if (delta > 0) return { symbol: '↑', color: 'var(--green)' }
  if (delta < 0) return { symbol: '↓', color: 'var(--red)' }
  return { symbol: '→', color: 'var(--text-secondary)' }
}

export default function ComparisonPanel({ comparison }) {
  const { improved, regressions = [], score_deltas = {} } = comparison

  const hasDeltas = Object.keys(score_deltas).length > 0

  return (
    <div className="comparison-panel">
      <div className="comparison-summary">
        <div className={`comparison-status ${improved ? 'comp-improved' : regressions.length ? 'comp-regressed' : 'comp-neutral'}`}>
          {improved ? '↑ Performance improved from last run'
            : regressions.length ? `↓ ${regressions.length} regression${regressions.length > 1 ? 's' : ''} detected`
            : '→ No change from last run'}
        </div>

        {regressions.length > 0 && (
          <div className="regression-tags">
            {regressions.map((r) => (
              <span key={r} className="badge badge-red">{r.replace(/_/g, ' ')}</span>
            ))}
          </div>
        )}
      </div>

      {hasDeltas && (
        <table className="delta-table">
          <thead>
            <tr>
              <th>Metric</th>
              <th className="center">Delta</th>
              <th className="center">Direction</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(METRIC_LABELS).map(([key, label]) => {
              const delta = Number(score_deltas[key] ?? 0)
              const { symbol, color } = deltaArrow(delta)
              return (
                <tr key={key}>
                  <td>{label}</td>
                  <td className="center">
                    <span style={{ color: delta !== 0 ? color : 'var(--text-secondary)' }}>
                      {delta > 0 ? '+' : ''}{delta.toFixed(2)}
                    </span>
                  </td>
                  <td className="center">
                    <span style={{ color, fontSize: '1.2rem' }}>{symbol}</span>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}
    </div>
  )
}
