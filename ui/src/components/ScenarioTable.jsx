const SCORE_KEYS = [
  'priority_score', 'relevance_score', 'completeness_score', 'clarity_score',
  'priority_correctness', 'conflict_handling_score', 'omission_score', 'noise_penalty',
]

function scenarioAvg(scores) {
  if (!scores) return 0
  const vals = SCORE_KEYS.map((k) => Number(scores[k] ?? 0))
  return vals.reduce((a, b) => a + b, 0) / vals.length
}

function scoreColor(avg) {
  if (avg >= 8) return 'green'
  if (avg >= 5) return 'yellow'
  return 'red'
}

export default function ScenarioTable({ scenarios, selected, onSelect }) {
  if (!scenarios.length) {
    return <p className="empty-state">No scenarios found.</p>
  }

  return (
    <table className="scenario-table">
      <thead>
        <tr>
          <th>Scenario</th>
          <th>Description</th>
          <th>Avg Score</th>
          <th>Priority ✓</th>
          <th>Issues</th>
        </tr>
      </thead>
      <tbody>
        {scenarios.map((row) => {
          const avg = scenarioAvg(row.scores)
          const color = scoreColor(avg)
          const isSelected = selected?.scenario_id === row.scenario_id
          const issueCount = row.issues?.length ?? 0
          const priorityOk = (row.scores?.priority_correctness ?? 0) === 10

          return (
            <tr
              key={row.scenario_id}
              className={`scenario-row scenario-row--${color}${isSelected ? ' scenario-row--selected' : ''}`}
              onClick={() => onSelect(isSelected ? null : row)}
            >
              <td>
                <code className="scenario-id">{row.scenario_id}</code>
              </td>
              <td className="scenario-desc">{row.description}</td>
              <td>
                <span className={`score-badge score-badge--${color}`}>
                  {avg.toFixed(1)}
                </span>
              </td>
              <td className="center">
                {priorityOk
                  ? <span className="icon-ok">✓</span>
                  : <span className="icon-fail">✗</span>}
              </td>
              <td className="center">
                {issueCount > 0
                  ? <span className="issue-count">{issueCount}</span>
                  : <span className="icon-ok">–</span>}
              </td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}
