import { useState } from 'react'

const PRIORITY_COLORS = { high: 'var(--red)', medium: 'var(--yellow)', low: 'var(--green)' }
const PRIORITY_ICONS = { high: '🔴', medium: '🟡', low: '🟢' }

function ExpandableCard({ title, meta, children, accent }) {
  const [open, setOpen] = useState(false)
  return (
    <div className="intel-card" style={{ borderLeftColor: accent }} onClick={() => setOpen(!open)}>
      <div className="intel-card-header">
        <span className="intel-card-title">{title}</span>
        {meta}
        <span className="intel-card-toggle">{open ? '▾' : '▸'}</span>
      </div>
      {open && <div className="intel-card-body">{children}</div>}
    </div>
  )
}

export default function DecisionPanel({ failurePatterns, decisionGaps, recommendations }) {
  const empty = !failurePatterns.length && !decisionGaps.length && !recommendations.length

  if (empty) {
    return <p className="empty-state no-issues" style={{ padding: '1.5rem' }}>✓ No failure patterns detected.</p>
  }

  return (
    <div className="decision-panel">
      {failurePatterns.length > 0 && (
        <section>
          <h3 className="section-label">Failure Patterns</h3>
          {failurePatterns.map((p) => (
            <ExpandableCard
              key={p.type}
              title={p.type.replace(/_/g, ' ')}
              accent="var(--red)"
              meta={
                <span className="badge badge-red" style={{ marginLeft: '0.5rem' }}>
                  {p.count}×
                </span>
              }
            >
              <p className="intel-card-detail">Affected scenarios:</p>
              <ul className="intel-list">
                {p.scenarios.map((s) => (
                  <li key={s}><code>{s}</code></li>
                ))}
              </ul>
            </ExpandableCard>
          ))}
        </section>
      )}

      {decisionGaps.length > 0 && (
        <section>
          <h3 className="section-label">Decision Gaps</h3>
          {decisionGaps.map((g, i) => (
            <ExpandableCard
              key={i}
              title={g.gap_type.replace(/_/g, ' ')}
              accent="var(--yellow)"
              meta={
                <span className="badge badge-yellow" style={{ marginLeft: '0.5rem' }}>
                  freq {g.frequency}
                </span>
              }
            >
              <p className="intel-card-detail">Source failure: <code>{g.source_failure}</code></p>
            </ExpandableCard>
          ))}
        </section>
      )}

      {recommendations.length > 0 && (
        <section>
          <h3 className="section-label">Recommendations</h3>
          {recommendations.map((r, i) => {
            const color = PRIORITY_COLORS[r.priority] ?? 'var(--text-secondary)'
            const icon = PRIORITY_ICONS[r.priority] ?? '⚪'
            return (
              <ExpandableCard
                key={i}
                title={r.recommendation.substring(0, 60) + (r.recommendation.length > 60 ? '…' : '')}
                accent={color}
                meta={
                  <span className="badge" style={{ marginLeft: '0.5rem', background: color + '22', color, border: `1px solid ${color}55` }}>
                    {icon} {r.priority}
                  </span>
                }
              >
                <p className="intel-card-detail">{r.recommendation}</p>
                <p className="intel-card-detail" style={{ marginTop: '0.25rem' }}>
                  Based on: <code>{r.based_on}</code>
                </p>
              </ExpandableCard>
            )
          })}
        </section>
      )}
    </div>
  )
}
