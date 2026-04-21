import { useCallback, useEffect, useState } from 'react'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'

function SeverityPill({ severity }) {
  return <span className={`ifb-severity ifb-severity--${severity}`}>{severity}</span>
}

function PatternCard({ insight }) {
  const [open, setOpen] = useState(false)

  return (
    <div className={`ifb-pattern-card ifb-pattern-card--${insight.severity}`}>
      <button className="ifb-pattern-toggle" onClick={() => setOpen((value) => !value)}>
        <div>
          <p className="ifb-pattern-type">{insight.type.replace(/_/g, ' ')}</p>
          <p className="ifb-pattern-desc">{insight.description}</p>
        </div>
        <div className="ifb-pattern-meta">
          <SeverityPill severity={insight.severity} />
          <span className="ifb-expand-mark">{open ? '−' : '+'}</span>
        </div>
      </button>

      {open && (
        <div className="ifb-pattern-body">
          <p className="ifb-body-label">Evidence Sources</p>
          <div className="ifb-chip-row">
            {(insight.evidence_sources ?? []).map((source) => (
              <span className="ifb-chip" key={source}>{source}</span>
            ))}
          </div>
          <p className="ifb-body-label">Affected Components</p>
          <div className="ifb-chip-row">
            {(insight.affected_components ?? []).map((component) => (
              <span className="ifb-chip" key={component}>{component}</span>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

export default function InsightsBridgePanel() {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  const loadSummary = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetch(`${API_BASE}/insights/summary`)
      if (!response.ok) {
        throw new Error(`Insights request failed (${response.status})`)
      }
      setData(await response.json())
    } catch (err) {
      setError(String(err))
      setData(null)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadSummary()
  }, [loadSummary])

  return (
    <div className="ifb-panel">
      <div className="card">
        <div className="card-header">
          <h2>Insights</h2>
          <button className="btn btn-primary" onClick={loadSummary}>Refresh</button>
        </div>
      </div>

      {loading && (
        <div className="center-state">
          <div className="spinner" />
          <p>Loading insight bridge summary…</p>
        </div>
      )}

      {!loading && error && (
        <div className="center-state error-state">
          <p>{error}</p>
          <button className="btn btn-primary" onClick={loadSummary}>Retry</button>
        </div>
      )}

      {!loading && !error && data && (
        <div className="ifb-grid">
          <div className="card">
            <div className="card-header">
              <h2>System Health Summary</h2>
            </div>
            <div className="ifb-health-grid">
              <div className="ifb-health-card">
                <p className="ifb-health-label">Stability Score</p>
                <p className="ifb-health-value">{data.system_health_summary?.stability_score ?? 0}</p>
              </div>
              <div className="ifb-health-card">
                <p className="ifb-health-label">Conflict Rate</p>
                <p className="ifb-health-value">{data.system_health_summary?.conflict_rate ?? 0}</p>
              </div>
              <div className="ifb-health-card">
                <p className="ifb-health-label">Priority Accuracy Estimate</p>
                <p className="ifb-health-value">{data.system_health_summary?.priority_accuracy_estimate ?? 0}</p>
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Detected Patterns</h2>
            </div>
            <div className="ifb-pattern-list">
              {(data.insights ?? []).map((item) => (
                <PatternCard key={`${item.type}-${item.description}`} insight={item} />
              ))}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Recommendations</h2>
            </div>
            <div className="ifb-recommendation-list">
              {(data.recommendations ?? []).map((item, index) => (
                <div className={`ifb-recommendation ifb-recommendation--${item.priority}`} key={`${item.recommendation}-${index}`}>
                  <div className="ifb-recommendation-top">
                    <p className="ifb-recommendation-title">{item.recommendation}</p>
                    <SeverityPill severity={item.priority} />
                  </div>
                  <p className="ifb-recommendation-reason">{item.reason}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}