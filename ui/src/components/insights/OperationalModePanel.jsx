import { useCallback, useEffect, useState } from 'react'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'

const VIEWS = [
  { id: 'run', label: 'Run' },
  { id: 'context', label: 'Context' },
  { id: 'brief', label: 'Brief' },
]

export default function OperationalModePanel() {
  const [view, setView] = useState('run')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [data, setData] = useState(null)

  const load = useCallback(async (targetView) => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetch(`${API_BASE}/operational/${targetView}`)
      if (!response.ok) {
        throw new Error(`Operational endpoint failed (${response.status})`)
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
    load(view)
  }, [load, view])

  return (
    <div className="operational-panel">
      <div className="card">
        <div className="card-header">
          <h2>Today (Operational Mode)</h2>
        </div>
        <div className="operational-toolbar">
          {VIEWS.map((item) => (
            <button
              key={item.id}
              className={`inspector-tab ${view === item.id ? 'inspector-tab--active' : ''}`}
              onClick={() => setView(item.id)}
            >
              {item.label}
            </button>
          ))}
        </div>
      </div>

      {loading && (
        <div className="center-state">
          <div className="spinner" />
          <p>Loading operational feed…</p>
        </div>
      )}

      {!loading && error && (
        <div className="center-state error-state">
          <p>{error}</p>
          <button className="btn btn-primary" onClick={() => load(view)}>Retry</button>
        </div>
      )}

      {!loading && !error && data && (
        <div className="operational-grid">
          <div className="card">
            <div className="card-header">
              <h2>Top Priorities</h2>
            </div>
            <div className="operational-list">
              {(data.top_priorities ?? []).map((item, index) => (
                <div className="operational-item" key={`${item.title}-${index}`}>
                  <p className="operational-title">{item.title}</p>
                  <p className="operational-sub">{item.priority_level} priority</p>
                  <p className="operational-sub">{item.reason}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Schedule Actions</h2>
            </div>
            <div className="operational-list">
              {(data.schedule_actions ?? []).map((item, index) => (
                <div className="operational-item" key={`${item.action}-${index}`}>
                  <p className="operational-title">{item.action}</p>
                  <p className="operational-sub">{item.time}</p>
                  <p className="operational-sub">Confidence {item.confidence}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Conflicts</h2>
            </div>
            <div className="operational-list">
              {(data.conflicts ?? []).length === 0 ? (
                <p className="empty-state">No conflicts currently detected.</p>
              ) : (
                (data.conflicts ?? []).map((item, index) => (
                  <div className="operational-item" key={`${item.conflict_type}-${index}`}>
                    <p className="operational-title">{item.conflict_type}</p>
                    <p className="operational-sub">Severity {item.severity}</p>
                    <p className="operational-sub">{item.description}</p>
                  </div>
                ))
              )}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>System Notes</h2>
            </div>
            <ul className="operational-notes">
              {(data.system_notes ?? []).map((item, index) => (
                <li key={`${item}-${index}`}>{item}</li>
              ))}
            </ul>
          </div>
        </div>
      )}
    </div>
  )
}
