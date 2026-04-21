import { useEffect, useState } from 'react'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'
const DEFAULT_QUERY = 'Plan today around appointments, dinner, groceries, and workout constraints'

export default function TodayDailyView() {
  const [query, setQuery] = useState(DEFAULT_QUERY)
  const [decision, setDecision] = useState(null)
  const [loading, setLoading] = useState(false)
  const [approving, setApproving] = useState(false)
  const [error, setError] = useState(null)

  const runDecision = async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetch(`${API_BASE}/assistant/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query }),
      })
      if (!response.ok) {
        throw new Error(`Assistant decision failed (${response.status})`)
      }
      setDecision(await response.json())
    } catch (err) {
      setError(String(err))
      setDecision(null)
    } finally {
      setLoading(false)
    }
  }

  const approveDecision = async () => {
    if (!decision) {
      return
    }

    const actionIds = (decision.grouped_approvals ?? []).flatMap((group) => group.action_ids ?? [])
    if (actionIds.length === 0) {
      return
    }

    setApproving(true)
    setError(null)
    try {
      const response = await fetch(`${API_BASE}/assistant/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: decision.request_id, action_ids: actionIds }),
      })
      if (!response.ok) {
        throw new Error(`Decision approval failed (${response.status})`)
      }
      setDecision(await response.json())
    } catch (err) {
      setError(String(err))
    } finally {
      setApproving(false)
    }
  }

  useEffect(() => {
    runDecision()
  }, [])

  return (
    <div className="daily-view-panel">
      <div className="card">
        <div className="card-header">
          <h2>Today State View</h2>
        </div>
        <div className="daily-view-query-shell">
          <label className="daily-view-label" htmlFor="daily-view-query">Household request</label>
          <textarea
            id="daily-view-query"
            className="daily-view-query"
            rows="4"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Ask for the next household decision with schedule, meal, and workout constraints"
          />
          <div className="daily-view-actions">
            <button className="btn btn-primary" onClick={runDecision} disabled={loading}>
              {loading ? 'Loading…' : 'Refresh State'}
            </button>
            <p className="daily-view-note">The assistant now exposes one household state summary and one recommended next action.</p>
          </div>
        </div>
      </div>

      {error && (
        <div className="center-state error-state">
          <p>{error}</p>
        </div>
      )}

      {decision && (
        <div className="daily-view-grid">
          <div className="card">
            <div className="card-header">
              <h2>Today State View</h2>
            </div>
            <div className="daily-view-body">
              <div className="daily-view-summary-list">
                <div className="daily-view-summary-card">
                  <p className="daily-view-summary-title">Intent</p>
                  <p className="daily-view-summary-copy">{decision.intent_summary}</p>
                </div>
                <div className="daily-view-summary-card">
                  <p className="daily-view-summary-title">Calendar Load</p>
                  <p className="daily-view-summary-copy">{decision.current_state_summary.calendar_event_count} events tracked</p>
                </div>
                <div className="daily-view-summary-card">
                  <p className="daily-view-summary-title">Pending Approvals</p>
                  <p className="daily-view-summary-copy">{decision.current_state_summary.pending_approval_count}</p>
                </div>
                <div className="daily-view-summary-card">
                  <p className="daily-view-summary-title">Low Inventory</p>
                  <p className="daily-view-summary-copy">
                    {(decision.current_state_summary.low_inventory_items ?? []).length > 0
                      ? decision.current_state_summary.low_inventory_items.join(', ')
                      : 'No immediate shortages'}
                  </p>
                </div>
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Recommended Next Action</h2>
            </div>
            <div className="daily-view-body">
              <div className="daily-view-summary-card">
                <p className="daily-view-summary-title">{decision.recommended_action.title}</p>
                <p className="daily-view-summary-copy">{decision.recommended_action.description}</p>
                <p className="daily-view-summary-copy">Domain {decision.recommended_action.domain}</p>
                <p className="daily-view-summary-copy">Urgency {decision.recommended_action.urgency}</p>
                <p className="daily-view-summary-copy">Scheduled {decision.recommended_action.scheduled_for ?? 'To be coordinated'}</p>
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Reasoning Trace</h2>
            </div>
            <div className="daily-view-body">
              <div className="daily-view-conflict-list">
                {(decision.reasoning_trace ?? []).map((item) => (
                  <div className="daily-view-conflict" key={item}>
                    <p className="daily-view-summary-copy">{item}</p>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Conflicts</h2>
            </div>
            <div className="daily-view-body">
              {(decision.current_state_summary.conflicts ?? []).length === 0 ? (
                <p className="empty-state">No household conflicts detected.</p>
              ) : (
                <div className="daily-view-conflict-list">
                  {(decision.current_state_summary.conflicts ?? []).map((conflict) => (
                    <div className="daily-view-conflict" key={`${conflict.conflict_type}-${conflict.description}`}>
                      <div className="daily-view-conflict-top">
                        <p className="daily-view-summary-title">{conflict.conflict_type.replace(/_/g, ' ')}</p>
                        <span className={`daily-view-severity daily-view-severity--${conflict.severity}`}>{conflict.severity}</span>
                      </div>
                      <p className="daily-view-summary-copy">{conflict.description}</p>
                    </div>
                  ))}
                </div>
              )}
              <button
                className="btn btn-primary"
                onClick={approveDecision}
                disabled={approving || decision.recommended_action.approval_status === 'approved'}
              >
                {decision.recommended_action.approval_status === 'approved' ? 'Approved' : approving ? 'Approving…' : 'Approve'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}