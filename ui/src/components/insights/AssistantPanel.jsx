import { useMemo, useState } from 'react'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'

const DEFAULT_QUERY = 'Schedule a doctor appointment for Monday morning after school drop-off'

function ActionRow({ action, onApprove, approving }) {
  return (
    <div className="assistant-action-row">
      <div>
        <p className="assistant-action-type">{action.action_type.replace(/_/g, ' ')}</p>
        <p className="assistant-action-description">{action.description}</p>
        <p className="assistant-action-target">Target: {action.target}</p>
      </div>
      <div className="assistant-action-meta">
        <span className={`assistant-status assistant-status--${action.approval_status}`}>{action.approval_status}</span>
        <button
          className="btn btn-primary"
          onClick={() => onApprove(action.action_id)}
          disabled={approving || action.approval_status === 'approved'}
        >
          {action.approval_status === 'approved' ? 'Approved' : approving ? 'Approving…' : 'Approve'}
        </button>
      </div>
    </div>
  )
}

function TimelinePreview({ blocks }) {
  if (!blocks?.length) {
    return <p className="empty-state">No suggested schedule blocks yet.</p>
  }

  return (
    <div className="assistant-timeline">
      {blocks.map((block) => (
        <div className="assistant-timeline-item" key={`${block.time_block}-${block.title}`}>
          <div className="assistant-time-block">{block.time_block}</div>
          <div>
            <p className="assistant-block-title">{block.title}</p>
            <p className="assistant-block-rationale">{block.rationale}</p>
          </div>
          <span className="assistant-confidence">{Math.round(block.confidence * 100)}%</span>
        </div>
      ))}
    </div>
  )
}

export default function AssistantPanel() {
  const [query, setQuery] = useState(DEFAULT_QUERY)
  const [response, setResponse] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [approvingActionId, setApprovingActionId] = useState(null)

  const timelineBlocks = useMemo(() => response?.plan?.recommended_plan?.timeline_blocks ?? [], [response])
  const mealPlan = response?.plan?.meal_plan
  const fitnessPlan = response?.plan?.fitness_plan

  const submitQuery = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch(`${API_BASE}/assistant/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query }),
      })
      if (!res.ok) {
        throw new Error(`Assistant query failed (${res.status})`)
      }
      setResponse(await res.json())
    } catch (err) {
      setError(String(err))
      setResponse(null)
    } finally {
      setLoading(false)
    }
  }

  const approveAction = async (actionId) => {
    if (!response?.request_id) {
      return
    }
    setApprovingActionId(actionId)
    setError(null)
    try {
      const res = await fetch(`${API_BASE}/assistant/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: response.request_id, action_ids: [actionId] }),
      })
      if (!res.ok) {
        throw new Error(`Approval failed (${res.status})`)
      }
      setResponse(await res.json())
    } catch (err) {
      setError(String(err))
    } finally {
      setApprovingActionId(null)
    }
  }

  return (
    <div className="assistant-panel">
      <div className="card">
        <div className="card-header">
          <h2>Assistant</h2>
        </div>
        <div className="assistant-query-shell">
          <label className="assistant-query-label" htmlFor="assistant-query">Natural language request</label>
          <textarea
            id="assistant-query"
            className="assistant-query-input"
            rows="4"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Ask for scheduling, meals, workouts, or household coordination suggestions"
          />
          <div className="assistant-query-actions">
            <button className="btn btn-primary" onClick={submitQuery} disabled={loading}>
              {loading ? 'Planning…' : 'Generate Plan'}
            </button>
            <p className="assistant-query-note">Suggestions stay inert until a proposed action is explicitly approved.</p>
          </div>
        </div>
      </div>

      {error && (
        <div className="center-state error-state">
          <p>{error}</p>
        </div>
      )}

      {response && (
        <div className="assistant-grid">
          <div className="card">
            <div className="card-header">
              <h2>Timeline Preview</h2>
            </div>
            <div className="assistant-card-body">
              <p className="assistant-summary">{response.plan.summary}</p>
              <TimelinePreview blocks={timelineBlocks} />
              {(response.conflicts ?? []).length > 0 && (
                <div className="assistant-conflicts">
                  <p className="assistant-section-label">Conflicts</p>
                  {response.conflicts.map((conflict) => (
                    <div className="assistant-conflict-card" key={`${conflict.description}-${conflict.severity}`}>
                      <span className={`badge badge-${conflict.severity === 'high' ? 'red' : conflict.severity === 'medium' ? 'yellow' : 'neutral'}`}>
                        {conflict.severity}
                      </span>
                      <p>{conflict.description}</p>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Meal Suggestion Panel</h2>
            </div>
            <div className="assistant-card-body assistant-domain-panel">
              {mealPlan ? (
                <>
                  <p className="assistant-domain-title">{mealPlan.recipe_name}</p>
                  <p className="assistant-domain-copy">Balance: {(mealPlan.nutrition_balance ?? []).join(', ')}</p>
                  <p className="assistant-domain-copy">Ingredients used: {(mealPlan.ingredients_used ?? []).join(', ') || 'None'}</p>
                  <p className="assistant-domain-copy">Grocery additions: {(mealPlan.grocery_additions ?? []).join(', ') || 'None needed'}</p>
                  <p className="assistant-domain-copy">Repeat window: {mealPlan.repeat_window_days} days</p>
                </>
              ) : (
                <p className="empty-state">Submit a meal-planning request to see inventory-aware meal suggestions.</p>
              )}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Fitness Plan Panel</h2>
            </div>
            <div className="assistant-card-body assistant-domain-panel">
              {fitnessPlan ? (
                <>
                  <p className="assistant-domain-title">{fitnessPlan.goal}</p>
                  <p className="assistant-domain-copy">{fitnessPlan.weekly_summary}</p>
                  <div className="assistant-session-list">
                    {(fitnessPlan.sessions ?? []).map((session) => (
                      <div className="assistant-session-card" key={`${session.day}-${session.time_block}`}>
                        <p className="assistant-session-title">{session.day}: {session.focus}</p>
                        <p className="assistant-domain-copy">{session.time_block} · {session.duration_minutes} min</p>
                        <p className="assistant-domain-copy">{session.rationale}</p>
                      </div>
                    ))}
                  </div>
                </>
              ) : (
                <p className="empty-state">Submit a fitness request to see schedule-aware workout suggestions.</p>
              )}
            </div>
          </div>

          <div className="card assistant-wide-card">
            <div className="card-header">
              <h2>Approval Gate</h2>
            </div>
            <div className="assistant-card-body">
              <div className="assistant-response-meta">
                <span className="assistant-pill">Request {response.request_id}</span>
                <span className="assistant-pill">Intent {response.intent.intent_type}</span>
                <span className="assistant-pill">Priority {response.intent.priority}</span>
              </div>
              <div className="assistant-action-list">
                {(response.proposed_actions ?? []).map((action) => (
                  <ActionRow
                    key={action.action_id}
                    action={action}
                    onApprove={approveAction}
                    approving={approvingActionId === action.action_id}
                  />
                ))}
              </div>
              <div className="assistant-trace-panel">
                <p className="assistant-section-label">Reasoning Trace</p>
                <ul className="assistant-trace-list">
                  {(response.reasoning_trace ?? []).map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}