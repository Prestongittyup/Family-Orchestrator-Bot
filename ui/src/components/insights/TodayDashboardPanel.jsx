import { useMemo, useState } from 'react'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'

const DEFAULT_QUERY = 'Plan today around school pickup with dinner and a workout block'

function SnapshotStat({ label, value }) {
  return (
    <div className="today-dashboard-stat">
      <p className="today-dashboard-stat-label">{label}</p>
      <p className="today-dashboard-stat-value">{value}</p>
    </div>
  )
}

function ProposalCard({ proposal, rank }) {
  return (
    <div className="today-dashboard-proposal">
      <div className="today-dashboard-proposal-top">
        <div>
          <p className="today-dashboard-proposal-rank">Rank {rank}</p>
          <p className="today-dashboard-proposal-title">{proposal.title}</p>
        </div>
        <span className="today-dashboard-proposal-confidence">{Math.round((proposal.confidence ?? 0) * 100)}%</span>
      </div>
      <p className="today-dashboard-proposal-domain">{proposal.domain}</p>
      <p className="today-dashboard-proposal-summary">{proposal.summary}</p>
      {(proposal.time_blocks ?? []).length > 0 && (
        <div className="today-dashboard-chip-row">
          {(proposal.time_blocks ?? []).map((item) => (
            <span className="today-dashboard-chip" key={item}>{item}</span>
          ))}
        </div>
      )}
    </div>
  )
}

export default function TodayDashboardPanel() {
  const [query, setQuery] = useState(DEFAULT_QUERY)
  const [plan, setPlan] = useState(null)
  const [loading, setLoading] = useState(false)
  const [approving, setApproving] = useState(false)
  const [error, setError] = useState(null)

  const rankedProposals = useMemo(() => {
    if (!plan) {
      return []
    }
    const proposalMap = Object.fromEntries((plan.proposals ?? []).map((item) => [item.proposal_id, item]))
    return (plan.ranked_plan ?? []).map((item) => ({ ...item, ...proposalMap[item.proposal_id] }))
  }, [plan])

  const runPlan = async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetch(`${API_BASE}/assistant/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query }),
      })
      if (!response.ok) {
        throw new Error(`Assistant runtime failed (${response.status})`)
      }
      setPlan(await response.json())
    } catch (err) {
      setError(String(err))
      setPlan(null)
    } finally {
      setLoading(false)
    }
  }

  const approvePlan = async () => {
    if (!plan?.execution_payload?.request_id) {
      return
    }

    const actionIds = (plan.execution_payload?.proposed_actions ?? []).map((item) => item.action_id)
    setApproving(true)
    setError(null)
    try {
      const response = await fetch(`${API_BASE}/assistant/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_id: plan.execution_payload.request_id, action_ids: actionIds }),
      })
      if (!response.ok) {
        throw new Error(`Plan approval failed (${response.status})`)
      }
      const approved = await response.json()
      setPlan((current) => {
        if (!current) {
          return current
        }
        return {
          ...current,
          execution_payload: {
            ...current.execution_payload,
            approved: true,
            proposed_actions: approved.proposed_actions ?? current.execution_payload.proposed_actions,
          },
        }
      })
    } catch (err) {
      setError(String(err))
    } finally {
      setApproving(false)
    }
  }

  return (
    <div className="today-dashboard-panel">
      <div className="card">
        <div className="card-header">
          <h2>Today Dashboard</h2>
        </div>
        <div className="today-dashboard-query-shell">
          <label className="today-dashboard-label" htmlFor="today-dashboard-query">Assistant request</label>
          <textarea
            id="today-dashboard-query"
            className="today-dashboard-query"
            rows="4"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Ask for one coordinated household plan across calendar, meals, fitness, and home logistics"
          />
          <div className="today-dashboard-actions">
            <button className="btn btn-primary" onClick={runPlan} disabled={loading}>
              {loading ? 'Building…' : 'Run Assistant'}
            </button>
            <p className="today-dashboard-note">The runtime returns one merged plan and keeps all execution inert until you approve it.</p>
          </div>
        </div>
      </div>

      {error && (
        <div className="center-state error-state">
          <p>{error}</p>
        </div>
      )}

      {plan && (
        <div className="today-dashboard-grid">
          <div className="card today-dashboard-wide">
            <div className="card-header">
              <h2>Today Plan</h2>
            </div>
            <div className="today-dashboard-body">
              <div className="today-dashboard-meta-row">
                <span className="today-dashboard-pill">Request {plan.request_id}</span>
                <span className="today-dashboard-pill">Intent {plan.intent.intent_type}</span>
                <span className="today-dashboard-pill">Approval {plan.requires_approval ? 'required' : 'not required'}</span>
              </div>
              <div className="today-dashboard-stats">
                <SnapshotStat label="Calendar Events" value={plan.state_snapshot?.calendar_events?.length ?? 0} />
                <SnapshotStat label="Recent Meals" value={plan.state_snapshot?.recent_meals?.length ?? 0} />
                <SnapshotStat label="Fitness Sessions" value={plan.state_snapshot?.fitness_schedule?.length ?? 0} />
                <SnapshotStat label="Tasks" value={plan.state_snapshot?.household_context?.task_count ?? 0} />
              </div>
              <div className="today-dashboard-proposal-list">
                {rankedProposals.map((proposal) => (
                  <ProposalCard key={proposal.proposal_id} proposal={proposal} rank={proposal.rank} />
                ))}
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Suggested Actions</h2>
            </div>
            <div className="today-dashboard-body">
              {(plan.execution_payload?.proposed_actions ?? []).length === 0 ? (
                <p className="empty-state">No approval-gated actions were generated for this run.</p>
              ) : (
                <div className="today-dashboard-action-list">
                  {(plan.execution_payload?.proposed_actions ?? []).map((action) => (
                    <div className="today-dashboard-action" key={action.action_id}>
                      <div>
                        <p className="today-dashboard-action-title">{action.action_type.replace(/_/g, ' ')}</p>
                        <p className="today-dashboard-action-copy">{action.description}</p>
                        <p className="today-dashboard-action-copy">Target: {action.target}</p>
                      </div>
                      <span className={`today-dashboard-status today-dashboard-status--${action.approval_status}`}>{action.approval_status}</span>
                    </div>
                  ))}
                </div>
              )}
              <button
                className="btn btn-primary"
                onClick={approvePlan}
                disabled={approving || !plan.requires_approval || plan.execution_payload?.approved}
              >
                {plan.execution_payload?.approved ? 'Plan Approved' : approving ? 'Approving…' : 'Approve Plan'}
              </button>
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Conflicts</h2>
            </div>
            <div className="today-dashboard-body">
              {(plan.conflicts ?? []).length === 0 ? (
                <p className="empty-state">No conflicts were detected across the merged plan.</p>
              ) : (
                <div className="today-dashboard-conflict-list">
                  {(plan.conflicts ?? []).map((conflict) => (
                    <div className="today-dashboard-conflict" key={`${conflict.conflict_type}-${conflict.description}`}>
                      <div className="today-dashboard-conflict-top">
                        <p className="today-dashboard-conflict-type">{conflict.conflict_type.replace(/_/g, ' ')}</p>
                        <span className={`today-dashboard-severity today-dashboard-severity--${conflict.severity}`}>{conflict.severity}</span>
                      </div>
                      <p className="today-dashboard-action-copy">{conflict.description}</p>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}