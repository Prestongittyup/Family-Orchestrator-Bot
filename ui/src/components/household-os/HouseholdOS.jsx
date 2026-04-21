import { useEffect, useState } from 'react'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'

function ChatView({ onQuerySubmit, loading, messages }) {
  const [input, setInput] = useState('')

  const handleSend = () => {
    if (input.trim()) {
      onQuerySubmit(input)
      setInput('')
    }
  }

  return (
    <div className="chat-view">
      <div className="chat-messages">
        {messages.map((msg, idx) => (
          <div key={idx} className={`message message-${msg.role}`}>
            <p>{msg.content}</p>
          </div>
        ))}
      </div>
      <div className="chat-input-area">
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && handleSend()}
          placeholder="Ask me anything about your household..."
          rows="3"
          className="chat-input"
          disabled={loading}
        />
        <button
          onClick={handleSend}
          disabled={loading || !input.trim()}
          className="btn btn-primary"
        >
          {loading ? 'Thinking…' : 'Send'}
        </button>
      </div>
    </div>
  )
}

function TodayStateView({ decision, loading }) {
  if (loading) {
    return (
      <div className="today-state-view">
        <div className="loading-state">
          <div className="spinner" />
          <p>Analyzing household state…</p>
        </div>
      </div>
    )
  }

  if (!decision) {
    return (
      <div className="today-state-view">
        <div className="empty-state">
          <p>Try asking "What should I do today?" to get started.</p>
        </div>
      </div>
    )
  }

  return (
    <div className="today-state-view">
      <div className="state-summary">
        <div className="summary-card">
          <h3>Household Status</h3>
          <p className="stat">📅 {decision.current_state_summary.calendar_events} calendar events</p>
          <p className="stat">✓ {decision.current_state_summary.open_tasks} open tasks</p>
          <p className="stat">🍽️ {decision.current_state_summary.meals_recorded} meals recorded</p>
          {decision.current_state_summary.low_grocery_items.length > 0 && (
            <p className="stat alert">⚠️ Low inventory: {decision.current_state_summary.low_grocery_items.join(', ')}</p>
          )}
        </div>

        <div className="recommendation-card">
          <h3>Recommended Next Action</h3>
          <p className="action-title">{decision.recommended_action.title}</p>
          <p className="action-description">{decision.recommended_action.description}</p>
          {decision.recommended_action.scheduled_for && (
            <p className="action-time">🕐 {decision.recommended_action.scheduled_for}</p>
          )}
          <p className="action-urgency">Urgency: <span className={`urgency-${decision.recommended_action.urgency}`}>{decision.recommended_action.urgency}</span></p>
        </div>

        {decision.reasoning_trace.length > 0 && (
          <div className="reasoning-card">
            <h3>Reasoning</h3>
            <ul className="reasoning-list">
              {decision.reasoning_trace.map((reason, idx) => (
                <li key={idx}>{reason}</li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </div>
  )
}

function ApprovalDrawer({ decision, onApprove, approving }) {
  if (!decision) return null

  return (
    <div className="approval-drawer">
      <div className="drawer-header">
        <h2>Approve Action</h2>
      </div>
      <div className="drawer-body">
        <p className="approval-title">{decision.recommended_action.title}</p>
        <p className="approval-description">{decision.recommended_action.description}</p>
        <div className="approval-details">
          <p><strong>Status:</strong> {decision.recommended_action.approval_status}</p>
          <p><strong>Domain:</strong> {decision.recommended_action.title.split(' ')[0]}</p>
        </div>
      </div>
      <div className="drawer-actions">
        <button
          className="btn btn-primary"
          onClick={() => onApprove(decision.recommended_action.action_id)}
          disabled={approving || decision.recommended_action.approval_status === 'approved'}
        >
          {approving ? 'Approving…' : decision.recommended_action.approval_status === 'approved' ? 'Already Approved' : 'Approve'}
        </button>
      </div>
    </div>
  )
}

export default function HouseholdOS() {
  const [decision, setDecision] = useState(null)
  const [loading, setLoading] = useState(false)
  const [approving, setApproving] = useState(false)
  const [error, setError] = useState(null)
  const [messages, setMessages] = useState([])

  const handleQuerySubmit = async (query) => {
    setLoading(true)
    setError(null)

    try {
      // Add user message
      setMessages((prev) => [...prev, { role: 'user', content: query }])

      const response = await fetch(`${API_BASE}/assistant/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query }),
      })

      if (!response.ok) {
        throw new Error(`Request failed: ${response.status}`)
      }

      const data = await response.json()
      setDecision(data)

      // Add assistant message
      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          content: `I recommend: ${data.recommended_action.title}`,
        },
      ])
    } catch (err) {
      setError(String(err))
      setMessages((prev) => [...prev, { role: 'system', content: `Error: ${err}` }])
    } finally {
      setLoading(false)
    }
  }

  const handleApprove = async (actionId) => {
    if (!decision) return

    setApproving(true)
    setError(null)

    try {
      const response = await fetch(`${API_BASE}/assistant/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          request_id: decision.request_id,
          action_ids: [actionId],
        }),
      })

      if (!response.ok) {
        throw new Error(`Approval failed: ${response.status}`)
      }

      const updated = await response.json()
      setDecision(updated)
      setMessages((prev) => [...prev, { role: 'system', content: 'Action approved and recorded.' }])
    } catch (err) {
      setError(String(err))
    } finally {
      setApproving(false)
    }
  }

  return (
    <div className="household-os-app">
      <header className="app-header">
        <h1>🏠 Household Operating System</h1>
        <p className="tagline">Cross-domain household reasoning. One action at a time.</p>
      </header>

      {error && (
        <div className="error-banner">
          <p>{error}</p>
        </div>
      )}

      <div className="os-layout">
        <div className="chat-panel">
          <ChatView onQuerySubmit={handleQuerySubmit} loading={loading} messages={messages} />
        </div>

        <div className="state-panel">
          <TodayStateView decision={decision} loading={loading} />
        </div>

        <div className="approval-panel">
          <ApprovalDrawer decision={decision} onApprove={handleApprove} approving={approving} />
        </div>
      </div>
    </div>
  )
}
