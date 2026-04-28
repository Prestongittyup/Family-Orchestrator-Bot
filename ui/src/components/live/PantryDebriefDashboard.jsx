import { useCallback, useEffect, useMemo, useState } from 'react'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'
const DEFAULT_FAMILY_ID = import.meta.env.VITE_FAMILY_ID ?? 'default_household'
const EMAIL_NOTIFICATION_PREFIX = 'notif:email_summary:'

function extractEmailId(notification) {
  if (!notification || typeof notification !== 'object') {
    return null
  }

  const notifId = typeof notification.notification_id === 'string'
    ? notification.notification_id
    : ''
  if (notifId.startsWith(EMAIL_NOTIFICATION_PREFIX)) {
    const derived = notifId.slice(EMAIL_NOTIFICATION_PREFIX.length).trim()
    return derived || null
  }

  const title = typeof notification.title === 'string' ? notification.title.toLowerCase() : ''

  const relatedEntity = typeof notification.related_entity === 'string'
    ? notification.related_entity.trim()
    : ''
  if (title.startsWith('email:') && relatedEntity) {
    return relatedEntity
  }

  return null
}

function isEmailDebrief(notification) {
  if (!notification || typeof notification !== 'object') {
    return false
  }

  const title = typeof notification.title === 'string' ? notification.title.toLowerCase() : ''
  const notifId = typeof notification.notification_id === 'string' ? notification.notification_id : ''

  return (
    notifId.startsWith(EMAIL_NOTIFICATION_PREFIX) ||
    title.startsWith('email:')
  )
}

function formatQuantity(item) {
  if (!item || typeof item !== 'object') {
    return ''
  }

  const quantity = typeof item.quantity === 'number' ? item.quantity : Number(item.quantity || 0)
  const unit = typeof item.unit === 'string' ? item.unit : 'unit'

  return `${quantity} ${unit}`
}

export default function PantryDebriefDashboard() {
  const [snapshot, setSnapshot] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [refreshTime, setRefreshTime] = useState(null)

  const loadSignals = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const endpoint = `${API_BASE}/v1/ui/bootstrap?family_id=${encodeURIComponent(DEFAULT_FAMILY_ID)}`
      const response = await fetch(endpoint)
      if (!response.ok) {
        throw new Error(`Failed to load household signals (${response.status})`)
      }

      const data = await response.json()
      setSnapshot(data?.snapshot ?? null)
      setRefreshTime(new Date().toLocaleTimeString())
    } catch (err) {
      setSnapshot(null)
      setError(err?.message || 'Failed to load pantry and email signals')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadSignals()
  }, [loadSignals])

  const pantry = snapshot?.pantry || null
  const lowStockItems = Array.isArray(pantry?.low_stock_items) ? pantry.low_stock_items : []
  const pantrySuggestions = Array.isArray(pantry?.meal_suggestions) ? pantry.meal_suggestions : []
  const notifications = Array.isArray(snapshot?.notifications) ? snapshot.notifications : []

  const emailDebriefs = useMemo(
    () => notifications.filter((notification) => isEmailDebrief(notification)),
    [notifications],
  )

  return (
    <div className="orchestration-dashboard">
      <div className="dashboard-controls">
        <button className="btn btn-primary" onClick={loadSignals} disabled={loading}>
          {loading ? 'Refreshing…' : 'Refresh Pantry + Email'}
        </button>
        {refreshTime && <p className="text-sm text-muted">Last updated: {refreshTime}</p>}
      </div>

      {error ? <p className="error-text">{error}</p> : null}

      <main className="dashboard">
        <section className="panel-row">
          <div style={{ flex: 1 }}>
            <div className="panel">
              <h3>Pantry</h3>
              {!pantry ? (
                <p className="text-muted">No pantry data available yet.</p>
              ) : (
                <>
                  <p className="text-sm text-muted">
                    Low stock items: {pantry.low_stock_count || 0}
                  </p>

                  {lowStockItems.length === 0 ? (
                    <p className="text-muted">No low-stock pantry items right now.</p>
                  ) : (
                    <ul className="list-panel">
                      {lowStockItems.map((item) => (
                        <li key={item.item_name || item.name}>
                          <strong>{item.item_name || item.name || 'Unknown item'}</strong>
                          <p className="text-muted">{formatQuantity(item)}</p>
                        </li>
                      ))}
                    </ul>
                  )}

                  {pantrySuggestions.length > 0 ? (
                    <>
                      <h4>Meal Suggestions</h4>
                      <ul className="list-panel">
                        {pantrySuggestions.map((suggestion) => (
                          <li key={suggestion.suggestion_id || suggestion.title}>
                            <strong>{suggestion.title || 'Suggested meal'}</strong>
                            {suggestion.rationale ? <p>{suggestion.rationale}</p> : null}
                          </li>
                        ))}
                      </ul>
                    </>
                  ) : null}
                </>
              )}
            </div>
          </div>

          <div style={{ flex: 1 }}>
            <div className="panel">
              <h3>Email Debriefing</h3>
              {emailDebriefs.length === 0 ? (
                <p className="text-muted">No debriefing notifications available yet.</p>
              ) : (
                <ul className="list-panel">
                  {emailDebriefs.map((notification) => (
                    <li key={notification.notification_id || notification.title}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', gap: '0.5rem' }}>
                        <strong>{notification.title || 'Email update'}</strong>
                        <span className="text-muted">{notification.level || 'info'}</span>
                      </div>
                      <p>{notification.message || 'No summary available.'}</p>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        </section>
      </main>
    </div>
  )
}