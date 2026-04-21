import { useState, useEffect, useCallback } from 'react'
import HouseholdBriefPanel from './HouseholdBriefPanel'
import PriorityTimeline from './PriorityTimeline'
import ConflictMonitor from './ConflictMonitor'
import MemberTaskView from './MemberTaskView'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'

export default function OrchestrationDashboard() {
  const [brief, setBrief] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [refreshTime, setRefreshTime] = useState(null)

  const loadBrief = useCallback(async () => {
    try {
      setLoading(true)
      setError(null)

      // For now, use a hardcoded household_id. In production, this would come from auth context.
      const householdId = 'default_household'
      const res = await fetch(`${API_BASE}/brief/${householdId}`)

      if (!res.ok) {
        throw new Error(`Failed to load brief (${res.status})`)
      }

      const data = await res.json()

      // Handle both direct brief and brief nested in response
      const briefData = data.brief ? { ...data.brief, generated_at: data.generated_at } : data
      setBrief(briefData)
      setRefreshTime(new Date().toLocaleTimeString())
    } catch (e) {
      setError(e.message || 'Failed to load household brief')
      console.error('Brief load error:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  // Load brief on mount
  useEffect(() => {
    loadBrief()
  }, [loadBrief])

  return (
    <div className="orchestration-dashboard">
      <div className="dashboard-controls">
        <button className="btn btn-primary" onClick={loadBrief} disabled={loading}>
          {loading ? 'Refreshing…' : 'Refresh Brief'}
        </button>
        {refreshTime && (
          <p className="text-sm text-muted">Last updated: {refreshTime}</p>
        )}
      </div>

      <main className="dashboard">
        {/* Row 1: Main brief + timeline */}
        <section className="panel-row">
          <div style={{ flex: 2 }}>
            <HouseholdBriefPanel brief={brief?.brief || brief} loading={loading} error={error} />
          </div>
          <div style={{ flex: 1 }}>
            <PriorityTimeline brief={brief?.brief || brief} />
          </div>
        </section>

        {/* Row 2: Conflicts + Member tasks */}
        <section className="panel-row">
          <div style={{ flex: 1 }}>
            <ConflictMonitor brief={brief?.brief || brief} />
          </div>
          <div style={{ flex: 1.5 }}>
            <MemberTaskView brief={brief?.brief || brief} />
          </div>
        </section>
      </main>
    </div>
  )
}
