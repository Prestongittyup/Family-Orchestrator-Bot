import { useCallback, useEffect, useMemo, useState } from 'react'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'
const SYNTHETIC_FALLBACK_ALLOWED = String(
  import.meta.env.VITE_COZI_ALLOW_SYNTHETIC_FALLBACK || '',
).toLowerCase() === 'true'

const SAMPLE_EVENTS = [
  { title: 'School drop-off and library return', time: '7:45 AM', assigned_to: 'Alex' },
  { title: 'Team sync and meal prep block', time: '12:00 PM', assigned_to: 'Jordan' },
  { title: 'Soccer pickup and homework check', time: '5:30 PM', assigned_to: 'Family' },
  { title: 'Family dinner and planning huddle', time: '7:00 PM', assigned_to: 'Everyone' },
]

const SAMPLE_TASKS = {
  Alex: [
    { title: 'Sign field trip form' },
    { title: 'Confirm dentist appointment' },
    { title: 'Pack soccer bag' },
  ],
  Jordan: [
    { title: 'Finalize grocery order' },
    { title: 'Move laundry to dryer' },
    { title: 'Refill pet medication' },
  ],
  Kids: [
    { title: 'Homework review' },
    { title: 'Set out tomorrow clothes' },
  ],
}

const SAMPLE_CONFLICTS = [
  {
    title: 'Pickup overlaps with gym window',
    description: 'Shift workout block by 45 minutes to avoid pickup crunch.',
    members: ['Alex', 'Jordan'],
  },
]

function normalizeTasksByMember(brief) {
  if (!brief) {
    return {}
  }

  if (brief.tasks_by_member && typeof brief.tasks_by_member === 'object') {
    return brief.tasks_by_member
  }

  if (brief.task_allocation && typeof brief.task_allocation === 'object') {
    return brief.task_allocation
  }

  return {}
}

function toAgendaItemsFromBrief(brief) {
  if (!brief) {
    return []
  }

  const agendaSources = [
    brief.top_events,
    brief.today_events,
    brief.priorities,
    brief.scheduled_actions,
  ]

  for (const source of agendaSources) {
    if (Array.isArray(source) && source.length > 0) {
      return source
    }
  }

  return []
}

function toAgendaItemsFromOperational(operational) {
  if (!operational || !Array.isArray(operational.schedule_actions)) {
    return []
  }

  return operational.schedule_actions.map((item) => ({
    title: item.action,
    time: item.time,
    assigned_to: 'Operational pipeline',
  }))
}

function normalizeConflicts(raw) {
  if (!Array.isArray(raw)) {
    return []
  }

  const normalized = []
  raw.forEach((entry) => {
    if (Array.isArray(entry) && entry.length >= 2) {
      const first = entry[0] || {}
      const second = entry[1] || {}
      normalized.push({
        title: `${first.title || 'Event A'} overlaps ${second.title || 'Event B'}`,
        description: 'Decision engine detected schedule overlap in canonical pipeline output.',
      })
      return
    }

    if (entry && typeof entry === 'object') {
      normalized.push({
        title: entry.title || entry.conflict_type || 'Scheduling conflict',
        description: entry.description || 'Potential overlap detected in the canonical output.',
        members: entry.members,
      })
      return
    }

    if (typeof entry === 'string') {
      normalized.push({
        title: 'Scheduling conflict',
        description: entry,
      })
    }
  })

  return normalized
}

function pickSyntheticList(primary, synthetic, enabled) {
  const hasPrimary = Array.isArray(primary) && primary.length > 0
  if (hasPrimary) {
    return { values: primary, syntheticUsed: false }
  }
  if (enabled) {
    return { values: synthetic, syntheticUsed: true }
  }
  return { values: [], syntheticUsed: false }
}

function pickSyntheticTaskMap(primary, enabled) {
  const hasPrimary = primary && Object.keys(primary).length > 0
  if (hasPrimary) {
    return { values: primary, syntheticUsed: false }
  }
  if (enabled) {
    return { values: SAMPLE_TASKS, syntheticUsed: true }
  }
  return { values: {}, syntheticUsed: false }
}

function agendaTitle(item) {
  if (typeof item === 'string') {
    return item
  }
  return item?.title || item?.action || 'Household activity'
}

function agendaTime(item) {
  if (typeof item === 'string') {
    return 'Anytime'
  }
  return item?.time || item?.start_time || item?.start || 'Anytime'
}

function agendaOwner(item) {
  if (typeof item === 'string') {
    return 'Family'
  }
  return item?.assigned_to || item?.owner || item?.source_module || 'Family'
}

function buildWeekBuckets(events) {
  const labels = []
  for (let offset = 0; offset < 5; offset += 1) {
    const day = new Date()
    day.setDate(day.getDate() + offset)
    labels.push(day.toLocaleDateString(undefined, { weekday: 'short' }))
  }

  const buckets = labels.map((label) => ({ label, items: [] }))
  events.forEach((event, idx) => {
    buckets[idx % buckets.length].items.push(event)
  })

  return buckets
}

export default function CoziDashboard() {
  const [briefPayload, setBriefPayload] = useState(null)
  const [operationalContext, setOperationalContext] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [refreshTime, setRefreshTime] = useState(null)
  const [pipelineStatus, setPipelineStatus] = useState('idle')

  const syntheticQueryEnabled = useMemo(() => {
    if (typeof window === 'undefined') {
      return false
    }
    return new URLSearchParams(window.location.search).get('synthetic') === '1'
  }, [])

  const syntheticEnabled = SYNTHETIC_FALLBACK_ALLOWED || syntheticQueryEnabled

  const loadBrief = useCallback(async () => {
    setLoading(true)
    setError(null)
    setPipelineStatus('loading')
    try {
      const householdId = 'default_household'
      const briefUrl = `${API_BASE}/brief/${householdId}?validate_contract_v1=true&include_observability=true`
      const contextUrl = `${API_BASE}/operational/context?household_id=${encodeURIComponent(householdId)}`

      const [briefResponse, contextResponse] = await Promise.all([
        fetch(briefUrl),
        fetch(contextUrl),
      ])

      if (!briefResponse.ok) {
        throw new Error(`Failed to load canonical brief (${briefResponse.status})`)
      }
      if (!contextResponse.ok) {
        throw new Error(`Failed to load operational context (${contextResponse.status})`)
      }

      const [briefData, contextData] = await Promise.all([
        briefResponse.json(),
        contextResponse.json(),
      ])

      setBriefPayload(briefData)
      setOperationalContext(contextData)
      setPipelineStatus('canonical')
      setRefreshTime(new Date().toLocaleTimeString())
    } catch (err) {
      setError(err.message || 'Unable to load dashboard data')
      setBriefPayload(null)
      setOperationalContext(null)
      setPipelineStatus('error')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadBrief()
  }, [loadBrief])

  const activeBrief = briefPayload?.brief || null

  const canonicalAgenda = useMemo(() => {
    const fromBrief = toAgendaItemsFromBrief(activeBrief)
    if (fromBrief.length > 0) {
      return fromBrief
    }
    return toAgendaItemsFromOperational(operationalContext)
  }, [activeBrief, operationalContext])

  const canonicalConflicts = useMemo(() => {
    const fromBrief = normalizeConflicts(activeBrief?.conflicts)
    if (fromBrief.length > 0) {
      return fromBrief
    }
    return normalizeConflicts(operationalContext?.conflicts)
  }, [activeBrief, operationalContext])

  const canonicalTasksByMember = useMemo(() => normalizeTasksByMember(activeBrief), [activeBrief])

  const agendaSelection = useMemo(
    () => pickSyntheticList(canonicalAgenda, SAMPLE_EVENTS, syntheticEnabled),
    [canonicalAgenda, syntheticEnabled],
  )
  const conflictSelection = useMemo(
    () => pickSyntheticList(canonicalConflicts, SAMPLE_CONFLICTS, syntheticEnabled),
    [canonicalConflicts, syntheticEnabled],
  )
  const taskSelection = useMemo(
    () => pickSyntheticTaskMap(canonicalTasksByMember, syntheticEnabled),
    [canonicalTasksByMember, syntheticEnabled],
  )

  const agendaItems = agendaSelection.values
  const conflicts = conflictSelection.values
  const tasksByMember = taskSelection.values
  const memberEntries = useMemo(() => Object.entries(tasksByMember), [tasksByMember])
  const weekBuckets = useMemo(() => buildWeekBuckets(agendaItems), [agendaItems])

  const syntheticOverlayUsed =
    agendaSelection.syntheticUsed || conflictSelection.syntheticUsed || taskSelection.syntheticUsed

  const summaryText =
    activeBrief?.summary ||
    operationalContext?.system_notes?.[0] ||
    'Canonical integration pipeline is connected. This board is ready for real data wiring.'

  const cacheState = briefPayload?.debug?.cache_state || 'unknown'
  const contractState = briefPayload ? 'validated-on-request' : 'not-loaded'

  return (
    <div className="cozi-dashboard">
      <section className="cozi-card cozi-hero cozi-stagger-1">
        <div className="cozi-hero-copy">
          <p className="cozi-eyebrow">Family organizer prototype</p>
          <h2 className="cozi-title">Cozi-style command board</h2>
          <p className="cozi-summary">{summaryText}</p>
          <div className="cozi-source-rail">
            <span className={`cozi-chip ${pipelineStatus === 'canonical' ? 'cozi-chip--ok' : 'cozi-chip--warn'}`}>
              Pipeline: {pipelineStatus}
            </span>
            <span className="cozi-chip">Cache state: {cacheState}</span>
            <span className="cozi-chip">Contract: {contractState}</span>
            <span className={`cozi-chip ${syntheticEnabled ? 'cozi-chip--warn' : 'cozi-chip--off'}`}>
              Synthetic fallback: {syntheticEnabled ? 'enabled' : 'disabled'}
            </span>
            {syntheticOverlayUsed && <span className="cozi-chip cozi-chip--warn">Synthetic overlay active</span>}
          </div>
          <div className="cozi-chip-row">
            <span className="cozi-chip">Agenda items: {agendaItems.length}</span>
            <span className="cozi-chip">Conflicts: {conflicts.length}</span>
            <span className="cozi-chip">Family members: {memberEntries.length}</span>
          </div>
          {!syntheticEnabled && agendaItems.length === 0 && (
            <p className="cozi-guard-note">
              No synthetic placeholders are shown. Add <code>?synthetic=1</code> to the URL only when intentionally
              testing UI fallback behavior.
            </p>
          )}
        </div>

        <div className="cozi-hero-actions">
          <button className="cozi-refresh-btn" onClick={loadBrief} disabled={loading}>
            {loading ? 'Refreshing board...' : 'Refresh board'}
          </button>
          <p className="cozi-updated-text">Updated {refreshTime || 'just now'}</p>
          {error && <p className="cozi-error-text">{error}</p>}
        </div>
      </section>

      <section className="cozi-main-grid">
        <article className="cozi-card cozi-stagger-2">
          <header className="cozi-card-header">
            <h3>Today agenda</h3>
          </header>
          <div className="cozi-agenda-list">
            {agendaItems.map((item, index) => (
              <div className="cozi-agenda-item" key={`${agendaTitle(item)}-${index}`}>
                <div className="cozi-time-pill">{agendaTime(item)}</div>
                <div>
                  <p className="cozi-item-title">{agendaTitle(item)}</p>
                  <p className="cozi-item-sub">Owner: {agendaOwner(item)}</p>
                </div>
              </div>
            ))}
            {agendaItems.length === 0 && (
              <p className="cozi-item-sub">No canonical agenda items are currently available.</p>
            )}
          </div>
        </article>

        <article className="cozi-card cozi-stagger-3">
          <header className="cozi-card-header">
            <h3>Family checklists</h3>
          </header>
          <div className="cozi-member-list">
            {memberEntries.map(([member, tasks]) => (
              <section className="cozi-member-card" key={member}>
                <h4>{member}</h4>
                <ul>
                  {(tasks || []).slice(0, 4).map((task, idx) => (
                    <li key={`${member}-${idx}`}>
                      <span className="cozi-task-dot" />
                      <span>{typeof task === 'string' ? task : task.title || 'Task'}</span>
                    </li>
                  ))}
                </ul>
              </section>
            ))}
            {memberEntries.length === 0 && (
              <p className="cozi-item-sub">No member task allocations returned by canonical pipeline.</p>
            )}
          </div>
        </article>

        <article className="cozi-card cozi-stagger-4">
          <header className="cozi-card-header">
            <h3>Conflict radar</h3>
          </header>
          <div className="cozi-conflict-list">
            {conflicts.map((conflict, idx) => (
              <div className="cozi-conflict" key={`${conflict.title || 'conflict'}-${idx}`}>
                <p className="cozi-item-title">{conflict.title || conflict.conflict_type || 'Scheduling conflict'}</p>
                <p className="cozi-item-sub">{conflict.description || 'Potential overlap detected in the day plan.'}</p>
                {Array.isArray(conflict.members) && conflict.members.length > 0 && (
                  <p className="cozi-people">Involved: {conflict.members.join(', ')}</p>
                )}
              </div>
            ))}
            {conflicts.length === 0 && (
              <p className="cozi-item-sub">No canonical conflicts reported.</p>
            )}
          </div>
        </article>
      </section>

      <section className="cozi-card cozi-stagger-5">
        <header className="cozi-card-header">
          <h3>Week at a glance</h3>
        </header>
        <div className="cozi-week-grid">
          {weekBuckets.map((bucket) => (
            <div className="cozi-week-column" key={bucket.label}>
              <p className="cozi-week-label">{bucket.label}</p>
              {bucket.items.length === 0 ? (
                <p className="cozi-item-sub">Open day</p>
              ) : (
                bucket.items.slice(0, 3).map((item, idx) => (
                  <div className="cozi-week-item" key={`${bucket.label}-${idx}`}>
                    <p className="cozi-item-title">{item.title || 'Activity'}</p>
                    <p className="cozi-item-sub">{item.time || 'Flexible time'}</p>
                  </div>
                ))
              )}
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}
