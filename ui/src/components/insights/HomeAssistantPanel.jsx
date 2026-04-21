import { useCallback, useEffect, useState } from 'react'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'

function PriorityBadge({ priority }) {
  return <span className={`pmr-priority pmr-priority--${priority}`}>{priority}</span>
}

function PolicyCard({ policy }) {
  const [open, setOpen] = useState(false)

  return (
    <div className="pmr-policy-card">
      <button className="pmr-policy-toggle" onClick={() => setOpen((value) => !value)}>
        <div>
          <p className="pmr-policy-type">{policy.policy_type.replace(/_/g, ' ')}</p>
          <p className="pmr-policy-description">{policy.description}</p>
        </div>
        <div className="pmr-policy-meta">
          <span className="pmr-confidence">{policy.confidence}</span>
          <span className="pmr-expand">{open ? '−' : '+'}</span>
        </div>
      </button>
      {open && (
        <div className="pmr-policy-body">
          <p className="pmr-body-label">Reasoning</p>
          <p className="pmr-policy-reasoning">{policy.reasoning}</p>
          <p className="pmr-body-label">Impact Areas</p>
          <div className="pmr-chip-row">
            {(policy.impact_area ?? []).map((area) => (
              <span className="pmr-chip" key={area}>{area}</span>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function MemoryGroup({ label, items }) {
  return (
    <div className="pmr-memory-group">
      <div className="pmr-memory-header">
        <h3>{label}</h3>
        <span className="badge badge-neutral">{items.length}</span>
      </div>
      <pre className="pmr-memory-json">{JSON.stringify(items, null, 2)}</pre>
    </div>
  )
}

export default function HomeAssistantPanel() {
  const [policyData, setPolicyData] = useState(null)
  const [memoryData, setMemoryData] = useState(null)
  const [itineraryData, setItineraryData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [recomputing, setRecomputing] = useState(false)

  const loadAll = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [policyResponse, memoryResponse, itineraryResponse] = await Promise.all([
        fetch(`${API_BASE}/policy/summary`),
        fetch(`${API_BASE}/policy/memory`),
        fetch(`${API_BASE}/policy/itinerary`),
      ])

      if (!policyResponse.ok || !memoryResponse.ok || !itineraryResponse.ok) {
        throw new Error('Failed to load Home Assistant policy surfaces')
      }

      const [policyJson, memoryJson, itineraryJson] = await Promise.all([
        policyResponse.json(),
        memoryResponse.json(),
        itineraryResponse.json(),
      ])

      setPolicyData(policyJson)
      setMemoryData(memoryJson)
      setItineraryData(itineraryJson)
    } catch (err) {
      setError(String(err))
      setPolicyData(null)
      setMemoryData(null)
      setItineraryData(null)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadAll()
  }, [loadAll])

  const recompute = useCallback(async () => {
    setRecomputing(true)
    setError(null)
    try {
      const response = await fetch(`${API_BASE}/policy/recompute`, { method: 'POST' })
      if (!response.ok) {
        throw new Error(`Policy recompute failed (${response.status})`)
      }
      await loadAll()
    } catch (err) {
      setError(String(err))
    } finally {
      setRecomputing(false)
    }
  }, [loadAll])

  return (
    <div className="pmr-panel">
      <div className="card">
        <div className="card-header">
          <h2>Home Assistant</h2>
          <div className="pmr-toolbar">
            <button className="btn btn-ghost" onClick={loadAll}>Refresh</button>
            <button className="btn btn-primary" onClick={recompute} disabled={recomputing}>
              {recomputing ? 'Recomputing…' : 'Recompute'}
            </button>
          </div>
        </div>
      </div>

      {loading && (
        <div className="center-state">
          <div className="spinner" />
          <p>Loading Home Assistant recommendations…</p>
        </div>
      )}

      {!loading && error && (
        <div className="center-state error-state">
          <p>{error}</p>
          <button className="btn btn-primary" onClick={loadAll}>Retry</button>
        </div>
      )}

      {!loading && !error && policyData && memoryData && itineraryData && (
        <div className="pmr-grid">
          <div className="card">
            <div className="card-header">
              <h2>Daily Itinerary Panel</h2>
            </div>
            <div className="pmr-timeline">
              {(itineraryData.recommended_itinerary ?? []).map((block) => (
                <div className={`pmr-timeline-item pmr-timeline-item--${block.priority}`} key={`${block.time_block}-${block.event}`}>
                  <div className="pmr-time-block">{block.time_block}</div>
                  <div className="pmr-timeline-content">
                    <div className="pmr-timeline-top">
                      <p className="pmr-event-title">{block.event}</p>
                      <PriorityBadge priority={block.priority} />
                    </div>
                    <p className="pmr-event-reason">{block.reason}</p>
                  </div>
                </div>
              ))}
            </div>
            <div className="pmr-note-strip">
              <p className="pmr-body-label">Optimization Notes</p>
              <ul className="pmr-note-list">
                {(itineraryData.optimization_notes ?? []).map((note) => (
                  <li key={note}>{note}</li>
                ))}
              </ul>
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Policy Suggestions Panel</h2>
            </div>
            <div className="pmr-policy-list">
              {(policyData.policies ?? []).map((policy) => (
                <PolicyCard key={policy.policy_type} policy={policy} />
              ))}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <h2>Household Memory Panel</h2>
            </div>
            <div className="pmr-memory-panel">
              <MemoryGroup label="Preferences" items={memoryData.memory?.preferences ?? []} />
              <MemoryGroup label="Patterns" items={memoryData.memory?.patterns ?? []} />
              <MemoryGroup label="Constraints" items={memoryData.memory?.constraints ?? []} />
              <MemoryGroup label="Routines" items={memoryData.memory?.routines ?? []} />
            </div>
          </div>
        </div>
      )}
    </div>
  )
}