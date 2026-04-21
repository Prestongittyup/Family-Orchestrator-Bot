import { useState, useEffect, useCallback } from 'react'
import EvaluationRunner from '../EvaluationRunner'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'

export default function ScenarioRunner() {
  const [results, setResults] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [runState, setRunState] = useState({ status: 'idle', timestamp: null, output: '' })

  const loadResults = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/evaluation_results.json`)
      if (!res.ok) {
        setError('Failed to load evaluation results')
        return
      }
      setError(null)
      setResults(await res.json())
    } catch (e) {
      setError('Cannot reach backend API for evaluation results.')
      console.error('Error loading results:', e)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    loadResults()
  }, [loadResults])

  const handleRun = useCallback(async () => {
    setRunState({ status: 'running', timestamp: null, output: '' })
    try {
      const res = await fetch(`${API_BASE}/evaluation/run`)
      const data = await res.json()
      setRunState({
        status: data.status === 'success' ? 'success' : 'error',
        timestamp: new Date().toISOString(),
        output: data.summary || '',
      })
      await loadResults()
    } catch (e) {
      setRunState({ status: 'error', timestamp: null, output: String(e) })
    }
  }, [loadResults])

  return (
    <div className="scenario-runner">
      <div className="runner-header">
        <h2>Scenario Simulation</h2>
        <p className="text-muted">Run synthetic household scenarios to test orchestration logic</p>
      </div>

      <div className="runner-controls-section">
        <EvaluationRunner runState={runState} onRun={handleRun} />
      </div>

      {loading && (
        <div className="center-state">
          <div className="spinner" />
          <p>Loading scenario results…</p>
        </div>
      )}

      {!loading && error && (
        <div className="center-state error-state">
          <span className="error-icon">⚠️</span>
          <p>{error}</p>
          <button className="btn btn-primary" onClick={loadResults}>
            Retry
          </button>
        </div>
      )}

      {!loading && !error && results && (
        <div className="scenario-results">
          <div className="card">
            <div className="card-header">
              <h3>Simulation Summary</h3>
              <span className="badge badge-neutral">
                {results.scenarios?.length ?? 0} scenarios
              </span>
            </div>
            <div className="scenario-summary">
              <p>Scenario execution data available. Use Insights mode for detailed analysis.</p>
              <p className="text-sm text-muted">
                Last updated: {results.generated_at ? new Date(results.generated_at).toLocaleString() : 'unknown'}
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
