import { useState, useEffect, useCallback } from 'react'
import ScenarioTable from '../ScenarioTable'
import ScenarioDetail from '../ScenarioDetail'
import MetricsPanel from '../MetricsPanel'
import DecisionPanel from '../DecisionPanel'
import ComparisonPanel from '../ComparisonPanel'

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '/api'

export default function EvaluationModePanel() {
  const [results, setResults] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selectedScenario, setSelectedScenario] = useState(null)

  const loadResults = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/evaluation_results.json`)
      if (!res.ok) {
        setError('Failed to load evaluation_results.json')
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

  if (loading) {
    return (
      <div className="center-state">
        <div className="spinner" />
        <p>Loading evaluation results…</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="center-state error-state">
        <span className="error-icon">⚠️</span>
        <p>{error}</p>
        <button className="btn btn-primary" onClick={loadResults}>Retry</button>
      </div>
    )
  }

  if (!results) {
    return <p className="empty-state">No evaluation data available.</p>
  }

  return (
    <main className="dashboard">
      <section className="panel-row">
        <div className="card panel-scenarios">
          <div className="card-header">
            <h2>Scenarios</h2>
            <span className="badge badge-neutral">{results.scenarios?.length ?? 0} runs</span>
          </div>
          <ScenarioTable
            scenarios={results.scenarios ?? []}
            selected={selectedScenario}
            onSelect={setSelectedScenario}
          />
        </div>

        <div className="card panel-detail">
          <div className="card-header">
            <h2>Scenario Detail</h2>
            {selectedScenario && (
              <span className="badge badge-blue">{selectedScenario.scenario_id}</span>
            )}
          </div>
          <ScenarioDetail scenario={selectedScenario} />
        </div>
      </section>

      <section className="panel-row">
        <div className="card panel-metrics">
          <div className="card-header">
            <h2>Aggregate Metrics</h2>
          </div>
          <MetricsPanel aggregate={results.aggregate ?? {}} />
        </div>

        <div className="card panel-decision">
          <div className="card-header">
            <h2>Decision Intelligence</h2>
          </div>
          <DecisionPanel
            failurePatterns={results.failure_patterns ?? []}
            decisionGaps={results.decision_gaps ?? []}
            recommendations={results.recommended_adjustments ?? []}
          />
        </div>
      </section>

      {results.comparison && (
        <section className="panel-row panel-row-full">
          <div className="card">
            <div className="card-header">
              <h2>Run Comparison</h2>
            </div>
            <ComparisonPanel comparison={results.comparison} />
          </div>
        </section>
      )}
    </main>
  )
}
