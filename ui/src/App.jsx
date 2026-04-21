import { useState } from 'react'
import TopNav from './components/core/TopNav'
import ModeSelector from './components/core/ModeSelector'
import OrchestrationDashboard from './components/live/OrchestrationDashboard'
import ScenarioRunner from './components/simulation/ScenarioRunner'
import EvaluationDashboard from './components/insights/EvaluationDashboard'

export default function App() {
  const [mode, setMode] = useState('LIVE')

  return (
    <div className="app">
      <TopNav />
      <ModeSelector mode={mode} onModeChange={setMode} />

      {mode === 'LIVE' && <OrchestrationDashboard />}
      {mode === 'SIMULATION' && <ScenarioRunner />}
      {mode === 'INSIGHTS' && <EvaluationDashboard />}
    </div>
  )
}
