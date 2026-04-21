import { useState } from 'react'

const PRESETS = [
  'school_work_balance',
  'after_school_rush',
  'health_interruption_day',
  'weekend_overload',
]

export default function SimulationControlPanel({ onRun, running }) {
  const [householdSize, setHouseholdSize] = useState(4)
  const [chaosLevel, setChaosLevel] = useState('medium')
  const [eventDensity, setEventDensity] = useState(18)
  const [scenarioPreset, setScenarioPreset] = useState('school_work_balance')

  const handleSubmit = () => {
    onRun({
      household_size: Number(householdSize),
      chaos_level: chaosLevel,
      event_density: Number(eventDensity),
      scenario_preset: scenarioPreset,
      seed: 42,
    })
  }

  return (
    <div className="card simulation-control-panel">
      <div className="card-header">
        <h2>Simulation Control</h2>
      </div>

      <div className="sim-form-grid">
        <label className="sim-field">
          <span>Household Size</span>
          <input
            type="number"
            min="1"
            max="10"
            value={householdSize}
            onChange={(e) => setHouseholdSize(e.target.value)}
          />
        </label>

        <label className="sim-field">
          <span>Chaos Level</span>
          <select value={chaosLevel} onChange={(e) => setChaosLevel(e.target.value)}>
            <option value="low">Low</option>
            <option value="medium">Medium</option>
            <option value="high">High</option>
          </select>
        </label>

        <label className="sim-field">
          <span>Event Density ({eventDensity})</span>
          <input
            type="range"
            min="6"
            max="40"
            value={eventDensity}
            onChange={(e) => setEventDensity(e.target.value)}
          />
        </label>

        <label className="sim-field">
          <span>Scenario Preset</span>
          <select value={scenarioPreset} onChange={(e) => setScenarioPreset(e.target.value)}>
            {PRESETS.map((preset) => (
              <option key={preset} value={preset}>{preset}</option>
            ))}
          </select>
        </label>
      </div>

      <div className="sim-actions">
        <button className="btn btn-primary" disabled={running} onClick={handleSubmit}>
          {running ? 'Running…' : 'Run Live Simulation'}
        </button>
      </div>
    </div>
  )
}
