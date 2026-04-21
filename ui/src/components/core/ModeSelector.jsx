const MODES = [
  { id: 'LIVE', label: 'Live Orchestration', icon: '🔴' },
  { id: 'SIMULATION', label: 'Simulation', icon: '⚗️' },
  { id: 'INSIGHTS', label: 'Insights', icon: '📊' },
]

export default function ModeSelector({ mode, onModeChange }) {
  return (
    <nav className="mode-selector">
      {MODES.map((m) => (
        <button
          key={m.id}
          className={`mode-btn ${m.id === mode ? 'mode-btn--active' : ''}`}
          onClick={() => onModeChange(m.id)}
          title={m.label}
        >
          <span className="mode-icon">{m.icon}</span>
          <span className="mode-label">{m.label}</span>
        </button>
      ))}
    </nav>
  )
}
