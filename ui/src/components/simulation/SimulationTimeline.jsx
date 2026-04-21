export default function SimulationTimeline({ result }) {
  const timeline = result?.event_timeline ?? []
  const evolution = result?.brief_evolution ?? []

  return (
    <div className="card simulation-timeline-panel">
      <div className="card-header">
        <h2>Simulation Timeline</h2>
        <span className="badge badge-neutral">{timeline.length} events</span>
      </div>

      {timeline.length === 0 ? (
        <p className="empty-state">Run simulation to view event injection order and brief evolution.</p>
      ) : (
        <div className="sim-timeline-list">
          {timeline.slice(0, 30).map((evt) => {
            const evo = evolution.find((row) => row.event_id === evt.event_id)
            return (
              <div key={evt.event_id} className="sim-timeline-item">
                <div className="sim-timeline-top">
                  <strong>{evt.title}</strong>
                  <span className="badge badge-blue">{evt.type}</span>
                </div>
                <p className="text-sm text-muted">{evt.timestamp}</p>
                {evo && (
                  <p className="text-sm">
                    Decision changes: +{evo.added_priorities?.length ?? 0} / -{evo.removed_priorities?.length ?? 0}
                  </p>
                )}
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
