export default function PriorityTimeline({ brief }) {
  if (!brief) {
    return (
      <div className="card panel-timeline">
        <div className="card-header">
          <h2>Priority Timeline</h2>
        </div>
        <p className="empty-state">No timeline data available.</p>
      </div>
    )
  }

  // Extract priorities from brief if available
  const priorities = brief.priorities || []

  return (
    <div className="card panel-timeline">
      <div className="card-header">
        <h2>Priority Timeline</h2>
        <span className="badge badge-neutral">{priorities.length} items</span>
      </div>

      {priorities.length === 0 ? (
        <p className="empty-state">No priorities scheduled.</p>
      ) : (
        <div className="timeline-list">
          {priorities.map((item, idx) => (
            <div key={idx} className="timeline-item">
              <div className="timeline-marker" />
              <div className="timeline-content">
                <p className="timeline-title">{item.title || 'Untitled'}</p>
                {item.time && (
                  <p className="timeline-time">{item.time}</p>
                )}
                {item.assigned_to && (
                  <p className="timeline-member">👤 {item.assigned_to}</p>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
