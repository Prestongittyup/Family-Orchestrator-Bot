export default function ConflictMonitor({ brief }) {
  if (!brief) {
    return (
      <div className="card panel-conflicts">
        <div className="card-header">
          <h2>Detected Conflicts</h2>
        </div>
        <p className="empty-state">No conflict data available.</p>
      </div>
    )
  }

  const conflicts = brief.conflicts || []

  return (
    <div className="card panel-conflicts">
      <div className="card-header">
        <h2>Detected Conflicts</h2>
        {conflicts.length > 0 && (
          <span className="badge badge-alert">{conflicts.length} alert{conflicts.length !== 1 ? 's' : ''}</span>
        )}
      </div>

      {conflicts.length === 0 ? (
        <div className="empty-state">
          <span className="check-icon">✓</span>
          <p>No conflicts detected.</p>
        </div>
      ) : (
        <div className="conflicts-list">
          {conflicts.map((conflict, idx) => (
            <div key={idx} className="conflict-item">
              <div className="conflict-header">
                <span className="conflict-icon">⚠️</span>
                <p className="conflict-title">{conflict.title || 'Conflict'}</p>
              </div>
              {conflict.description && (
                <p className="conflict-desc">{conflict.description}</p>
              )}
              {conflict.members && conflict.members.length > 0 && (
                <div className="conflict-members">
                  <p className="text-sm">Involved: {conflict.members.join(', ')}</p>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
