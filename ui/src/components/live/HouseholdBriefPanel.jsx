export default function HouseholdBriefPanel({ brief, loading, error }) {
  if (loading) {
    return (
      <div className="card panel-brief">
        <div className="card-header">
          <h2>Today's Brief</h2>
        </div>
        <div className="loading-state">
          <div className="spinner" />
          <p>Loading household status…</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="card panel-brief">
        <div className="card-header">
          <h2>Today's Brief</h2>
        </div>
        <div className="error-state">
          <span className="error-icon">⚠️</span>
          <p>{error}</p>
        </div>
      </div>
    )
  }

  if (!brief) {
    return (
      <div className="card panel-brief">
        <div className="card-header">
          <h2>Today's Brief</h2>
        </div>
        <p className="empty-state">No brief data available.</p>
      </div>
    )
  }

  const { summary, generated_at } = brief
  const generatedTime = generated_at
    ? new Date(generated_at).toLocaleTimeString()
    : 'unknown'

  return (
    <div className="card panel-brief">
      <div className="card-header">
        <h2>Today's Brief</h2>
        <span className="badge badge-neutral">Updated {generatedTime}</span>
      </div>

      <div className="brief-summary">
        {summary ? (
          <p>{summary}</p>
        ) : (
          <p className="empty-state">No summary available.</p>
        )}
      </div>

      {brief.debug && (
        <div className="brief-debug">
          <p className="text-sm text-muted">Cache: {brief.debug.cache_state}</p>
        </div>
      )}
    </div>
  )
}
