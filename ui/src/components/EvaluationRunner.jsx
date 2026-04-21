import { useState } from 'react'

const STATUS_LABELS = {
  idle: 'Run Evaluation',
  running: 'Running…',
  success: 'Run Evaluation',
  error: 'Run Evaluation',
}

const STATUS_CLASSES = {
  idle: 'btn btn-primary',
  running: 'btn btn-primary btn-disabled',
  success: 'btn btn-success',
  error: 'btn btn-error',
}

export default function EvaluationRunner({ runState, onRun }) {
  const [showLog, setShowLog] = useState(false)
  const { status, timestamp, output } = runState

  const fmtTs = timestamp
    ? new Date(timestamp).toLocaleTimeString()
    : null

  return (
    <div className="runner">
      <div className="runner-controls">
        <button
          className={STATUS_CLASSES[status]}
          disabled={status === 'running'}
          onClick={onRun}
        >
          {status === 'running' && <span className="spinner-sm" />}
          {STATUS_LABELS[status]}
        </button>

        {output && (
          <button
            className="btn btn-ghost"
            onClick={() => setShowLog(!showLog)}
            title="Toggle test output"
          >
            {showLog ? 'Hide log' : 'Show log'}
          </button>
        )}
      </div>

      <div className="runner-meta">
        {status === 'success' && fmtTs && (
          <span className="runner-status runner-status--success">✓ Passed · {fmtTs}</span>
        )}
        {status === 'error' && (
          <span className="runner-status runner-status--error">✗ Failed{fmtTs ? ` · ${fmtTs}` : ''}</span>
        )}
        {status === 'running' && (
          <span className="runner-status runner-status--running">Running pytest…</span>
        )}
      </div>

      {showLog && output && (
        <pre className="runner-log">{output}</pre>
      )}
    </div>
  )
}
