export default function MemberTaskView({ brief }) {
  if (!brief) {
    return (
      <div className="card panel-members">
        <div className="card-header">
          <h2>Tasks by Member</h2>
        </div>
        <p className="empty-state">No member data available.</p>
      </div>
    )
  }

  const tasksByMember = brief.tasks_by_member || {}
  const members = Object.keys(tasksByMember)

  return (
    <div className="card panel-members">
      <div className="card-header">
        <h2>Tasks by Member</h2>
        <span className="badge badge-neutral">{members.length} member{members.length !== 1 ? 's' : ''}</span>
      </div>

      {members.length === 0 ? (
        <p className="empty-state">No member tasks.</p>
      ) : (
        <div className="members-grid">
          {members.map((member) => {
            const tasks = tasksByMember[member] || []
            return (
              <div key={member} className="member-card">
                <div className="member-header">
                  <span className="member-icon">👤</span>
                  <h3>{member}</h3>
                </div>
                <div className="member-task-list">
                  {tasks.length === 0 ? (
                    <p className="text-sm text-muted">No tasks</p>
                  ) : (
                    <ul>
                      {tasks.map((task, idx) => (
                        <li key={idx} className="member-task">
                          <span className="task-indicator">•</span>
                          <span>{task.title || task}</span>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
                <div className="member-status">
                  <span className="status-badge">
                    {tasks.length} task{tasks.length !== 1 ? 's' : ''}
                  </span>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
