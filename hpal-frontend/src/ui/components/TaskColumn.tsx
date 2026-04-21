import React from "react";
import type { TaskSummary } from "../../api/contracts";

interface TaskColumnProps {
  title: string;
  tasks: TaskSummary[];
}

export const TaskColumn: React.FC<TaskColumnProps> = ({ title, tasks }) => {
  return (
    <section className="task-column" aria-label={title}>
      <h3>{title}</h3>
      {tasks.length === 0 ? <p className="empty-text">No tasks</p> : null}
      <ul>
        {tasks.map((task) => (
          <li key={task.task_id} className="task-item">
            <p className="task-title">{task.title}</p>
            <p className="task-meta">
              {task.assigned_to} | {task.priority}
            </p>
            <p className="task-meta">{task.plan_id}</p>
          </li>
        ))}
      </ul>
    </section>
  );
};
