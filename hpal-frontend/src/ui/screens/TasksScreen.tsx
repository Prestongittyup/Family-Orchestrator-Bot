import React from "react";
import { useRuntimeStore } from "../../runtime/store";
import { SyncStatusPill } from "../components/SyncStatusPill";
import { TaskColumn } from "../components/TaskColumn";

export const TasksScreen: React.FC = () => {
  const runtimeState = useRuntimeStore((state) => state.runtimeState);

  if (!runtimeState) {
    return <section className="screen-panel">Loading tasks...</section>;
  }

  return (
    <section className="screen-panel">
      <header className="screen-header">
        <h2>Task Board</h2>
        <SyncStatusPill status={runtimeState.sync_status} />
      </header>

      <div className="task-grid">
        <TaskColumn title="Pending" tasks={runtimeState.snapshot.task_board.pending} />
        <TaskColumn title="In Progress" tasks={runtimeState.snapshot.task_board.in_progress} />
        <TaskColumn title="Completed" tasks={runtimeState.snapshot.task_board.completed} />
        <TaskColumn title="Failed" tasks={runtimeState.snapshot.task_board.failed} />
      </div>
    </section>
  );
};
