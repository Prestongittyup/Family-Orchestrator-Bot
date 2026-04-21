import type { ChatSessionState, FrontendState } from "./types";

export function selectTaskCounts(state: FrontendState): {
  pending: number;
  inProgress: number;
  completed: number;
  failed: number;
} {
  return {
    pending: state.snapshot.task_board.pending.length,
    inProgress: state.snapshot.task_board.in_progress.length,
    completed: state.snapshot.task_board.completed.length,
    failed: state.snapshot.task_board.failed.length,
  };
}

export function selectCalendarEvents(state: FrontendState) {
  return [...state.snapshot.calendar.events].sort((a, b) => a.start.localeCompare(b.start));
}

export function selectNotifications(state: FrontendState) {
  return [...state.snapshot.notifications].sort((a, b) => a.level.localeCompare(b.level));
}

export function selectChatSession(state: FrontendState, sessionId: string): ChatSessionState {
  return (
    state.chat_sessions[sessionId] ?? {
      session_id: sessionId,
      message_history: [],
      pending_action_cards: [],
      last_ui_patch: [],
      awaiting_confirmation: false,
    }
  );
}
