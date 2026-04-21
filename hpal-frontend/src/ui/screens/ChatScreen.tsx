import React from "react";
import { useRuntimeStore } from "../../runtime/store";
import { selectChatSession } from "../../runtime/selectors";
import { ChatPanel } from "../components/ChatPanel";
import { SyncStatusPill } from "../components/SyncStatusPill";

const DEFAULT_SESSION_ID = "main-ui-session";

export const ChatScreen: React.FC = () => {
  const [message, setMessage] = React.useState("");
  const runtimeState = useRuntimeStore((state) => state.runtimeState);
  const isLoading = useRuntimeStore((state) => state.isLoading);
  const sendMessage = useRuntimeStore((state) => state.sendMessage);
  const executeAction = useRuntimeStore((state) => state.executeAction);

  if (!runtimeState) {
    return <section className="screen-panel">Loading chat...</section>;
  }

  const session = selectChatSession(runtimeState, DEFAULT_SESSION_ID);

  const onSend = async () => {
    const trimmed = message.trim();
    if (!trimmed) {
      return;
    }
    await sendMessage(DEFAULT_SESSION_ID, trimmed);
    setMessage("");
  };

  return (
    <section className="screen-panel">
      <header className="screen-header">
        <h2>Conversation</h2>
        <SyncStatusPill status={runtimeState.sync_status} />
      </header>

      <ChatPanel
        input={message}
        history={session.message_history}
        actionCards={session.pending_action_cards}
        awaitingConfirmation={session.awaiting_confirmation}
        disabled={isLoading}
        onInputChange={setMessage}
        onSend={onSend}
        onAction={(action) => executeAction(DEFAULT_SESSION_ID, action)}
      />
    </section>
  );
};
