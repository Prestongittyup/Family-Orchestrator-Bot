import React from "react";
import type { ActionCard } from "../../api/contracts";

interface ChatPanelProps {
  input: string;
  history: string[];
  actionCards: ActionCard[];
  awaitingConfirmation: boolean;
  disabled: boolean;
  onInputChange: (value: string) => void;
  onSend: () => void;
  onAction: (action: ActionCard) => void;
}

export const ChatPanel: React.FC<ChatPanelProps> = ({
  input,
  history,
  actionCards,
  awaitingConfirmation,
  disabled,
  onInputChange,
  onSend,
  onAction,
}) => {
  return (
    <div className="chat-panel">
      <section className="chat-history" aria-label="Chat history">
        {history.length === 0 ? <p className="empty-text">No assistant messages yet.</p> : null}
        {history.map((message, index) => (
          <article key={`${index}-${message.slice(0, 16)}`} className="assistant-message">
            {message}
          </article>
        ))}
      </section>

      <section className="chat-input" aria-label="Chat input">
        <textarea
          value={input}
          onChange={(event) => onInputChange(event.target.value)}
          placeholder="Ask the planner to schedule, replan, or summarize..."
          rows={3}
          disabled={disabled}
        />
        <button type="button" onClick={onSend} disabled={disabled || input.trim().length === 0}>
          Send
        </button>
      </section>

      <section className="action-cards" aria-label="Action cards">
        <h3>
          Pending Actions {awaitingConfirmation ? "(Confirmation required)" : ""}
        </h3>
        {actionCards.length === 0 ? <p className="empty-text">No action cards</p> : null}
        <ul>
          {actionCards.map((card) => (
            <li key={card.id} className="action-card">
              <div>
                <p className="action-title">{card.title}</p>
                <p>{card.description}</p>
                <p className="task-meta">Risk: {card.risk_level}</p>
              </div>
              <button type="button" onClick={() => onAction(card)} disabled={disabled}>
                Execute
              </button>
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
};
