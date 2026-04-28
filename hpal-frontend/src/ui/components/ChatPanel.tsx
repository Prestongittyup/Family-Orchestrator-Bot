import React from "react";
import type { ActionCard } from "../../api/contracts";

interface ChatPanelProps {
  input: string;
  history: string[];
  actionCards: ActionCard[];
  awaitingConfirmation: boolean;
  disabled: boolean;
  voiceInputSupported: boolean;
  voiceOutputSupported: boolean;
  voiceScopeOptions: Array<{ value: string; label: string }>;
  voiceOptions: Array<{ value: string; label: string }>;
  selectedVoiceName: string;
  voiceScope: string;
  isListening: boolean;
  isSpeaking: boolean;
  voiceStatus: string | null;
  voiceReplyEnabled: boolean;
  onInputChange: (value: string) => void;
  onSend: () => void;
  onAction: (action: ActionCard) => void;
  onToggleListening: () => void;
  onToggleVoiceReply: () => void;
  onSpeakLatest: () => void;
  onSelectVoice: (voiceName: string) => void;
  onVoiceScopeChange: (scope: string) => void;
}

export const ChatPanel: React.FC<ChatPanelProps> = ({
  input,
  history,
  actionCards,
  awaitingConfirmation,
  disabled,
  voiceInputSupported,
  voiceOutputSupported,
  voiceScopeOptions,
  voiceOptions,
  selectedVoiceName,
  voiceScope,
  isListening,
  isSpeaking,
  voiceStatus,
  voiceReplyEnabled,
  onInputChange,
  onSend,
  onAction,
  onToggleListening,
  onToggleVoiceReply,
  onSpeakLatest,
  onSelectVoice,
  onVoiceScopeChange,
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
        <div className="voice-controls-row">
          <label className="voice-controls-label" htmlFor="voice-scope-select">
            Voice language
          </label>
          <select
            id="voice-scope-select"
            className="voice-select"
            value={voiceScope}
            disabled={disabled || !voiceOutputSupported}
            onChange={(event) => onVoiceScopeChange(event.target.value)}
          >
            {voiceScopeOptions.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>

          <label className="voice-controls-label" htmlFor="voice-select">
            Readback voice
          </label>
          <select
            id="voice-select"
            className="voice-select"
            value={selectedVoiceName}
            disabled={disabled || !voiceOutputSupported || voiceOptions.length === 0}
            onChange={(event) => onSelectVoice(event.target.value)}
          >
            {voiceOptions.length === 0 ? <option value="">Default voice</option> : null}
            {voiceOptions.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </div>
        <div className="chat-input-actions">
          <button type="button" onClick={onSend} disabled={disabled || input.trim().length === 0}>
            Send
          </button>
          <button
            type="button"
            className={isListening ? "voice-btn voice-btn-active" : "voice-btn"}
            onClick={onToggleListening}
            disabled={disabled || !voiceInputSupported}
            aria-pressed={isListening}
          >
            {isListening ? "Stop Listening" : "Voice Input"}
          </button>
          <button
            type="button"
            className={voiceReplyEnabled ? "voice-btn voice-btn-active" : "voice-btn"}
            onClick={onToggleVoiceReply}
            disabled={disabled || !voiceOutputSupported}
            aria-pressed={voiceReplyEnabled}
          >
            {voiceReplyEnabled ? "Voice Reply: On" : "Voice Reply: Off"}
          </button>
          <button
            type="button"
            className="voice-btn"
            onClick={onSpeakLatest}
            disabled={disabled || !voiceOutputSupported || history.length === 0 || isSpeaking}
          >
            {isSpeaking ? "Speaking..." : "Read Latest Reply"}
          </button>
        </div>
        {voiceStatus ? <p className="voice-status">{voiceStatus}</p> : null}
      </section>

      <section className="action-cards" aria-label="Action cards">
        <h3>
          Pending Actions {awaitingConfirmation ? "(Confirmation required)" : ""}
        </h3>
        {awaitingConfirmation ? (
          <p className="chat-hint">Tip: reply with "confirm" or "reject" to handle the latest action quickly.</p>
        ) : null}
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
