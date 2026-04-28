import React from "react";
import { useSearchParams } from "react-router-dom";
import { useRuntimeStore } from "../../runtime/store";
import { selectChatSession } from "../../runtime/selectors";
import type { ActionCard } from "../../api/contracts";
import { ChatPanel } from "../components/ChatPanel";
import { SyncStatusPill } from "../components/SyncStatusPill";

const DEFAULT_SESSION_ID = "main-ui-session";
const VOICE_STORAGE_KEY = "hpal.voice.readback";
const VOICE_SCOPE_STORAGE_KEY = "hpal.voice.readback.scope";
const DEFAULT_VOICE_SCOPE = "all";
const ASSISTANT_PENDING_PROMPT_KEY = "hpal.assistant.pending_prompt";
const ASSISTANT_PENDING_AUTOSEND_KEY = "hpal.assistant.pending_autosend";

type AssistantQuickWorkflow = {
  id: string;
  label: string;
  prompt: string;
};

const ASSISTANT_QUICK_WORKFLOWS: AssistantQuickWorkflow[] = [
  {
    id: "inbox-prioritize",
    label: "Prioritize Inbox",
    prompt: "Please prioritize my inbox and give me the top 5 actions with short follow-up suggestions.",
  },
  {
    id: "inbox-followups",
    label: "Draft Follow-ups",
    prompt: "Draft concise follow-up replies for my highest-priority inbox items, including calendar confirmations I should send.",
  },
  {
    id: "kids-rollup",
    label: "Kids Weekly Rollup",
    prompt: "Tell me everything the kids have going on this week, grouped by child and day, and flag overlap or impossible transitions.",
  },
  {
    id: "weekend-purchases",
    label: "Friday/Weekend Purchases",
    prompt: "Based on upcoming events and tasks, suggest Friday and weekend purchase reminders with item lists and ask which reminders to activate.",
  },
  {
    id: "place-purchase-tasks",
    label: "Place Purchase Tasks",
    prompt: "Find free time blocks this week and propose where purchase errands should be scheduled as tasks with at least two options.",
  },
  {
    id: "rebalance-week",
    label: "Rebalance Week",
    prompt: "Detect whether this week is overloaded; if so, suggest which events or tasks to shift and provide a revised balanced plan.",
  },
];

type VoiceScope = string;

type VoiceRecognitionEvent = {
  results: ArrayLike<{
    0?: {
      transcript?: string;
    };
  }>;
};

type VoiceRecognitionErrorEvent = {
  error?: string;
};

type VoiceRecognition = {
  lang: string;
  continuous: boolean;
  interimResults: boolean;
  maxAlternatives: number;
  onstart: (() => void) | null;
  onend: (() => void) | null;
  onresult: ((event: VoiceRecognitionEvent) => void) | null;
  onerror: ((event: VoiceRecognitionErrorEvent) => void) | null;
  start: () => void;
  stop: () => void;
};

type VoiceRecognitionConstructor = new () => VoiceRecognition;

function normalizeLanguageCode(locale: string): string {
  const normalized = locale.trim().toLowerCase().replace("_", "-");
  if (!normalized) {
    return "und";
  }
  return normalized.split("-")[0] || "und";
}

function languageLabel(languageCode: string): string {
  if (languageCode === "all") {
    return "All installed voices";
  }

  const normalizedCode = normalizeLanguageCode(languageCode);
  if (typeof Intl !== "undefined" && "DisplayNames" in Intl) {
    try {
      const names = new Intl.DisplayNames(["en"], { type: "language" });
      const label = names.of(normalizedCode);
      if (label) {
        return label;
      }
    } catch {
      // Fallback to code when DisplayNames fails in older browsers.
    }
  }

  return normalizedCode.toUpperCase();
}

function normalizeVoiceScope(value: string | null): VoiceScope {
  const normalized = (value || "").trim().toLowerCase();
  if (!normalized) {
    return DEFAULT_VOICE_SCOPE;
  }

  // Backward compatibility for earlier English-only scope values.
  if (normalized === "english_non_uk" || normalized === "english_all") {
    return "en";
  }

  if (normalized === "all") {
    return "all";
  }

  if (/^[a-z]{2,3}([_-][a-z]{2})?$/.test(normalized)) {
    return normalizeLanguageCode(normalized);
  }

  return DEFAULT_VOICE_SCOPE;
}

function resolveScopedVoices(voices: SpeechSynthesisVoice[], scope: VoiceScope): SpeechSynthesisVoice[] {
  if (scope === "all") {
    return voices;
  }

  const scoped = voices.filter((voice) => normalizeLanguageCode(voice.lang) === scope);
  return scoped.length > 0 ? scoped : voices;
}

function buildVoiceScopeOptions(
  voices: SpeechSynthesisVoice[],
  selectedScope: VoiceScope,
): Array<{ value: string; label: string }> {
  const counts = new Map<string, number>();
  for (const voice of voices) {
    const code = normalizeLanguageCode(voice.lang);
    counts.set(code, (counts.get(code) || 0) + 1);
  }

  const options: Array<{ value: string; label: string }> = [
    { value: "all", label: "All installed voices" },
    ...Array.from(counts.entries())
      .sort(([left], [right]) => languageLabel(left).localeCompare(languageLabel(right)))
      .map(([code, count]) => ({
        value: code,
        label: `${languageLabel(code)} (${count})`,
      })),
  ];

  if (selectedScope !== "all" && !counts.has(selectedScope)) {
    options.push({
      value: selectedScope,
      label: `${languageLabel(selectedScope)} (no installed voices)`,
    });
  }

  return options;
}

function voiceScore(voice: SpeechSynthesisVoice): number {
  const name = voice.name.toLowerCase();
  let score = 0;

  if (/(female|samantha|victoria|karen|zira|jenny|aria|ava|emma|susan|allison|luna|nova|serena)/i.test(name)) {
    score += 28;
  }
  if (/(aria|sara|libby|jessa|alloy|nova|joy)/i.test(name)) {
    score += 15;
  }
  if (/(natural|neural|premium|enhanced|expressive|studio|online)/i.test(name)) {
    score += 24;
  }
  if (/(robot|compact)/i.test(name)) {
    score -= 12;
  }
  if (voice.default) {
    score += 4;
  }

  return score;
}

function pickComfortVoice(voices: SpeechSynthesisVoice[]): SpeechSynthesisVoice | null {
  if (voices.length === 0) {
    return null;
  }

  const ranked = [...voices].sort((a, b) => {
    const scoreDelta = voiceScore(b) - voiceScore(a);
    if (scoreDelta !== 0) {
      return scoreDelta;
    }
    return a.name.localeCompare(b.name);
  });
  return ranked[0] ?? null;
}

function voiceOptionsSort(a: SpeechSynthesisVoice, b: SpeechSynthesisVoice): number {
  const scoreDelta = voiceScore(b) - voiceScore(a);
  if (scoreDelta !== 0) {
    return scoreDelta;
  }
  return a.name.localeCompare(b.name);
}

function pickActionForIntent(actionCards: ActionCard[], intent: "confirm" | "reject"): ActionCard | null {
  if (actionCards.length === 0) {
    return null;
  }

  if (intent === "confirm") {
    return (
      actionCards.find((card) => card.type === "confirm") ??
      actionCards.find((card) => card.type === "approve") ??
      actionCards.find((card) => card.type === "edit") ??
      actionCards.find((card) => card.type === "reschedule") ??
      actionCards[0]
    );
  }

  return actionCards.find((card) => card.type === "reject") ?? null;
}

function parseQuickIntent(input: string): "confirm" | "reject" | null {
  const normalized = input.trim().toLowerCase();
  if (!normalized) {
    return null;
  }

  const compact = normalized.replace(/[.!?]+$/g, "").trim();

  if (/^(confirm|yes|yep|yeah|ok|okay|go ahead|do it|approved?|sounds good)( please)?$/.test(compact)) {
    return "confirm";
  }
  if (compact.startsWith("confirm ") || compact.startsWith("approve ")) {
    return "confirm";
  }

  if (/^(reject|no|nope|cancel|decline|stop|never mind|nevermind)( please)?$/.test(compact)) {
    return "reject";
  }
  if (compact.startsWith("reject ") || compact.startsWith("cancel ")) {
    return "reject";
  }

  return null;
}

function toChatErrorMessage(raw: string | null): string | null {
  const value = (raw || "").trim();
  if (!value) {
    return null;
  }

  if (value === "permission_denied:chat") {
    return "Chat is disabled for this account role. Re-authenticate with an ADULT or ADMIN account.";
  }
  if (value.startsWith("message_failed:")) {
    return "Assistant request failed on the server. Please retry in a moment.";
  }
  if (value.startsWith("action_failed:")) {
    return "Assistant action failed on the server. Try again after refresh.";
  }
  if (value.includes("Network error contacting API")) {
    return "Cannot reach the API right now. Verify backend connectivity and retry.";
  }

  return value;
}

export const ChatScreen: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [message, setMessage] = React.useState("");
  const [queuedAssistantPrompt, setQueuedAssistantPrompt] = React.useState<string | null>(null);
  const [voiceInputSupported, setVoiceInputSupported] = React.useState(false);
  const [voiceOutputSupported, setVoiceOutputSupported] = React.useState(false);
  const [allVoices, setAllVoices] = React.useState<SpeechSynthesisVoice[]>([]);
  const [availableVoices, setAvailableVoices] = React.useState<SpeechSynthesisVoice[]>([]);
  const [voiceScopeOptions, setVoiceScopeOptions] = React.useState<Array<{ value: string; label: string }>>([
    { value: "all", label: "All installed voices" },
  ]);
  const [selectedVoiceName, setSelectedVoiceName] = React.useState("");
  const [voiceScope, setVoiceScope] = React.useState<VoiceScope>(() => {
    if (typeof window === "undefined") {
      return DEFAULT_VOICE_SCOPE;
    }
    try {
      return normalizeVoiceScope(window.localStorage.getItem(VOICE_SCOPE_STORAGE_KEY));
    } catch {
      return DEFAULT_VOICE_SCOPE;
    }
  });
  const [isListening, setIsListening] = React.useState(false);
  const [isSpeaking, setIsSpeaking] = React.useState(false);
  const [voiceStatus, setVoiceStatus] = React.useState<string | null>(null);
  const [voiceReplyEnabled, setVoiceReplyEnabled] = React.useState(true);

  const recognitionRef = React.useRef<VoiceRecognition | null>(null);
  const previousHistoryLengthRef = React.useRef(0);

  const runtimeState = useRuntimeStore((state) => state.runtimeState);
  const isLoading = useRuntimeStore((state) => state.isLoading);
  const error = useRuntimeStore((state) => state.error);
  const sendMessage = useRuntimeStore((state) => state.sendMessage);
  const executeAction = useRuntimeStore((state) => state.executeAction);

  const chatErrorMessage = React.useMemo(() => toChatErrorMessage(error), [error]);

  const selectedVoice = React.useMemo(
    () => availableVoices.find((voice) => voice.name === selectedVoiceName) ?? null,
    [availableVoices, selectedVoiceName],
  );

  const voiceOptions = React.useMemo(
    () =>
      [...availableVoices].sort(voiceOptionsSort).map((voice) => ({
        value: voice.name,
        label: `${voice.name} (${voice.lang}${voice.localService ? ", local" : ", cloud"})`,
      })),
    [availableVoices],
  );

  React.useEffect(() => {
    const promptFromQuery = (searchParams.get("prompt") || "").trim();
    const queryAutoSend = searchParams.get("autosend") === "1";

    let prompt = promptFromQuery;
    let shouldAutoSend = queryAutoSend;

    if (!prompt) {
      try {
        const storedPrompt = (localStorage.getItem(ASSISTANT_PENDING_PROMPT_KEY) || "").trim();
        if (storedPrompt) {
          prompt = storedPrompt;
          shouldAutoSend = localStorage.getItem(ASSISTANT_PENDING_AUTOSEND_KEY) === "1";
        }
      } catch {
        // Ignore storage failures and rely on URL params only.
      }
    }

    if (!prompt) {
      return;
    }

    setMessage(prompt);
    if (shouldAutoSend) {
      setQueuedAssistantPrompt(prompt);
      setVoiceStatus("Sending assistant workflow request...");
    } else {
      setVoiceStatus("Prompt loaded. Press Send when ready.");
    }

    try {
      localStorage.removeItem(ASSISTANT_PENDING_PROMPT_KEY);
      localStorage.removeItem(ASSISTANT_PENDING_AUTOSEND_KEY);
    } catch {
      // Ignore storage cleanup failures.
    }

    if (promptFromQuery) {
      setSearchParams({}, { replace: true });
    }
  }, [searchParams, setSearchParams]);

  React.useEffect(() => {
    if (!queuedAssistantPrompt) {
      return;
    }

    if (!runtimeState || isLoading) {
      return;
    }

    let isMounted = true;
    const sendQueuedPrompt = async () => {
      try {
        await sendMessage(DEFAULT_SESSION_ID, queuedAssistantPrompt);
        if (isMounted) {
          setMessage("");
          setVoiceStatus("Assistant workflow request sent.");
        }
      } catch {
        if (isMounted) {
          setVoiceStatus("Unable to send assistant workflow request right now.");
        }
      } finally {
        if (isMounted) {
          setQueuedAssistantPrompt(null);
        }
      }
    };

    void sendQueuedPrompt();

    return () => {
      isMounted = false;
    };
  }, [queuedAssistantPrompt, runtimeState, isLoading, sendMessage]);

  React.useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    if (!selectedVoiceName) {
      return;
    }
    try {
      window.localStorage.setItem(VOICE_STORAGE_KEY, selectedVoiceName);
    } catch {
      // Ignore storage failures in restricted contexts.
    }
  }, [selectedVoiceName]);

  React.useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    try {
      window.localStorage.setItem(VOICE_SCOPE_STORAGE_KEY, voiceScope);
    } catch {
      // Ignore storage failures in restricted contexts.
    }
  }, [voiceScope]);

  React.useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    let removeVoicesListener: (() => void) | null = null;
    const hasSpeechOutput = "speechSynthesis" in window && "SpeechSynthesisUtterance" in window;
    setVoiceOutputSupported(hasSpeechOutput);

    if (hasSpeechOutput) {
      const synth = window.speechSynthesis;
      const refreshVoices = () => {
        const rawVoices = synth.getVoices().sort(voiceOptionsSort);
        setAllVoices(rawVoices);
        setVoiceScopeOptions(buildVoiceScopeOptions(rawVoices, voiceScope));

        const nextVoices = resolveScopedVoices(rawVoices, voiceScope).sort(voiceOptionsSort);
        setAvailableVoices(nextVoices);
        setSelectedVoiceName((current) => {
          if (current && nextVoices.some((voice) => voice.name === current)) {
            return current;
          }

          let stored = "";
          try {
            stored = window.localStorage.getItem(VOICE_STORAGE_KEY) ?? "";
          } catch {
            stored = "";
          }
          if (stored && nextVoices.some((voice) => voice.name === stored)) {
            return stored;
          }

          const preferred = pickComfortVoice(nextVoices);
          return preferred?.name ?? "";
        });
      };

      refreshVoices();
      synth.addEventListener("voiceschanged", refreshVoices);

      removeVoicesListener = () => {
        synth.removeEventListener("voiceschanged", refreshVoices);
      };
    }

    type SpeechWindow = Window & {
      SpeechRecognition?: VoiceRecognitionConstructor;
      webkitSpeechRecognition?: VoiceRecognitionConstructor;
    };

    const speechWindow = window as SpeechWindow;
    const Recognition = speechWindow.SpeechRecognition ?? speechWindow.webkitSpeechRecognition;
    if (!Recognition) {
      setVoiceInputSupported(false);
      setVoiceStatus("Voice input is unavailable in this browser.");
      return () => {
        if (removeVoicesListener) {
          removeVoicesListener();
        }
        if ("speechSynthesis" in window) {
          window.speechSynthesis.cancel();
        }
      };
    }

    const recognition = new Recognition();
    recognition.lang = voiceScope === "all" ? "en-US" : voiceScope;
    recognition.continuous = false;
    recognition.interimResults = false;
    recognition.maxAlternatives = 1;

    recognition.onstart = () => {
      setIsListening(true);
      setVoiceStatus("Listening for your request...");
    };

    recognition.onresult = (event) => {
      const chunks: string[] = [];
      for (let index = 0; index < event.results.length; index += 1) {
        const transcript = event.results[index]?.[0]?.transcript?.trim();
        if (transcript) {
          chunks.push(transcript);
        }
      }
      if (chunks.length > 0) {
        setMessage((current) => {
          const prefix = current.trim().length > 0 ? `${current.trim()} ` : "";
          return `${prefix}${chunks.join(" ")}`;
        });
        setVoiceStatus("Voice input captured.");
      }
    };

    recognition.onerror = (event) => {
      setIsListening(false);
      setVoiceStatus(`Voice input error${event.error ? `: ${event.error}` : ""}.`);
    };

    recognition.onend = () => {
      setIsListening(false);
    };

    recognitionRef.current = recognition;
    setVoiceInputSupported(true);

    return () => {
      if (removeVoicesListener) {
        removeVoicesListener();
      }
      if (recognitionRef.current) {
        recognitionRef.current.stop();
      }
      recognitionRef.current = null;
      if ("speechSynthesis" in window) {
        window.speechSynthesis.cancel();
      }
    };
  }, [voiceScope]);

  if (!runtimeState) {
    return <section className="screen-panel">Loading chat...</section>;
  }

  const session = selectChatSession(runtimeState, DEFAULT_SESSION_ID);

  const queueAssistantWorkflow = React.useCallback((prompt: string) => {
    const trimmed = prompt.trim();
    if (!trimmed) {
      return;
    }

    setMessage(trimmed);
    setQueuedAssistantPrompt(trimmed);
    setVoiceStatus("Sending assistant workflow request...");
  }, []);

  const speakText = React.useCallback((text: string) => {
    if (typeof window === "undefined" || !("speechSynthesis" in window) || !("SpeechSynthesisUtterance" in window)) {
      setVoiceStatus("Voice reply is unavailable in this browser.");
      return;
    }

    const utterance = new SpeechSynthesisUtterance(text);
    if (selectedVoice) {
      utterance.voice = selectedVoice;
      utterance.lang = selectedVoice.lang;
    }
    // Keep readback warm and smooth while still sounding energetic.
    utterance.rate = 0.9;
    utterance.pitch = 1.02;
    utterance.volume = 0.95;
    utterance.onstart = () => {
      setIsSpeaking(true);
      const voiceName = selectedVoice?.name ? ` using ${selectedVoice.name}` : "";
      setVoiceStatus(`Reading assistant response${voiceName}...`);
    };
    utterance.onend = () => {
      setIsSpeaking(false);
      setVoiceStatus("Voice reply finished.");
    };
    utterance.onerror = () => {
      setIsSpeaking(false);
      setVoiceStatus("Unable to play voice reply.");
    };

    window.speechSynthesis.cancel();
    window.speechSynthesis.speak(utterance);
  }, [selectedVoice]);

  React.useEffect(() => {
    if (!recognitionRef.current) {
      return;
    }
    recognitionRef.current.lang = selectedVoice?.lang || (voiceScope === "all" ? "en-US" : voiceScope);
  }, [selectedVoice, voiceScope]);

  const onToggleListening = React.useCallback(() => {
    if (!recognitionRef.current || !voiceInputSupported) {
      setVoiceStatus("Voice input is unavailable in this browser.");
      return;
    }

    try {
      if (isListening) {
        recognitionRef.current.stop();
        setVoiceStatus("Voice input stopped.");
      } else {
        recognitionRef.current.start();
      }
    } catch {
      setVoiceStatus("Voice input could not start. Please try again.");
    }
  }, [isListening, voiceInputSupported]);

  const onToggleVoiceReply = React.useCallback(() => {
    setVoiceReplyEnabled((current) => {
      const next = !current;
      setVoiceStatus(next ? "Voice reply enabled." : "Voice reply muted.");
      if (!next && typeof window !== "undefined" && "speechSynthesis" in window) {
        window.speechSynthesis.cancel();
        setIsSpeaking(false);
      }
      return next;
    });
  }, []);

  const onSpeakLatest = React.useCallback(() => {
    const latest = session.message_history[session.message_history.length - 1];
    if (!latest) {
      setVoiceStatus("No assistant response available to read yet.");
      return;
    }
    speakText(latest);
  }, [session.message_history, speakText]);

  const onSelectVoice = React.useCallback(
    (voiceName: string) => {
      setSelectedVoiceName(voiceName);
      const chosen = availableVoices.find((voice) => voice.name === voiceName);
      if (chosen) {
        setVoiceStatus(`Readback voice set to ${chosen.name} (${chosen.lang}).`);
      }
    },
    [availableVoices],
  );

  const onVoiceScopeChange = React.useCallback((scope: VoiceScope) => {
    const normalizedScope = normalizeVoiceScope(scope);
    setVoiceScope(normalizedScope);

    if (normalizedScope === "all") {
      setVoiceStatus("Voice library switched to all installed voices.");
      return;
    }

    const hasLanguageVoices = allVoices.some(
      (voice) => normalizeLanguageCode(voice.lang) === normalizedScope,
    );
    if (hasLanguageVoices) {
      setVoiceStatus(`Voice library switched to ${languageLabel(normalizedScope)}.`);
      return;
    }

    setVoiceStatus(
      `No ${languageLabel(normalizedScope)} voices are installed, so all installed voices are shown.`,
    );
  }, [allVoices]);

  React.useEffect(() => {
    const history = session.message_history;
    if (history.length <= previousHistoryLengthRef.current) {
      previousHistoryLengthRef.current = history.length;
      return;
    }

    previousHistoryLengthRef.current = history.length;
    if (!voiceReplyEnabled) {
      return;
    }

    const latest = history[history.length - 1];
    if (!latest) {
      return;
    }

    speakText(latest);
  }, [session.message_history, speakText, voiceReplyEnabled]);

  const onSend = async () => {
    const trimmed = message.trim();
    if (!trimmed) {
      return;
    }

    const quickIntent = parseQuickIntent(trimmed);
    if (quickIntent && session.pending_action_cards.length > 0) {
      const selectedAction = pickActionForIntent(session.pending_action_cards, quickIntent);
      if (selectedAction) {
        await executeAction(DEFAULT_SESSION_ID, selectedAction);
        setMessage("");
        setVoiceStatus(
          quickIntent === "confirm"
            ? "Confirmed. Executing the pending action."
            : "Rejected. Sending the decline action.",
        );
        return;
      }
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
      {chatErrorMessage ? <p className="error-text">{chatErrorMessage}</p> : null}

      <section className="dashboard-section" aria-label="Assistant intelligence quick links">
        <div className="dashboard-section-header">
          <h3>Assistant Intelligence Quick Links</h3>
        </div>
        <p>Launch common planning workflows directly in the assistant.</p>
        <div className="dashboard-list-controls">
          {ASSISTANT_QUICK_WORKFLOWS.map((workflow) => (
            <button
              key={workflow.id}
              type="button"
              className="dashboard-detail-button"
              onClick={() => queueAssistantWorkflow(workflow.prompt)}
              disabled={isLoading}
            >
              Ask Assistant: {workflow.label}
            </button>
          ))}
        </div>
      </section>

      <ChatPanel
        input={message}
        history={session.message_history}
        actionCards={session.pending_action_cards}
        awaitingConfirmation={session.awaiting_confirmation}
        disabled={isLoading}
        voiceInputSupported={voiceInputSupported}
        voiceOutputSupported={voiceOutputSupported}
        voiceScopeOptions={voiceScopeOptions}
        voiceOptions={voiceOptions}
        selectedVoiceName={selectedVoiceName}
        voiceScope={voiceScope}
        isListening={isListening}
        isSpeaking={isSpeaking}
        voiceStatus={voiceStatus}
        voiceReplyEnabled={voiceReplyEnabled}
        onInputChange={setMessage}
        onSend={onSend}
        onAction={(action) => executeAction(DEFAULT_SESSION_ID, action)}
        onToggleListening={onToggleListening}
        onToggleVoiceReply={onToggleVoiceReply}
        onSpeakLatest={onSpeakLatest}
        onSelectVoice={onSelectVoice}
        onVoiceScopeChange={onVoiceScopeChange}
      />
    </section>
  );
};
