import type {
  HomeActionItem,
  HomeCalendarItem,
  HomeDecisionItem,
  HomePriority,
  HomeV0Contract,
} from "../../api/contracts";

export type HomePriorityCardKind = "decision" | "action" | "calendar";
export type HomeAttentionTier = "critical_now" | "next_up" | "later";
export type HomeActionabilityClass = "decide_now" | "do_next" | "stay_informed";
export type HomeFontEmphasis = "high_emphasis" | "medium_emphasis" | "low_emphasis";
export type HomeCardSize = "hero" | "standard" | "compact";

export interface HomePrioritySemanticRule {
  ui_rank: number;
  stack: "top_of_ui" | "secondary_stack" | "collapsed_default";
  collapsed_by_default: boolean;
  label: string;
}

export interface HomeActionabilityRule {
  zone: "sticky_cta_zone" | "actionable_list" | "passive_feed";
  requires_primary_cta: boolean;
  label: string;
}

export interface HomeVisualDominanceRules {
  font_emphasis: Record<HomeAttentionTier, HomeFontEmphasis>;
  card_size_hierarchy: Record<HomeAttentionTier, HomeCardSize>;
  grouping_behavior: Record<HomeAttentionTier, "always_above_fold" | "secondary_group" | "collapsed_group">;
  collapse_thresholds: {
    next_up_visible_max: number;
    later_visible_max: number;
  };
}

export interface HomePriorityCardVisualDominance {
  font_emphasis: HomeFontEmphasis;
  card_size: HomeCardSize;
  group: "primary_lane" | "secondary_lane" | "collapsed_lane";
}

export interface HomePriorityCard {
  card_id: string;
  source_id: string;
  kind: HomePriorityCardKind;
  priority: HomePriority;
  attention_tier: HomeAttentionTier;
  actionability: HomeActionabilityClass;
  title: string;
  detail: string;
  decision_options?: string[];
  cta_label: string;
  cta_route: string;
  score: number;
  visual_dominance: HomePriorityCardVisualDominance;
}

export interface HomeThreeSecondFocus {
  headline: string;
  supporting_text: string;
  primary_cta_label: string;
  primary_cta_route: string;
  priority_signal: string | null;
  context_line: string;
}

export interface HomeUxSurfaceModel {
  contract_version: "home-ux-surface-v1";
  first_three_seconds: HomeThreeSecondFocus;
  ordered_cards: HomePriorityCard[];
  visible_cards: HomePriorityCard[];
  collapsed_cards: HomePriorityCard[];
  counts: {
    decisions: number;
    actions: number;
    calendar: number;
  };
}

const prioritySemanticMap: Record<HomeAttentionTier, HomePrioritySemanticRule> = {
  critical_now: {
    ui_rank: 0,
    stack: "top_of_ui",
    collapsed_by_default: false,
    label: "Critical now",
  },
  next_up: {
    ui_rank: 1,
    stack: "secondary_stack",
    collapsed_by_default: false,
    label: "Next up",
  },
  later: {
    ui_rank: 2,
    stack: "collapsed_default",
    collapsed_by_default: true,
    label: "Later",
  },
};

const actionabilityMap: Record<HomeActionabilityClass, HomeActionabilityRule> = {
  decide_now: {
    zone: "sticky_cta_zone",
    requires_primary_cta: true,
    label: "Decision",
  },
  do_next: {
    zone: "actionable_list",
    requires_primary_cta: true,
    label: "Action",
  },
  stay_informed: {
    zone: "passive_feed",
    requires_primary_cta: false,
    label: "Awareness",
  },
};

const visualDominanceRules: HomeVisualDominanceRules = {
  font_emphasis: {
    critical_now: "high_emphasis",
    next_up: "medium_emphasis",
    later: "low_emphasis",
  },
  card_size_hierarchy: {
    critical_now: "hero",
    next_up: "standard",
    later: "compact",
  },
  grouping_behavior: {
    critical_now: "always_above_fold",
    next_up: "secondary_group",
    later: "collapsed_group",
  },
  collapse_thresholds: {
    next_up_visible_max: 2,
    later_visible_max: 1,
  },
};

const firstThreeSecondsRule = {
  required_elements: ["headline", "primary_action", "priority_signal_if_present"] as const,
};

export const HOME_PRIORITY_SEMANTIC_MAP = Object.freeze(prioritySemanticMap);
export const HOME_ACTIONABILITY_MAP = Object.freeze(actionabilityMap);
export const HOME_VISUAL_DOMINANCE_RULES = Object.freeze(visualDominanceRules);
export const HOME_FIRST_THREE_SECONDS_RULE = Object.freeze(firstThreeSecondsRule);

const PRIORITY_WEIGHT: Record<HomePriority, number> = {
  high: 30,
  medium: 16,
  low: 4,
};

const KIND_WEIGHT: Record<HomePriorityCardKind, number> = {
  decision: 100,
  action: 72,
  calendar: 44,
};

const KIND_SORT_ORDER: Record<HomePriorityCardKind, number> = {
  decision: 0,
  action: 1,
  calendar: 2,
};

const normalizePriority = (value: unknown): HomePriority => {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "high" || normalized === "medium" || normalized === "low") {
    return normalized;
  }
  return "low";
};

const normalizeText = (value: unknown, fallback: string): string => {
  const normalized = String(value || "").trim();
  return normalized || fallback;
};

const formatDecisionBlockingSummary = (count: number): string => {
  return `${count} decision${count === 1 ? "" : "s"} blocking your day.`;
};

const scoreCalendarUrgency = (start: string, now: Date): number => {
  const epoch = Date.parse(start);
  if (Number.isNaN(epoch)) {
    return 0;
  }

  const deltaMinutes = Math.floor((epoch - now.getTime()) / 60000);
  if (deltaMinutes <= 120) {
    return 24;
  }
  if (deltaMinutes <= 480) {
    return 16;
  }
  if (deltaMinutes <= 1440) {
    return 8;
  }
  return 3;
};

const formatCalendarTime = (value: string): string => {
  const epoch = Date.parse(value);
  if (Number.isNaN(epoch)) {
    return "time TBD";
  }

  return new Intl.DateTimeFormat(undefined, {
    weekday: "short",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(epoch));
};

const toVisualDominance = (tier: HomeAttentionTier): HomePriorityCardVisualDominance => {
  return {
    font_emphasis: HOME_VISUAL_DOMINANCE_RULES.font_emphasis[tier],
    card_size: HOME_VISUAL_DOMINANCE_RULES.card_size_hierarchy[tier],
    group: tier === "critical_now" ? "primary_lane" : tier === "next_up" ? "secondary_lane" : "collapsed_lane",
  };
};

const toDecisionCard = (item: HomeDecisionItem, index: number): HomePriorityCard => {
  const priority = normalizePriority(item.priority);
  const sourceId = normalizeText(item.id, `decision-${index + 1}`);
  const options = Array.isArray(item.options)
    ? item.options
      .map((option) => normalizeText(option, ""))
      .filter((option) => option.length > 0)
    : [];
  const optionsCount = options.length;
  const title = normalizeText(item.question, "This needs your decision.");

  const score = KIND_WEIGHT.decision + PRIORITY_WEIGHT[priority] + Math.min(optionsCount, 3);
  const attentionTier: HomeAttentionTier = "critical_now";

  return {
    card_id: `decision:${sourceId}`,
    source_id: sourceId,
    kind: "decision",
    priority,
    attention_tier: attentionTier,
    actionability: "decide_now",
    title,
    detail: "Pick what should happen. We can't proceed until this is decided.",
    decision_options: options,
    cta_label: "Pick what should happen",
    cta_route: "/home#dashboard-home-focus",
    score,
    visual_dominance: toVisualDominance(attentionTier),
  };
};

const toActionCard = (item: HomeActionItem, index: number): HomePriorityCard => {
  const priority = normalizePriority(item.priority);
  const sourceId = normalizeText(item.id, `action-${index + 1}`);
  const source = normalizeText(item.source, "home");
  const title = normalizeText(item.title, "Review pending action");

  const score = KIND_WEIGHT.action + PRIORITY_WEIGHT[priority] + (source === "email" ? 4 : 0);
  const attentionTier: HomeAttentionTier = score >= 86 ? "critical_now" : "next_up";

  return {
    card_id: `action:${sourceId}`,
    source_id: sourceId,
    kind: "action",
    priority,
    attention_tier: attentionTier,
    actionability: "do_next",
    title,
    detail: `Source: ${source}`,
    cta_label: source === "email" ? "Open Inbox" : "Open Tasks",
    cta_route: source === "email" ? "/inbox" : "/tasks",
    score,
    visual_dominance: toVisualDominance(attentionTier),
  };
};

const toCalendarCard = (item: HomeCalendarItem, index: number, now: Date): HomePriorityCard => {
  const sourceId = normalizeText(item.id, `calendar-${index + 1}`);
  const title = normalizeText(item.title, "Upcoming event");
  const start = normalizeText(item.start, "");
  const end = normalizeText(item.end, "");
  const urgency = scoreCalendarUrgency(start, now);
  const score = KIND_WEIGHT.calendar + urgency;
  const windowText = `${formatCalendarTime(start)} - ${formatCalendarTime(end)}`;
  const attentionTier: HomeAttentionTier = urgency >= 16 ? "next_up" : "later";

  return {
    card_id: `calendar:${sourceId}`,
    source_id: sourceId,
    kind: "calendar",
    priority: urgency >= 16 ? "high" : urgency >= 8 ? "medium" : "low",
    attention_tier: attentionTier,
    actionability: "stay_informed",
    title,
    detail: windowText,
    cta_label: "Open Calendar",
    cta_route: "/calendar",
    score,
    visual_dominance: toVisualDominance(attentionTier),
  };
};

const compareCards = (left: HomePriorityCard, right: HomePriorityCard): number => {
  if (right.score !== left.score) {
    return right.score - left.score;
  }

  if (HOME_PRIORITY_SEMANTIC_MAP[left.attention_tier].ui_rank !== HOME_PRIORITY_SEMANTIC_MAP[right.attention_tier].ui_rank) {
    return HOME_PRIORITY_SEMANTIC_MAP[left.attention_tier].ui_rank - HOME_PRIORITY_SEMANTIC_MAP[right.attention_tier].ui_rank;
  }

  if (KIND_SORT_ORDER[left.kind] !== KIND_SORT_ORDER[right.kind]) {
    return KIND_SORT_ORDER[left.kind] - KIND_SORT_ORDER[right.kind];
  }

  if (left.title !== right.title) {
    return left.title.localeCompare(right.title);
  }

  return left.card_id.localeCompare(right.card_id);
};

const buildPrioritySignal = (orderedCards: HomePriorityCard[]): string | null => {
  const topCard = orderedCards[0];
  if (!topCard) {
    return null;
  }

  if (topCard.kind === "decision") {
    const decisionCount = orderedCards.filter((card) => card.kind === "decision").length;
    return formatDecisionBlockingSummary(decisionCount);
  }

  if (topCard.attention_tier === "critical_now") {
    return `${HOME_PRIORITY_SEMANTIC_MAP.critical_now.label} item requires immediate attention.`;
  }

  if (topCard.attention_tier === "next_up") {
    return `${HOME_PRIORITY_SEMANTIC_MAP.next_up.label} queue is active.`;
  }

  return null;
};

const buildFirstThreeSeconds = (
  ordered: HomePriorityCard[],
  fallbackSummary: string,
  counts: HomeUxSurfaceModel["counts"],
): HomeThreeSecondFocus => {
  if (counts.decisions > 0) {
    return {
      headline: "This needs your decision",
      supporting_text: "Pick what should happen. We can't proceed until this is decided.",
      primary_cta_label: "Pick what should happen",
      primary_cta_route: "/home#dashboard-home-focus",
      priority_signal: formatDecisionBlockingSummary(counts.decisions),
      context_line: formatDecisionBlockingSummary(counts.decisions),
    };
  }

  const topCard = ordered[0];

  const contextLine = `${counts.decisions} decision${counts.decisions === 1 ? "" : "s"}, `
    + `${counts.actions} action${counts.actions === 1 ? "" : "s"}, `
    + `${counts.calendar} event${counts.calendar === 1 ? "" : "s"}.`;

  if (!topCard) {
    return {
      headline: "No decisions blocking you",
      supporting_text: fallbackSummary || "You're clear to execute.",
      primary_cta_label: "Open Tasks",
      primary_cta_route: "/tasks",
      priority_signal: null,
      context_line: contextLine,
    };
  }

  const prioritySignal = buildPrioritySignal(ordered);

  if (topCard.kind === "action") {
    return {
      headline: "No decisions blocking you",
      supporting_text: topCard.title,
      primary_cta_label: topCard.cta_label,
      primary_cta_route: topCard.cta_route,
      priority_signal: prioritySignal,
      context_line: contextLine,
    };
  }

  return {
    headline: "Upcoming schedule pressure",
    supporting_text: `${topCard.title} (${topCard.detail})`,
    primary_cta_label: topCard.cta_label,
    primary_cta_route: topCard.cta_route,
    priority_signal: prioritySignal,
    context_line: contextLine,
  };
};

export const formatHomeAttentionTierLabel = (tier: HomeAttentionTier): string => {
  return HOME_PRIORITY_SEMANTIC_MAP[tier].label;
};

export const formatHomeActionabilityLabel = (value: HomeActionabilityClass): string => {
  return HOME_ACTIONABILITY_MAP[value].label;
};

export const interpretHomeUxSurfaceContract = (
  home: HomeV0Contract,
  options?: { now?: Date },
): HomeUxSurfaceModel => {
  const now = options?.now ?? new Date();

  const decisionCards = (home.needs_decision || []).map((item, index) => toDecisionCard(item, index));
  const actionCards = (home.actions || []).map((item, index) => toActionCard(item, index));
  const calendarCards = (home.calendar || []).map((item, index) => toCalendarCard(item, index, now));

  const orderedCards = [...decisionCards, ...actionCards, ...calendarCards].sort(compareCards);

  const criticalNow = orderedCards.filter((item) => item.attention_tier === "critical_now");
  const nextUp = orderedCards.filter((item) => item.attention_tier === "next_up");
  const later = orderedCards.filter((item) => item.attention_tier === "later");

  const visibleCards = [
    ...criticalNow,
    ...nextUp.slice(0, HOME_VISUAL_DOMINANCE_RULES.collapse_thresholds.next_up_visible_max),
    ...later.slice(0, HOME_VISUAL_DOMINANCE_RULES.collapse_thresholds.later_visible_max),
  ];

  const visibleIdSet = new Set(visibleCards.map((item) => item.card_id));
  const collapsedCards = orderedCards.filter((item) => !visibleIdSet.has(item.card_id));

  const counts = {
    decisions: decisionCards.length,
    actions: actionCards.length,
    calendar: calendarCards.length,
  };

  return {
    contract_version: "home-ux-surface-v1",
    first_three_seconds: buildFirstThreeSeconds(orderedCards, home.summary, counts),
    ordered_cards: orderedCards,
    visible_cards: visibleCards,
    collapsed_cards: collapsedCards,
    counts,
  };
};
