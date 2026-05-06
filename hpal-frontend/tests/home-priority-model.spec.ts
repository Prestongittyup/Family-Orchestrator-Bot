import { describe, expect, it } from "vitest";
import type { HomeV0Contract } from "../src/api/contracts";
import {
  HOME_FIRST_THREE_SECONDS_RULE,
  HOME_PRIORITY_SEMANTIC_MAP,
  HOME_VISUAL_DOMINANCE_RULES,
  interpretHomeUxSurfaceContract,
} from "../src/ui/contracts/homeUxSurfaceContract";

const FIXED_NOW = new Date("2026-05-01T08:00:00Z");

const sampleHome: HomeV0Contract = {
  needs_decision: [
    {
      id: "decision-1",
      type: "calendar_conflict",
      priority: "high",
      question: "Which event should keep this time slot?",
      options: ["Keep doctor visit", "Keep team call", "Reschedule one"],
    },
  ],
  actions: [
    {
      id: "action-1",
      title: "Confirm school pickup details",
      source: "email",
      priority: "high",
    },
    {
      id: "action-2",
      title: "Review grocery budget",
      source: "email",
      priority: "medium",
    },
  ],
  calendar: [
    {
      id: "event-1",
      title: "Dentist appointment",
      start: "2026-05-01T09:00:00Z",
      end: "2026-05-01T10:00:00Z",
    },
    {
      id: "event-2",
      title: "Soccer practice",
      start: "2026-05-02T17:00:00Z",
      end: "2026-05-02T18:00:00Z",
    },
  ],
  summary: "Conflicts: 1 conflict requires decisions. Actions: 2 email actions pending.",
};

describe("homeUxSurfaceContract", () => {
  it("prioritizes decisions above actions and calendar items", () => {
    const model = interpretHomeUxSurfaceContract(sampleHome, { now: FIXED_NOW });

    expect(model.ordered_cards.length).toBeGreaterThan(0);
    expect(model.ordered_cards[0]?.kind).toBe("decision");
    expect(model.first_three_seconds.headline).toBe("This needs your decision");
    expect(model.first_three_seconds.supporting_text).toContain("can't proceed until this is decided");
    expect(HOME_PRIORITY_SEMANTIC_MAP.critical_now.stack).toBe("top_of_ui");
  });

  it("carries decision options through for guided single-focus rendering", () => {
    const model = interpretHomeUxSurfaceContract(sampleHome, { now: FIXED_NOW });
    const topDecision = model.ordered_cards.find((card) => card.kind === "decision");

    expect(topDecision).toBeTruthy();
    expect(topDecision?.decision_options?.[0]).toBe("Keep doctor visit");
    expect(topDecision?.cta_label).toBe("Pick what should happen");
  });

  it("is deterministic for the same input and reference time", () => {
    const first = interpretHomeUxSurfaceContract(sampleHome, { now: FIXED_NOW });
    const second = interpretHomeUxSurfaceContract(sampleHome, { now: FIXED_NOW });

    expect(second).toEqual(first);
  });

  it("first-3-seconds rule always yields headline, CTA, and priority signal when present", () => {
    const model = interpretHomeUxSurfaceContract(sampleHome, { now: FIXED_NOW });

    expect(HOME_FIRST_THREE_SECONDS_RULE.required_elements).toEqual([
      "headline",
      "primary_action",
      "priority_signal_if_present",
    ]);
    expect(model.first_three_seconds.headline.trim().length).toBeGreaterThan(0);
    expect(model.first_three_seconds.primary_cta_label.trim().length).toBeGreaterThan(0);
    expect(model.first_three_seconds.primary_cta_route.trim().length).toBeGreaterThan(0);
    expect(model.first_three_seconds.priority_signal).toBeTruthy();
  });

  it("collapses lower tiers according to contract thresholds", () => {
    const model = interpretHomeUxSurfaceContract(sampleHome, { now: FIXED_NOW });

    const nextUpVisible = model.visible_cards.filter((card) => card.attention_tier === "next_up").length;
    const laterVisible = model.visible_cards.filter((card) => card.attention_tier === "later").length;

    expect(model.visible_cards.length).toBeGreaterThan(0);
    expect(model.visible_cards.length).toBeLessThan(model.ordered_cards.length);
    expect(model.collapsed_cards.length).toBe(model.ordered_cards.length - model.visible_cards.length);
    expect(nextUpVisible).toBeLessThanOrEqual(HOME_VISUAL_DOMINANCE_RULES.collapse_thresholds.next_up_visible_max);
    expect(laterVisible).toBeLessThanOrEqual(HOME_VISUAL_DOMINANCE_RULES.collapse_thresholds.later_visible_max);
  });
});
