/**
 * HPAL Frontend State Store (Zustand)
 *
 * Central state machine for projection consumption.
 * All updates are replace-by-version (never patch merges).
 * Watermark is used to prevent regression overwrites.
 */

import { create } from "zustand";
import {
  HPALFrontendState,
  Family,
  Plan,
  Task,
  Event,
  ProjectionWatermark,
  ChangeEvent,
  HouseholdOverview,
} from "../types/index";

interface HPALStore extends HPALFrontendState {
  // Read-only getter for computed state
  getFamilyById: (id: string) => Family | undefined;
  getPlansByFamily: () => Plan[];
  getTasksByStatus: (status: string) => Task[];
  getEventsByTimeWindow: (start: string, end: string) => Event[];
  getTasksByPlan: (planId: string) => Task[];

  // Update actions — all replace-by-version
  setFamily: (family: Family) => void;
  setPlans: (plans: Plan[]) => void;
  setTasks: (tasks: Task[]) => void;
  setEvents: (events: Event[]) => void;
  setWatermark: (watermark: ProjectionWatermark | null) => void;

  // Bulk update for consistency
  updateProjection: (overview: HouseholdOverview, watermark: ProjectionWatermark) => void;

  // Selection state
  selectPlan: (planId: string | null) => void;
  selectPerson: (personId: string | null) => void;

  // UI state
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  setLastSyncAt: (time: string) => void;

  // Change tracking for UI explanation
  recordChange: (change: ChangeEvent) => void;

  // Regression detection
  isRegression: (newWatermark: ProjectionWatermark) => boolean;

  // Reset
  reset: () => void;
}

const initialState: HPALFrontendState = {
  family: null,
  plans: [],
  tasks: [],
  events: [],
  projection_watermark: null,
  selected_plan_id: null,
  selected_person_id: null,
  error: null,
  loading: false,
  last_sync_at: null,
  last_change: null,
};

export const useHPALStore = create<HPALStore>((set, get) => ({
  ...initialState,

  getFamilyById: (id: string) => {
    const state = get();
    if (state.family?.family_id === id) {
      return state.family;
    }
    return undefined;
  },

  getPlansByFamily: () => {
    return get().plans;
  },

  getTasksByStatus: (status: string) => {
    return get().tasks.filter((task) => task.status === status);
  },

  getEventsByTimeWindow: (start: string, end: string) => {
    return get().events.filter((event) => {
      const eventStart = new Date(event.time_window.start).getTime();
      const eventEnd = new Date(event.time_window.end).getTime();
      const windowStart = new Date(start).getTime();
      const windowEnd = new Date(end).getTime();
      return eventStart >= windowStart && eventEnd <= windowEnd;
    });
  },

  getTasksByPlan: (planId: string) => {
    return get().tasks.filter((task) => task.plan_id === planId);
  },

  setFamily: (family: Family) => {
    set({ family });
  },

  setPlans: (plans: Plan[]) => {
    set({ plans });
  },

  setTasks: (tasks: Task[]) => {
    set({ tasks });
  },

  setEvents: (events: Event[]) => {
    set({ events });
  },

  setWatermark: (watermark: ProjectionWatermark | null) => {
    set({ projection_watermark: watermark });
  },

  /**
   * Bulk projection update with watermark check.
   * This is the safe path for receiving new data from backend.
   */
  updateProjection: (overview: HouseholdOverview, watermark: ProjectionWatermark) => {
    const state = get();

    // Regression check: reject if new watermark is older
    if (state.isRegression(watermark)) {
      console.warn(
        "[HPAL] Rejecting regression: new epoch",
        watermark.projection_epoch,
        "vs current",
        state.projection_watermark?.projection_epoch
      );
      return;
    }

    set({
      family: overview.family,
      plans: overview.family.active_plans.map((pid) => ({
        plan_id: pid,
        family_id: overview.family.family_id,
        title: "Loading...",
        intent_origin: "unknown",
        status: "active",
        linked_tasks: [],
        schedule_window: { start: "", end: "" },
        last_recomputed_at: null,
        revision: 0,
        stability_state: "adjusting",
      })),
      tasks: [],
      events: overview.today_events,
      projection_watermark: watermark,
      last_sync_at: new Date().toISOString(),
    });
  },

  selectPlan: (planId: string | null) => {
    set({ selected_plan_id: planId });
  },

  selectPerson: (personId: string | null) => {
    set({ selected_person_id: personId });
  },

  setLoading: (loading: boolean) => {
    set({ loading });
  },

  setError: (error: string | null) => {
    set({ error });
  },

  setLastSyncAt: (time: string) => {
    set({ last_sync_at: time });
  },

  recordChange: (change: ChangeEvent) => {
    set({ last_change: change });
  },

  /**
   * Check if incoming watermark represents a regression.
   * Frontend rejects updates if epoch goes backward or event/transition counts decrease.
   */
  isRegression: (newWatermark: ProjectionWatermark): boolean => {
    const state = get();
    const current = state.projection_watermark;

    if (!current) {
      // First update is never a regression
      return false;
    }

    // Reject if epoch goes backward
    if (newWatermark.projection_epoch < current.projection_epoch) {
      return true;
    }

    // Reject if transition or event counts go backward
    if (newWatermark.transition_count < current.transition_count) {
      return true;
    }

    if (newWatermark.event_count < current.event_count) {
      return true;
    }

    return false;
  },

  reset: () => {
    set(initialState);
  },
}));
