/**
 * Onboarding state and flow management for HPAL.
 *
 * Handles:
 * - Household creation flow
 * - Join household via invite
 * - User role selection
 * - Device registration handshake
 * - Deterministic flow progression
 */

import type { HouseholdRole } from "./identity";
import { HouseholdRole as HouseholdRoleEnum } from "./identity";

export type OnboardingStep =
  | "welcome"
  | "create-household"
  | "join-household"
  | "household-name"
  | "founder-name"
  | "founder-email"
  | "select-role"
  | "device-setup"
  | "connecting"
  | "complete";

export interface OnboardingState {
  step: OnboardingStep;
  householdId: string | null;
  householdName: string;
  founderName: string;
  founderEmail: string;
  userRole: HouseholdRole;
  joinToken: string | null;
  deviceName: string;
  isProcessing: boolean;
  error: string | null;
  progress: number; // 0-100
}

export class OnboardingFlow {
  private static readonly STORAGE_KEY = "hpal.onboarding.v1";

  private state: OnboardingState = this._loadPersistedOrDefault();

  private _defaultState(): OnboardingState {
    return {
      step: "welcome",
      householdId: null,
      householdName: "",
      founderName: "",
      founderEmail: "",
      userRole: HouseholdRoleEnum.CHILD,
      joinToken: null,
      deviceName: "",
      isProcessing: false,
      error: null,
      progress: 0,
    };
  }

  private _loadPersistedOrDefault(): OnboardingState {
    try {
      const raw = typeof window !== "undefined"
        ? window.localStorage.getItem(OnboardingFlow.STORAGE_KEY)
        : null;
      if (raw) {
        const parsed = JSON.parse(raw) as Partial<OnboardingState>;
        return { ...this._defaultState(), ...parsed };
      }
    } catch {
      // ignore storage errors
    }
    return this._defaultState();
  }

  private _persist(): void {
    try {
      if (typeof window !== "undefined") {
        window.localStorage.setItem(
          OnboardingFlow.STORAGE_KEY,
          JSON.stringify(this.state)
        );
      }
    } catch {
      // ignore storage errors
    }
  }

  private _fakeState: OnboardingState = {
  private listeners: Set<(state: OnboardingState) => void> = new Set();

  /**
   * Get current onboarding state
   */
  getState(): OnboardingState {
    return { ...this.state };
  }

  /**
   * Subscribe to state changes
   */
  subscribe(listener: (state: OnboardingState) => void): () => void {
    this.listeners.add(listener);
    listener({ ...this.state });

    return () => {
      this.listeners.delete(listener);
    };
  }

  /**
   * Choose to create new household
   */
  selectCreateHousehold(): void {
    this.setState({
      step: "household-name",
      householdName: "",
      founderName: "",
      founderEmail: "",
      progress: 10,
    });
  }

  /**
   * Choose to join existing household
   */
  selectJoinHousehold(token?: string): void {
    this.setState({
      step: "join-household",
      joinToken: token || null,
      progress: 10,
    });
  }

  /**
   * Set household name (create flow)
   */
  setHouseholdName(name: string): void {
    this.setState({
      householdName: name,
      error: null,
    });
  }

  /**
   * Set founder/user name
   */
  setFounderName(name: string): void {
    this.setState({
      founderName: name,
      error: null,
    });
  }

  /**
   * Set founder email (optional)
   */
  setFounderEmail(email: string): void {
    this.setState({
      founderEmail: email,
      error: null,
    });
  }

  /**
   * Select user role
   */
  selectRole(role: HouseholdRole): void {
    this.setState({
      userRole: role,
      step: "device-setup",
      progress: 70,
      error: null,
    });
  }

  /**
   * Set device name
   */
  setDeviceName(name: string): void {
    this.setState({
      deviceName: name,
      error: null,
    });
  }

  /**
   * Create household and complete onboarding
   */
  async completeHouseholdCreation(): Promise<boolean> {
    if (!this.state.householdName || !this.state.founderName) {
      this.setState({
        error: "Household name and founder name are required",
      });
      return false;
    }

    this.setState({ isProcessing: true, progress: 85 });

    try {
      // Call backend to create household
      const response = await fetch("/api/v1/identity/household/create", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: this.state.householdName,
          timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
          founder_user_name: this.state.founderName,
          founder_email: this.state.founderEmail || undefined,
        }),
      });

      if (!response.ok) {
        throw new Error(`Failed to create household: ${response.status}`);
      }

      const data = await response.json();
      this.setState({
        householdId: data.household.household_id,
        step: "connecting",
        progress: 90,
      });

      return true;
    } catch (err) {
      const errorMessage =
        err instanceof Error ? err.message : "Unknown error";
      this.setState({
        error: errorMessage,
        isProcessing: false,
      });
      return false;
    }
  }

  /**
   * Complete onboarding after successful identity bootstrap
   */
  completeOnboarding(): void {
    this.setState({
      step: "complete",
      progress: 100,
      isProcessing: false,
      error: null,
    });
  }

  /**
   * Go back to previous step (with validation)
   */
  goBack(): void {
    const previousSteps: Record<OnboardingStep, OnboardingStep> = {
      welcome: "welcome",
      "create-household": "welcome",
      "join-household": "welcome",
      "household-name": "create-household",
      "founder-name": "household-name",
      "founder-email": "founder-name",
      "select-role": "founder-email",
      "device-setup": "select-role",
      "connecting": "device-setup",
      "complete": "complete",
    };

    const previous = previousSteps[this.state.step];
    if (previous && previous !== this.state.step) {
      this.setState({
        step: previous,
        error: null,
      });
    }
  }

  /**
   * Reset onboarding to welcome screen
   */
  reset(): void {
    this.state = this._defaultState();
    try {
      if (typeof window !== "undefined") {
        window.localStorage.removeItem(OnboardingFlow.STORAGE_KEY);
      }
    } catch { /* ignore */ }
    this.notifyListeners();
  }

  /**
   * Update state and notify listeners
   */
  private setState(updates: Partial<OnboardingState>): void {
    this.state = { ...this.state, ...updates };
    this._persist();
    this.notifyListeners();
  }

  /**
   * Notify all listeners of state change
   */
  private notifyListeners(): void {
    const state = { ...this.state };
    this.listeners.forEach((listener) => listener(state));
  }

  /**
   * Validate if onboarding is complete
   */
  isComplete(): boolean {
    return this.state.step === "complete" && this.state.householdId !== null;
  }

  /**
   * Validate if we have minimum required data for completion
   */
  canProgress(): boolean {
    switch (this.state.step) {
      case "household-name":
        return this.state.householdName.length > 0;
      case "founder-name":
        return this.state.founderName.length > 0;
      case "device-setup":
        return this.state.deviceName.length > 0;
      default:
        return true;
    }
  }
}

/**
 * Global singleton instance
 */
export const onboardingFlow = new OnboardingFlow();
