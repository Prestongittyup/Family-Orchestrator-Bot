/**
 * OnboardingContainer component that orchestrates all onboarding screens.
 * Manages state transitions and backend integration.
 */

import React, { useEffect, useState } from "react";
import { onboardingFlow } from "../../runtime/onboarding";
import { WelcomeScreen } from "./WelcomeScreen";
import { HouseholdSetupScreen } from "./HouseholdSetupScreen";
import { RoleSelectionScreen } from "./RoleSelectionScreen";
import { DeviceSetupScreen } from "./DeviceSetupScreen";
import { pushNotificationManager } from "../../runtime/pushNotifications";
import { buildApiUrl, fetchWithApiFallback } from "../../api/network";
import type { OnboardingState } from "../../runtime/onboarding";

const SHADOW_INTEGRATION_ENDPOINTS_ENABLED = false;

const makeIdempotencyKey = (scope: string): string => {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `${scope}-${crypto.randomUUID()}`;
  }
  return `${scope}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
};

const parseResponseDetail = async (response: Response): Promise<string> => {
  try {
    const payload = (await response.clone().json()) as {
      detail?: unknown;
      error?: unknown;
      error_code?: unknown;
    };
    if (typeof payload.detail === "string") {
      return payload.detail;
    }
    if (typeof payload.error === "string") {
      return payload.error;
    }
    if (typeof payload.error_code === "string") {
      return payload.error_code;
    }
  } catch {
    // Ignore JSON parse errors; fallback to text.
  }

  try {
    const text = await response.clone().text();
    return text.trim();
  } catch {
    return "";
  }
};

const buildCreateHouseholdBody = (state: OnboardingState, includeFounderEmail: boolean): string =>
  JSON.stringify({
    name: state.householdName,
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
    founder_user_name: state.founderName,
    founder_email: includeFounderEmail ? state.founderEmail || undefined : undefined,
  });

interface OnboardingContainerProps {
  onComplete: () => void;
}

/**
 * Main onboarding container that orchestrates all screens.
 */
export const OnboardingContainer: React.FC<OnboardingContainerProps> = ({
  onComplete,
}) => {
  const [state, setState] = useState<OnboardingState>(onboardingFlow.getState());
  const [uiError, setUiError] = useState<string | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);

  // Subscribe to onboarding state changes
  useEffect(() => {
    const unsubscribe = onboardingFlow.subscribe((newState) => {
      setState(newState);
    });

    return () => unsubscribe();
  }, []);

  useEffect(() => {
    if (state.step === "complete") {
      onComplete();
    }
  }, [state.step, onComplete]);

  // Handle welcome screen selection
  const handleCreateHousehold = () => {
    setUiError(null);
    onboardingFlow.selectCreateHousehold();
  };

  const handleJoinHousehold = () => {
    setUiError(null);
    onboardingFlow.selectJoinHousehold("");
  };

  // Handle household setup
  const handleHouseholdNameChange = (name: string) => {
    onboardingFlow.setHouseholdName(name);
  };

  const handleFounderNameChange = (name: string) => {
    onboardingFlow.setFounderName(name);
  };

  const handleFounderEmailChange = (email: string) => {
    onboardingFlow.setFounderEmail(email);
  };

  const handleHouseholdSetupNext = async () => {
    if (isProcessing) {
      return;
    }

    try {
      setUiError(null);
      setIsProcessing(true);

      // 1) Create household + founder user (server authoritative)
      const createKey = makeIdempotencyKey("household-create");
      let response = await fetchWithApiFallback("/v1/identity/household/create", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-idempotency-key": createKey,
        },
        body: buildCreateHouseholdBody(state, true),
      });

      // Founder emails are globally unique; if email already exists, retry once without email.
      if (!response.ok && response.status === 400 && state.founderEmail) {
        const detail = await parseResponseDetail(response);
        if (detail.includes("founder_email_already_exists")) {
          const fallbackCreateKey = makeIdempotencyKey("household-create-no-email");
          response = await fetchWithApiFallback("/v1/identity/household/create", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "x-idempotency-key": fallbackCreateKey,
            },
            body: buildCreateHouseholdBody(state, false),
          });
        }
      }

      if (!response.ok) {
        const detail = await parseResponseDetail(response);
        if (response.status === 409 && detail.includes("duplicate_request")) {
          throw new Error("Duplicate request detected. Please press Continue once after a short pause.");
        }
        if (response.status === 400 && detail.includes("founder_email_already_exists")) {
          throw new Error("That email is already linked to another account. Try a different email or leave email blank.");
        }
        throw new Error(
          `Failed to create household (HTTP ${response.status})${detail ? `: ${detail}` : ""}`
        );
      }

      const data = await response.json();
      const householdId = data.household?.household_id;
      const founderUserId = data.founder_user?.user_id;
      if (!householdId || !founderUserId) {
        throw new Error("Invalid household creation response");
      }

      localStorage.setItem("hpal-household-id", householdId);
      localStorage.setItem(
        "hpal-household-name",
        (typeof data.household?.name === "string" && data.household.name.trim())
          ? data.household.name
          : state.householdName,
      );
      localStorage.setItem("hpal-user-id", founderUserId);
      localStorage.setItem("hpal-auth-name", state.founderName);
      if (typeof data.founder_user?.email === "string" && data.founder_user.email) {
        localStorage.setItem("hpal-auth-email", data.founder_user.email);
      }

      // Move to role selection
      onboardingFlow.selectRole("ADULT");
    } catch (error) {
      console.error("Household creation failed:", error);
      const message = error instanceof Error ? error.message : "Failed to create household";
      setUiError(message);
    } finally {
      setIsProcessing(false);
    }
  };

  // Handle role selection
  const handleRoleSelect = (role: any) => {
    onboardingFlow.selectRole(role);
  };

  // Handle device setup
  const handleDeviceNameChange = (name: string) => {
    onboardingFlow.setDeviceName(name);
  };

  const handleConnectGoogleAccount = () => {
    if (!SHADOW_INTEGRATION_ENDPOINTS_ENABLED) {
      setUiError("Google connect is disabled in canonical mode.");
      return;
    }

    const userId = localStorage.getItem("hpal-user-id");
    if (!userId) {
      setUiError("Complete household creation first so we can link the right Google account.");
      return;
    }

    const householdId = localStorage.getItem("hpal-household-id") || "";
    const returnBase = window.location.origin;
    const encodedUser = encodeURIComponent(userId);
    const query = new URLSearchParams({
      return_base: returnBase,
      household_id: householdId,
    }).toString();
    // TODO: REMOVE_SHADOW_ENDPOINT
    window.location.href = buildApiUrl(`/integrations/google-calendar/connect/${encodedUser}?${query}`);
  };

  const handleRequestPermissions = async (): Promise<boolean> => {
    try {
      const householdId = localStorage.getItem("hpal-household-id") || "family-1";
      const userId = localStorage.getItem("hpal-user-id") || "user-admin";
      const permission = await pushNotificationManager.requestPermission(householdId, userId);
      return permission === "granted";
    } catch (error) {
      console.error("Permission request failed:", error);
      return false;
    }
  };

  const handleDeviceSetupComplete = async () => {
    if (isProcessing) {
      return;
    }

    try {
      setUiError(null);
      setIsProcessing(true);

      const householdId = localStorage.getItem("hpal-household-id");
      const userId = localStorage.getItem("hpal-user-id");
      if (!householdId || !userId) {
        throw new Error("Missing household/user identity during device setup");
      }

      // 2) Register device with backend
      const platform = /iphone|ipad|ios/i.test(navigator.userAgent)
        ? "iOS"
        : /android/i.test(navigator.userAgent)
        ? "Android"
        : "Web";
      const deviceRegisterKey = makeIdempotencyKey("device-register");

      const deviceResponse = await fetchWithApiFallback("/v1/identity/device/register", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-idempotency-key": deviceRegisterKey,
        },
        body: JSON.stringify({
          household_id: householdId,
          user_id: userId,
          device_name: state.deviceName,
          user_agent: navigator.userAgent,
          platform,
        }),
      });

      if (!deviceResponse.ok) {
        throw new Error(`Failed to register device (HTTP ${deviceResponse.status})`);
      }

      const deviceData = await deviceResponse.json();
      const deviceId = deviceData.device?.device_id;
      if (!deviceId) {
        throw new Error("Invalid device registration response");
      }
      localStorage.setItem("hpal-device-id", deviceId);

      // 3) Bootstrap identity to establish server-issued session token
      const bootstrapKey = makeIdempotencyKey("identity-bootstrap");
      const bootstrapResponse = await fetchWithApiFallback("/v1/identity/bootstrap", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-idempotency-key": bootstrapKey,
        },
        body: JSON.stringify({
          household_id: householdId,
          user_id: userId,
          device_id: deviceId,
        }),
      });
      if (!bootstrapResponse.ok) {
        throw new Error(`Failed to establish session (HTTP ${bootstrapResponse.status})`);
      }

      const bootstrap = await bootstrapResponse.json();
      if (!bootstrap.session_token) {
        throw new Error("Missing session token in bootstrap response");
      }

      localStorage.setItem("hpal.session.token", bootstrap.session_token);
      localStorage.setItem("hpal-role", bootstrap.identity_context?.user_role || "ADULT");

      // Subscribe to push notifications if user granted permission
      const permission = pushNotificationManager.getPermissionStatus();
      if (permission === "granted") {
        await pushNotificationManager.subscribeAndRegister(householdId, userId);
      }

      // Complete onboarding
      onboardingFlow.completeOnboarding();
      onComplete();
    } catch (error) {
      console.error("Device setup failed:", error);
      const message = error instanceof Error ? error.message : "Failed to complete device setup";
      setUiError(message);
    } finally {
      setIsProcessing(false);
    }
  };

  // Render appropriate screen based on current step
  const renderScreen = () => {
    switch (state.step) {
      case "welcome":
        return (
          <WelcomeScreen
            onCreateHousehold={handleCreateHousehold}
            onJoinHousehold={handleJoinHousehold}
          />
        );

      case "create-household":

      case "household-name":

      case "founder-name":

      case "founder-email":
        return (
          <HouseholdSetupScreen
            householdName={state.householdName}
            founderName={state.founderName}
            founderEmail={state.founderEmail}
            onHouseholdNameChange={handleHouseholdNameChange}
            onFounderNameChange={handleFounderNameChange}
            onFounderEmailChange={handleFounderEmailChange}
            onNext={handleHouseholdSetupNext}
            onBack={() => onboardingFlow.goBack()}
            canProgress={onboardingFlow.canProgress() && !isProcessing}
            isProcessing={isProcessing}
            progress={state.progress}
          />
        );

      case "join-household":
        // Join flow UI is not implemented yet; avoid a blank screen and keep navigation usable.
        return (
          <WelcomeScreen
            onCreateHousehold={handleCreateHousehold}
            onJoinHousehold={handleJoinHousehold}
          />
        );

      case "select-role":
        return (
          <RoleSelectionScreen
            selectedRole={state.userRole}
            onRoleSelect={handleRoleSelect}
            onBack={() => onboardingFlow.goBack()}
            progress={state.progress}
          />
        );

      case "device-setup":
        return (
          <DeviceSetupScreen
            deviceName={state.deviceName}
            onDeviceNameChange={handleDeviceNameChange}
            onRequestPermissions={handleRequestPermissions}
            onConnectGoogleAccount={handleConnectGoogleAccount}
            onComplete={handleDeviceSetupComplete}
            onBack={() => onboardingFlow.goBack()}
            canProgress={onboardingFlow.canProgress() && !isProcessing}
            isProcessing={isProcessing}
            progress={state.progress}
          />
        );

      case "connecting":
        return (
          <div className="screen-panel">
            Finalizing setup...
          </div>
        );

      case "complete":
        return (
          <div className="screen-panel">
            Setup complete. Loading dashboard...
          </div>
        );

      default:
        return (
          <WelcomeScreen
            onCreateHousehold={handleCreateHousehold}
            onJoinHousehold={handleJoinHousehold}
          />
        );
    }
  };

  return (
    <div className="onboarding-container">
      {uiError ? <p className="error-text">{uiError}</p> : null}
      {renderScreen()}
    </div>
  );
};
