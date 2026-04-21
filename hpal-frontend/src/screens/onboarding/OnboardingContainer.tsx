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
import { PushNotificationManager } from "../../runtime/pushNotifications";
import type { OnboardingState } from "../../runtime/onboarding";

const API_BASE_URL =
  (import.meta.env.VITE_API_BASE_URL as string | undefined) ??
  "http://localhost:8000";

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
  const [isProcessing, setIsProcessing] = useState(false);

  const pushManager = PushNotificationManager.getInstance();

  // Subscribe to onboarding state changes
  useEffect(() => {
    const unsubscribe = onboardingFlow.subscribe((newState) => {
      setState(newState);
    });

    return () => unsubscribe();
  }, []);

  // Handle welcome screen selection
  const handleCreateHousehold = () => {
    onboardingFlow.selectCreateHousehold();
  };

  const handleJoinHousehold = () => {
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
    try {
      setIsProcessing(true);

      // 1) Create household + founder user (server authoritative)
      const response = await fetch(`${API_BASE_URL}/v1/identity/household/create`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: state.householdName,
          timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
          founder_user_name: state.founderName,
          founder_email: state.founderEmail || undefined,
        }),
      });

      if (!response.ok) {
        throw new Error("Failed to create household");
      }

      const data = await response.json();
      const householdId = data.household?.household_id;
      const founderUserId = data.founder_user?.user_id;
      if (!householdId || !founderUserId) {
        throw new Error("Invalid household creation response");
      }

      localStorage.setItem("hpal-household-id", householdId);
      localStorage.setItem("hpal-user-id", founderUserId);
      localStorage.setItem("hpal-auth-name", state.founderName);
      if (state.founderEmail) {
        localStorage.setItem("hpal-auth-email", state.founderEmail);
      }

      // Move to role selection
      onboardingFlow.selectRole("ADULT");
    } catch (error) {
      console.error("Household creation failed:", error);
      // Show error to user - TODO: add error toast
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

  const handleRequestPermissions = async (): Promise<boolean> => {
    try {
      const householdId = localStorage.getItem("hpal-household-id") || "family-1";
      const userId = localStorage.getItem("hpal-user-id") || "user-admin";
      const permission = await pushManager.requestPermission(householdId, userId);
      return permission === "granted";
    } catch (error) {
      console.error("Permission request failed:", error);
      return false;
    }
  };

  const handleDeviceSetupComplete = async () => {
    try {
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

      const deviceResponse = await fetch(`${API_BASE_URL}/v1/identity/device/register`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          household_id: householdId,
          user_id: userId,
          device_name: state.deviceName,
          user_agent: navigator.userAgent,
          platform,
        }),
      });

      if (!deviceResponse.ok) {
        throw new Error("Failed to register device");
      }

      const deviceData = await deviceResponse.json();
      const deviceId = deviceData.device?.device_id;
      if (!deviceId) {
        throw new Error("Invalid device registration response");
      }
      localStorage.setItem("hpal-device-id", deviceId);

      // 3) Bootstrap identity to establish server-issued session token
      const bootstrapResponse = await fetch(`${API_BASE_URL}/v1/identity/bootstrap`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          household_id: householdId,
          user_id: userId,
          device_id: deviceId,
        }),
      });
      if (!bootstrapResponse.ok) {
        throw new Error("Failed to establish session");
      }

      const bootstrap = await bootstrapResponse.json();
      if (!bootstrap.session_token) {
        throw new Error("Missing session token in bootstrap response");
      }

      localStorage.setItem("hpal.session.token", bootstrap.session_token);
      localStorage.setItem("hpal-role", bootstrap.identity_context?.user_role || "ADULT");

      // Subscribe to push notifications if user granted permission
      const permission = pushManager.getPermissionStatus();
      if (permission === "granted") {
        await pushManager.subscribeAndRegister(householdId, userId);
      }

      // Complete onboarding
      onboardingFlow.completeOnboarding();
      onComplete();
    } catch (error) {
      console.error("Device setup failed:", error);
      // Show error to user - TODO: add error toast
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

      case "household-name":
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
            canProgress={onboardingFlow.canProgress()}
            progress={state.progress}
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
            onComplete={handleDeviceSetupComplete}
            onBack={() => onboardingFlow.goBack()}
            canProgress={onboardingFlow.canProgress()}
            progress={state.progress}
          />
        );

      default:
        return null;
    }
  };

  return <div className="onboarding-container">{renderScreen()}</div>;
};
