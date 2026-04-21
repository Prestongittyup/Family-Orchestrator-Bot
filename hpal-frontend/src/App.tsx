import React from "react";
import React, { useEffect, useState } from "react";
import { BrowserRouter as Router, Routes, Route, Navigate } from "react-router-dom";
import { AppShell } from "./ui/components/AppShell";
import { CalendarScreen } from "./ui/screens/CalendarScreen";
import { ChatScreen } from "./ui/screens/ChatScreen";
import { DashboardScreen } from "./ui/screens/DashboardScreen";
import { TasksScreen } from "./ui/screens/TasksScreen";
import { useRuntimeStore } from "./runtime/store";
import { onboardingFlow } from "./runtime/onboarding";
import { OnboardingContainer } from "./screens/onboarding/OnboardingContainer";

/** Retrieve stored household id without requiring URL params. */
function resolveStoredHouseholdId(): string | null {
  try {
    return localStorage.getItem("hpal-household-id");
  } catch {
    return null;
  }
}

const App: React.FC = () => {
  // Prioritise URL param (dev override) then localStorage then null (needs onboarding)
  const urlFamilyId = new URLSearchParams(window.location.search).get("familyId");
  const storedFamilyId = resolveStoredHouseholdId();
  const initialFamilyId = urlFamilyId || storedFamilyId;

  const initialize = useRuntimeStore((state) => state.initialize);
  const stopSyncLoop = useRuntimeStore((state) => state.stopSyncLoop);

  // Track whether onboarding has been completed:
  // true  → show main app
  // false → show onboarding screens
  const [onboardingDone, setOnboardingDone] = useState<boolean>(
    () => onboardingFlow.isComplete() || initialFamilyId !== null
  );

  // If familyId exists, init is safe; otherwise wait for onboarding to complete.
  const [familyId, setFamilyId] = useState<string>(initialFamilyId || "");

  useEffect(() => {
    if (onboardingDone && familyId) {
      initialize(familyId);
    }
    return () => {
      stopSyncLoop();
    };
  }, [onboardingDone, familyId, initialize, stopSyncLoop]);

  const handleOnboardingComplete = () => {
    const newHouseholdId = localStorage.getItem("hpal-household-id") || "family-1";
    setFamilyId(newHouseholdId);
    setOnboardingDone(true);
  };

  if (!onboardingDone) {
    return <OnboardingContainer onComplete={handleOnboardingComplete} />;
  }

  return (
    <Router>
      <AppShell>
        <Routes>
          <Route path="/" element={<DashboardScreen />} />
          <Route path="/tasks" element={<TasksScreen />} />
          <Route path="/calendar" element={<CalendarScreen />} />
          <Route path="/chat" element={<ChatScreen />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </AppShell>
    </Router>
  );
};

export default App;
