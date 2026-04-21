/**
 * Device Setup Screen for onboarding flow.
 * Collects device name and requests permissions.
 */

import React, { useState } from "react";
import styles from "./OnboardingScreens.module.css";

interface DeviceSetupScreenProps {
  deviceName?: string;
  onDeviceNameChange: (name: string) => void;
  onRequestPermissions: () => Promise<boolean>;
  onComplete: () => void;
  onBack: () => void;
  canProgress: boolean;
  progress: number;
}

export const DeviceSetupScreen: React.FC<DeviceSetupScreenProps> = ({
  deviceName = "",
  onDeviceNameChange,
  onRequestPermissions,
  onComplete,
  onBack,
  canProgress,
  progress,
}) => {
  const [isRequestingPermissions, setIsRequestingPermissions] = useState(false);
  const [permissionsGranted, setPermissionsGranted] = useState(false);
  const [error, setError] = useState("");

  const handleRequestPermissions = async () => {
    setIsRequestingPermissions(true);
    setError("");

    try {
      const granted = await onRequestPermissions();
      setPermissionsGranted(granted);

      if (!granted) {
        setError(
          "Notifications are recommended for household alerts. You can enable them later in settings."
        );
      }
    } catch (err) {
      setError("Failed to request permissions. Please try again.");
      console.error("Permission request error:", err);
    } finally {
      setIsRequestingPermissions(false);
    }
  };

  const handleComplete = () => {
    if (canProgress) {
      onComplete();
    }
  };

  return (
    <div className={styles.screen}>
      <div className={styles.progressBar}>
        <div className={styles.progress} style={{ width: `${progress}%` }} />
      </div>

      <div className={styles.screenContent}>
        <div className={styles.header}>
          <button
            className={styles.backButton}
            onClick={onBack}
            aria-label="Go back to previous step"
          >
            ←
          </button>
          <h1 className={styles.title}>Set Up This Device</h1>
          <div className={styles.spacer} />
        </div>

        <div className={styles.formSection}>
          <div className={styles.formGroup}>
            <label htmlFor="deviceName" className={styles.label}>
              Device Name *
            </label>
            <input
              id="deviceName"
              type="text"
              className={styles.input}
              value={deviceName}
              onChange={(e) => onDeviceNameChange(e.target.value)}
              placeholder="e.g., Mom's iPhone"
              autoFocus
              aria-label="Device name"
              aria-describedby="deviceName-hint"
            />
            <p className={styles.helperText} id="deviceName-hint">
              This helps identify the device in household notifications
            </p>
          </div>

          <div className={styles.permissionSection}>
            <h3 className={styles.sectionTitle}>Permissions</h3>
            <p className={styles.sectionDescription}>
              Enable notifications to stay updated with household events, tasks,
              and messages.
            </p>

            {error && (
              <div className={styles.errorMessage} role="alert">
                {error}
              </div>
            )}

            <div className={styles.permissionCard}>
              <div className={styles.permissionHeader}>
                <span className={styles.permissionIcon}>🔔</span>
                <div>
                  <h4 className={styles.permissionName}>Notifications</h4>
                  <p className={styles.permissionStatus}>
                    {permissionsGranted
                      ? "✓ Enabled"
                      : isRequestingPermissions
                      ? "Requesting..."
                      : "Not enabled"}
                  </p>
                </div>
              </div>
              <p className={styles.permissionDescription}>
                Receive push notifications for household updates, task reminders,
                and family messages.
              </p>
              {!permissionsGranted && (
                <button
                  className={`${styles.button} ${styles.secondary} ${
                    isRequestingPermissions ? styles.disabled : ""
                  }`}
                  onClick={handleRequestPermissions}
                  disabled={isRequestingPermissions}
                  aria-label="Enable notifications"
                >
                  {isRequestingPermissions ? "Requesting..." : "Enable"}
                </button>
              )}
              {permissionsGranted && (
                <div className={styles.checkmark}>✓</div>
              )}
            </div>

            <div className={styles.permissionCard}>
              <div className={styles.permissionHeader}>
                <span className={styles.permissionIcon}>📍</span>
                <div>
                  <h4 className={styles.permissionName}>Background Sync</h4>
                  <p className={styles.permissionStatus}>✓ Automatic</p>
                </div>
              </div>
              <p className={styles.permissionDescription}>
                Messages and tasks are queued and synced automatically when you're
                offline. No action needed.
              </p>
            </div>

            <div className={styles.permissionCard}>
              <div className={styles.permissionHeader}>
                <span className={styles.permissionIcon}>💾</span>
                <div>
                  <h4 className={styles.permissionName}>Local Storage</h4>
                  <p className={styles.permissionStatus}>✓ Enabled</p>
                </div>
              </div>
              <p className={styles.permissionDescription}>
                Device data is cached locally for offline access. The app uses
                minimal storage space.
              </p>
            </div>
          </div>
        </div>

        <div className={styles.actions}>
          <button
            className={`${styles.button} ${styles.primary} ${
              !canProgress ? styles.disabled : ""
            }`}
            onClick={handleComplete}
            disabled={!canProgress || isRequestingPermissions}
            aria-label="Complete onboarding"
          >
            Complete Setup
          </button>
          <label className={styles.skipLabel}>
            <input
              type="checkbox"
              className={styles.skipCheckbox}
              onChange={(e) => {
                if (e.target.checked) {
                  setPermissionsGranted(true);
                }
              }}
              aria-label="Skip notifications setup (can be enabled later)"
            />
            <span>Skip for now (can be enabled later)</span>
          </label>
        </div>
      </div>
    </div>
  );
};
