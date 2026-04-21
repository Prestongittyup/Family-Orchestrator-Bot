/**
 * Household Setup Screen for onboarding flow.
 * Collects household name and founder name/email.
 */

import React, { useState } from "react";
import styles from "./OnboardingScreens.module.css";

interface HouseholdSetupScreenProps {
  householdName?: string;
  founderName?: string;
  founderEmail?: string;
  onHouseholdNameChange: (name: string) => void;
  onFounderNameChange: (name: string) => void;
  onFounderEmailChange: (email: string) => void;
  onNext: () => void;
  onBack: () => void;
  canProgress: boolean;
  progress: number;
}

export const HouseholdSetupScreen: React.FC<HouseholdSetupScreenProps> = ({
  householdName = "",
  founderName = "",
  founderEmail = "",
  onHouseholdNameChange,
  onFounderNameChange,
  onFounderEmailChange,
  onNext,
  onBack,
  canProgress,
  progress,
}) => {
  const [errors, setErrors] = useState<Record<string, string>>({});

  const validateInputs = () => {
    const newErrors: Record<string, string> = {};

    if (!householdName.trim()) {
      newErrors.householdName = "Household name is required";
    }
    if (!founderName.trim()) {
      newErrors.founderName = "Your name is required";
    }
    if (founderEmail && !isValidEmail(founderEmail)) {
      newErrors.founderEmail = "Invalid email address";
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const isValidEmail = (email: string) => {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  };

  const handleNext = () => {
    if (validateInputs()) {
      onNext();
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
          <h1 className={styles.title}>Set Up Your Household</h1>
          <div className={styles.spacer} />
        </div>

        <div className={styles.formSection}>
          <div className={styles.formGroup}>
            <label htmlFor="householdName" className={styles.label}>
              Household Name *
            </label>
            <input
              id="householdName"
              type="text"
              className={`${styles.input} ${
                errors.householdName ? styles.inputError : ""
              }`}
              value={householdName}
              onChange={(e) => onHouseholdNameChange(e.target.value)}
              placeholder="e.g., Smith Family"
              autoFocus
              aria-label="Household name"
              aria-invalid={!!errors.householdName}
              aria-describedby={errors.householdName ? "householdName-error" : undefined}
            />
            {errors.householdName && (
              <span
                className={styles.error}
                id="householdName-error"
                role="alert"
              >
                {errors.householdName}
              </span>
            )}
            <p className={styles.helperText}>
              This will be shared with all family members
            </p>
          </div>

          <div className={styles.formGroup}>
            <label htmlFor="founderName" className={styles.label}>
              Your Name *
            </label>
            <input
              id="founderName"
              type="text"
              className={`${styles.input} ${
                errors.founderName ? styles.inputError : ""
              }`}
              value={founderName}
              onChange={(e) => onFounderNameChange(e.target.value)}
              placeholder="e.g., John Smith"
              aria-label="Your name"
              aria-invalid={!!errors.founderName}
              aria-describedby={errors.founderName ? "founderName-error" : undefined}
            />
            {errors.founderName && (
              <span
                className={styles.error}
                id="founderName-error"
                role="alert"
              >
                {errors.founderName}
              </span>
            )}
          </div>

          <div className={styles.formGroup}>
            <label htmlFor="founderEmail" className={styles.label}>
              Email Address (Optional)
            </label>
            <input
              id="founderEmail"
              type="email"
              className={`${styles.input} ${
                errors.founderEmail ? styles.inputError : ""
              }`}
              value={founderEmail}
              onChange={(e) => onFounderEmailChange(e.target.value)}
              placeholder="you@example.com"
              aria-label="Email address"
              aria-invalid={!!errors.founderEmail}
              aria-describedby={errors.founderEmail ? "founderEmail-error" : undefined}
            />
            {errors.founderEmail && (
              <span
                className={styles.error}
                id="founderEmail-error"
                role="alert"
              >
                {errors.founderEmail}
              </span>
            )}
            <p className={styles.helperText}>
              Used for household invitations and notifications
            </p>
          </div>
        </div>

        <div className={styles.actions}>
          <button
            className={`${styles.button} ${styles.primary} ${
              !canProgress ? styles.disabled : ""
            }`}
            onClick={handleNext}
            disabled={!canProgress}
            aria-label="Continue to next step"
          >
            Continue
          </button>
        </div>
      </div>
    </div>
  );
};
