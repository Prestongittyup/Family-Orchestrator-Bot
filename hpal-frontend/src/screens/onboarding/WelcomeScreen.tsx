/**
 * Welcome Screen for onboarding flow.
 * Displays options to create a new household or join an existing one.
 */

import React from "react";
import styles from "./OnboardingScreens.module.css";

interface WelcomeScreenProps {
  onCreateHousehold: () => void;
  onJoinHousehold: () => void;
}

export const WelcomeScreen: React.FC<WelcomeScreenProps> = ({
  onCreateHousehold,
  onJoinHousehold,
}) => {
  return (
    <div className={styles.screen}>
      <div className={styles.screenContent}>
        <div className={styles.header}>
          <h1 className={styles.title}>Welcome to HPAL</h1>
          <p className={styles.subtitle}>
            Family Orchestration & Coordination Platform
          </p>
        </div>

        <div className={styles.illustration}>
          <svg
            viewBox="0 0 200 200"
            className={styles.logo}
            aria-label="HPAL Logo"
          >
            <circle cx="100" cy="100" r="90" fill="none" stroke="currentColor" strokeWidth="2" />
            <circle cx="70" cy="80" r="20" fill="currentColor" />
            <circle cx="130" cy="80" r="20" fill="currentColor" />
            <circle cx="100" cy="130" r="20" fill="currentColor" />
            <path
              d="M 70 100 Q 100 115 130 100"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
            />
          </svg>
        </div>

        <div className={styles.description}>
          <h2>Coordinate Your Household</h2>
          <p>
            Keep your family synchronized with shared calendars, task management,
            budgets, and real-time communication — all in one place.
          </p>
        </div>

        <div className={styles.actions}>
          <button
            className={`${styles.button} ${styles.primary}`}
            onClick={onCreateHousehold}
            aria-label="Create a new household"
          >
            <span className={styles.buttonIcon}>+</span>
            Create Household
          </button>

          <button
            className={`${styles.button} ${styles.secondary}`}
            onClick={onJoinHousehold}
            aria-label="Join an existing household"
          >
            <span className={styles.buttonIcon}>→</span>
            Join Household
          </button>
        </div>

        <div className={styles.features}>
          <div className={styles.feature}>
            <span className={styles.featureIcon}>📅</span>
            <span className={styles.featureText}>Shared Calendar</span>
          </div>
          <div className={styles.feature}>
            <span className={styles.featureIcon}>✓</span>
            <span className={styles.featureText}>Task Management</span>
          </div>
          <div className={styles.feature}>
            <span className={styles.featureIcon}>💬</span>
            <span className={styles.featureText}>Family Chat</span>
          </div>
          <div className={styles.feature}>
            <span className={styles.featureIcon}>💰</span>
            <span className={styles.featureText}>Budget Tracking</span>
          </div>
        </div>
      </div>
    </div>
  );
};
