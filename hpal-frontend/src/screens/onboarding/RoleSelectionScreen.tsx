/**
 * Role Selection Screen for onboarding flow.
 * Displays role options with descriptions.
 */

import React from "react";
import styles from "./OnboardingScreens.module.css";
import type { UserRole } from "../../runtime/onboarding";

interface RoleSelectionScreenProps {
  selectedRole?: UserRole;
  onRoleSelect: (role: UserRole) => void;
  onBack: () => void;
  progress: number;
}

const ROLE_INFO: Record<UserRole, { label: string; description: string; icon: string }> = {
  ADMIN: {
    label: "Administrator",
    description: "Full access to all features and settings. Can manage household members and permissions.",
    icon: "👑",
  },
  ADULT: {
    label: "Adult",
    description: "Full access to shared information. Can create tasks and events, manage budgets.",
    icon: "👨",
  },
  CHILD: {
    label: "Child",
    description: "Limited access. Can view tasks and calendar, complete assigned tasks.",
    icon: "👶",
  },
  VIEW_ONLY: {
    label: "View Only",
    description: "Read-only access to household information. Cannot create or modify items.",
    icon: "👁️",
  },
};

export const RoleSelectionScreen: React.FC<RoleSelectionScreenProps> = ({
  selectedRole,
  onRoleSelect,
  onBack,
  progress,
}) => {
  const roles: UserRole[] = ["ADMIN", "ADULT", "CHILD", "VIEW_ONLY"];

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
          <h1 className={styles.title}>Select Your Role</h1>
          <div className={styles.spacer} />
        </div>

        <p className={styles.description}>
          Choose the access level that best fits your role in the household.
        </p>

        <div className={styles.roleGrid}>
          {roles.map((role) => (
            <button
              key={role}
              className={`${styles.roleCard} ${
                selectedRole === role ? styles.roleCardSelected : ""
              }`}
              onClick={() => onRoleSelect(role)}
              aria-label={`Select ${ROLE_INFO[role].label} role`}
              aria-pressed={selectedRole === role}
            >
              <div className={styles.roleIcon}>{ROLE_INFO[role].icon}</div>
              <h3 className={styles.roleLabel}>{ROLE_INFO[role].label}</h3>
              <p className={styles.roleDescription}>{ROLE_INFO[role].description}</p>
              {selectedRole === role && (
                <div className={styles.roleCheckmark} aria-hidden="true">
                  ✓
                </div>
              )}
            </button>
          ))}
        </div>

        <div className={styles.roleInfo}>
          {selectedRole && (
            <div className={styles.infoBox}>
              <h4>Role Permissions</h4>
              <RolePermissions role={selectedRole} />
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

interface RolePermissionsProps {
  role: UserRole;
}

const RolePermissions: React.FC<RolePermissionsProps> = ({ role }) => {
  const permissions: Record<UserRole, string[]> = {
    ADMIN: [
      "View & manage household",
      "Add/remove members",
      "Manage permissions",
      "Access all features",
      "Modify household settings",
    ],
    ADULT: [
      "View household information",
      "Create & manage tasks",
      "Create & manage events",
      "Manage family budgets",
      "Send messages",
    ],
    CHILD: [
      "View assigned tasks",
      "Mark tasks complete",
      "View calendar events",
      "Send messages",
      "View family health info",
    ],
    VIEW_ONLY: [
      "View tasks & calendar",
      "View budgets",
      "View messages",
      "No create/edit rights",
    ],
  };

  return (
    <ul className={styles.permissionsList}>
      {permissions[role].map((permission, idx) => (
        <li key={idx} className={styles.permissionItem}>
          ✓ {permission}
        </li>
      ))}
    </ul>
  );
};
