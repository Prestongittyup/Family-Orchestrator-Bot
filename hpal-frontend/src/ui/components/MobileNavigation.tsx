import React from "react";
import { useLocation } from "react-router-dom";
import styles from "./MobileNavigation.module.css";

export interface NavItem {
  label: string;
  path: string;
  icon: string;
  ariaLabel: string;
}

export interface MobileNavigationProps {
  items?: NavItem[];
  activeIndex?: number;
  onNavigate?: (path: string) => void;
}

const DEFAULT_NAV_ITEMS: NavItem[] = [
  {
    label: "Dashboard",
    path: "/",
    icon: "📊",
    ariaLabel: "Dashboard",
  },
  {
    label: "Tasks",
    path: "/tasks",
    icon: "✓",
    ariaLabel: "Tasks",
  },
  {
    label: "Chat",
    path: "/chat",
    icon: "💬",
    ariaLabel: "Chat with assistant",
  },
  {
    label: "Calendar",
    path: "/calendar",
    icon: "📅",
    ariaLabel: "Family calendar",
  },
];

/**
 * MobileNavigation - Bottom navigation bar for mobile-first UI
 *
 * Features:
 * - Touch-optimized tap targets (44px minimum)
 * - Responsive to viewport size
 * - Active state indicators
 * - Platform-aware (hides on desktop > 1024px)
 * - Accessible labels and ARIA attributes
 * - Deterministic active state from current route
 */
export const MobileNavigation: React.FC<MobileNavigationProps> = ({
  items = DEFAULT_NAV_ITEMS,
  onNavigate,
}) => {
  const location = useLocation();
  const currentPath = location.pathname;

  return (
    <nav className={styles.navigation} role="navigation" aria-label="Main navigation">
      <ul className={styles.navList}>
        {items.map((item, index) => (
          <li key={`${item.path}-${index}`} className={styles.navItem}>
            <a
              href={item.path}
              className={`${styles.navLink} ${
                currentPath === item.path ? styles.active : ""
              }`}
              aria-label={item.ariaLabel}
              aria-current={currentPath === item.path ? "page" : undefined}
              onClick={(e) => {
                if (onNavigate) {
                  e.preventDefault();
                  onNavigate(item.path);
                }
              }}
            >
              <span className={styles.icon}>{item.icon}</span>
              <span className={styles.label}>{item.label}</span>
              {currentPath === item.path && (
                <span className={styles.indicator} aria-hidden="true" />
              )}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  );
};

export default MobileNavigation;
