import React, { useEffect } from "react";
import { useRuntimeStore } from "../runtime/store";
import { backgroundSyncManager } from "../runtime/backgroundSync";
import { MobileNavigation } from "./MobileNavigation";
import styles from "./MobileLayout.module.css";

export interface MobileLayoutProps {
  children: React.ReactNode;
}

/**
 * Mobile-first responsive layout with:
 * - Bottom navigation on mobile/tablet
 * - Offline sync status indicator
 * - Safe area inset support (notches, home indicators)
 * - Responsive main content area
 */
export const MobileLayout: React.FC<MobileLayoutProps> = ({ children }) => {
  const [syncStatus, setSyncStatus] = React.useState(
    backgroundSyncManager.getStatus()
  );
  const [showOfflineIndicator, setShowOfflineIndicator] = React.useState(false);

  useEffect(() => {
    // Subscribe to sync status changes
    const unsubscribe = backgroundSyncManager.subscribe((status) => {
      setSyncStatus(status);

      // Show offline indicator if we have pending items
      setShowOfflineIndicator(!status.isOnline && status.pending > 0);
    });

    return unsubscribe;
  }, []);

  return (
    <div className={styles.layout}>
      {/* Offline status indicator */}
      {showOfflineIndicator && (
        <div className={styles.offlineIndicator} role="status" aria-live="polite">
          <span className={styles.offline Icon}>📡</span>
          <span className={styles.offlineText}>
            {syncStatus.syncing
              ? `Syncing ${syncStatus.pending} pending...`
              : `${syncStatus.pending} pending items`}
          </span>
        </div>
      )}

      {/* Main content area with safe padding for mobile nav */}
      <main className={styles.mainContent}>{children}</main>

      {/* Bottom navigation (mobile/tablet only) */}
      <MobileNavigation />
    </div>
  );
};

export default MobileLayout;
