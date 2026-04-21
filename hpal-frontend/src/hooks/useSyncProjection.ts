/**
 * useSyncProjection Hook
 *
 * Polling-based sync layer that respects watermark versioning
 * and prevents regression overwrites.
 * Polls every 10-30s depending on data freshness.
 */

import { useEffect, useRef } from "react";
import { useHPALStore } from "../store/hpal-store";
import { hpalClient } from "../api/hpal-client";

interface UseSyncProjectionOptions {
  familyId: string;
  enabled?: boolean;
  pollInterval?: number; // milliseconds
  onError?: (error: Error) => void;
}

export function useSyncProjection(options: UseSyncProjectionOptions) {
  const {
    familyId,
    enabled = true,
    pollInterval = 15000,
    onError,
  } = options;

  const {
    setLoading,
    setError,
    updateProjection,
    projection_watermark,
    last_change,
  } = useHPALStore();

  const intervalRef = useRef<NodeJS.Timeout | null>(null);
  const isMountedRef = useRef(true);

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
    };
  }, []);

  const sync = async () => {
    if (!enabled || !isMountedRef.current) {
      return;
    }

    try {
      setLoading(true);
      setError(null);

      // Fetch household overview and tasks in parallel
      const [overview, tasks, events] = await Promise.all([
        hpalClient.getHouseholdOverview(familyId),
        hpalClient.getTasksByFamily(familyId),
        hpalClient.getEventsByFamily(familyId),
      ]);

      if (!isMountedRef.current) {
        return;
      }

      // Reconstruct watermark from known state
      // In production, the backend would return this directly
      const watermark = {
        projection_epoch: (overview.family.system_state_summary.projection_epoch || 0) + 1,
        transition_count: overview.family.system_state_summary.pending_actions || 0,
        event_count: events.length,
        source_state_version: overview.family.system_state_summary.state_version || 0,
        snapshot_hash: "",
        last_projection_at: overview.family.system_state_summary.last_projection_at,
      };

      // Update projection (will reject if regression detected)
      updateProjection(overview, watermark);
      setLoading(false);
    } catch (error) {
      if (!isMountedRef.current) {
        return;
      }

      const err = error instanceof Error ? error : new Error(String(error));
      setError(err.message);
      setLoading(false);

      if (onError) {
        onError(err);
      }
    }
  };

  useEffect(() => {
    if (!enabled) {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
      return;
    }

    // Initial sync
    sync();

    // Set up polling
    intervalRef.current = setInterval(sync, pollInterval);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [enabled, familyId, pollInterval]);

  return {
    sync,
    watermark: projection_watermark,
  };
}

/**
 * useTaskSync Hook
 * Polls tasks more frequently (10s default) than overall projection
 */
export function useTaskSync(options: UseSyncProjectionOptions) {
  const { familyId, enabled = true, onError } = options;
  const { setTasks, setLoading, setError } = useHPALStore();
  const intervalRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    if (!enabled || !familyId) {
      return;
    }

    const syncTasks = async () => {
      try {
        setLoading(true);
        const tasks = await hpalClient.getTasksByFamily(familyId);
        setTasks(tasks);
        setLoading(false);
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        setError(err.message);
        if (onError) onError(err);
      }
    };

    syncTasks();
    intervalRef.current = setInterval(syncTasks, 10000);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [enabled, familyId, onError]);

  return {
    refetch: async () => {
      setLoading(true);
      try {
        const tasks = await hpalClient.getTasksByFamily(familyId);
        setTasks(tasks);
      } finally {
        setLoading(false);
      }
    },
  };
}
