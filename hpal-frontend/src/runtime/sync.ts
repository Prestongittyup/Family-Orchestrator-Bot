import type { FrontendState, SyncLoopConfig } from "./types";

export const DEFAULT_SYNC_CONFIG: SyncLoopConfig = {
  syncedMs: 30000,
  laggingMs: 10000,
  desyncedMs: 3000,
};

export function pollingInterval(state: FrontendState, config: SyncLoopConfig = DEFAULT_SYNC_CONFIG): number {
  if (state.sync_status === "synced") {
    return config.syncedMs;
  }
  if (state.sync_status === "lagging") {
    return config.laggingMs;
  }
  return config.desyncedMs;
}
