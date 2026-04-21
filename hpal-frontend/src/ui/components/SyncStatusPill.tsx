import React from "react";
import type { SyncStatus } from "../../runtime/types";

interface SyncStatusPillProps {
  status: SyncStatus;
}

export const SyncStatusPill: React.FC<SyncStatusPillProps> = ({ status }) => {
  const label = status === "synced" ? "Synced" : status === "lagging" ? "Lagging" : "Desynced";
  return <span className={`sync-pill sync-${status}`}>{label}</span>;
};
