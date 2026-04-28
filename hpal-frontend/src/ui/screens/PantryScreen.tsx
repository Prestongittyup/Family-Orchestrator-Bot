import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import type { PantryState } from "../../api/contracts";
import { useRuntimeStore } from "../../runtime/store";
import { SyncStatusPill } from "../components/SyncStatusPill";

const ASSISTANT_PENDING_PROMPT_KEY = "hpal.assistant.pending_prompt";
const ASSISTANT_PENDING_AUTOSEND_KEY = "hpal.assistant.pending_autosend";

const EMPTY_PANTRY: PantryState = {
  low_stock_count: 0,
  inventory_items: [],
  weekly_recipe_suggestions: [],
  grocery_recommendations: [],
};

const statusClassName = (status: string): string => {
  if (status === "out_of_stock") {
    return "pantry-status-critical";
  }
  if (status === "low") {
    return "pantry-status-warning";
  }
  return "pantry-status-ok";
};

const statusLabel = (status: string): string => {
  if (status === "out_of_stock") {
    return "Out of stock";
  }
  if (status === "low") {
    return "Low";
  }
  return "In stock";
};

const prettyMealType = (value: string): string =>
  value.replace(/_/g, " ").replace(/\b\w/g, (match) => match.toUpperCase());

const normalizeInventoryName = (value: string): string => value.trim().toLowerCase();

const inventorySelectionKey = (itemName: string, unit: string): string =>
  `${normalizeInventoryName(itemName)}::${unit}`;

const INVENTORY_UNIT_OPTIONS = [
  { value: "count", label: "Count" },
  { value: "can", label: "Can" },
  { value: "pack", label: "Pack" },
  { value: "oz", label: "oz" },
  { value: "lb", label: "lb" },
  { value: "g", label: "g" },
  { value: "kg", label: "kg" },
  { value: "ml", label: "ml" },
  { value: "fl_oz", label: "fl oz" },
  { value: "l", label: "L" },
] as const;

const prettyUnit = (value: string): string => {
  const option = INVENTORY_UNIT_OPTIONS.find((candidate) => candidate.value === value);
  return option ? option.label : value;
};

const formatQuantity = (value: number): string => {
  if (!Number.isFinite(value)) {
    return "0";
  }
  if (Math.abs(value - Math.round(value)) < 0.001) {
    return String(Math.round(value));
  }
  return value.toFixed(2).replace(/\.00$/, "").replace(/(\.\d)0$/, "$1");
};

const normalizeRecipeUrl = (rawValue: string | null | undefined): string | null => {
  const value = (rawValue || "").trim();
  if (!value) {
    return null;
  }

  if (/^https?:\/\//i.test(value)) {
    return value;
  }

  const sanitized = value.replace(/^\/+/, "");
  if (!sanitized) {
    return null;
  }

  return `https://${sanitized}`;
};

const parseUrl = (value: string): URL | null => {
  try {
    return new URL(value);
  } catch {
    return null;
  }
};

const isRootRecipeUrl = (parsed: URL): boolean =>
  (parsed.pathname === "" || parsed.pathname === "/")
  && !parsed.search
  && !parsed.hash;

const buildRecipeSearchUrl = (
  recipeName: string,
  recipeSource: string | null | undefined,
  normalizedRecipeUrl: string | null,
): string | null => {
  const name = recipeName.trim();
  if (!name) {
    return null;
  }

  const encodedName = encodeURIComponent(name);
  if (normalizedRecipeUrl) {
    const parsed = parseUrl(normalizedRecipeUrl);
    if (parsed) {
      return `${parsed.origin}/search?q=${encodedName}`;
    }
  }

  const source = (recipeSource || "").trim();
  const fallbackQuery = source ? `${name} recipe ${source}` : `${name} recipe`;
  return `https://www.google.com/search?q=${encodeURIComponent(fallbackQuery)}`;
};

const resolveRecipeLink = (
  recipeName: string,
  recipeUrl: string | null | undefined,
  recipeSource: string | null | undefined,
): { href: string; label: string } | null => {
  const normalizedRecipeUrl = normalizeRecipeUrl(recipeUrl);
  const sourceLabel = (recipeSource || "").trim();

  if (!normalizedRecipeUrl) {
    const fallbackHref = buildRecipeSearchUrl(recipeName, recipeSource, null);
    if (!fallbackHref) {
      return null;
    }

    return {
      href: fallbackHref,
      label: sourceLabel || "Search recipe",
    };
  }

  const parsed = parseUrl(normalizedRecipeUrl);
  if (parsed && isRootRecipeUrl(parsed)) {
    const searchHref = buildRecipeSearchUrl(recipeName, recipeSource, normalizedRecipeUrl);
    if (searchHref) {
      return {
        href: searchHref,
        label: sourceLabel || parsed.hostname,
      };
    }
  }

  return {
    href: normalizedRecipeUrl,
    label: sourceLabel || parsed?.hostname || normalizedRecipeUrl,
  };
};

export const PantryScreen: React.FC = () => {
  const navigate = useNavigate();
  const runtimeState = useRuntimeStore((state) => state.runtimeState);
  const adjustPantryInventory = useRuntimeStore((state) => state.adjustPantryInventory);
  const ingestPantryReceipt = useRuntimeStore((state) => state.ingestPantryReceipt);
  const [draftItemName, setDraftItemName] = useState("");
  const [draftQuantity, setDraftQuantity] = useState("1");
  const [draftUnit, setDraftUnit] = useState("count");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [showDepletedItems, setShowDepletedItems] = useState(false);
  const [inventoryUpdateError, setInventoryUpdateError] = useState<string | null>(null);
  const [inventoryUpdateMessage, setInventoryUpdateMessage] = useState<string | null>(null);
  const [receiptInputKey, setReceiptInputKey] = useState(0);
  const [receiptFile, setReceiptFile] = useState<File | null>(null);
  const [receiptBusyMode, setReceiptBusyMode] = useState<"preview" | "apply" | null>(null);
  const [receiptMessage, setReceiptMessage] = useState<string | null>(null);
  const [receiptError, setReceiptError] = useState<string | null>(null);
  const [receiptDetectedItems, setReceiptDetectedItems] = useState<Array<{ item: string; delta: number; unit?: string }>>([]);
  const [selectedInventoryItems, setSelectedInventoryItems] = useState<string[]>([]);

  if (!runtimeState) {
    return <section className="screen-panel">Loading pantry...</section>;
  }

  const pantry = runtimeState.snapshot.pantry ?? EMPTY_PANTRY;
  const inventoryRows = pantry.inventory_items.map((item) => ({
    item,
    selectionKey: inventorySelectionKey(item.name, item.unit),
  }));
  const totalItems = inventoryRows.length;
  const visibleInventoryRows = showDepletedItems
    ? inventoryRows
    : inventoryRows.filter((row) => row.item.quantity > 0);
  const visibleInventoryItems = visibleInventoryRows.map((row) => row.item);
  const hiddenDepletedCount = inventoryRows.length - visibleInventoryRows.length;
  const outOfStockCount = pantry.inventory_items.filter((item) => item.status === "out_of_stock").length;
  const selectedSet = new Set(selectedInventoryItems);
  const inventoryRowBySelectionKey = new Map(inventoryRows.map((row) => [row.selectionKey, row.item]));
  const selectedInventoryRows = selectedInventoryItems
    .map((selectionKey) => inventoryRowBySelectionKey.get(selectionKey))
    .filter((row): row is PantryState["inventory_items"][number] => Boolean(row));
  const allVisibleSelected =
    visibleInventoryRows.length > 0
    && visibleInventoryRows.every((row) => selectedSet.has(row.selectionKey));
  const selectedCount = selectedInventoryRows.length;
  const selectedInventorySummary = Array.from(new Set(selectedInventoryRows.map((item) => item.name))).slice(0, 5).join(", ");

  const openAssistantWorkflow = (prompt: string) => {
    try {
      localStorage.setItem(ASSISTANT_PENDING_PROMPT_KEY, prompt);
      localStorage.setItem(ASSISTANT_PENDING_AUTOSEND_KEY, "1");
    } catch {
      // Best-effort fallback for assistant quick links.
    }

    const params = new URLSearchParams({
      prompt,
      autosend: "1",
    });
    navigate(`/assistant?${params.toString()}`);
  };

  const onToggleInventorySelection = (selectionKey: string) => {
    setSelectedInventoryItems((current) => {
      if (current.includes(selectionKey)) {
        return current.filter((name) => name !== selectionKey);
      }
      return [...current, selectionKey].sort((left, right) => left.localeCompare(right));
    });
  };

  const onToggleSelectAllVisible = () => {
    if (visibleInventoryRows.length === 0) {
      return;
    }

    setSelectedInventoryItems((current) => {
      const currentSet = new Set(current);
      const visibleSelectionKeys = visibleInventoryRows.map((row) => row.selectionKey);
      const shouldClearVisible = visibleSelectionKeys.every((selectionKey) => currentSet.has(selectionKey));

      if (shouldClearVisible) {
        const next = current.filter((selectionKey) => !visibleSelectionKeys.includes(selectionKey));
        return next.sort((left, right) => left.localeCompare(right));
      }

      for (const selectionKey of visibleSelectionKeys) {
        currentSet.add(selectionKey);
      }
      return Array.from(currentSet).sort((left, right) => left.localeCompare(right));
    });
  };

  const onClearSelection = () => {
    setSelectedInventoryItems([]);
  };

  const onBulkAdjustInventory = async (mode: "remove" | "decrement") => {
    if (isSubmitting || selectedInventoryRows.length === 0) {
      return;
    }

    const grouped = new Map<string, { item: string; delta: number; unit: string }>();
    for (const item of selectedInventoryRows) {
      const normalizedItemName = normalizeInventoryName(item.name);
      const unit = item.unit || "count";
      const bucketKey = `${normalizedItemName}::${unit}`;
      const delta = mode === "remove" ? -item.quantity : -Math.min(item.quantity, 1);
      const current = grouped.get(bucketKey);
      if (current) {
        current.delta += delta;
      } else {
        grouped.set(bucketKey, {
          item: normalizedItemName,
          delta,
          unit,
        });
      }
    }

    const updates = Array.from(grouped.values()).filter(
      (row) => Number.isFinite(row.delta) && row.delta !== 0,
    );

    if (updates.length === 0) {
      setInventoryUpdateError("Selected items cannot be adjusted right now.");
      setInventoryUpdateMessage(null);
      return;
    }

    setIsSubmitting(true);
    setInventoryUpdateError(null);
    setInventoryUpdateMessage(null);

    try {
      await adjustPantryInventory(
        updates,
        mode === "remove" ? "manual bulk inventory remove" : "manual bulk inventory decrement",
      );
      const verb = mode === "remove" ? "Deleted" : "Decremented";
      setInventoryUpdateMessage(`${verb} ${updates.length} selected pantry item${updates.length === 1 ? "" : "s"}.`);
      setSelectedInventoryItems([]);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to update selected items right now.";
      setInventoryUpdateError(message);
    } finally {
      setIsSubmitting(false);
    }
  };

  const onAddInventoryItem = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (isSubmitting) {
      return;
    }

    const itemName = normalizeInventoryName(draftItemName);
    const quantity = Number.parseFloat(draftQuantity);
    if (!itemName) {
      setInventoryUpdateError("Enter an item name to add.");
      setInventoryUpdateMessage(null);
      return;
    }

    if (!Number.isFinite(quantity) || quantity <= 0) {
      setInventoryUpdateError("Quantity must be greater than 0.");
      setInventoryUpdateMessage(null);
      return;
    }

    setIsSubmitting(true);
    setInventoryUpdateError(null);
    setInventoryUpdateMessage(null);
    try {
      await adjustPantryInventory(
        [{ item: itemName, delta: quantity, unit: draftUnit }],
        "manual inventory add",
      );
      setDraftItemName("");
      setDraftQuantity("1");
      setDraftUnit("count");
      setSelectedInventoryItems([]);
      const quantityLabel = formatQuantity(quantity);
      const unitLabel = prettyUnit(draftUnit);
      setInventoryUpdateMessage(`Added ${quantityLabel} ${unitLabel} of ${itemName}.`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to update inventory right now.";
      setInventoryUpdateError(message);
    } finally {
      setIsSubmitting(false);
    }
  };

  const onAdjustInventoryItem = async (
    itemName: string,
    delta: number,
    unit: string,
    note: string,
    successMessage: string,
  ) => {
    if (isSubmitting) {
      return;
    }

    if (!Number.isFinite(delta) || delta === 0) {
      return;
    }

    setIsSubmitting(true);
    setInventoryUpdateError(null);
    setInventoryUpdateMessage(null);
    try {
      await adjustPantryInventory([{ item: itemName, delta, unit }], note);
      const normalizedName = normalizeInventoryName(itemName);
      setSelectedInventoryItems((current) =>
        current.filter((selectionKey) => {
          const [selectedName = "", selectedUnit = "count"] = selectionKey.split("::");
          return !(
            selectedName === normalizedName
            && selectedUnit === (unit || "count")
          );
        }),
      );
      setInventoryUpdateMessage(successMessage);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to update inventory right now.";
      setInventoryUpdateError(message);
    } finally {
      setIsSubmitting(false);
    }
  };

  const onPreviewReceipt = async () => {
    if (!receiptFile) {
      setReceiptError("Choose a receipt image before running preview.");
      setReceiptMessage(null);
      return;
    }

    setReceiptBusyMode("preview");
    setReceiptError(null);
    setReceiptMessage(null);
    try {
      const response = await ingestPantryReceipt(receiptFile, true);
      const detectedItems = response.status === "dry_run" ? response.detected_items : [];
      setReceiptDetectedItems(detectedItems);
      setReceiptMessage(
        detectedItems.length > 0
          ? `Detected ${detectedItems.length} inventory item(s).`
          : "No inventory items were detected.",
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to preview receipt right now.";
      setReceiptError(message);
      setReceiptDetectedItems([]);
    } finally {
      setReceiptBusyMode(null);
    }
  };

  const onApplyReceipt = async () => {
    if (!receiptFile) {
      setReceiptError("Choose a receipt image before applying updates.");
      setReceiptMessage(null);
      return;
    }

    setReceiptBusyMode("apply");
    setReceiptError(null);
    setReceiptMessage(null);
    try {
      const response = await ingestPantryReceipt(receiptFile, false);
      const appliedCount = response.status === "applied" ? response.applied.length : 0;
      setReceiptMessage(`Applied ${appliedCount} inventory update(s) from receipt.`);
      setReceiptDetectedItems([]);
      setReceiptFile(null);
      setReceiptInputKey((previous) => previous + 1);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unable to apply receipt right now.";
      setReceiptError(message);
    } finally {
      setReceiptBusyMode(null);
    }
  };

  return (
    <section className="screen-panel pantry-panel">
      <header className="screen-header">
        <div>
          <h2>Pantry</h2>
          <p>Inventory-driven meal ideas for the week.</p>
        </div>
        <SyncStatusPill status={runtimeState.sync_status} />
      </header>

      <div className="metric-grid pantry-summary-grid">
        <article className="metric-card pantry-summary-card">
          <h3>Items tracked</h3>
          <p>{totalItems}</p>
        </article>
        <article className="metric-card pantry-summary-card">
          <h3>Low stock</h3>
          <p>{pantry.low_stock_count}</p>
        </article>
        <article className="metric-card pantry-summary-card">
          <h3>Out of stock</h3>
          <p>{outOfStockCount}</p>
        </article>
        <article className="metric-card pantry-summary-card">
          <h3>Weekly suggestions</h3>
          <p>{pantry.weekly_recipe_suggestions.length}</p>
        </article>
      </div>

      <section className="pantry-section" aria-label="Pantry assistant shortcuts">
        <div className="dashboard-section-header">
          <h3>Assistant Shortcuts</h3>
        </div>
        <div className="dashboard-list-controls">
          <button
            type="button"
            className="dashboard-detail-button"
            onClick={() =>
              openAssistantWorkflow(
                `Build a smart restock plan for household ${runtimeState.snapshot.family.family_id}. I have ${pantry.low_stock_count} low-stock and ${outOfStockCount} out-of-stock pantry items. Prioritize what to buy this week and suggest substitutions.`,
              )
            }
          >
            Ask Assistant: Restock Plan
          </button>
          <button
            type="button"
            className="dashboard-detail-button"
            onClick={() =>
              openAssistantWorkflow(
                `Create a 7-day meal plan from my pantry with minimal waste. I currently track ${totalItems} pantry items and have ${pantry.weekly_recipe_suggestions.length} recipe suggestions. Avoid repeating the same meal ideas.`,
              )
            }
          >
            Ask Assistant: Meal Plan
          </button>
          <button
            type="button"
            className="dashboard-detail-button"
            disabled={selectedCount === 0}
            onClick={() => {
              const selectedNames = Array.from(new Set(selectedInventoryRows.map((item) => item.name))).join(", ");
              openAssistantWorkflow(
                `Help me clean up pantry inventory. I selected ${selectedCount} item(s): ${selectedNames}. Suggest what to remove, what to restock, and what meals can use what remains this week.`,
              );
            }}
          >
            Ask Assistant: Cleanup Selected ({selectedCount})
          </button>
        </div>
      </section>

      <section className="pantry-section" aria-label="Pantry inventory">
        <div className="dashboard-section-header">
          <h3>Inventory</h3>
          <button
            type="button"
            className="dashboard-detail-button pantry-inline-toggle"
            onClick={() => setShowDepletedItems((value) => !value)}
          >
            {showDepletedItems
              ? "Hide depleted items"
              : hiddenDepletedCount > 0
              ? `Show depleted (${hiddenDepletedCount})`
              : "Show depleted items"}
          </button>
        </div>
        <form className="pantry-adjust-form" onSubmit={(event) => void onAddInventoryItem(event)}>
          <label className="pantry-adjust-field" htmlFor="pantry-item-name">
            Item
            <input
              id="pantry-item-name"
              type="text"
              value={draftItemName}
              onChange={(event) => setDraftItemName(event.target.value)}
              placeholder="e.g. avocados"
              autoComplete="off"
            />
          </label>
          <label className="pantry-adjust-field pantry-adjust-field-small" htmlFor="pantry-item-quantity">
            Quantity
            <input
              id="pantry-item-quantity"
              type="number"
              min={0.01}
              step={0.01}
              value={draftQuantity}
              onChange={(event) => setDraftQuantity(event.target.value)}
            />
          </label>
          <label className="pantry-adjust-field pantry-adjust-field-small" htmlFor="pantry-item-unit">
            Unit
            <select
              id="pantry-item-unit"
              value={draftUnit}
              onChange={(event) => setDraftUnit(event.target.value)}
            >
              {INVENTORY_UNIT_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <button type="submit" className="dashboard-detail-button" disabled={isSubmitting}>
            {isSubmitting ? "Adding..." : "Add Item"}
          </button>
        </form>
        <div className="dashboard-list-controls">
          <button
            type="button"
            className="dashboard-detail-button"
            onClick={onToggleSelectAllVisible}
            disabled={visibleInventoryItems.length === 0 || isSubmitting}
          >
            {allVisibleSelected ? "Clear visible selection" : `Select visible (${visibleInventoryItems.length})`}
          </button>
          <button
            type="button"
            className="dashboard-detail-button"
            onClick={onClearSelection}
            disabled={selectedCount === 0 || isSubmitting}
          >
            Clear selection ({selectedCount})
          </button>
          <button
            type="button"
            className="dashboard-detail-button"
            onClick={() => void onBulkAdjustInventory("decrement")}
            disabled={selectedCount === 0 || isSubmitting}
          >
            -1 selected ({selectedCount})
          </button>
          <button
            type="button"
            className="dashboard-detail-button pantry-action-danger"
            onClick={() => void onBulkAdjustInventory("remove")}
            disabled={selectedCount === 0 || isSubmitting}
          >
            Delete selected ({selectedCount})
          </button>
        </div>
        {selectedCount > 0 ? (
          <p className="task-meta">
            Selected for bulk actions: {selectedInventorySummary}
            {selectedCount > 5 ? ` +${selectedCount - 5} more` : ""}
          </p>
        ) : null}
        {inventoryUpdateMessage ? <p className="task-meta pantry-adjust-success">{inventoryUpdateMessage}</p> : null}
        {inventoryUpdateError ? <p className="error-text">{inventoryUpdateError}</p> : null}
        {visibleInventoryItems.length === 0 ? (
          <p className="empty-text">No pantry items available yet.</p>
        ) : (
          <ul className="pantry-inventory-list">
            {visibleInventoryRows.map(({ item, selectionKey }) => (
              <li key={selectionKey} className="pantry-inventory-row">
                <div>
                  <label className="task-meta pantry-selection-toggle">
                    <input
                      type="checkbox"
                      checked={selectedSet.has(selectionKey)}
                      onChange={() => onToggleInventorySelection(selectionKey)}
                      disabled={isSubmitting}
                    />
                    Select
                  </label>
                  <p className="pantry-inventory-name">{item.name}</p>
                  <p className="task-meta">Quantity: {formatQuantity(item.quantity)} {prettyUnit(item.unit)}</p>
                </div>
                <div className="pantry-inventory-actions-wrap">
                  <span className={`level-pill ${statusClassName(item.status)}`}>
                    {statusLabel(item.status)}
                  </span>
                  <div className="pantry-inventory-actions">
                    <button
                      type="button"
                      className="pantry-action-button"
                      disabled={isSubmitting || item.quantity <= 0}
                      onClick={() => {
                        const step = Math.min(item.quantity, 1);
                        const quantityLabel = formatQuantity(step);
                        void onAdjustInventoryItem(
                          item.name,
                          -step,
                          item.unit,
                          "manual inventory decrement",
                          `Removed ${quantityLabel} ${prettyUnit(item.unit)} of ${item.name}.`,
                        );
                      }}
                    >
                      -1
                    </button>
                    <button
                      type="button"
                      className="pantry-action-button pantry-action-danger"
                      disabled={isSubmitting || item.quantity <= 0}
                      onClick={() => {
                        const quantityLabel = formatQuantity(item.quantity);
                        void onAdjustInventoryItem(
                          item.name,
                          -item.quantity,
                          item.unit,
                          "manual inventory remove",
                          `Removed ${quantityLabel} ${prettyUnit(item.unit)} of ${item.name}.`,
                        );
                      }}
                    >
                      Remove
                    </button>
                  </div>
                </div>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="pantry-section" aria-label="Receipt import">
        <div className="dashboard-section-header">
          <h3>Receipt Import</h3>
        </div>
        <p className="task-meta">Upload a receipt photo to detect and apply pantry inventory updates.</p>
        <div className="pantry-receipt-controls">
          <label className="pantry-adjust-field" htmlFor="pantry-receipt-upload">
            Receipt image
            <input
              key={receiptInputKey}
              id="pantry-receipt-upload"
              type="file"
              accept="image/*,.txt"
              onChange={(event) => {
                const nextFile = event.target.files && event.target.files.length > 0 ? event.target.files[0] : null;
                setReceiptFile(nextFile);
                setReceiptError(null);
                setReceiptMessage(null);
              }}
            />
          </label>
          <div className="pantry-receipt-buttons">
            <button
              type="button"
              className="dashboard-detail-button"
              disabled={!receiptFile || receiptBusyMode !== null}
              onClick={() => void onPreviewReceipt()}
            >
              {receiptBusyMode === "preview" ? "Previewing..." : "Preview Items"}
            </button>
            <button
              type="button"
              className="dashboard-detail-button"
              disabled={!receiptFile || receiptBusyMode !== null}
              onClick={() => void onApplyReceipt()}
            >
              {receiptBusyMode === "apply" ? "Applying..." : "Apply to Inventory"}
            </button>
          </div>
        </div>
        {receiptMessage ? <p className="task-meta pantry-adjust-success">{receiptMessage}</p> : null}
        {receiptError ? <p className="error-text">{receiptError}</p> : null}
        {receiptDetectedItems.length > 0 ? (
          <ul className="pantry-receipt-detected-list">
            {receiptDetectedItems.map((row) => (
              <li key={`${row.item}:${row.unit || "count"}`}>
                {row.item}: +{formatQuantity(row.delta)} {prettyUnit(row.unit || "count")}
              </li>
            ))}
          </ul>
        ) : null}
      </section>

      <section className="pantry-section" aria-label="Recipe suggestions">
        <div className="dashboard-section-header">
          <h3>Recipe Suggestions This Week</h3>
        </div>
        {pantry.weekly_recipe_suggestions.length === 0 ? (
          <p className="empty-text">Recipe suggestions will appear once inventory data is available.</p>
        ) : (
          <div className="pantry-suggestion-grid">
            {pantry.weekly_recipe_suggestions.map((suggestion) => {
              const recipeLink = resolveRecipeLink(
                suggestion.recipe_name,
                suggestion.recipe_url,
                suggestion.recipe_source,
              );

              return (
                <article
                  key={`${suggestion.date}:${suggestion.recipe_name}`}
                  className="pantry-suggestion-card"
                >
                  <div className="pantry-suggestion-header">
                    <p className="pantry-suggestion-day">{suggestion.day}</p>
                    <span className="pantry-suggestion-date">{suggestion.date}</span>
                  </div>
                  <h4>{suggestion.recipe_name}</h4>
                  <p className="task-meta">{prettyMealType(suggestion.meal_type)}</p>
                  <p className="task-meta">Servings: {suggestion.servings}</p>
                  {recipeLink ? (
                    <p className="task-meta">
                      Source: <a href={recipeLink.href} target="_blank" rel="noreferrer">{recipeLink.label}</a>
                    </p>
                  ) : suggestion.recipe_source ? (
                    <p className="task-meta">Source: {suggestion.recipe_source}</p>
                  ) : null}
                  <p className="task-meta">Inventory match: {suggestion.inventory_match_score.toFixed(1)}%</p>
                  {suggestion.ingredient_requirements.length > 0 ? (
                    <div>
                      <p className="task-meta">Need:</p>
                      <ul className="pantry-recipe-requirements">
                        {suggestion.ingredient_requirements.map((requirement) => (
                          <li key={`${suggestion.recipe_name}:${suggestion.date}:${requirement.item}`}>
                            {requirement.item}: {formatQuantity(requirement.quantity)} {prettyUnit(requirement.unit)}
                          </li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                  <p className="task-meta">
                    Uses: {suggestion.ingredients_used.length > 0 ? suggestion.ingredients_used.join(", ") : "None"}
                  </p>
                  <p className="task-meta">
                    Missing: {suggestion.missing_ingredients.length > 0 ? suggestion.missing_ingredients.join(", ") : "Nothing"}
                  </p>
                </article>
              );
            })}
          </div>
        )}
      </section>

      <section className="pantry-section" aria-label="Grocery recommendations">
        <div className="dashboard-section-header">
          <h3>Grocery Recommendations</h3>
        </div>
        {pantry.grocery_recommendations.length === 0 ? (
          <p className="empty-text">No immediate grocery additions needed.</p>
        ) : (
          <div className="pantry-chip-row">
            {pantry.grocery_recommendations.map((ingredient) => (
              <span key={ingredient} className="pantry-chip">
                {ingredient}
              </span>
            ))}
          </div>
        )}
      </section>
    </section>
  );
};
