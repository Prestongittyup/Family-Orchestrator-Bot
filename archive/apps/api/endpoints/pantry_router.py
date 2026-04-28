from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from io import BytesIO
import re
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, ConfigDict, Field

from archive.apps.api.hpal.orchestration_adapter import OrchestrationAdapter
from archive.apps.assistant_core.meal_planner import RECIPES


router = APIRouter(prefix="/v1/pantry", tags=["pantry"])
_adapter = OrchestrationAdapter()


class InventoryDelta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item: str = Field(..., min_length=1)
    delta: float
    unit: str | None = None


class PantryAdjustRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    updates: list[InventoryDelta] = Field(default_factory=list)
    note: str | None = None


class CookRecipeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recipe_name: str = Field(..., min_length=1)
    servings: int = Field(default=1, ge=1, le=12)
    consumed_at: str | None = None


_RECIPE_LOOKUP = {recipe.name.lower(): recipe for recipe in RECIPES}
_KNOWN_INGREDIENTS = {ingredient.lower() for recipe in RECIPES for ingredient in recipe.ingredients}
_RECEIPT_STOP_WORDS = {
    "total",
    "subtotal",
    "tax",
    "change",
    "cash",
    "credit",
    "debit",
    "visa",
    "mastercard",
    "balance",
}
_RECEIPT_ALIASES = {
    "brown rice": "brown rice",
    "rice": "brown rice",
    "sweet potatoes": "sweet potato",
    "sweet potato": "sweet potato",
    "egg": "eggs",
    "eggs": "eggs",
}
_QTY_PREFIX_PATTERN = re.compile(r"^(?P<qty>\d{1,3})\s*(?:x|X)?\s+(?P<name>[A-Za-z][A-Za-z0-9'\-\s]{1,80})$")
_QTY_SUFFIX_PATTERN = re.compile(r"^(?P<name>[A-Za-z][A-Za-z0-9'\-\s]{1,80})\s+(?:x|X)\s*(?P<qty>\d{1,3})$")
_QTY_TRAILING_PATTERN = re.compile(r"^(?P<name>[A-Za-z][A-Za-z0-9'\-\s]{1,80})\s+(?P<qty>\d{1,3})$")

_DEFAULT_INVENTORY_UNIT = "count"
_ALLOWED_INVENTORY_UNITS = {
    "count",
    "can",
    "pack",
    "oz",
    "lb",
    "g",
    "kg",
    "ml",
    "fl_oz",
    "l",
}
_INVENTORY_UNIT_ALIASES = {
    "": _DEFAULT_INVENTORY_UNIT,
    "ea": "count",
    "each": "count",
    "piece": "count",
    "pieces": "count",
    "pcs": "count",
    "cans": "can",
    "packs": "pack",
    "floz": "fl_oz",
    "fl oz": "fl_oz",
    "fluid ounce": "fl_oz",
    "fluid ounces": "fl_oz",
    "liter": "l",
    "liters": "l",
    "litre": "l",
    "litres": "l",
}

InventoryEntry = dict[str, float | str]


@router.post("/{family_id}/adjust")
def adjust_inventory(family_id: str, request: PantryAdjustRequest) -> dict[str, Any]:
    if not request.updates:
        raise HTTPException(status_code=400, detail="updates_required")

    try:
        graph = _load_graph(family_id)
        inventory = _inventory_from_graph(graph)
        applied = _apply_inventory_deltas(inventory, request.updates)
        _write_inventory(graph, inventory)
        _append_event(
            graph,
            event_type="pantry_adjusted",
            payload={
                "changes": applied,
                "note": request.note,
            },
        )
        persisted = _persist_graph(family_id, graph)
        return {
            "status": "updated",
            "family_id": family_id,
            "applied": applied,
            "inventory": _inventory_quantity_snapshot(_inventory_from_graph(persisted)),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"pantry_adjust_failed: {exc}")


@router.post("/{family_id}/cook")
def cook_recipe(family_id: str, request: CookRecipeRequest) -> dict[str, Any]:
    recipe = _RECIPE_LOOKUP.get(request.recipe_name.strip().lower())
    if recipe is None:
        raise HTTPException(status_code=404, detail="recipe_not_found")

    try:
        graph = _load_graph(family_id)
        inventory = _inventory_from_graph(graph)

        deltas = [
            InventoryDelta(item=ingredient, delta=-request.servings)
            for ingredient in recipe.ingredients
        ]
        applied = _apply_inventory_deltas(inventory, deltas)
        _write_inventory(graph, inventory)

        meal_history = graph.get("meal_history")
        if not isinstance(meal_history, list):
            meal_history = []
            graph["meal_history"] = meal_history
        meal_history.append(
            {
                "recipe_name": recipe.name,
                "served_on": _coerce_served_on(request.consumed_at),
            }
        )

        _append_event(
            graph,
            event_type="pantry_recipe_cooked",
            payload={
                "recipe_name": recipe.name,
                "servings": request.servings,
                "changes": applied,
            },
        )
        persisted = _persist_graph(family_id, graph)
        return {
            "status": "updated",
            "family_id": family_id,
            "recipe_name": recipe.name,
            "servings": request.servings,
            "applied": applied,
            "inventory": _inventory_quantity_snapshot(_inventory_from_graph(persisted)),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"pantry_cook_failed: {exc}")


@router.post("/{family_id}/ingest-receipt")
async def ingest_receipt(
    family_id: str,
    file: UploadFile = File(...),
    dry_run: bool = Form(False),
) -> dict[str, Any]:
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="empty_receipt_file")

    try:
        text = _extract_receipt_text(
            content_type=file.content_type,
            filename=file.filename,
            raw_bytes=raw,
        )
        updates = _parse_receipt_inventory_updates(text)
    except RuntimeError as exc:
        if str(exc) == "ocr_dependencies_missing":
            raise HTTPException(
                status_code=503,
                detail=(
                    "ocr_dependencies_missing: install Pillow + pytesseract and ensure "
                    "Tesseract OCR runtime is available"
                ),
            )
        raise HTTPException(status_code=400, detail=str(exc))

    if not updates:
        raise HTTPException(status_code=422, detail="no_inventory_items_detected")

    if dry_run:
        return {
            "status": "dry_run",
            "family_id": family_id,
            "detected_items": [update.model_dump() for update in updates],
        }

    try:
        graph = _load_graph(family_id)
        inventory = _inventory_from_graph(graph)
        applied = _apply_inventory_deltas(inventory, updates)
        _write_inventory(graph, inventory)
        _append_event(
            graph,
            event_type="pantry_receipt_ingested",
            payload={
                "filename": file.filename,
                "changes": applied,
            },
        )
        persisted = _persist_graph(family_id, graph)
        return {
            "status": "applied",
            "family_id": family_id,
            "applied": applied,
            "inventory": _inventory_quantity_snapshot(_inventory_from_graph(persisted)),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"pantry_receipt_ingest_failed: {exc}")


def _load_graph(family_id: str) -> dict[str, Any]:
    if not family_id.strip():
        raise ValueError("family_id is required")
    graph = _adapter.load_graph(family_id)
    if not isinstance(graph, dict):
        raise ValueError("state graph is unavailable")
    return graph


def _persist_graph(family_id: str, graph: dict[str, Any]) -> dict[str, Any]:
    _refresh_projection_watermark(graph)
    expected_version = int(graph.get("state_version", 0))
    return _adapter.save_hpal_state(
        family_id=family_id,
        graph=graph,
        expected_state_version=expected_version,
    )


def _refresh_projection_watermark(graph: dict[str, Any]) -> None:
    hpal = graph.get("hpal")
    if not isinstance(hpal, dict):
        hpal = {}
        graph["hpal"] = hpal

    watermark = hpal.get("projection_watermark")
    if not isinstance(watermark, dict):
        watermark = {}
        hpal["projection_watermark"] = watermark

    transition_count = len(graph.get("action_lifecycle", {}).get("transition_log", []))
    event_count = len(graph.get("event_history", []))
    snapshot_hash = _hash_payload(
        {
            "plans": hpal.get("plans", {}),
            "tasks": hpal.get("tasks", []),
            "events": hpal.get("events", {}),
            "transition_count": transition_count,
            "event_count": event_count,
        }
    )

    watermark["projection_epoch"] = int(watermark.get("projection_epoch", 0)) + 1
    watermark["transition_count"] = transition_count
    watermark["event_count"] = event_count
    watermark["source_state_version"] = int(graph.get("state_version", 0))
    watermark["snapshot_hash"] = snapshot_hash
    watermark["last_projection_at"] = str(graph.get("updated_at", _utc_now_iso()))


def _hash_payload(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _inventory_from_graph(graph: dict[str, Any]) -> dict[str, InventoryEntry]:
    raw_inventory = graph.get("inventory")
    if not isinstance(raw_inventory, dict):
        raw_inventory = graph.get("grocery_inventory")
    if not isinstance(raw_inventory, dict):
        return {}

    normalized: dict[str, InventoryEntry] = {}
    for item_name, raw_entry in raw_inventory.items():
        normalized_name = _normalize_item_name(str(item_name))
        if not normalized_name:
            continue

        quantity_value: float | None
        unit_value: str
        if isinstance(raw_entry, dict):
            quantity_value = _coerce_quantity(raw_entry.get("quantity"))
            try:
                unit_value = _normalize_unit(raw_entry.get("unit"))
            except ValueError:
                unit_value = _DEFAULT_INVENTORY_UNIT
        else:
            quantity_value = _coerce_quantity(raw_entry)
            unit_value = _DEFAULT_INVENTORY_UNIT

        if quantity_value is None:
            continue
        normalized[normalized_name] = {
            "quantity": max(0.0, float(quantity_value)),
            "unit": unit_value,
        }
    return normalized


def _normalize_item_name(item_name: str) -> str:
    collapsed = re.sub(r"\s+", " ", item_name.strip().lower())
    return collapsed


def _write_inventory(graph: dict[str, Any], inventory: dict[str, InventoryEntry]) -> None:
    canonical_inventory = {
        item_name: {
            "quantity": _format_quantity(float(entry.get("quantity", 0.0))),
            "unit": _normalize_unit(entry.get("unit")),
        }
        for item_name, entry in sorted(inventory.items(), key=lambda row: row[0])
    }
    graph["inventory"] = canonical_inventory
    graph["grocery_inventory"] = canonical_inventory


def _inventory_quantity_snapshot(inventory: dict[str, InventoryEntry]) -> dict[str, float | int]:
    return {
        item_name: _format_quantity(float(entry.get("quantity", 0.0)))
        for item_name, entry in sorted(inventory.items(), key=lambda row: row[0])
    }


def _apply_inventory_deltas(
    inventory: dict[str, InventoryEntry],
    deltas: list[InventoryDelta],
) -> list[dict[str, float | int | str]]:
    applied: list[dict[str, float | int | str]] = []
    for delta in deltas:
        item = _normalize_item_name(delta.item)
        if not item:
            continue

        current = inventory.get(item, {"quantity": 0.0, "unit": _DEFAULT_INVENTORY_UNIT})
        before = float(current.get("quantity", 0.0))
        try:
            existing_unit = _normalize_unit(current.get("unit"))
        except ValueError:
            existing_unit = _DEFAULT_INVENTORY_UNIT
        requested_unit = _normalize_unit(delta.unit if delta.unit is not None else existing_unit)
        delta_value = float(delta.delta)
        after = max(0.0, before + delta_value)

        inventory[item] = {
            "quantity": after,
            "unit": requested_unit,
        }
        applied.append(
            {
                "item": item,
                "before": _format_quantity(before),
                "delta": _format_quantity(delta_value),
                "after": _format_quantity(after),
                "unit": requested_unit,
            }
        )
    if not applied:
        raise ValueError("no_valid_inventory_updates")
    return applied


def _coerce_quantity(raw_value: Any) -> float | None:
    try:
        quantity = float(raw_value)
    except (TypeError, ValueError):
        return None

    if quantity != quantity:  # NaN guard
        return None
    if quantity == float("inf") or quantity == float("-inf"):
        return None
    return quantity


def _normalize_unit(raw_value: Any) -> str:
    normalized = str(raw_value or "").strip().lower().replace("-", "_")
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = _INVENTORY_UNIT_ALIASES.get(normalized, normalized.replace(" ", "_"))

    if not normalized:
        normalized = _DEFAULT_INVENTORY_UNIT

    if normalized not in _ALLOWED_INVENTORY_UNITS:
        raise ValueError(f"invalid_inventory_unit:{raw_value}")

    return normalized


def _format_quantity(value: float) -> float | int:
    rounded = round(float(value), 3)
    if abs(rounded - round(rounded)) < 0.001:
        return int(round(rounded))
    return rounded


def _append_event(graph: dict[str, Any], *, event_type: str, payload: dict[str, Any]) -> None:
    event_history = graph.get("event_history")
    if not isinstance(event_history, list):
        event_history = []
        graph["event_history"] = event_history

    event_history.append(
        {
            "event_type": event_type,
            "recorded_at": _utc_now_iso(),
            **payload,
        }
    )


def _coerce_served_on(raw_value: str | None) -> str:
    if not raw_value:
        return datetime.now(timezone.utc).date().isoformat()

    candidate = raw_value.strip()
    if not candidate:
        return datetime.now(timezone.utc).date().isoformat()

    try:
        parsed_datetime = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        return parsed_datetime.date().isoformat()
    except ValueError:
        pass

    try:
        parsed_date = date.fromisoformat(candidate)
        return parsed_date.isoformat()
    except ValueError:
        return datetime.now(timezone.utc).date().isoformat()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _extract_receipt_text(*, content_type: str | None, filename: str | None, raw_bytes: bytes) -> str:
    detected_type = (content_type or "").lower()
    name = (filename or "").lower()

    if detected_type.startswith("text/") or name.endswith(".txt"):
        return raw_bytes.decode("utf-8", errors="ignore")

    image_extensions = (".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff")
    if detected_type.startswith("image/") or name.endswith(image_extensions):
        return _extract_text_from_image(raw_bytes)

    raise RuntimeError("unsupported_receipt_format")


def _extract_text_from_image(raw_bytes: bytes) -> str:
    try:
        from PIL import Image
        import pytesseract
    except Exception as exc:  # pragma: no cover - optional dependency path
        raise RuntimeError("ocr_dependencies_missing") from exc

    with Image.open(BytesIO(raw_bytes)) as image:
        return str(pytesseract.image_to_string(image))


def _parse_receipt_inventory_updates(text: str) -> list[InventoryDelta]:
    changes: dict[str, int] = {}

    for raw_line in text.splitlines():
        parsed = _parse_receipt_line(raw_line)
        if parsed is None:
            continue
        item_name, quantity = parsed
        changes[item_name] = changes.get(item_name, 0) + quantity

    return [
        InventoryDelta(item=item, delta=delta)
        for item, delta in sorted(changes.items(), key=lambda row: row[0])
        if delta > 0
    ]


def _parse_receipt_line(raw_line: str) -> tuple[str, int] | None:
    line = raw_line.strip()
    if not line:
        return None

    line = re.sub(r"\s+\$?\d+\.\d{2}\s*$", "", line)
    line = re.sub(r"\s+", " ", line).strip()
    if not line:
        return None

    quantity: int | None = None
    item_name: str | None = None

    for pattern in (_QTY_PREFIX_PATTERN, _QTY_SUFFIX_PATTERN, _QTY_TRAILING_PATTERN):
        match = pattern.match(line)
        if match is None:
            continue
        quantity = int(match.group("qty"))
        item_name = match.group("name")
        break

    if quantity is None or item_name is None:
        return None
    if quantity <= 0 or quantity > 100:
        return None

    canonical_name = _canonicalize_receipt_item(item_name)
    if not canonical_name or canonical_name in _RECEIPT_STOP_WORDS:
        return None

    return canonical_name, quantity


def _canonicalize_receipt_item(item_name: str) -> str:
    candidate = _normalize_item_name(re.sub(r"[^A-Za-z0-9'\-\s]", " ", item_name))
    candidate = re.sub(r"\s+", " ", candidate).strip()
    if not candidate:
        return ""

    alias = _RECEIPT_ALIASES.get(candidate)
    if alias:
        return alias

    if candidate in _KNOWN_INGREDIENTS:
        return candidate

    if candidate.endswith("s"):
        singular = candidate[:-1]
        alias = _RECEIPT_ALIASES.get(singular)
        if alias:
            return alias
        if singular in _KNOWN_INGREDIENTS:
            return singular

    return candidate
