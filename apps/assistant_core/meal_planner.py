from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from apps.assistant_core.contracts import MealSuggestion


REFERENCE_DATE = date(2026, 4, 19)


@dataclass(frozen=True)
class Recipe:
    name: str
    meal_type: str
    ingredients: tuple[str, ...]
    nutrition_balance: tuple[str, ...]


RECIPES: tuple[Recipe, ...] = (
    Recipe(
        name="Salmon Rice Plate",
        meal_type="dinner",
        ingredients=("salmon", "brown rice", "broccoli", "olive oil"),
        nutrition_balance=("protein", "vegetable", "complex_carb"),
    ),
    Recipe(
        name="Chicken Quinoa Bowl",
        meal_type="dinner",
        ingredients=("chicken", "quinoa", "spinach", "bell pepper"),
        nutrition_balance=("protein", "vegetable", "complex_carb"),
    ),
    Recipe(
        name="Black Bean Taco Night",
        meal_type="dinner",
        ingredients=("black beans", "tortillas", "spinach", "avocado"),
        nutrition_balance=("protein", "vegetable", "healthy_fat"),
    ),
    Recipe(
        name="Egg and Sweet Potato Skillet",
        meal_type="breakfast",
        ingredients=("eggs", "sweet potato", "spinach"),
        nutrition_balance=("protein", "vegetable", "complex_carb"),
    ),
)


def default_inventory() -> dict[str, int]:
    return {
        "salmon": 1,
        "brown rice": 2,
        "broccoli": 1,
        "olive oil": 1,
        "chicken": 2,
        "quinoa": 1,
        "spinach": 2,
        "black beans": 2,
        "tortillas": 1,
        "eggs": 8,
        "sweet potato": 3,
    }


def default_recipe_history() -> list[dict[str, str]]:
    return [
        {"recipe_name": "Salmon Rice Plate", "served_on": "2026-04-12"},
        {"recipe_name": "Egg and Sweet Potato Skillet", "served_on": "2026-04-16"},
        {"recipe_name": "Chicken Quinoa Bowl", "served_on": "2026-04-08"},
    ]


def _recent_recipe_names(recipe_history: list[dict[str, str]], repeat_window_days: int) -> set[str]:
    cutoff = REFERENCE_DATE - timedelta(days=repeat_window_days)
    recent: set[str] = set()
    for row in recipe_history:
        served_on = row.get("served_on", "")
        try:
            served_date = date.fromisoformat(served_on)
        except ValueError:
            continue
        if served_date >= cutoff:
            recent.add(str(row.get("recipe_name", "")))
    return recent


def _score_recipe(recipe: Recipe, inventory: dict[str, int]) -> tuple[int, str]:
    missing = sum(1 for ingredient in recipe.ingredients if inventory.get(ingredient, 0) <= 0)
    in_stock = sum(1 for ingredient in recipe.ingredients if inventory.get(ingredient, 0) > 0)
    balance = len(recipe.nutrition_balance)
    return (-missing, in_stock + balance, recipe.name)


def plan_meal(
    *,
    inventory: dict[str, int] | None = None,
    recipe_history: list[dict[str, str]] | None = None,
    repeat_window_days: int = 10,
) -> MealSuggestion:
    current_inventory = dict(inventory or default_inventory())
    history = list(recipe_history or default_recipe_history())
    recent_recipes = _recent_recipe_names(history, repeat_window_days)

    eligible = [recipe for recipe in RECIPES if recipe.name not in recent_recipes]
    if not eligible:
        eligible = list(RECIPES)

    selected = sorted(eligible, key=lambda recipe: _score_recipe(recipe, current_inventory), reverse=True)[0]
    grocery_additions = [ingredient for ingredient in selected.ingredients if current_inventory.get(ingredient, 0) <= 0]
    ingredients_used = [ingredient for ingredient in selected.ingredients if current_inventory.get(ingredient, 0) > 0]

    return MealSuggestion(
        recipe_name=selected.name,
        meal_type=selected.meal_type,
        ingredients_used=ingredients_used,
        grocery_additions=grocery_additions,
        nutrition_balance=list(selected.nutrition_balance),
        repeat_window_days=repeat_window_days,
    )