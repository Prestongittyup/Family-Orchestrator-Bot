from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from archive.apps.assistant_core.contracts import MealSuggestion


REFERENCE_DATE = date(2026, 4, 19)


@dataclass(frozen=True)
class IngredientRequirement:
    item: str
    amount_per_serving: float
    unit: str = "count"


@dataclass(frozen=True)
class Recipe:
    name: str
    meal_type: str
    ingredient_requirements: tuple[IngredientRequirement, ...]
    nutrition_balance: tuple[str, ...]
    source_name: str
    source_url: str

    @property
    def ingredients(self) -> tuple[str, ...]:
        return tuple(requirement.item for requirement in self.ingredient_requirements)


def _req(item: str, amount_per_serving: float, unit: str = "count") -> IngredientRequirement:
    return IngredientRequirement(item=item, amount_per_serving=amount_per_serving, unit=unit)


RECIPES: tuple[Recipe, ...] = (
    Recipe(
        name="Salmon Rice Plate",
        meal_type="dinner",
        ingredient_requirements=(
            _req("salmon", 0.25, "lb"),
            _req("brown rice", 2.5, "oz"),
            _req("broccoli", 0.5),
            _req("olive oil", 0.2, "fl_oz"),
        ),
        nutrition_balance=("protein", "vegetable", "complex_carb"),
        source_name="Allrecipes",
        source_url="https://www.allrecipes.com/recipe/228319/miso-glazed-salmon/",
    ),
    Recipe(
        name="Chicken Quinoa Bowl",
        meal_type="dinner",
        ingredient_requirements=(
            _req("chicken", 0.25, "lb"),
            _req("quinoa", 2.0, "oz"),
            _req("spinach", 0.75),
            _req("bell pepper", 0.3),
        ),
        nutrition_balance=("protein", "vegetable", "complex_carb"),
        source_name="EatingWell",
        source_url="https://www.eatingwell.com/recipe/269820/chicken-quinoa-bowl/",
    ),
    Recipe(
        name="Black Bean Taco Night",
        meal_type="dinner",
        ingredient_requirements=(
            _req("black beans", 0.5, "can"),
            _req("tortillas", 2),
            _req("spinach", 0.5),
            _req("avocado", 0.5),
        ),
        nutrition_balance=("protein", "vegetable", "healthy_fat"),
        source_name="Budget Bytes",
        source_url="https://www.budgetbytes.com/weeknight-black-bean-tacos/",
    ),
    Recipe(
        name="Egg and Sweet Potato Skillet",
        meal_type="breakfast",
        ingredient_requirements=(
            _req("eggs", 2),
            _req("sweet potato", 0.5),
            _req("spinach", 0.5),
            _req("olive oil", 0.1, "fl_oz"),
        ),
        nutrition_balance=("protein", "vegetable", "complex_carb"),
        source_name="Ambitious Kitchen",
        source_url="https://www.ambitiouskitchen.com/sweet-potato-hash-brown-egg-cups/",
    ),
    Recipe(
        name="Lentil Veggie Soup",
        meal_type="dinner",
        ingredient_requirements=(
            _req("lentils", 2.0, "oz"),
            _req("carrots", 0.5),
            _req("onion", 0.25),
            _req("diced tomatoes", 0.5, "can"),
            _req("garlic", 0.25),
        ),
        nutrition_balance=("protein", "vegetable", "fiber"),
        source_name="Cookie and Kate",
        source_url="https://cookieandkate.com/best-lentil-soup-recipe/",
    ),
    Recipe(
        name="Turkey Pasta Primavera",
        meal_type="dinner",
        ingredient_requirements=(
            _req("turkey", 0.25, "lb"),
            _req("whole wheat pasta", 2.5, "oz"),
            _req("zucchini", 0.25),
            _req("bell pepper", 0.25),
            _req("olive oil", 0.15, "fl_oz"),
        ),
        nutrition_balance=("protein", "vegetable", "complex_carb"),
        source_name="Food Network",
        source_url="https://www.foodnetwork.com/recipes/food-network-kitchen/turkey-primavera-3364396",
    ),
    Recipe(
        name="Tofu Stir-Fry Noodles",
        meal_type="dinner",
        ingredient_requirements=(
            _req("tofu", 0.2, "lb"),
            _req("rice noodles", 2.5, "oz"),
            _req("broccoli", 0.4),
            _req("carrots", 0.4),
            _req("soy sauce", 0.2, "fl_oz"),
        ),
        nutrition_balance=("protein", "vegetable", "complex_carb"),
        source_name="Minimalist Baker",
        source_url="https://minimalistbaker.com/tofu-noodle-stir-fry-with-spring-vegetables/",
    ),
    Recipe(
        name="Chickpea Spinach Curry",
        meal_type="dinner",
        ingredient_requirements=(
            _req("chickpeas", 0.5, "can"),
            _req("spinach", 0.75),
            _req("coconut milk", 0.3, "can"),
            _req("onion", 0.25),
            _req("curry paste", 0.2, "oz"),
        ),
        nutrition_balance=("protein", "vegetable", "healthy_fat"),
        source_name="BBC Good Food",
        source_url="https://www.bbcgoodfood.com/recipes/chickpea-spinach-curry",
    ),
    Recipe(
        name="Shrimp Fried Rice",
        meal_type="dinner",
        ingredient_requirements=(
            _req("shrimp", 0.2, "lb"),
            _req("brown rice", 2.0, "oz"),
            _req("eggs", 1),
            _req("peas", 0.3),
            _req("carrots", 0.3),
        ),
        nutrition_balance=("protein", "vegetable", "complex_carb"),
        source_name="Serious Eats",
        source_url="https://www.seriouseats.com/easy-shrimp-fried-rice-recipe",
    ),
    Recipe(
        name="Greek Yogurt Berry Oats",
        meal_type="breakfast",
        ingredient_requirements=(
            _req("greek yogurt", 0.5, "pack"),
            _req("oats", 1.5, "oz"),
            _req("berries", 0.5, "pack"),
            _req("chia seeds", 0.2, "oz"),
        ),
        nutrition_balance=("protein", "fiber", "healthy_fat"),
        source_name="EatingWell",
        source_url="https://www.eatingwell.com/recipe/266495/strawberry-blueberry-overnight-oats/",
    ),
    Recipe(
        name="Sheet Pan Sausage Vegetables",
        meal_type="dinner",
        ingredient_requirements=(
            _req("chicken sausage", 0.3, "lb"),
            _req("potatoes", 0.5),
            _req("broccoli", 0.5),
            _req("olive oil", 0.15, "fl_oz"),
        ),
        nutrition_balance=("protein", "vegetable", "complex_carb"),
        source_name="Simply Recipes",
        source_url="https://www.simplyrecipes.com/sheet-pan-sausage-and-vegetables-recipe-5221373/",
    ),
    Recipe(
        name="Veggie Quesadilla Plate",
        meal_type="dinner",
        ingredient_requirements=(
            _req("tortillas", 2),
            _req("black beans", 0.4, "can"),
            _req("bell pepper", 0.3),
            _req("cheddar cheese", 1.0, "oz"),
            _req("spinach", 0.4),
        ),
        nutrition_balance=("protein", "vegetable", "healthy_fat"),
        source_name="Love and Lemons",
        source_url="https://www.loveandlemons.com/quesadilla-recipe/",
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
        "avocado": 2,
        "eggs": 8,
        "sweet potato": 3,
        "olive oil": 2,
        "lentils": 1,
        "carrots": 4,
        "onion": 2,
        "diced tomatoes": 2,
        "garlic": 2,
        "turkey": 1,
        "whole wheat pasta": 1,
        "zucchini": 2,
        "tofu": 1,
        "rice noodles": 1,
        "soy sauce": 1,
        "chickpeas": 2,
        "coconut milk": 1,
        "curry paste": 1,
        "shrimp": 1,
        "peas": 1,
        "greek yogurt": 2,
        "oats": 1,
        "berries": 2,
        "chia seeds": 1,
        "chicken sausage": 1,
        "potatoes": 4,
        "cheddar cheese": 1,
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