"""
Вспомогательные функции для работы с тарифами ферм.
"""
from __future__ import annotations

from typing import Iterable

from UsersDash.models import Account

# Конфигурация тарифов и их биллинговый статус
TARIFFS: dict[int, dict[str, object]] = {
    0: {"name": "Своя ферма", "billable": False},
    500: {"name": "Только Фарм", "billable": True},
    1000: {"name": "Расширенный", "billable": True},
    1400: {"name": "Премиум", "billable": True},
}

# Упрощённая карта для шаблонов, где нужен только маппинг цена -> название
TARIFF_PRICE_MAP: dict[int, str] = {price: info["name"] for price, info in TARIFFS.items()}


def _normalize_price(price: int | str | None) -> int | None:
    """Пытается привести цену тарифа к int для последующей проверки."""

    if price is None:
        return None

    if isinstance(price, int):
        return price

    if isinstance(price, str) and price.isdigit():
        return int(price)

    return None


def get_tariff_name_by_price(price: int | str | None) -> str | None:
    """Возвращает название тарифа по его цене."""

    normalized_price = _normalize_price(price)
    if normalized_price is None:
        return None

    tariff = TARIFFS.get(normalized_price)
    return tariff.get("name") if tariff else None


def is_tariff_billable(price: int | str | None) -> bool:
    """Возвращает True, если тариф учитывается в денежных метриках."""

    normalized_price = _normalize_price(price)
    if normalized_price is None:
        return False

    tariff = TARIFFS.get(normalized_price)
    if tariff is None:
        return True

    return bool(tariff.get("billable", True))


def summarize_tariffs(
    accounts: Iterable[Account], *, include_non_billable: bool = True
) -> tuple[list[dict], int]:
    """
    Возвращает список тарифов с количеством ферм на каждом из них и общее число
    назначенных тарифов.
    """
    counts: dict[str, int] = {}
    for acc in accounts:
        price = getattr(acc, "next_payment_amount", None)
        tariff_name = get_tariff_name_by_price(price)
        if not tariff_name:
            continue

        if not include_non_billable and not is_tariff_billable(price):
            continue
        counts[tariff_name] = counts.get(tariff_name, 0) + 1

    summary: list[dict] = []
    total = 0
    for price, info in sorted(TARIFFS.items()):
        if not include_non_billable and not bool(info.get("billable", True)):
            continue

        name = info["name"]
        count = counts.get(name, 0)
        if count:
            summary.append({"name": name, "count": count, "price": price})
            total += count

    return summary, total


def sum_billable_tariffs(accounts: Iterable[Account]) -> int:
    """Возвращает сумму тарифов, исключая небиллябельные варианты."""

    total = 0
    for acc in accounts:
        amount = getattr(acc, "next_payment_amount", None)
        if amount is None or not is_tariff_billable(amount):
            continue

        normalized_amount = _normalize_price(amount)
        if normalized_amount is not None:
            total += normalized_amount

    return total
