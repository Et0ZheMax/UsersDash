"""
Вспомогательные функции для работы с тарифами ферм.
"""
from __future__ import annotations

from typing import Iterable

from UsersDash.models import Account

# Тарифы, подтягиваемые по фиксированной цене
TARIFF_PRICE_MAP = {
    500: "Только Фарм",
    1000: "Расширенный",
    1400: "Премиум",
}


def get_tariff_name_by_price(price: int | None) -> str | None:
    """Возвращает название тарифа по его цене."""
    if price is None:
        return None
    return TARIFF_PRICE_MAP.get(price)


def summarize_tariffs(accounts: Iterable[Account]) -> tuple[list[dict], int]:
    """
    Возвращает список тарифов с количеством ферм на каждом из них и общее число
    назначенных тарифов.
    """
    counts: dict[str, int] = {}
    for acc in accounts:
        tariff_name = get_tariff_name_by_price(getattr(acc, "next_payment_amount", None))
        if not tariff_name:
            continue
        counts[tariff_name] = counts.get(tariff_name, 0) + 1

    summary: list[dict] = []
    total = 0
    for price, name in sorted(TARIFF_PRICE_MAP.items()):
        count = counts.get(name, 0)
        if count:
            summary.append({"name": name, "count": count, "price": price})
            total += count

    return summary, total
