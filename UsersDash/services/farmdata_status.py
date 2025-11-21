"""Helpers for checking client's farm data completeness."""
from __future__ import annotations

from typing import Dict, List

from UsersDash.models import Account, FarmData


def collect_farmdata_status(user_id: int) -> Dict[str, object]:
    """Return info about missing farm data for the given user."""

    required_fields = (
        "email",
        "password",
        "igg_id",
        "server",
        "telegram_tag",
    )
    field_titles = {
        "email": "E-mail",
        "password": "Пароль",
        "igg_id": "IGG ID",
        "server": "Сервер",
        "telegram_tag": "Telegram",
    }

    accounts: List[Account] = (
        Account.query.filter_by(owner_id=user_id, is_active=True)
        .order_by(Account.name.asc())
        .all()
    )

    if not accounts:
        return {"has_issues": False, "missing_accounts": [], "missing_details": []}

    entries: List[FarmData] = FarmData.query.filter_by(user_id=user_id).all()
    fd_by_name = {entry.farm_name: entry for entry in entries}

    missing_accounts: List[str] = []
    missing_details: List[Dict[str, object]] = []
    for acc in accounts:
        fd = fd_by_name.get(acc.name)
        if not fd:
            missing_accounts.append(acc.name)
            missing_details.append(
                {
                    "farm_name": acc.name,
                    "missing_fields": [field_titles[f] for f in required_fields],
                }
            )
            continue

        missing_fields = [
            field_titles[field]
            for field in required_fields
            if not getattr(fd, field)
        ]

        if missing_fields:
            missing_accounts.append(acc.name)
            missing_details.append({
                "farm_name": acc.name,
                "missing_fields": missing_fields,
            })

    return {
        "has_issues": bool(missing_accounts),
        "missing_accounts": missing_accounts,
        "missing_details": missing_details,
    }

