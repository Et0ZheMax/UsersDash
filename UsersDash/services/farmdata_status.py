"""Helpers for checking client's farm data completeness."""
from __future__ import annotations

from typing import Dict, List

from UsersDash.models import Account, FarmData


def collect_farmdata_status(user_id: int) -> Dict[str, object]:
    """Return info about missing farm data for the given user."""
    accounts: List[Account] = (
        Account.query.filter_by(owner_id=user_id, is_active=True)
        .order_by(Account.name.asc())
        .all()
    )

    if not accounts:
        return {"has_issues": False, "missing_accounts": []}

    entries: List[FarmData] = FarmData.query.filter_by(user_id=user_id).all()
    fd_by_name = {entry.farm_name: entry for entry in entries}

    missing_accounts: List[str] = []
    for acc in accounts:
        fd = fd_by_name.get(acc.name)
        if not fd:
            missing_accounts.append(acc.name)
            continue

        has_any_value = any([
            fd.email,
            fd.password,
            fd.igg_id,
            fd.server,
            fd.telegram_tag,
        ])
        if not has_any_value:
            missing_accounts.append(acc.name)

    return {
        "has_issues": bool(missing_accounts),
        "missing_accounts": missing_accounts,
    }
