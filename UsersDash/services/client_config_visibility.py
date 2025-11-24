"""Обёртка для работы с таблицей client_config_visibility."""

from types import SimpleNamespace
from typing import List, Optional

from UsersDash.models import ClientConfigVisibility, db


def list_for_script(script_id: str, scope: Optional[str] = None) -> List[ClientConfigVisibility]:
    """Возвращает записи для указанного скрипта, отсортированные по order_index."""

    query = ClientConfigVisibility.query.filter_by(script_id=script_id)
    if scope:
        query = query.filter_by(scope=scope)

    return query.order_by(
        ClientConfigVisibility.order_index.asc(),
        ClientConfigVisibility.id.asc(),
    ).all()


def get_record(
    script_id: str,
    config_key: str,
    scope: str = "global",
    group_key: Optional[str] = None,
) -> Optional[ClientConfigVisibility]:
    """Возвращает одну запись по ключам."""

    return (
        ClientConfigVisibility.query.filter_by(
            script_id=script_id,
            config_key=config_key,
            scope=scope,
            group_key=group_key,
        )
        .order_by(ClientConfigVisibility.id.asc())
        .first()
    )


def upsert_record(
    *,
    script_id: str,
    config_key: str,
    scope: str = "global",
    group_key: Optional[str] = None,
    client_visible: bool = True,
    client_label: Optional[str] = None,
    order_index: int = 0,
) -> ClientConfigVisibility:
    """Создаёт или обновляет запись о видимости конфигурации."""

    record = get_record(script_id=script_id, config_key=config_key, scope=scope, group_key=group_key)

    if record is None:
        record = ClientConfigVisibility(
            script_id=script_id,
            config_key=config_key,
            scope=scope,
            group_key=group_key,
        )

    record.client_visible = client_visible
    record.client_label = client_label
    record.order_index = order_index

    db.session.add(record)
    db.session.commit()

    return record


def delete_record(
    *,
    script_id: str,
    config_key: str,
    scope: str = "global",
    group_key: Optional[str] = None,
) -> int:
    """Удаляет записи и возвращает количество удалённых."""

    query = ClientConfigVisibility.query.filter_by(
        script_id=script_id,
        config_key=config_key,
        scope=scope,
        group_key=group_key,
    )
    count = query.delete()
    db.session.commit()
    return count

DEFAULT_VISIBILITY_RULES = [
    {
        "script_id": "vikingbot.base.gathervip",
        "config_key": "Farm",
        "scope": "global",
        "group_key": "gathering",
        "client_label": "Сбор на карте",
        "client_visible": True,
        "order_index": 0,
    },
    {
        "script_id": "vikingbot.base.alliancegather",
        "config_key": "Farm",
        "scope": "global",
        "group_key": "gathering",
        "client_label": "Сбор альянса",
        "client_visible": True,
        "order_index": 0,
    },
]


def _default_rules(scope: str = "global", script_id: Optional[str] = None) -> list[dict]:
    current_scope = scope or "global"
    return [
        rule
        for rule in DEFAULT_VISIBILITY_RULES
        if (rule.get("scope") or "global") == current_scope
        and (script_id is None or rule.get("script_id") == script_id)
    ]


def merge_records_with_defaults(
    records: List[ClientConfigVisibility] | None,
    *,
    scope: str = "global",
    script_id: Optional[str] = None,
) -> List[ClientConfigVisibility | SimpleNamespace]:
    """Дополняет записи правилами по умолчанию.

    В БД могут отсутствовать предзаданные правила, которые нужны для
    группировки шагов и кастомных меток. Возвращаем список, объединённый
    с дефолтами, не дублируя существующие записи.
    """

    merged = list(records or [])
    defaults = _default_rules(scope, script_id)
    if not defaults:
        return merged

    existing_keys = {
        (rec.script_id, rec.config_key)
        for rec in merged
        if (getattr(rec, "scope", None) or "global") == (scope or "global")
    }

    for rule in defaults:
        key = (rule.get("script_id"), rule.get("config_key"))
        if key in existing_keys:
            continue
        merged.append(
            SimpleNamespace(
                script_id=rule.get("script_id"),
                config_key=rule.get("config_key"),
                group_key=rule.get("group_key"),
                client_visible=rule.get("client_visible", True),
                client_label=rule.get("client_label"),
                order_index=rule.get("order_index", 0),
                scope=rule.get("scope", "global"),
                id=None,
            )
        )

    return merged


def defaults_for_script(script_id: str, scope: str = "global") -> list[dict]:
    """Возвращает дефолтные правила для указанного скрипта в виде dict."""

    return [
        {
            "config_key": rule.get("config_key"),
            "group_key": rule.get("group_key"),
            "client_visible": rule.get("client_visible", True),
            "client_label": rule.get("client_label"),
            "order_index": rule.get("order_index", 0),
        }
        for rule in _default_rules(scope, script_id)
    ]

