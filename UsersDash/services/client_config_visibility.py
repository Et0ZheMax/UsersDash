"""Обёртка для работы с таблицей client_config_visibility."""

from types import SimpleNamespace
from typing import Iterable, List, Optional

from UsersDash.models import ClientConfigVisibility, db

SCRIPT_LABEL_CONFIG_KEY = ""


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
) -> Optional[ClientConfigVisibility]:
    """Возвращает одну запись по ключам."""

    return (
        ClientConfigVisibility.query.filter_by(
            script_id=script_id,
            config_key=config_key,
            scope=scope,
        )
        .order_by(ClientConfigVisibility.id.asc())
        .first()
    )


def get_script_label_record(
    script_id: str,
    *,
    scope: str = "global",
) -> Optional[ClientConfigVisibility]:
    """Возвращает запись с пользовательской меткой скрипта."""

    return get_record(script_id=script_id, config_key=SCRIPT_LABEL_CONFIG_KEY, scope=scope)


def upsert_record(
    *,
    script_id: str,
    config_key: str,
    scope: str = "global",
    client_visible: bool = True,
    client_label: Optional[str] = None,
    order_index: int = 0,
) -> ClientConfigVisibility:
    """Создаёт или обновляет запись о видимости конфигурации."""

    record = get_record(script_id=script_id, config_key=config_key, scope=scope)

    if record is None:
        record = ClientConfigVisibility(
            script_id=script_id,
            config_key=config_key,
            scope=scope,
        )

    record.client_visible = client_visible
    record.client_label = client_label
    record.order_index = order_index

    db.session.add(record)
    db.session.commit()

    return record


def upsert_script_label(
    *,
    script_id: str,
    script_label: Optional[str],
    scope: str = "global",
) -> Optional[ClientConfigVisibility]:
    """Создаёт, обновляет или удаляет пользовательскую метку скрипта."""

    if not script_label:
        delete_record(script_id=script_id, config_key=SCRIPT_LABEL_CONFIG_KEY, scope=scope)
        return None

    return upsert_record(
        script_id=script_id,
        config_key=SCRIPT_LABEL_CONFIG_KEY,
        scope=scope,
        client_visible=True,
        client_label=script_label,
        order_index=0,
    )


def delete_record(
    *,
    script_id: str,
    config_key: str,
    scope: str = "global",
) -> int:
    """Удаляет записи и возвращает количество удалённых."""

    query = ClientConfigVisibility.query.filter_by(
        script_id=script_id,
        config_key=config_key,
        scope=scope,
    )
    count = query.delete()
    db.session.commit()
    return count

# Специальный ключ config_key="__step__" используется для скрытия целого шага
# на клиентской стороне. Такие шаги остаются в исходном списке (для корректных
# индексов), но не отображаются в интерфейсе клиента.
STEP_HIDDEN_KEY = "__step__"
STEP_HIDDEN_LABEL = "Скрыть шаг"

DEFAULT_VISIBILITY_RULES = [
    {
        "script_id": "vikingbot.base.gathervip",
        "config_key": "Farm",
        "scope": "global",
        "client_label": "Сбор на карте",
        "client_visible": True,
        "order_index": 0,
    },
    {
        "script_id": "vikingbot.base.alliancegather",
        "config_key": "Farm",
        "scope": "global",
        "client_label": "Сбор альянса",
        "client_visible": True,
        "order_index": 0,
    },
    # Таймеры и сервисные шаги не должны показываться клиентам
    {
        "script_id": "vikingbot.base.accountswitch",
        "config_key": STEP_HIDDEN_KEY,
        "client_label": "Таймеры: переключение аккаунтов",
        "client_visible": False,
        "order_index": 0,
    },
    {
        "script_id": "vikingbot.base.transfer",
        "config_key": STEP_HIDDEN_KEY,
        "client_label": "Таймеры: перевод ресурсов",
        "client_visible": False,
        "order_index": 0,
    },
    {
        "script_id": "vikingbot.base.openinventory",
        "config_key": STEP_HIDDEN_KEY,
        "client_label": "Таймеры: инвентарь",
        "client_visible": False,
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
    script_ids: Iterable[str] | None = None,
) -> List[ClientConfigVisibility | SimpleNamespace]:
    """Дополняет записи правилами по умолчанию.

    В БД могут отсутствовать предзаданные правила, которые нужны для меток.
    Возвращаем список, объединённый с дефолтами, не дублируя существующие
    записи.
    """

    merged = list(records or [])
    defaults = _default_rules(scope, script_id)
    current_scope = scope or "global"
    if not defaults:
        defaults = []

    existing_keys = {
        (rec.script_id, rec.config_key)
        for rec in merged
        if (getattr(rec, "scope", None) or "global") == current_scope
    }

    max_order_by_script: dict[str, int] = {}
    for rec in merged:
        if (getattr(rec, "scope", None) or "global") != current_scope:
            continue
        order_value = rec.order_index or 0
        max_order_by_script[rec.script_id] = max(
            max_order_by_script.get(rec.script_id, order_value),
            order_value,
        )

    for rule in defaults:
        key = (rule.get("script_id"), rule.get("config_key"))
        if key in existing_keys:
            continue
        order_index = rule.get("order_index") or 0
        merged.append(
            SimpleNamespace(
                script_id=rule.get("script_id"),
                config_key=rule.get("config_key"),
                client_visible=rule.get("client_visible", True),
                client_label=rule.get("client_label"),
                order_index=order_index,
                scope=rule.get("scope", "global"),
                id=None,
            )
        )
        existing_keys.add(key)
        if rule.get("script_id"):
            max_order_by_script[rule["script_id"]] = max(
                max_order_by_script.get(rule["script_id"], order_index),
                order_index,
            )

    script_ids_set = set(script_ids or [])
    if script_id:
        script_ids_set.add(script_id)

    for sid in script_ids_set:
        key = (sid, STEP_HIDDEN_KEY)
        if key in existing_keys:
            continue

        next_order_idx = max_order_by_script.get(sid, -1) + 1
        merged.append(
            SimpleNamespace(
                script_id=sid,
                config_key=STEP_HIDDEN_KEY,
                client_visible=True,
                client_label=STEP_HIDDEN_LABEL,
                order_index=next_order_idx,
                scope=current_scope,
                id=None,
            )
        )
        existing_keys.add(key)
        max_order_by_script[sid] = next_order_idx

    return merged


def defaults_for_script(script_id: str, scope: str = "global") -> list[dict]:
    """Возвращает дефолтные правила для указанного скрипта в виде dict."""

    records = merge_records_with_defaults(
        [], scope=scope, script_id=script_id, script_ids=[script_id]
    )
    return [
        {
            "config_key": rec.config_key,
            "client_visible": rec.client_visible,
            "client_label": rec.client_label,
            "order_index": rec.order_index,
        }
        for rec in records
    ]

