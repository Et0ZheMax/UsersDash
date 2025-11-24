"""Обёртка для работы с таблицей client_config_visibility."""

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

