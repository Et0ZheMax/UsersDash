"""Сервис для работы с единым сообщением "Инфо" для всех клиентов."""

from datetime import datetime

from UsersDash.models import GlobalInfoMessage, db


def _ensure_singleton() -> GlobalInfoMessage:
    """Возвращает единственную запись с сообщением, создавая пустую при отсутствии."""

    existing = GlobalInfoMessage.query.order_by(GlobalInfoMessage.id.asc()).first()
    if existing:
        return existing

    new_msg = GlobalInfoMessage(message_text="")
    db.session.add(new_msg)
    db.session.commit()
    return new_msg


def get_global_info_message_text() -> str:
    """Возвращает текст сообщения для блока «Инфо» (или пустую строку)."""

    message = _ensure_singleton()
    return message.message_text or ""


def set_global_info_message_text(new_text: str) -> GlobalInfoMessage:
    """Обновляет текст сообщения для всех клиентов и возвращает модель."""

    message = _ensure_singleton()
    message.message_text = (new_text or "").strip()
    message.updated_at = datetime.utcnow()
    db.session.commit()
    return message


def get_global_info_message() -> GlobalInfoMessage:
    """Возвращает модель сообщения (создаёт при необходимости)."""

    return _ensure_singleton()
