# api_views.py
# REST API для RssV7: единый источник данных по фермам.
#
# Основные эндпойнты:
#   GET  /api/farms/v1        -> список ферм по серверу бота
#   POST /api/farms/v1/save   -> сохранить изменения из модалки RssV7

from datetime import datetime

from flask import Blueprint, request, jsonify
from sqlalchemy import text
from werkzeug.exceptions import BadRequest

from UsersDash.models import db, Server, Account, FarmData, User

api_bp = Blueprint("api", __name__)


# ----------------- Вспомогательная авторизация серверов -----------------


def _merge_farmdata_for_move(
    account_id: int,
    new_owner_id: int,
    new_name: str,
) -> FarmData:
    """
    Обновляет привязку FarmData к аккаунту при смене владельца или имени.
    """
    target_fd = FarmData.query.filter_by(account_id=account_id).first()
    if not target_fd:
        target_fd = FarmData(
            account_id=account_id,
            user_id=new_owner_id,
            farm_name=new_name,
        )
        db.session.add(target_fd)
    else:
        target_fd.user_id = new_owner_id
        target_fd.farm_name = new_name
    return target_fd


def _get_server_from_request() -> Server:
    """
    Достаём из запроса:
      - server: имя сервера-бота (F99, 208, R9)
      - token: api_token, прописанный в БД для этого сервера.

    Если что-то не так — бросаем BadRequest (вернётся 400/403).
    """
    server_name = request.args.get("server", "").strip()
    token = request.args.get("token", "").strip()

    if not server_name:
        raise BadRequest("Missing 'server' parameter.")
    if not token:
        raise BadRequest("Missing 'token' parameter.")

    srv = Server.query.filter_by(name=server_name).first()
    if not srv:
        raise BadRequest(f"Unknown server '{server_name}'.")
    if not srv.api_token or srv.api_token.strip() != token:
        raise BadRequest("Invalid token.")

    return srv


def _get_or_create_farmdata_entry(
    account_id: int,
    user_id: int,
    farm_name: str,
) -> FarmData:
    existing = FarmData.query.filter_by(account_id=account_id).first()
    if existing:
        return existing

    if db.session.bind and db.session.bind.dialect.name == "sqlite":
        now = datetime.utcnow()
        db.session.execute(
            text(
                """
                INSERT OR IGNORE INTO farm_data
                    (account_id, user_id, farm_name, created_at, updated_at)
                VALUES
                    (:account_id, :user_id, :farm_name, :created_at, :updated_at)
                """
            ),
            {
                "account_id": account_id,
                "user_id": user_id,
                "farm_name": farm_name,
                "created_at": now,
                "updated_at": now,
            },
        )
        return FarmData.query.filter_by(account_id=account_id).first()

    farm_data = FarmData(
        account_id=account_id,
        user_id=user_id,
        farm_name=farm_name,
    )
    db.session.add(farm_data)
    return farm_data


# ------------------------ GET /api/farms/v1 ------------------------------


@api_bp.route("/farms/v1", methods=["GET"])
def get_farms_v1():
    """
    Возвращает список ферм для конкретного сервера-бота (F99, 208, R9...).

    Параметры:
      - server: имя Server.name
      - token: Server.api_token

    Ответ:
    {
      "ok": true,
      "server": "F99",
      "items": [
        {
          "internal_id": "54",
          "name": "EtoZheMax",
          "user": "User1",
          "email": "...",
          "login": "...",
          "password": "...",
          "igg_id": "11112",
          "kingdom": "111",
          "next_payment_at": "2025-12-01",
          "tariff": 500
        }
      ]
    }
    """
    try:
        srv = _get_server_from_request()
    except BadRequest as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    # Все аккаунты на этом сервере
    accounts = (
        Account.query
        .filter_by(server_id=srv.id)
        .join(Server, Account.server_id == Server.id)
        .join(User, Account.owner)
        .all()
    )

    # Все FarmData для этих аккаунтов
    account_ids = [acc.id for acc in accounts]
    farm_data_entries = (
        FarmData.query
        .filter(FarmData.account_id.in_(account_ids))
        .all()
    )

    # Индекс FarmData по account_id
    fd_index = {fd.account_id: fd for fd in farm_data_entries}

    items = []
    for acc in accounts:
        fd = fd_index.get(acc.id)
        owner = acc.owner

        items.append({
            "internal_id": acc.internal_id or "",
            "name": acc.name,
            "user": owner.username if owner else "",
            "email": (fd.email if fd and fd.email else ""),
            "login": (fd.login if fd and fd.login else ""),
            "password": (fd.password if fd and fd.password else ""),
            "igg_id": (fd.igg_id if fd and fd.igg_id else ""),
            "kingdom": (fd.server if fd and fd.server else ""),
            "telegram_tag": (fd.telegram_tag if fd and fd.telegram_tag else ""),
            "next_payment_at": acc.next_payment_at.strftime("%Y-%m-%d") if acc.next_payment_at else "",
            "is_active": bool(acc.is_active),
            "active": bool(acc.is_active and not acc.blocked_for_payment),
            "blocked_for_payment": bool(acc.blocked_for_payment),
            "tariff": acc.next_payment_amount if acc.next_payment_amount is not None else None,
        })

    return jsonify({
        "ok": True,
        "server": srv.name,
        "items": items,
    })


# ---------------------- POST /api/farms/v1/save --------------------------


@api_bp.route("/farms/v1/save", methods=["POST"])
def save_farms_v1():
    """
    Принимает изменения из модалки RssV7 и сохраняет их в UsersDash.

    Параметры запроса:
      - server: имя Server.name
      - token: Server.api_token

    Тело JSON:
    {
      "items": [
        {
          "internal_id": "54",
          "name": "EtoZheMax",
          "email": "...",
          "login": "...",
          "password": "...",
          "igg_id": "11112",
          "kingdom": "111",
          "next_payment_at": "2025-12-01",
          "tariff": 500
        }
      ]
    }
    """
    try:
        srv = _get_server_from_request()
    except BadRequest as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    payload = request.get_json(silent=True) or {}
    items = payload.get("items") or []

    if not isinstance(items, list) or not items:
        return jsonify({"ok": False, "error": "No items to save."}), 400

    try:
        for row in items:
            internal_id = (row.get("internal_id") or "").strip()
            name = (row.get("name") or "").strip()

            if not internal_id and not name:
                # Нечем сопоставлять
                continue

            if internal_id:
                # Пытаемся по internal_id (GUID или instanceId)
                acc = Account.query.filter_by(internal_id=internal_id).first()
            else:
                acc = None

            if not acc and name:
                # Фоллбек по имени, если internal_id ещё не забит
                acc = Account.query.filter_by(name=name).first()

            if not acc:
                # Не нашли соответствие — можно логировать, но не падаем
                continue

            server_changed = acc.server_id != srv.id
            if server_changed:
                acc.server_id = srv.id

            if name and acc.name != name:
                _merge_farmdata_for_move(acc.id, acc.owner_id, name)
                acc.name = name

            if internal_id and acc.internal_id != internal_id:
                acc.internal_id = internal_id

            owner_id = acc.owner_id
            farm_name = acc.name

            email = (row.get("email") or "").strip()
            login_val = (row.get("login") or "").strip()
            password_val = (row.get("password") or "").strip()
            igg_id = (row.get("igg_id") or "").strip()
            kingdom = (row.get("kingdom") or "").strip()
            telegram_tag = (row.get("telegram_tag") or "").strip()
            next_payment_at = (row.get("next_payment_at") or "").strip()
            tariff_raw = row.get("tariff")

            # upsert FarmData
            fd = FarmData.query.filter_by(account_id=acc.id).first()
            if not fd and any([email, login_val, password_val, igg_id, kingdom, telegram_tag]):
                fd = _get_or_create_farmdata_entry(acc.id, owner_id, farm_name)

            if fd:
                fd.user_id = owner_id
                fd.farm_name = farm_name
                def _apply_if_needed(field_name: str, value: str) -> None:
                    if server_changed and not value:
                        return
                    setattr(fd, field_name, value or None)

                _apply_if_needed("email", email)
                _apply_if_needed("login", login_val)
                _apply_if_needed("password", password_val)
                _apply_if_needed("igg_id", igg_id)
                _apply_if_needed("server", kingdom)
                _apply_if_needed("telegram_tag", telegram_tag)

            # обновляем оплату/тариф
            if next_payment_at:
                try:
                    acc.next_payment_at = datetime.strptime(next_payment_at, "%Y-%m-%d")
                except ValueError:
                    # Игнорируем неверный формат
                    pass
            elif not server_changed:
                acc.next_payment_at = None

            if tariff_raw not in (None, ""):
                try:
                    parsed_tariff = int(tariff_raw)
                    acc.next_payment_amount = parsed_tariff
                    acc.next_payment_tariff = parsed_tariff
                except (TypeError, ValueError):
                    pass
            elif not server_changed:
                acc.next_payment_amount = None
                acc.next_payment_tariff = None

        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        print(f"[save_farms_v1] ERROR: {exc}")
        return jsonify({"ok": False, "error": "Internal error while saving."}), 500

    return jsonify({"ok": True})
