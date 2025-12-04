# api_views.py
# REST API для RssV7: единый источник данных по фермам.
#
# Основные эндпойнты:
#   GET  /api/farms/v1        -> список ферм по серверу бота
#   POST /api/farms/v1/save   -> сохранить изменения из модалки RssV7

from datetime import datetime

from flask import Blueprint, request, jsonify
from werkzeug.exceptions import BadRequest

from UsersDash.models import db, Server, Account, FarmData, User

api_bp = Blueprint("api", __name__)


# ----------------- Вспомогательная авторизация серверов -----------------


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

    # Все FarmData для владельцев этих аккаунтов
    owner_ids = {acc.owner_id for acc in accounts}
    farm_data_entries = (
        FarmData.query
        .filter(FarmData.user_id.in_(owner_ids))
        .all()
    )

    # Индекс FarmData по (user_id, farm_name)
    fd_index = {(fd.user_id, fd.farm_name): fd for fd in farm_data_entries}

    items = []
    for acc in accounts:
        fd = fd_index.get((acc.owner_id, acc.name))
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

            # Ищем аккаунт в рамках этого сервера:
            query = Account.query.filter_by(server_id=srv.id)

            if internal_id:
                # Пытаемся по internal_id (GUID или instanceId)
                acc = query.filter_by(internal_id=internal_id).first()
            else:
                acc = None

            if not acc and name:
                # Фоллбек по имени, если internal_id ещё не забит
                acc = query.filter_by(name=name).first()

            if not acc:
                # Не нашли соответствие — можно логировать, но не падаем
                continue

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
            fd = (
                FarmData.query
                .filter_by(user_id=owner_id, farm_name=farm_name)
                .first()
            )

            if not fd and any([email, login_val, password_val, igg_id, kingdom]):
                fd = FarmData(
                    user_id=owner_id,
                    farm_name=farm_name,
                )
                db.session.add(fd)

            if fd:
                fd.email = email or None
                fd.login = login_val or None
                fd.password = password_val or None
                fd.igg_id = igg_id or None
                fd.server = kingdom or None
                fd.telegram_tag = telegram_tag or None

            # обновляем оплату/тариф
            if next_payment_at:
                try:
                    acc.next_payment_at = datetime.strptime(next_payment_at, "%Y-%m-%d")
                except ValueError:
                    # Игнорируем неверный формат
                    pass
            else:
                acc.next_payment_at = None

            if tariff_raw not in (None, ""):
                try:
                    acc.next_payment_amount = int(tariff_raw)
                except (TypeError, ValueError):
                    pass
            else:
                acc.next_payment_amount = None

        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        print(f"[save_farms_v1] ERROR: {exc}")
        return jsonify({"ok": False, "error": "Internal error while saving."}), 500

    return jsonify({"ok": True})
