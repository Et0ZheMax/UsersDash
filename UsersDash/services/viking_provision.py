"""Подтверждаемый импорт новых ферм из Viking Tools с ограничением по серверу."""

from __future__ import annotations

import hashlib
import json
import re
from functools import wraps

import requests
from flask import current_app, jsonify, request
from sqlalchemy import func
from werkzeug.exceptions import BadRequest, Conflict

from UsersDash.models import Account, FarmData, db
from UsersDash.services.notifications import _iter_telegram_chats
from UsersDash.services.tariffs import TARIFFS


def receipt(row: dict) -> str:
    """Отпечаток подтверждённых полей без отправки пароля в ответе."""

    fields = {key: row[key] for key in ("internal_id", "name", "email", "password", "igg_id", "tariff")}
    return hashlib.sha256(json.dumps(fields, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def marker(account: Account) -> dict:
    try:
        value = json.loads(account.notes or "{}")
    except ValueError:
        return {}
    return value.get("VikingTools", {}) if isinstance(value, dict) else {}


def saved_row(account: Account, data: FarmData) -> dict:
    return {
        "internal_id": account.internal_id, "name": account.name,
        "email": data.email or "", "password": data.password or "", "igg_id": data.igg_id or "",
        "tariff": account.next_payment_tariff,
    }


def register_provision_routes(blueprint, authenticate) -> None:
    """Добавить API создания, завершения и уведомления с проверкой токена сервера."""

    def guarded(function):
        @wraps(function)
        def wrapped():
            try:
                server = authenticate()
                if not server.is_active:
                    raise BadRequest("Сервер отключён")
                return function(server)
            except (BadRequest, Conflict) as exc:
                db.session.rollback()
                return jsonify(ok=False, error=exc.description), exc.code
            except Exception:
                db.session.rollback()
                # SQLAlchemy может включить пароль в текст ошибки/параметры запроса.
                current_app.logger.error("VikingTools: операция БД или Telegram не завершена")
                return jsonify(ok=False, error="Операция не завершена; повторите запрос"), 500
        return wrapped

    @blueprint.route("/farms/v1/provision", methods=["GET"])
    @guarded
    def provision_capabilities(server):
        return jsonify(
            ok=True, version=1, server=server.name,
            tariffs={str(price): TARIFFS[price]["name"] for price in (500, 1000, 1400)},
            telegram_ready=bool(list(_iter_telegram_chats(current_app.config))),
        )

    @blueprint.route("/farms/v1/provision/reserve", methods=["POST"])
    @guarded
    def provision_reserve(server):
        row = request.get_json(silent=True) or {}
        if not isinstance(row, dict):
            raise BadRequest("Ожидался объект фермы")
        internal_id = str(row.get("internal_id") or "")
        name = str(row.get("name") or "").strip()
        email = str(row.get("email") or "").strip()
        password = str(row.get("password") or "")
        igg = str(row.get("igg_id") or "").strip()
        try:
            tariff = int(row.get("tariff"))
        except (TypeError, ValueError):
            raise BadRequest("Неверный тариф")
        if not re.fullmatch(r"[a-f0-9]{32}", internal_id) or not name or len(name) > 128:
            raise BadRequest("Неверные ID или имя фермы")
        if not email or "@" not in email or not password or tariff not in (500, 1000, 1400):
            raise BadRequest("Заполните почту, пароль и тариф")
        if igg and (not igg.isdigit() or len(igg) < 5):
            raise BadRequest("IGG ID: минимум пять цифр или пустое поле")
        normalized = dict(internal_id=internal_id, name=name, email=email, password=password, igg_id=igg, tariff=tariff)
        account = Account.query.filter_by(internal_id=internal_id).first()
        if account:
            if account.server_id != server.id or marker(account).get("operation_id") != internal_id:
                raise Conflict("Этот ID принадлежит другой операции")
            data = FarmData.query.filter_by(account_id=account.id).one()
            # Завершённый аккаунт не откатываем в pending при повторном запросе.
            original = marker(account).get("request_receipt")
            if original != receipt(normalized):
                raise Conflict("Данные операции изменились; используйте первоначальные данные")
        else:
            if Account.query.filter_by(server_id=server.id).filter(func.lower(Account.name) == name.lower()).first():
                raise Conflict("Ферма с таким именем уже существует на сервере")
            from UsersDash.admin_views import _get_or_create_client_for_farm

            owner = _get_or_create_client_for_farm(name)
            account = Account(
                name=name, server_id=server.id, owner_id=owner.id, internal_id=internal_id,
                is_active=False, blocked_for_payment=False,
                next_payment_amount=tariff, next_payment_tariff=tariff,
                notes=json.dumps({"VikingTools": {
                    "operation_id": internal_id, "status": "reserved", "request_receipt": receipt(normalized),
                }}),
            )
            db.session.add(account)
            db.session.flush()
            data = FarmData(
                account_id=account.id, user_id=owner.id, farm_name=name,
                email=email, password=password, igg_id=igg or None,
            )
            db.session.add(data)
        db.session.commit()
        db.session.expire_all()
        account = Account.query.filter_by(internal_id=internal_id).one()
        data = FarmData.query.filter_by(account_id=account.id).one()
        return jsonify(
            ok=True, account_id=account.id, internal_id=internal_id,
            status=marker(account)["status"], receipt=receipt(saved_row(account, data)),
        )

    @blueprint.route("/farms/v1/provision/complete", methods=["POST"])
    @guarded
    def provision_complete(server):
        row = request.get_json(silent=True) or {}
        account = Account.query.filter_by(server_id=server.id, internal_id=row.get("internal_id")).first()
        if not account or marker(account).get("operation_id") != account.internal_id:
            raise Conflict("Операция создания не найдена на этом сервере")
        try:
            index = int(row.get("instance_id"))
        except (ValueError, TypeError):
            raise BadRequest("Неверный LDPlayer ID")
        igg = str(row.get("igg_id") or "")
        if index <= 0 or not igg.isdigit() or len(igg) < 5 or row.get("name") != account.name:
            raise BadRequest("Не подтверждены эмулятор, имя или игровой ID")
        data = FarmData.query.filter_by(account_id=account.id).one()
        original = data.igg_id or ""
        if original and not igg.startswith(original[:5]):
            raise Conflict("Выбранный игровой ID не соответствует введённому")
        meta = marker(account)
        if meta.get("status") == "completed" and (meta.get("instance_id") != index or original != igg):
            raise Conflict("Операция уже завершена с другой связкой")
        data.igg_id = igg
        account.is_active = True
        meta.update(status="completed", instance_id=index)
        account.notes = json.dumps({"VikingTools": meta})
        db.session.commit()
        db.session.expire_all()
        account = Account.query.filter_by(server_id=server.id, internal_id=account.internal_id).one()
        data = FarmData.query.filter_by(account_id=account.id).one()
        return jsonify(
            ok=True, internal_id=account.internal_id, instance_id=index, receipt=receipt(saved_row(account, data)),
        )

    @blueprint.route("/farms/v1/provision/notify", methods=["POST"])
    @guarded
    def provision_notify(server):
        ids = (request.get_json(silent=True) or {}).get("internal_ids")
        if not isinstance(ids, list) or not ids or len(ids) > 100:
            raise BadRequest("Не указаны созданные фермы")
        accounts = Account.query.filter(Account.server_id == server.id, Account.internal_id.in_(ids)).all()
        if len(accounts) != len(set(ids)) or any(marker(a).get("status") != "completed" for a in accounts):
            raise Conflict("В пакет включены незавершённые фермы")
        chats = list(_iter_telegram_chats(current_app.config))
        if not chats:
            raise BadRequest("Telegram не настроен в UsersDash")
        message = f"✅ Viking Tools · {server.name}\nБот запущен, задачи восстановлены.\nСозданы фермы:\n" + "\n".join(
            f"{a.name} · LDPlayer {marker(a)['instance_id']} · {TARIFFS[a.next_payment_tariff]['name']}"
            for a in accounts
        )
        delivered = 0
        for token, chat_id in chats:
            result = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message[:4000]}, timeout=20,
            )
            if result.ok and result.json().get("ok"):
                delivered += 1
        if delivered != len(chats):
            raise BadRequest("Telegram не подтвердил доставку во все настроенные чаты")
        return jsonify(ok=True, delivered=delivered)
