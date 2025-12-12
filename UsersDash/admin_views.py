# admin_views.py
# Маршруты админ-панели: общий дашборд, пользователи, сервера, фермы.
# Здесь только админская логика, доступная пользователям с role='admin'.

import os
import re
import csv
import io
import json
import difflib
from pathlib import Path
from calendar import monthrange
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import traceback
from typing import Any

from flask import (
    Blueprint,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
    Response,
    current_app,
)
from flask_login import current_user, login_required
from werkzeug.security import generate_password_hash
from sqlalchemy.orm import joinedload

from UsersDash.models import (
    Account,
    ClientConfigVisibility,
    FarmData,
    Server,
    SettingsAuditLog,
    User,
    db,
)
from UsersDash.config import Config
from UsersDash.services.db_backup import backup_database
from UsersDash.services import client_config_visibility
from UsersDash.services.audit import settings_audit_context
from UsersDash.services.remote_api import (
    fetch_account_settings,
    fetch_resources_for_accounts,
    fetch_rssv7_accounts_meta,
    fetch_template_payload,
    fetch_template_schema,
    fetch_templates_list,
    fetch_templates_check,
    fetch_server_self_status,
    fetch_server_cycle_time,
    fetch_watch_summary,
    rename_template_payload,
    save_template_payload,
    update_account_active,
    delete_template_payload,
)
from UsersDash.services.default_settings import apply_defaults_for_account, has_defaults_for_tariff
from UsersDash.services.tariffs import TARIFF_PRICE_MAP, get_tariff_name_by_price
from UsersDash.services.info_message import (
    get_global_info_message,
    set_global_info_message_text,
)
from UsersDash.services.notifications import send_notification



admin_bp = Blueprint("admin", __name__)


def admin_required():
    """
    Простая проверка, что текущий пользователь — администратор.
    Если нет — возвращаем 403 (доступ запрещён).
    """
    if not current_user.is_authenticated or current_user.role != "admin":
        abort(403)


MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def _to_moscow_time(dt: datetime) -> datetime:
    """Переводит datetime в часовой пояс Москвы для отображения."""

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MOSCOW_TZ)


def _get_unassigned_user(return_created: bool = False):
    """Возвращает (или создаёт) плейсхолдер-клиента для безымянных ферм."""

    username = "Unassigned"
    user = User.query.filter_by(username=username).first()
    created = False

    if not user:
        user = User(
            username=username,
            role="client",
            is_active=False,
            password_hash=generate_password_hash("generated"),
        )
        db.session.add(user)
        db.session.flush()
        created = True

    return (user, created) if return_created else user


def _get_or_create_client_for_farm(
    farm_name: str, *, return_created: bool = False
) -> User | tuple[User, bool]:
    """
    Возвращает клиента с базовым именем фермы (без числового суффикса).

    Примеры:
    - "Ivan" или "Ivan1" или "Ivan2" -> клиент "Ivan".
    - Если клиента ещё нет, создаёт его с дефолтным паролем.
    """
    base_name = (farm_name or "").strip()
    if not base_name:
        return _get_unassigned_user(return_created=return_created)

    match = re.match(r"^(.*?)(\d+)?$", base_name)
    username = (match.group(1) if match else base_name).strip(" _-") or base_name

    existing = User.query.filter_by(username=username).first()
    if existing:
        return (existing, False) if return_created else existing

    user = User(
        username=username,
        role="client",
        is_active=True,
        password_hash=generate_password_hash("123456789m"),
    )
    db.session.add(user)
    db.session.flush()  # чтобы id появился до использования
    return (user, True) if return_created else user


def _merge_farmdata_for_move(
    old_owner_id: int | None,
    old_name: str | None,
    new_owner_id: int,
    new_name: str,
):
    """
    Переносит/объединяет FarmData при смене владельца или имени фермы.
    - Если данных не было — создаёт запись для нового владельца/имени.
    - Если были, но у нового владельца пусто — переносит полностью.
    - Если у нового владельца уже были данные, то заполняет пустые поля
      значениями из старой записи.
    """

    if not old_owner_id or not old_name:
        target_fd = FarmData.query.filter_by(
            user_id=new_owner_id, farm_name=new_name
        ).first()
        if not target_fd:
            target_fd = FarmData(user_id=new_owner_id, farm_name=new_name)
            db.session.add(target_fd)
        return

    old_fd = FarmData.query.filter_by(user_id=old_owner_id, farm_name=old_name).first()
    target_fd = FarmData.query.filter_by(
        user_id=new_owner_id, farm_name=new_name
    ).first()

    if old_fd and not target_fd:
        old_fd.user_id = new_owner_id
        old_fd.farm_name = new_name
        target_fd = old_fd
    elif old_fd and target_fd and old_fd is not target_fd:
        for field in ["email", "login", "password", "igg_id", "server", "telegram_tag"]:
            current_val = getattr(target_fd, field)
            if not current_val:
                setattr(target_fd, field, getattr(old_fd, field))

    if not target_fd:
        target_fd = FarmData(user_id=new_owner_id, farm_name=new_name)
        db.session.add(target_fd)


def _format_checked_at(raw_value: str | None) -> str | None:
    """Форматирует timestamp self_status в «ДД.ММ ЧЧ:ММ» или возвращает исходник."""

    if not raw_value:
        return None

    try:
        dt = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        dt = _to_moscow_time(dt)
        return dt.strftime("%d.%m %H:%M")
    except ValueError:
        return raw_value


TEMPLATES_CHECK_CACHE = Config.DATA_DIR / "templates_check_cache.json"


def _load_templates_check_cache() -> dict[str, Any]:
    if not TEMPLATES_CHECK_CACHE.exists():
        return {}

    try:
        with Path(TEMPLATES_CHECK_CACHE).open("r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, dict):
                return data
    except Exception:
        current_app.logger.warning("[templates-check] Не удалось прочитать кеш, пересоздаём")

    return {}


def _save_templates_check_cache(payload: dict[str, Any]) -> None:
    try:
        TEMPLATES_CHECK_CACHE.parent.mkdir(parents=True, exist_ok=True)
        with Path(TEMPLATES_CHECK_CACHE).open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
    except Exception as exc:
        current_app.logger.warning("[templates-check] Не удалось сохранить кеш: %s", exc)


def _normalize_template_check(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Приводит отчёт /templates/check к единому виду."""

    raw = raw or {}
    missing_raw = (
        raw.get("missing")
        or raw.get("skipped")
        or raw.get("gaps")
        or raw.get("issues")
        or []
    )
    missing: list[dict[str, Any]] = []
    for item in missing_raw:
        if not isinstance(item, dict):
            continue
        missing.append(
            {
                "alias": item.get("alias") or item.get("name") or item.get("alias_name"),
                "template": item.get("template")
                or item.get("template_name")
                or item.get("expected"),
                "reason": item.get("reason") or item.get("detail") or item.get("error"),
            }
        )

    problem_count = raw.get("problem_count") or raw.get("issues_total") or raw.get("total")
    if not isinstance(problem_count, int):
        problem_count = len(missing)

    checked_at = raw.get("checked_at") or raw.get("updated_at") or raw.get("generated_at")
    checked_at_fmt = _format_checked_at(checked_at) if isinstance(checked_at, str) else None

    return {
        "checked_at": checked_at,
        "checked_at_fmt": checked_at_fmt,
        "problem_count": problem_count,
        "missing": missing,
    }


def _build_gap_key(item: dict[str, Any]) -> str:
    alias = (item.get("alias") or "").strip().lower()
    template = (item.get("template") or "").strip().lower()
    return f"{alias}::{template}" if alias or template else ""


def _diff_template_gaps(current: list[dict[str, Any]], previous: list[dict[str, Any]] | None):
    previous = previous or []
    prev_map = {_build_gap_key(item): item for item in previous if _build_gap_key(item)}
    curr_map = {_build_gap_key(item): item for item in current if _build_gap_key(item)}

    new_keys = set(curr_map) - set(prev_map)
    resolved_keys = set(prev_map) - set(curr_map)

    return {
        "new": [curr_map[key] for key in sorted(new_keys)],
        "resolved": [prev_map[key] for key in sorted(resolved_keys)],
    }


def _notify_template_gaps(server: Server, new_gaps: list[dict[str, Any]], report: dict[str, Any]):
    aliases = []
    for gap in new_gaps:
        alias = gap.get("alias") or gap.get("template") or "?"
        template = gap.get("template") or gap.get("alias") or "?"
        aliases.append(f"{alias} → {template}")

    timestamp = report.get("checked_at_fmt") or report.get("checked_at") or ""
    message = (
        f"[templates] На сервере {server.name} новые пропуски ({len(new_gaps)}): "
        f"{'; '.join(aliases)}. Проверка: {timestamp}"
    )
    send_notification(message)


def _build_server_link(server: Server) -> str | None:
    """Возвращает ссылку на сервер (api_base_url или host) с протоколом."""

    raw_link = (server.api_base_url or server.host or "").strip()
    if not raw_link:
        return None

    if not raw_link.startswith(("http://", "https://")):
        raw_link = "http://" + raw_link

    return raw_link


def _collect_server_states(servers: list[Server]) -> list[dict[str, Any]]:
    """Подтягивает self_status со всех активных серверов."""

    states: list[dict[str, Any]] = []

    for srv in servers:
        if not srv.is_active:
            continue

        status, status_err = fetch_server_self_status(srv)
        cycle_stats, _cycle_err = fetch_server_cycle_time(srv)
        link = _build_server_link(srv)

        states.append(
            {
                "name": srv.name,
                "updated": _format_checked_at(status.get("checked_at"))
                if isinstance(status, dict)
                else None,
                "error": status_err,
                "ping": bool(status.get("pingOk")) if isinstance(status, dict) else False,
                "gn": bool(status.get("gnOk")) if isinstance(status, dict) else False,
                "dn": bool(status.get("dnOk")) if isinstance(status, dict) else False,
                "dn_count": status.get("dnCount") if isinstance(status, dict) else None,
                "cycle_avg": cycle_stats.get("avg_cycle_hms")
                if isinstance(cycle_stats, dict)
                else None,
                "link": link,
            }
        )

    return states


def _collect_watch_cards(servers: list[Server]) -> list[dict[str, Any]]:
    """Собирает сводку наблюдения по активным серверам."""

    cards: list[dict[str, Any]] = []

    for srv in servers:
        if not srv.is_active:
            continue

        summary, err = fetch_watch_summary(srv)
        raw_updated = summary.get("generated_at") if summary else None
        cards.append(
            {
                "server": summary.get("server") if summary else srv.name,
                "updated": _format_checked_at(raw_updated) if raw_updated else None,
                "updated_raw": raw_updated,
                "accounts": summary.get("accounts") if summary else [],
                "error": err,
            }
        )

    return cards


def _build_farmdata_index(
    accounts: list[Account],
) -> dict[tuple[int, str], FarmData]:
    """Возвращает индекс FarmData по ключу (owner_id, farm_name)."""

    owner_ids = {acc.owner_id for acc in accounts if acc.owner_id}
    if not owner_ids:
        return {}

    farmdata_entries = FarmData.query.filter(FarmData.user_id.in_(owner_ids)).all()
    return {(fd.user_id, fd.farm_name): fd for fd in farmdata_entries}


def _collect_incomplete_farms(
    accounts: list[Account], farmdata_index: dict[tuple[int, str], FarmData]
) -> list[dict[str, Any]]:
    """Собирает фермы с незаполненными полями email/password/pay_date/tariff."""

    items: list[dict[str, Any]] = []

    for acc in accounts:
        fd = farmdata_index.get((acc.owner_id, acc.name)) if acc.owner_id else None
        email = fd.email if fd else None
        password = fd.password if fd else None
        next_payment = acc.next_payment_at.strftime("%Y-%m-%d") if acc.next_payment_at else None
        tariff = acc.next_payment_amount
        tariff_name = get_tariff_name_by_price(tariff)
        is_own_farm_tariff = tariff_name == "Своя ферма"

        missing = {
            "email": not email,
            "password": not password,
            "next_payment_date": False if is_own_farm_tariff else not next_payment,
            "tariff": False if is_own_farm_tariff else tariff is None,
        }

        if not any(missing.values()):
            continue

        items.append(
            {
                "account_id": acc.id,
                "owner_name": acc.owner.username if acc.owner else "—",
                "farm_name": acc.name,
                "server_bot": acc.server.name if acc.server else "—",
                "email": email,
                "password": password,
                "next_payment_at": next_payment,
                "tariff": tariff,
                "missing": missing,
            }
        )

    return items


# -------------------- Общий дашборд админа --------------------


@admin_bp.route("/dashboard")
@login_required
def admin_dashboard():
    """
    Главная страница админ-панели.
    Показывает базовые метрики: кол-во пользователей, клиентов, аккаунтов, серверов.
    """
    admin_required()

    total_users = User.query.count()
    total_clients = User.query.filter_by(role="client").count()
    total_admins = User.query.filter_by(role="admin").count()
    total_accounts = Account.query.count()
    total_servers = Server.query.count()

    accounts = (
        Account.query.options(
            joinedload(Account.server),
            joinedload(Account.owner),
        )
        .order_by(Account.is_active.desc(), Account.server_id.asc(), Account.name.asc())
        .all()
    )

    farmdata_index = _build_farmdata_index(accounts)

    active_accounts_count = sum(1 for acc in accounts if acc.is_active)

    resources_map = fetch_resources_for_accounts(accounts)
    accounts_data = []
    for acc in accounts:
        res_info = resources_map.get(acc.id)

        if res_info:
            resources_brief = res_info.get("brief", "—")
            today_gain = res_info.get("today_gain")
            last_updated = res_info.get("last_updated_fmt") or res_info.get("last_updated")
            has_data = True
        else:
            resources_brief = "—"
            today_gain = None
            last_updated = None
            has_data = False

        accounts_data.append(
            {
                "account": acc,
                "resources_brief": resources_brief,
                "today_gain": today_gain,
                "last_updated": last_updated,
                "has_data": has_data,
            }
        )

    today_date = datetime.utcnow().date()
    payment_accounts = [acc for acc in accounts if acc.next_payment_at]

    payment_cards = []
    for acc in payment_accounts:
        if acc.blocked_for_payment:
            # Уже отключенные за неоплату не показываем повторно в списке платежей
            continue
        pay_date = acc.next_payment_at.date()
        days_left = (pay_date - today_date).days
        if days_left <= 0:
            status = "due"
        elif days_left <= 3:
            status = "upcoming"
        else:
            continue

        telegram_tag = None
        fd = farmdata_index.get((acc.owner_id, acc.name)) if acc.owner_id else None
        if fd and fd.telegram_tag:
            telegram_tag = fd.telegram_tag.lstrip("@")

        telegram_link = f"https://t.me/{telegram_tag}" if telegram_tag else None

        payment_cards.append(
            {
                "account": acc,
                "pay_date": pay_date,
                "status": status,
                "amount": acc.next_payment_amount,
                "days_left": days_left,
                "telegram_link": telegram_link,
                "blocked_for_payment": acc.blocked_for_payment,
            }
        )

    payment_cards.sort(key=lambda x: (x["pay_date"], 0 if x["status"] == "due" else 1))

    servers = Server.query.order_by(Server.name.asc()).all()
    watch_cards = _collect_watch_cards(servers)

    today_date = datetime.utcnow().date()
    days_in_month = monthrange(today_date.year, today_date.month)[1]
    days_left = days_in_month - today_date.day + 1

    server_profit_map: dict[int, dict[str, Any]] = {}

    for srv in servers:
        if not srv.is_active:
            continue

        server_profit_map[srv.id] = {
            "server_id": srv.id,
            "server_name": srv.name,
            "active_accounts": 0,
            "monthly_total": 0,
            "remaining_total": 0,
        }

    server_states = _collect_server_states(servers)

    for acc in accounts:
        if not acc.is_active or not acc.server_id:
            continue

        srv_profit = server_profit_map.get(acc.server_id)
        if not srv_profit:
            continue

        monthly_amount = acc.next_payment_amount or 0
        if monthly_amount < 0:
            monthly_amount = 0

        srv_profit["active_accounts"] += 1
        srv_profit["monthly_total"] += monthly_amount

    for srv_profit in server_profit_map.values():
        srv_profit["remaining_total"] = (
            srv_profit["monthly_total"] * days_left // days_in_month
        )

    server_profits = sorted(
        server_profit_map.values(), key=lambda item: item.get("server_name", "")
    )

    overall_monthly_profit = sum(item["monthly_total"] for item in server_profits)
    overall_remaining_profit = sum(
        item["remaining_total"] for item in server_profits
    )

    incomplete_accounts = _collect_incomplete_farms(accounts, farmdata_index)

    return render_template(
        "admin/dashboard.html",
        total_users=total_users,
        total_clients=total_clients,
        total_admins=total_admins,
        total_accounts=total_accounts,
        total_servers=total_servers,
        accounts_data=accounts_data,
        payment_cards=payment_cards,
        watch_cards=watch_cards,
        server_states=server_states,
        server_profits=server_profits,
        cash_totals={
            "monthly_total": overall_monthly_profit,
            "remaining_total": overall_remaining_profit,
            "days_left": days_left,
            "days_in_month": days_in_month,
        },
        incomplete_accounts_total=len(incomplete_accounts),
    )


@admin_bp.route("/api/server-states", methods=["GET"])
@login_required
def api_server_states():
    """Возвращает self_status по всем активным серверам для админ-панели."""

    admin_required()

    servers = Server.query.order_by(Server.name.asc()).all()
    server_states = _collect_server_states(servers)

    return jsonify({
        "items": server_states,
        "generated_at": datetime.utcnow().isoformat(),
    })


@admin_bp.route("/api/watch-cards", methods=["GET"])
@login_required
def api_watch_cards():
    """Возвращает сводку наблюдения по всем активным серверам для админки."""

    admin_required()

    servers = Server.query.order_by(Server.name.asc()).all()
    watch_cards = _collect_watch_cards(servers)

    return jsonify({
        "items": watch_cards,
        "generated_at": datetime.utcnow().isoformat(),
    })


@admin_bp.route("/api/incomplete-farm-data", methods=["GET"])
@login_required
def api_incomplete_farm_data():
    """Возвращает список ферм с незаполненными контактами/оплатой для быстрой правки."""

    admin_required()

    accounts = (
        Account.query.options(
            joinedload(Account.server),
            joinedload(Account.owner),
        )
        .order_by(Account.owner_id.asc(), Account.name.asc())
        .all()
    )

    farmdata_index = _build_farmdata_index(accounts)
    items = _collect_incomplete_farms(accounts, farmdata_index)

    return jsonify(
        {
            "ok": True,
            "items": items,
            "total": len(items),
        }
    )


@admin_bp.route("/templates")
@login_required
def templates_editor():
    """Страница визуального редактора manage-шаблонов на выбранном сервере."""

    admin_required()
    servers = Server.query.order_by(Server.name.asc()).all()
    return render_template("admin/templates_editor.html", servers=servers)


@admin_bp.route("/templates/check")
@login_required
def templates_check_page():
    """Страница проверки manage-шаблонов и расхождений по алиасам."""

    admin_required()
    servers = Server.query.order_by(Server.name.asc()).all()
    return render_template("admin/templates_check.html", servers=servers)


def _get_server_from_request():
    server_id = request.args.get("server_id", type=int)
    if not server_id:
        return None, (jsonify({"error": "server_id обязателен"}), 400)

    server = Server.query.get(server_id)
    if not server:
        return None, (jsonify({"error": "server не найден"}), 404)

    return server, None


@admin_bp.route("/api/templates", methods=["GET"])
@login_required
def api_admin_templates_list():
    """Отдаёт список manage-шаблонов с учётом алиасов для выбранного сервера."""

    admin_required()
    server, err = _get_server_from_request()
    if err:
        return err

    data, message = fetch_templates_list(server)
    if data is None:
        return jsonify({"error": message or "не удалось получить шаблоны"}), 502

    return jsonify(data)


@admin_bp.route("/api/templates/check", methods=["GET"])
@login_required
def api_templates_check():
    """Возвращает свежий отчёт /templates/check с диффом по последнему сохранённому."""

    admin_required()
    server, err = _get_server_from_request()
    if err:
        return err

    raw_data, message = fetch_templates_check(server)
    if raw_data is None:
        return jsonify({"error": message or "не удалось получить отчёт"}), 502

    current_report = _normalize_template_check(raw_data)
    cache = _load_templates_check_cache()
    prev_report = cache.get(str(server.id)) if cache else None
    diff = _diff_template_gaps(
        current_report.get("missing", []),
        prev_report.get("missing") if isinstance(prev_report, dict) else [],
    )

    cache[str(server.id)] = current_report
    _save_templates_check_cache(cache)

    if diff.get("new"):
        _notify_template_gaps(server, diff["new"], current_report)

    return jsonify(
        {
            "ok": True,
            "server_id": server.id,
            "server_name": server.name,
            "current": current_report,
            "previous": prev_report,
            "diff": diff,
        }
    )


@admin_bp.route("/api/templates/schema", methods=["GET"])
@login_required
def api_admin_template_schema():
    """Возвращает schema_cache выбранного сервера для автодополнения ключей."""

    admin_required()
    server, err = _get_server_from_request()
    if err:
        return err

    data, message = fetch_template_schema(server)
    if data is None:
        return jsonify({"error": message or "не удалось загрузить схему"}), 502

    return jsonify(data)


@admin_bp.route("/api/templates/<path:template_name>", methods=["GET"])
@login_required
def api_admin_template_get(template_name: str):
    """Получить содержимое шаблона на конкретном сервере."""

    admin_required()
    server, err = _get_server_from_request()
    if err:
        return err

    data, message = fetch_template_payload(server, template_name)
    if data is None:
        return jsonify({"error": message or "не удалось загрузить шаблон"}), 502

    return jsonify(data)


@admin_bp.route("/api/templates/<path:template_name>", methods=["PUT"])
@login_required
def api_admin_template_put(template_name: str):
    """Сохранить/создать manage-шаблон на сервере."""

    admin_required()
    server, err = _get_server_from_request()
    if err:
        return err

    payload = request.get_json(silent=True) or {}
    steps = payload.get("steps")
    if not isinstance(steps, list):
        return jsonify({"error": "steps должен быть массивом"}), 400

    data, message = save_template_payload(server, template_name, steps)
    if data is None:
        return jsonify({"error": message or "не удалось сохранить шаблон"}), 502

    return jsonify(data)


@admin_bp.route("/api/templates/<path:template_name>", methods=["DELETE"])
@login_required
def api_admin_template_delete(template_name: str):
    """Удалить manage-шаблон на сервере."""

    admin_required()
    server, err = _get_server_from_request()
    if err:
        return err

    data, message = delete_template_payload(server, template_name)
    if data is None:
        return jsonify({"error": message or "не удалось удалить шаблон"}), 502

    return jsonify(data)


@admin_bp.route("/api/templates/<path:template_name>/rename", methods=["PATCH"])
@login_required
def api_admin_template_rename(template_name: str):
    """Переименовать manage-шаблон на сервере и обновить алиасы."""

    admin_required()
    server, err = _get_server_from_request()
    if err:
        return err

    payload = request.get_json(silent=True) or {}
    new_name = (payload.get("new_name") or "").strip()
    if not new_name:
        return jsonify({"error": "new_name обязателен"}), 400

    data, message = rename_template_payload(server, template_name, new_name)
    if data is None:
        return jsonify({"error": message or "не удалось переименовать"}), 502

    return jsonify(data)


@admin_bp.route("/payments/<int:account_id>/mark-paid", methods=["POST"])
@login_required
def mark_account_paid(account_id: int):
    admin_required()

    account = (
        Account.query.options(joinedload(Account.owner), joinedload(Account.server))
        .filter_by(id=account_id)
        .first()
    )
    if not account:
        return jsonify({"ok": False, "error": "account not found"}), 404

    base_date = account.next_payment_at.date() if account.next_payment_at else datetime.utcnow().date()
    next_date = base_date + timedelta(days=30)
    account.next_payment_at = datetime.combine(next_date, datetime.min.time())

    with settings_audit_context(
        account.owner,
        current_user,
        "account_toggle",
        {
            "account": account,
            "field": "account:IsActive",
            "old_value": account.is_active,
            "new_value": True,
            "reason": "mark_paid",
        },
    ) as audit_ctx:
        ok, msg = update_account_active(account, True)
        if not ok:
            audit_ctx["result"] = "failed"
            db.session.rollback()
            return (
                jsonify({"ok": False, "error": f"Не удалось включить ферму: {msg}"}),
                500,
            )

        account.is_active = True
        account.blocked_for_payment = False
        db.session.commit()
        audit_ctx["result"] = "ok"

    return jsonify(
        {
            "ok": True,
            "next_payment_at": account.next_payment_at.strftime("%Y-%m-%d"),
            "blocked_for_payment": account.blocked_for_payment,
            "is_active": account.is_active,
        }
    )


@admin_bp.route("/payments/<int:account_id>/mark-unpaid", methods=["POST"])
@login_required
def mark_account_unpaid(account_id: int):
    admin_required()

    account = (
        Account.query.options(joinedload(Account.owner), joinedload(Account.server))
        .filter_by(id=account_id)
        .first()
    )
    if not account:
        return jsonify({"ok": False, "error": "account not found"}), 404

    with settings_audit_context(
        account.owner,
        current_user,
        "account_toggle",
        {
            "account": account,
            "field": "account:IsActive",
            "old_value": account.is_active,
            "new_value": False,
            "reason": "mark_unpaid",
        },
    ) as audit_ctx:
        ok, msg = update_account_active(account, False)
        if not ok:
            audit_ctx["result"] = "failed"
            db.session.rollback()
            return (
                jsonify({"ok": False, "error": f"Не удалось выключить ферму: {msg}"}),
                500,
            )

        account.is_active = False
        account.blocked_for_payment = True
        db.session.commit()
        audit_ctx["result"] = "ok"

    return jsonify(
        {
            "ok": True,
            "blocked_for_payment": account.blocked_for_payment,
            "is_active": account.is_active,
        }
    )


@admin_bp.route("/info-message", methods=["GET", "POST"], endpoint="info_message_page")
@login_required
def info_message_page():
    """Управление общим сообщением «Инфо», отображаемым у всех клиентов."""

    admin_required()

    if request.method == "POST":
        new_message = request.form.get("info_message", "")
        set_global_info_message_text(new_message)
        flash("Сообщение для клиентов обновлено.", "success")
        return redirect(url_for("admin.info_message_page"))

    message = get_global_info_message()
    clients = User.query.filter_by(role="client").order_by(User.username.asc()).all()

    return render_template(
        "admin/info_message.html",
        info_message=message.message_text or "",
        info_message_updated_at=message.updated_at,
        clients=clients,
    )


# -------------------- Manage / настройки бота --------------------


@admin_bp.route("/manage", endpoint="manage")
@login_required
def manage():
    """Страница manage для админа с доступом ко всем фермам."""

    admin_required()

    from UsersDash.client_views import (
        _apply_visibility_to_steps,
        _build_manage_view_steps,
        _build_visibility_map,
        _extract_steps_and_menu,
    )

    accounts = (
        Account.query.options(
            joinedload(Account.server),
            joinedload(Account.owner),
        )
        .order_by(Account.is_active.desc(), Account.server_id.asc(), Account.name.asc())
        .all()
    )

    active_accounts_count = sum(1 for acc in accounts if acc.is_active)

    selected_account = None
    selected_id = request.args.get("account_id")
    if selected_id:
        try:
            selected_id_int = int(selected_id)
        except (TypeError, ValueError):
            selected_id_int = None
    else:
        selected_id_int = None

    for acc in accounts:
        if selected_id_int and acc.id == selected_id_int:
            selected_account = acc
            break
    if not selected_account:
        selected_account = next((acc for acc in accounts if acc.is_active), None) or (accounts[0] if accounts else None)

    view_steps = []
    steps_error = None
    raw_steps = []
    menu_data = None
    debug_info = None
    visibility_map = {}
    script_labels_map: dict[str, str] = {}
    selected_tariff_price = selected_account.next_payment_amount if selected_account else None
    selected_tariff_name = get_tariff_name_by_price(selected_tariff_price)
    selected_has_defaults = has_defaults_for_tariff(selected_tariff_price) if selected_account else False
    if selected_account:
        raw_settings = fetch_account_settings(selected_account)
        raw_steps, menu_data, debug_info = _extract_steps_and_menu(
            raw_settings, return_debug=True
        )
        visibility_map = _build_visibility_map(raw_steps)
        script_labels_map = _extract_script_labels_from_visibility(visibility_map)
        raw_steps = _apply_visibility_to_steps(raw_steps, visibility_map, is_admin=True)
        if raw_steps:
            view_steps = _build_manage_view_steps(
                raw_settings,
                steps_override=raw_steps,
                script_labels_map=script_labels_map,
            )
        else:
            steps_error = "Не удалось загрузить настройки этой фермы."

    return render_template(
        "admin/manage.html",
        accounts=accounts,
        selected_account=selected_account,
        view_steps=view_steps,
        raw_steps=raw_steps,
        visibility_map=visibility_map,
        script_labels_map=script_labels_map,
        menu_data=menu_data,
        steps_error=steps_error,
        debug_info=debug_info,
        active_accounts_count=active_accounts_count,
        selected_tariff_price=selected_tariff_price,
        selected_tariff_name=selected_tariff_name,
        selected_has_defaults=selected_has_defaults,
    )


def _parse_js_dict_constants(js_text: str, const_name: str) -> dict[str, str]:
    pattern = rf"const\\s+{const_name}\\s*=\\s*\\{{(.*?)\\}}\\s*;"
    match = re.search(pattern, js_text, re.S)
    if not match:
        return {}

    body = match.group(1)
    return {key: val for key, val in re.findall(r'"([^"]+)"\s*:\s*"([^"]*)"', body)}


def _parse_js_order_map(js_text: str) -> dict[str, list[str]]:
    pattern = r"const\\s+ORDER_MAP\\s*=\\s*\\{(.*?)\\}\\s*;"
    match = re.search(pattern, js_text, re.S)
    if not match:
        return {}

    body = match.group(1)
    result: dict[str, list[str]] = {}
    for block in re.finditer(r'"([^"]+)"\s*:\s*\[(.*?)\]', body, re.S):
        script_id = block.group(1)
        keys = [k for k in re.findall(r'"([^"]+)"', block.group(2))]
        result[script_id] = keys
    return result


def _load_manage_js_meta() -> dict:
    manage_js_path = os.path.join(os.path.dirname(__file__), "static", "js", "manage.js")
    try:
        with open(manage_js_path, "r", encoding="utf-8") as f:
            js_text = f.read()
    except OSError:
        return {"config_labels": {}, "script_labels": {}, "order_map": {}}

    return {
        "config_labels": _parse_js_dict_constants(js_text, "CONFIG_LABELS"),
        "script_labels": _parse_js_dict_constants(js_text, "SCRIPT_LABELS"),
        "order_map": _parse_js_order_map(js_text),
    }


def _load_studyfull_meta() -> dict:
    """Читает studyFULL.json и извлекает пары ScriptId/Config."""

    study_path = os.path.join(
        os.path.dirname(__file__), "bot_farm_configs", "studyFULL.json"
    )
    try:
        with open(study_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"scripts": {}, "items_count": 0, "steps_count": 0}

    scripts: dict[str, dict[str, set[str]]] = {}
    steps_count = 0

    for entry in raw_data if isinstance(raw_data, list) else []:
        data_raw = entry.get("Data")
        if not isinstance(data_raw, str):
            continue

        try:
            steps = json.loads(data_raw)
        except json.JSONDecodeError:
            continue

        if not isinstance(steps, list):
            continue

        for step in steps:
            if not isinstance(step, dict):
                continue

            script_id = step.get("ScriptId") or step.get("script_id")
            if not script_id:
                continue

            config_obj = step.get("Config") or step.get("config") or {}
            config_keys = set(config_obj.keys()) if isinstance(config_obj, dict) else set()
            scripts.setdefault(script_id, {"config_keys": set()})["config_keys"].update(
                config_keys
            )
            steps_count += 1

    return {
        "scripts": scripts,
        "items_count": len(raw_data) if isinstance(raw_data, list) else 0,
        "steps_count": steps_count,
    }


def _collect_server_step_meta():
    from UsersDash.client_views import _extract_steps_and_menu

    accounts = (
        Account.query.options(joinedload(Account.server))
        .filter(Account.is_active.is_(True))
        .order_by(Account.id.asc())
        .all()
    )

    for acc in accounts:
        raw_settings = fetch_account_settings(acc)
        steps, _ = _extract_steps_and_menu(raw_settings)
        if not steps:
            continue

        scripts: dict[str, dict[str, set[str]]] = {}
        for step in steps:
            script_id = step.get("ScriptId") or step.get("script_id")
            if not script_id:
                continue

            config_keys = set()
            config_obj = step.get("Config") or step.get("config") or {}
            if isinstance(config_obj, dict):
                config_keys.update(config_obj.keys())

            if script_id not in scripts:
                scripts[script_id] = {"config_keys": set()}
            scripts[script_id]["config_keys"].update(config_keys)

        return {"scripts": scripts, "sample_account": acc, "steps_count": len(steps)}

    return {"scripts": {}, "sample_account": None, "steps_count": 0}


def _collect_config_sources(manage_meta, server_meta, study_meta):
    sources: dict[str, dict[str, set[str]]] = {}

    def add_source(script_id: str, config_key: str, origin: str):
        scripts_map = sources.setdefault(script_id, {})
        scripts_map.setdefault(config_key, set()).add(origin)

    for script_id, cfg_keys in (manage_meta.get("order_map") or {}).items():
        for cfg_key in cfg_keys:
            add_source(script_id, cfg_key, "manage.js")

    for script_id, meta in (server_meta.get("scripts") or {}).items():
        for cfg_key in meta.get("config_keys", set()):
            add_source(script_id, cfg_key, "API")

    for script_id, meta in (study_meta.get("scripts") or {}).items():
        for cfg_key in meta.get("config_keys", set()):
            add_source(script_id, cfg_key, "studyFULL")

    return sources


def _extract_script_labels_from_visibility(visibility_map: dict) -> dict[str, str]:
    labels: dict[str, str] = {}

    for script_id, items in (visibility_map or {}).items():
        for item in items or []:
            if (
                item
                and item.get("config_key") == client_config_visibility.SCRIPT_LABEL_CONFIG_KEY
                and item.get("client_label")
            ):
                labels[script_id] = item["client_label"]

    return labels


def _build_visibility_rows(manage_meta, server_meta, study_meta, db_records):
    order_map = manage_meta.get("order_map") or {}
    script_labels = manage_meta.get("script_labels") or {}
    config_labels = manage_meta.get("config_labels") or {}
    server_scripts: dict = server_meta.get("scripts") or {}
    study_scripts: dict = study_meta.get("scripts") or {}
    sources_map = _collect_config_sources(manage_meta, server_meta, study_meta)

    script_label_records: dict[str, ClientConfigVisibility] = {}
    db_records_map: dict[tuple[str, str], ClientConfigVisibility] = {}
    for rec in db_records:
        if rec.config_key == client_config_visibility.SCRIPT_LABEL_CONFIG_KEY:
            script_label_records[rec.script_id] = rec
            continue
        db_records_map[(rec.script_id, rec.config_key)] = rec

    combined_records = client_config_visibility.merge_records_with_defaults(
        db_records, scope="global", script_ids=sources_map.keys()
    )
    records_map: dict[tuple[str, str], Any] = {}
    for rec in combined_records:
        if rec.config_key == client_config_visibility.SCRIPT_LABEL_CONFIG_KEY:
            continue
        key = (rec.script_id, rec.config_key)
        if key not in records_map:
            records_map[key] = rec

    scripts = set(order_map.keys()) | set(script_labels.keys()) | set(server_scripts.keys())
    scripts.update(study_scripts.keys())
    scripts.update(rec.script_id for rec in db_records)
    scripts.update(rec.script_id for rec in combined_records)

    rows = []
    for script_id in sorted(scripts):
        config_keys: set[str] = set(order_map.get(script_id, []))
        server_cfg = server_scripts.get(script_id) or {}
        study_cfg = study_scripts.get(script_id) or {}
        config_keys.update(server_cfg.get("config_keys", set()))
        config_keys.update(study_cfg.get("config_keys", set()))
        for rec in db_records:
            if (
                rec.script_id == script_id
                and rec.config_key != client_config_visibility.SCRIPT_LABEL_CONFIG_KEY
            ):
                config_keys.add(rec.config_key)

        for rec in combined_records:
            if (
                rec.script_id == script_id
                and rec.config_key != client_config_visibility.SCRIPT_LABEL_CONFIG_KEY
            ):
                config_keys.add(rec.config_key)

        if client_config_visibility.STEP_HIDDEN_KEY not in config_keys:
            config_keys.add(client_config_visibility.STEP_HIDDEN_KEY)

        script_label_rec = script_label_records.get(script_id)
        script_label = script_labels.get(script_id, script_id)
        script_label_from_db = False
        if script_label_rec and script_label_rec.client_label:
            script_label = script_label_rec.client_label
            script_label_from_db = True

        for config_key in sorted(config_keys):
            db_rec = db_records_map.get((script_id, config_key))
            record = records_map.get((script_id, config_key))
            order_idx = 0
            if record:
                order_idx = record.order_index or 0
            elif config_key in order_map.get(script_id, []):
                order_idx = order_map[script_id].index(config_key)

            source_labels = (sources_map.get(script_id) or {}).get(config_key, set())
            from_manage_js = "manage.js" in source_labels or config_key in config_labels

            default_label = config_labels.get(config_key, config_key)
            if config_key == client_config_visibility.STEP_HIDDEN_KEY:
                default_label = "Скрыть шаг"

            rows.append(
                {
                    "script_id": script_id,
                    "config_key": config_key,
                    "script_label": script_label,
                    "default_label": default_label,
                    "client_label": record.client_label if record else None,
                    "client_visible": record.client_visible if record else True,
                    "order_index": order_idx,
                    "from_js": from_manage_js,
                    "from_server": "API" in source_labels,
                    "from_studyfull": "studyFULL" in source_labels,
                    "has_db": db_rec is not None,
                    "script_label_from_db": script_label_from_db,
                    "script_label_from_js": script_id in script_labels,
                }
            )

    for idx, row in enumerate(rows):
        row["form_key"] = f"row{idx}"

    return rows


@admin_bp.route("/config-visibility", methods=["GET", "POST"])
@login_required
def config_visibility_matrix():
    admin_required()

    manage_meta = _load_manage_js_meta()
    server_meta = _collect_server_step_meta()
    study_meta = _load_studyfull_meta()
    db_records = ClientConfigVisibility.query.order_by(
        ClientConfigVisibility.script_id.asc(),
        ClientConfigVisibility.order_index.asc(),
        ClientConfigVisibility.config_key.asc(),
    ).all()

    rows = _build_visibility_rows(manage_meta, server_meta, study_meta, db_records)
    script_errors: dict[str, str] = {}
    script_ids = sorted({row["script_id"] for row in rows})

    if request.method == "POST":
        if request.form.get("action") == "refresh":
            flash("Источники перечитаны, матрица обновлена.", "success")
            return redirect(url_for("admin.config_visibility_matrix"))

        for script_id in script_ids:
            script_label_value = request.form.get(f"script_label::{script_id}", "")
            script_label_value = (script_label_value or "").strip() or None
            try:
                client_config_visibility.upsert_script_label(
                    script_id=script_id,
                    script_label=script_label_value,
                    scope="global",
                )
            except Exception:  # pragma: no cover - логирование ошибок
                current_app.logger.exception(
                    "Не удалось сохранить метку для скрипта %s", script_id
                )
                db.session.rollback()
                script_errors[script_id] = "Ошибка сохранения метки скрипта."

        for row in rows:
            prefix = row["form_key"]
            label = request.form.get(f"label::{prefix}", "").strip() or None
            order_raw = request.form.get(f"order::{prefix}")
            try:
                order_index = int(order_raw) if order_raw is not None else 0
            except (TypeError, ValueError):
                order_index = 0

            visible = request.form.get(f"visible::{prefix}") == "on"

            client_config_visibility.upsert_record(
                script_id=row["script_id"],
                config_key=row["config_key"],
                client_visible=visible,
                client_label=label,
                order_index=order_index,
                scope="global",
            )

        if script_errors:
            flash("Не все метки скриптов сохранены. Проверьте ошибки ниже.", "danger")
        else:
            flash("Настройки видимости сохранены.", "success")
            return redirect(url_for("admin.config_visibility_matrix"))

        db_records = ClientConfigVisibility.query.order_by(
            ClientConfigVisibility.script_id.asc(),
            ClientConfigVisibility.order_index.asc(),
            ClientConfigVisibility.config_key.asc(),
        ).all()
        rows = _build_visibility_rows(manage_meta, server_meta, study_meta, db_records)
        grouped_rows: dict[str, list[dict]] = {}
        for row in rows:
            grouped_rows.setdefault(row["script_id"], []).append(row)

        for script in grouped_rows:
            grouped_rows[script].sort(key=lambda r: (r.get("order_index", 0), r["config_key"]))

        return render_template(
            "admin/visibility_matrix.html",
            rows=rows,
            grouped_rows=grouped_rows,
            manage_meta=manage_meta,
            server_meta=server_meta,
            study_meta=study_meta,
            script_errors=script_errors,
        )

    grouped_rows: dict[str, list[dict]] = {}
    for row in rows:
        grouped_rows.setdefault(row["script_id"], []).append(row)

    for script in grouped_rows:
        grouped_rows[script].sort(key=lambda r: (r.get("order_index", 0), r["config_key"]))

    return render_template(
        "admin/visibility_matrix.html",
        rows=rows,
        grouped_rows=grouped_rows,
        manage_meta=manage_meta,
        server_meta=server_meta,
        study_meta=study_meta,
        script_errors=script_errors,
    )


# -------------------- Управление пользователями --------------------


@admin_bp.route("/users")
@login_required
def users_list():
    """
    Список всех пользователей (админы + клиенты).
    """
    admin_required()
    users = User.query.order_by(User.created_at.desc()).all()
    return render_template("admin/users.html", users=users)


@admin_bp.route("/users/create", methods=["GET", "POST"])
@login_required
def user_create():
    """
    Создание нового пользователя (клиента или админа).
    """
    admin_required()

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        role = request.form.get("role", "client")
        password = request.form.get("password", "")
        is_active = request.form.get("is_active") == "on"

        # Валидация
        if not username or not password:
            flash("Логин и пароль обязательны.", "danger")
            return render_template("admin/user_edit.html", user=None, is_new=True)

        existing = User.query.filter_by(username=username).first()
        if existing:
            flash("Пользователь с таким логином уже существует.", "danger")
            return render_template("admin/user_edit.html", user=None, is_new=True)

        # Создаём пользователя
        user = User(
            username=username,
            role=role,
            is_active=is_active,
            password_hash=generate_password_hash(password),
        )
        db.session.add(user)
        db.session.commit()

        flash("Пользователь успешно создан.", "success")
        return redirect(url_for("admin.users_list"))

    # GET — просто показать форму создания
    return render_template("admin/user_edit.html", user=None, is_new=True)


@admin_bp.route("/users/<int:user_id>/edit", methods=["GET", "POST"])
@login_required
def user_edit(user_id: int):
    """
    Редактирование существующего пользователя:
    - логин
    - роль
    - активность
    - при желании смена пароля
    """
    admin_required()

    user = User.query.get_or_404(user_id)

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        role = request.form.get("role", "client")
        is_active = request.form.get("is_active") == "on"
        new_password = request.form.get("new_password", "")

        if not username:
            flash("Логин не может быть пустым.", "danger")
            return render_template("admin/user_edit.html", user=user, is_new=False)

        # Проверка на уникальность логина
        existing = User.query.filter(
            User.username == username,
            User.id != user.id,
        ).first()
        if existing:
            flash("Пользователь с таким логином уже существует.", "danger")
            return render_template("admin/user_edit.html", user=user, is_new=False)

        user.username = username
        user.role = role
        user.is_active = is_active

        # Если введён новый пароль — хэшируем и сохраняем
        if new_password:
            user.password_hash = generate_password_hash(new_password)

        db.session.commit()
        flash("Изменения сохранены.", "success")
        return redirect(url_for("admin.users_list"))

    return render_template("admin/user_edit.html", user=user, is_new=False)


# -------------------- Управление серверами --------------------


@admin_bp.route("/servers")
@login_required
def servers_list():
    """
    Список серверов (F99, 208, DELL и т.д.).
    """
    admin_required()
    servers = Server.query.order_by(Server.name.asc()).all()
    return render_template("admin/servers.html", servers=servers)


@admin_bp.route("/servers/create", methods=["GET", "POST"])
@login_required
def server_create():
    """
    Создание нового серверного ПК.
    """
    admin_required()

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        host = request.form.get("host", "").strip()
        api_base_url = request.form.get("api_base_url", "").strip()
        description = request.form.get("description", "").strip()
        is_active = request.form.get("is_active") == "on"

        if not name or not host:
            flash("Имя сервера и host обязательны.", "danger")
            return render_template("admin/server_edit.html", server=None, is_new=True)

        existing = Server.query.filter_by(name=name).first()
        if existing:
            flash("Сервер с таким именем уже существует.", "danger")
            return render_template("admin/server_edit.html", server=None, is_new=True)

        server = Server(
            name=name,
            host=host,
            api_base_url=api_base_url or None,
            description=description or None,
            is_active=is_active,
        )
        db.session.add(server)
        db.session.commit()

        flash("Сервер успешно добавлен.", "success")
        return redirect(url_for("admin.servers_list"))

    return render_template("admin/server_edit.html", server=None, is_new=True)


@admin_bp.route("/servers/<int:server_id>/edit", methods=["GET", "POST"])
@login_required
def server_edit(server_id: int):
    """
    Редактирование существующего сервера.
    """
    admin_required()

    server = Server.query.get_or_404(server_id)

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        host = request.form.get("host", "").strip()
        api_base_url = request.form.get("api_base_url", "").strip()
        description = request.form.get("description", "").strip()
        is_active = request.form.get("is_active") == "on"

        if not name or not host:
            flash("Имя сервера и host обязательны.", "danger")
            return render_template("admin/server_edit.html", server=server, is_new=False)

        # Проверка уникальности имени
        existing = Server.query.filter(
            Server.name == name,
            Server.id != server.id,
        ).first()
        if existing:
            flash("Сервер с таким именем уже существует.", "danger")
            return render_template("admin/server_edit.html", server=server, is_new=False)

        server.name = name
        server.host = host
        server.api_base_url = api_base_url or None
        server.description = description or None
        server.is_active = is_active

        db.session.commit()
        flash("Изменения по серверу сохранены.", "success")
        return redirect(url_for("admin.servers_list"))

    return render_template("admin/server_edit.html", server=server, is_new=False)


# -------------------- Управление аккаунтами/фермами --------------------


@admin_bp.route("/accounts")
@login_required
def accounts_list():
    """
    Список всех аккаунтов/ферм.
    Админ видит всё, с указанием сервера и владельца.
    """
    admin_required()

    accounts = (
        Account.query
        .order_by(Account.created_at.desc())
        .all()
    )

    return render_template("admin/accounts.html", accounts=accounts)


@admin_bp.route("/accounts/create", methods=["GET", "POST"])
@login_required
def account_create():
    """
    Создание новой фермы/аккаунта:
    - имя
    - internal_id
    - сервер
    - владелец (клиент)
    """
    admin_required()

    servers = Server.query.order_by(Server.name.asc()).all()
    clients = User.query.filter_by(role="client").order_by(User.username.asc()).all()

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        internal_id = request.form.get("internal_id", "").strip()
        server_id = request.form.get("server_id")
        owner_id = request.form.get("owner_id")
        is_active = request.form.get("is_active") == "on"
        game_world = request.form.get("game_world", "").strip()
        notes = request.form.get("notes", "").strip()

        if not name or not internal_id or not server_id:
            flash("Имя, internal_id и сервер обязательны.", "danger")
            return render_template(
                "admin/account_edit.html",
                account=None,
                is_new=True,
                servers=servers,
                clients=clients,
            )

        # если владелец не выбран — создаём/берём клиента по имени фермы
        owner_user = None
        if owner_id:
            try:
                owner_user = User.query.filter_by(id=int(owner_id)).first()
            except (TypeError, ValueError):
                owner_user = None
        if owner_user is None:
            owner_user = _get_or_create_client_for_farm(name)

        account = Account(
            name=name,
            internal_id=internal_id,
            server_id=int(server_id),
            owner_id=owner_user.id,
            is_active=is_active,
            game_world=game_world or None,
            notes=notes or None,
        )
        db.session.add(account)
        db.session.commit()

        flash("Аккаунт успешно создан.", "success")
        return redirect(url_for("admin.accounts_list"))

    return render_template(
        "admin/account_edit.html",
        account=None,
        is_new=True,
        servers=servers,
        clients=clients,
    )


@admin_bp.route("/accounts/<int:account_id>/edit", methods=["GET", "POST"])
@login_required
def account_edit(account_id: int):
    """
    Редактирование существующей фермы/аккаунта.
    """
    admin_required()

    account = Account.query.get_or_404(account_id)
    servers = Server.query.order_by(Server.name.asc()).all()
    clients = User.query.filter_by(role="client").order_by(User.username.asc()).all()

    if request.method == "POST":
        old_owner_id = account.owner_id
        old_name = account.name

        name = request.form.get("name", "").strip()
        internal_id = request.form.get("internal_id", "").strip()
        server_id = request.form.get("server_id")
        owner_id = request.form.get("owner_id")
        is_active = request.form.get("is_active") == "on"
        game_world = request.form.get("game_world", "").strip()
        notes = request.form.get("notes", "").strip()

        if not name or not internal_id or not server_id:
            flash("Имя, internal_id и сервер обязательны.", "danger")
            return render_template(
                "admin/account_edit.html",
                account=account,
                is_new=False,
                servers=servers,
                clients=clients,
            )

        owner_user = None
        if owner_id:
            try:
                owner_user = User.query.filter_by(id=int(owner_id)).first()
            except (TypeError, ValueError):
                owner_user = None
        if owner_user is None:
            owner_user = _get_or_create_client_for_farm(name)

        account.name = name
        account.internal_id = internal_id
        account.server_id = int(server_id)
        account.owner_id = owner_user.id
        account.is_active = is_active
        account.game_world = game_world or None
        account.notes = notes or None

        owner_changed = old_owner_id != account.owner_id
        name_changed = old_name != account.name

        if owner_changed or name_changed:
            _merge_farmdata_for_move(
                old_owner_id,
                old_name,
                account.owner_id,
                account.name,
            )

        db.session.commit()
        flash("Изменения по аккаунту сохранены.", "success")
        return redirect(url_for("admin.accounts_list"))

    return render_template(
        "admin/account_edit.html",
        account=account,
        is_new=False,
        servers=servers,
        clients=clients,
    )


# ====================== ДАННЫЕ ФЕРМ (админ) ==========================

@admin_bp.route("/farm-data")
@login_required
def admin_farm_data():
    """
    Админская таблица 'Аккаунты / Данные':
    - показывает все фермы всех клиентов;
    - даёт возможность редактировать email/login/password/IGG/королевство;
    - даёт возможность менять дату следующей оплаты и тариф.
    """
    if current_user.role != "admin":
        abort(403)

    total_accounts = Account.query.count()

    return render_template(
        "admin/farm_data.html",
        tariff_price_map=TARIFF_PRICE_MAP,
        total_accounts=total_accounts,
    )


@admin_bp.route("/farm-data/chunk")
@login_required
def admin_farm_data_chunk():
    """Возвращает порцию данных ферм для ленивой подгрузки на клиенте."""

    if current_user.role != "admin":
        return jsonify({"ok": False, "error": "Access denied"}), 403

    total_accounts = Account.query.count()

    try:
        offset = max(0, int(request.args.get("offset", 0)))
    except ValueError:
        offset = 0

    try:
        raw_limit = int(request.args.get("limit", 200))
    except ValueError:
        raw_limit = 200

    limit = min(400, max(50, raw_limit))

    accounts = (
        Account.query
        .join(User, Account.owner_id == User.id)
        .join(Server, Account.server_id == Server.id)
        .order_by(User.username.asc(), Account.name.asc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    owner_ids = {acc.owner_id for acc in accounts}
    farmdata_entries = (
        FarmData.query
        .filter(FarmData.user_id.in_(owner_ids))
        .all()
    )
    fd_index = {(fd.user_id, fd.farm_name): fd for fd in farmdata_entries}

    items: list[dict[str, Any]] = []
    for acc in accounts:
        fd = fd_index.get((acc.owner_id, acc.name))
        items.append(
            {
                "account_id": acc.id,
                "owner_name": acc.owner.username if acc.owner else "—",
                "farm_name": acc.name,
                "server_bot": acc.server.name if acc.server else "—",
                "is_active": acc.is_active,
                "blocked_for_payment": acc.blocked_for_payment,
                "email": fd.email if fd else None,
                "password": fd.password if fd else None,
                "igg_id": fd.igg_id if fd else None,
                "server": fd.server if fd else None,
                "telegram_tag": fd.telegram_tag if fd else None,
                "next_payment_at": acc.next_payment_at.strftime("%Y-%m-%d")
                if acc.next_payment_at
                else None,
                "tariff": acc.next_payment_amount,
                "manage_url": url_for("admin.manage", account_id=acc.id),
            }
        )

    return jsonify(
        {
            "ok": True,
            "total": total_accounts,
            "offset": offset,
            "limit": limit,
            "items": items,
        }
    )


@admin_bp.route("/farm-data/autocreate-clients", methods=["POST"])
@login_required
def admin_farm_data_autocreate_clients():
    if current_user.role != "admin":
        return jsonify({"ok": False, "error": "Access denied"}), 403

    accounts = Account.query.order_by(Account.id.asc()).all()

    created_clients = 0
    reassigned_accounts = 0
    warnings: list[str] = []

    try:
        try:
            backup_path = backup_database("before_autocreate_clients")
            print(f"[farm-data autocreate] Backup created: {backup_path}")
        except Exception:
            warnings.append("Не удалось создать бэкап перед автосозданием клиентов.")
            traceback.print_exc()

        for acc in accounts:
            farm_name = (acc.name or "").strip()
            if not farm_name:
                continue

            owner_user, was_created = _get_or_create_client_for_farm(
                farm_name, return_created=True
            )
            if was_created:
                created_clients += 1

            if acc.owner_id != owner_user.id:
                _merge_farmdata_for_move(
                    acc.owner_id,
                    acc.name,
                    owner_user.id,
                    acc.name,
                )
                acc.owner_id = owner_user.id
                reassigned_accounts += 1

        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        print("[farm-data autocreate] ERROR:", exc)
        return jsonify({"ok": False, "error": "Ошибка при автосоздании клиентов"}), 500

    return jsonify(
        {
            "ok": True,
            "created_clients": created_clients,
            "reassigned_accounts": reassigned_accounts,
            "total_accounts": len(accounts),
            "warnings": warnings,
        }
    )


@admin_bp.route("/farm-data/save", methods=["POST"])
@login_required
def admin_farm_data_save():
    if current_user.role != "admin":
        return jsonify({"ok": False, "error": "Access denied"}), 403

    data = request.get_json(silent=True) or {}
    items = data.get("items") or []

    def parse_next_payment(raw_value):
        if not raw_value:
            return None
        value = raw_value.strip()
        if not value:
            return None
        formats = (
            "%Y-%m-%d",
            "%d.%m.%Y",
            "%d.%m.%y",
            "%d/%m/%Y",
            "%d/%m/%y",
            "%d-%m-%Y",
            "%d-%m-%y",
        )
        for fmt in formats:
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def parse_tariff(raw_value):
        if raw_value is None:
            return None
        if isinstance(raw_value, (int, float)):
            return int(raw_value)
        value = str(raw_value).strip()
        if not value:
            return None
        no_spaces = value.replace(" ", "")
        cleaned = no_spaces
        if "," in no_spaces or "." in no_spaces:
            if not re.fullmatch(r"\d{1,3}([.,]\d{3})+", no_spaces):
                return None
            cleaned = no_spaces.replace(",", "").replace(".", "")
        return int(cleaned) if cleaned.isdigit() else None

    warnings = []
    defaults_to_apply: list[tuple[Account, int]] = []
    defaults_results: list[dict[str, str]] = []

    for row in items:
        acc_id = int(row.get("account_id", 0))
        acc = Account.query.filter_by(id=acc_id).first()
        if not acc:
            continue

        fd = FarmData.query.filter_by(
            user_id=acc.owner_id, farm_name=acc.name
        ).first()

        if not fd:
            fd = FarmData(user_id=acc.owner_id, farm_name=acc.name)
            db.session.add(fd)

        fd.email = (row.get("email") or "").strip() or None
        fd.password = (row.get("password") or "").strip() or None
        fd.igg_id = (row.get("igg_id") or "").strip() or None
        fd.server = (row.get("server") or "").strip() or None
        fd.telegram_tag = (row.get("telegram_tag") or "").strip() or None

        # обновим тариф и оплату
        next_payment_raw = row.get("next_payment_date")
        if next_payment_raw:
            parsed_dt = parse_next_payment(next_payment_raw)
            if parsed_dt:
                acc.next_payment_at = parsed_dt
            else:
                warnings.append(
                    f"{acc.name}: не удалось распознать дату '{next_payment_raw}'"
                )
        else:
            acc.next_payment_at = None

        tariff_raw = row.get("tariff")
        if tariff_raw is None or str(tariff_raw).strip() == "":
            acc.next_payment_amount = None
        else:
            parsed_tariff = parse_tariff(tariff_raw)
            if parsed_tariff is not None:
                previous_tariff = acc.next_payment_amount
                acc.next_payment_amount = parsed_tariff
                if parsed_tariff != previous_tariff:
                    defaults_to_apply.append((acc, parsed_tariff))
            else:
                warnings.append(
                    f"{acc.name}: некорректное значение тарифа '{tariff_raw}'"
                )

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print("farm-data save error:", e)
        return jsonify({"ok": False, "error": str(e)})

    for acc, tariff_price in defaults_to_apply:
        tariff_label = get_tariff_name_by_price(tariff_price) or str(tariff_price)
        ok, msg = apply_defaults_for_account(acc, tariff_price=tariff_price)
        defaults_results.append(
            {
                "account": acc.name,
                "tariff": tariff_label,
                "ok": ok,
                "message": msg,
            }
        )
        if not ok:
            warnings.append(
                f"{acc.name}: не удалось применить настройки по умолчанию ({msg})"
            )
        else:
            print(
                f"[defaults] applied {tariff_label} for {acc.name}: {msg}".strip()
            )

    return jsonify({"ok": True, "warnings": warnings, "defaults_results": defaults_results})

@admin_bp.route("/farm-data/sync-preview", methods=["GET"])
@login_required
def admin_farm_data_sync_preview():
    """
    Предпросмотр синхронизации с RssV7.

    Для каждого активного сервера:
      - запрашиваем /api/accounts_meta_full;
      - сопоставляем с Account + FarmData;
      - собираем различия по полям.

    Возвращает JSON вида:
    {
      "ok": true,
      "changes": [
        {
          "server_id": 1,
          "server_name": "F99",
          "account_id": 123,
          "internal_id": "GUID_1",
          "farm_name": "Gugi1",
          "field": "email",
          "local": "old@example.com",
          "remote": "new@example.com"
        },
        ...
      ],
      "errors": ["..."]
    }
    """
    if current_user.role != "admin":
        return jsonify({"ok": False, "error": "Access denied"}), 403

    servers = (
        Server.query.filter(Server.is_active.is_(True)).order_by(Server.name).all()
    )

    changes: list[dict] = []
    errors: list[str] = []

    for srv in servers:
        remote_items, err = fetch_rssv7_accounts_meta(srv)
        if err:
            errors.append(f"{srv.name}: {err}")
            continue

        # Локальные аккаунты для этого сервера
        accounts = Account.query.filter_by(server_id=srv.id).all()
        acc_by_internal = {acc.internal_id: acc for acc in accounts if acc.internal_id}
        acc_by_name = {acc.name: acc for acc in accounts}

        # Подтягиваем FarmData пачкой
        farm_names = {acc.name for acc in accounts}
        fd_entries = (
            FarmData.query.filter(
                FarmData.farm_name.in_(farm_names)
            ).all()
        )
        fd_by_key = {(fd.user_id, fd.farm_name): fd for fd in fd_entries}

        for item in remote_items:
            internal_id = item.get("id") or item.get("internal_id")
            name = item.get("name")

            acc = None
            if internal_id and internal_id in acc_by_internal:
                acc = acc_by_internal[internal_id]
            elif name and name in acc_by_name:
                acc = acc_by_name[name]

            if not acc:
                # опционально можно добавить change типа "аккаунт не найден"
                continue

            fd = fd_by_key.get((acc.owner_id, acc.name))

            # Соберём локальные и удалённые значения в одном месте
            local = {
                "email": (fd.email if fd and fd.email else ""),
                "password": (fd.password if fd and fd.password else ""),
                "igg_id": (fd.igg_id if fd and fd.igg_id else ""),
                "server": (fd.server if fd and fd.server else ""),
                "telegram": (fd.telegram_tag if fd and fd.telegram_tag else ""),
                "next_payment_at": acc.next_payment_at.strftime("%Y-%m-%d")
                if acc.next_payment_at
                else "",
                "tariff": acc.next_payment_amount
                if acc.next_payment_amount is not None
                else "",
            }

            remote = {
                "email": item.get("email") or "",
                "password": item.get("passwd") or "",
                "igg_id": item.get("igg") or "",
                "server": item.get("server") or "",
                "telegram": item.get("tg_tag") or "",
                "next_payment_at": item.get("pay_until") or "",
                "tariff": item.get("tariff_rub") if item.get("tariff_rub") is not None else "",
            }

            # Какие поля сравниваем
            fields = [
                "email",
                "password",
                "igg_id",
                "server",
                "telegram",
                "next_payment_at",
                "tariff",
            ]

            for field in fields:
                lv = str(local.get(field) or "")
                rv = str(remote.get(field) or "")
                if lv == rv:
                    continue

                changes.append({
                    "server_id": srv.id,
                    "server_name": srv.name,
                    "account_id": acc.id,
                    "internal_id": acc.internal_id,
                    "farm_name": acc.name,
                    "field": field,
                    "local": lv,
                    "remote": rv,
                })

    return jsonify({"ok": True, "changes": changes, "errors": errors})

@admin_bp.route("/farm-data/sync-apply", methods=["POST"])
@login_required
def admin_farm_data_sync_apply():
    """
    Применяет изменения, выбранные в модалке синхронизации.

    Ожидает JSON:
    {
      "changes": [
        {
          "account_id": 123,
          "field": "email",
          "remote": "new@example.com"
        },
        ...
      ]
    }

    Считаем, что направление синхронизации здесь: RssV7 -> UsersDash
    (то есть всегда применяем значение "remote").
    """
    if current_user.role != "admin":
        return jsonify({"ok": False, "error": "Access denied"}), 403

    payload = request.get_json(silent=True) or {}
    items = payload.get("changes") or []
    if not isinstance(items, list) or not items:
        return jsonify({"ok": False, "error": "Нет изменений для применения"}), 400

    try:
        for row in items:
            acc_id = row.get("account_id")
            field = row.get("field")
            remote_val = (row.get("remote") or "").strip()

            try:
                acc_id_int = int(acc_id)
            except (TypeError, ValueError):
                continue

            acc = Account.query.filter_by(id=acc_id_int).first()
            if not acc:
                continue

            fd = FarmData.query.filter_by(
                user_id=acc.owner_id,
                farm_name=acc.name
            ).first()
            if not fd:
                fd = FarmData(user_id=acc.owner_id, farm_name=acc.name)
                db.session.add(fd)

            # В зависимости от поля, обновляем FarmData или Account
            if field == "email":
                fd.email = remote_val or None
            elif field == "password":
                fd.password = remote_val or None
            elif field == "igg_id":
                fd.igg_id = remote_val or None
            elif field == "server":
                fd.server = remote_val or None
            elif field == "telegram":
                fd.telegram_tag = remote_val or None
            elif field == "next_payment_at":
                # ожидаем формат YYYY-MM-DD
                if remote_val:
                    try:
                        acc.next_payment_at = datetime.strptime(remote_val, "%Y-%m-%d").date()
                    except ValueError:
                        # если дата невалидная — просто пропускаем это поле
                        pass
                else:
                    acc.next_payment_at = None
            elif field == "tariff":
                if remote_val:
                    try:
                        acc.next_payment_amount = int(remote_val)
                    except ValueError:
                        pass
                else:
                    acc.next_payment_amount = None

        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        print("[farm-data sync-apply] ERROR:", exc)
        return jsonify({"ok": False, "error": "Ошибка при применении изменений"}), 500

    return jsonify({"ok": True})


@admin_bp.route("/farm-data/pull-preview", methods=["GET"])
@login_required
def admin_farm_data_pull_preview():
    """
    Запрашивает все аккаунты со всех активных серверов RssV7 и возвращает
    плоский список для предпросмотра импорта в UsersDash.

    Формат ответа:
    {
      "ok": true,
      "items": [
        {
          "server_id": 1,
          "server_name": "F99",
          "account_id": 123,               # None, если не нашли локально
          "owner_name": "client1",
          "farm_name": "FarmA",
          "internal_id": "GUID",
          "remote": {...},                # email/password/igg/server/telegram/next_payment_at/tariff
          "local": {...},                 # те же поля, но из UsersDash
          "can_apply": true/false
        }, ...
      ],
      "errors": ["..."]
    }
    """
    if current_user.role != "admin":
        return jsonify({"ok": False, "error": "Access denied"}), 403

    servers = (
        Server.query.filter(Server.is_active.is_(True)).order_by(Server.name).all()
    )

    items: list[dict] = []
    errors: list[str] = []

    for srv in servers:
        remote_items, err = fetch_rssv7_accounts_meta(srv)
        if err:
            errors.append(f"{srv.name}: {err}")
            continue

        accounts = Account.query.filter_by(server_id=srv.id).all()
        acc_by_internal = {acc.internal_id: acc for acc in accounts if acc.internal_id}
        acc_by_name = {acc.name: acc for acc in accounts}

        farm_names = {acc.name for acc in accounts}
        fd_entries = (
            FarmData.query.filter(
                FarmData.farm_name.in_(farm_names)
            ).all()
        )
        fd_by_key = {(fd.user_id, fd.farm_name): fd for fd in fd_entries}

        for item in remote_items:
            internal_id = item.get("id") or item.get("internal_id")
            name = item.get("name")

            acc = None
            if internal_id and internal_id in acc_by_internal:
                acc = acc_by_internal[internal_id]
            elif name and name in acc_by_name:
                acc = acc_by_name[name]

            fd = None
            owner_name = ""
            if acc:
                fd = fd_by_key.get((acc.owner_id, acc.name))
                owner_name = acc.owner.username if acc.owner else ""

            local_data = {
                "email": (fd.email if fd and fd.email else ""),
                "password": (fd.password if fd and fd.password else ""),
                "igg_id": (fd.igg_id if fd and fd.igg_id else ""),
                "server": (fd.server if fd and fd.server else ""),
                "telegram": (fd.telegram_tag if fd and fd.telegram_tag else ""),
                "next_payment_at": acc.next_payment_at.strftime("%Y-%m-%d")
                if acc and acc.next_payment_at
                else "",
                "tariff": acc.next_payment_amount
                if acc and acc.next_payment_amount is not None
                else "",
            }

            remote_data = {
                "email": item.get("email") or "",
                "password": item.get("passwd") or "",
                "igg_id": item.get("igg") or "",
                "server": item.get("server") or "",
                "telegram": item.get("tg_tag") or "",
                "next_payment_at": item.get("pay_until") or "",
                "tariff": item.get("tariff_rub") if item.get("tariff_rub") is not None else "",
            }

            items.append(
                {
                    "server_id": srv.id,
                    "server_name": srv.name,
                    "account_id": acc.id if acc else None,
                    "owner_name": owner_name,
                    "farm_name": name or "",
                    "internal_id": internal_id or "",
                    "remote": remote_data,
                    "local": local_data,
                    "can_apply": True,  # даём выбрать и существующие, и новые
                    "is_new": acc is None,
                }
            )

    return jsonify({"ok": True, "items": items, "errors": errors})


@admin_bp.route("/farm-data/pull-apply", methods=["POST"])
@login_required
def admin_farm_data_pull_apply():
    """
    Применяет данные, полученные через pull-preview.

    Ожидает JSON:
    {
      "items": [
        {
          "account_id": 123,
          "email": "...",
          "password": "...",
          "igg_id": "...",
          "server": "...",
          "telegram": "...",
          "next_payment_at": "YYYY-MM-DD",
          "tariff": 900
        },
        ...
      ]
    }
    """
    if current_user.role != "admin":
        return jsonify({"ok": False, "error": "Access denied"}), 403

    payload = request.get_json(silent=True) or {}
    rows = payload.get("items") or []
    if not isinstance(rows, list) or not rows:
        return jsonify({"ok": False, "error": "Нет данных для применения"}), 400

    updated = 0
    warnings: list[str] = []

    try:
        try:
            backup_path = backup_database("before_pull_apply")
            print(f"[farm-data pull-apply] Backup created: {backup_path}")
        except Exception:
            warnings.append("Не удалось создать бэкап перед применением.")
            traceback.print_exc()

        for row in rows:
            acc_id = row.get("account_id")
            acc: Account | None = None
            is_new = bool(row.get("is_new"))

            if acc_id is not None:
                try:
                    acc_id_int = int(acc_id)
                except (TypeError, ValueError):
                    acc_id_int = None

                if acc_id_int is not None:
                    acc = Account.query.filter_by(id=acc_id_int).first()

            # Уже существующие аккаунты не перезаписываем в рамках pull-apply
            if acc is not None and not is_new:
                warnings.append(
                    f"{acc.name}: уже есть в UsersDash, пропущен при импорте"
                )
                continue

            # Если не нашли аккаунт — создаём новый на указанном сервере
            if acc is None:
                srv_id = row.get("server_id")
                try:
                    srv_id_int = int(srv_id)
                except (TypeError, ValueError):
                    srv_id_int = None

                farm_name = (row.get("farm_name") or "").strip()
                if not farm_name or srv_id_int is None:
                    warnings.append("Пропущена строка без сервера или имени фермы")
                    continue

                server_obj = Server.query.filter_by(id=srv_id_int).first()
                if not server_obj:
                    warnings.append(f"Сервер id={srv_id_int} не найден — строка пропущена")
                    continue

                owner_user = _get_or_create_client_for_farm(farm_name)
                acc = Account(
                    name=farm_name,
                    server_id=server_obj.id,
                    owner_id=owner_user.id,
                    internal_id=(row.get("internal_id") or "").strip() or None,
                    is_active=True,
                )
                db.session.add(acc)

            fd = FarmData.query.filter_by(
                user_id=acc.owner_id,
                farm_name=acc.name
            ).first()
            if not fd:
                fd = FarmData(user_id=acc.owner_id, farm_name=acc.name)
                db.session.add(fd)

            fd.email = (row.get("email") or "").strip() or None
            fd.password = (row.get("password") or "").strip() or None
            fd.igg_id = (row.get("igg_id") or "").strip() or None
            fd.server = (row.get("server") or "").strip() or None
            fd.telegram_tag = (row.get("telegram") or "").strip() or None

            pay_until_raw = (row.get("next_payment_at") or "").strip()
            if pay_until_raw:
                try:
                    acc.next_payment_at = datetime.strptime(pay_until_raw, "%Y-%m-%d").date()
                except ValueError:
                    warnings.append(
                        f"{acc.name}: не удалось разобрать дату '{pay_until_raw}'"
                    )
            else:
                acc.next_payment_at = None

            tariff_raw = row.get("tariff")
            if tariff_raw in ("", None):
                acc.next_payment_amount = None
            else:
                try:
                    acc.next_payment_amount = int(tariff_raw)
                except (TypeError, ValueError):
                    warnings.append(
                        f"{acc.name}: некорректное значение тарифа '{tariff_raw}'"
                    )

            updated += 1

        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        print("[farm-data pull-apply] ERROR:", exc)
        return jsonify({"ok": False, "error": "Ошибка при применении данных"}), 500

    return jsonify({"ok": True, "updated": updated, "warnings": warnings})


# -------------------- Лог настроек --------------------


def _parse_date_param(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _export_settings_log(entries, export_format: str) -> Response:
    headers = {
        "Content-Disposition": f"attachment; filename=settings_log.{export_format}",
    }

    if export_format == "json":
        payload = [
            {
                "id": log.id,
                "user": log.user.username if log.user else None,
                "actor": log.actor.username if log.actor else None,
                "action_type": log.action_type,
                "field_name": log.field_name,
                "old_value": log.old_value,
                "new_value": log.new_value,
                "created_at": log.created_at.isoformat() if log.created_at else None,
            }
            for log in entries
        ]
        return Response(json.dumps(payload, ensure_ascii=False), mimetype="application/json", headers=headers)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "user", "actor", "action", "field", "old", "new", "created_at"])
    for log in entries:
        writer.writerow(
            [
                log.id,
                log.user.username if log.user else "—",
                log.actor.username if log.actor else "—",
                log.action_type,
                log.field_name or "",
                (log.old_value or "").replace("\n", " "),
                (log.new_value or "").replace("\n", " "),
                log.created_at,
            ]
        )

    return Response(output.getvalue(), mimetype="text/csv", headers=headers)


def _safe_parse_json(value: str | None):
    if value is None:
        return None

    try:
        return json.loads(value)
    except Exception:
        return value


def _format_diff_value(value: Any) -> str:
    if value is None:
        return "—"

    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False, indent=2)
        except Exception:
            return str(value)

    return str(value)


def _collect_diff_changes(old_value: Any, new_value: Any) -> list[dict[str, str]]:
    if isinstance(old_value, dict) and isinstance(new_value, dict):
        keys = sorted(set(old_value.keys()) | set(new_value.keys()))
        return [
            {
                "label": key,
                "old": _format_diff_value(old_value.get(key)),
                "new": _format_diff_value(new_value.get(key)),
            }
            for key in keys
            if old_value.get(key) != new_value.get(key)
        ]

    if old_value != new_value:
        return [
            {
                "label": "Значение",
                "old": _format_diff_value(old_value),
                "new": _format_diff_value(new_value),
            }
        ]

    return []


@admin_bp.route("/settings-log")
@login_required
def settings_log():
    admin_required()

    page = max(1, int(request.args.get("page", 1)))
    per_page = min(100, int(request.args.get("per_page", 25)))
    action_type = request.args.get("action")
    field_query = request.args.get("field")
    user_id = request.args.get("user_id")
    actor_id = request.args.get("actor_id")
    start_date = _parse_date_param(request.args.get("start"))
    end_date = _parse_date_param(request.args.get("end"))
    export_format = request.args.get("format")
    search_term = request.args.get("search")
    sort_dir = request.args.get("sort", "desc")

    query = SettingsAuditLog.query.options(
        joinedload(SettingsAuditLog.user),
        joinedload(SettingsAuditLog.actor),
        joinedload(SettingsAuditLog.account),
    )

    if action_type:
        query = query.filter(SettingsAuditLog.action_type == action_type)
    if field_query:
        query = query.filter(SettingsAuditLog.field_name.ilike(f"%{field_query}%"))
    if user_id:
        query = query.filter(SettingsAuditLog.user_id == user_id)
    if actor_id:
        query = query.filter(SettingsAuditLog.actor_id == actor_id)
    if start_date:
        query = query.filter(SettingsAuditLog.created_at >= start_date)
    if end_date:
        query = query.filter(SettingsAuditLog.created_at <= end_date)
    if search_term:
        query = query.filter(SettingsAuditLog.new_value.ilike(f"%{search_term}%"))

    order = SettingsAuditLog.created_at.asc() if sort_dir == "asc" else SettingsAuditLog.created_at.desc()
    query = query.order_by(order)

    if export_format in {"csv", "json"}:
        entries = query.all()
        return _export_settings_log(entries, export_format)

    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    return render_template(
        "admin/settings_audit_log.html",
        logs=pagination.items,
        pagination=pagination,
        filters={
            "action": action_type,
            "field": field_query,
            "user_id": user_id,
            "actor_id": actor_id,
            "start": request.args.get("start"),
            "end": request.args.get("end"),
            "search": search_term,
            "sort": sort_dir,
        },
    )


@admin_bp.route("/settings-log/<int:log_id>/diff")
@login_required
def settings_log_diff(log_id: int):
    admin_required()

    log_entry = SettingsAuditLog.query.get_or_404(log_id)
    old_raw = log_entry.old_value
    new_raw = log_entry.new_value

    old_val = (old_raw or "").split("\n")
    new_val = (new_raw or "").split("\n")
    diff_lines = list(
        difflib.unified_diff(
            old_val,
            new_val,
            fromfile="old",
            tofile="new",
            lineterm="",
        )
    )

    old_parsed = _safe_parse_json(old_raw)
    new_parsed = _safe_parse_json(new_raw)
    changes = _collect_diff_changes(old_parsed, new_parsed)

    return jsonify({
        "diff": "\n".join(diff_lines),
        "id": log_entry.id,
        "field_name": log_entry.field_name,
        "account": {
            "id": log_entry.account.id,
            "name": log_entry.account.name,
        }
        if log_entry.account
        else None,
        "old_value": _format_diff_value(old_parsed),
        "new_value": _format_diff_value(new_parsed),
        "changes": changes,
    })
