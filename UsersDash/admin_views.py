# admin_views.py
# Маршруты админ-панели: общий дашборд, пользователи, сервера, фермы.
# Здесь только админская логика, доступная пользователям с role='admin'.

import os
import re
import csv
import io
import json
import difflib
from datetime import datetime
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
from UsersDash.services.db_backup import backup_database
from UsersDash.services import client_config_visibility
from UsersDash.services.remote_api import (
    fetch_account_settings,
    fetch_resources_for_accounts,
    fetch_rssv7_accounts_meta,
)
from UsersDash.services.tariffs import TARIFF_PRICE_MAP
from UsersDash.services.info_message import (
    get_global_info_message,
    set_global_info_message_text,
)



admin_bp = Blueprint("admin", __name__)


def admin_required():
    """
    Простая проверка, что текущий пользователь — администратор.
    Если нет — возвращаем 403 (доступ запрещён).
    """
    if not current_user.is_authenticated or current_user.role != "admin":
        abort(403)


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

    return render_template(
        "admin/dashboard.html",
        total_users=total_users,
        total_clients=total_clients,
        total_admins=total_admins,
        total_accounts=total_accounts,
        total_servers=total_servers,
        accounts_data=accounts_data,
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
    if selected_account:
        raw_settings = fetch_account_settings(selected_account)
        raw_steps, menu_data, debug_info = _extract_steps_and_menu(
            raw_settings, return_debug=True
        )
        visibility_map = _build_visibility_map(raw_steps)
        raw_steps = _apply_visibility_to_steps(raw_steps, visibility_map, is_admin=True)
        if raw_steps:
            view_steps = _build_manage_view_steps(
                raw_settings, steps_override=raw_steps
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
        menu_data=menu_data,
        steps_error=steps_error,
        debug_info=debug_info,
        active_accounts_count=active_accounts_count,
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


def _build_visibility_rows(manage_meta, server_meta, db_records):
    order_map = manage_meta.get("order_map") or {}
    script_labels = manage_meta.get("script_labels") or {}
    config_labels = manage_meta.get("config_labels") or {}
    server_scripts: dict = server_meta.get("scripts") or {}

    db_records_map: dict[tuple[str, str], ClientConfigVisibility] = {}
    for rec in db_records:
        db_records_map[(rec.script_id, rec.config_key)] = rec

    combined_records = client_config_visibility.merge_records_with_defaults(
        db_records, scope="global"
    )
    records_map: dict[tuple[str, str], Any] = {}
    for rec in combined_records:
        key = (rec.script_id, rec.config_key)
        if key not in records_map:
            records_map[key] = rec

    scripts = set(order_map.keys()) | set(script_labels.keys()) | set(server_scripts.keys())
    scripts.update(rec.script_id for rec in db_records)
    scripts.update(rec.script_id for rec in combined_records)

    rows = []
    for script_id in sorted(scripts):
        config_keys: set[str] = set(order_map.get(script_id, []))
        server_cfg = server_scripts.get(script_id) or {}
        config_keys.update(server_cfg.get("config_keys", set()))
        for rec in db_records:
            if rec.script_id == script_id:
                config_keys.add(rec.config_key)

        for config_key in sorted(config_keys):
            db_rec = db_records_map.get((script_id, config_key))
            record = records_map.get((script_id, config_key))
            order_idx = 0
            if record:
                order_idx = record.order_index or 0
            elif config_key in order_map.get(script_id, []):
                order_idx = order_map[script_id].index(config_key)

            rows.append(
                {
                    "script_id": script_id,
                    "config_key": config_key,
                    "script_label": script_labels.get(script_id, script_id),
                    "default_label": config_labels.get(config_key, config_key),
                    "client_label": record.client_label if record else None,
                    "client_visible": record.client_visible if record else True,
                    "order_index": order_idx,
                    "from_js": config_key in config_labels,
                    "from_server": config_key in (server_cfg.get("config_keys") or set()),
                    "has_db": db_rec is not None,
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
    db_records = ClientConfigVisibility.query.order_by(
        ClientConfigVisibility.script_id.asc(),
        ClientConfigVisibility.order_index.asc(),
        ClientConfigVisibility.config_key.asc(),
    ).all()

    rows = _build_visibility_rows(manage_meta, server_meta, db_records)

    if request.method == "POST":
        for row in rows:
            prefix = row["form_key"]
            label = request.form.get(f"label::{prefix}", "").strip() or None
            order_raw = request.form.get(f"order::{prefix}")
            try:
                order_index = int(order_raw) if order_raw is not None else 0
            except (TypeError, ValueError):
                order_index = 0

            visible = request.form.get(f"visible::{prefix}") == "on"

            existing = next(
                (
                    rec
                    for rec in db_records
                    if rec.script_id == row["script_id"]
                    and rec.config_key == row["config_key"]
                ),
                None,
            )

            client_config_visibility.upsert_record(
                script_id=row["script_id"],
                config_key=row["config_key"],
                client_visible=visible,
                client_label=label,
                order_index=order_index,
                scope="global",
            )

        flash("Настройки видимости сохранены.", "success")
        return redirect(url_for("admin.config_visibility_matrix"))

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

    # Получаем все аккаунты, вместе с их владельцами и серверами
    accounts = (
        Account.query
        .join(User, Account.owner_id == User.id)
        .join(Server, Account.server_id == Server.id)
        .order_by(User.username.asc(), Account.name.asc())
        .all()
    )

    # Подтягиваем все FarmData для владельцев этих аккаунтов
    owner_ids = {acc.owner_id for acc in accounts}
    farmdata_entries = (
        FarmData.query
        .filter(FarmData.user_id.in_(owner_ids))
        .all()
    )

    # Индекс FarmData по (user_id, farm_name)
    fd_index = {(fd.user_id, fd.farm_name): fd for fd in farmdata_entries}

    # Собираем удобную структуру для шаблона
    items = []
    for acc in accounts:
        owner = acc.owner
        fd = fd_index.get((acc.owner_id, acc.name))
        items.append({
            "account": acc,
            "owner": owner,
            "farmdata": fd,
        })

    return render_template(
        "admin/farm_data.html",
        items=items,
        tariff_price_map=TARIFF_PRICE_MAP,
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
                acc.next_payment_amount = parsed_tariff
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

    return jsonify({"ok": True, "warnings": warnings})

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
    old_val = (log_entry.old_value or "").split("\n")
    new_val = (log_entry.new_value or "").split("\n")
    diff_lines = list(
        difflib.unified_diff(
            old_val,
            new_val,
            fromfile="old",
            tofile="new",
            lineterm="",
        )
    )

    return jsonify({
        "diff": "\n".join(diff_lines),
        "id": log_entry.id,
    })
