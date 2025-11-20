# admin_views.py
# Маршруты админ-панели: общий дашборд, пользователи, сервера, фермы.
# Здесь только админская логика, доступная пользователям с role='admin'.

import re
from datetime import datetime

from flask import (
    Blueprint,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_login import current_user, login_required
from werkzeug.security import generate_password_hash

from models import Account, FarmData, Server, User, db
from services.remote_api import fetch_rssv7_accounts_meta



admin_bp = Blueprint("admin", __name__)


def admin_required():
    """
    Простая проверка, что текущий пользователь — администратор.
    Если нет — возвращаем 403 (доступ запрещён).
    """
    if not current_user.is_authenticated or current_user.role != "admin":
        abort(403)


def _get_unassigned_user():
    """
    Возвращает "служебного" пользователя для новых импортированных ферм,
    у которых пока не выбран клиент. Если его нет — создаёт неактивного
    клиента с предсказуемым логином и паролем.
    """
    placeholder = User.query.filter_by(username="unassigned").first()
    if placeholder:
        return placeholder

    placeholder = User(
        username="unassigned",
        role="client",
        is_active=False,
        password_hash=generate_password_hash("changeme"),
    )
    db.session.add(placeholder)
    db.session.commit()
    return placeholder


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

    return render_template(
        "admin/dashboard.html",
        total_users=total_users,
        total_clients=total_clients,
        total_admins=total_admins,
        total_accounts=total_accounts,
        total_servers=total_servers,
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

        if not name or not internal_id or not server_id or not owner_id:
            flash("Имя, internal_id, сервер и владелец обязательны.", "danger")
            return render_template(
                "admin/account_edit.html",
                account=None,
                is_new=True,
                servers=servers,
                clients=clients,
            )

        account = Account(
            name=name,
            internal_id=internal_id,
            server_id=int(server_id),
            owner_id=int(owner_id),
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
        name = request.form.get("name", "").strip()
        internal_id = request.form.get("internal_id", "").strip()
        server_id = request.form.get("server_id")
        owner_id = request.form.get("owner_id")
        is_active = request.form.get("is_active") == "on"
        game_world = request.form.get("game_world", "").strip()
        notes = request.form.get("notes", "").strip()

        if not name or not internal_id or not server_id or not owner_id:
            flash("Имя, internal_id, сервер и владелец обязательны.", "danger")
            return render_template(
                "admin/account_edit.html",
                account=account,
                is_new=False,
                servers=servers,
                clients=clients,
            )

        account.name = name
        account.internal_id = internal_id
        account.server_id = int(server_id)
        account.owner_id = int(owner_id)
        account.is_active = is_active
        account.game_world = game_world or None
        account.notes = notes or None

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
        placeholder_user = _get_unassigned_user()

        for row in rows:
            acc_id = row.get("account_id")
            acc: Account | None = None

            if acc_id is not None:
                try:
                    acc_id_int = int(acc_id)
                except (TypeError, ValueError):
                    acc_id_int = None

                if acc_id_int is not None:
                    acc = Account.query.filter_by(id=acc_id_int).first()

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

                acc = Account(
                    name=farm_name,
                    server_id=server_obj.id,
                    owner_id=placeholder_user.id,
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
