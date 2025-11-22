# client_views.py
# Маршруты клиентской части (UsersDash):
# - /dashboard                       — список ферм + ресурсы
# - /account/<id>/settings           — настройки фермы (мини-manage)
# - /account/<id>/settings/step/...  — AJAX-тоггл шагов
# - /account/<id>/refresh            — AJAX-обновление ресурсов по ферме

from flask import (
    Blueprint,
    render_template,
    abort,
    jsonify,
    request,
    redirect,
    url_for,
    g,
)
from flask_login import login_required, current_user
from sqlalchemy.orm import joinedload
from typing import Any
from UsersDash.models import Account, FarmData, db
from UsersDash.services.farmdata_status import collect_farmdata_status
from UsersDash.services.remote_api import (
    fetch_resources_for_accounts,
    fetch_account_settings,
    update_account_step_settings,
)

client_bp = Blueprint("client", __name__, url_prefix="")


def _extract_steps_and_menu(raw_settings):
    """Returns steps list and menu data from manage payload with fallbacks.

    Rssv7 может возвращать данные как в поле "Data", так и в нижнем регистре
    "data". Также некоторые окружения могут присылать сразу список шагов без
    обёртки. Чтобы интерфейс всегда получал настройки, поддерживаем все эти
    варианты.
    """

    def _safe_menu(data: Any, fallback: Any = None):
        menu = (
            (data.get("MenuData") if isinstance(data, dict) else None)
            or (data.get("menu") if isinstance(data, dict) else None)
            or (data.get("menu_data") if isinstance(data, dict) else None)
            or fallback
            or {}
        )
        return menu if isinstance(menu, dict) else {}

    def _safe_steps(val: Any):
        """Возвращает список шагов независимо от структуры.

        Поддерживает варианты:
        - список шагов;
        - словарь с полем Data/data/steps;
        - словарь с числовыми ключами ("0", "1", ...);
        - объект одного шага (если прилетел без списка).
        """

        # 1) Уже список
        if isinstance(val, list):
            return val

        # 2) Один шаг без обёртки
        if isinstance(val, dict) and (
            "Config" in val
            or "config" in val
            or "ScriptId" in val
            or "script_id" in val
        ):
            return [val]

        if isinstance(val, dict):
            nested = val.get("Data") or val.get("data") or val.get("steps") or val.get("Steps")

            # 3) Data — сразу список
            if isinstance(nested, list):
                return nested

            # 4) Data — словарь с числовыми ключами
            if isinstance(nested, dict):
                keys = list(nested.keys())
                if keys and all(str(k).isdigit() for k in keys):
                    return [nested[k] for k in sorted(nested.keys(), key=lambda x: int(x))]

            # 5) Сам объект — словарь с числовыми ключами
            keys = list(val.keys())
            if keys and all(str(k).isdigit() for k in keys):
                return [val[k] for k in sorted(val.keys(), key=lambda x: int(x))]

        return []

    if isinstance(raw_settings, list):
        return raw_settings, {}

    if isinstance(raw_settings, dict):
        primary = raw_settings.get("Data") or raw_settings.get("data") or raw_settings

        # В некоторых окружениях Data может быть объектом, в котором снова лежит Data/MenuData
        if isinstance(primary, dict) and not isinstance(primary, list):
            steps = _safe_steps(primary)
            menu_data = _safe_menu(primary, _safe_menu(raw_settings))
            if not steps:
                # Может быть ещё один уровень вложенности Data -> {Data: [...], MenuData: {...}}
                nested = primary.get("Data") or primary.get("data")
                steps = _safe_steps(nested)
                if not menu_data:
                    menu_data = _safe_menu(nested, _safe_menu(raw_settings))
        else:
            steps = _safe_steps(primary)
            menu_data = _safe_menu(raw_settings)

        return steps, menu_data

    return [], {}


def _build_manage_view_steps(raw_settings):
    steps, _ = _extract_steps_and_menu(raw_settings)
    view_steps = []

    script_labels = {
        "vikingbot.base.gathervip": "Сбор ресурсов",
        "vikingbot.base.dailies": "Ежедневные задания",
        "vikingbot.base.alliancedonation": "Техи и подарки племени",
        "vikingbot.base.mail": "Почта",
        "vikingbot.base.buffs": "Баффы",
        "vikingbot.base.recruitment": "Найм войск",
        "vikingbot.base.upgrade": "Стройка",
        "vikingbot.base.research": "Исследования",
        "vikingbot.base.divinationshack": "Хижина Гадалки",
        "vikingbot.base.exploration": "Экспедиции в поручениях (перья, яблоки)",
        "vikingbot.base.commission": "Выполнять поручения",
        "vikingbot.base.dragoncave": "Пещера дракона",
        "vikingbot.base.stagingpost": "Пост разгрузки",
        "vikingbot.base.build": "Строить новые здания (молоток)",
        "vikingbot.base.villages": "Сбор наград с орлов",
        "vikingbot.base.heal": "Лечение",
        "vikingbot.base.eaglenest": "Орлиное гнездо",
    }

    def _fmt_schedule_rule(rule):
        if not isinstance(rule, dict):
            return None

        start = (
            rule.get("StartAt")
            or rule.get("Start")
            or rule.get("From")
            or rule.get("TimeFrom")
        )
        end = (
            rule.get("EndAt")
            or rule.get("End")
            or rule.get("To")
            or rule.get("TimeTo")
        )
        every = rule.get("Every") or rule.get("Interval") or rule.get("EveryMinutes")
        days = rule.get("Days") or rule.get("WeekDays") or rule.get("Weekdays")
        label = rule.get("Label") or rule.get("Name")

        parts = []
        if days:
            if isinstance(days, (list, tuple)):
                parts.append("Дни: " + ", ".join(map(str, days)))
            else:
                parts.append(f"Дни: {days}")
        if start or end:
            parts.append(f"{start or '00:00'} — {end or '24:00'}")
        if every:
            parts.append(f"каждые {every}")
        if label:
            parts.append(str(label))

        return ", ".join(parts) if parts else None

    for idx, step in enumerate(steps):
        if not isinstance(step, dict):
            step = {}

        cfg = step.get("Config") or {}
        script_id = step.get("ScriptId")
        name = (
            cfg.get("Name")
            or cfg.get("name")
            or script_labels.get(script_id)
            or script_id
            or f"Шаг {idx + 1}"
        )
        description = cfg.get("Description") or cfg.get("description") or ""

        schedule_rules = step.get("ScheduleRules") or []
        summaries = [s for s in (_fmt_schedule_rule(r) for r in schedule_rules) if s]
        schedule_summary = "; ".join(summaries) if summaries else None

        view_steps.append(
            {
                "index": idx,
                "name": name,
                "script_id": script_id,
                "config": cfg,
                "description": description,
                "is_active": bool(step.get("IsActive", True)),
                "schedule_summary": schedule_summary,
                "schedule_rules_count": len(schedule_rules),
            }
        )

    return view_steps



@client_bp.route("/dashboard")
@login_required
def dashboard():
    """
    Дашборд клиента:
    - если пользователь админ — перенаправляем в админ-панель;
    - если клиент — показываем только его фермы и ресурсы по ним.
    """
    if getattr(current_user, "role", None) == "admin":
        return redirect(url_for("admin.admin_dashboard"))

    if not hasattr(g, "farmdata_status_cache"):
        g.farmdata_status_cache = collect_farmdata_status(current_user.id)

    farmdata_status = g.farmdata_status_cache

    accounts = (
        Account.query
        .options(
            joinedload(Account.server),
            joinedload(Account.owner),
        )
        .filter_by(owner_id=current_user.id, is_active=True)
        .order_by(Account.created_at.desc())
        .all()
    )

    total_accounts = len(accounts)

    # Ближайшие оплаты по фермам (если в БД есть такие данные)
    upcoming_all = []
    for acc in accounts:
        # поля next_payment_at / next_payment_amount добавим в Account (см. ниже)
        if getattr(acc, "next_payment_at", None):
            upcoming_all.append({
                "account_name": acc.name,
                "date": acc.next_payment_at,
                "date_str": acc.next_payment_at.strftime("%d.%m.%Y"),
                "amount": acc.next_payment_amount,
            })

    upcoming_all.sort(key=lambda x: x["date"])
    upcoming_payments = upcoming_all[:3]
    upcoming_more = max(0, len(upcoming_all) - len(upcoming_payments))

    if not accounts:
        return render_template(
            "client/dashboard.html",
            accounts_data=[],
            total_accounts=0,
            upcoming_payments=[],
            upcoming_more=0,
            farmdata_status=farmdata_status,
        )

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

        accounts_data.append({
            "account": acc,
            "resources_brief": resources_brief,
            "today_gain": today_gain,
            "last_updated": last_updated,
            "has_data": has_data,
        })

    return render_template(
        "client/dashboard.html",
        accounts_data=accounts_data,
        total_accounts=total_accounts,
        upcoming_payments=upcoming_payments,
        upcoming_more=upcoming_more,
        farmdata_status=farmdata_status,
    )


@client_bp.route("/manage", endpoint="manage_page")
@login_required
def manage_page():
    """Полноценная страница manage с выбором всех ферм пользователя."""

    if getattr(current_user, "role", None) == "admin":
        return redirect(url_for("admin.manage"))

    accounts = (
        Account.query
        .options(joinedload(Account.server))
        .filter_by(owner_id=current_user.id, is_active=True)
        .order_by(Account.name.asc())
        .all()
    )

    selected_account = None
    selected_id = request.args.get("account_id")
    if selected_id:
        try:
            selected_id_int = int(selected_id)
        except (TypeError, ValueError):
            selected_id_int = None
    else:
        selected_id_int = accounts[0].id if accounts else None

    for acc in accounts:
        if selected_id_int and acc.id == selected_id_int:
            selected_account = acc
            break
    if not selected_account and accounts:
        selected_account = accounts[0]

    view_steps = []
    steps_error = None
    raw_steps = []
    menu_data = None
    if selected_account:
        raw_settings = fetch_account_settings(selected_account)
        raw_steps, menu_data = _extract_steps_and_menu(raw_settings)
        if raw_steps:
            view_steps = _build_manage_view_steps(raw_settings)
        else:
            steps_error = "Не удалось загрузить настройки этой фермы."

    return render_template(
        "client/manage.html",
        accounts=accounts,
        selected_account=selected_account,
        view_steps=view_steps,
        raw_steps=raw_steps,
        menu_data=menu_data,
        steps_error=steps_error,
    )




@client_bp.route("/account/<int:account_id>/settings")
@login_required
def account_settings(account_id: int):
    """
    Страница настроек конкретной фермы для клиента.

    Защита:
    - клиент видит только свои аккаунты (owner_id == current_user.id).
    """
    query = (
        Account.query
        .options(
            joinedload(Account.server),
            joinedload(Account.owner),
        )
        .filter(Account.id == account_id)
    )

    if getattr(current_user, "role", None) != "admin":
        query = query.filter(Account.owner_id == current_user.id)

    account = query.first()
    if not account:
        abort(404)

    raw_settings = fetch_account_settings(account)
    steps, menu = _extract_steps_and_menu(raw_settings)
    if not steps:
        return render_template(
            "client/account_settings_error.html",
            account=account,
        )

    view_steps = []
    for idx, step in enumerate(steps):
        if not isinstance(step, dict):
            step = {}

        cfg = step.get("Config", {})
        name = ""
        if isinstance(cfg, dict):
            name = cfg.get("Name") or cfg.get("name") or ""
        if not name:
            name = f"Шаг {idx + 1}"

        is_active = bool(step.get("IsActive", True))

        view_steps.append({
            "index": idx,
            "name": name,
            "is_active": is_active,
        })

    return render_template(
        "client/account_settings.html",
        account=account,
        steps=steps,
        view_steps=view_steps,
        menu=menu,
    )


@client_bp.route("/manage/account/<int:account_id>/details")
@login_required
def manage_account_details(account_id: int):
    query = (
        Account.query
        .options(joinedload(Account.server))
        .filter(Account.id == account_id)
    )

    if getattr(current_user, "role", None) != "admin":
        query = query.filter_by(owner_id=current_user.id, is_active=True)

    account = query.first()
    if not account:
        return jsonify({"ok": False, "error": "account not found"}), 404

    raw_settings = fetch_account_settings(account)
    raw_steps, menu_data = _extract_steps_and_menu(raw_settings)
    if not raw_steps:
        return jsonify({"ok": False, "error": "failed to load settings"}), 500

    steps = _build_manage_view_steps(raw_settings)

    return jsonify(
        {
            "ok": True,
            "account": {
                "id": account.id,
                "name": account.name,
                "server": account.server.name if account.server else None,
            },
            "steps": steps,
            "raw_steps": raw_steps,
            "menu": menu_data,
        }
    )


@client_bp.route("/account/<int:account_id>/settings/step/<int:step_idx>/toggle", methods=["POST"])
@login_required
def account_toggle_step(account_id: int, step_idx: int):
    """
    AJAX-эндпоинт: включение/выключение шага (IsActive) для аккаунта.

    Принимает JSON: {"is_active": true|false}
    """
    query = (
        Account.query
        .options(joinedload(Account.server))
        .filter(Account.id == account_id)
    )

    if getattr(current_user, "role", None) != "admin":
        query = query.filter_by(owner_id=current_user.id)

    account = query.first()
    if not account:
        return jsonify({"ok": False, "error": "account not found"}), 404

    payload = request.get_json(silent=True) or {}
    if "is_active" not in payload:
        return jsonify({"ok": False, "error": "is_active is required"}), 400

    new_active = bool(payload.get("is_active"))

    ok, msg = update_account_step_settings(account, step_idx, {"IsActive": new_active})
    status = 200 if ok else 500
    return jsonify({"ok": ok, "message": msg}), status


@client_bp.route("/manage/account/<int:account_id>/settings/<int:step_idx>", methods=["PUT"])
@login_required
def manage_update_step(account_id: int, step_idx: int):
    query = (
        Account.query
        .options(joinedload(Account.server))
        .filter(Account.id == account_id)
    )

    if getattr(current_user, "role", None) != "admin":
        query = query.filter_by(owner_id=current_user.id, is_active=True)

    account = query.first()
    if not account:
        return jsonify({"ok": False, "error": "account not found"}), 404

    payload = request.get_json(silent=True) or {}
    update_payload = {}

    if "IsActive" in payload:
        update_payload["IsActive"] = bool(payload.get("IsActive"))

    if isinstance(payload.get("Config"), dict):
        update_payload["Config"] = payload.get("Config")

    if isinstance(payload.get("ScheduleRules"), list):
        update_payload["ScheduleRules"] = payload.get("ScheduleRules")

    if not update_payload:
        return jsonify({"ok": False, "error": "no valid fields"}), 400

    ok, msg = update_account_step_settings(account, step_idx, update_payload)
    status = 200 if ok else 500
    return jsonify({"ok": ok, "message": msg}), status


@client_bp.route("/account/<int:account_id>/refresh", methods=["POST"])
@login_required
def account_refresh(account_id: int):
    """
    AJAX-эндпоинт: обновление ресурсов для одной фермы.

    Логика:
    - проверяем, что аккаунт принадлежит текущему пользователю;
    - вызываем fetch_resources_for_accounts([account]);
    - отдаём JSON с текущими ресурсами для обновления строки в таблице.
    """
    query = (
        Account.query
        .options(joinedload(Account.server))
        .filter(Account.id == account_id)
    )

    if getattr(current_user, "role", None) != "admin":
        query = query.filter_by(owner_id=current_user.id)

    account = query.first()
    if not account:
        return jsonify({"ok": False, "error": "account not found"}), 404

    res_map = fetch_resources_for_accounts([account])
    res_info = res_map.get(account.id)

    if not res_info:
        return jsonify({"ok": False, "error": "no resource data for this account"}), 404


    return jsonify({
        "ok": True,
        "resources_brief": res_info.get("brief"),
        "today_gain": res_info.get("today_gain"),
        "last_updated": res_info.get("last_updated_fmt") or res_info.get("last_updated"),
    })


@client_bp.route("/farm-data")
@login_required
def farm_data():
    """
    Страница 'Данные ферм' для клиента.

    Теперь:
    - подставляем список ферм (Account) текущего пользователя;
    - к каждой ферме подтягиваем, если есть, FarmData по имени;
    - имя фермы пользователь НЕ вводит, оно всегда из Account.name.
    """
    if getattr(current_user, "role", None) == "admin":
        return redirect(url_for("admin.admin_dashboard"))

    # Все активные аккаунты клиента
    accounts = (
        Account.query
        .filter_by(owner_id=current_user.id, is_active=True)
        .order_by(Account.name.asc())
        .all()
    )

    # Все FarmData этого пользователя
    farm_entries = (
        FarmData.query
        .filter_by(user_id=current_user.id)
        .all()
    )
    by_name = {entry.farm_name: entry for entry in farm_entries}

    items = []
    for acc in accounts:
        items.append({
            "account": acc,
            "farmdata": by_name.get(acc.name),
        })

    return render_template(
        "client/farm_data.html",
        items=items,
    )


@client_bp.route("/farm-data/save", methods=["POST"])
@login_required
def farm_data_save():
    """
    Сохранение клиентских «Данных ферм» из таблицы.

    Фронтенд (static/js/main.js → handleFarmDataSave) отправляет JSON вида:
    {
      "items": [
        {
          "account_id": "123",   // ID из таблицы accounts
          "email": "...",
          "password": "...",
          "igg_id": "123456789",
          "server": "K72"
        },
        ...
      ]
    }

    В базе мы:
      - находим все аккаунты текущего пользователя по переданным account_id;
      - для каждого аккаунта создаём/обновляем запись FarmData
        (user_id = current_user.id, farm_name = Account.name).
    """
    # Админ не должен сюда писать — у него есть своя админская таблица
    if getattr(current_user, "role", None) == "admin":
        return jsonify({"ok": False, "error": "admin cannot edit farm data here"}), 403

    payload = request.get_json(silent=True) or {}
    items = payload.get("items") or []

    if not isinstance(items, list) or not items:
        return jsonify({"ok": False, "error": "Нет данных для сохранения"}), 400

    # Собираем валидные account_id из payload
    cleaned_items = []
    account_ids = set()

    for row in items:
        account_id_raw = row.get("account_id")
        try:
            acc_id = int(account_id_raw)
        except (TypeError, ValueError):
            # некорректный ID — пропускаем
            continue

        cleaned_items.append((acc_id, row))
        account_ids.add(acc_id)

    if not cleaned_items:
        return jsonify({"ok": False, "error": "Нет валидных ферм для сохранения"}), 400

    # Подтягиваем аккаунты текущего пользователя одним запросом
    from UsersDash.models import Account, FarmData  # локальный импорт, чтобы избежать циклов

    accounts = (
        Account.query
        .filter(
            Account.id.in_(account_ids),
            Account.owner_id == current_user.id,
            Account.is_active.is_(True),
        )
        .all()
    )
    acc_by_id = {acc.id: acc for acc in accounts}

    if not acc_by_id:
        return jsonify({"ok": False, "error": "Фермы не найдены или не принадлежат вам"}), 400

    # Для оптимизации сразу вытаскиваем все FarmData по (user_id, farm_name)
    farm_names = {acc.name for acc in accounts}
    farmdata_entries = (
        FarmData.query
        .filter(
            FarmData.user_id == current_user.id,
            FarmData.farm_name.in_(farm_names),
        )
        .all()
    )
    fd_by_key = {(fd.user_id, fd.farm_name): fd for fd in farmdata_entries}

    # Первая проходка — валидация IGG
    for acc_id, row in cleaned_items:
        acc = acc_by_id.get(acc_id)
        if not acc:
            continue

        igg_id = (row.get("igg_id") or "").strip()
        if igg_id and not igg_id.isdigit():
            return jsonify({
                "ok": False,
                "error": f"IGG ID для фермы «{acc.name}» должен содержать только цифры",
            }), 400

    # Вторая проходка — применение изменений
    try:
        for acc_id, row in cleaned_items:
            acc = acc_by_id.get(acc_id)
            if not acc:
                # чужой аккаунт или неактивный — на всякий случай игнорируем
                continue

            farm_name = acc.name
            key = (current_user.id, farm_name)

            email = (row.get("email") or "").strip()
            password_val = (row.get("password") or "").strip()
            igg_id = (row.get("igg_id") or "").strip()
            server_val = (row.get("server") or "").strip()
            telegram_tag_val = (row.get("telegram_tag") or "").strip()

            fd = fd_by_key.get(key)
            if not fd:
                # создаём только если есть хоть какие-то данные
                if not any([email, password_val, igg_id, server_val, telegram_tag_val]):
                    continue

                fd = FarmData(
                    user_id=current_user.id,
                    farm_name=farm_name,
                )
                db.session.add(fd)
                fd_by_key[key] = fd

            # Обновляем поля
            fd.email = email or None
            fd.password = password_val or None
            fd.igg_id = igg_id or None
            fd.server = server_val or None
            fd.telegram_tag = telegram_tag_val or None

        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        print(f"[farm_data_save] ERROR: {exc}")
        return jsonify({"ok": False, "error": "Ошибка при сохранении данных."}), 500

    farmdata_status = collect_farmdata_status(current_user.id)
    g.farmdata_status_cache = farmdata_status

    return jsonify(
        {
            "ok": True,
            "farmdata_status": farmdata_status,
            "farmdata_required": bool(farmdata_status.get("has_issues")),
        }
    )


