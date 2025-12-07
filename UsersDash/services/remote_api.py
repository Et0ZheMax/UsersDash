# services/remote_api.py
# Работа с удалённым RssCounterWebV7:
# - /api/resources        — ресурсы ферм
# - /api/server/self_status — self-health без SSH
# - /api/manage/account/... — настройки шагов (manage)

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
from markupsafe import Markup, escape
from requests import RequestException

from UsersDash.models import Server

log = logging.getLogger(__name__)


# Таймауты для HTTP-запросов (в секундах)
DEFAULT_TIMEOUT = 15
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def _get_effective_api_base(server) -> Optional[str]:
    """
    Возвращает "правильный" base URL для API конкретного сервера.

    Пытаемся использовать api_base_url, а при его отсутствии — host/base_url,
    чтобы не требовать жёсткого заполнения только одного поля.

    На выходе хотим строку вида "http://host:5000/api".
    """

    raw = (
        getattr(server, "api_base_url", None)
        or getattr(server, "host", None)
        or getattr(server, "base_url", None)
        or ""
    ).strip()

    if not raw:
        print(f"[remote_api] WARNING: api_base_url/host не заполнен для сервера {server}")
        return None

    base = raw.rstrip("/")

    # Если строка не начинается с http, добавим http://
    if not base.startswith("http://") and not base.startswith("https://"):
        base = "http://" + base

    # Если админ уже указал .../api — оставляем как есть
    if base.endswith("/api"):
        return base

    return base + "/api"


def _safe_get_json(url: str, timeout: int = DEFAULT_TIMEOUT) -> Optional[Dict[str, Any]]:
    """
    Безопасный GET-запрос:
    - не роняет приложение при ошибке;
    - логирует ошибку;
    - на ошибке возвращает None.
    """
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        try:
            return resp.json()
        except ValueError:
            # На некоторых хостах content-type может быть text/plain — пробуем сами
            try:
                return json.loads(resp.text)
            except Exception as exc:
                print(f"[remote_api] ERROR: GET {url} JSON decode failed: {exc}")
                return None
    except Exception as exc:
        print(f"[remote_api] ERROR: GET {url} failed: {exc}")
        return None


def ping_server(server) -> Tuple[bool, str]:
    """
    Лёгкая проверка доступности сервера по /api/serverStatus.
    Используется только в health-check'е (лог в консоль).
    """
    base = _get_effective_api_base(server)
    if not base:
        return False, "api_base_url не задан"

    url = f"{base}/serverStatus"
    data = _safe_get_json(url, timeout=DEFAULT_TIMEOUT)
    if data is None:
        return False, f"Нет ответа от {url}"

    return True, "OK"


def fetch_resources_for_server(server) -> Dict[str, Dict[str, Any]]:
    """
    Подтягивает ресурсы со старого RssCounter через /api/resources
    для ОДНОГО сервера.

    Возвращает словарь:
      {
        "<account_id>": {
            "id": ...,
            "nickname": ...,
            "instanceId": ...,
            "food_view": ...,
            "wood_view": ...,
            "stone_view": ...,
            "gold_view": ...,
            "today_gain": ...,
            "last_updated": ...,
            ... (прочие поля)
        },
        ...
      }

    Ключ — строковый id (GUID).
    """
    base = _get_effective_api_base(server)
    if not base:
        return {}

    url = f"{base}/resources"
    data = _safe_get_json(url, timeout=DEFAULT_TIMEOUT)
    if not data or "accounts" not in data:
        print(f"[remote_api] WARNING: /api/resources вернул пусто или без 'accounts' для сервера {server.name}")
        return {}

    result: Dict[str, Dict[str, Any]] = {}
    for acc in data.get("accounts", []):
        acc_id = acc.get("id")
        if not acc_id:
            continue
        key = str(acc_id)
        result[key] = acc

    return result


def _build_resource_indexes(server_resources: Dict[str, Dict[str, Any]]) -> Tuple[
    Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]
]:
    """
    Строит индексы:
      - by_id[<id>]         = ресурсный объект
      - by_nickname[<nick>] = ресурсный объект
    """
    by_id: Dict[str, Dict[str, Any]] = server_resources or {}
    by_nickname: Dict[str, Dict[str, Any]] = {}

    for res in by_id.values():
        nick = res.get("nickname")
        if not nick:
            continue
        by_nickname[str(nick)] = res

    return by_id, by_nickname


def _resolve_remote_account(
    account,
    server_resources: Dict[str, Dict[str, Any]],
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Универсальный резолвер "удалённого" аккаунта по данным из /api/resources.

    Пытается найти ресурсный объект для Account:
      1) если задан internal_id — ищем по:
            - id == internal_id  (GUID, который возвращает /api/resources),
            - instanceId == internal_id (если удобнее хранить номер инстанса);
      2) если не нашли — ищем по name == nickname.

    Возвращает:
      (remote_id, resource_obj)  или  (None, None),
      где remote_id — это всегда удалённый GUID (поле id).
    """
    if not server_resources:
        return None, None

    # Индексы по разным ключам
    by_id: Dict[str, Dict[str, Any]] = {}
    by_nickname: Dict[str, Dict[str, Any]] = {}
    by_instance: Dict[str, Dict[str, Any]] = {}

    for key, res in server_resources.items():
        rid = res.get("id")
        if rid is not None:
            by_id[str(rid)] = res

        nick = res.get("nickname")
        if nick:
            by_nickname[str(nick)] = res

        inst = res.get("instanceId")
        if inst is not None:
            by_instance[str(inst)] = res

    # 1) По internal_id — сначала по GUID id, потом по instanceId
    internal_id = getattr(account, "internal_id", None)
    if internal_id:
        key = str(internal_id)
        # Попробуем сопоставить как GUID
        if key in by_id:
            return key, by_id[key]
        # Если не нашли — пробуем как instanceId
        if key in by_instance:
            res = by_instance[key]
            return str(res.get("id")), res

    # 2) По имени аккаунта → nickname
    name = getattr(account, "name", None)
    if name:
        key = str(name)
        if key in by_nickname:
            res = by_nickname[key]
            return str(res.get("id")), res

    return None, None


def _fmt_last_updated(dt_str: Optional[str]) -> Optional[str]:
    """
    Преобразует ISO-строку из /api/resources в формат 'чч:мм дд:мм:гггг'.
    Если что-то пошло не так — возвращает исходную строку.
    """
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str)
        dt = _to_moscow_time(dt)
        return dt.strftime("%H:%M %d.%m.%Y")
    except Exception:
        return dt_str


def _fmt_generated_at(dt_str: Optional[str]) -> Optional[str]:
    """Форматирует отметку времени сводки наблюдения."""

    if not dt_str:
        return None


def _format_resource_value(view: Any, emoji: str) -> Markup:
    """Возвращает HTML-строку с ресурсом, приростом и эмодзи."""

    if view is None:
        return Markup(f"?<span class=\"resource-emoji\">{emoji}</span>")

    text = str(view)

    if "<" in text:
        safe_html = Markup(text.replace("gainValue", "resource-gain"))
        return safe_html + Markup(f"<span class=\"resource-emoji\">{emoji}</span>")

    base_part = text
    gain_part = ""

    match = re.match(r"^(.+?)(\+.+)$", text)
    if match:
        base_part, gain_part = match.groups()

    base_html = escape(base_part.strip())
    gain_html = (
        Markup(f'<span class="resource-gain">{escape(gain_part)}</span>') if gain_part else Markup("")
    )

    return Markup(base_html) + gain_html + Markup(f"<span class=\"resource-emoji\">{emoji}</span>")


def _to_moscow_time(dt: datetime) -> datetime:
    """Переводит datetime в часовой пояс Москвы."""

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MOSCOW_TZ)

    try:
        dt = datetime.fromisoformat(dt_str)
    except ValueError:
        try:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except Exception:
            return dt_str

    dt = _to_moscow_time(dt)
    return dt.strftime("%d.%m %H:%M")


def fetch_resources_for_accounts(accounts: List[Any]) -> Dict[int, Dict[str, Any]]:
    """
    Подтягивает ресурсы сразу для списка аккаунтов (Account-моделей).

    Логика:
    - для каждого сервера делаем ОДИН запрос /api/resources;
    - по каждому аккаунту вызываем _resolve_remote_account(...) —
      ищем совпадение либо по internal_id, либо по name;
    - на основе ресурсных данных формируем краткое представление.

    На выходе:
        {
          account.id (наш PK): {
              "raw": {... полный JSON из /api/resources ...},
              "brief": "Food / Wood / Stone / Gold (HTML)",
              "today_gain": "...",
              "last_updated": "ISO",
              "last_updated_fmt": "чч:мм дд:мм:гггг",
              "remote_id": "..."
          },
          ...
        }
    """
    by_server: Dict[int, List[Any]] = {}
    for acc in accounts:
        if not acc.server_id:
            print(f"[remote_api] WARNING: у аккаунта {acc} нет server_id")
            continue
        by_server.setdefault(acc.server_id, []).append(acc)

    result: Dict[int, Dict[str, Any]] = {}

    from UsersDash.models import Server  # локальный импорт, чтобы избежать циклов

    for server_id, acc_list in by_server.items():
        server = Server.query.get(server_id)
        if not server:
            print(f"[remote_api] WARNING: сервер с id={server_id} не найден в БД")
            continue

        server_resources = fetch_resources_for_server(server)
        if not server_resources:
            continue

        for acc in acc_list:
            remote_id, res = _resolve_remote_account(acc, server_resources)
            if not res:
                continue

            brief_parts = [
                _format_resource_value(res.get("food_view"), "🍗"),
                _format_resource_value(res.get("wood_view"), "🌲"),
                _format_resource_value(res.get("stone_view"), "🧱"),
                _format_resource_value(res.get("gold_view"), "📀"),
            ]

            brief = Markup(" / ").join(brief_parts)

            last_raw = res.get("last_updated")
            last_fmt = _fmt_last_updated(last_raw)

            result[acc.id] = {
                "raw": res,
                "brief": brief,
                "today_gain": res.get("today_gain"),
                "last_updated": last_raw,
                "last_updated_fmt": last_fmt,
                "remote_id": remote_id,
            }

    return result


def _decode_json_if_str(value: Any) -> Any:
    """Возвращает распарсенный JSON, если передана строка."""

    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _deep_decode_manage(value: Any) -> Any:
    """Рекурсивно декодирует строковый JSON внутри manage-пэйлоада."""

    value = _decode_json_if_str(value)

    if isinstance(value, dict) and len(value) == 1:
        (only_key, only_val), = value.items()
        if only_key in ("data", "settings", "payload"):
            return _deep_decode_manage(only_val)

    if isinstance(value, list):
        return [_deep_decode_manage(item) for item in value]

    if isinstance(value, dict):
        return {key: _deep_decode_manage(val) for key, val in value.items()}

    return value


def _unwrap_manage_payload(data: Any) -> Any:
    """Извлекает полезную нагрузку настроек из разных оболочек."""

    # Если пришла строка — пробуем распарсить JSON
    data = _decode_json_if_str(data)

    while isinstance(data, dict) and len(data) == 1:
        (only_key, only_val), = data.items()
        if only_key in ("data", "settings", "payload"):
            data = _decode_json_if_str(only_val)
            continue
        break
    return data


def fetch_account_settings(account) -> Optional[Dict[str, Any]]:
    """
    Получаем настройки конкретного аккаунта (фермы) через
    /api/manage/account/<remote_id>/settings на старом RssCounter.

    remote_id — тот же id, что и в /api/resources (GUID), либо тот,
    который используется в твоём manage.
    """
    server = getattr(account, "server", None)
    if not server:
        print(f"[remote_api] WARNING: у аккаунта {account} нет server")
        return None

    base = _get_effective_api_base(server)
    if not base:
        return None

    # Сначала подтягиваем ресурсы по серверу, чтобы определить remote_id
    server_resources = fetch_resources_for_server(server)
    remote_id = None
    if server_resources:
        remote_id, _ = _resolve_remote_account(account, server_resources)

    # Если ресурсы не достались или не сопоставились — пробуем прямой fallback
    if not remote_id:
        fallback_remote = getattr(account, "internal_id", None) or getattr(account, "name", None)
        if fallback_remote:
            remote_id = str(fallback_remote)

    if not remote_id:
        print(f"[remote_api] WARNING: не удалось определить remote_id для аккаунта {account}")
        return None

    url = f"{base}/manage/account/{remote_id}/settings"
    data = _safe_get_json(url, timeout=DEFAULT_TIMEOUT)
    data = _unwrap_manage_payload(data)

    if isinstance(data, dict):
        data = {key: _unwrap_manage_payload(val) for key, val in data.items()}

    if isinstance(data, list):
        data = {"Data": data}
    if isinstance(data, dict):
        for key in ("Data", "MenuData"):
            data[key] = _deep_decode_manage(data.get(key))
    if data is None:
        print(f"[remote_api] WARNING: не удалось получить настройки аккаунта {remote_id} с {url}")
    return data


def update_account_step_settings(account, step_idx: int, payload: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Обновляет один шаг настроек аккаунта через
    PUT /api/manage/account/<remote_id>/settings/<step_idx>

    payload может содержать:
      - "IsActive": true|false
      - "Config": {...}
      - "ScheduleRules": [...]
    """
    server = getattr(account, "server", None)
    if not server:
        return False, "server is not set for account"

    base = _get_effective_api_base(server)
    if not base:
        return False, "api_base_url is empty"

    # Аналогично fetch_account_settings — определяем remote_id
    server_resources = fetch_resources_for_server(server)
    remote_id, _ = _resolve_remote_account(account, server_resources)
    if not remote_id:
        return False, "unable to resolve remote_id for account"

    url = f"{base}/manage/account/{remote_id}/settings/{step_idx}"

    try:
        resp = requests.put(url, json=payload, timeout=DEFAULT_TIMEOUT)
        if 200 <= resp.status_code < 300:
            return True, "OK"

        try:
            body = resp.json()
        except Exception:
            body = resp.text
        return False, f"HTTP {resp.status_code}: {body}"
    except Exception as exc:
        print(f"[remote_api] ERROR: PUT {url} failed: {exc}")
        return False, str(exc)


def update_account_active(account, is_active: bool) -> Tuple[bool, str]:
    """Включает/выключает ферму через RssV7 (/api/manage/account/<id>)."""

    server = getattr(account, "server", None)
    if not server:
        return False, "server is not set for account"

    base = _get_effective_api_base(server)
    if not base:
        return False, "api_base_url is empty"

    server_resources = fetch_resources_for_server(server)
    remote_id, _ = _resolve_remote_account(account, server_resources)
    if not remote_id:
        fallback_remote = getattr(account, "internal_id", None) or getattr(account, "name", None)
        if fallback_remote:
            remote_id = str(fallback_remote)

    if not remote_id:
        return False, "unable to resolve remote_id for account"

    url = f"{base}/manage/account/{remote_id}"

    payload = {"Active": is_active, "IsActive": is_active}

    try:
        resp = requests.put(url, json=payload, timeout=DEFAULT_TIMEOUT)
    except Exception as exc:
        print(f"[remote_api] ERROR: PUT {url} failed: {exc}")
        return False, str(exc)

    if 200 <= resp.status_code < 300:
        return True, "OK"

    try:
        body = resp.json()
    except Exception:
        body = resp.text

    return False, f"HTTP {resp.status_code}: {body}"


def apply_template_for_account(account, template: str) -> Tuple[bool, str]:
    """
    Применяет manage-шаблон к аккаунту через
    POST /api/manage/account/<remote_id>/apply_template.

    template — имя/ключ шаблона (например, "500", "OnlyFarm", ...).
    """

    server = getattr(account, "server", None)
    if not server:
        return False, "server is not set for account"

    base = _get_effective_api_base(server)
    if not base:
        return False, "api_base_url is empty"

    server_resources = fetch_resources_for_server(server)
    remote_id, _ = _resolve_remote_account(account, server_resources)
    if not remote_id:
        return False, "unable to resolve remote_id for account"

    url = f"{base}/manage/account/{remote_id}/apply_template"

    try:
        resp = requests.post(url, json={"template": template}, timeout=DEFAULT_TIMEOUT)
    except Exception as exc:
        print(f"[remote_api] ERROR: POST {url} failed: {exc}")
        return False, str(exc)

    if 200 <= resp.status_code < 300:
        try:
            body = resp.json() or {}
        except Exception:
            body = {}

        status = body.get("status") or body.get("ok")
        message = body.get("message") or body.get("error") or "OK"
        if status == "ok" or status is True:
            return True, message

        return False, message

    try:
        err_body = resp.json()
    except Exception:
        err_body = resp.text
    return False, f"HTTP {resp.status_code}: {err_body}"


def _request_template_api(
    server: Server, method: str, path: str, payload: Optional[dict] = None
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    base = _get_effective_api_base(server)
    if not base:
        return None, "api_base_url is empty"

    url = f"{base}{path}"
    try:
        resp = requests.request(method, url, json=payload, timeout=DEFAULT_TIMEOUT)
    except Exception as exc:
        return None, str(exc)

    if 200 <= resp.status_code < 300:
        try:
            return resp.json(), None
        except Exception:
            return {}, None

    try:
        body = resp.json()
    except Exception:
        body = resp.text
    return None, f"HTTP {resp.status_code}: {body}"


def fetch_templates_list(server: Server) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    return _request_template_api(server, "GET", "/templates/list")


def fetch_template_schema(server: Server) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    return _request_template_api(server, "GET", "/schema/get")


def fetch_template_payload(server: Server, name: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    return _request_template_api(server, "GET", f"/templates/{quote(name)}")


def save_template_payload(server: Server, name: str, steps: list) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    return _request_template_api(server, "PUT", f"/templates/{quote(name)}", {"steps": steps})


def delete_template_payload(server: Server, name: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    return _request_template_api(server, "DELETE", f"/templates/{quote(name)}")


def rename_template_payload(
    server: Server, name: str, new_name: str
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    return _request_template_api(
        server,
        "PATCH",
        f"/templates/{quote(name)}/rename",
        {"new_name": new_name},
    )

def _build_server_base_url(server: Server) -> str:
    """
    Аккуратно собираем базовый URL для RssV7.
    Поддерживаем несколько возможных полей у модели Server, чтобы не хардкодить.
    """
    # Пытаемся вытащить один из возможных атрибутов, которые ты мог использовать
    base_url = (
        getattr(server, "api_base_url", None)
        or getattr(server, "base_url", None)
        or getattr(server, "host", None)
        or ""
    )
    base_url = (base_url or "").strip().rstrip("/")
    if not base_url:
        return ""

    # Если строка не начинается с http, добавим http://
    if not base_url.startswith("http://") and not base_url.startswith("https://"):
        base_url = "http://" + base_url

    return base_url


def fetch_rssv7_accounts_meta(server: Server) -> Tuple[List[Dict], str]:
    """
    Запрашивает с конкретного сервера RssV7 полный список аккаунтов и их meta.
    Ожидаемый эндпоинт на стороне RssV7: /api/accounts_meta_full

    Возвращает:
      (список_аккаунтов, строка_ошибки_если_есть)
    """
    base_url = _build_server_base_url(server)
    if not base_url:
        err = f"Не задан URL для сервера id={server.id} name={server.name}"
        log.warning(err)
        return [], err

    url = f"{base_url}/api/accounts_meta_full"

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except RequestException as exc:
        err = f"Ошибка запроса к {url}: {exc}"
        log.error(err)
        return [], err

    try:
        data = resp.json()
    except ValueError as exc:
        err = f"Некорректный JSON от {url}: {exc}"
        log.error(err)
        return [], err

    # Поддерживаем два формата ответа:
    #  1) {"ok": true, "items": [...]} — прежний контракт
    #  2) [ {...}, {...} ] — RssV7 отдаёт список напрямую
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("items")

        # Если ok == False, попробуем достать список аккаунтов из других ключей,
        # прежде чем возвращать ошибку.
        if data.get("ok") is False:
            # Некоторые инстансы RssV7 кладут аккаунты в поле error
            # (хотя статус при этом 200). Если это список словарей — считаем, что
            # это и есть нужные элементы.
            if items is None and isinstance(data.get("error"), list):
                candidate = data.get("error")
                if all(isinstance(x, dict) for x in candidate):
                    items = candidate
                    log.warning(
                        "RssV7 вернул ok=false, но прислал список аккаунтов в error"
                    )

            # Если так и не нашли элементы — это реальная ошибка
            if items is None:
                err = f"RssV7 вернул ошибку: {data!r}"
                log.error(err)
                return [], err

        if items is None:
            err = f"Некорректный ответ от {url}: нет items"
            log.error(err)
            return [], err
    else:
        err = f"Некорректный формат ответа от {url}: {data!r}"
        log.error(err)
        return [], err

    if not isinstance(items, list):
        err = f"Некорректный формат items от {url}"
        log.error(err)
        return [], err

    return items, ""


def _format_problem_summary(item: Dict[str, Any]) -> str:
    """Собирает компактную строку проблем из LD_problems."""

    problems = item.get("problems") or []
    parts: list[str] = []

    if isinstance(problems, list):
        for prob in problems:
            label = prob.get("label") if isinstance(prob, dict) else None
            if not label:
                continue
            count_raw = prob.get("count") if isinstance(prob, dict) else None
            try:
                count_int = int(count_raw) if count_raw is not None else 0
            except (TypeError, ValueError):
                count_int = 0
            suffix = f"({count_int})" if count_int and count_int > 1 else ""
            parts.append(f"{label}{suffix}")

    return " + ".join(parts)


def fetch_watch_summary(server) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Подтягивает сводку проблем с конкретного сервера (LD_problems → /api/problems/summary).
    Возвращает нормализованные данные для отображения в UsersDash.
    """

    base = _get_effective_api_base(server)
    if not base:
        return None, "api_base_url не задан"

    url = f"{base}/problems/summary"
    data = _safe_get_json(url, timeout=DEFAULT_TIMEOUT)
    if data is None:
        return None, f"Нет ответа от {url}"

    accounts = []
    for item in data.get("accounts") or []:
        if not isinstance(item, dict):
            continue
        nick = item.get("nickname") or item.get("account")
        if not nick:
            continue
        summary = item.get("summary") or _format_problem_summary(item)
        accounts.append(
            {
                "nickname": nick,
                "summary": summary,
                "total": item.get("total", 0),
                "kind": item.get("kind"),
                "remote_id": item.get("acc_id") or item.get("id"),
            }
        )

    payload = {
        "server": data.get("server") or getattr(server, "name", ""),
        "generated_at": data.get("generated_at"),
        "generated_at_fmt": _fmt_generated_at(data.get("generated_at")),
        "accounts": accounts,
    }

    return payload, ""


def fetch_server_self_status(server) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Запрашивает self_status у RssCounter (/api/server/self_status).
    Возвращает (payload, error).
    """

    base = _get_effective_api_base(server)
    if not base:
        return None, "api_base_url не задан"

    url = f"{base}/server/self_status"
    data = _safe_get_json(url, timeout=DEFAULT_TIMEOUT)
    if data is None:
        return None, f"Нет ответа от {url}"
    if not isinstance(data, dict):
        return None, f"Некорректный ответ от {url}"

    return data, ""


def fetch_server_cycle_time(
    server,
    *,
    window_hours: int = 24,
    min_gap_minutes: int = 5,
    max_gap_hours: int = 6,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Получает статистику «времени круга» с /api/cycle_time.
    Возвращает (payload, error).
    """

    base = _get_effective_api_base(server)
    if not base:
        return None, "api_base_url не задан"

    url = (
        f"{base}/cycle_time?window_hours={window_hours}"
        f"&min_gap_minutes={min_gap_minutes}&max_gap_hours={max_gap_hours}"
    )
    data = _safe_get_json(url, timeout=DEFAULT_TIMEOUT)
    if data is None:
        return None, f"Нет ответа от {url}"
    if not isinstance(data, dict):
        return None, f"Некорректный ответ от {url}"

    return data, ""
