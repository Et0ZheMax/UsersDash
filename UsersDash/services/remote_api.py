# services/remote_api.py
# Работа с удалённым RssCounterWebV7:
# - /api/resources        — ресурсы ферм
# - /api/serverStatus     — health-check (для админского лога)
# - /api/manage/account/... — настройки шагов (manage)

from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
import requests


# Таймауты для HTTP-запросов (в секундах)
DEFAULT_TIMEOUT = 15


def _get_effective_api_base(server) -> Optional[str]:
    """
    Возвращает "правильный" base URL для API конкретного сервера.

    В БД server.api_base_url ожидается что-то вроде:
      - "https://hotly-large-coral.cloudpub.ru/"
      - "http://192.168.31.234:5000"
      - "http://host:5000/api"

    На выходе хотим:
      - "https://hotly-large-coral.cloudpub.ru/api"
      - "http://192.168.31.234:5000/api"
    """
    raw = (server.api_base_url or "").strip()
    if not raw:
        print(f"[remote_api] WARNING: api_base_url не заполнен для сервера {server}")
        return None

    base = raw.rstrip("/")

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
        return resp.json()
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
        return dt.strftime("%H:%M %d.%m.%Y")
    except Exception:
        return dt_str


def fetch_resources_for_accounts(accounts: List[Any]) -> Dict[int, Dict[str, Any]]:
    """
    Подтягивает ресурсы сразу для списка аккаунтов (Account-моделей).

    Логика:
    - группируем аккаунты по server_id;
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

    from models import Server  # локальный импорт, чтобы избежать циклов

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

            food_view = res.get("food_view", "?")
            wood_view = res.get("wood_view", "?")
            stone_view = res.get("stone_view", "?")
            gold_view = res.get("gold_view", "?")

            brief = f"{food_view} / {wood_view} / {stone_view} / {gold_view}"

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
    remote_id, _ = _resolve_remote_account(account, server_resources)
    if not remote_id:
        print(f"[remote_api] WARNING: не удалось определить remote_id для аккаунта {account}")
        return None

    url = f"{base}/manage/account/{remote_id}/settings"
    data = _safe_get_json(url, timeout=DEFAULT_TIMEOUT)
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

import logging
from typing import List, Dict, Tuple

import requests
from requests import RequestException

from models import Server

log = logging.getLogger(__name__)


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
