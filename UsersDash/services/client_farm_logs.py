"""Подготавливает компактную и безопасную ленту логов для клиентов UsersDash."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from UsersDash.models import Account, FarmLogEntry


_LOG_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:\s+[+-]\d{2}:\d{2})?\s*"
)
_LOG_LEVEL_RE = re.compile(r"\[(?:DBG|INF|WRN|ERR)\]\s*", re.IGNORECASE)
_LOG_PREFIX_RE = re.compile(
    r"^(?:INFO|WARN|WARNING|ERROR|ERR|EXCEPTION|DEBUG)\|(?:Main|[0-9a-f]{8,64})\|",
    re.IGNORECASE,
)
_DURATION_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\]")
_FINISHED_RE = re.compile(r"\b([a-z]+)\s+Finished\b", re.IGNORECASE)
_RUNNING_RE = re.compile(r"^Running\s+([a-z]+)(?:\s+on\s+.+)?$", re.IGNORECASE)

_SCENARIOS = {
    "gathervip": "Сбор ресурсов",
    "recruitment": "Обучение войск",
    "research": "Исследование",
    "upgrade": "Улучшение зданий",
    "alliancedonation": "Пожертвования альянсу",
    "buffs": "Активация усилений",
    "dailies": "Ежедневные задания",
    "mail": "Сбор почты",
}
_SCENARIO_FINISHED_TITLES = {
    "gathervip": "Сбор ресурсов завершён",
    "recruitment": "Обучение войск завершено",
    "research": "Исследование завершено",
    "upgrade": "Проверка улучшений завершена",
    "alliancedonation": "Пожертвования альянсу выполнены",
    "buffs": "Активация усилений завершена",
    "dailies": "Ежедневные задания выполнены",
    "mail": "Почта собрана",
}

_HIDDEN_PATTERNS = (
    re.compile(r"^Skipping\s+.+\[only run on:\s*wrong time\]$", re.IGNORECASE),
    re.compile(r"^Found\s+\d+\s+active Actions$", re.IGNORECASE),
    re.compile(r"^Starting Account\s+.+\s+with\s+\d+\s+active actions$", re.IGNORECASE),
    re.compile(r"^(?:Starting|Stopping) Emulator(?:\.|\s+on\s+.+)?$", re.IGNORECASE),
    re.compile(r"^Launch:\s*(?:Running|Reading Version|Check Game Version|Init Completed|Completed)", re.IGNORECASE),
    re.compile(r"^Account Switch:", re.IGNORECASE),
    re.compile(r"^Running accountswitch(?:\s+on\s+.+)?$", re.IGNORECASE),
    re.compile(r"^accountswitch Finished\b", re.IGNORECASE),
    re.compile(r"^Check Current Marches:", re.IGNORECASE),
    re.compile(r"^(?:Gather|Research|Alliance):\s*Open\s+.+Menu$", re.IGNORECASE),
    re.compile(r"^Gather:\s*Cannot\s+find\s+Level\s+Menu$", re.IGNORECASE),
    re.compile(r"^Recruit:\s*Check\s+", re.IGNORECASE),
)


def _clean_raw_text(value: str | None) -> str:
    """Убирает технический префикс, сохраняя текст только для внутренней классификации."""

    text = str(value or "").strip()
    text = _LOG_TIMESTAMP_RE.sub("", text, count=1).strip()
    text = _LOG_LEVEL_RE.sub("", text, count=1).strip()
    text = _LOG_PREFIX_RE.sub("", text, count=1).strip()
    return text


def _event_timestamp(row: FarmLogEntry) -> str:
    if row.event_at_source:
        return row.event_at_source
    if row.event_time:
        return row.event_time.replace(tzinfo=timezone.utc).isoformat()
    return ""


def _duration_label(text: str) -> str:
    match = _DURATION_RE.search(text)
    if not match:
        return ""

    hours, minutes, seconds = (int(part) for part in match.groups())
    parts = []
    if hours:
        parts.append(f"{hours} ч")
    if minutes:
        parts.append(f"{minutes} мин")
    if seconds or not parts:
        parts.append(f"{seconds} сек")
    return " ".join(parts)


def _make_event(
    row: FarmLogEntry,
    *,
    title: str,
    detail: str = "",
    tone: str = "info",
    key: str,
    scenario: str = "",
    phase: str = "",
) -> dict[str, Any]:
    return {
        "event_at": _event_timestamp(row),
        "title": title,
        "detail": detail,
        "tone": tone,
        "event_code": row.event_code or "system_message",
        "count": 1,
        "_key": key,
        "_scenario": scenario,
        "_phase": phase,
        "_event_time": row.event_time,
    }


def _translate_row(row: FarmLogEntry) -> dict[str, Any] | None:
    """Переводит только события, которые несут понятный пользователю результат."""

    event_text = str(row.event_text or "").strip()
    raw_text = _clean_raw_text(row.raw_text)
    source_text = raw_text or event_text
    code = str(row.event_code or "").lower()
    lower_text = source_text.lower()

    if any(pattern.match(source_text) for pattern in _HIDDEN_PATTERNS):
        return None

    if source_text.lower() == "preparing account":
        return _make_event(
            row,
            title="Бот вошел в аккаунт",
            detail="Начинается новый цикл действий.",
            tone="start",
            key="account:started",
        )

    if source_text.lower() == "account done":
        return _make_event(
            row,
            title="Сценарий завершен",
            detail="Бот успешно выполнил заданный цикл действий на аккаунте.",
            tone="complete",
            key="account:done",
        )

    running_match = _RUNNING_RE.match(source_text)
    if running_match:
        scenario = running_match.group(1).lower()
        label = _SCENARIOS.get(scenario)
        if not label:
            return None
        return _make_event(
            row,
            title=f"{label}: запуск",
            detail="Бот приступил к выполнению.",
            key=f"scenario:{scenario}:running",
            scenario=scenario,
            phase="running",
        )

    finished_match = _FINISHED_RE.search(source_text)
    if finished_match or code in {"finished", "gathervip_finished"}:
        if finished_match:
            scenario = finished_match.group(1).lower()
        elif code == "gathervip_finished":
            scenario = "gathervip"
        else:
            scenario = ""
        duration = _duration_label(source_text)
        detail = f"Выполнено за {duration}." if duration else "Действие успешно завершено."
        return _make_event(
            row,
            title=_SCENARIO_FINISHED_TITLES.get(scenario, "Сценарий завершён"),
            detail=detail,
            tone="success",
            key=f"scenario:{scenario}:finished",
            scenario=scenario,
            phase="finished",
        )

    if code == "send_troops" or source_text.lower() == "march: send troops":
        return _make_event(
            row,
            title="Отряд отправлен на сбор",
            detail="Марш успешно создан и отправлен.",
            tone="success",
            key="gather:troops-sent",
        )

    if code == "create_march_troop" or source_text.lower() == "march: create march troop":
        return None

    if code == "reached_max_marches" or source_text.lower() in {
        "marches: reached maximum of marches",
        "marches: no more marches left",
    }:
        return _make_event(
            row,
            title="Все марши заняты",
            detail="Новый отряд будет отправлен, когда освободится марш.",
            key="marches:full",
        )

    marches_match = re.match(r"^Marches:\s*(\d+)\s*/\s*(\d+)$", source_text, re.IGNORECASE)
    if marches_match:
        used, total = marches_match.groups()
        return _make_event(
            row,
            title=f"Занято маршей: {used} из {total}",
            key="marches:progress",
        )

    resource_match = re.match(
        r"^Gather:\s*(Farm|Sawmill|Quarry|Gold)\s+Level\s+(\d+)$",
        source_text,
        re.IGNORECASE,
    )
    if resource_match:
        resource, level = resource_match.groups()
        resource_label = {
            "farm": "еда",
            "sawmill": "дерево",
            "quarry": "камень",
            "gold": "золото",
        }[resource.lower()]
        return _make_event(
            row,
            title=f"Найдено: {resource_label} {level} уровня",
            key=f"gather:{resource.lower()}:{level}",
        )

    select_match = re.match(
        r"^Gather:\s*Select\s+(Farm|Sawmill|Quarry|Gold)$",
        source_text,
        re.IGNORECASE,
    )
    if select_match:
        resource = select_match.group(1).lower()
        resource_label = {
            "farm": "еды",
            "sawmill": "дерева",
            "quarry": "камня",
            "gold": "золота",
        }[resource]
        return _make_event(
            row,
            title=f"Выбран сбор {resource_label}",
            key=f"gather:select:{resource}",
        )

    if source_text.lower() == "speedup: queue is already full":
        return _make_event(
            row,
            title="Очередь строительства заполнена",
            detail="Дополнительное ускорение сейчас не требуется.",
            key="speedup:queue-full",
        )

    if source_text.lower() == "recruit: porter already in progress":
        return _make_event(
            row,
            title="Грузчики уже обучаются",
            key="recruit:porter-progress",
        )

    if source_text.lower() == "recruit: train porter":
        return _make_event(
            row,
            title="Запущено обучение грузчиков",
            tone="success",
            key="recruit:porter-started",
        )

    if source_text.lower() == "research: already in progress":
        return _make_event(
            row,
            title="Исследование уже выполняется",
            key="research:progress",
        )

    if source_text.lower() == "upgrade: building already in progress":
        return _make_event(
            row,
            title="Строительство уже выполняется",
            key="upgrade:progress",
        )

    if source_text.lower() == "alliance tech: no more donation left":
        return _make_event(
            row,
            title="Лимит пожертвований альянсу исчерпан",
            key="alliance:donation-limit",
        )

    if source_text.lower() == "alliance gift: crystal chest":
        return _make_event(
            row,
            title="Получен кристальный сундук альянса",
            tone="success",
            key="alliance:crystal-chest",
        )

    if "cannot find tile" in lower_text:
        return _make_event(
            row,
            title="Подходящая клетка ресурсов не найдена",
            detail="Бот повторит поиск в следующем цикле.",
            tone="warning",
            key="gather:tile-not-found",
        )

    if "cannot find" in lower_text or "failed" in lower_text:
        return _make_event(
            row,
            title="Не удалось выполнить действие",
            detail="Бот попробует восстановиться автоматически.",
            tone="warning",
            key="action:retry",
        )

    if code == "update_game_required" or "update the game" in lower_text:
        return _make_event(
            row,
            title="Требуется обновить игру",
            detail="До обновления отдельные действия могут быть недоступны.",
            tone="warning",
            key="system:update-required",
        )

    if str(row.level or "").lower() == "error" or code == "error":
        return _make_event(
            row,
            title="Бот столкнулся с ошибкой",
            detail="Выполняется автоматическое восстановление.",
            tone="error",
            key="system:error",
        )

    return None


def _seconds_between(left: dict[str, Any], right: dict[str, Any]) -> float | None:
    left_time = left.get("_event_time")
    right_time = right.get("_event_time")
    if not isinstance(left_time, datetime) or not isinstance(right_time, datetime):
        return None
    return abs((right_time - left_time).total_seconds())


def _compact_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Схлопывает повторы и заменяет пару «запуск → завершение» итоговым событием."""

    compact: list[dict[str, Any]] = []
    for event in events:
        scenario = event.get("_scenario")
        if scenario and event.get("_phase") == "finished":
            for index in range(len(compact) - 1, -1, -1):
                candidate = compact[index]
                if candidate.get("_scenario") != scenario or candidate.get("_phase") != "running":
                    continue
                elapsed = _seconds_between(candidate, event)
                if elapsed is None or elapsed <= 4 * 60 * 60:
                    compact.pop(index)
                break

        duplicate_index = None
        for index in range(len(compact) - 1, max(-1, len(compact) - 8), -1):
            candidate = compact[index]
            if candidate.get("_key") != event.get("_key"):
                continue
            elapsed = _seconds_between(candidate, event)
            if elapsed is None or elapsed <= 10 * 60:
                duplicate_index = index
            break

        if duplicate_index is not None:
            previous = compact.pop(duplicate_index)
            event["count"] = int(previous.get("count") or 1) + 1
        compact.append(event)

    return compact


def build_client_account_logs_payload(account: Account, *, limit: int = 30) -> dict[str, Any]:
    """Возвращает последние смысловые события без технических данных исходного лога."""

    safe_limit = max(1, min(int(limit or 30), 30))
    source_limit = min(max(safe_limit * 15, 150), 600)
    rows_desc = (
        FarmLogEntry.query.filter(FarmLogEntry.account_id == account.id)
        .filter(FarmLogEntry.level != "debug")
        .order_by(FarmLogEntry.event_time.desc().nullslast(), FarmLogEntry.id.desc())
        .limit(source_limit)
        .all()
    )

    translated = [event for row in reversed(rows_desc) if (event := _translate_row(row))]
    compact = _compact_events(translated)
    selected = list(reversed(compact[-safe_limit:]))

    for event in selected:
        for internal_key in ("_key", "_scenario", "_phase", "_event_time"):
            event.pop(internal_key, None)

    tones = {str(event.get("tone") or "info") for event in selected}
    if "error" in tones:
        status = "error"
    elif "warning" in tones:
        status = "warning"
    elif selected:
        status = "ok"
    else:
        status = "empty"

    last_activity_at = _event_timestamp(rows_desc[0]) if rows_desc else ""
    return {
        "ok": True,
        "account_id": account.id,
        "account_name": account.name,
        "server_name": account.server.name if account.server else None,
        "last_activity_at": last_activity_at,
        "status": status,
        "items": selected,
    }
