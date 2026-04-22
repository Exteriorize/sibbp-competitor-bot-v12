from __future__ import annotations

from typing import Dict, List

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup

from competitor_service import (
    build_competitor_snapshot,
    get_manual_review_priority_rows,
    get_portfolio_priority_rows,
    load_all_competitor_snapshots,
    summarize_all_competitors,
)
from competitors import DEFAULT_COMPETITOR_CODE, refresh_competitors
from config import BOT_TOKEN
from dynamics_report import create_dynamics_report, create_portfolio_dynamics_report, create_role_comparison_report
from history_store import get_competitor_history, get_portfolio_history, get_role_comparison_history, upsert_weekly_snapshot
from lifecycle_store import get_archive_items, get_recent_changes
from manual_store import (
    RELIABILITY_OPTIONS,
    ROOM_TYPE_OPTIONS,
    SOURCE_OPTIONS,
    STATUS_OPTIONS,
    delete_manual_competitor_data,
    delete_manual_item,
    list_manual_competitors_with_records,
    list_manual_items_for_competitor,
    load_manual_competitors,
    load_manual_items,
    save_manual_item,
    save_manual_record,
    upsert_manual_competitor,
)
from portfolio_report import create_portfolio_report
from report import create_report
from sib_parser import ParserError


if not BOT_TOKEN:
    raise RuntimeError(
        "Не указан BOT_TOKEN. Добавь новый токен в config.py или через переменную окружения BOT_TOKEN."
    )


bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

CHAT_COMPETITOR: Dict[int, str] = {}
FLOW: Dict[int, Dict[str, object]] = {}

MAIN_BUTTONS = {
    "Выбор объекта",
    "Проверить текущую сводку",
    "Выгрузить Excel",
    "Динамика",
    "Моя компания vs конкуренты",
    "Сводка по всей базе",
    "Excel по всей базе",
    "Добавить конкурента",
    "Добавить помещение",
    "Ручная ревизия",
    "Приоритет прозвона",
    "Архив",
    "Изменения",
    "Удалить помещение",
    "Удалить ручного конкурента",
}


def _escape(value) -> str:
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_rub(value: float) -> str:
    try:
        value = float(value or 0)
    except Exception:
        value = 0
    if not value:
        return "нет"
    if abs(value - round(value)) < 1e-9:
        text = f"{int(round(value)):,}".replace(",", " ")
    else:
        text = f"{value:,.2f}".replace(",", " ").replace(".", ",")
    return f"{text} ₽"


def _format_rub_m2(value: float) -> str:
    try:
        value = float(value or 0)
    except Exception:
        value = 0
    if not value:
        return "нет"
    if abs(value - round(value)) < 1e-9:
        text = f"{int(round(value)):,}".replace(",", " ")
    else:
        text = f"{value:,.2f}".replace(",", " ").replace(".", ",")
    return f"{text} ₽/м²"


def _parse_number(text: str) -> float:
    value = str(text).strip().replace(" ", "").replace(",", ".")
    return round(float(value), 2)


def _role_label(role: str) -> str:
    return "Моя компания" if role == "own_company" else "Конкурент"


def _main_keyboard() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.row(KeyboardButton("Выбор объекта"), KeyboardButton("Сводка по всей базе"))
    keyboard.row(KeyboardButton("Проверить текущую сводку"), KeyboardButton("Выгрузить Excel"))
    keyboard.row(KeyboardButton("Динамика"), KeyboardButton("Моя компания vs конкуренты"))
    keyboard.row(KeyboardButton("Excel по всей базе"), KeyboardButton("Приоритет прозвона"))
    keyboard.row(KeyboardButton("Добавить конкурента"), KeyboardButton("Добавить помещение"))
    keyboard.row(KeyboardButton("Ручная ревизия"), KeyboardButton("Удалить помещение"))
    keyboard.row(KeyboardButton("Изменения"), KeyboardButton("Архив"), KeyboardButton("Удалить ручного конкурента"))
    return keyboard


def _quick_actions_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Проверить", callback_data="qa:check"),
        InlineKeyboardButton("Excel", callback_data="qa:report"),
        InlineKeyboardButton("Динамика", callback_data="qa:dynamic"),
        InlineKeyboardButton("Сравнение", callback_data="qa:compare"),
    )
    kb.add(
        InlineKeyboardButton("Вся база", callback_data="qa:all"),
        InlineKeyboardButton("Ревизия", callback_data="qa:review"),
    )
    return kb


def _competitor_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    items = list(refresh_competitors().values())
    items.sort(key=lambda item: (0 if item.get("entity_role") == "own_company" else 1, str(item.get("name", "")).lower()))
    for item in items:
        if not item.get("enabled"):
            continue
        prefix = "🏢 " if item.get("entity_role") == "own_company" else "🏬 "
        kb.add(InlineKeyboardButton(f"{prefix}{item['name']}", callback_data=f"select:{item['code']}"))
    return kb


def _manual_competitor_keyboard(action: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for item in load_manual_competitors():
        kb.add(InlineKeyboardButton(item["name"], callback_data=f"{action}:{item['code']}"))
    return kb


def _room_type_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    for code, name in ROOM_TYPE_OPTIONS.items():
        kb.insert(InlineKeyboardButton(name, callback_data=f"roomtype:{code}"))
    return kb


def _source_keyboard(prefix: str = "source") -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    for code, name in SOURCE_OPTIONS.items():
        kb.insert(InlineKeyboardButton(name, callback_data=f"{prefix}:{code}"))
    return kb


def _status_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for code, name in STATUS_OPTIONS.items():
        kb.add(InlineKeyboardButton(name, callback_data=f"aggstatus:{code}"))
    return kb


def _reliability_keyboard(prefix: str = "reliability") -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=3)
    for code, name in RELIABILITY_OPTIONS.items():
        kb.insert(InlineKeyboardButton(name, callback_data=f"{prefix}:{code}"))
    return kb


def _delete_room_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    rows = sorted(load_manual_items(), key=lambda item: str(item.get("checked_at", "")), reverse=True)[:25]
    for item in rows:
        title = f"{item.get('competitor_name', '')} — {item.get('title', '')}"[:64]
        kb.add(InlineKeyboardButton(title, callback_data=f"delroom:{item['id']}"))
    return kb


def _delete_competitor_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for row in list_manual_competitors_with_records():
        label = f"{row['name']} ({row.get('room_count', 0)} пом.)"[:64]
        kb.add(InlineKeyboardButton(label, callback_data=f"delcomp:{row['code']}"))
    return kb


def _confirm_keyboard(kind: str, value: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Да", callback_data=f"confirm:{kind}:{value}"),
        InlineKeyboardButton("Отмена", callback_data="confirm:cancel:x"),
    )
    return kb


def _get_selected_competitor(chat_id: int) -> Dict[str, object]:
    code = CHAT_COMPETITOR.get(chat_id, DEFAULT_COMPETITOR_CODE)
    competitors = refresh_competitors()
    if code not in competitors:
        code = DEFAULT_COMPETITOR_CODE
        CHAT_COMPETITOR[chat_id] = code
    return competitors[code]


async def _load_selected(chat_id: int) -> Dict:
    competitor = _get_selected_competitor(chat_id)
    return build_competitor_snapshot(str(competitor["code"]), sync_state=True)


def _render_items(items, limit: int = 15) -> str:
    if not items:
        return "Ничего не найдено."
    lines: List[str] = []
    for item in items[:limit]:
        lines.append(
            f"• <b>{_escape(item.get('type', 'Не указан'))}</b> — {_escape(item.get('title', 'Без названия'))}\n"
            f"   Площадь: {item.get('area', 0)} м² | Цена/м²: {_format_rub_m2(item.get('price_value') or item.get('price_per_sqm') or 0)} | Всего: {_format_rub(item.get('total_price_value') or item.get('total_price') or 0)}"
            + (f"\n   Ссылка: {_escape(item.get('source_url') or item.get('url') or '')}" if (item.get('source_url') or item.get('url')) else "")
        )
    if len(items) > limit:
        lines.append(f"\n… и ещё {len(items) - limit} помещений")
    return "\n".join(lines)


def _render_changes(changes: List[Dict], limit: int = 10) -> str:
    if not changes:
        return "Изменений за 14 дней пока нет."
    lines = []
    for row in changes[:limit]:
        lines.append(
            f"• <b>{_escape(row.get('title', 'Без названия'))}</b> [{_escape(row.get('type', ''))}]\n"
            f"   {row.get('event_at', '')} — {_escape(row.get('event_type', ''))} | {_escape(row.get('note', ''))}"
        )
    return "\n".join(lines)


def _render_archive(rows: List[Dict], limit: int = 10) -> str:
    if not rows:
        return "Архив пуст."
    lines = []
    for row in rows[:limit]:
        lines.append(
            f"• <b>{_escape(row.get('title', 'Без названия'))}</b> [{_escape(row.get('type', ''))}]\n"
            f"   Статус: {_escape(row.get('status', ''))} | Последний раз найдено: {_escape(row.get('last_seen', ''))}"
        )
    return "\n".join(lines)


def _render_priority_rows(rows: List[Dict], limit: int = 8) -> str:
    if not rows:
        return "Сейчас нет объектов, которым срочно нужен прозвон."
    lines = []
    for row in rows[:limit]:
        if int(row.get("Балл", 0) or 0) <= 0:
            continue
        lines.append(
            f"• <b>{_escape(row.get('Конкурент', ''))}</b> — {_escape(row.get('Приоритет', ''))}\n"
            f"   Причины: {_escape(row.get('Причины', '—'))}\n"
            f"   Свежесть: {_escape(row.get('Свежесть', ''))} | Последняя проверка: {_escape(row.get('Последняя проверка', ''))}"
        )
    return "\n".join(lines) if lines else "Сейчас нет объектов, которым срочно нужен прозвон."


def _render_review_rows(rows: List[Dict], limit: int = 12) -> str:
    if not rows:
        return "Нет ручных помещений, требующих проверки."
    lines = []
    for row in rows[:limit]:
        lines.append(
            f"• <b>{_escape(row.get('Конкурент', ''))}</b> — {_escape(row.get('Помещение', ''))}\n"
            f"   {row.get('Статус', '')} | Возраст: {row.get('Возраст, дней', 0)} дн. | Источник: {_escape(row.get('Источник', ''))}"
        )
    return "\n".join(lines)


def _render_all_summary_text(snapshots: List[Dict]) -> str:
    stats = summarize_all_competitors(snapshots)
    own = stats.get("own_company", {})
    competitors = stats.get("competitors", {})
    lines = [
        "<b>Сводка по всей базе</b>",
        f"<b>Моя компания:</b> {own.get('count', 0)} помещ., {own.get('total_area', 0)} м², {own.get('avg_price', 0)} ₽/м²",
        f"<b>Конкуренты:</b> {competitors.get('count', 0)} помещ., {competitors.get('total_area', 0)} м², {competitors.get('avg_price', 0)} ₽/м²",
        f"Всего объектов в базе: {stats.get('competitors_total', 0)}",
        f"Всего помещений: {stats.get('count', 0)}",
        f"Всего площадь: {stats.get('total_area', 0)} м²",
        f"Всего стоимость: {_format_rub(stats.get('total_price', 0))}",
        f"Неподтвержденных объектов: {stats.get('unconfirmed_count', 0)}",
        f"Ручных помещений к проверке: {stats.get('review_items_count', 0)}",
        "",
        "<b>По объектам базы:</b>",
    ]
    for snapshot in snapshots:
        competitor = snapshot.get("competitor", {})
        role = _role_label(str(competitor.get("entity_role") or "competitor"))
        stats_row = snapshot.get("stats", {})
        freshness = snapshot.get("freshness", {})
        lines.append(
            f"• <b>{_escape(competitor.get('name', ''))}</b> [{role}] — {stats_row.get('count', 0)} помещ., {stats_row.get('total_area', 0)} м²"
            f" | свежесть: {_escape(freshness.get('freshness_label', ''))}"
        )
    lines.append("\n<b>Приоритет прозвона:</b>")
    lines.append(_render_priority_rows(get_portfolio_priority_rows(snapshots), limit=5))
    return "\n".join(lines)


async def _send_check(message: types.Message):
    await message.answer("Проверяю данные...")
    try:
        snapshot = await _load_selected(message.chat.id)
    except ParserError as exc:
        await message.answer(f"Парсер не смог получить помещения: {exc}", reply_markup=_main_keyboard())
        return
    except Exception as exc:
        await message.answer(f"Ошибка парсинга: {exc}", reply_markup=_main_keyboard())
        return

    competitor = snapshot["competitor"]
    stats = snapshot["stats"]
    freshness = snapshot.get("freshness", {})
    lifecycle = snapshot.get("lifecycle", {})
    role = _role_label(str(competitor.get("entity_role") or "competitor"))
    text = (
        f"<b>{role}: {_escape(competitor.get('short_name', competitor.get('name', '')))}</b>\n"
        f"Найдено помещений: {stats['count']}\n"
        f"Суммарная площадь: {stats['total_area']} м²\n"
        f"Средневзвешенная цена: {stats['avg_price']} ₽/м²\n"
        f"Суммарная стоимость: {_format_rub(stats['total_price'])}\n"
        f"Свежесть данных: {_escape(freshness.get('freshness_label', ''))}\n"
        f"Неподтвержденных объектов: {lifecycle.get('unconfirmed_count', 0)}\n"
        f"Приоритет прозвона: {_escape(snapshot.get('priority_label', 'Низкий'))}\n\n"
        f"<b>Примеры:</b>\n{_render_items(snapshot.get('items') or [])}"
    )
    await message.answer(text, reply_markup=_main_keyboard())
    await message.answer("Быстрые действия:", reply_markup=_quick_actions_keyboard())


async def _send_report(message: types.Message):
    await message.answer("Готовлю Excel...")
    try:
        snapshot = await _load_selected(message.chat.id)
        report_path = create_report(snapshot["items"], competitor=snapshot["competitor"], lifecycle=snapshot.get("lifecycle"))
    except Exception as exc:
        await message.answer(f"Не удалось собрать отчет: {exc}", reply_markup=_main_keyboard())
        return
    with open(report_path, "rb") as f:
        await message.answer_document(f, caption=f"Excel-отчет — {snapshot['competitor']['name']}", reply_markup=_main_keyboard())


async def _send_all_summary(message: types.Message):
    await message.answer("Собираю сводку по всей базе...")
    snapshots = load_all_competitor_snapshots(sync_state=True)
    await message.answer(_render_all_summary_text(snapshots), reply_markup=_main_keyboard())
    await message.answer("Быстрые действия:", reply_markup=_quick_actions_keyboard())


async def _send_all_report(message: types.Message):
    await message.answer("Готовлю Excel по всей базе...")
    snapshots = load_all_competitor_snapshots(sync_state=True)
    report_path = create_portfolio_report(snapshots)
    with open(report_path, "rb") as f:
        await message.answer_document(f, caption="Excel-отчет по всей базе", reply_markup=_main_keyboard())


async def _send_dynamic(message: types.Message):
    competitor = _get_selected_competitor(message.chat.id)
    snapshot = await _load_selected(message.chat.id)
    upsert_weekly_snapshot(
        str(competitor.get("code", "")),
        str(competitor.get("name", "")),
        snapshot.get("stats", {}),
        lifecycle=snapshot.get("lifecycle", {}),
        freshness=(snapshot.get("freshness", {}) or {}).get("freshness_label", ""),
        entity_role=str(competitor.get("entity_role") or "competitor"),
    )
    report_path = create_dynamics_report(get_competitor_history(str(competitor.get("code", ""))), str(competitor.get("name", "")))
    with open(report_path, "rb") as f:
        await message.answer_document(f, caption=f"Динамика — {competitor['name']}", reply_markup=_main_keyboard())


async def _send_compare(message: types.Message):
    await message.answer("Готовлю сравнение моей компании и конкурентов...")
    snapshots = load_all_competitor_snapshots(sync_state=True)
    for snapshot in snapshots:
        competitor = snapshot.get("competitor", {})
        upsert_weekly_snapshot(
            str(competitor.get("code", "")),
            str(competitor.get("name", "")),
            snapshot.get("stats", {}),
            lifecycle=snapshot.get("lifecycle", {}),
            freshness=(snapshot.get("freshness", {}) or {}).get("freshness_label", ""),
            entity_role=str(competitor.get("entity_role") or "competitor"),
        )
    report_path = create_role_comparison_report(get_role_comparison_history())
    with open(report_path, "rb") as f:
        await message.answer_document(f, caption="Сравнение: моя компания vs конкуренты", reply_markup=_main_keyboard())


async def _send_review(message: types.Message):
    rows = get_manual_review_priority_rows()
    await message.answer("<b>Ручная ревизия помещений</b>\n" + _render_review_rows(rows), reply_markup=_main_keyboard())


async def _send_changes(message: types.Message):
    snapshot = await _load_selected(message.chat.id)
    await message.answer(
        f"<b>Изменения за 14 дней — {_escape(snapshot['competitor']['name'])}</b>\n" + _render_changes(snapshot.get("recent_changes") or []),
        reply_markup=_main_keyboard(),
    )


async def _send_archive(message: types.Message):
    snapshot = await _load_selected(message.chat.id)
    await message.answer(
        f"<b>Архив / неподтвержденные объекты — {_escape(snapshot['competitor']['name'])}</b>\n" + _render_archive(snapshot.get("archive_items") or []),
        reply_markup=_main_keyboard(),
    )


async def _send_priority(message: types.Message):
    snapshots = load_all_competitor_snapshots(sync_state=True)
    await message.answer(
        "<b>Приоритет прозвона</b>\n" + _render_priority_rows(get_portfolio_priority_rows(snapshots)),
        reply_markup=_main_keyboard(),
    )


@dp.message_handler(commands=["start", "menu"])
async def start(message: types.Message):
    competitor = _get_selected_competitor(message.chat.id)
    text = (
        "База аналитики недвижимости запущена.\n\n"
        f"Текущий объект: <b>{_escape(competitor['name'])}</b> ({_role_label(str(competitor.get('entity_role') or 'competitor'))})\n\n"
        "Что умеет бот:\n"
        "• хранить общую базу по моей компании и конкурентам\n"
        "• парсить сайты и учитывать ручные помещения\n"
        "• сравнивать динамику моей компании и рынка\n"
        "• напоминать о ревизии ручных помещений\n"
        "• выгружать Excel по объекту и по всей базе"
    )
    await message.answer(text, reply_markup=_main_keyboard())
    await message.answer("Быстрые действия:", reply_markup=_quick_actions_keyboard())


@dp.message_handler(commands=["cancel"])
async def cancel(message: types.Message):
    FLOW.pop(message.chat.id, None)
    await message.answer("Активный ввод отменен.", reply_markup=_main_keyboard())


@dp.callback_query_handler(lambda c: c.data.startswith("qa:"))
async def quick_action(callback: types.CallbackQuery):
    action = callback.data.split(":", 1)[1]
    await callback.answer()
    if action == "check":
        await _send_check(callback.message)
    elif action == "report":
        await _send_report(callback.message)
    elif action == "dynamic":
        await _send_dynamic(callback.message)
    elif action == "compare":
        await _send_compare(callback.message)
    elif action == "all":
        await _send_all_summary(callback.message)
    elif action == "review":
        await _send_review(callback.message)


@dp.message_handler(lambda m: m.text == "Выбор объекта")
async def choose_competitor(message: types.Message):
    await message.answer("Выбери объект базы:", reply_markup=_competitor_keyboard())


@dp.callback_query_handler(lambda c: c.data.startswith("select:"))
async def select_competitor(callback: types.CallbackQuery):
    code = callback.data.split(":", 1)[1]
    competitors = refresh_competitors()
    if code not in competitors:
        await callback.answer("Неизвестный объект", show_alert=True)
        return
    CHAT_COMPETITOR[callback.message.chat.id] = code
    competitor = competitors[code]
    await callback.answer("Выбрано")
    await callback.message.answer(
        f"Текущий объект: <b>{_escape(competitor['name'])}</b> ({_role_label(str(competitor.get('entity_role') or 'competitor'))})",
        reply_markup=_main_keyboard(),
    )


@dp.message_handler(lambda m: m.text == "Проверить текущую сводку")
@dp.message_handler(commands=["check"])
async def cmd_check(message: types.Message):
    await _send_check(message)


@dp.message_handler(lambda m: m.text == "Выгрузить Excel")
@dp.message_handler(commands=["report"])
async def cmd_report(message: types.Message):
    await _send_report(message)


@dp.message_handler(lambda m: m.text == "Сводка по всей базе")
@dp.message_handler(commands=["all"])
async def cmd_all(message: types.Message):
    await _send_all_summary(message)


@dp.message_handler(lambda m: m.text == "Excel по всей базе")
@dp.message_handler(commands=["allreport"])
async def cmd_allreport(message: types.Message):
    await _send_all_report(message)


@dp.message_handler(lambda m: m.text == "Динамика")
@dp.message_handler(commands=["dynamic"])
async def cmd_dynamic(message: types.Message):
    await _send_dynamic(message)


@dp.message_handler(lambda m: m.text == "Моя компания vs конкуренты")
@dp.message_handler(commands=["compare"])
async def cmd_compare(message: types.Message):
    await _send_compare(message)


@dp.message_handler(lambda m: m.text == "Ручная ревизия")
@dp.message_handler(commands=["review"])
async def cmd_review(message: types.Message):
    await _send_review(message)


@dp.message_handler(lambda m: m.text == "Изменения")
@dp.message_handler(commands=["changes"])
async def cmd_changes(message: types.Message):
    await _send_changes(message)


@dp.message_handler(lambda m: m.text == "Архив")
@dp.message_handler(commands=["archive"])
async def cmd_archive(message: types.Message):
    await _send_archive(message)


@dp.message_handler(lambda m: m.text == "Приоритет прозвона")
@dp.message_handler(commands=["priority"])
async def cmd_priority(message: types.Message):
    await _send_priority(message)


@dp.message_handler(lambda m: m.text == "Добавить конкурента")
@dp.message_handler(commands=["add_competitor"])
async def add_competitor_start(message: types.Message):
    FLOW[message.chat.id] = {"state": "await_competitor_name", "data": {}}
    await message.answer("Введи название нового конкурента. Для отмены — /cancel", reply_markup=_main_keyboard())


@dp.message_handler(lambda m: m.text == "Добавить помещение")
@dp.message_handler(commands=["add_room"])
async def add_room_start(message: types.Message):
    manuals = load_manual_competitors()
    if not manuals:
        await message.answer("Сначала добавь конкурента через кнопку «Добавить конкурента».", reply_markup=_main_keyboard())
        return
    FLOW[message.chat.id] = {"state": "await_room_competitor", "data": {}}
    await message.answer("Выбери конкурента, к которому нужно добавить помещение:", reply_markup=_manual_competitor_keyboard("roomcomp"))


@dp.callback_query_handler(lambda c: c.data.startswith("roomcomp:"))
async def room_competitor_selected(callback: types.CallbackQuery):
    state = FLOW.get(callback.message.chat.id)
    if not state:
        await callback.answer("Сначала начни добавление помещения", show_alert=True)
        return
    code = callback.data.split(":", 1)[1]
    manuals = {item["code"]: item for item in load_manual_competitors()}
    if code not in manuals:
        await callback.answer("Конкурент не найден", show_alert=True)
        return
    state["data"]["competitor_code"] = code
    state["data"]["competitor_name"] = manuals[code]["name"]
    state["state"] = "await_room_title"
    await callback.answer("Конкурент выбран")
    await callback.message.answer(f"Введи название помещения для <b>{_escape(manuals[code]['name'])}</b>.")


@dp.callback_query_handler(lambda c: c.data.startswith("roomtype:"))
async def room_type_selected(callback: types.CallbackQuery):
    state = FLOW.get(callback.message.chat.id)
    if not state:
        await callback.answer("Нет активного ввода", show_alert=True)
        return
    code = callback.data.split(":", 1)[1]
    state["data"]["type"] = code
    state["data"]["type_label"] = ROOM_TYPE_OPTIONS.get(code, "Другое")
    state["state"] = "await_room_source"
    await callback.answer("Тип выбран")
    await callback.message.answer("Выбери источник:", reply_markup=_source_keyboard("roomsource"))


@dp.callback_query_handler(lambda c: c.data.startswith("roomsource:"))
async def room_source_selected(callback: types.CallbackQuery):
    state = FLOW.get(callback.message.chat.id)
    if not state:
        await callback.answer("Нет активного ввода", show_alert=True)
        return
    code = callback.data.split(":", 1)[1]
    state["data"]["source"] = code
    state["data"]["source_label"] = SOURCE_OPTIONS.get(code, "Другое")
    state["state"] = "await_room_link"
    await callback.answer("Источник выбран")
    await callback.message.answer("Отправь ссылку на помещение или '-' если ссылки нет.")


@dp.callback_query_handler(lambda c: c.data.startswith("reliability:"))
async def room_reliability_selected(callback: types.CallbackQuery):
    state = FLOW.get(callback.message.chat.id)
    if not state:
        await callback.answer("Нет активного ввода", show_alert=True)
        return
    code = callback.data.split(":", 1)[1]
    data = state.get("data", {})
    data["reliability"] = code
    data["reliability_label"] = RELIABILITY_OPTIONS.get(code, "Средняя")

    if state.get("state") == "await_room_reliability":
        item = save_manual_item(data)
        CHAT_COMPETITOR[callback.message.chat.id] = str(item["competitor_code"])
        FLOW.pop(callback.message.chat.id, None)
        await callback.answer("Помещение сохранено")
        await callback.message.answer(
            f"Сохранено помещение <b>{_escape(item['title'])}</b>\n"
            f"Конкурент: {_escape(item['competitor_name'])}\n"
            f"Тип: {_escape(item['type_label'])}\n"
            f"Площадь: {item['area']} м²\n"
            f"Ставка: {_format_rub_m2(item['price_per_sqm'])}\n"
            f"Всего: {_format_rub(item['total_price'])}",
            reply_markup=_main_keyboard(),
        )
        return

    if state.get("state") == "await_agg_reliability":
        record = save_manual_record(data)
        CHAT_COMPETITOR[callback.message.chat.id] = str(record["competitor_code"])
        FLOW.pop(callback.message.chat.id, None)
        await callback.answer("Сводка сохранена")
        await callback.message.answer(
            f"Ручная сводка сохранена для <b>{_escape(record['competitor_name'])}</b>",
            reply_markup=_main_keyboard(),
        )
        return


@dp.message_handler(lambda m: m.text == "Удалить помещение")
@dp.message_handler(commands=["delete_room"])
async def delete_room_start(message: types.Message):
    if not load_manual_items():
        await message.answer("Ручных помещений пока нет.", reply_markup=_main_keyboard())
        return
    await message.answer("Выбери помещение для удаления:", reply_markup=_delete_room_keyboard())


@dp.callback_query_handler(lambda c: c.data.startswith("delroom:"))
async def delete_room_selected(callback: types.CallbackQuery):
    item_id = callback.data.split(":", 1)[1]
    item_map = {item["id"]: item for item in load_manual_items()}
    item = item_map.get(item_id)
    if not item:
        await callback.answer("Помещение не найдено", show_alert=True)
        return
    await callback.answer()
    await callback.message.answer(
        f"Удалить помещение <b>{_escape(item['title'])}</b> у <b>{_escape(item['competitor_name'])}</b>?",
        reply_markup=_confirm_keyboard("room", item_id),
    )


@dp.message_handler(lambda m: m.text == "Удалить ручного конкурента")
@dp.message_handler(commands=["delete_competitor"])
async def delete_competitor_start(message: types.Message):
    rows = list_manual_competitors_with_records()
    if not rows:
        await message.answer("Ручных конкурентов пока нет.", reply_markup=_main_keyboard())
        return
    await message.answer("Выбери ручного конкурента для удаления:", reply_markup=_delete_competitor_keyboard())


@dp.callback_query_handler(lambda c: c.data.startswith("delcomp:"))
async def delete_competitor_selected(callback: types.CallbackQuery):
    code = callback.data.split(":", 1)[1]
    rows = {row["code"]: row for row in list_manual_competitors_with_records()}
    row = rows.get(code)
    if not row:
        await callback.answer("Конкурент не найден", show_alert=True)
        return
    await callback.answer()
    await callback.message.answer(
        f"Удалить ручного конкурента <b>{_escape(row['name'])}</b> вместе со всеми ручными помещениями?",
        reply_markup=_confirm_keyboard("comp", code),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("confirm:"))
async def confirm_action(callback: types.CallbackQuery):
    _, kind, value = callback.data.split(":", 2)
    if kind == "cancel":
        await callback.answer("Отменено")
        return
    if kind == "room":
        ok = delete_manual_item(value)
        await callback.answer("Удалено" if ok else "Не найдено")
        await callback.message.answer("Помещение удалено." if ok else "Помещение уже удалено.", reply_markup=_main_keyboard())
        return
    if kind == "comp":
        result = delete_manual_competitor_data(value)
        await callback.answer("Удалено")
        await callback.message.answer(
            f"Удалено записей: {result.get('deleted_items', 0)} помещений, {result.get('deleted_records', 0)} сводок.",
            reply_markup=_main_keyboard(),
        )
        return


@dp.message_handler(lambda m: m.chat.id in FLOW)
async def flow_text(message: types.Message):
    state = FLOW.get(message.chat.id)
    if not state:
        return
    text = (message.text or "").strip()
    if text in MAIN_BUTTONS:
        await message.answer("Сначала заверши ввод или отправь /cancel.")
        return
    data = state.setdefault("data", {})
    current = state.get("state")

    try:
        if current == "await_competitor_name":
            competitor = upsert_manual_competitor(text)
            FLOW.pop(message.chat.id, None)
            CHAT_COMPETITOR[message.chat.id] = competitor["code"]
            await message.answer(f"Добавлен конкурент <b>{_escape(competitor['name'])}</b>.", reply_markup=_main_keyboard())
            return

        if current == "await_room_title":
            data["title"] = text
            state["state"] = "await_room_type"
            await message.answer("Выбери тип помещения:", reply_markup=_room_type_keyboard())
            return

        if current == "await_room_link":
            data["source_url"] = "" if text == "-" else text
            state["state"] = "await_room_area"
            await message.answer("Введи площадь помещения в м². Например: 125,5")
            return

        if current == "await_room_area":
            data["area"] = _parse_number(text)
            state["state"] = "await_room_rate"
            await message.answer("Введи ставку за м² в рублях.")
            return

        if current == "await_room_rate":
            data["price_per_sqm"] = _parse_number(text)
            data["total_price"] = round(float(data.get("area", 0)) * float(data.get("price_per_sqm", 0)), 2)
            state["state"] = "await_room_comment"
            await message.answer("Добавь комментарий или отправь '-'.")
            return

        if current == "await_room_comment":
            data["comment"] = "" if text == "-" else text
            state["state"] = "await_room_reliability"
            await message.answer("Оцени достоверность данных:", reply_markup=_reliability_keyboard())
            return
    except ValueError:
        await message.answer("Не смог распознать число. Попробуй еще раз, например: 1250,5")
        return

    await message.answer("Сначала выбери вариант на кнопках или отправь /cancel.")
