from __future__ import annotations

import os
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Set
from urllib.parse import urlencode

import pandas as pd
from aiogram import types
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup

import bot_app
from access_control import director_ids
from competitor_service import get_portfolio_priority_rows, load_all_competitor_snapshots, summarize_all_competitors
from config import CHAT_ID
from dynamics_report import create_role_comparison_report
from history_store import get_role_comparison_history, read_history, upsert_weekly_snapshot
from manual_store import load_manual_competitors, load_manual_items
from portfolio_report import create_portfolio_report

MORE_BUTTONS = {
    "Открыть сайт",
    "Обновить сайт",
    "Отправить руководителю",
    "Отчет за неделю",
    "Проблемные данные",
    "Проверить дубли",
    "Скачать базу",
    "Список конкурентов",
}


def _escape(value) -> str:
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_area(value: float) -> str:
    return f"{round(float(value or 0), 1):,.1f}".replace(",", " ").replace(".", ",") + " м²"


def _format_money(value: float) -> str:
    return f"{round(float(value or 0)):,.0f}".replace(",", " ") + " ₽"


def _format_price(value: float) -> str:
    return f"{round(float(value or 0), 2):,.2f}".replace(",", " ").replace(".", ",") + " ₽/м²"


def _parse_ids(value: str) -> Set[int]:
    result: Set[int] = set()
    for part in str(value or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            continue
    return result


def _dashboard_url() -> str:
    base = os.getenv("DASHBOARD_URL") or os.getenv("RENDER_EXTERNAL_URL") or "https://sibbp-competitor-bot-v12.onrender.com"
    base = base.rstrip("/") + "/"
    token = os.getenv("DASHBOARD_TOKEN", "").strip()
    if token:
        return base + "?" + urlencode({"key": token, "refresh": "1"})
    return base + "?refresh=1"


def _report_recipients() -> List[int]:
    ids: Set[int] = set()
    ids.update(_parse_ids(os.getenv("REPORT_CHAT_IDS", "")))
    ids.update(_parse_ids(os.getenv("LEADER_CHAT_IDS", "")))
    ids.update(director_ids())
    if CHAT_ID:
        ids.add(int(CHAT_ID))
    return sorted(ids)


def _snapshot_all() -> List[Dict]:
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
    return snapshots


def _summary_text(snapshots: List[Dict]) -> str:
    stats = summarize_all_competitors(snapshots)
    own = stats.get("own_company", {})
    competitors = stats.get("competitors", {})
    priority_rows = get_portfolio_priority_rows(snapshots)
    lines = [
        "<b>Еженедельная сводка по конкурентам</b>",
        f"Моя компания: <b>{own.get('count', 0)}</b> помещ., <b>{own.get('total_area', 0)}</b> м², <b>{own.get('avg_price', 0)}</b> ₽/м²",
        f"Конкуренты: <b>{competitors.get('count', 0)}</b> помещ., <b>{competitors.get('total_area', 0)}</b> м², <b>{competitors.get('avg_price', 0)}</b> ₽/м²",
        f"Всего объектов в базе: <b>{stats.get('competitors_total', 0)}</b>",
        f"Всего помещений: <b>{stats.get('count', 0)}</b>",
        f"Всего площадь: <b>{stats.get('total_area', 0)}</b> м²",
        f"Суммарная стоимость: <b>{_format_money(stats.get('total_price', 0))}</b>",
        "",
        f"Онлайн-дашборд: {_dashboard_url()}",
    ]
    rows = [row for row in priority_rows if int(row.get("Балл", 0) or 0) > 0]
    if rows:
        lines.append("\n<b>Что проверить:</b>")
        for row in rows[:5]:
            lines.append(f"• <b>{_escape(row.get('Конкурент', ''))}</b> — {_escape(row.get('Приоритет', ''))}: {_escape(row.get('Причины', '—'))}")
    return "\n".join(lines)


def _weekly_change_text() -> str:
    df = read_history()
    if df.empty or "snapshot_date" not in df.columns:
        return "Пока нет истории для недельного сравнения."
    df = df.copy()
    for col in ["count", "total_area", "avg_price", "total_price"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0) if col in df.columns else 0
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    max_date = df["snapshot_date"].max()
    if pd.isna(max_date):
        return "Пока нет корректных дат для недельного сравнения."
    prev_cutoff = max_date - timedelta(days=7)
    lines = ["<b>Отчет за неделю</b>"]
    for code, group in df.sort_values("snapshot_date").groupby("competitor_code"):
        cur = group.iloc[-1]
        prev_group = group.loc[group["snapshot_date"] <= prev_cutoff]
        if prev_group.empty:
            continue
        prev = prev_group.iloc[-1]
        name = str(cur.get("competitor_name", code))
        area_diff = float(cur.get("total_area", 0) or 0) - float(prev.get("total_area", 0) or 0)
        count_diff = int(cur.get("count", 0) or 0) - int(prev.get("count", 0) or 0)
        price_diff = float(cur.get("avg_price", 0) or 0) - float(prev.get("avg_price", 0) or 0)
        lines.append(
            f"• <b>{_escape(name)}</b>: площадь {area_diff:+.1f} м², помещения {count_diff:+d}, ставка {price_diff:+.2f} ₽/м²"
        )
    if len(lines) == 1:
        lines.append("Недостаточно данных старше 7 дней для сравнения.")
    lines.append(f"\nПолный дашборд: {_dashboard_url()}")
    return "\n".join(lines)


def _problem_rows() -> List[str]:
    lines: List[str] = []
    snapshots = load_all_competitor_snapshots(sync_state=True)
    for snapshot in snapshots:
        comp = snapshot.get("competitor", {})
        stats = snapshot.get("stats", {})
        freshness = snapshot.get("freshness", {})
        error = snapshot.get("error")
        if error:
            lines.append(f"• <b>{_escape(comp.get('name', ''))}</b>: ошибка загрузки — {_escape(error)}")
        if int(stats.get("count", 0) or 0) == 0:
            lines.append(f"• <b>{_escape(comp.get('name', ''))}</b>: 0 активных помещений")
        if str(freshness.get("freshness", "")) == "stale":
            lines.append(f"• <b>{_escape(comp.get('name', ''))}</b>: данные требуют прозвона")
    for item in load_manual_items():
        name = item.get("competitor_name", "")
        title = item.get("title", "")
        area = float(item.get("area", 0) or 0)
        price = float(item.get("price_per_sqm", 0) or 0)
        link = str(item.get("source_url", "") or "").strip()
        if area <= 0:
            lines.append(f"• <b>{_escape(name)}</b> — {_escape(title)}: нет площади")
        if price <= 0:
            lines.append(f"• <b>{_escape(name)}</b> — {_escape(title)}: нет ставки")
        if not link:
            lines.append(f"• <b>{_escape(name)}</b> — {_escape(title)}: нет ссылки")
        if area > 5000:
            lines.append(f"• <b>{_escape(name)}</b> — {_escape(title)}: очень большая площадь ({_format_area(area)})")
        if 0 < price < 100:
            lines.append(f"• <b>{_escape(name)}</b> — {_escape(title)}: подозрительно низкая ставка ({_format_price(price)})")
    return lines[:30]


def _duplicate_rows() -> List[str]:
    items = load_manual_items()
    groups: Dict[str, List[Dict]] = {}
    for item in items:
        key = "|".join([
            str(item.get("competitor_code", "")),
            str(item.get("title", "")).strip().lower(),
            str(round(float(item.get("area", 0) or 0), 1)),
            str(item.get("source_url", "")).strip().lower(),
        ])
        groups.setdefault(key, []).append(item)
    lines = []
    for rows in groups.values():
        if len(rows) < 2:
            continue
        first = rows[0]
        lines.append(f"• <b>{_escape(first.get('competitor_name', ''))}</b> — {_escape(first.get('title', ''))}: дублей {len(rows)}")
    return lines[:30]


def _competitors_text() -> str:
    snapshots = load_all_competitor_snapshots(sync_state=True)
    lines = ["<b>Список конкурентов / объектов</b>"]
    for snapshot in snapshots:
        comp = snapshot.get("competitor", {})
        stats = snapshot.get("stats", {})
        freshness = snapshot.get("freshness", {})
        role = "Моя компания" if comp.get("entity_role") == "own_company" else "Конкурент"
        mode = "парсер" if comp.get("mode") == "parsed" else "ручной ввод"
        lines.append(
            f"• <b>{_escape(comp.get('name', ''))}</b> [{role}, {mode}] — {stats.get('count', 0)} помещ., {stats.get('total_area', 0)} м² | {_escape(freshness.get('freshness_label', ''))}"
        )
    return "\n".join(lines)


async def open_site(message: types.Message):
    await message.answer(f"<b>Онлайн-дашборд</b>\n{_dashboard_url()}")


async def refresh_site(message: types.Message):
    await message.answer("Обновляю данные для сайта...")
    _snapshot_all()
    await message.answer(f"Дашборд обновлен.\n{_dashboard_url()}")


async def send_to_manager(message: types.Message):
    await message.answer("Готовлю отчет для руководителя...")
    snapshots = _snapshot_all()
    text = _summary_text(snapshots)
    portfolio_path = create_portfolio_report(snapshots)
    comparison_path = create_role_comparison_report(get_role_comparison_history())
    recipients = _report_recipients()
    sent = 0
    for chat_id in recipients:
        try:
            await bot_app.bot.send_message(chat_id, text)
            with open(portfolio_path, "rb") as f:
                await bot_app.bot.send_document(chat_id, f, caption="Excel-отчет по всей базе")
            with open(comparison_path, "rb") as f:
                await bot_app.bot.send_document(chat_id, f, caption="Моя компания vs конкуренты")
            sent += 1
        except Exception as exc:
            await message.answer(f"Не удалось отправить на {chat_id}: {exc}")
    await message.answer(f"Отчет отправлен получателям: {sent}")


async def week_report(message: types.Message):
    _snapshot_all()
    await message.answer(_weekly_change_text())


async def problem_data(message: types.Message):
    rows = _problem_rows()
    await message.answer("<b>Проблемные данные</b>\n" + ("\n".join(rows) if rows else "Проблемных данных не найдено."))


async def check_duplicates(message: types.Message):
    rows = _duplicate_rows()
    await message.answer("<b>Проверка дублей</b>\n" + ("\n".join(rows) if rows else "Дубли не найдены."))


async def download_database(message: types.Message):
    Path("reports").mkdir(exist_ok=True)
    path = Path("reports") / f"database_backup_{datetime.now():%Y-%m-%d_%H-%M}.zip"
    files = [
        Path("data/manual_competitors.json"),
        Path("data/manual_items.json"),
        Path("data/manual_records.json"),
        Path("data/history.csv"),
        Path("data/category_history.csv"),
        Path("data/item_registry.csv"),
        Path("data/change_log.csv"),
    ]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for file in files:
            if file.exists():
                zf.write(file, file.as_posix())
    with open(path, "rb") as f:
        await message.answer_document(f, caption="Резервная копия базы")


async def list_competitors(message: types.Message):
    await message.answer(_competitors_text())


def setup_more_buttons(dp, flow: Dict[int, Dict[str, object]]) -> None:
    old_keyboard = bot_app._main_keyboard
    bot_app.MAIN_BUTTONS = set(getattr(bot_app, "MAIN_BUTTONS", set())) | MORE_BUTTONS

    def enhanced_keyboard() -> ReplyKeyboardMarkup:
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.row(KeyboardButton("Открыть сайт"), KeyboardButton("Обновить сайт"))
        keyboard.row(KeyboardButton("Отправить руководителю"), KeyboardButton("Отчет за неделю"))
        keyboard.row(KeyboardButton("Проблемные данные"), KeyboardButton("Проверить дубли"))
        keyboard.row(KeyboardButton("Список конкурентов"), KeyboardButton("Скачать базу"))
        base = old_keyboard()
        for row in base.keyboard:
            keyboard.row(*row)
        return keyboard

    bot_app._main_keyboard = enhanced_keyboard
    dp.register_message_handler(open_site, lambda m: m.text == "Открыть сайт")
    dp.register_message_handler(refresh_site, lambda m: m.text == "Обновить сайт")
    dp.register_message_handler(send_to_manager, lambda m: m.text == "Отправить руководителю")
    dp.register_message_handler(week_report, lambda m: m.text == "Отчет за неделю")
    dp.register_message_handler(problem_data, lambda m: m.text == "Проблемные данные")
    dp.register_message_handler(check_duplicates, lambda m: m.text == "Проверить дубли")
    dp.register_message_handler(download_database, lambda m: m.text == "Скачать базу")
    dp.register_message_handler(list_competitors, lambda m: m.text == "Список конкурентов")
