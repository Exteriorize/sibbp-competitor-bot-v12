from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
from aiogram import types
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup

import bot_app
from analytics import summarize
from competitor_service import build_competitor_snapshot, load_all_competitor_snapshots, get_portfolio_priority_rows
from competitors import refresh_competitors
from history_store import get_role_comparison_history, read_history, upsert_weekly_snapshot
from manual_store import (
    ROOM_TYPE_OPTIONS,
    SOURCE_OPTIONS,
    clear_manual_items_for_competitor,
    load_manual_competitors,
    load_manual_items,
    save_manual_item,
    upsert_manual_competitor,
)

EXTRA_BUTTONS = {
    "Импорт Excel",
    "Шаблон Excel",
    "Начать новую проверку",
    "Выводы",
    "Доля рынка",
    "Рейтинг конкурентов",
    "Резкие изменения",
    "Что проверить сегодня",
}

TYPE_ALIASES = {
    "офис": "office",
    "офисы": "office",
    "office": "office",
    "склад": "warehouse",
    "склады": "warehouse",
    "warehouse": "warehouse",
    "универсальное": "universal",
    "свободного назначения": "universal",
    "псн": "universal",
    "производство": "production",
    "производственное": "production",
    "production": "production",
    "торговое": "retail",
    "торговля": "retail",
    "retail": "retail",
}

SOURCE_ALIASES = {
    "сайт": "site",
    "site": "site",
    "авито": "avito",
    "avito": "avito",
    "циан": "cian",
    "cian": "cian",
    "звонок": "call",
    "call": "call",
    "2гис": "2gis",
    "2gis": "2gis",
    "другое": "other",
    "other": "other",
}


def _escape(value) -> str:
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _num(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    text = str(value).strip().replace(" ", "").replace("₽", "").replace("р", "").replace(",", ".")
    try:
        return round(float(text), 2)
    except Exception:
        return 0.0


def _normalize_col(value: object) -> str:
    text = str(value or "").strip().lower().replace("ё", "е")
    for ch in [" ", "_", "-", ".", ",", ";", ":", "\n"]:
        text = text.replace(ch, "")
    return text


def _pick(row: pd.Series, columns: Dict[str, str], aliases: Iterable[str], default=""):
    for alias in aliases:
        col = columns.get(_normalize_col(alias))
        if col is None:
            continue
        value = row.get(col)
        if pd.isna(value):
            continue
        return value
    return default


def _room_type(value: object) -> Tuple[str, str]:
    text = str(value or "").strip().lower().replace("ё", "е")
    code = TYPE_ALIASES.get(text, "other")
    return code, ROOM_TYPE_OPTIONS.get(code, "Другое")


def _source(value: object) -> Tuple[str, str]:
    text = str(value or "").strip().lower().replace("ё", "е")
    code = SOURCE_ALIASES.get(text, "other")
    return code, SOURCE_OPTIONS.get(code, "Другое")


def _category_for_item(item: Dict) -> str:
    text = str(item.get("type") or item.get("type_label") or "").lower().replace("ё", "е")
    if any(word in text for word in ("офис", "универс", "свобод", "торгов", "retail", "office")):
        return "commercial"
    if any(word in text for word in ("склад", "производ", "warehouse", "production")):
        return "industrial"
    return "other"


def _format_area(value: float) -> str:
    return f"{round(float(value or 0), 1):,.1f}".replace(",", " ").replace(".", ",") + " м²"


def _format_money(value: float) -> str:
    return f"{round(float(value or 0)):,.0f}".replace(",", " ") + " ₽"


def _format_price(value: float) -> str:
    return f"{round(float(value or 0), 2):,.2f}".replace(",", " ").replace(".", ",") + " ₽/м²"


def _manual_competitor_keyboard(action: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    for item in load_manual_competitors():
        kb.add(InlineKeyboardButton(item["name"], callback_data=f"{action}:{item['code']}"))
    return kb


def _patch_keyboard() -> None:
    old_main_buttons = set(getattr(bot_app, "MAIN_BUTTONS", set()))
    bot_app.MAIN_BUTTONS = old_main_buttons | EXTRA_BUTTONS

    def enhanced_keyboard() -> ReplyKeyboardMarkup:
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.row(KeyboardButton("Выбор объекта"), KeyboardButton("Сводка по всей базе"))
        keyboard.row(KeyboardButton("Проверить текущую сводку"), KeyboardButton("Выгрузить Excel"))
        keyboard.row(KeyboardButton("Динамика"), KeyboardButton("Моя компания vs конкуренты"))
        keyboard.row(KeyboardButton("Excel по всей базе"), KeyboardButton("Приоритет прозвона"))
        keyboard.row(KeyboardButton("Выводы"), KeyboardButton("Доля рынка"))
        keyboard.row(KeyboardButton("Рейтинг конкурентов"), KeyboardButton("Резкие изменения"))
        keyboard.row(KeyboardButton("Что проверить сегодня"), KeyboardButton("Ручная ревизия"))
        keyboard.row(KeyboardButton("Добавить конкурента"), KeyboardButton("Добавить помещение"))
        keyboard.row(KeyboardButton("Импорт Excel"), KeyboardButton("Шаблон Excel"))
        keyboard.row(KeyboardButton("Начать новую проверку"), KeyboardButton("Удалить помещение"))
        keyboard.row(KeyboardButton("Изменения"), KeyboardButton("Архив"), KeyboardButton("Удалить ручного конкурента"))
        return keyboard

    bot_app._main_keyboard = enhanced_keyboard


def _save_current_snapshot(competitor_code: str) -> None:
    snapshot = build_competitor_snapshot(competitor_code, sync_state=True)
    competitor = snapshot.get("competitor", {})
    upsert_weekly_snapshot(
        str(competitor.get("code", competitor_code)),
        str(competitor.get("name", competitor_code)),
        snapshot.get("stats", {}),
        lifecycle=snapshot.get("lifecycle", {}),
        freshness=(snapshot.get("freshness", {}) or {}).get("freshness_label", ""),
        entity_role=str(competitor.get("entity_role") or "competitor"),
    )


def _latest_rows() -> pd.DataFrame:
    df = read_history()
    if df.empty:
        return df
    sort_cols = ["snapshot_date", "snapshot_datetime"] if "snapshot_datetime" in df.columns else ["snapshot_date"]
    rows = []
    for _, group in df.sort_values(sort_cols).groupby("competitor_code"):
        rows.append(group.iloc[-1].to_dict())
    return pd.DataFrame(rows)


def build_insights_text() -> str:
    df = _latest_rows()
    if df.empty:
        return "<b>Выводы</b>\nПока нет истории. Сначала сформируйте отчет «Моя компания vs конкуренты»."
    for c in ["count", "total_area", "avg_price", "total_price"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    own = df.loc[df.get("entity_role", "") == "own_company"]
    comps = df.loc[df.get("entity_role", "") != "own_company"]
    total_area = float(df["total_area"].sum() or 0)
    lines = ["<b>Выводы по рынку</b>"]

    if not own.empty and total_area > 0:
        own_area = float(own["total_area"].sum())
        lines.append(f"• Доля моей компании по свободной площади: <b>{round(own_area / total_area * 100, 1)}%</b> ({_format_area(own_area)} из {_format_area(total_area)}).")
    if not comps.empty:
        leader = comps.sort_values("total_area", ascending=False).iloc[0]
        lines.append(f"• Самый крупный конкурент по площади: <b>{_escape(leader['competitor_name'])}</b> — {_format_area(leader['total_area'])}.")
        price_leader = comps.loc[comps["avg_price"] > 0].sort_values("avg_price", ascending=False)
        if not price_leader.empty:
            row = price_leader.iloc[0]
            lines.append(f"• Самая высокая средняя ставка среди конкурентов: <b>{_escape(row['competitor_name'])}</b> — {_format_price(row['avg_price'])}.")
    stale = df[df.get("data_freshness", "").astype(str).str.contains("прозвон|нет актуальной|устар", case=False, na=False)] if "data_freshness" in df.columns else pd.DataFrame()
    if not stale.empty:
        lines.append(f"• Требуют внимания/проверки: <b>{len(stale)}</b> объект(а).")
    lines.append("• Для управленческого анализа смотрите листы по категориям: офисы/торговые отдельно от складов/производств.")
    return "\n".join(lines)


def build_market_share_text() -> str:
    snapshots = load_all_competitor_snapshots(sync_state=True)
    items = []
    for snap in snapshots:
        comp = snap.get("competitor", {})
        stats = snap.get("stats", {})
        items.append({
            "name": comp.get("name", ""),
            "role": comp.get("entity_role", "competitor"),
            "area": float(stats.get("total_area", 0) or 0),
            "count": int(stats.get("count", 0) or 0),
        })
    total_area = sum(x["area"] for x in items)
    lines = ["<b>Доля рынка по свободной площади</b>"]
    if total_area <= 0:
        return "\n".join(lines + ["Пока нет данных по площади."])
    for row in sorted(items, key=lambda x: x["area"], reverse=True):
        share = round(row["area"] / total_area * 100, 1) if total_area else 0
        prefix = "🏢" if row["role"] == "own_company" else "🏬"
        lines.append(f"• {prefix} <b>{_escape(row['name'])}</b>: {share}% — {_format_area(row['area'])}")
    return "\n".join(lines)


def build_ranking_text() -> str:
    snapshots = load_all_competitor_snapshots(sync_state=True)
    rows = []
    for snap in snapshots:
        comp = snap.get("competitor", {})
        stats = snap.get("stats", {})
        rows.append({
            "name": comp.get("name", ""),
            "area": float(stats.get("total_area", 0) or 0),
            "count": int(stats.get("count", 0) or 0),
            "avg_price": float(stats.get("avg_price", 0) or 0),
        })
    lines = ["<b>Рейтинг конкурентов</b>", "", "<b>По свободной площади:</b>"]
    for i, row in enumerate(sorted(rows, key=lambda x: x["area"], reverse=True), 1):
        lines.append(f"{i}. {_escape(row['name'])} — {_format_area(row['area'])}")
    lines.append("\n<b>По количеству помещений:</b>")
    for i, row in enumerate(sorted(rows, key=lambda x: x["count"], reverse=True), 1):
        lines.append(f"{i}. {_escape(row['name'])} — {row['count']} помещ.")
    lines.append("\n<b>По средней ставке:</b>")
    for i, row in enumerate(sorted([x for x in rows if x["avg_price"] > 0], key=lambda x: x["avg_price"], reverse=True), 1):
        lines.append(f"{i}. {_escape(row['name'])} — {_format_price(row['avg_price'])}")
    return "\n".join(lines)


def build_alerts_text() -> str:
    df = read_history()
    if df.empty:
        return "<b>Резкие изменения</b>\nПока нет истории для сравнения."
    for c in ["count", "total_area", "avg_price"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    lines = ["<b>Резкие изменения</b>"]
    alerts = []
    for _, group in df.sort_values(["snapshot_date", "snapshot_datetime"]).groupby("competitor_code"):
        if len(group) < 2:
            continue
        prev = group.iloc[-2]
        cur = group.iloc[-1]
        name = str(cur.get("competitor_name", cur.get("competitor_code", "")))
        for field, label, fmt, threshold in [
            ("total_area", "площадь", _format_area, 20),
            ("count", "количество помещений", lambda x: f"{int(x)}", 20),
            ("avg_price", "ставка", _format_price, 15),
        ]:
            old = float(prev.get(field, 0) or 0)
            new = float(cur.get(field, 0) or 0)
            if old <= 0:
                continue
            diff = round((new - old) / old * 100, 1)
            if abs(diff) >= threshold:
                sign = "+" if diff > 0 else ""
                alerts.append(f"• <b>{_escape(name)}</b>: {label} {sign}{diff}% ({fmt(old)} → {fmt(new)})")
    if not alerts:
        lines.append("Сильных изменений не найдено.")
    else:
        lines.extend(alerts[:12])
    return "\n".join(lines)


def build_today_check_text() -> str:
    snapshots = load_all_competitor_snapshots(sync_state=True)
    priority = get_portfolio_priority_rows(snapshots)
    rows = [x for x in priority if int(x.get("Балл", 0) or 0) > 0]
    lines = ["<b>Что проверить сегодня</b>"]
    if not rows:
        lines.append("Сейчас нет срочных проверок.")
    else:
        for row in rows[:10]:
            lines.append(f"• <b>{_escape(row.get('Конкурент', ''))}</b> — {_escape(row.get('Приоритет', ''))}\n  Причины: {_escape(row.get('Причины', '—'))}")
    return "\n".join(lines)


def _create_template(path: str) -> str:
    rows = [
        {
            "Конкурент": "Гранд Аренда",
            "Название помещения": "Офис 101",
            "Тип": "Офис",
            "Площадь, м²": 100,
            "Ставка, ₽/м²": 850,
            "Суммарная стоимость, ₽": "",
            "Источник": "Avito",
            "Ссылка": "https://example.com",
            "Комментарий": "пример строки",
        },
        {
            "Конкурент": "Гранд Аренда",
            "Название помещения": "Склад 1",
            "Тип": "Склад",
            "Площадь, м²": 500,
            "Ставка, ₽/м²": 450,
            "Суммарная стоимость, ₽": "",
            "Источник": "Звонок",
            "Ссылка": "-",
            "Комментарий": "пример строки",
        },
    ]
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_excel(path, index=False)
    return path


async def send_excel_template(message: types.Message):
    path = _create_template(str(Path("reports") / "manual_import_template.xlsx"))
    with open(path, "rb") as f:
        await message.answer_document(f, caption="Шаблон для массового импорта помещений")


async def import_start(message: types.Message):
    bot_app.FLOW[message.chat.id] = {"state": "await_import_competitor", "data": {}}
    await message.answer(
        "Выбери ручного конкурента для импорта или отправь Excel с колонкой «Конкурент», чтобы загрузить сразу несколько конкурентов.",
        reply_markup=_manual_competitor_keyboard("importcomp"),
    )


async def import_competitor_selected(callback: types.CallbackQuery):
    code = callback.data.split(":", 1)[1]
    manuals = {item["code"]: item for item in load_manual_competitors()}
    if code not in manuals:
        await callback.answer("Конкурент не найден", show_alert=True)
        return
    bot_app.FLOW[callback.message.chat.id] = {
        "state": "await_import_file",
        "data": {"competitor_code": code, "competitor_name": manuals[code]["name"]},
    }
    await callback.answer("Выбрано")
    await callback.message.answer(
        f"Отправь Excel-файл для <b>{_escape(manuals[code]['name'])}</b>.\n"
        "Старые актуальные помещения этого конкурента будут заменены новыми из файла."
    )


def _parse_excel(path: str, selected_competitor: Dict | None = None) -> Dict[str, int]:
    df = pd.read_excel(path)
    if df.empty:
        raise ValueError("Excel-файл пустой")
    columns = {_normalize_col(c): c for c in df.columns}
    imported = 0
    skipped = 0
    cleared: set[str] = set()

    for _, row in df.iterrows():
        selected_name = (selected_competitor or {}).get("competitor_name", "")
        comp_name = _pick(row, columns, ["Конкурент", "Компания", "Объект"], selected_name)
        comp_name = str(comp_name or "").strip()
        if not comp_name:
            skipped += 1
            continue
        competitor = upsert_manual_competitor(comp_name)
        code = competitor["code"]
        if selected_competitor and selected_competitor.get("competitor_code"):
            code = str(selected_competitor["competitor_code"])
            comp_name = str(selected_competitor["competitor_name"])
            competitor = {"code": code, "name": comp_name}
        if code not in cleared:
            try:
                _save_current_snapshot(code)
            except Exception:
                pass
            clear_manual_items_for_competitor(code)
            cleared.add(code)

        title = _pick(row, columns, ["Название помещения", "Помещение", "Название", "Объявление", "Адрес"])
        area = _num(_pick(row, columns, ["Площадь, м²", "Площадь", "м2", "м²"]))
        price = _num(_pick(row, columns, ["Ставка, ₽/м²", "Цена за м²", "Цена/м²", "Ставка", "Цена м2"]))
        total = _num(_pick(row, columns, ["Суммарная стоимость, ₽", "Стоимость", "Итого", "Сумма"]))
        room_type, room_label = _room_type(_pick(row, columns, ["Тип", "Категория", "Назначение"], "Другое"))
        source, source_label = _source(_pick(row, columns, ["Источник", "Площадка"], "Другое"))
        link = str(_pick(row, columns, ["Ссылка", "URL", "url"], "")).strip()
        if link == "-":
            link = ""
        comment = str(_pick(row, columns, ["Комментарий", "Примечание", "Описание"], "")).strip()

        if not str(title or "").strip() or area <= 0:
            skipped += 1
            continue
        save_manual_item({
            "competitor_code": code,
            "competitor_name": comp_name,
            "title": str(title).strip(),
            "type": room_type,
            "type_label": room_label,
            "area": area,
            "price_per_sqm": price,
            "total_price": total,
            "source": source,
            "source_label": source_label,
            "source_url": link,
            "comment": comment,
            "reliability": "medium",
            "reliability_label": "Средняя",
            "replace_existing": False,
        })
        imported += 1

    return {"imported": imported, "skipped": skipped, "competitors": len(cleared)}


async def handle_document(message: types.Message):
    state = bot_app.FLOW.get(message.chat.id) or {}
    if state.get("state") != "await_import_file":
        return
    document = message.document
    if not document:
        return
    filename = document.file_name or "import.xlsx"
    if not filename.lower().endswith((".xlsx", ".xls")):
        await message.answer("Нужен Excel-файл .xlsx или .xls")
        return
    Path("tmp").mkdir(exist_ok=True)
    local_path = str(Path("tmp") / f"manual_import_{message.chat.id}_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
    file_obj = await bot_app.bot.get_file(document.file_id)
    await bot_app.bot.download_file(file_obj.file_path, local_path)
    try:
        result = _parse_excel(local_path, state.get("data") or None)
    except Exception as exc:
        await message.answer(f"Не удалось импортировать Excel: {exc}")
        return
    finally:
        try:
            Path(local_path).unlink(missing_ok=True)
        except Exception:
            pass
    bot_app.FLOW.pop(message.chat.id, None)
    await message.answer(
        "Импорт завершен.\n"
        f"Загружено помещений: <b>{result['imported']}</b>\n"
        f"Пропущено строк: <b>{result['skipped']}</b>\n"
        f"Обновлено конкурентов: <b>{result['competitors']}</b>"
    )


async def reset_start(message: types.Message):
    await message.answer(
        "Выбери ручного конкурента, для которого начинается новая проверка.\n"
        "Старые актуальные помещения будут очищены, история сохранится в динамике.",
        reply_markup=_manual_competitor_keyboard("resetcomp"),
    )


async def reset_competitor(callback: types.CallbackQuery):
    code = callback.data.split(":", 1)[1]
    manuals = {item["code"]: item for item in load_manual_competitors()}
    if code not in manuals:
        await callback.answer("Конкурент не найден", show_alert=True)
        return
    try:
        _save_current_snapshot(code)
    except Exception:
        pass
    deleted = clear_manual_items_for_competitor(code)
    await callback.answer("Готово")
    await callback.message.answer(
        f"Новая проверка начата для <b>{_escape(manuals[code]['name'])}</b>.\n"
        f"Удалено старых актуальных помещений: <b>{deleted}</b>.\n"
        "Теперь можно добавлять новые помещения вручную или импортировать Excel."
    )


async def send_insights(message: types.Message):
    await message.answer(build_insights_text())


async def send_market_share(message: types.Message):
    await message.answer(build_market_share_text())


async def send_ranking(message: types.Message):
    await message.answer(build_ranking_text())


async def send_alerts(message: types.Message):
    await message.answer(build_alerts_text())


async def send_today_check(message: types.Message):
    await message.answer(build_today_check_text())


def setup_extra_features(dp, flow: Dict[int, Dict[str, object]]) -> None:
    _patch_keyboard()
    dp.register_message_handler(send_excel_template, lambda m: m.text == "Шаблон Excel")
    dp.register_message_handler(import_start, lambda m: m.text == "Импорт Excel")
    dp.register_callback_query_handler(import_competitor_selected, lambda c: c.data.startswith("importcomp:"))
    dp.register_message_handler(handle_document, content_types=[types.ContentType.DOCUMENT])
    dp.register_message_handler(reset_start, lambda m: m.text == "Начать новую проверку")
    dp.register_callback_query_handler(reset_competitor, lambda c: c.data.startswith("resetcomp:"))
    dp.register_message_handler(send_insights, lambda m: m.text == "Выводы")
    dp.register_message_handler(send_market_share, lambda m: m.text == "Доля рынка")
    dp.register_message_handler(send_ranking, lambda m: m.text == "Рейтинг конкурентов")
    dp.register_message_handler(send_alerts, lambda m: m.text == "Резкие изменения")
    dp.register_message_handler(send_today_check, lambda m: m.text == "Что проверить сегодня")
