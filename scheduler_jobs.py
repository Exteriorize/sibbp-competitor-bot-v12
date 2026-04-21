from __future__ import annotations

from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz

from competitor_service import get_manual_review_priority_rows, get_portfolio_priority_rows, load_all_competitor_snapshots, summarize_all_competitors
from config import CHAT_ID
from dynamics_report import create_portfolio_dynamics_report, create_role_comparison_report
from history_store import get_portfolio_history, get_role_comparison_history, upsert_weekly_snapshot
from portfolio_report import create_portfolio_report


MOSCOW_TZ = pytz.timezone("Europe/Moscow")


def _format_rub(value: float) -> str:
    if not value:
        return "нет"
    if abs(value - round(value)) < 1e-9:
        text = f"{int(round(value)):,}".replace(",", " ")
    else:
        text = f"{value:,.2f}".replace(",", " ").replace(".", ",")
    return f"{text} ₽"


def _render_summary_text(stats, priority_rows) -> str:
    own = stats.get("own_company", {})
    competitors = stats.get("competitors", {})
    lines = [
        "<b>Автоматическая сводка по всей базе</b>",
        f"Моя компания: {own.get('count', 0)} помещ., {own.get('total_area', 0)} м², {own.get('avg_price', 0)} ₽/м²",
        f"Конкуренты: {competitors.get('count', 0)} помещ., {competitors.get('total_area', 0)} м², {competitors.get('avg_price', 0)} ₽/м²",
        f"Всего объектов в базе: {stats['competitors_total']}",
        f"Всего найдено помещений: {stats['count']}",
        f"Всего суммарная площадь: {stats['total_area']} м²",
        f"Всего суммарная стоимость: {_format_rub(stats['total_price'])}",
        f"Неподтвержденных объектов: {stats['unconfirmed_count']}",
        f"Ручных помещений к проверке: {stats.get('review_items_count', 0)}",
    ]
    if priority_rows:
        lines.append("\n<b>Кого проверить в первую очередь:</b>")
        for row in priority_rows[:5]:
            if row.get("Балл", 0) <= 0:
                continue
            lines.append(f"• <b>{row['Конкурент']}</b> — {row['Приоритет']}: {row['Причины']}")
    return "\n".join(lines)


async def _snapshot_all() -> list:
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


async def send_scheduled_summary(bot: Bot) -> None:
    if not CHAT_ID:
        return

    snapshots = await _snapshot_all()
    stats = summarize_all_competitors(snapshots)
    priority_rows = get_portfolio_priority_rows(snapshots)
    report_path = create_portfolio_report(snapshots)
    await bot.send_message(CHAT_ID, _render_summary_text(stats, priority_rows))
    with open(report_path, "rb") as report_file:
        await bot.send_document(CHAT_ID, report_file, caption="Автоматический Excel-отчет по всей базе")


async def send_monday_dynamics(bot: Bot) -> None:
    if not CHAT_ID:
        return

    await _snapshot_all()
    history = get_portfolio_history()
    report_path = create_portfolio_dynamics_report(history)
    comparison_path = create_role_comparison_report(get_role_comparison_history())
    with open(report_path, "rb") as report_file:
        await bot.send_document(CHAT_ID, report_file, caption="Динамика свободных площадей — вся база")
    with open(comparison_path, "rb") as report_file:
        await bot.send_document(CHAT_ID, report_file, caption="Сравнение: моя компания vs конкуренты")


async def send_call_priority_reminder(bot: Bot) -> None:
    if not CHAT_ID:
        return

    snapshots = load_all_competitor_snapshots(sync_state=True)
    priority_rows = [row for row in get_portfolio_priority_rows(snapshots) if int(row.get("Балл", 0) or 0) > 0]
    review_rows = get_manual_review_priority_rows()
    if not priority_rows and not review_rows:
        return

    lines = ["<b>Напоминание по ручной верификации</b>", "Ручные данные рекомендуется актуализировать раз в 2 недели.", ""]
    for row in priority_rows[:5]:
        lines.append(f"• <b>{row['Конкурент']}</b> — {row['Приоритет']}: {row['Причины']}")
    if review_rows:
        lines.append("\n<b>Помещения, которые пора проверить:</b>")
        for row in review_rows[:7]:
            lines.append(f"• <b>{row['Конкурент']}</b> — {row['Помещение']} ({row['Возраст, дней']} дн.) | {row['Статус']}")
    await bot.send_message(CHAT_ID, "\n".join(lines))


async def on_startup_scheduler(dispatcher) -> None:
    scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)
    bot = dispatcher.bot

    scheduler.add_job(send_scheduled_summary, "cron", day_of_week="mon,thu", hour=10, minute=0, args=[bot], id="scheduled_summary", replace_existing=True)
    scheduler.add_job(send_monday_dynamics, "cron", day_of_week="mon", hour=10, minute=20, args=[bot], id="monday_dynamics", replace_existing=True)
    scheduler.add_job(send_call_priority_reminder, "cron", day_of_week="mon", hour=9, minute=40, args=[bot], id="call_priority_reminder", replace_existing=True)

    scheduler.start()
    dispatcher["scheduler"] = scheduler


async def on_shutdown_scheduler(dispatcher) -> None:
    scheduler = dispatcher.get("scheduler")
    if scheduler:
        scheduler.shutdown(wait=False)
