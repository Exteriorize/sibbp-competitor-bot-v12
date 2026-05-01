from __future__ import annotations

import os
from functools import wraps
from typing import Callable, Dict, Iterable, Set

from aiogram import types

try:
    from config import CHAT_ID
except Exception:
    CHAT_ID = 0


ADMIN = "admin"
EDITOR = "editor"
DIRECTOR = "director"
DENIED = "denied"


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


ADMIN_IDS = _parse_ids(os.getenv("ADMIN_IDS", str(CHAT_ID or "")))
EDITOR_IDS = _parse_ids(os.getenv("EDITOR_IDS", ""))
DIRECTOR_IDS = _parse_ids(os.getenv("DIRECTOR_IDS", ""))


ROLE_LABELS = {
    ADMIN: "Админ",
    EDITOR: "Заполняющий",
    DIRECTOR: "Директор",
    DENIED: "Нет доступа",
}


ADMIN_TEXTS = {
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


EDITOR_TEXTS = {
    "Выбор объекта",
    "Проверить текущую сводку",
    "Добавить конкурента",
    "Добавить помещение",
    "Ручная ревизия",
    "Приоритет прозвона",
    "Удалить помещение",
}


DIRECTOR_TEXTS = {
    "Выбор объекта",
    "Проверить текущую сводку",
    "Выгрузить Excel",
    "Динамика",
    "Моя компания vs конкуренты",
    "Сводка по всей базе",
    "Excel по всей базе",
    "Приоритет прозвона",
    "Архив",
    "Изменения",
}


COMMON_COMMANDS = {"/start", "/menu", "/cancel", "/myid"}


EDITOR_CALLBACK_PREFIXES = (
    "select:",
    "qa:check",
    "qa:review",
    "roomcomp:",
    "roomtype:",
    "roomsource:",
    "reliability:",
    "source:",
    "aggstatus:",
    "aggreliability:",
    "delroom:",
    "confirm:cancel:",
    "confirm:room:",
    "confirm:delroom:",
)


DIRECTOR_CALLBACK_PREFIXES = (
    "select:",
    "qa:check",
    "qa:report",
    "qa:dynamic",
    "qa:compare",
    "qa:all",
)


def get_role(user_id: int) -> str:
    user_id = int(user_id or 0)
    if user_id in ADMIN_IDS:
        return ADMIN
    if user_id in EDITOR_IDS:
        return EDITOR
    if user_id in DIRECTOR_IDS:
        return DIRECTOR
    return DENIED


def _is_command(text: str, command: str) -> bool:
    text = str(text or "").strip()
    return text == command or text.startswith(command + "@")


def _has_active_flow(flow: Dict[int, Dict[str, object]], user_id: int) -> bool:
    return bool(flow.get(int(user_id or 0)))


def is_message_allowed(user_id: int, text: str, flow: Dict[int, Dict[str, object]]) -> bool:
    text = str(text or "").strip()
    role = get_role(user_id)

    if _is_command(text, "/myid"):
        return True
    if role == DENIED:
        return False
    if role == ADMIN:
        return True
    if text in COMMON_COMMANDS:
        return True
    if _has_active_flow(flow, user_id) and role == EDITOR:
        return True
    if role == EDITOR:
        return text in EDITOR_TEXTS
    if role == DIRECTOR:
        return text in DIRECTOR_TEXTS
    return False


def is_callback_allowed(user_id: int, data: str, flow: Dict[int, Dict[str, object]]) -> bool:
    data = str(data or "")
    role = get_role(user_id)

    if role == DENIED:
        return False
    if role == ADMIN:
        return True
    if _has_active_flow(flow, user_id) and role == EDITOR:
        return True
    if role == EDITOR:
        return data.startswith(EDITOR_CALLBACK_PREFIXES)
    if role == DIRECTOR:
        return data.startswith(DIRECTOR_CALLBACK_PREFIXES)
    return False


def role_menu_hint(user_id: int) -> str:
    role = get_role(user_id)
    if role == ADMIN:
        return "Доступ: Админ. Доступны все функции."
    if role == EDITOR:
        return "Доступ: Заполняющий. Можно добавлять конкурентов, помещения, удалять ошибочные помещения и смотреть текущую сводку."
    if role == DIRECTOR:
        return "Доступ: Директор. Доступен просмотр статистики, динамики и Excel-отчетов."
    return "Доступ не выдан. Отправьте этот Telegram ID администратору."


def allowed_buttons_for_role(user_id: int, all_buttons: Iterable[str]) -> Set[str]:
    role = get_role(user_id)
    if role == ADMIN:
        return set(all_buttons)
    if role == EDITOR:
        return set(EDITOR_TEXTS)
    if role == DIRECTOR:
        return set(DIRECTOR_TEXTS)
    return set()


async def send_myid(message: types.Message) -> None:
    user = message.from_user
    user_id = user.id if user else 0
    await message.answer(
        f"Ваш Telegram ID: <code>{user_id}</code>\n"
        f"Роль: <b>{ROLE_LABELS.get(get_role(user_id), 'Нет доступа')}</b>\n"
        f"{role_menu_hint(user_id)}"
    )


async def deny_message(message: types.Message) -> None:
    user = message.from_user
    user_id = user.id if user else 0
    await message.answer(
        "⛔ У вас нет доступа к этому разделу.\n\n"
        f"Ваш Telegram ID: <code>{user_id}</code>\n"
        "Отправьте этот ID администратору, чтобы он добавил вас в нужный профиль."
    )


async def deny_callback(callback: types.CallbackQuery) -> None:
    await callback.answer("У вас нет доступа к этому разделу", show_alert=True)


def apply_access_control(dp, flow: Dict[int, Dict[str, object]]) -> None:
    """Wrap already registered aiogram handlers with role checks.

    This keeps the existing bot_app.py almost unchanged and applies access rules
    centrally after all handlers are registered.
    """

    def wrap_message_handler(fn: Callable):
        if getattr(fn, "_access_wrapped", False):
            return fn

        @wraps(fn)
        async def wrapped(message: types.Message, *args, **kwargs):
            text = str(message.text or "").strip()
            user_id = message.from_user.id if message.from_user else 0
            if _is_command(text, "/myid"):
                await send_myid(message)
                return
            if not is_message_allowed(user_id, text, flow):
                await deny_message(message)
                return
            return await fn(message, *args, **kwargs)

        wrapped._access_wrapped = True
        return wrapped

    def wrap_callback_handler(fn: Callable):
        if getattr(fn, "_access_wrapped", False):
            return fn

        @wraps(fn)
        async def wrapped(callback: types.CallbackQuery, *args, **kwargs):
            user_id = callback.from_user.id if callback.from_user else 0
            if not is_callback_allowed(user_id, callback.data or "", flow):
                await deny_callback(callback)
                return
            return await fn(callback, *args, **kwargs)

        wrapped._access_wrapped = True
        return wrapped

    for handler_obj in getattr(dp.message_handlers, "handlers", []):
        if hasattr(handler_obj, "handler"):
            handler_obj.handler = wrap_message_handler(handler_obj.handler)

    for handler_obj in getattr(dp.callback_query_handlers, "handlers", []):
        if hasattr(handler_obj, "handler"):
            handler_obj.handler = wrap_callback_handler(handler_obj.handler)
