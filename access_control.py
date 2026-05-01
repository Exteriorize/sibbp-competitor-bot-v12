from __future__ import annotations

import os
from typing import Dict, Iterable, Set

from aiogram import types
from aiogram.dispatcher.handler import CancelHandler
from aiogram.dispatcher.middlewares import BaseMiddleware

try:
    from config import CHAT_ID
except Exception:
    CHAT_ID = 0


ADMIN = "admin"
EDITOR = "editor"
DIRECTOR = "director"
DENIED = "denied"

ROLE_LABELS = {
    ADMIN: "Админ",
    EDITOR: "Заполняющий",
    DIRECTOR: "Директор",
    DENIED: "Нет доступа",
}

DENIED_TEXT = (
    "⛔ У вас нет доступа к этому разделу.\n\n"
    "Ваш Telegram ID: <code>{user_id}</code>\n"
    "Отправьте этот ID администратору, чтобы он добавил вас в нужный профиль."
)

UNKNOWN_TEXT = (
    "⛔ Бот закрыт для посторонних пользователей.\n\n"
    "Ваш Telegram ID: <code>{user_id}</code>\n"
    "Передайте этот ID администратору, чтобы он добавил вас в нужный профиль."
)


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


def _env_ids(name: str, default: str = "") -> Set[int]:
    return _parse_ids(os.getenv(name, default))


def admin_ids() -> Set[int]:
    default_admin = str(CHAT_ID or "")
    return _env_ids("ADMIN_IDS", default_admin)


def editor_ids() -> Set[int]:
    return _env_ids("EDITOR_IDS", "")


def director_ids() -> Set[int]:
    return _env_ids("DIRECTOR_IDS", "")


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

PUBLIC_COMMANDS = {"/myid"}
COMMON_COMMANDS = {"/start", "/menu", "/cancel"}

ADMIN_COMMANDS = {
    "/check",
    "/report",
    "/dynamic",
    "/compare",
    "/all",
    "/allreport",
    "/add_competitor",
    "/add_room",
    "/review",
    "/priority",
    "/changes",
    "/archive",
}

EDITOR_COMMANDS = {
    "/check",
    "/add_competitor",
    "/add_room",
    "/review",
    "/priority",
}

DIRECTOR_COMMANDS = {
    "/check",
    "/report",
    "/dynamic",
    "/compare",
    "/all",
    "/allreport",
    "/priority",
    "/changes",
    "/archive",
}

EDITOR_CALLBACK_PREFIXES = (
    "select:",
    "qa:check",
    "qa:review",
    "roomcomp:",
    "roomtype:",
    "roomsource:",
    "source:",
    "aggstatus:",
    "aggreliability:",
    "reliability:",
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
    if user_id in admin_ids():
        return ADMIN
    if user_id in editor_ids():
        return EDITOR
    if user_id in director_ids():
        return DIRECTOR
    return DENIED


def _command(text: str) -> str:
    value = str(text or "").strip()
    if not value.startswith("/"):
        return ""
    return value.split(maxsplit=1)[0].split("@", 1)[0].lower()


def _starts_with(value: str, prefixes: Iterable[str]) -> bool:
    return any(str(value or "").startswith(prefix) for prefix in prefixes)


def _has_active_flow(flow: Dict[int, Dict[str, object]], user_id: int) -> bool:
    return bool(flow.get(int(user_id or 0)))


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
    user_id = message.from_user.id if message.from_user else 0
    await message.answer(
        f"Ваш Telegram ID: <code>{user_id}</code>\n"
        f"Роль: <b>{ROLE_LABELS.get(get_role(user_id), 'Нет доступа')}</b>\n"
        f"{role_menu_hint(user_id)}"
    )


class AccessControlMiddleware(BaseMiddleware):
    def __init__(self, flow: Dict[int, Dict[str, object]]):
        super().__init__()
        self.flow = flow

    def _message_allowed(self, user_id: int, text: str) -> bool:
        role = get_role(user_id)
        command = _command(text)
        text = str(text or "").strip()

        if command in PUBLIC_COMMANDS:
            return True
        if role == DENIED:
            return False
        if role == ADMIN:
            return True
        if command in COMMON_COMMANDS:
            return True
        if _has_active_flow(self.flow, user_id) and role == EDITOR:
            return True
        if role == EDITOR:
            return text in EDITOR_TEXTS or command in EDITOR_COMMANDS
        if role == DIRECTOR:
            return text in DIRECTOR_TEXTS or command in DIRECTOR_COMMANDS
        return False

    def _callback_allowed(self, user_id: int, data: str) -> bool:
        role = get_role(user_id)
        data = str(data or "")

        if role == DENIED:
            return False
        if role == ADMIN:
            return True
        if _has_active_flow(self.flow, user_id) and role == EDITOR:
            return True
        if role == EDITOR:
            return _starts_with(data, EDITOR_CALLBACK_PREFIXES)
        if role == DIRECTOR:
            return _starts_with(data, DIRECTOR_CALLBACK_PREFIXES)
        return False

    async def on_pre_process_message(self, message: types.Message, data: dict) -> None:
        user_id = message.from_user.id if message.from_user else 0
        text = message.text or ""
        command = _command(text)

        if command == "/myid":
            await send_myid(message)
            raise CancelHandler()

        if not self._message_allowed(user_id, text):
            role = get_role(user_id)
            await message.answer(DENIED_TEXT.format(user_id=user_id) if role != DENIED else UNKNOWN_TEXT.format(user_id=user_id))
            raise CancelHandler()

    async def on_pre_process_callback_query(self, callback_query: types.CallbackQuery, data: dict) -> None:
        user_id = callback_query.from_user.id if callback_query.from_user else 0
        if not self._callback_allowed(user_id, callback_query.data or ""):
            await callback_query.answer("У вас нет доступа к этому разделу", show_alert=True)
            raise CancelHandler()


def setup_access_control(dp, flow: Dict[int, Dict[str, object]]) -> None:
    dp.middleware.setup(AccessControlMiddleware(flow))


# Backward-compatible name. main.py may still import this.
def apply_access_control(dp, flow: Dict[int, Dict[str, object]]) -> None:
    setup_access_control(dp, flow)
