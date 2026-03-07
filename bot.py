import asyncio
import html
import os
import sqlite3
import time
from contextlib import closing
from typing import Optional, List

from aiogram import Bot, Dispatcher, F, Router
from dotenv import load_dotenv
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)
from aiogram.client.default import DefaultBotProperties

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "bot.db")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "")  # example: @china_parcel_board
BOT_USERNAME = os.getenv("BOT_USERNAME", "poputchik_kitay_laovaev_bot").lstrip("@")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
BUMP_PRICE_TEXT = os.getenv(
    "BUMP_PRICE_TEXT",
    "Площадка не принимает оплату. Пользователи договариваются между собой напрямую."
)
MAX_ACTIVE_POSTS_PER_USER = int(os.getenv("MAX_ACTIVE_POSTS_PER_USER", "10"))
MIN_SECONDS_BETWEEN_ACTIONS = int(os.getenv("MIN_SECONDS_BETWEEN_ACTIONS", "3"))

router = Router()

COUNTRIES = [
    "Китай", "Россия", "США", "Украина", "Казахстан", "Узбекистан",
    "Грузия", "Турция", "ОАЭ", "Таиланд", "Вьетнам", "Другое"
]

TYPE_TRIP = "trip"
TYPE_PARCEL = "parcel"

STATUS_PENDING = "pending"
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"
STATUS_REJECTED = "rejected"


def now_ts() -> int:
    return int(time.time())


def bot_link(start_param: Optional[str] = None) -> str:
    if start_param:
        return f"https://t.me/{BOT_USERNAME}?start={start_param}"
    return f"https://t.me/{BOT_USERNAME}"


def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(connect_db()) as conn, conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            created_at INTEGER NOT NULL,
            last_action_at INTEGER DEFAULT 0,
            is_banned INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            post_type TEXT NOT NULL,
            from_country TEXT NOT NULL,
            from_city TEXT,
            to_country TEXT NOT NULL,
            to_city TEXT,
            travel_date TEXT,
            weight_kg TEXT,
            description TEXT NOT NULL,
            contact_note TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            is_anonymous_contact INTEGER DEFAULT 1,
            channel_message_id INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            bumped_at INTEGER,
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS complaints (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            from_user_id INTEGER NOT NULL,
            reason TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dialogs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            requester_user_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_posts_search
        ON posts(post_type, status, from_country, to_country, created_at);

        CREATE INDEX IF NOT EXISTS idx_posts_user
        ON posts(user_id, status, created_at);
        """)


def upsert_user(message_or_callback):
    user = message_or_callback.from_user
    with closing(connect_db()) as conn, conn:
        conn.execute("""
            INSERT INTO users (user_id, username, full_name, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name
        """, (
            user.id,
            user.username,
            (user.full_name or "")[:200],
            now_ts()
        ))


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def anti_spam_check(user_id: int) -> Optional[str]:
    with closing(connect_db()) as conn, conn:
        row = conn.execute(
            "SELECT is_banned, last_action_at FROM users WHERE user_id=?",
            (user_id,)
        ).fetchone()
        if not row:
            return None
        if row["is_banned"]:
            return "Ваш аккаунт ограничен администратором."
        last_action_at = row["last_action_at"] or 0
        if now_ts() - last_action_at < MIN_SECONDS_BETWEEN_ACTIONS:
            return "Слишком быстро. Подождите пару секунд и попробуйте снова."
        conn.execute("UPDATE users SET last_action_at=? WHERE user_id=?", (now_ts(), user_id))
    return None


def active_post_count(user_id: int) -> int:
    with closing(connect_db()) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM posts WHERE user_id=? AND status IN ('pending','active')",
            (user_id,)
        ).fetchone()
        return int(row["c"])


def short_post_type(post_type: str) -> str:
    return "✈️ Попутчик" if post_type == TYPE_TRIP else "📦 Посылка"


def post_text(row, for_channel: bool = False) -> str:
    route = f"{html.escape(row['from_country'])}"
    if row["from_city"]:
        route += f", {html.escape(row['from_city'])}"
    route += " → "
    route += f"{html.escape(row['to_country'])}"
    if row["to_city"]:
        route += f", {html.escape(row['to_city'])}"

    lines = [
        f"<b>{short_post_type(row['post_type'])}</b>",
        f"<b>Маршрут:</b> {route}",
    ]
    if row["travel_date"]:
        lines.append(f"<b>Дата:</b> {html.escape(row['travel_date'])}")
    if row["weight_kg"]:
        lines.append(f"<b>Вес/объем:</b> {html.escape(row['weight_kg'])}")
    lines.append(f"<b>Описание:</b> {html.escape(row['description'])}")
    if row["contact_note"]:
        lines.append(f"<b>Контакт:</b> {html.escape(row['contact_note'])}")
    lines.append(f"<b>ID объявления:</b> {row['id']}")

    if for_channel:
        lines.append("Откройте бота, чтобы добавить своё объявление или найти совпадения.")
    else:
        owner = row["username"] if "username" in row.keys() else None
        if owner:
            lines.append(f"<b>Telegram:</b> @{html.escape(owner)}")

    return "\n".join(lines)


def main_menu(user_id: Optional[int] = None):
    keyboard = [
        [KeyboardButton(text="✈️ Добавить поездку"), KeyboardButton(text="📦 Добавить посылку")],
        [KeyboardButton(text="🔎 Найти совпадения"), KeyboardButton(text="📋 Мои объявления")],
        [KeyboardButton(text="🆘 Жалоба"), KeyboardButton(text="💰 Поднять объявление")],
    ]
    if user_id is not None and is_admin(user_id):
        keyboard.append([KeyboardButton(text="👨‍💼 Админка"), KeyboardButton(text="ℹ️ Помощь")])
    else:
        keyboard.append([KeyboardButton(text="ℹ️ Помощь")])

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True
    )


def countries_kb(prefix: str):
    rows = []
    row = []
    for country in COUNTRIES:
        row.append(InlineKeyboardButton(text=country, callback_data=f"{prefix}:{country}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def my_posts_kb(posts: List[sqlite3.Row]):
    rows = []
    for p in posts:
        label = f"{p['id']} • {('✈️' if p['post_type'] == 'trip' else '📦')} • {p['from_country']}→{p['to_country']} • {p['status']}"
        rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"mypost:{p['id']}")])
    return InlineKeyboardMarkup(
        inline_keyboard=rows or [[InlineKeyboardButton(text="Нет объявлений", callback_data="noop")]]
    )


def post_actions_kb(post_id: int, status: str):
    rows = [
        [
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete:{post_id}"),
            InlineKeyboardButton(text="⏸ Деактивировать", callback_data=f"deactivate:{post_id}")
        ],
        [
            InlineKeyboardButton(text="🔼 Поднять", callback_data=f"bump:{post_id}"),
            InlineKeyboardButton(text="👀 Совпадения", callback_data=f"matches:{post_id}")
        ],
    ]
    if status != STATUS_ACTIVE:
        rows.append([InlineKeyboardButton(text="▶️ Активировать", callback_data=f"activate:{post_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_post_actions_kb(post_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"adminapprove:{post_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"adminreject:{post_id}")
        ],
        [
            InlineKeyboardButton(text="🚫 Бан user", callback_data=f"adminbanpost:{post_id}")
        ]
    ])


def public_post_kb(post_id: int, owner_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🤖 Открыть бота",
                    url=bot_link()
                )
            ],
            [
                InlineKeyboardButton(
                    text="📦 Добавить посылку",
                    url=bot_link("parcel")
                ),
                InlineKeyboardButton(
                    text="✈️ Добавить поездку",
                    url=bot_link("trip")
                )
            ]
        ]
    )


class CreatePost(StatesGroup):
    from_country = State()
    from_city = State()
    to_country = State()
    to_city = State()
    travel_date = State()
    weight = State()
    description = State()
    contact_note = State()


class FindFlow(StatesGroup):
    looking_for = State()
    from_country = State()
    to_country = State()


class ComplaintFlow(StatesGroup):
    post_id = State()
    reason = State()


class ContactFlow(StatesGroup):
    message_text = State()


def create_post_record(data: dict, user_id: int) -> int:
    ts = now_ts()
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("""
            INSERT INTO posts (
                user_id, post_type, from_country, from_city, to_country, to_city,
                travel_date, weight_kg, description, contact_note, status,
                is_anonymous_contact, created_at, updated_at, bumped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user_id,
            data["post_type"],
            data["from_country"],
            data.get("from_city"),
            data["to_country"],
            data.get("to_city"),
            data.get("travel_date"),
            data.get("weight_kg"),
            data["description"],
            data.get("contact_note"),
            STATUS_PENDING if ADMIN_IDS else STATUS_ACTIVE,
            1,
            ts,
            ts,
            ts
        ))
        return cur.lastrowid


def get_post(post_id: int) -> Optional[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.id=?
        """, (post_id,)).fetchone()


def publish_to_channel(bot: Bot, post_id: int):
    if not CHANNEL_USERNAME:
        return
    row = get_post(post_id)
    if not row or row["status"] != STATUS_ACTIVE:
        return

    async def _send():
        msg = await bot.send_message(
            CHANNEL_USERNAME,
            post_text(row, for_channel=True),
            reply_markup=public_post_kb(post_id, row["user_id"])
        )
        with closing(connect_db()) as conn, conn:
            conn.execute("UPDATE posts SET channel_message_id=? WHERE id=?", (msg.message_id, post_id))

    return _send()


async def safe_publish(bot: Bot, post_id: int):
    try:
        coro = publish_to_channel(bot, post_id)
        if coro:
            await coro
    except Exception:
        pass


def search_matches(
    post_type: str,
    from_country: str,
    to_country: str,
    exclude_user_id: Optional[int] = None
):
    target_type = TYPE_PARCEL if post_type == TYPE_TRIP else TYPE_TRIP
    query = """
        SELECT p.*, u.username, u.full_name
        FROM posts p
        LEFT JOIN users u ON u.user_id = p.user_id
        WHERE p.post_type=?
          AND p.status='active'
          AND p.from_country=?
          AND p.to_country=?
    """
    params = [target_type, from_country, to_country]

    if exclude_user_id is not None:
        query += " AND p.user_id != ?"
        params.append(exclude_user_id)

    query += " ORDER BY COALESCE(p.bumped_at, p.created_at) DESC LIMIT 20"

    with closing(connect_db()) as conn:
        return conn.execute(query, params).fetchall()


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    upsert_user(message)
    await state.clear()

    start_arg = ""
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) > 1:
        start_arg = parts[1].strip().lower()

    text = (
        "Привет. Это <b>Попутчик Китай | Laovaev.net</b> — бот для поиска попутчиков и передачи посылок между Китаем и другими странами.\n\n"
        "Что можно делать:\n"
        "• добавить поездку\n"
        "• добавить посылку\n"
        "• найти совпадения по маршруту\n"
        "• писать владельцу объявления через бота\n"
        "• жаловаться на объявления\n"
        "• управлять своими объявлениями\n\n"
        "Важно: бот не принимает оплату и не выступает посредником сделки. Пользователи сами договариваются об условиях и оплате между собой.\n"
    )

    await message.answer(text, reply_markup=main_menu(message.from_user.id))

    if start_arg == "parcel":
        await begin_create(message, state, TYPE_PARCEL)
    elif start_arg == "trip":
        await begin_create(message, state, TYPE_TRIP)


@router.message(Command("help"))
@router.message(F.text == "ℹ️ Помощь")
async def help_handler(message: Message):
    upsert_user(message)
    await message.answer(
        "Команды:\n"
        "/start — старт\n"
        "/help — помощь\n"
        "/my — мои объявления\n"
        "/find — поиск совпадений\n"
        "/new_trip — добавить поездку\n"
        "/new_parcel — добавить посылку\n"
        "/admin — админка\n\n"
        "Важно: бот — только площадка для поиска. Мы не принимаем оплату и не выступаем посредником.\n"
        "Для безопасности не переводите предоплату незнакомым людям без проверки.",
        reply_markup=main_menu(message.from_user.id)
    )


async def begin_create(message: Message, state: FSMContext, post_type: str):
    upsert_user(message)
    limit = anti_spam_check(message.from_user.id)
    if limit:
        await message.answer(limit)
        return

    if active_post_count(message.from_user.id) >= MAX_ACTIVE_POSTS_PER_USER:
        await message.answer(
            f"Лимит активных объявлений: {MAX_ACTIVE_POSTS_PER_USER}. "
            "Сначала удалите или деактивируйте часть объявлений."
        )
        return

    await state.clear()
    await state.update_data(post_type=post_type)
    await state.set_state(CreatePost.from_country)

    await message.answer("Выбери страну отправления:", reply_markup=ReplyKeyboardRemove())
    await message.answer("Страна отправления:", reply_markup=countries_kb("from"))


@router.message(Command("new_trip"))
@router.message(F.text == "✈️ Добавить поездку")
async def add_trip(message: Message, state: FSMContext):
    await begin_create(message, state, TYPE_TRIP)


@router.message(Command("new_parcel"))
@router.message(F.text == "📦 Добавить посылку")
async def add_parcel(message: Message, state: FSMContext):
    await begin_create(message, state, TYPE_PARCEL)


@router.callback_query(F.data.startswith("from:"))
async def choose_from_country(callback: CallbackQuery, state: FSMContext):
    upsert_user(callback)
    country = callback.data.split(":", 1)[1]
    await state.update_data(from_country=country)
    await state.set_state(CreatePost.from_city)
    await callback.message.answer(f"Город отправления в стране {country}.\nЕсли неважно — напиши '-'")
    await callback.answer()


@router.message(CreatePost.from_city)
async def enter_from_city(message: Message, state: FSMContext):
    await state.update_data(
        from_city=None if message.text.strip() == "-" else message.text.strip()[:80]
    )
    await state.set_state(CreatePost.to_country)
    await message.answer("Выбери страну назначения:", reply_markup=countries_kb("to"))


@router.callback_query(F.data.startswith("to:"))
async def choose_to_country(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    await state.update_data(to_country=country)
    await state.set_state(CreatePost.to_city)
    await callback.message.answer(f"Город назначения в стране {country}.\nЕсли неважно — напиши '-'")
    await callback.answer()


@router.message(CreatePost.to_city)
async def enter_to_city(message: Message, state: FSMContext):
    await state.update_data(
        to_city=None if message.text.strip() == "-" else message.text.strip()[:80]
    )
    await state.set_state(CreatePost.travel_date)
    await message.answer(
        "Дата поездки/отправки. Например: 15 марта 2026. Если дата не точная — напиши как удобно."
    )


@router.message(CreatePost.travel_date)
async def enter_date(message: Message, state: FSMContext):
    await state.update_data(travel_date=message.text.strip()[:100])
    await state.set_state(CreatePost.weight)
    await message.answer("Вес или объем. Например: до 3 кг. Если неизвестно — напиши '-'")


@router.message(CreatePost.weight)
async def enter_weight(message: Message, state: FSMContext):
    await state.update_data(
        weight_kg=None if message.text.strip() == "-" else message.text.strip()[:50]
    )
    await state.set_state(CreatePost.description)
    await message.answer("Опиши объявление подробно: что нужно передать / сколько места есть / условия.")


@router.message(CreatePost.description)
async def enter_description(message: Message, state: FSMContext):
    await state.update_data(description=message.text.strip()[:1000])
    await state.set_state(CreatePost.contact_note)
    await message.answer(
        "Доп. контакт или примечание. Например: WeChat / только text / без звонков. Если не надо — напиши '-'"
    )


@router.message(CreatePost.contact_note)
async def finalize_post(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    data["contact_note"] = None if message.text.strip() == "-" else message.text.strip()[:200]
    post_id = create_post_record(data, message.from_user.id)
    row = get_post(post_id)

    await state.clear()

    await message.answer(
        "Объявление создано.\n" +
        ("Оно отправлено на модерацию." if ADMIN_IDS else "Оно уже активно."),
        reply_markup=main_menu(message.from_user.id)
    )

    await message.answer(post_text(row), reply_markup=post_actions_kb(post_id, row["status"]))

    if ADMIN_IDS and row["status"] == STATUS_PENDING:
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    "Новое объявление на модерации:\n\n" + post_text(row),
                    reply_markup=admin_post_actions_kb(post_id)
                )
            except Exception:
                pass
    else:
        await safe_publish(bot, post_id)


@router.message(Command("my"))
@router.message(F.text == "📋 Мои объявления")
async def my_posts(message: Message):
    upsert_user(message)
    with closing(connect_db()) as conn:
        posts = conn.execute("""
            SELECT * FROM posts
            WHERE user_id=?
            ORDER BY COALESCE(bumped_at, created_at) DESC
            LIMIT 30
        """, (message.from_user.id,)).fetchall()

    if not posts:
        await message.answer("У тебя пока нет объявлений.", reply_markup=main_menu(message.from_user.id))
        return

    await message.answer("Твои объявления:", reply_markup=my_posts_kb(posts))


@router.callback_query(F.data.startswith("mypost:"))
async def open_my_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = get_post(post_id)
    if not row or row["user_id"] != callback.from_user.id:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    await callback.message.answer(post_text(row), reply_markup=post_actions_kb(post_id, row["status"]))
    await callback.answer()


def owner_only(callback: CallbackQuery, post_id: int) -> Optional[sqlite3.Row]:
    row = get_post(post_id)
    if not row or row["user_id"] != callback.from_user.id:
        return None
    return row


@router.callback_query(F.data.startswith("delete:"))
async def delete_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("DELETE FROM posts WHERE id=?", (post_id,))

    await callback.message.answer(f"Объявление {post_id} удалено.")
    await callback.answer()


@router.callback_query(F.data.startswith("deactivate:"))
async def deactivate_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (STATUS_INACTIVE, now_ts(), post_id)
        )

    await callback.message.answer(f"Объявление {post_id} деактивировано.")
    await callback.answer()


@router.callback_query(F.data.startswith("activate:"))
async def activate_post(callback: CallbackQuery, bot: Bot):
    post_id = int(callback.data.split(":")[1])
    row = owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    new_status = STATUS_PENDING if ADMIN_IDS else STATUS_ACTIVE

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (new_status, now_ts(), post_id)
        )

    await callback.message.answer(
        f"Объявление {post_id} " + ("отправлено на повторную модерацию." if ADMIN_IDS else "активировано.")
    )

    if not ADMIN_IDS:
        await safe_publish(bot, post_id)

    await callback.answer()


@router.callback_query(F.data.startswith("bump:"))
async def bump_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    if row["status"] != STATUS_ACTIVE:
        await callback.answer("Поднимать можно только активное объявление", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET bumped_at=?, updated_at=? WHERE id=?",
            (now_ts(), now_ts(), post_id)
        )

    await callback.message.answer(
        f"Объявление {post_id} поднято выше в поиске.\n{BUMP_PRICE_TEXT}"
    )
    await callback.answer("Готово")


@router.message(F.text == "💰 Поднять объявление")
async def bump_info(message: Message):
    await message.answer(
        BUMP_PRICE_TEXT + "\n\nОткрой 'Мои объявления' и нажми 'Поднять' у нужного объявления.",
        reply_markup=main_menu(message.from_user.id)
    )


@router.message(Command("find"))
@router.message(F.text == "🔎 Найти совпадения")
async def find_start(message: Message, state: FSMContext):
    upsert_user(message)
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ищу попутчика для посылки", callback_data="findtype:parcel")],
        [InlineKeyboardButton(text="Ищу посылку для попутчика", callback_data="findtype:trip")]
    ])

    await state.set_state(FindFlow.looking_for)
    await message.answer("Что ищем?", reply_markup=kb)


@router.callback_query(F.data.startswith("findtype:"))
async def find_type(callback: CallbackQuery, state: FSMContext):
    looking_for = callback.data.split(":")[1]
    await state.update_data(looking_for=looking_for)
    await state.set_state(FindFlow.from_country)
    await callback.message.answer("Выбери страну отправления:", reply_markup=countries_kb("findfrom"))
    await callback.answer()


@router.callback_query(F.data.startswith("findfrom:"))
async def find_from(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    await state.update_data(from_country=country)
    await state.set_state(FindFlow.to_country)
    await callback.message.answer("Выбери страну назначения:", reply_markup=countries_kb("findto"))
    await callback.answer()


@router.callback_query(F.data.startswith("findto:"))
async def find_to(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = TYPE_TRIP if data["looking_for"] == "trip" else TYPE_PARCEL

    matches = search_matches(
        post_type,
        data["from_country"],
        country,
        exclude_user_id=callback.from_user.id
    )

    await state.clear()

    if not matches:
        await callback.message.answer("Совпадений пока нет.")
    else:
        await callback.message.answer(f"Найдено совпадений: {len(matches)}")
        for row in matches[:10]:
            await callback.message.answer(
                post_text(row),
                reply_markup=public_post_kb(row["id"], row["user_id"])
            )

    await callback.answer()


@router.callback_query(F.data.startswith("matches:"))
async def matches_for_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    matches = search_matches(
        row["post_type"],
        row["from_country"],
        row["to_country"],
        exclude_user_id=callback.from_user.id
    )

    if not matches:
        await callback.message.answer("Совпадений пока нет.")
    else:
        await callback.message.answer(f"Найдено совпадений: {len(matches)}")
        for item in matches[:10]:
            await callback.message.answer(
                post_text(item),
                reply_markup=public_post_kb(item["id"], item["user_id"])
            )

    await callback.answer()


@router.callback_query(F.data.startswith("contact:"))
async def contact_owner(callback: CallbackQuery, state: FSMContext):
    _, post_id, owner_id = callback.data.split(":")
    if int(owner_id) == callback.from_user.id:
        await callback.answer("Это ваше объявление", show_alert=True)
        return

    await state.set_state(ContactFlow.message_text)
    await state.update_data(post_id=int(post_id), target_user_id=int(owner_id))
    await callback.message.answer("Напиши сообщение владельцу объявления. Я перешлю его через бота.")
    await callback.answer()


@router.message(ContactFlow.message_text)
async def relay_message(message: Message, state: FSMContext):
    data = await state.get_data()
    target_user_id = data["target_user_id"]
    post_id = data["post_id"]
    text = message.text.strip()

    try:
        await message.bot.send_message(
            target_user_id,
            f"Новое сообщение по объявлению ID {post_id}:\n\n"
            f"От: {message.from_user.full_name}"
            + (f" (@{message.from_user.username})" if message.from_user.username else "")
            + f"\n\n{text}\n\n"
            f"Чтобы ответить, напиши пользователю напрямую"
            + (f": @{message.from_user.username}" if message.from_user.username else " или попроси его оставить контакт.")
        )

        with closing(connect_db()) as conn, conn:
            conn.execute(
                "INSERT INTO dialogs (post_id, owner_user_id, requester_user_id, created_at) VALUES (?, ?, ?, ?)",
                (post_id, target_user_id, message.from_user.id, now_ts())
            )

        await message.answer("Сообщение отправлено владельцу объявления.", reply_markup=main_menu(message.from_user.id))
    except Exception:
        await message.answer("Не удалось отправить сообщение. Возможно, владелец еще не запускал бота.")

    await state.clear()


@router.message(F.text == "🆘 Жалоба")
async def complaint_start(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(ComplaintFlow.post_id)
    await message.answer("Введи ID объявления, на которое хочешь пожаловаться.")


@router.callback_query(F.data.startswith("complain:"))
async def complaint_from_button(callback: CallbackQuery, state: FSMContext):
    post_id = int(callback.data.split(":")[1])
    await state.set_state(ComplaintFlow.reason)
    await state.update_data(post_id=post_id)
    await callback.message.answer(f"Опиши причину жалобы на объявление {post_id}.")
    await callback.answer()


@router.message(ComplaintFlow.post_id)
async def complaint_post_id(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Нужен числовой ID объявления.")
        return

    await state.update_data(post_id=int(message.text))
    await state.set_state(ComplaintFlow.reason)
    await message.answer("Опиши причину жалобы.")


@router.message(ComplaintFlow.reason)
async def complaint_reason(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    post_id = data["post_id"]

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "INSERT INTO complaints (post_id, from_user_id, reason, created_at) VALUES (?, ?, ?, ?)",
            (post_id, message.from_user.id, message.text.strip()[:1000], now_ts())
        )

    await message.answer("Жалоба отправлена.", reply_markup=main_menu(message.from_user.id))

    for admin_id in ADMIN_IDS:
        try:
            row = get_post(post_id)
            await bot.send_message(
                admin_id,
                f"Новая жалоба на объявление {post_id}:\n\nПричина: {message.text.strip()}\n\n"
                + (post_text(row) if row else "Объявление не найдено")
            )
        except Exception:
            pass

    await state.clear()


@router.message(Command("admin"))
@router.message(F.text == "👨‍💼 Админка")
async def admin_menu(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Админка доступна только администраторам.")
        return

    with closing(connect_db()) as conn:
        pending = conn.execute("SELECT COUNT(*) AS c FROM posts WHERE status='pending'").fetchone()["c"]
        complaints = conn.execute("SELECT COUNT(*) AS c FROM complaints").fetchone()["c"]

    await message.answer(
        f"Админка\n\nНа модерации: {pending}\nЖалоб: {complaints}\n\n"
        "Команды:\n/admin_pending\n/admin_complaints\n/admin_ban USER_ID\n/admin_unban USER_ID"
    )


@router.message(Command("admin_pending"))
async def admin_pending(message: Message):
    if not is_admin(message.from_user.id):
        return

    with closing(connect_db()) as conn:
        rows = conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.status='pending'
            ORDER BY p.created_at ASC
            LIMIT 20
        """).fetchall()

    if not rows:
        await message.answer("Нет объявлений на модерации.")
        return

    for row in rows:
        await message.answer(post_text(row), reply_markup=admin_post_actions_kb(row["id"]))


@router.callback_query(F.data.startswith("adminapprove:"))
async def admin_approve(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])

    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE posts SET status='active', updated_at=? WHERE id=?", (now_ts(), post_id))

    row = get_post(post_id)
    await callback.message.answer(f"Объявление {post_id} одобрено.")

    if row:
        try:
            await bot.send_message(row["user_id"], f"Ваше объявление {post_id} одобрено и опубликовано.")
        except Exception:
            pass

    await safe_publish(bot, post_id)
    await callback.answer("Одобрено")


@router.callback_query(F.data.startswith("adminreject:"))
async def admin_reject(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])

    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE posts SET status='rejected', updated_at=? WHERE id=?", (now_ts(), post_id))

    row = get_post(post_id)
    await callback.message.answer(f"Объявление {post_id} отклонено.")

    if row:
        try:
            await bot.send_message(row["user_id"], f"Ваше объявление {post_id} отклонено модератором.")
        except Exception:
            pass

    await callback.answer("Отклонено")


@router.callback_query(F.data.startswith("adminbanpost:"))
async def admin_ban_post_owner(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])
    row = get_post(post_id)
    if not row:
        await callback.answer("Не найдено", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (row["user_id"],))
        conn.execute("UPDATE posts SET status='inactive' WHERE user_id=?", (row["user_id"],))

    try:
        await bot.send_message(row["user_id"], "Ваш аккаунт был ограничен администратором.")
    except Exception:
        pass

    await callback.message.answer(f"Пользователь {row['user_id']} забанен, его объявления деактивированы.")
    await callback.answer("Готово")


@router.message(Command("admin_complaints"))
async def admin_complaints(message: Message):
    if not is_admin(message.from_user.id):
        return

    with closing(connect_db()) as conn:
        rows = conn.execute("""
            SELECT c.*, p.user_id as post_owner_id, p.post_type, p.from_country, p.to_country
            FROM complaints c
            LEFT JOIN posts p ON p.id=c.post_id
            ORDER BY c.created_at DESC
            LIMIT 20
        """).fetchall()

    if not rows:
        await message.answer("Жалоб нет.")
        return

    for row in rows:
        await message.answer(
            f"Жалоба #{row['id']}\n"
            f"Объявление: {row['post_id']}\n"
            f"От пользователя: {row['from_user_id']}\n"
            f"Владелец объявления: {row['post_owner_id']}\n"
            f"Маршрут: {row['from_country']} → {row['to_country']}\n"
            f"Причина: {row['reason']}"
        )


@router.message(Command("admin_ban"))
async def admin_ban(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_ban USER_ID")
        return

    user_id = int(parts[1])

    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (user_id,))
        conn.execute("UPDATE posts SET status='inactive' WHERE user_id=?", (user_id,))

    await message.answer(f"Пользователь {user_id} забанен.")


@router.message(Command("admin_unban"))
async def admin_unban(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_unban USER_ID")
        return

    user_id = int(parts[1])

    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_banned=0 WHERE user_id=?", (user_id,))

    await message.answer(f"Пользователь {user_id} разбанен.")


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN env var")

    init_db()

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
