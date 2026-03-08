import asyncio
import html
import os
import re
import sqlite3
import time
from contextlib import closing
from datetime import datetime
from typing import Optional, List, Tuple

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
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "Poputchik_china_bot").lstrip("@")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
BUMP_PRICE_TEXT = os.getenv(
    "BUMP_PRICE_TEXT",
    "Площадка не принимает оплату. Пользователи договариваются между собой напрямую."
)
MAX_ACTIVE_POSTS_PER_USER = int(os.getenv("MAX_ACTIVE_POSTS_PER_USER", "10"))
MIN_SECONDS_BETWEEN_ACTIONS = int(os.getenv("MIN_SECONDS_BETWEEN_ACTIONS", "3"))
POST_TTL_DAYS = int(os.getenv("POST_TTL_DAYS", "14"))
COINCIDENCE_NOTIFY_LIMIT = int(os.getenv("COINCIDENCE_NOTIFY_LIMIT", "5"))

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
STATUS_EXPIRED = "expired"


def now_ts() -> int:
    return int(time.time())


def days_to_seconds(days: int) -> int:
    return days * 24 * 60 * 60


def format_age(ts: int) -> str:
    diff = max(0, now_ts() - ts)
    if diff < 60:
        return "только что"
    if diff < 3600:
        mins = diff // 60
        return f"{mins} мин назад"
    if diff < 86400:
        hrs = diff // 3600
        return f"{hrs} ч назад"
    days = diff // 86400
    return f"{days} дн назад"


def normalize_text(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def parse_weight_kg(value: Optional[str]) -> Optional[float]:
    """
    Из текста вроде:
    - "до 3 кг"
    - "3kg"
    - "примерно 8.5"
    пытается вытащить число.
    """
    if not value:
        return None

    text = value.lower().replace(",", ".")
    found = re.findall(r"\d+(?:\.\d+)?", text)
    if not found:
        return None

    try:
        return float(found[0])
    except ValueError:
        return None


def parse_date_loose(value: Optional[str]) -> Optional[datetime]:
    """
    Поддерживает:
    - YYYY-MM-DD
    - DD.MM.YYYY
    - DD/MM/YYYY
    - DD-MM-YYYY
    Если не распознано — None.
    """
    if not value:
        return None

    text = value.strip()

    fmts = [
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%d-%m-%Y",
    ]

    for fmt in fmts:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    return None


def bot_link(start_param: Optional[str] = None) -> str:
    if start_param:
        return f"https://t.me/{BOT_USERNAME}?start={start_param}"
    return f"https://t.me/{BOT_USERNAME}"


def post_deeplink(post_id: int) -> str:
    return bot_link(f"post_{post_id}")


def route_deeplink(post_type: str, from_country: str, to_country: str) -> str:
    return bot_link(f"route_{post_type}_{from_country}_{to_country}")


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
            is_banned INTEGER DEFAULT 0,
            is_verified INTEGER DEFAULT 0
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
            expires_at INTEGER,
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

        CREATE TABLE IF NOT EXISTS route_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            post_type TEXT NOT NULL,
            from_country TEXT NOT NULL,
            to_country TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reviewer_user_id INTEGER NOT NULL,
            reviewed_user_id INTEGER NOT NULL,
            post_id INTEGER,
            rating INTEGER NOT NULL,
            text TEXT,
            created_at INTEGER NOT NULL,
            UNIQUE(reviewer_user_id, reviewed_user_id, post_id)
        );

        CREATE TABLE IF NOT EXISTS coincidence_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_a_id INTEGER NOT NULL,
            post_b_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE(post_a_id, post_b_id)
        );

        CREATE INDEX IF NOT EXISTS idx_posts_search
        ON posts(post_type, status, from_country, to_country, created_at);

        CREATE INDEX IF NOT EXISTS idx_posts_user
        ON posts(user_id, status, created_at);

        CREATE INDEX IF NOT EXISTS idx_subscriptions_search
        ON route_subscriptions(post_type, from_country, to_country);

        CREATE INDEX IF NOT EXISTS idx_reviews_user
        ON reviews(reviewed_user_id, created_at);
        """)

        existing_users = [r["name"] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
        if "is_verified" not in existing_users:
            conn.execute("ALTER TABLE users ADD COLUMN is_verified INTEGER DEFAULT 0")

        existing_posts = [r["name"] for r in conn.execute("PRAGMA table_info(posts)").fetchall()]
        if "expires_at" not in existing_posts:
            conn.execute("ALTER TABLE posts ADD COLUMN expires_at INTEGER")


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
            "SELECT COUNT(*) AS c FROM posts WHERE user_id=? AND status IN ('pending','active','inactive')",
            (user_id,)
        ).fetchone()
        return int(row["c"])


def short_post_type(post_type: str) -> str:
    return "✈️ Попутчик" if post_type == TYPE_TRIP else "📦 Посылка"


def user_rating_summary(user_id: int) -> Tuple[float, int]:
    with closing(connect_db()) as conn:
        row = conn.execute("""
            SELECT AVG(rating) AS avg_rating, COUNT(*) AS cnt
            FROM reviews
            WHERE reviewed_user_id=?
        """, (user_id,)).fetchone()
        avg_rating = float(row["avg_rating"] or 0)
        cnt = int(row["cnt"] or 0)
        return avg_rating, cnt


def is_user_verified(user_id: int) -> bool:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT is_verified FROM users WHERE user_id=?", (user_id,)).fetchone()
        return bool(row and row["is_verified"])


def format_rating_line(user_id: int) -> Optional[str]:
    avg_rating, cnt = user_rating_summary(user_id)
    if cnt <= 0:
        return None
    stars = "⭐" * max(1, min(5, round(avg_rating)))
    return f"{stars} {avg_rating:.1f} ({cnt} отзывов)"


def format_coincidence_badges(score: int, notes: List[str]) -> str:
    if score >= 75:
        level = "✅ Совпадение"
    elif score >= 55:
        level = "🟡 Частичное совпадение"
    else:
        level = "⚠️ Минимальное совпадение"

    if notes:
        return f"{level}\n" + "\n".join(f"• {html.escape(note)}" for note in notes)
    return level


def post_text(row, for_channel: bool = False) -> str:
    route = f"{html.escape(row['from_country'])}"
    if row["from_city"]:
        route += f", {html.escape(row['from_city'])}"
    route += " → "
    route += f"{html.escape(row['to_country'])}"
    if row["to_city"]:
        route += f", {html.escape(row['to_city'])}"

    owner_user_id = row["user_id"]
    verified_badge = " ✅ Проверенный" if is_user_verified(owner_user_id) else ""
    rating_line = format_rating_line(owner_user_id)

    lines = [
        f"<b>{short_post_type(row['post_type'])}{verified_badge}</b>",
        f"<b>Маршрут:</b> {route}",
    ]
    if row["travel_date"]:
        lines.append(f"<b>Дата:</b> {html.escape(row['travel_date'])}")
    if row["weight_kg"]:
        lines.append(f"<b>Вес/объем:</b> {html.escape(row['weight_kg'])}")
    lines.append(f"<b>Описание:</b> {html.escape(row['description'])}")
    if row["contact_note"]:
        lines.append(f"<b>Контакт:</b> {html.escape(row['contact_note'])}")
    if rating_line:
        lines.append(f"<b>Рейтинг:</b> {rating_line}")
    lines.append(f"<b>ID объявления:</b> {row['id']}")

    if for_channel:
        lines.append("Откройте бот, чтобы добавить своё объявление или найти совпадения.")
    else:
        owner = row["username"] if "username" in row.keys() else None
        if owner:
            lines.append(f"<b>Telegram:</b> @{html.escape(owner)}")

    return "\n".join(lines)


def main_menu(user_id: Optional[int] = None):
    keyboard = [
        [KeyboardButton(text="✈️ Взять посылку"), KeyboardButton(text="📦 Отправить посылку")],
        [KeyboardButton(text="🔎 Найти совпадения"), KeyboardButton(text="📋 Мои объявления")],
        [KeyboardButton(text="🔥 Популярные маршруты"), KeyboardButton(text="🆕 Новые объявления")],
        [KeyboardButton(text="🔔 Подписки"), KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="💰 Поднять объявление"), KeyboardButton(text="⭐ Оставить отзыв")],
        [KeyboardButton(text="🆘 Жалоба"), KeyboardButton(text="ℹ️ Помощь")],
    ]
    if user_id is not None and is_admin(user_id):
        keyboard.append([KeyboardButton(text="👨‍💼 Админка")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


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
        label = f"{p['id']} • {('✈️' if p['post_type']=='trip' else '📦')} • {p['from_country']}→{p['to_country']} • {p['status']}"
        rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"mypost:{p['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Нет объявлений", callback_data="noop")]])


def post_actions_kb(post_id: int, status: str):
    rows = [
        [
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete:{post_id}"),
            InlineKeyboardButton(text="⏸ Деактивировать", callback_data=f"deactivate:{post_id}")
        ],
        [
            InlineKeyboardButton(text="🔼 Поднять", callback_data=f"bump:{post_id}"),
            InlineKeyboardButton(text="👀 Совпадения", callback_data=f"coincidences:{post_id}")
        ],
        [
            InlineKeyboardButton(text="📤 Поделиться", url=post_deeplink(post_id))
        ]
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


def public_post_kb(post_id: int, owner_id: int, post_type: Optional[str] = None):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✉️ Написать владельцу", callback_data=f"contact:{post_id}:{owner_id}")],
            [InlineKeyboardButton(text="⚠️ Пожаловаться", callback_data=f"complain:{post_id}")],
            [InlineKeyboardButton(text="📤 Поделиться", url=post_deeplink(post_id))]
        ]
    )


def channel_post_kb(post_id: int, post_type: Optional[str] = None):
    second_button_text = "📦 Мне нужно отправить"
    second_button_url = bot_link("parcel")
    if post_type == TYPE_PARCEL:
        second_button_text = "✈️ Могу взять"
        second_button_url = bot_link("trip")

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Открыть бота", url=bot_link())],
            [InlineKeyboardButton(text=second_button_text, url=second_button_url)],
            [InlineKeyboardButton(
    text="📤 Поделиться",
    url=f"https://t.me/share/url?url={post_deeplink(post_id)}"
)]
        ]
    )


def subscription_actions_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔔 Подписаться на маршрут", callback_data="sub:new")],
        [InlineKeyboardButton(text="📋 Мои подписки", callback_data="sub:list")]
    ])


def popular_routes_kb(rows: List[sqlite3.Row]):
    buttons = []
    for row in rows:
        label = f"{row['from_country']} → {row['to_country']} ({row['cnt']})"
        buttons.append([
            InlineKeyboardButton(
                text=label[:64],
                callback_data=f"popular:{row['post_type']}:{row['from_country']}:{row['to_country']}"
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons or [[InlineKeyboardButton(text="Пока пусто", callback_data="noop")]])


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


class SubscriptionFlow(StatesGroup):
    looking_for = State()
    from_country = State()
    to_country = State()


class ReviewFlow(StatesGroup):
    reviewed_user_id = State()
    post_id = State()
    rating = State()
    text = State()


def create_post_record(data: dict, user_id: int) -> int:
    ts = now_ts()
    expires_at = ts + days_to_seconds(POST_TTL_DAYS)
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("""
            INSERT INTO posts (
                user_id, post_type, from_country, from_city, to_country, to_city,
                travel_date, weight_kg, description, contact_note, status,
                is_anonymous_contact, created_at, updated_at, bumped_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ts,
            expires_at
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


def get_recent_posts(limit: int = 10) -> List[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.status='active'
              AND (p.expires_at IS NULL OR p.expires_at > ?)
            ORDER BY p.created_at DESC
            LIMIT ?
        """, (now_ts(), limit)).fetchall()


def get_popular_routes(limit: int = 10) -> List[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT post_type, from_country, to_country, COUNT(*) AS cnt
            FROM posts
            WHERE status='active'
              AND (expires_at IS NULL OR expires_at > ?)
            GROUP BY post_type, from_country, to_country
            ORDER BY cnt DESC, MAX(COALESCE(bumped_at, created_at)) DESC
            LIMIT ?
        """, (now_ts(), limit)).fetchall()


def search_route_posts(post_type: str, from_country: str, to_country: str, limit: int = 10) -> List[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.post_type=?
              AND p.from_country=?
              AND p.to_country=?
              AND p.status='active'
              AND (p.expires_at IS NULL OR p.expires_at > ?)
            ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
            LIMIT ?
        """, (post_type, from_country, to_country, now_ts(), limit)).fetchall()


def service_stats() -> sqlite3.Row:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT
                (SELECT COUNT(*) FROM users) AS users_count,
                (SELECT COUNT(*) FROM posts WHERE status='active' AND (expires_at IS NULL OR expires_at > ?)) AS active_posts,
                (SELECT COUNT(*) FROM posts WHERE status='active' AND post_type='trip' AND (expires_at IS NULL OR expires_at > ?)) AS active_trips,
                (SELECT COUNT(*) FROM posts WHERE status='active' AND post_type='parcel' AND (expires_at IS NULL OR expires_at > ?)) AS active_parcels
        """, (now_ts(), now_ts(), now_ts())).fetchone()


def top_route() -> Optional[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT from_country, to_country, COUNT(*) AS cnt
            FROM posts
            WHERE status='active'
              AND (expires_at IS NULL OR expires_at > ?)
            GROUP BY from_country, to_country
            ORDER BY cnt DESC
            LIMIT 1
        """, (now_ts(),)).fetchone()


def add_route_subscription(user_id: int, post_type: str, from_country: str, to_country: str):
    with closing(connect_db()) as conn, conn:
        conn.execute("""
            INSERT INTO route_subscriptions (user_id, post_type, from_country, to_country, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, post_type, from_country, to_country, now_ts()))


def list_route_subscriptions(user_id: int) -> List[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT * FROM route_subscriptions
            WHERE user_id=?
            ORDER BY created_at DESC
            LIMIT 20
        """, (user_id,)).fetchall()


def subscription_list_kb(subs: List[sqlite3.Row]):
    rows = []
    for s in subs:
        label = f"{s['id']} • {('✈️' if s['post_type']=='trip' else '📦')} • {s['from_country']}→{s['to_country']}"
        rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"subdel:{s['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Нет подписок", callback_data="noop")]])


def delete_subscription(user_id: int, sub_id: int) -> bool:
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("DELETE FROM route_subscriptions WHERE id=? AND user_id=?", (sub_id, user_id))
        return cur.rowcount > 0


def coincidence_already_notified(post_a_id: int, post_b_id: int) -> bool:
    a, b = sorted([post_a_id, post_b_id])
    with closing(connect_db()) as conn:
        row = conn.execute("""
            SELECT id FROM coincidence_notifications
            WHERE post_a_id=? AND post_b_id=?
        """, (a, b)).fetchone()
        return row is not None


def mark_coincidence_notified(post_a_id: int, post_b_id: int):
    a, b = sorted([post_a_id, post_b_id])
    with closing(connect_db()) as conn, conn:
        conn.execute("""
            INSERT OR IGNORE INTO coincidence_notifications (post_a_id, post_b_id, created_at)
            VALUES (?, ?, ?)
        """, (a, b, now_ts()))


def calculate_coincidence_score(source_row: sqlite3.Row, candidate_row: sqlite3.Row) -> Tuple[int, List[str]]:
    """
    source_row — текущее объявление
    candidate_row — противоположный тип объявления
    """
    score = 40  # базовое совпадение по странам
    notes: List[str] = []

    source_from_city = normalize_text(source_row["from_city"])
    candidate_from_city = normalize_text(candidate_row["from_city"])
    source_to_city = normalize_text(source_row["to_city"])
    candidate_to_city = normalize_text(candidate_row["to_city"])

    # Город отправления
    if source_from_city and candidate_from_city:
        if source_from_city == candidate_from_city:
            score += 15
            notes.append("Совпадает город отправления")
        else:
            score -= 8
            notes.append("Разные города отправления")
    else:
        score += 6
        notes.append("Один из городов отправления не указан")

    # Город назначения
    if source_to_city and candidate_to_city:
        if source_to_city == candidate_to_city:
            score += 15
            notes.append("Совпадает город назначения")
        else:
            score -= 8
            notes.append("Разные города назначения")
    else:
        score += 6
        notes.append("Один из городов назначения не указан")

    # Дата
    source_date = parse_date_loose(source_row["travel_date"])
    candidate_date = parse_date_loose(candidate_row["travel_date"])
    if source_date and candidate_date:
        days_diff = abs((source_date - candidate_date).days)
        if days_diff <= 2:
            score += 18
            notes.append("Даты очень близки")
        elif days_diff <= 7:
            score += 10
            notes.append("Даты близки")
        else:
            score -= 6
            notes.append("Даты заметно отличаются")
    else:
        score += 4
        notes.append("Хотя бы одна дата указана неточно")

    # Вес
    source_weight = parse_weight_kg(source_row["weight_kg"])
    candidate_weight = parse_weight_kg(candidate_row["weight_kg"])

    # Нужно понять кто попутчик, кто посылка
    trip_weight = None
    parcel_weight = None
    if source_row["post_type"] == TYPE_TRIP:
        trip_weight = source_weight
        parcel_weight = candidate_weight
    else:
        trip_weight = candidate_weight
        parcel_weight = source_weight

    if trip_weight is not None and parcel_weight is not None:
        if trip_weight >= parcel_weight:
            score += 18
            notes.append("Вес подходит полностью")
        else:
            ratio = 0 if parcel_weight == 0 else trip_weight / parcel_weight
            if ratio >= 0.5:
                score += 10
                notes.append(f"Вес подходит частично: можно взять около {trip_weight:g} кг из {parcel_weight:g} кг")
            elif ratio > 0:
                score += 4
                notes.append(f"Вес подходит слабо: можно взять около {trip_weight:g} кг из {parcel_weight:g} кг")
            else:
                score -= 4
                notes.append("По весу совпадение слабое")
    else:
        score += 4
        notes.append("Вес указан неточно")

    return score, notes


def get_coincidences(
    post_type: str,
    from_country: str,
    to_country: str,
    exclude_user_id: Optional[int] = None,
    source_row: Optional[sqlite3.Row] = None,
    limit: int = 20
) -> List[dict]:
    target_type = TYPE_PARCEL if post_type == TYPE_TRIP else TYPE_TRIP

    query = """
        SELECT p.*, u.username, u.full_name
        FROM posts p
        LEFT JOIN users u ON u.user_id = p.user_id
        WHERE p.post_type=?
          AND p.status='active'
          AND p.from_country=?
          AND p.to_country=?
          AND (p.expires_at IS NULL OR p.expires_at > ?)
    """
    params: List = [target_type, from_country, to_country, now_ts()]

    if exclude_user_id is not None:
        query += " AND p.user_id != ?"
        params.append(exclude_user_id)

    query += " ORDER BY COALESCE(p.bumped_at, p.created_at) DESC LIMIT 100"

    with closing(connect_db()) as conn:
        rows = conn.execute(query, params).fetchall()

    results: List[dict] = []

    for row in rows:
        score = 45
        notes: List[str] = ["Совпадает маршрут по странам"]

        if source_row is not None:
            score, notes = calculate_coincidence_score(source_row, row)

        if score < 35:
            continue

        if score >= 75:
            coincidence_type = "strong"
        elif score >= 55:
            coincidence_type = "good"
        else:
            coincidence_type = "possible"

        results.append({
            "row": row,
            "score": score,
            "notes": notes,
            "type": coincidence_type
        })

    results.sort(key=lambda x: (x["score"], x["row"]["bumped_at"] or x["row"]["created_at"]), reverse=True)
    return results[:limit]


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
            reply_markup=channel_post_kb(post_id, row["post_type"])
        )
        with closing(connect_db()) as conn, conn:
            conn.execute("UPDATE posts SET channel_message_id=? WHERE id=?", (msg.message_id, post_id))
    return _send()


async def safe_publish(bot: Bot, post_id: int):
    try:
        coro = publish_to_channel(bot, post_id)
        if coro:
            await coro
    except Exception as e:
        print(f"CHANNEL PUBLISH ERROR: {e}")


async def remove_post_from_channel(bot: Bot, row):
    if not CHANNEL_USERNAME or not row:
        return
    channel_message_id = row["channel_message_id"] if "channel_message_id" in row.keys() else None
    if not channel_message_id:
        return
    try:
        await bot.delete_message(CHANNEL_USERNAME, channel_message_id)
    except Exception as e:
        print(f"CHANNEL DELETE ERROR: {e}")


async def notify_coincidence_users(bot: Bot, new_post_id: int):
    new_row = get_post(new_post_id)
    if not new_row or new_row["status"] != STATUS_ACTIVE:
        return

    coincidences = get_coincidences(
        post_type=new_row["post_type"],
        from_country=new_row["from_country"],
        to_country=new_row["to_country"],
        exclude_user_id=new_row["user_id"],
        source_row=new_row,
        limit=COINCIDENCE_NOTIFY_LIMIT
    )

    if not coincidences:
        return

    for item in coincidences:
        row = item["row"]
        score = item["score"]
        notes = item["notes"]

        if coincidence_already_notified(new_row["id"], row["id"]):
            continue

        intro = format_coincidence_badges(score, notes)

        try:
            await bot.send_message(
                new_row["user_id"],
                f"🔔 Найдено новое совпадение!\n\n{intro}\n\n{post_text(row)}",
                reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
            )
        except Exception as e:
            print(f"COINCIDENCE SEND A ERROR: {e}")

        try:
            await bot.send_message(
                row["user_id"],
                f"🔔 Найдено новое совпадение!\n\n{intro}\n\n{post_text(new_row)}",
                reply_markup=public_post_kb(new_row["id"], new_row["user_id"], new_row["post_type"])
            )
        except Exception as e:
            print(f"COINCIDENCE SEND B ERROR: {e}")

        mark_coincidence_notified(new_row["id"], row["id"])


async def notify_subscribers(bot: Bot, post_id: int):
    row = get_post(post_id)
    if not row or row["status"] != STATUS_ACTIVE:
        return

    with closing(connect_db()) as conn:
        subscribers = conn.execute("""
            SELECT * FROM route_subscriptions
            WHERE post_type=? AND from_country=? AND to_country=? AND user_id != ?
            ORDER BY created_at DESC
            LIMIT 50
        """, (row["post_type"], row["from_country"], row["to_country"], row["user_id"])).fetchall()

    for sub in subscribers:
        try:
            await bot.send_message(
                sub["user_id"],
                "🔔 По вашей подписке появилось новое объявление:\n\n"
                + post_text(row),
                reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
            )
        except Exception as e:
            print(f"SUBSCRIBER SEND ERROR: {e}")


async def run_global_coincidence_scan(bot: Bot):
    try:
        with closing(connect_db()) as conn:
            rows = conn.execute("""
                SELECT p.*, u.username, u.full_name
                FROM posts p
                LEFT JOIN users u ON u.user_id = p.user_id
                WHERE p.status='active'
                  AND (p.expires_at IS NULL OR p.expires_at > ?)
                ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
                LIMIT 300
            """, (now_ts(),)).fetchall()

        for row in rows:
            coincidences = get_coincidences(
                post_type=row["post_type"],
                from_country=row["from_country"],
                to_country=row["to_country"],
                exclude_user_id=row["user_id"],
                source_row=row,
                limit=COINCIDENCE_NOTIFY_LIMIT
            )

            for item in coincidences:
                target = item["row"]
                score = item["score"]
                notes = item["notes"]

                if coincidence_already_notified(row["id"], target["id"]):
                    continue

                intro = format_coincidence_badges(score, notes)

                try:
                    await bot.send_message(
                        row["user_id"],
                        f"🔔 Найдено новое совпадение!\n\n{intro}\n\n{post_text(target)}",
                        reply_markup=public_post_kb(target["id"], target["user_id"], target["post_type"])
                    )
                except Exception as e:
                    print(f"GLOBAL COINCIDENCE SEND A ERROR: {e}")

                try:
                    await bot.send_message(
                        target["user_id"],
                        f"🔔 Найдено новое совпадение!\n\n{intro}\n\n{post_text(row)}",
                        reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
                    )
                except Exception as e:
                    print(f"GLOBAL COINCIDENCE SEND B ERROR: {e}")

                mark_coincidence_notified(row["id"], target["id"])

    except Exception as e:
        print(f"GLOBAL COINCIDENCE SCAN ERROR: {e}")


def owner_only(callback: CallbackQuery, post_id: int) -> Optional[sqlite3.Row]:
    row = get_post(post_id)
    if not row or row["user_id"] != callback.from_user.id:
        return None
    return row


async def expire_old_posts(bot: Bot):
    while True:
        try:
            with closing(connect_db()) as conn:
                rows = conn.execute("""
                    SELECT p.*, u.username, u.full_name
                    FROM posts p
                    LEFT JOIN users u ON u.user_id = p.user_id
                    WHERE p.status IN ('active','inactive')
                      AND p.expires_at IS NOT NULL
                      AND p.expires_at <= ?
                    LIMIT 50
                """, (now_ts(),)).fetchall()

            if rows:
                for row in rows:
                    await remove_post_from_channel(bot, row)

                with closing(connect_db()) as conn, conn:
                    for row in rows:
                        conn.execute(
                            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
                            (STATUS_EXPIRED, now_ts(), row["id"])
                        )

                for row in rows:
                    try:
                        await bot.send_message(
                            row["user_id"],
                            f"⌛ Ваше объявление ID {row['id']} истекло и скрыто.\n"
                            "Откройте 'Мои объявления', чтобы активировать его снова.",
                            reply_markup=main_menu(row["user_id"])
                        )
                    except Exception as e:
                        print(f"EXPIRE USER NOTIFY ERROR: {e}")
        except Exception as e:
            print(f"EXPIRE LOOP ERROR: {e}")

        await asyncio.sleep(300)


async def global_coincidence_loop(bot: Bot):
    while True:
        await run_global_coincidence_scan(bot)
        await asyncio.sleep(300)


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    upsert_user(message)
    await state.clear()

    text = (
        "👋 <b>Привет.</b>\n\n"

        "Это <b>Попутчик Китай</b> — бот для передачи посылок через попутчиков.\n\n"
  
        "<b>Здесь ты сможешь отправить свою и доставить посылку других людей за вознаграждение.</b>\n\n"
        
        "🔎 <b>Первым делом подпишись на наш канал. Туда будут приходить все новые обьявления о посылках </b>\n"
       "t.me/china_poputchik\n\n"
        
        "Я сам буду искать попутчиков для тебя.\n"
        "Я сам буду уведомлять тебя о совпадениях.\n"
        
        "Что же необходимо от тебя?\n"
        
        "<b>Нажми на кнопку МЕНЮ и тчательно заполни заявку</b>\n"
        "📦 <b>P.s. Если у тебя что-то не получается - перезапусти БОТ снова нажав на кнопку НАЧАТЬ.</b>\n\

        "⬇️ <b>Выберите действие в меню ниже</b>"
    )
    await message.answer(text, reply_markup=main_menu(message.from_user.id))

    if start_arg == "parcel":
        await begin_create(message, state, TYPE_PARCEL)
        return
    if start_arg == "trip":
        await begin_create(message, state, TYPE_TRIP)
        return

    if start_arg.startswith("post_"):
        post_id_str = start_arg.replace("post_", "", 1)
        if post_id_str.isdigit():
            row = get_post(int(post_id_str))
            if row and row["status"] == STATUS_ACTIVE:
                await message.answer(
                    "📤 Открыто объявление по ссылке:\n\n" + post_text(row),
                    reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
                )
            else:
                await message.answer("Объявление не найдено или уже неактивно.")
        return

    if start_arg.startswith("route_"):
        try:
            _, post_type, from_country, to_country = start_arg.split("_", 3)
            rows = search_route_posts(post_type, from_country, to_country, limit=10)
            if not rows:
                await message.answer(f"По маршруту {from_country} → {to_country} пока нет активных объявлений.")
            else:
                await message.answer(
                    f"Маршрут: <b>{from_country} → {to_country}</b>\n"
                    f"Тип: {'✈️ Попутчики' if post_type == TYPE_TRIP else '📦 Посылки'}\n"
                    f"Найдено: {len(rows)}"
                )
                for row in rows:
                    await message.answer(
                        post_text(row),
                        reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
                    )
        except Exception:
            await message.answer("Не удалось открыть маршрут.")
        return


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
        "Важно: бот — только площадка для поиска. Мы не принимаем оплату и не выступаем посредником.",
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
    await state.update_data(from_city=None if message.text.strip() == "-" else message.text.strip()[:80])
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
    await state.update_data(to_city=None if message.text.strip() == "-" else message.text.strip()[:80])
    await state.set_state(CreatePost.travel_date)
    await message.answer("Дата поездки/отправки. Например: 2026-03-15 или 15.03.2026. Если дата не точная — напиши как удобно.")


@router.message(CreatePost.travel_date)
async def enter_date(message: Message, state: FSMContext):
    await state.update_data(travel_date=message.text.strip()[:100])
    await state.set_state(CreatePost.weight)
    await message.answer("Вес или объем. Например: до 3 кг. Если неизвестно — напиши '-'")


@router.message(CreatePost.weight)
async def enter_weight(message: Message, state: FSMContext):
    await state.update_data(weight_kg=None if message.text.strip() == "-" else message.text.strip()[:50])
    await state.set_state(CreatePost.description)
    await message.answer("Опиши объявление подробно: что нужно передать / сколько места есть / условия.")


@router.message(CreatePost.description)
async def enter_description(message: Message, state: FSMContext):
    await state.update_data(description=message.text.strip()[:1000])
    await state.set_state(CreatePost.contact_note)
    await message.answer("Доп. контакт или примечание. Например: WeChat / только text / без звонков. Если не надо — напиши '-'")


@router.message(CreatePost.contact_note)
async def finalize_post(message: Message, state: FSMContext, bot: Bot):
    try:
        data = await state.get_data()
        data["contact_note"] = None if message.text.strip() == "-" else message.text.strip()[:200]

        post_id = create_post_record(data, message.from_user.id)
        row = get_post(post_id)

        await state.clear()

        if not row:
            await message.answer(
                "Ошибка: объявление создалось некорректно. Попробуйте ещё раз.",
                reply_markup=main_menu(message.from_user.id)
            )
            return

        await message.answer(
            "✅ Объявление создано.\n" +
            ("Оно отправлено на модерацию." if ADMIN_IDS else "Оно уже активно."),
            reply_markup=main_menu(message.from_user.id)
        )

        await message.answer(
            post_text(row),
            reply_markup=post_actions_kb(post_id, row["status"])
        )

        if ADMIN_IDS and row["status"] == STATUS_PENDING:
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        "Новое объявление на модерации:\n\n" + post_text(row),
                        reply_markup=admin_post_actions_kb(post_id)
                    )
                except Exception as e:
                    print(f"ADMIN NOTIFY ERROR: {e}")
        else:
            try:
                await safe_publish(bot, post_id)
            except Exception as e:
                print(f"SAFE_PUBLISH ERROR: {e}")

            try:
                await notify_coincidence_users(bot, post_id)
            except Exception as e:
                print(f"NOTIFY_COINCIDENCE_USERS ERROR: {e}")

            try:
                await notify_subscribers(bot, post_id)
            except Exception as e:
                print(f"NOTIFY_SUBSCRIBERS ERROR: {e}")

    except Exception as e:
        print(f"FINALIZE_POST ERROR: {e}")
        await message.answer(
            f"Произошла ошибка при сохранении объявления: {e}",
            reply_markup=main_menu(message.from_user.id)
        )


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


@router.callback_query(F.data.startswith("delete:"))
async def delete_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await remove_post_from_channel(callback.bot, row)

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

    await remove_post_from_channel(callback.bot, row)

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
    expires_at = now_ts() + days_to_seconds(POST_TTL_DAYS)

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET status=?, updated_at=?, expires_at=? WHERE id=?",
            (new_status, now_ts(), expires_at, post_id)
        )

    await callback.message.answer(
        f"Объявление {post_id} " + ("отправлено на повторную модерацию." if ADMIN_IDS else "активировано.")
    )

    if not ADMIN_IDS:
        await safe_publish(bot, post_id)
        await notify_coincidence_users(bot, post_id)
        await notify_subscribers(bot, post_id)

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
        conn.execute("UPDATE posts SET bumped_at=?, updated_at=? WHERE id=?", (now_ts(), now_ts(), post_id))
    await callback.message.answer(f"Объявление {post_id} поднято выше в поиске.\n{BUMP_PRICE_TEXT}")
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
    source_post_type = TYPE_TRIP if data["looking_for"] == "trip" else TYPE_PARCEL

    pseudo_source = {
        "post_type": source_post_type,
        "from_country": data["from_country"],
        "to_country": country,
        "from_city": None,
        "to_city": None,
        "travel_date": None,
        "weight_kg": None,
        "user_id": callback.from_user.id,
    }

    coincidences = get_coincidences(
        post_type=source_post_type,
        from_country=data["from_country"],
        to_country=country,
        exclude_user_id=callback.from_user.id,
        source_row=pseudo_source,
        limit=10
    )

    await state.clear()

    if not coincidences:
        await callback.message.answer("Совпадений пока нет.")
    else:
        await callback.message.answer(f"Найдено совпадений: {len(coincidences)}")
        for item in coincidences:
            row = item["row"]
            score = item["score"]
            notes = item["notes"]
            intro = format_coincidence_badges(score, notes)
            await callback.message.answer(
                f"{intro}\n\n{post_text(row)}",
                reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
            )

    await callback.answer()


@router.callback_query(F.data.startswith("coincidences:"))
async def coincidences_for_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    coincidences = get_coincidences(
        post_type=row["post_type"],
        from_country=row["from_country"],
        to_country=row["to_country"],
        exclude_user_id=callback.from_user.id,
        source_row=row,
        limit=10
    )

    if not coincidences:
        await callback.message.answer("Совпадений пока нет.")
    else:
        await callback.message.answer(f"Найдено совпадений: {len(coincidences)}")
        for item in coincidences:
            found_row = item["row"]
            score = item["score"]
            notes = item["notes"]
            intro = format_coincidence_badges(score, notes)
            await callback.message.answer(
                f"{intro}\n\n{post_text(found_row)}",
                reply_markup=public_post_kb(found_row["id"], found_row["user_id"], found_row["post_type"])
            )

    await callback.answer()


@router.message(F.text == "🔥 Популярные маршруты")
async def popular_routes_handler(message: Message):
    rows = get_popular_routes(10)
    if not rows:
        await message.answer("Пока нет активных маршрутов.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer("🔥 Популярные маршруты сейчас:", reply_markup=popular_routes_kb(rows))


@router.callback_query(F.data.startswith("popular:"))
async def popular_route_open(callback: CallbackQuery):
    _, post_type, from_country, to_country = callback.data.split(":", 3)
    rows = search_route_posts(post_type, from_country, to_country, limit=10)
    if not rows:
        await callback.message.answer("По этому маршруту сейчас нет активных объявлений.")
    else:
        await callback.message.answer(
            f"Маршрут: <b>{from_country} → {to_country}</b>\n"
            f"Тип: {'✈️ Попутчики' if post_type == TYPE_TRIP else '📦 Посылки'}\n"
            f"Найдено: {len(rows)}"
        )
        for row in rows:
            await callback.message.answer(
                post_text(row),
                reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
            )
    await callback.answer()


@router.message(F.text == "🆕 Новые объявления")
async def recent_posts_handler(message: Message):
    rows = get_recent_posts(10)
    if not rows:
        await message.answer("Пока нет новых активных объявлений.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer("🆕 Последние объявления:")
    for row in rows:
        await message.answer(
            f"{post_text(row)}\n\n<b>Добавлено:</b> {format_age(row['created_at'])}",
            reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
        )


@router.message(F.text == "📊 Статистика")
async def stats_handler(message: Message):
    stats = service_stats()
    route = top_route()
    text = (
        "📊 <b>Статистика сервиса</b>\n\n"
        f"Пользователей: <b>{stats['users_count']}</b>\n"
        f"Активных объявлений: <b>{stats['active_posts']}</b>\n"
        f"✈️ Попутчиков: <b>{stats['active_trips']}</b>\n"
        f"📦 Посылок: <b>{stats['active_parcels']}</b>\n"
    )
    if route:
        text += f"\nПопулярный маршрут:\n<b>{route['from_country']} → {route['to_country']}</b> ({route['cnt']})"
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.message(F.text == "🔔 Подписки")
async def subscriptions_menu(message: Message):
    await message.answer("Подписки на маршруты:", reply_markup=subscription_actions_kb())


@router.callback_query(F.data == "sub:new")
async def sub_new_start(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Хочу получать попутчиков для посылки", callback_data="subtype:trip")],
        [InlineKeyboardButton(text="Хочу получать посылки для маршрута", callback_data="subtype:parcel")]
    ])
    await state.set_state(SubscriptionFlow.looking_for)
    await callback.message.answer("Что отслеживать?", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("subtype:"))
async def sub_type(callback: CallbackQuery, state: FSMContext):
    post_type = callback.data.split(":")[1]
    await state.update_data(post_type=post_type)
    await state.set_state(SubscriptionFlow.from_country)
    await callback.message.answer("Выбери страну отправления:", reply_markup=countries_kb("subfrom"))
    await callback.answer()


@router.callback_query(F.data.startswith("subfrom:"))
async def sub_from(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    await state.update_data(from_country=country)
    await state.set_state(SubscriptionFlow.to_country)
    await callback.message.answer("Выбери страну назначения:", reply_markup=countries_kb("subto"))
    await callback.answer()


@router.callback_query(F.data.startswith("subto:"))
async def sub_to(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    data = await state.get_data()
    add_route_subscription(callback.from_user.id, data["post_type"], data["from_country"], country)
    await state.clear()
    await callback.message.answer(
        f"✅ Подписка сохранена: {data['from_country']} → {country}\n"
        "Бот будет присылать новые подходящие объявления.",
        reply_markup=main_menu(callback.from_user.id)
    )
    await callback.answer()


@router.callback_query(F.data == "sub:list")
async def sub_list(callback: CallbackQuery):
    subs = list_route_subscriptions(callback.from_user.id)
    if not subs:
        await callback.message.answer("У вас пока нет подписок.")
    else:
        await callback.message.answer("Ваши подписки. Нажмите на нужную, чтобы удалить:", reply_markup=subscription_list_kb(subs))
    await callback.answer()


@router.callback_query(F.data.startswith("subdel:"))
async def sub_delete(callback: CallbackQuery):
    sub_id = int(callback.data.split(":")[1])
    ok = delete_subscription(callback.from_user.id, sub_id)
    await callback.answer("Подписка удалена" if ok else "Не найдено", show_alert=True)


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
            + "Чтобы ответить, напиши пользователю напрямую"
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


@router.message(F.text == "⭐ Оставить отзыв")
async def review_start(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(ReviewFlow.reviewed_user_id)
    await message.answer("Введите USER_ID пользователя, которому хотите оставить отзыв.")


@router.message(ReviewFlow.reviewed_user_id)
async def review_user_step(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Нужен числовой USER_ID.")
        return
    await state.update_data(reviewed_user_id=int(message.text))
    await state.set_state(ReviewFlow.post_id)
    await message.answer("Введите ID объявления, по которому был контакт. Если нет — напишите 0.")


@router.message(ReviewFlow.post_id)
async def review_post_step(message: Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Нужен числовой ID объявления.")
        return
    await state.update_data(post_id=int(message.text))
    await state.set_state(ReviewFlow.rating)
    await message.answer("Поставьте оценку от 1 до 5.")


@router.message(ReviewFlow.rating)
async def review_rating_step(message: Message, state: FSMContext):
    if not message.text.isdigit() or int(message.text) not in [1, 2, 3, 4, 5]:
        await message.answer("Введите число от 1 до 5.")
        return
    await state.update_data(rating=int(message.text))
    await state.set_state(ReviewFlow.text)
    await message.answer("Напишите короткий отзыв или '-' если без текста.")


@router.message(ReviewFlow.text)
async def review_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    review_text = None if message.text.strip() == "-" else message.text.strip()[:500]
    reviewed_user_id = data["reviewed_user_id"]
    post_id = data["post_id"]
    if post_id == 0:
        post_id = None

    with closing(connect_db()) as conn, conn:
        try:
            conn.execute("""
                INSERT INTO reviews (reviewer_user_id, reviewed_user_id, post_id, rating, text, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                message.from_user.id,
                reviewed_user_id,
                post_id,
                data["rating"],
                review_text,
                now_ts()
            ))
        except sqlite3.IntegrityError:
            await message.answer("Такой отзыв уже оставлен.", reply_markup=main_menu(message.from_user.id))
            await state.clear()
            return

    await message.answer("Спасибо! Отзыв сохранен.", reply_markup=main_menu(message.from_user.id))
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
        "Команды:\n"
        "/admin_pending\n"
        "/admin_complaints\n"
        "/admin_ban USER_ID\n"
        "/admin_unban USER_ID\n"
        "/admin_verify USER_ID\n"
        "/admin_unverify USER_ID"
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
    await notify_coincidence_users(bot, post_id)
    await notify_subscribers(bot, post_id)
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


@router.message(Command("admin_verify"))
async def admin_verify(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_verify USER_ID")
        return
    user_id = int(parts[1])
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_verified=1 WHERE user_id=?", (user_id,))
    await message.answer(f"Пользователь {user_id} отмечен как проверенный.")


@router.message(Command("admin_unverify"))
async def admin_unverify(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_unverify USER_ID")
        return
    user_id = int(parts[1])
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_verified=0 WHERE user_id=?", (user_id,))
    await message.answer(f"Статус проверенного у пользователя {user_id} снят.")


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

    asyncio.create_task(expire_old_posts(bot))
    asyncio.create_task(global_coincidence_loop(bot))

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
