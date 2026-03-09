import asyncio
import html
import os
import re
import sqlite3
import time
from contextlib import closing
from datetime import datetime
from typing import List, Optional, Tuple

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "/data/bot.db")

db_dir = os.path.dirname(DB_PATH)
if db_dir:
    os.makedirs(db_dir, exist_ok=True)

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "Poputchik_china_bot").lstrip("@")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

BUMP_PRICE_TEXT = os.getenv(
    "BUMP_PRICE_TEXT",
    "Поднятие объявления оплачивается вручную. После оплаты администратор поднимет ваше объявление выше в выдаче."
)

MAX_ACTIVE_POSTS_PER_USER = int(os.getenv("MAX_ACTIVE_POSTS_PER_USER", "5"))
MIN_SECONDS_BETWEEN_ACTIONS = int(os.getenv("MIN_SECONDS_BETWEEN_ACTIONS", "2"))
POST_TTL_DAYS = int(os.getenv("POST_TTL_DAYS", "14"))
COINCIDENCE_NOTIFY_LIMIT = int(os.getenv("COINCIDENCE_NOTIFY_LIMIT", "5"))
BUMP_PRICE_AMOUNT = int(os.getenv("BUMP_PRICE_AMOUNT", "10"))
BUMP_PRICE_CURRENCY = os.getenv("BUMP_PRICE_CURRENCY", "CNY")

router = Router()

TYPE_TRIP = "trip"
TYPE_PARCEL = "parcel"

STATUS_PENDING = "pending"
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"
STATUS_REJECTED = "rejected"
STATUS_EXPIRED = "expired"
STATUS_DELETED = "deleted"

DEAL_CONTACTED = "contacted"
DEAL_OFFERED = "offered"
DEAL_ACCEPTED = "accepted"
DEAL_COMPLETED_BY_OWNER = "completed_by_owner"
DEAL_COMPLETED_BY_REQUESTER = "completed_by_requester"
DEAL_COMPLETED = "completed"
DEAL_FAILED = "failed"
DEAL_CANCELLED = "cancelled"

MANUAL_COUNTRY = "🌍 Ввести другую страну"
MANUAL_CITY = "✏️ Ввести другой город"
MANUAL_WEIGHT = "✏️ Указать другой вес"
MANUAL_DATE = "✏️ Указать точную дату"
SKIP_VALUE = "-"

COUNTRY_OPTIONS = [
    ("🇨🇳 Китай", "Китай"),
    ("🇷🇺 Россия", "Россия"),
    ("🇰🇿 Казахстан", "Казахстан"),
    ("🇺🇿 Узбекистан", "Узбекистан"),
    ("🇰🇬 Кыргызстан", "Кыргызстан"),
    ("🇹🇯 Таджикистан", "Таджикистан"),
    ("🇦🇿 Азербайджан", "Азербайджан"),
    ("🇦🇲 Армения", "Армения"),
    ("🇬🇪 Грузия", "Грузия"),
    ("🇧🇾 Беларусь", "Беларусь"),
    ("🇺🇦 Украина", "Украина"),
    ("🇲🇩 Молдова", "Молдова"),
    ("🇻🇳 Вьетнам", "Вьетнам"),
    ("🇹🇭 Таиланд", "Таиланд"),
    ("🇦🇪 ОАЭ", "ОАЭ"),
]

COUNTRY_CITIES_RU = {
    "Китай": [
        "Шэньчжэнь", "Гуанчжоу", "Шанхай", "Пекин", "Ханчжоу",
        "Иу", "Гонконг", "Дунгуань", "Фошань", "Чжухай",
        "Сямынь", "Чэнду", "Чунцин", "Сучжоу", "Циндао",
        "Тяньцзинь", "Нинбо"
    ],
    "Россия": [
        "Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург",
        "Казань", "Владивосток", "Хабаровск", "Иркутск"
    ],
    "Казахстан": ["Алматы", "Астана", "Шымкент"],
    "Узбекистан": ["Ташкент", "Самарканд"],
    "Кыргызстан": ["Бишкек", "Ош"],
    "Таджикистан": ["Душанбе", "Худжанд"],
    "Азербайджан": ["Баку"],
    "Армения": ["Ереван"],
    "Грузия": ["Тбилиси", "Батуми"],
    "Беларусь": ["Минск"],
    "Украина": ["Киев", "Львов", "Одесса"],
    "Молдова": ["Кишинёв"],
    "Вьетнам": ["Хошимин", "Ханой", "Дананг"],
    "Таиланд": ["Бангкок", "Паттайя", "Пхукет"],
    "ОАЭ": ["Дубай", "Абу-Даби"],
}

POPULAR_WEIGHTS = [
    "0.5 кг", "1 кг", "2 кг", "3 кг",
    "5 кг", "10 кг", "20 кг", "Более 20 кг"
]

POPULAR_DATES = [
    "Сегодня", "Завтра", "Через 2-3 дня",
    "В течение недели", "В течение месяца"
]

COUNTRY_ALIASES = {
    "китай": "Китай",
    "china": "Китай",
    "кнр": "Китай",
    "россия": "Россия",
    "russia": "Россия",
    "рф": "Россия",
    "казахстан": "Казахстан",
    "kazakhstan": "Казахстан",
    "узбекистан": "Узбекистан",
    "uzbekistan": "Узбекистан",
    "кыргызстан": "Кыргызстан",
    "киргизия": "Кыргызстан",
    "kyrgyzstan": "Кыргызстан",
    "таджикистан": "Таджикистан",
    "tajikistan": "Таджикистан",
    "азербайджан": "Азербайджан",
    "azerbaijan": "Азербайджан",
    "армения": "Армения",
    "armenia": "Армения",
    "грузия": "Грузия",
    "georgia": "Грузия",
    "беларусь": "Беларусь",
    "belarus": "Беларусь",
    "украина": "Украина",
    "ukraine": "Украина",
    "молдова": "Молдова",
    "moldova": "Молдова",
    "вьетнам": "Вьетнам",
    "vietnam": "Вьетнам",
    "таиланд": "Таиланд",
    "thailand": "Таиланд",
    "оаэ": "ОАЭ",
    "uae": "ОАЭ",
    "дубай": "ОАЭ",
}

CITY_ALIASES = {
    "шэньчжэнь": "Шэньчжэнь",
    "шеньчжень": "Шэньчжэнь",
    "шенжень": "Шэньчжэнь",
    "shenzhen": "Шэньчжэнь",
    "гуанчжоу": "Гуанчжоу",
    "guangzhou": "Гуанчжоу",
    "кантон": "Гуанчжоу",
    "шанхай": "Шанхай",
    "shanghai": "Шанхай",
    "пекин": "Пекин",
    "beijing": "Пекин",
    "иу": "Иу",
    "yiwu": "Иу",
    "гонконг": "Гонконг",
    "hong kong": "Гонконг",
    "hongkong": "Гонконг",
    "дунгуань": "Дунгуань",
    "dongguan": "Дунгуань",
    "фошань": "Фошань",
    "foshan": "Фошань",
    "чжухай": "Чжухай",
    "zhuhai": "Чжухай",
    "сямынь": "Сямынь",
    "xiamen": "Сямынь",
    "чэнду": "Чэнду",
    "chengdu": "Чэнду",
    "чунцин": "Чунцин",
    "chongqing": "Чунцин",
    "сучжоу": "Сучжоу",
    "suzhou": "Сучжоу",
    "циндао": "Циндао",
    "qingdao": "Циндао",
    "тяньцзинь": "Тяньцзинь",
    "tianjin": "Тяньцзинь",
    "нинбо": "Нинбо",
    "ningbo": "Нинбо",
    "москва": "Москва",
    "moscow": "Москва",
    "санкт-петербург": "Санкт-Петербург",
    "санкт петербург": "Санкт-Петербург",
    "питер": "Санкт-Петербург",
    "spb": "Санкт-Петербург",
    "saint petersburg": "Санкт-Петербург",
    "алматы": "Алматы",
    "almaty": "Алматы",
    "астана": "Астана",
    "astana": "Астана",
    "ташкент": "Ташкент",
    "tashkent": "Ташкент",
    "бишкек": "Бишкек",
    "bishkek": "Бишкек",
    "дубай": "Дубай",
    "dubai": "Дубай",
    "абу-даби": "Абу-Даби",
    "абу даби": "Абу-Даби",
    "abu dhabi": "Абу-Даби",
}


# -------------------------
# SERVICE HELPERS
# -------------------------

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


def normalize_free_text(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.replace("ё", "е")
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_country_input(value: str) -> str:
    raw = normalize_free_text(value)
    return COUNTRY_ALIASES.get(raw, value.strip().title())


def normalize_city_input(value: str) -> str:
    raw = normalize_free_text(value)
    return CITY_ALIASES.get(raw, value.strip().title())


def parse_weight_kg(value: Optional[str]) -> Optional[float]:
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


def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str):
    cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


# -------------------------
# DB INIT
# -------------------------

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

        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            requester_user_id INTEGER NOT NULL,
            initiator_user_id INTEGER,
            status TEXT NOT NULL DEFAULT 'contacted',
            owner_confirmed INTEGER DEFAULT 0,
            requester_confirmed INTEGER DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            completed_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS bump_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            post_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            currency TEXT NOT NULL DEFAULT 'CNY',
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            paid_at INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_posts_search
        ON posts(post_type, status, from_country, to_country, created_at);

        CREATE INDEX IF NOT EXISTS idx_posts_user
        ON posts(user_id, status, created_at);

        CREATE INDEX IF NOT EXISTS idx_subscriptions_search
        ON route_subscriptions(post_type, from_country, to_country);

        CREATE INDEX IF NOT EXISTS idx_reviews_user
        ON reviews(reviewed_user_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_deals_owner
        ON deals(owner_user_id, status, created_at);

        CREATE INDEX IF NOT EXISTS idx_deals_requester
        ON deals(requester_user_id, status, created_at);
        """)

        ensure_column(conn, "users", "is_verified", "is_verified INTEGER DEFAULT 0")
        ensure_column(conn, "users", "is_banned", "is_banned INTEGER DEFAULT 0")
        ensure_column(conn, "users", "last_action_at", "last_action_at INTEGER DEFAULT 0")
        ensure_column(conn, "posts", "expires_at", "expires_at INTEGER")
        ensure_column(conn, "deals", "initiator_user_id", "initiator_user_id INTEGER")
        ensure_column(conn, "deals", "owner_confirmed", "owner_confirmed INTEGER DEFAULT 0")
        ensure_column(conn, "deals", "requester_confirmed", "requester_confirmed INTEGER DEFAULT 0")
        ensure_column(conn, "deals", "updated_at", "updated_at INTEGER DEFAULT 0")

        conn.execute("UPDATE deals SET updated_at = created_at WHERE updated_at IS NULL OR updated_at = 0")
        conn.execute("UPDATE deals SET status='contacted' WHERE status='pending'")


# -------------------------
# USER / PROFILE HELPERS
# -------------------------

def upsert_user(message_or_callback):
    user = message_or_callback.from_user
    with closing(connect_db()) as conn, conn:
        existing = conn.execute(
            "SELECT created_at FROM users WHERE user_id=?",
            (user.id,)
        ).fetchone()
        created_at = int(existing["created_at"]) if existing and existing["created_at"] else now_ts()

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
            created_at
        ))


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def is_user_banned(user_id: int) -> bool:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT is_banned FROM users WHERE user_id=?", (user_id,)).fetchone()
        return bool(row and row["is_banned"])


def ban_user(user_id: int):
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (user_id,))
        conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE user_id=? AND status IN ('active','pending','inactive')",
            (STATUS_INACTIVE, now_ts(), user_id)
        )


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


def user_service_days(user_id: int) -> int:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT created_at FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not row or not row["created_at"]:
            return 0
        return max(0, (now_ts() - int(row["created_at"])) // 86400)


def user_service_text(user_id: int) -> str:
    days = user_service_days(user_id)
    if days < 30:
        return f"{days} дн"
    if days < 365:
        months = max(1, days // 30)
        return f"{months} мес"
    years = max(1, days // 365)
    return f"{years} г"


def user_completed_deals_count(user_id: int) -> int:
    with closing(connect_db()) as conn:
        row = conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM deals
            WHERE status='completed'
              AND (owner_user_id=? OR requester_user_id=?)
        """, (user_id, user_id)).fetchone()
        return int(row["cnt"] or 0)


def is_user_verified(user_id: int) -> bool:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT is_verified FROM users WHERE user_id=?", (user_id,)).fetchone()
        return bool(row and row["is_verified"])


def reviews_word(n: int) -> str:
    n = abs(n) % 100
    n1 = n % 10
    if 11 <= n <= 19:
        return "отзывов"
    if n1 == 1:
        return "отзыв"
    if 2 <= n1 <= 4:
        return "отзыва"
    return "отзывов"


def format_rating_line(user_id: int) -> Optional[str]:
    avg_rating, cnt = user_rating_summary(user_id)
    if cnt <= 0:
        return None
    stars = "⭐" * max(1, min(5, round(avg_rating)))
    return f"{stars} {avg_rating:.1f} ({cnt} {reviews_word(cnt)})"


def get_username_by_user_id(user_id: int) -> Optional[str]:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT username FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row["username"] if row and row["username"] else None


def format_user_ref(user_id: int) -> str:
    username = get_username_by_user_id(user_id)
    return f"@{html.escape(username)}" if username else f"USER_ID {user_id}"


def get_user_reviews(user_id: int, limit: int = 10):
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT r.rating, r.text, r.created_at, u.username, u.full_name
            FROM reviews r
            LEFT JOIN users u ON u.user_id = r.reviewer_user_id
            WHERE r.reviewed_user_id=?
            ORDER BY r.created_at DESC
            LIMIT ?
        """, (user_id, limit)).fetchall()


# -------------------------
# TEXT FORMATTING
# -------------------------

def short_post_type(post_type: str) -> str:
    return "✈️ Попутчик" if post_type == TYPE_TRIP else "📦 Посылка"


def format_deal_status(status: str) -> str:
    mapping = {
        DEAL_CONTACTED: "контакт начат",
        DEAL_OFFERED: "предложена",
        DEAL_ACCEPTED: "принята",
        DEAL_COMPLETED_BY_OWNER: "подтвердил владелец",
        DEAL_COMPLETED_BY_REQUESTER: "подтвердил откликнувшийся",
        DEAL_COMPLETED: "завершена",
        DEAL_FAILED: "не договорились",
        DEAL_CANCELLED: "отменена",
    }
    return mapping.get(status, status)


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


def form_header(post_type: str, step: int, total_steps: int = 8) -> str:
    title = "📦 Отправить посылку" if post_type == TYPE_PARCEL else "✈️ Взять посылку"
    return (
        f"{title}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"Шаг {step} / {total_steps}\n"
        f"━━━━━━━━━━━━━━\n\n"
    )


def form_text(post_type: str, step: int, prompt: str, total_steps: int = 8) -> str:
    return form_header(post_type, step, total_steps) + prompt


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
    completed_deals = user_completed_deals_count(owner_user_id)
    service_text = user_service_text(owner_user_id)

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

    lines.append("")
    lines.append("<b>👤 Профиль пользователя</b>")

    if rating_line:
        lines.append(f"⭐ <b>Рейтинг:</b> {rating_line}")
    else:
        lines.append("⭐ <b>Рейтинг:</b> пока нет отзывов")

    lines.append(f"📦 <b>Передач:</b> {completed_deals}")
    lines.append(f"📅 <b>В сервисе:</b> {service_text}")

    lines.append("")
    lines.append(f"<b>ID объявления:</b> {row['id']}")

    if for_channel:
        lines.append("Откройте бот, чтобы добавить своё объявление или найти совпадения.")
    else:
        owner = row["username"] if "username" in row.keys() else None
        if owner:
            lines.append(f"<b>Telegram:</b> @{html.escape(owner)}")

    return "\n".join(lines)


# -------------------------
# KEYBOARDS
# -------------------------

def chunk_buttons(items: List[tuple], prefix: str, per_row: int = 2):
    rows = []
    row = []
    for label, value in items:
        row.append(InlineKeyboardButton(text=label, callback_data=f"{prefix}:{value}"))
        if len(row) == per_row:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def countries_kb(prefix: str):
    rows = chunk_buttons(COUNTRY_OPTIONS, prefix, per_row=2)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def countries_select_kb(prefix: str):
    rows = chunk_buttons(COUNTRY_OPTIONS, prefix, per_row=2)
    rows.append([InlineKeyboardButton(text=MANUAL_COUNTRY, callback_data=f"{prefix}:__manual__")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def cities_select_kb(prefix: str, country: str):
    cities = COUNTRY_CITIES_RU.get(country, [])
    rows = []
    row = []
    for city in cities:
        row.append(InlineKeyboardButton(text=city, callback_data=f"{prefix}:{city}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text=MANUAL_CITY, callback_data=f"{prefix}:__manual__")])
    rows.append([InlineKeyboardButton(text="Не важно", callback_data=f"{prefix}:__skip__")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def weight_select_kb():
    items = [(w, w) for w in POPULAR_WEIGHTS]
    rows = chunk_buttons(items, "weightpick", per_row=2)
    rows.append([InlineKeyboardButton(text=MANUAL_WEIGHT, callback_data="weightpick:__manual__")])
    rows.append([InlineKeyboardButton(text="Не знаю", callback_data="weightpick:__skip__")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def date_select_kb():
    items = [(d, d) for d in POPULAR_DATES]
    rows = chunk_buttons(items, "datepick", per_row=2)
    rows.append([InlineKeyboardButton(text=MANUAL_DATE, callback_data="datepick:__manual__")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def my_posts_kb(posts: List[sqlite3.Row]):
    rows = []
    for p in posts:
        label = f"{p['id']} • {('✈️' if p['post_type'] == TYPE_TRIP else '📦')} • {p['from_country']}→{p['to_country']} • {p['status']}"
        rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"mypost:{p['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Нет объявлений", callback_data="noop")]])


def deal_list_kb(deals: List[sqlite3.Row]):
    rows = []
    for d in deals:
        label = f"{d['id']} • post {d['post_id']} • {format_deal_status(d['status'])}"
        rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"mydeal:{d['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Нет сделок", callback_data="noop")]])


def post_actions_kb(post_id: int, status: str):
    share_url = f"https://t.me/share/url?url={post_deeplink(post_id)}"
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
            InlineKeyboardButton(text="📤 Поделиться", url=share_url)
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
    _, reviews_count = user_rating_summary(owner_id)

    rows = [
        [InlineKeyboardButton(text="✉️ Написать владельцу", callback_data=f"contact:{post_id}:{owner_id}")],
        [InlineKeyboardButton(text="🤝 Предложить сделку", callback_data=f"offer_deal:{post_id}:{owner_id}")]
    ]

    if reviews_count > 0:
        rows.append([
            InlineKeyboardButton(
                text=f"⭐ {reviews_count} {reviews_word(reviews_count)}",
                callback_data=f"user_reviews:{owner_id}"
            )
        ])

    rows.append([InlineKeyboardButton(text="⚠️ Пожаловаться", callback_data=f"complain:{post_id}")])
    rows.append([InlineKeyboardButton(
        text="📤 Поделиться",
        url=f"https://t.me/share/url?url={post_deeplink(post_id)}"
    )])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_post_kb(post_id: int, post_type: Optional[str] = None):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="✉️ Связаться с владельцем",
                url=bot_link(f"contact_{post_id}")
            )],
            [InlineKeyboardButton(
                text="🤝 Открыть объявление",
                url=post_deeplink(post_id)
            )],
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
                callback_data=f"popular:{row['from_country']}:{row['to_country']}"
            )
        ])
    return InlineKeyboardMarkup(
        inline_keyboard=buttons or [[InlineKeyboardButton(text="Пока пусто", callback_data="noop")]]
    )


def deal_open_kb(deal: sqlite3.Row, user_id: int) -> InlineKeyboardMarkup:
    rows = []

    if deal["status"] in (DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
        rows.append([InlineKeyboardButton(text="✅ Подтвердить завершение", callback_data=f"deal_confirm:{deal['id']}")])

    if deal["status"] == DEAL_COMPLETED:
        rows.append([InlineKeyboardButton(text="⭐ Оставить отзыв", callback_data=f"deal_review:{deal['id']}")])

    if deal["status"] in (DEAL_CONTACTED, DEAL_OFFERED, DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
        rows.append([InlineKeyboardButton(text="❌ Не договорились", callback_data=f"deal_fail_direct:{deal['id']}")])

    if not rows:
        rows = [[InlineKeyboardButton(text="Ок", callback_data="noop")]]

    return InlineKeyboardMarkup(inline_keyboard=rows)


def main_menu(user_id: Optional[int] = None):
    keyboard = [
        [KeyboardButton(text="✈️ Взять посылку"), KeyboardButton(text="📦 Отправить посылку")],
        [KeyboardButton(text="🔎 Найти совпадения"), KeyboardButton(text="📋 Мои объявления")],
        [KeyboardButton(text="🤝 Мои сделки"), KeyboardButton(text="🔥 Популярные маршруты")],
        [KeyboardButton(text="🆕 Новые объявления"), KeyboardButton(text="🔔 Подписки")],
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="💰 Поднять объявление")],
        [KeyboardButton(text="🆘 Жалоба"), KeyboardButton(text="ℹ️ Помощь")],
    ]
    if user_id is not None and is_admin(user_id):
        keyboard.append([KeyboardButton(text="👨‍💼 Админка")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


# -------------------------
# FSM STATES
# -------------------------

class CreatePost(StatesGroup):
    from_country = State()
    from_country_manual = State()

    from_city = State()
    from_city_manual = State()

    to_country = State()
    to_country_manual = State()

    to_city = State()
    to_city_manual = State()

    travel_date = State()
    travel_date_manual = State()

    weight = State()
    weight_manual = State()

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


# -------------------------
# POST / DEAL DATA HELPERS
# -------------------------

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
            SELECT from_country, to_country, COUNT(*) AS cnt
            FROM posts
            WHERE status='active'
              AND (expires_at IS NULL OR expires_at > ?)
            GROUP BY from_country, to_country
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


def search_route_posts_all(from_country: str, to_country: str, limit: int = 20) -> List[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.from_country=?
              AND p.to_country=?
              AND p.status='active'
              AND (p.expires_at IS NULL OR p.expires_at > ?)
            ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
            LIMIT ?
        """, (from_country, to_country, now_ts(), limit)).fetchall()


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
        return int(cur.lastrowid)


def add_route_subscription(user_id: int, post_type: str, from_country: str, to_country: str):
    with closing(connect_db()) as conn, conn:
        exists = conn.execute("""
            SELECT id FROM route_subscriptions
            WHERE user_id=? AND post_type=? AND from_country=? AND to_country=?
            LIMIT 1
        """, (user_id, post_type, from_country, to_country)).fetchone()
        if exists:
            return
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


def delete_subscription(user_id: int, sub_id: int) -> bool:
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("DELETE FROM route_subscriptions WHERE id=? AND user_id=?", (sub_id, user_id))
        return cur.rowcount > 0


def reserve_coincidence_notification(post_a_id: int, post_b_id: int) -> bool:
    a, b = sorted([post_a_id, post_b_id])
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("""
            INSERT OR IGNORE INTO coincidence_notifications (post_a_id, post_b_id, created_at)
            VALUES (?, ?, ?)
        """, (a, b, now_ts()))
        return cur.rowcount > 0


def calculate_coincidence_score(source_row, candidate_row: sqlite3.Row) -> Tuple[int, List[str]]:
    score = 40
    notes: List[str] = []

    source_from_city = normalize_text(source_row["from_city"])
    candidate_from_city = normalize_text(candidate_row["from_city"])
    source_to_city = normalize_text(source_row["to_city"])
    candidate_to_city = normalize_text(candidate_row["to_city"])

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

    source_weight = parse_weight_kg(source_row["weight_kg"])
    candidate_weight = parse_weight_kg(candidate_row["weight_kg"])

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
    source_row=None,
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

        coincidence_type = "strong" if score >= 75 else "good" if score >= 55 else "possible"
        results.append({
            "row": row,
            "score": score,
            "notes": notes,
            "type": coincidence_type
        })

    results.sort(key=lambda x: (x["score"], x["row"]["bumped_at"] or x["row"]["created_at"]), reverse=True)
    return results[:limit]


def search_posts_inline(query: str, limit: int = 10) -> List[sqlite3.Row]:
    q = f"%{query.strip().lower()}%"
    with closing(connect_db()) as conn:
        if query.strip():
            return conn.execute("""
                SELECT p.*, u.username, u.full_name
                FROM posts p
                LEFT JOIN users u ON u.user_id = p.user_id
                WHERE p.status='active'
                  AND (p.expires_at IS NULL OR p.expires_at > ?)
                  AND (
                        lower(p.from_country) LIKE ?
                     OR lower(COALESCE(p.from_city, '')) LIKE ?
                     OR lower(p.to_country) LIKE ?
                     OR lower(COALESCE(p.to_city, '')) LIKE ?
                     OR lower(COALESCE(p.description, '')) LIKE ?
                     OR lower(COALESCE(p.travel_date, '')) LIKE ?
                  )
                ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
                LIMIT ?
            """, (now_ts(), q, q, q, q, q, q, limit)).fetchall()

        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.status='active'
              AND (p.expires_at IS NULL OR p.expires_at > ?)
            ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
            LIMIT ?
        """, (now_ts(), limit)).fetchall()


def ensure_deal(post_id: int, owner_user_id: int, requester_user_id: int, initiator_user_id: int) -> int:
    with closing(connect_db()) as conn, conn:
        row = conn.execute("""
            SELECT id
            FROM deals
            WHERE post_id=? AND owner_user_id=? AND requester_user_id=?
            ORDER BY id DESC
            LIMIT 1
        """, (post_id, owner_user_id, requester_user_id)).fetchone()

        if row:
            return int(row["id"])

        cur = conn.execute("""
            INSERT INTO deals (
                post_id,
                owner_user_id,
                requester_user_id,
                initiator_user_id,
                status,
                owner_confirmed,
                requester_confirmed,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
        """, (
            post_id,
            owner_user_id,
            requester_user_id,
            initiator_user_id,
            DEAL_CONTACTED,
            now_ts(),
            now_ts(),
        ))
        return int(cur.lastrowid)


def get_deal(deal_id: int) -> Optional[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()


def list_user_deals(user_id: int) -> List[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT *
            FROM deals
            WHERE owner_user_id=? OR requester_user_id=?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 30
        """, (user_id, user_id)).fetchall()


def mark_deal_failed(post_id: int, user_id: int) -> bool:
    with closing(connect_db()) as conn, conn:
        row = conn.execute("""
            SELECT id
            FROM deals
            WHERE post_id=?
              AND (owner_user_id=? OR requester_user_id=?)
              AND status IN (?, ?, ?, ?, ?)
            ORDER BY id DESC
            LIMIT 1
        """, (
            post_id, user_id, user_id,
            DEAL_CONTACTED, DEAL_OFFERED, DEAL_ACCEPTED,
            DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER
        )).fetchone()

        if not row:
            return False

        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), row["id"]))
        return True


def create_bump_order(user_id: int, post_id: int, amount: int = BUMP_PRICE_AMOUNT, currency: str = BUMP_PRICE_CURRENCY) -> int:
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("""
            INSERT INTO bump_orders (user_id, post_id, amount, currency, status, created_at)
            VALUES (?, ?, ?, ?, 'pending', ?)
        """, (user_id, post_id, amount, currency, now_ts()))
        return int(cur.lastrowid)


# -------------------------
# CHANNEL / NOTIFY HELPERS
# -------------------------

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

    for item in coincidences:
        row = item["row"]
        score = item["score"]
        notes = item["notes"]

        if not reserve_coincidence_notification(new_row["id"], row["id"]):
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
                "🔔 По вашей подписке появилось новое объявление:\n\n" + post_text(row),
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

                if not reserve_coincidence_notification(row["id"], target["id"]):
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

    except Exception as e:
        print(f"GLOBAL COINCIDENCE SCAN ERROR: {e}")


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


# -------------------------
# FLOWS
# -------------------------

async def begin_create(message: Message, state: FSMContext, post_type: str):
    upsert_user(message)

    if is_user_banned(message.from_user.id):
        await message.answer(
            "⛔ Ваш аккаунт ограничен. Если это ошибка — свяжитесь с администратором.",
            reply_markup=main_menu(message.from_user.id)
        )
        return

    spam_error = anti_spam_check(message.from_user.id)
    if spam_error:
        await message.answer(spam_error, reply_markup=main_menu(message.from_user.id))
        return

    if active_post_count(message.from_user.id) >= MAX_ACTIVE_POSTS_PER_USER:
        await message.answer(
            f"У вас уже слишком много объявлений. Лимит: {MAX_ACTIVE_POSTS_PER_USER}. "
            "Удалите или деактивируйте старые объявления.",
            reply_markup=main_menu(message.from_user.id)
        )
        return

    await state.clear()
    await state.update_data(post_type=post_type)
    await state.set_state(CreatePost.from_country)

    await message.answer(
        form_text(post_type, 1, "Выберите страну отправления"),
        reply_markup=countries_select_kb("from_country_pick")
    )


def owner_only(callback: CallbackQuery, post_id: int) -> Optional[sqlite3.Row]:
    row = get_post(post_id)
    if not row or row["user_id"] != callback.from_user.id:
        return None
    return row


# -------------------------
# HANDLERS
# -------------------------

@router.inline_query()
async def inline_search_handler(inline_query: InlineQuery):
    query = (inline_query.query or "").strip()
    rows = search_posts_inline(query, limit=10)
    results = []

    for row in rows:
        title = f"{'✈️' if row['post_type'] == TYPE_TRIP else '📦'} {row['from_country']} → {row['to_country']}"
        if row["from_city"] or row["to_city"]:
            from_part = row["from_city"] or row["from_country"]
            to_part = row["to_city"] or row["to_country"]
            title = f"{'✈️' if row['post_type'] == TYPE_TRIP else '📦'} {from_part} → {to_part}"

        description_parts = []
        if row["travel_date"]:
            description_parts.append(f"Дата: {row['travel_date']}")
        if row["weight_kg"]:
            description_parts.append(f"Вес: {row['weight_kg']}")
        if row["description"]:
            description_parts.append(row["description"][:80])

        description = " | ".join(description_parts)[:200] or "Открыть объявление"
        text = f"{post_text(row)}\n\n🤖 Открыть в боте: {post_deeplink(row['id'])}"

        results.append(
            InlineQueryResultArticle(
                id=str(row["id"]),
                title=title[:256],
                description=description,
                input_message_content=InputTextMessageContent(
                    message_text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                ),
                reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"]),
            )
        )

    if not results:
        results = [
            InlineQueryResultArticle(
                id="no_results",
                title="Ничего не найдено",
                description="Попробуйте: Китай Россия, Шэньчжэнь Москва, посылка, попутчик",
                input_message_content=InputTextMessageContent(
                    message_text=(
                        "Ничего не найдено.\n\n"
                        f"Открой бота и создай объявление: {bot_link()}"
                    ),
                    disable_web_page_preview=True,
                ),
            )
        ]

    await inline_query.answer(results, cache_time=1, is_personal=True)


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    upsert_user(message)
    await state.clear()

    if is_user_banned(message.from_user.id):
        await message.answer(
            "⛔ Ваш аккаунт ограничен из-за жалоб пользователей.\n"
            "Если это ошибка — свяжитесь с администратором."
        )
        return

    start_arg = ""
    if message.text and " " in message.text:
        start_arg = message.text.split(" ", 1)[1].strip()

    if start_arg == "parcel":
        await begin_create(message, state, TYPE_PARCEL)
        return

    if start_arg == "trip":
        await begin_create(message, state, TYPE_TRIP)
        return

    if start_arg.startswith("contact_"):
        post_id_str = start_arg.replace("contact_", "", 1)
        if post_id_str.isdigit():
            row = get_post(int(post_id_str))

            if row and row["status"] == STATUS_ACTIVE:
                if row["user_id"] == message.from_user.id:
                    await message.answer("Это ваше объявление.", reply_markup=main_menu(message.from_user.id))
                    return

                deal_id = ensure_deal(
                    post_id=row["id"],
                    owner_user_id=row["user_id"],
                    requester_user_id=message.from_user.id,
                    initiator_user_id=message.from_user.id
                )

                await state.set_state(ContactFlow.message_text)
                await state.update_data(
                    post_id=row["id"],
                    target_user_id=row["user_id"],
                    deal_id=deal_id
                )

                await message.answer(
                    "✉️ Вы открыли связь с владельцем объявления:\n\n"
                    f"{post_text(row)}\n\n"
                    "Напишите сообщение, и я перешлю его владельцу."
                )
            else:
                await message.answer("Объявление не найдено или уже неактивно.")
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

    text = (
        "👋 <b>Привет.</b>\n\n"
        "Это <b>Попутчик Китай</b> — бот для передачи посылок через попутчиков.\n\n"
        "<b>Здесь можно отправить свою посылку или взять чужую по маршруту.</b>\n\n"
        "🔎 <b>Подпишись на канал с объявлениями:</b>\n"
        "t.me/china_poputchik\n\n"
        "Я сам ищу совпадения и уведомляю о них.\n\n"
        "<b>Нажми нужную кнопку в меню и заполни заявку.</b>\n\n"
        "⬇️ <b>Выберите действие в меню ниже</b>"
    )

    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.message(StateFilter("*"), Command("new_trip"))
@router.message(StateFilter("*"), F.text == "✈️ Взять посылку")
async def add_trip(message: Message, state: FSMContext):
    await state.clear()
    await begin_create(message, state, TYPE_TRIP)


@router.message(StateFilter("*"), Command("new_parcel"))
@router.message(StateFilter("*"), F.text == "📦 Отправить посылку")
async def add_parcel(message: Message, state: FSMContext):
    await state.clear()
    await begin_create(message, state, TYPE_PARCEL)


# -------------------------
# CREATE POST FLOW WITH BUTTONS
# -------------------------

@router.callback_query(F.data.startswith("from_country_pick:"))
async def pick_from_country(callback: CallbackQuery, state: FSMContext):
    upsert_user(callback)
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.from_country_manual)
        await callback.message.answer(
            form_text(post_type, 1, "Введите страну отправления вручную")
        )
        await callback.answer()
        return

    await state.update_data(from_country=value)
    await state.set_state(CreatePost.from_city)
    await callback.message.answer(
        form_text(post_type, 2, f"Выберите город отправления в стране {value}"),
        reply_markup=cities_select_kb("from_city_pick", value)
    )
    await callback.answer()


@router.message(CreatePost.from_country_manual)
async def from_country_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
    value = normalize_country_input(message.text.strip()[:80])

    await state.update_data(from_country=value)
    await state.set_state(CreatePost.from_city)
    await message.answer(
        form_text(post_type, 2, f"Выберите город отправления в стране {value}"),
        reply_markup=cities_select_kb("from_city_pick", value)
    )


@router.callback_query(F.data.startswith("from_city_pick:"))
async def pick_from_city(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.from_city_manual)
        await callback.message.answer(
            form_text(post_type, 2, "Введите город отправления вручную")
        )
        await callback.answer()
        return

    if value == "__skip__":
        await state.update_data(from_city=None)
    else:
        await state.update_data(from_city=value)

    await state.set_state(CreatePost.to_country)
    await callback.message.answer(
        form_text(post_type, 3, "Выберите страну назначения"),
        reply_markup=countries_select_kb("to_country_pick")
    )
    await callback.answer()


@router.message(CreatePost.from_city_manual)
async def from_city_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    value = message.text.strip()
    await state.update_data(from_city=None if value == SKIP_VALUE else normalize_city_input(value[:80]))
    await state.set_state(CreatePost.to_country)
    await message.answer(
        form_text(post_type, 3, "Выберите страну назначения"),
        reply_markup=countries_select_kb("to_country_pick")
    )


@router.callback_query(F.data.startswith("to_country_pick:"))
async def pick_to_country(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.to_country_manual)
        await callback.message.answer(
            form_text(post_type, 3, "Введите страну назначения вручную")
        )
        await callback.answer()
        return

    await state.update_data(to_country=value)
    await state.set_state(CreatePost.to_city)
    await callback.message.answer(
        form_text(post_type, 4, f"Выберите город назначения в стране {value}"),
        reply_markup=cities_select_kb("to_city_pick", value)
    )
    await callback.answer()


@router.message(CreatePost.to_country_manual)
async def to_country_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
    value = normalize_country_input(message.text.strip()[:80])

    await state.update_data(to_country=value)
    await state.set_state(CreatePost.to_city)
    await message.answer(
        form_text(post_type, 4, f"Выберите город назначения в стране {value}"),
        reply_markup=cities_select_kb("to_city_pick", value)
    )


@router.callback_query(F.data.startswith("to_city_pick:"))
async def pick_to_city(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.to_city_manual)
        await callback.message.answer(
            form_text(post_type, 4, "Введите город назначения вручную")
        )
        await callback.answer()
        return

    if value == "__skip__":
        await state.update_data(to_city=None)
    else:
        await state.update_data(to_city=value)

    await state.set_state(CreatePost.travel_date)
    await callback.message.answer(
        form_text(post_type, 5, "Выберите дату поездки / отправки"),
        reply_markup=date_select_kb()
    )
    await callback.answer()


@router.message(CreatePost.to_city_manual)
async def to_city_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    value = message.text.strip()
    await state.update_data(to_city=None if value == SKIP_VALUE else normalize_city_input(value[:80]))
    await state.set_state(CreatePost.travel_date)
    await message.answer(
        form_text(post_type, 5, "Выберите дату поездки / отправки"),
        reply_markup=date_select_kb()
    )


@router.callback_query(F.data.startswith("datepick:"))
async def pick_date(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.travel_date_manual)
        await callback.message.answer(
            form_text(post_type, 5, "Введите точную дату\nНапример: 15.03.2026")
        )
        await callback.answer()
        return

    await state.update_data(travel_date=value)
    await state.set_state(CreatePost.weight)
    await callback.message.answer(
        form_text(post_type, 6, "Выберите вес или объём"),
        reply_markup=weight_select_kb()
    )
    await callback.answer()


@router.message(CreatePost.travel_date_manual)
async def date_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    await state.update_data(travel_date=message.text.strip()[:100])
    await state.set_state(CreatePost.weight)
    await message.answer(
        form_text(post_type, 6, "Выберите вес или объём"),
        reply_markup=weight_select_kb()
    )


@router.callback_query(F.data.startswith("weightpick:"))
async def pick_weight(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.weight_manual)
        await callback.message.answer(
            form_text(post_type, 6, "Введите свой вес / объём\nНапример: 7 кг")
        )
        await callback.answer()
        return

    if value == "__skip__":
        await state.update_data(weight_kg=None)
    else:
        await state.update_data(weight_kg=value)

    await state.set_state(CreatePost.description)
    await callback.message.answer(
        form_text(post_type, 7, "Опишите объявление подробно\nЧто нужно передать / сколько места есть / условия")
    )
    await callback.answer()


@router.message(CreatePost.weight_manual)
async def weight_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    value = message.text.strip()
    await state.update_data(weight_kg=None if value == SKIP_VALUE else value[:50])
    await state.set_state(CreatePost.description)
    await message.answer(
        form_text(post_type, 7, "Опишите объявление подробно\nЧто нужно передать / сколько места есть / условия")
    )


@router.message(CreatePost.description)
async def enter_description(message: Message, state: FSMContext):
    desc = (message.text or "").strip()
    if len(desc) < 3:
        await message.answer("Описание слишком короткое. Напишите подробнее.")
        return

    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    await state.update_data(description=desc[:1000])
    await state.set_state(CreatePost.contact_note)

    await message.answer(
        form_text(post_type, 8, "Введите дополнительный контакт или примечание\nНапример: WeChat ID / только текст / без звонков\nЕсли не нужно — напишите -")
    )


@router.message(CreatePost.contact_note)
async def finalize_post(message: Message, state: FSMContext, bot: Bot):
    try:
        upsert_user(message)

        if is_user_banned(message.from_user.id):
            await message.answer("⛔ Ваш аккаунт ограничен.", reply_markup=main_menu(message.from_user.id))
            await state.clear()
            return

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
            await safe_publish(bot, post_id)
            await notify_coincidence_users(bot, post_id)
            await notify_subscribers(bot, post_id)

    except Exception as e:
        print(f"FINALIZE_POST ERROR: {e}")
        await message.answer(
            f"Произошла ошибка при сохранении объявления: {html.escape(str(e))}",
            reply_markup=main_menu(message.from_user.id)
        )


@router.message(Command("my"))
@router.message(F.text == "📋 Мои объявления")
async def my_posts_handler(message: Message):
    upsert_user(message)
    with closing(connect_db()) as conn:
        posts = conn.execute("""
            SELECT * FROM posts
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT 30
        """, (message.from_user.id,)).fetchall()

    if not posts:
        await message.answer("У тебя пока нет объявлений.", reply_markup=main_menu(message.from_user.id))
        return

    await message.answer("Твои объявления:", reply_markup=my_posts_kb(posts))


@router.callback_query(F.data.startswith("mypost:"))
async def open_my_post(callback: CallbackQuery):
    await callback.answer()
    try:
        post_id = int(callback.data.split(":")[1])
        row = get_post(post_id)

        if not row or row["user_id"] != callback.from_user.id:
            await callback.message.answer("Объявление не найдено.")
            return

        text = post_text(row)
        if len(text) > 4000:
            text = text[:3900] + "\n\n..."

        await callback.message.answer(
            text,
            reply_markup=post_actions_kb(post_id, row["status"])
        )

    except Exception as e:
        print(f"OPEN_MY_POST ERROR: {e}")
        await callback.message.answer("Не удалось открыть объявление.")


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


@router.callback_query(F.data.startswith("user_reviews:"))
async def show_user_reviews(callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    reviews = get_user_reviews(user_id, limit=10)

    if not reviews:
        await callback.message.answer("У пользователя пока нет отзывов.")
        await callback.answer()
        return

    avg_rating, cnt = user_rating_summary(user_id)

    lines = [
        "⭐ <b>Отзывы о пользователе</b>",
        f"Средний рейтинг: <b>{avg_rating:.1f}</b>",
        f"Всего отзывов: <b>{cnt}</b>",
        ""
    ]

    for r in reviews:
        stars = "⭐" * int(r["rating"])
        author = f"@{r['username']}" if r["username"] else (r["full_name"] or "Пользователь")
        text = r["text"] or "Без текста"

        lines.append(stars)
        lines.append(html.escape(text))
        lines.append(f"— {html.escape(author)}")
        lines.append("")

    result = "\n".join(lines)
    await callback.message.answer(result[:4000])
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

    if is_user_banned(callback.from_user.id):
        await callback.answer("Ваш аккаунт ограничен", show_alert=True)
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

    order_id = create_bump_order(callback.from_user.id, post_id)

    await callback.message.answer(
        f"💰 Поднятие объявления стоит {BUMP_PRICE_AMOUNT} {BUMP_PRICE_CURRENCY}.\n\n"
        "Оплатите через WeChat / Alipay и отправьте скрин администратору.\n"
        "После подтверждения оплаты объявление будет поднято выше.\n\n"
        f"ID заказа: <b>{order_id}</b>"
    )
    await callback.answer("Заявка создана")


@router.message(F.text == "💰 Поднять объявление")
async def bump_info(message: Message):
    await message.answer(
        f"{BUMP_PRICE_TEXT}\n\n"
        f"Стоимость: {BUMP_PRICE_AMOUNT} {BUMP_PRICE_CURRENCY}\n"
        "Открой 'Мои объявления' и нажми 'Поднять' у нужного объявления.",
        reply_markup=main_menu(message.from_user.id)
    )


@router.message(Command("admin_bump_paid"))
async def admin_bump_paid(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_bump_paid ORDER_ID")
        return

    order_id = int(parts[1])

    with closing(connect_db()) as conn, conn:
        order = conn.execute("SELECT * FROM bump_orders WHERE id=?", (order_id,)).fetchone()
        if not order:
            await message.answer("Заказ не найден.")
            return

        if order["status"] == "paid":
            await message.answer("Этот заказ уже подтвержден.")
            return

        conn.execute("""
            UPDATE bump_orders
            SET status='paid', paid_at=?
            WHERE id=?
        """, (now_ts(), order_id))

        conn.execute("""
            UPDATE posts
            SET bumped_at=?, updated_at=?
            WHERE id=?
        """, (now_ts(), now_ts(), order["post_id"]))

    try:
        await message.bot.send_message(
            order["user_id"],
            f"✅ Оплата по заказу {order_id} подтверждена.\n"
            "Ваше объявление поднято выше в поиске."
        )
    except Exception:
        pass

    await message.answer("Объявление поднято.")


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
    await callback.answer()
    try:
        _, from_country, to_country = callback.data.split(":", 2)
        rows = search_route_posts_all(from_country, to_country, limit=20)

        if not rows:
            await callback.message.answer("По этому маршруту сейчас нет активных объявлений.")
            return

        trips = sum(1 for r in rows if r["post_type"] == TYPE_TRIP)
        parcels = sum(1 for r in rows if r["post_type"] == TYPE_PARCEL)

        await callback.message.answer(
            f"Маршрут: <b>{html.escape(from_country)} → {html.escape(to_country)}</b>\n"
            f"Найдено: <b>{len(rows)}</b>\n"
            f"✈️ Попутчиков: <b>{trips}</b>\n"
            f"📦 Посылок: <b>{parcels}</b>"
        )

        for row in rows:
            try:
                text = post_text(row)
                if len(text) > 4000:
                    text = text[:3900] + "\n\n..."
                await callback.message.answer(
                    text,
                    reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
                )
            except Exception as inner_e:
                print(f"POPULAR_ROUTE_SEND_ROW ERROR: {inner_e}")

    except Exception as e:
        print(f"POPULAR_ROUTE_OPEN ERROR: {e}")
        await callback.message.answer("Не удалось открыть маршрут.")


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
        rows = []
        for s in subs:
            label = f"{s['id']} • {('✈️' if s['post_type'] == 'trip' else '📦')} • {s['from_country']}→{s['to_country']}"
            rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"subdel:{s['id']}")])
        await callback.message.answer(
            "Ваши подписки. Нажмите на нужную, чтобы удалить:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
        )
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

    deal_id = ensure_deal(
        post_id=int(post_id),
        owner_user_id=int(owner_id),
        requester_user_id=callback.from_user.id,
        initiator_user_id=callback.from_user.id
    )

    await state.set_state(ContactFlow.message_text)
    await state.update_data(post_id=int(post_id), target_user_id=int(owner_id), deal_id=deal_id)
    await callback.message.answer("Напиши сообщение владельцу объявления. Я перешлю его через бота.")
    await callback.answer()


@router.message(ContactFlow.message_text)
async def relay_message(message: Message, state: FSMContext):
    data = await state.get_data()
    target_user_id = data["target_user_id"]
    post_id = data["post_id"]
    deal_id = data.get("deal_id")
    text = (message.text or "").strip()

    if not text:
        await message.answer("Сообщение не должно быть пустым.")
        return

    try:
        from_name = html.escape(message.from_user.full_name or "Пользователь")
        username_part = f" (@{html.escape(message.from_user.username)})" if message.from_user.username else ""
        safe_text = html.escape(text)

        await message.bot.send_message(
            target_user_id,
            f"Новое сообщение по объявлению ID {post_id}:\n\n"
            f"От: {from_name}{username_part}\n\n{safe_text}\n\n"
            "Чтобы ответить, напиши пользователю напрямую или дождись предложения сделки."
        )

        with closing(connect_db()) as conn, conn:
            conn.execute(
                "INSERT INTO dialogs (post_id, owner_user_id, requester_user_id, created_at) VALUES (?, ?, ?, ?)",
                (post_id, target_user_id, message.from_user.id, now_ts())
            )

            if deal_id:
                conn.execute("""
                    UPDATE deals
                    SET status=?, updated_at=?
                    WHERE id=? AND status NOT IN (?, ?, ?)
                """, (DEAL_CONTACTED, now_ts(), deal_id, DEAL_COMPLETED, DEAL_FAILED, DEAL_CANCELLED))

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤝 Предложить сделку", callback_data=f"offer_deal:{post_id}:{target_user_id}")],
            [InlineKeyboardButton(text="❌ Не договорились", callback_data=f"deal_fail_post:{post_id}")]
        ])

        await message.answer(
            "Сообщение отправлено владельцу объявления.\n\n"
            "Следующий шаг — предложить сделку:",
            reply_markup=kb
        )

    except Exception:
        await message.answer("Не удалось отправить сообщение. Возможно, владелец еще не запускал бота.")

    await state.clear()


@router.callback_query(F.data.startswith("offer_deal:"))
async def offer_deal_handler(callback: CallbackQuery):
    _, post_id_str, owner_id_str = callback.data.split(":")
    post_id = int(post_id_str)
    owner_id = int(owner_id_str)
    requester_id = callback.from_user.id

    if owner_id == requester_id:
        await callback.answer("Это ваше объявление", show_alert=True)
        return

    row = get_post(post_id)
    if not row or row["status"] != STATUS_ACTIVE:
        await callback.answer("Объявление не найдено или неактивно", show_alert=True)
        return

    deal_id = ensure_deal(
        post_id=post_id,
        owner_user_id=owner_id,
        requester_user_id=requester_id,
        initiator_user_id=requester_id
    )

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_OFFERED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            owner_id,
            f"🤝 Вам предложили сделку по объявлению ID {post_id}.\n\n"
            f"Пользователь: {html.escape(callback.from_user.full_name or 'Пользователь')}"
            + (f" (@{html.escape(callback.from_user.username)})" if callback.from_user.username else ""),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Принять сделку", callback_data=f"deal_accept:{deal_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"deal_reject:{deal_id}")
                ]
            ])
        )
    except Exception:
        pass

    await callback.message.answer("Предложение сделки отправлено владельцу.")
    await callback.answer("Готово")


@router.callback_query(F.data.startswith("deal_accept:"))
async def deal_accept_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or deal["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_ACCEPTED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            deal["requester_user_id"],
            f"✅ Ваша сделка по объявлению ID {deal['post_id']} принята.\n\n"
            "Управление сделками происходит во вкладке МЕНЮ '🤝 Мои сделки'.\n"
            "Там вы сможете закрыть сделку и оставить отзыв.\n\n"
            "Для перехода — откройте МЕНЮ бота."
        )
    except Exception:
        pass

    await callback.message.answer("Сделка принята.")
    await callback.answer()


@router.callback_query(F.data.startswith("deal_reject:"))
async def deal_reject_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or deal["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            deal["requester_user_id"],
            f"❌ Ваша сделка по объявлению ID {deal['post_id']} отклонена."
        )
    except Exception:
        pass

    await callback.message.answer("Сделка отклонена.")
    await callback.answer()


@router.message(F.text == "🤝 Мои сделки")
async def my_deals_menu(message: Message):
    upsert_user(message)
    deals = list_user_deals(message.from_user.id)
    if not deals:
        await message.answer("У вас пока нет сделок.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer("Ваши сделки:", reply_markup=deal_list_kb(deals))


@router.callback_query(F.data.startswith("mydeal:"))
async def open_my_deal(callback: CallbackQuery):
    await callback.answer()
    try:
        deal_id = int(callback.data.split(":")[1])
        deal = get_deal(deal_id)

        if not deal or (deal["owner_user_id"] != callback.from_user.id and deal["requester_user_id"] != callback.from_user.id):
            await callback.message.answer("Сделка не найдена.")
            return

        role = "владелец объявления" if callback.from_user.id == deal["owner_user_id"] else "откликнувшийся"
        other_user_id = deal["requester_user_id"] if callback.from_user.id == deal["owner_user_id"] else deal["owner_user_id"]

        text = (
            f"🤝 <b>Сделка #{deal['id']}</b>\n\n"
            f"Объявление ID: <b>{deal['post_id']}</b>\n"
            f"Статус: <b>{format_deal_status(deal['status'])}</b>\n"
            f"Ваша роль: <b>{role}</b>\n"
            f"Вторая сторона: <b>{format_user_ref(other_user_id)}</b>\n"
            f"Подтверждение владельца: <b>{'Да' if deal['owner_confirmed'] else 'Нет'}</b>\n"
            f"Подтверждение откликнувшегося: <b>{'Да' if deal['requester_confirmed'] else 'Нет'}</b>"
        )

        await callback.message.answer(text, reply_markup=deal_open_kb(deal, callback.from_user.id))

    except Exception as e:
        print(f"OPEN_MY_DEAL ERROR: {e}")
        await callback.message.answer("Не удалось открыть сделку.")


@router.callback_query(F.data.startswith("deal_confirm:"))
async def deal_confirm_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if deal["status"] not in (DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
        await callback.answer("Эту сделку нельзя подтвердить", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        if callback.from_user.id == deal["owner_user_id"]:
            conn.execute("""
                UPDATE deals
                SET owner_confirmed=1, updated_at=?
                WHERE id=?
            """, (now_ts(), deal_id))
        elif callback.from_user.id == deal["requester_user_id"]:
            conn.execute("""
                UPDATE deals
                SET requester_confirmed=1, updated_at=?
                WHERE id=?
            """, (now_ts(), deal_id))
        else:
            await callback.answer("Нет доступа", show_alert=True)
            return

        refreshed = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()

        if refreshed["owner_confirmed"] and refreshed["requester_confirmed"]:
            conn.execute("""
                UPDATE deals
                SET status=?, completed_at=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED, now_ts(), now_ts(), deal_id))
            final_status = DEAL_COMPLETED
        elif refreshed["owner_confirmed"]:
            conn.execute("""
                UPDATE deals
                SET status=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED_BY_OWNER, now_ts(), deal_id))
            final_status = DEAL_COMPLETED_BY_OWNER
        else:
            conn.execute("""
                UPDATE deals
                SET status=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED_BY_REQUESTER, now_ts(), deal_id))
            final_status = DEAL_COMPLETED_BY_REQUESTER

    other_user_id = (
        deal["requester_user_id"]
        if callback.from_user.id == deal["owner_user_id"]
        else deal["owner_user_id"]
    )

    if final_status == DEAL_COMPLETED:
        for uid in [deal["owner_user_id"], deal["requester_user_id"]]:
            try:
                await callback.bot.send_message(
                    uid,
                    f"✅ Сделка #{deal_id} завершена. Теперь вы можете оставить отзыв.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="⭐ Оставить отзыв", callback_data=f"deal_review:{deal_id}")]
                    ])
                )
            except Exception as e:
                print(f"DEAL COMPLETE SEND ERROR: {e}")

        await callback.message.answer("Сделка полностью завершена.")

    else:
        try:
            await callback.bot.send_message(
                other_user_id,
                f"📌 Вторая сторона подтвердила завершение сделки по объявлению ID {deal['post_id']}.\n\n"
                "Чтобы окончательно закрыть сделку, откройте МЕНЮ → '🤝 Мои сделки' "
                "и подтвердите завершение со своей стороны."
            )
        except Exception as e:
            print(f"DEAL CONFIRM NOTIFY ERROR: {e}")

        await callback.message.answer("Ваше подтверждение сохранено. Ждем подтверждение второй стороны.")

    await callback.answer()


@router.callback_query(F.data.startswith("deal_review:"))
async def deal_review_handler(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if deal["status"] != DEAL_COMPLETED:
        await callback.answer("Отзыв можно оставить только после завершенной сделки", show_alert=True)
        return

    if callback.from_user.id == deal["owner_user_id"]:
        reviewed_user_id = deal["requester_user_id"]
    elif callback.from_user.id == deal["requester_user_id"]:
        reviewed_user_id = deal["owner_user_id"]
    else:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.clear()
    await state.update_data(reviewed_user_id=reviewed_user_id, post_id=deal["post_id"], deal_id=deal_id)
    await state.set_state(ReviewFlow.rating)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1", callback_data="review_rating:1"),
            InlineKeyboardButton(text="2", callback_data="review_rating:2"),
            InlineKeyboardButton(text="3", callback_data="review_rating:3")
        ],
        [
            InlineKeyboardButton(text="4", callback_data="review_rating:4"),
            InlineKeyboardButton(text="5", callback_data="review_rating:5")
        ]
    ])

    await callback.message.answer("Поставьте оценку пользователю от 1 до 5:", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("review_rating:"))
async def review_rating_callback(callback: CallbackQuery, state: FSMContext):
    rating = int(callback.data.split(":")[1])
    await state.update_data(rating=rating)
    await state.set_state(ReviewFlow.text)
    await callback.message.answer("Напишите короткий отзыв или отправьте '-' если без текста.")
    await callback.answer()


@router.message(ReviewFlow.text)
async def review_finish(message: Message, state: FSMContext):
    data = await state.get_data()

    review_text = None if message.text.strip() == "-" else message.text.strip()[:500]
    reviewed_user_id = data["reviewed_user_id"]
    post_id = data["post_id"]
    rating = data["rating"]

    with closing(connect_db()) as conn, conn:
        try:
            conn.execute("""
                INSERT INTO reviews (reviewer_user_id, reviewed_user_id, post_id, rating, text, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                message.from_user.id,
                reviewed_user_id,
                post_id,
                rating,
                review_text,
                now_ts()
            ))
        except sqlite3.IntegrityError:
            await message.answer("Такой отзыв уже оставлен.", reply_markup=main_menu(message.from_user.id))
            await state.clear()
            return

    await message.answer("⭐ Спасибо! Отзыв сохранен.", reply_markup=main_menu(message.from_user.id))
    await state.clear()


@router.callback_query(F.data.startswith("deal_fail_post:"))
async def deal_fail_post_handler(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    ok = mark_deal_failed(post_id, callback.from_user.id)
    if ok:
        await callback.message.answer("❌ Сделка отмечена как несостоявшаяся.")
    else:
        await callback.message.answer("Не удалось найти активную сделку для этого объявления.")
    await callback.answer()


@router.callback_query(F.data.startswith("deal_fail_direct:"))
async def deal_fail_direct_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or (deal["owner_user_id"] != callback.from_user.id and deal["requester_user_id"] != callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), deal_id))

    await callback.message.answer("❌ Сделка отмечена как несостоявшаяся.")
    await callback.answer()


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

    row = get_post(post_id)
    if not row:
        await message.answer("Объявление не найдено.", reply_markup=main_menu(message.from_user.id))
        await state.clear()
        return

    owner_user_id = row["user_id"]

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "INSERT INTO complaints (post_id, from_user_id, reason, created_at) VALUES (?, ?, ?, ?)",
            (post_id, message.from_user.id, message.text.strip()[:1000], now_ts())
        )

        complaints_count = conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM complaints c
            JOIN posts p ON p.id = c.post_id
            WHERE p.user_id = ?
        """, (owner_user_id,)).fetchone()["cnt"]

    if complaints_count >= 3:
        ban_user(owner_user_id)

    await message.answer("Жалоба отправлена.", reply_markup=main_menu(message.from_user.id))

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"Новая жалоба на объявление {post_id}:\n\n"
                f"Причина: {html.escape(message.text.strip())}\n\n"
                f"Всего жалоб на пользователя: {complaints_count}\n\n"
                + post_text(row)
            )
        except Exception:
            pass

    if complaints_count >= 3:
        try:
            await bot.send_message(
                owner_user_id,
                "⛔ Ваш аккаунт автоматически заблокирован, потому что на ваши объявления поступило 3 жалобы.\n"
                "Если это ошибка — свяжитесь с администратором."
            )
        except Exception:
            pass

    await state.clear()


@router.message(Command("admin"))
@router.message(F.text == "👨‍💼 Админка")
async def admin_menu_handler(message: Message):
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
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
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

    ban_user(row["user_id"])

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
            f"Причина: {html.escape(row['reason'])}"
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
    ban_user(user_id)
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


@router.message(F.text == "ℹ️ Помощь")
async def help_handler(message: Message):
    text = (
        "<b>Помощь</b>\n\n"
        "✈️ <b>Взять посылку</b> — если вы летите и можете что-то передать.\n"
        "📦 <b>Отправить посылку</b> — если вам нужно что-то передать.\n"
        "🔎 <b>Найти совпадения</b> — быстрый поиск подходящих объявлений.\n"
        "📋 <b>Мои объявления</b> — управление своими объявлениями.\n"
        "🤝 <b>Мои сделки</b> — ваши активные и завершенные сделки.\n"
        "🔔 <b>Подписки</b> — уведомления по нужным маршрутам.\n"
        "🆘 <b>Жалоба</b> — пожаловаться на объявление."
    )
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


# -------------------------
# RUN
# -------------------------

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
    asyncio.run(main()).   вот мой код сейчас - верни мне новый полный код бота с учетом 4х правок. сделай код максимально легким и структуированным, как будто его писал опытный программист с более 10 лет опыта . Сейчас есть несколько правок

1. При выборе стран вместо ОАЭ добавь США
2. При выборе городов  - в странах кроме России сейчас есть некоторые где выбор только одного города - в каждой стране минимум 4 города самых больших должно быть (в китае можно/ россии/ украине - больше)
3. При заполнение заявки если человек ввел неправильно что-то и его сразу перекинуло на следующий шаг - у него должна быть кнопка вернуться на шаг назад - иначе ему придется заполнять все заново
4. В поле даты должны быть 3 кнопки - в течение недели, в течение месяца, указать точную дату 
Если человек выбирает в течение недели, в течение месяца - в заявке должен отображаться диапазон даты цифрами с момента дня заполнения заявки  + нужно добавить что обьявление должно автоматически деактивироваться и удаляться из канала не только после 14 дней с момента поста - но и с момента когда дата поездки указанная в заявке просрочена (что наступает первее)
Сейчас есть несколько правок

1. При выборе стран вместо ОАЭ добавь США
2. При выборе городов  - в странах кроме России сейчас есть некоторые где выбор только одного города - в каждой стране минимум 4 города самых больших должно быть
3. При заполнение заявки если человек ввел неправильно что-то и его сразу перекинуло на следующий шаг - у него должна быть кнопка вернуться на шаг назад - иначе ему придется заполнять все заново
4. В поле даты должны быть 3 кнопки - в течение недели, в течение месяца, указать точную дату 
Если человек выбирает в течение недели, в течение месяца - в заявке должен отображаться диапазон даты цифрами с момента дня заполнения заявки  + нужно добавить что обьявление должно автоматически деактивироваться и удаляться из канала не только после 14 дней с момента поста - но и с момента когда дата поездки указанная в заявке просрочена (что наступает первее)


Thought for 47s

ты не дописал код - продолжи с @router.callback_query(F.data.startswith("deactivate:"))
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
            (STATUS_INACTIVE,_

Вот продолжение кода с @router.callback_query(F.data.startswith("deactivate:")) до конца файла.

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

    if is_user_banned(callback.from_user.id):
        await callback.answer("Ваш аккаунт ограничен", show_alert=True)
        return

    new_status = STATUS_PENDING if ADMIN_IDS else STATUS_ACTIVE
    expires_at = calculate_post_expires_at(
        now_ts(),
        row["travel_date"],
        post_ttl_days=POST_TTL_DAYS
    )

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET status=?, updated_at=?, expires_at=? WHERE id=?",
            (new_status, now_ts(), expires_at, post_id)
        )

    await callback.message.answer(
        f"Объявление {post_id} "
        + ("отправлено на повторную модерацию." if ADMIN_IDS else "активировано.")
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

    order_id = create_bump_order(callback.from_user.id, post_id)

    await callback.message.answer(
        f"💰 Поднятие объявления стоит {BUMP_PRICE_AMOUNT} {BUMP_PRICE_CURRENCY}.\n\n"
        "Оплатите через WeChat / Alipay и отправьте скрин администратору.\n"
        "После подтверждения оплаты объявление будет поднято выше.\n\n"
        f"ID заказа: <b>{order_id}</b>"
    )
    await callback.answer("Заявка создана")


@router.message(F.text == "💰 Поднять объявление")
async def bump_info(message: Message):
    await message.answer(
        f"{BUMP_PRICE_TEXT}\n\n"
        f"Стоимость: {BUMP_PRICE_AMOUNT} {BUMP_PRICE_CURRENCY}\n"
        "Открой 'Мои объявления' и нажми 'Поднять' у нужного объявления.",
        reply_markup=main_menu(message.from_user.id)
    )


@router.message(Command("admin_bump_paid"))
async def admin_bump_paid(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_bump_paid ORDER_ID")
        return

    order_id = int(parts[1])

    with closing(connect_db()) as conn, conn:
        order = conn.execute("SELECT * FROM bump_orders WHERE id=?", (order_id,)).fetchone()
        if not order:
            await message.answer("Заказ не найден.")
            return

        if order["status"] == "paid":
            await message.answer("Этот заказ уже подтвержден.")
            return

        conn.execute("""
            UPDATE bump_orders
            SET status='paid', paid_at=?
            WHERE id=?
        """, (now_ts(), order_id))

        conn.execute("""
            UPDATE posts
            SET bumped_at=?, updated_at=?
            WHERE id=?
        """, (now_ts(), now_ts(), order["post_id"]))

    try:
        await message.bot.send_message(
            order["user_id"],
            f"✅ Оплата по заказу {order_id} подтверждена.\n"
            "Ваше объявление поднято выше в поиске."
        )
    except Exception:
        pass

    await message.answer("Объявление поднято.")


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
    looking_for = callback.data.split(":", 1)[1]
    await state.update_data(looking_for=looking_for)
    await state.set_state(FindFlow.from_country)
    await callback.message.answer(
        "Выбери страну отправления:",
        reply_markup=countries_kb("findfrom")
    )
    await callback.answer()


@router.callback_query(F.data.startswith("findfrom:"))
async def find_from(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    await state.update_data(from_country=country)
    await state.set_state(FindFlow.to_country)
    await callback.message.answer(
        "Выбери страну назначения:",
        reply_markup=countries_kb("findto")
    )
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
    await callback.answer()
    try:
        _, from_country, to_country = callback.data.split(":", 2)
        rows = search_route_posts_all(from_country, to_country, limit=20)

        if not rows:
            await callback.message.answer("По этому маршруту сейчас нет активных объявлений.")
            return

        trips = sum(1 for r in rows if r["post_type"] == TYPE_TRIP)
        parcels = sum(1 for r in rows if r["post_type"] == TYPE_PARCEL)

        await callback.message.answer(
            f"Маршрут: <b>{html.escape(from_country)} → {html.escape(to_country)}</b>\n"
            f"Найдено: <b>{len(rows)}</b>\n"
            f"✈️ Попутчиков: <b>{trips}</b>\n"
            f"📦 Посылок: <b>{parcels}</b>"
        )

        for row in rows:
            try:
                text = post_text(row)
                if len(text) > 4000:
                    text = text[:3900] + "\n\n..."
                await callback.message.answer(
                    text,
                    reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
                )
            except Exception as inner_e:
                print(f"POPULAR_ROUTE_SEND_ROW ERROR: {inner_e}")

    except Exception as e:
        print(f"POPULAR_ROUTE_OPEN ERROR: {e}")
        await callback.message.answer("Не удалось открыть маршрут.")


@router.message(F.text == "🆕 Новые объявления")
async def recent_posts_handler(message: Message):
    rows = get_recent_posts(10)
    if not rows:
        await message.answer(
            "Пока нет новых активных объявлений.",
            reply_markup=main_menu(message.from_user.id)
        )
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
        text += (
            f"\nПопулярный маршрут:\n"
            f"<b>{route['from_country']} → {route['to_country']}</b> ({route['cnt']})"
        )
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
    post_type = callback.data.split(":", 1)[1]
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
    add_route_subscription(
        callback.from_user.id,
        data["post_type"],
        data["from_country"],
        country
    )
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
        rows = []
        for s in subs:
            label = (
                f"{s['id']} • "
                f"{'✈️' if s['post_type'] == TYPE_TRIP else '📦'} • "
                f"{s['from_country']}→{s['to_country']}"
            )
            rows.append([
                InlineKeyboardButton(text=label[:64], callback_data=f"subdel:{s['id']}")
            ])
        await callback.message.answer(
            "Ваши подписки. Нажмите на нужную, чтобы удалить:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
        )
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

    deal_id = ensure_deal(
        post_id=int(post_id),
        owner_user_id=int(owner_id),
        requester_user_id=callback.from_user.id,
        initiator_user_id=callback.from_user.id
    )

    await state.set_state(ContactFlow.message_text)
    await state.update_data(
        post_id=int(post_id),
        target_user_id=int(owner_id),
        deal_id=deal_id
    )
    await callback.message.answer(
        "Напиши сообщение владельцу объявления. Я перешлю его через бота."
    )
    await callback.answer()


@router.message(ContactFlow.message_text)
async def relay_message(message: Message, state: FSMContext):
    data = await state.get_data()
    target_user_id = data["target_user_id"]
    post_id = data["post_id"]
    deal_id = data.get("deal_id")
    text = (message.text or "").strip()

    if not text:
        await message.answer("Сообщение не должно быть пустым.")
        return

    try:
        from_name = html.escape(message.from_user.full_name or "Пользователь")
        username_part = (
            f" (@{html.escape(message.from_user.username)})"
            if message.from_user.username else ""
        )
        safe_text = html.escape(text)

        await message.bot.send_message(
            target_user_id,
            f"Новое сообщение по объявлению ID {post_id}:\n\n"
            f"От: {from_name}{username_part}\n\n"
            f"{safe_text}\n\n"
            "Чтобы ответить, напиши пользователю напрямую или дождись предложения сделки."
        )

        with closing(connect_db()) as conn, conn:
            conn.execute(
                """
                INSERT INTO dialogs (post_id, owner_user_id, requester_user_id, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (post_id, target_user_id, message.from_user.id, now_ts())
            )

            if deal_id:
                conn.execute("""
                    UPDATE deals
                    SET status=?, updated_at=?
                    WHERE id=? AND status NOT IN (?, ?, ?)
                """, (
                    DEAL_CONTACTED,
                    now_ts(),
                    deal_id,
                    DEAL_COMPLETED,
                    DEAL_FAILED,
                    DEAL_CANCELLED
                ))

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤝 Предложить сделку", callback_data=f"offer_deal:{post_id}:{target_user_id}")],
            [InlineKeyboardButton(text="❌ Не договорились", callback_data=f"deal_fail_post:{post_id}")]
        ])

        await message.answer(
            "Сообщение отправлено владельцу объявления.\n\n"
            "Следующий шаг — предложить сделку:",
            reply_markup=kb
        )

    except Exception:
        await message.answer(
            "Не удалось отправить сообщение. Возможно, владелец еще не запускал бота."
        )

    await state.clear()


@router.callback_query(F.data.startswith("offer_deal:"))
async def offer_deal_handler(callback: CallbackQuery):
    _, post_id_str, owner_id_str = callback.data.split(":")
    post_id = int(post_id_str)
    owner_id = int(owner_id_str)
    requester_id = callback.from_user.id

    if owner_id == requester_id:
        await callback.answer("Это ваше объявление", show_alert=True)
        return

    row = get_post(post_id)
    if not row or row["status"] != STATUS_ACTIVE:
        await callback.answer("Объявление не найдено или неактивно", show_alert=True)
        return

    deal_id = ensure_deal(
        post_id=post_id,
        owner_user_id=owner_id,
        requester_user_id=requester_id,
        initiator_user_id=requester_id
    )

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_OFFERED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            owner_id,
            f"🤝 Вам предложили сделку по объявлению ID {post_id}.\n\n"
            f"Пользователь: {html.escape(callback.from_user.full_name or 'Пользователь')}"
            + (f" (@{html.escape(callback.from_user.username)})" if callback.from_user.username else ""),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Принять сделку", callback_data=f"deal_accept:{deal_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"deal_reject:{deal_id}")
                ]
            ])
        )
    except Exception:
        pass

    await callback.message.answer("Предложение сделки отправлено владельцу.")
    await callback.answer("Готово")


@router.callback_query(F.data.startswith("deal_accept:"))
async def deal_accept_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or deal["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_ACCEPTED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            deal["requester_user_id"],
            f"✅ Ваша сделка по объявлению ID {deal['post_id']} принята.\n\n"
            "Управление сделками происходит во вкладке МЕНЮ '🤝 Мои сделки'.\n"
            "Там вы сможете закрыть сделку и оставить отзыв.\n\n"
            "Для перехода — откройте МЕНЮ бота."
        )
    except Exception:
        pass

    await callback.message.answer("Сделка принята.")
    await callback.answer()


@router.callback_query(F.data.startswith("deal_reject:"))
async def deal_reject_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or deal["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            deal["requester_user_id"],
            f"❌ Ваша сделка по объявлению ID {deal['post_id']} отклонена."
        )
    except Exception:
        pass

    await callback.message.answer("Сделка отклонена.")
    await callback.answer()


@router.message(F.text == "🤝 Мои сделки")
async def my_deals_menu(message: Message):
    upsert_user(message)
    deals = list_user_deals(message.from_user.id)
    if not deals:
        await message.answer("У вас пока нет сделок.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer("Ваши сделки:", reply_markup=deal_list_kb(deals))


@router.callback_query(F.data.startswith("mydeal:"))
async def open_my_deal(callback: CallbackQuery):
    await callback.answer()
    try:
        deal_id = int(callback.data.split(":")[1])
        deal = get_deal(deal_id)

        if not deal or (
            deal["owner_user_id"] != callback.from_user.id
            and deal["requester_user_id"] != callback.from_user.id
        ):
            await callback.message.answer("Сделка не найдена.")
            return

        role = (
            "владелец объявления"
            if callback.from_user.id == deal["owner_user_id"]
            else "откликнувшийся"
        )
        other_user_id = (
            deal["requester_user_id"]
            if callback.from_user.id == deal["owner_user_id"]
            else deal["owner_user_id"]
        )

        text = (
            f"🤝 <b>Сделка #{deal['id']}</b>\n\n"
            f"Объявление ID: <b>{deal['post_id']}</b>\n"
            f"Статус: <b>{format_deal_status(deal['status'])}</b>\n"
            f"Ваша роль: <b>{role}</b>\n"
            f"Вторая сторона: <b>{format_user_ref(other_user_id)}</b>\n"
            f"Подтверждение владельца: <b>{'Да' if deal['owner_confirmed'] else 'Нет'}</b>\n"
            f"Подтверждение откликнувшегося: <b>{'Да' if deal['requester_confirmed'] else 'Нет'}</b>"
        )

        await callback.message.answer(
            text,
            reply_markup=deal_open_kb(deal, callback.from_user.id)
        )

    except Exception as e:
        print(f"OPEN_MY_DEAL ERROR: {e}")
        await callback.message.answer("Не удалось открыть сделку.")


@router.callback_query(F.data.startswith("deal_confirm:"))
async def deal_confirm_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if deal["status"] not in (
        DEAL_ACCEPTED,
        DEAL_COMPLETED_BY_OWNER,
        DEAL_COMPLETED_BY_REQUESTER
    ):
        await callback.answer("Эту сделку нельзя подтвердить", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        if callback.from_user.id == deal["owner_user_id"]:
            conn.execute("""
                UPDATE deals
                SET owner_confirmed=1, updated_at=?
                WHERE id=?
            """, (now_ts(), deal_id))
        elif callback.from_user.id == deal["requester_user_id"]:
            conn.execute("""
                UPDATE deals
                SET requester_confirmed=1, updated_at=?
                WHERE id=?
            """, (now_ts(), deal_id))
        else:
            await callback.answer("Нет доступа", show_alert=True)
            return

        refreshed = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()

        if refreshed["owner_confirmed"] and refreshed["requester_confirmed"]:
            conn.execute("""
                UPDATE deals
                SET status=?, completed_at=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED, now_ts(), now_ts(), deal_id))
            final_status = DEAL_COMPLETED
        elif refreshed["owner_confirmed"]:
            conn.execute("""
                UPDATE deals
                SET status=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED_BY_OWNER, now_ts(), deal_id))
            final_status = DEAL_COMPLETED_BY_OWNER
        else:
            conn.execute("""
                UPDATE deals
                SET status=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED_BY_REQUESTER, now_ts(), deal_id))
            final_status = DEAL_COMPLETED_BY_REQUESTER

    other_user_id = (
        deal["requester_user_id"]
        if callback.from_user.id == deal["owner_user_id"]
        else deal["owner_user_id"]
    )

    if final_status == DEAL_COMPLETED:
        for uid in [deal["owner_user_id"], deal["requester_user_id"]]:
            try:
                await callback.bot.send_message(
                    uid,
                    f"✅ Сделка #{deal_id} завершена. Теперь вы можете оставить отзыв.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="⭐ Оставить отзыв", callback_data=f"deal_review:{deal_id}")]
                    ])
                )
            except Exception as e:
                print(f"DEAL COMPLETE SEND ERROR: {e}")

        await callback.message.answer("Сделка полностью завершена.")

    else:
        try:
            await callback.bot.send_message(
                other_user_id,
                f"📌 Вторая сторона подтвердила завершение сделки по объявлению ID {deal['post_id']}.\n\n"
                "Чтобы окончательно закрыть сделку, откройте МЕНЮ → '🤝 Мои сделки' "
                "и подтвердите завершение со своей стороны."
            )
        except Exception as e:
            print(f"DEAL CONFIRM NOTIFY ERROR: {e}")

        await callback.message.answer(
            "Ваше подтверждение сохранено. Ждем подтверждение второй стороны."
        )

    await callback.answer()


@router.callback_query(F.data.startswith("deal_review:"))
async def deal_review_handler(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if deal["status"] != DEAL_COMPLETED:
        await callback.answer(
            "Отзыв можно оставить только после завершенной сделки",
            show_alert=True
        )
        return

    if callback.from_user.id == deal["owner_user_id"]:
        reviewed_user_id = deal["requester_user_id"]
    elif callback.from_user.id == deal["requester_user_id"]:
        reviewed_user_id = deal["owner_user_id"]
    else:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.clear()
    await state.update_data(
        reviewed_user_id=reviewed_user_id,
        post_id=deal["post_id"],
        deal_id=deal_id
    )
    await state.set_state(ReviewFlow.rating)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1", callback_data="review_rating:1"),
            InlineKeyboardButton(text="2", callback_data="review_rating:2"),
            InlineKeyboardButton(text="3", callback_data="review_rating:3"),
        ],
        [
            InlineKeyboardButton(text="4", callback_data="review_rating:4"),
            InlineKeyboardButton(text="5", callback_data="review_rating:5"),
        ],
    ])

    await callback.message.answer(
        "Поставьте оценку пользователю от 1 до 5:",
        reply_markup=kb
    )
    await callback.answer()


@router.callback_query(F.data.startswith("review_rating:"))
async def review_rating_callback(callback: CallbackQuery, state: FSMContext):
    rating = int(callback.data.split(":")[1])
    await state.update_data(rating=rating)
    await state.set_state(ReviewFlow.text)
    await callback.message.answer("Напишите короткий отзыв или отправьте '-' если без текста.")
    await callback.answer()


@router.message(ReviewFlow.text)
async def review_finish(message: Message, state: FSMContext):
    data = await state.get_data()

    review_text = None if message.text.strip() == "-" else message.text.strip()[:500]
    reviewed_user_id = data["reviewed_user_id"]
    post_id = data["post_id"]
    rating = data["rating"]

    with closing(connect_db()) as conn, conn:
        try:
            conn.execute("""
                INSERT INTO reviews (
                    reviewer_user_id,
                    reviewed_user_id,
                    post_id,
                    rating,
                    text,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                message.from_user.id,
                reviewed_user_id,
                post_id,
                rating,
                review_text,
                now_ts()
            ))
        except sqlite3.IntegrityError:
            await message.answer(
                "Такой отзыв уже оставлен.",
                reply_markup=main_menu(message.from_user.id)
            )
            await state.clear()
            return

    await message.answer(
        "⭐ Спасибо! Отзыв сохранен.",
        reply_markup=main_menu(message.from_user.id)
    )
    await state.clear()


@router.callback_query(F.data.startswith("deal_fail_post:"))
async def deal_fail_post_handler(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    ok = mark_deal_failed(post_id, callback.from_user.id)
    if ok:
        await callback.message.answer("❌ Сделка отмечена как несостоявшаяся.")
    else:
        await callback.message.answer(
            "Не удалось найти активную сделку для этого объявления."
        )
    await callback.answer()


@router.callback_query(F.data.startswith("deal_fail_direct:"))
async def deal_fail_direct_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or (
        deal["owner_user_id"] != callback.from_user.id
        and deal["requester_user_id"] != callback.from_user.id
    ):
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), deal_id))

    await callback.message.answer("❌ Сделка отмечена как несостоявшаяся.")
    await callback.answer()


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

    row = get_post(post_id)
    if not row:
        await message.answer(
            "Объявление не найдено.",
            reply_markup=main_menu(message.from_user.id)
        )
        await state.clear()
        return

    owner_user_id = row["user_id"]

    with closing(connect_db()) as conn, conn:
        conn.execute(
            """
            INSERT INTO complaints (post_id, from_user_id, reason, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (post_id, message.from_user.id, message.text.strip()[:1000], now_ts())
        )

        complaints_count = conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM complaints c
            JOIN posts p ON p.id = c.post_id
            WHERE p.user_id = ?
        """, (owner_user_id,)).fetchone()["cnt"]

    if complaints_count >= 3:
        ban_user(owner_user_id)

    await message.answer("Жалоба отправлена.", reply_markup=main_menu(message.from_user.id))

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"Новая жалоба на объявление {post_id}:\n\n"
                f"Причина: {html.escape(message.text.strip())}\n\n"
                f"Всего жалоб на пользователя: {complaints_count}\n\n"
                + post_text(row)
            )
        except Exception:
            pass

    if complaints_count >= 3:
        try:
            await bot.send_message(
                owner_user_id,
                "⛔ Ваш аккаунт автоматически заблокирован, потому что на ваши объявления поступило 3 жалобы.\n"
                "Если это ошибка — свяжитесь с администратором."
            )
        except Exception:
            pass

    await state.clear()


@router.message(Command("admin"))
@router.message(F.text == "👨‍💼 Админка")
async def admin_menu_handler(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Админка доступна только администраторам.")
        return

    with closing(connect_db()) as conn:
        pending = conn.execute(
            "SELECT COUNT(*) AS c FROM posts WHERE status='pending'"
        ).fetchone()["c"]
        complaints = conn.execute(
            "SELECT COUNT(*) AS c FROM complaints"
        ).fetchone()["c"]

    await message.answer(
        f"Админка\n\n"
        f"На модерации: {pending}\n"
        f"Жалоб: {complaints}\n\n"
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
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.status='pending'
            ORDER BY p.created_at ASC
            LIMIT 20
        """).fetchall()

    if not rows:
        await message.answer("Нет объявлений на модерации.")
        return

    for row in rows:
        await message.answer(
            post_text(row),
            reply_markup=admin_post_actions_kb(row["id"])
        )


@router.callback_query(F.data.startswith("adminapprove:"))
async def admin_approve(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET status='active', updated_at=? WHERE id=?",
            (now_ts(), post_id)
        )

    row = get_post(post_id)
    await callback.message.answer(f"Объявление {post_id} одобрено.")

    if row:
        try:
            await bot.send_message(
                row["user_id"],
                f"Ваше объявление {post_id} одобрено и опубликовано."
            )
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
        conn.execute(
            "UPDATE posts SET status='rejected', updated_at=? WHERE id=?",
            (now_ts(), post_id)
        )

    row = get_post(post_id)
    await callback.message.answer(f"Объявление {post_id} отклонено.")

    if row:
        try:
            await bot.send_message(
                row["user_id"],
                f"Ваше объявление {post_id} отклонено модератором."
            )
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

    ban_user(row["user_id"])

    try:
        await bot.send_message(
            row["user_id"],
            "Ваш аккаунт был ограничен администратором."
        )
    except Exception:
        pass

    await callback.message.answer(
        f"Пользователь {row['user_id']} забанен, его объявления деактивированы."
    )
    await callback.answer("Готово")


@router.message(Command("admin_complaints"))
async def admin_complaints(message: Message):
    if not is_admin(message.from_user.id):
        return

    with closing(connect_db()) as conn:
        rows = conn.execute("""
            SELECT c.*, p.user_id as post_owner_id, p.post_type, p.from_country, p.to_country
            FROM complaints c
            LEFT JOIN posts p ON p.id = c.post_id
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
            f"Причина: {html.escape(row['reason'])}"
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
    ban_user(user_id)
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

    await message.answer(
        f"Статус проверенного у пользователя {user_id} снят."
    )


@router.message(F.text == "ℹ️ Помощь")
async def help_handler(message: Message):
    text = (
        "<b>Помощь</b>\n\n"
        "✈️ <b>Взять посылку</b> — если вы летите и можете что-то передать.\n"
        "📦 <b>Отправить посылку</b> — если вам нужно что-то передать.\n"
        "🔎 <b>Найти совпадения</b> — быстрый поиск подходящих объявлений.\n"
        "📋 <b>Мои объявления</b> — управление своими объявлениями.\n"
        "🤝 <b>Мои сделки</b> — ваши активные и завершенные сделки.\n"
        "🔔 <b>Подписки</b> — уведомления по нужным маршрутам.\n"
        "🆘 <b>Жалоба</b> — пожаловаться на объявление."
    )
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


# -------------------------
# RUN
# -------------------------

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
Важно: в этом куске я использую функцию:

calculate_post_expires_at(...)
Она должна быть определена выше в файле, вместе с helper-функциями. Если у тебя её еще нет, вставь вот эту версию:

def calculate_post_expires_at(created_ts: int, travel_date_text: Optional[str], post_ttl_days: int = 14) -> int:
    ttl_expire = created_ts + days_to_seconds(post_ttl_days)

    if not travel_date_text:
        return ttl_expire

    travel_expire = None
    text = (travel_date_text or "").strip()

    exact_dt = parse_date_loose(text)
    if exact_dt:
        travel_expire = int(datetime(exact_dt.year, exact_dt.month, exact_dt.day, 23, 59, 59).timestamp())

    range_match = re.match(r"^(\d{2}\.\d{2}\.\d{4})\s*[-–—]\s*(\d{2}\.\d{2}\.\d{4})$", text)
    if range_match:
        end_dt = parse_date_loose(range_match.group(2))
        if end_dt:
            travel_expire = int(datetime(end_dt.year, end_dt.month, end_dt.day, 23, 59, 59).timestamp())

    if travel_expire is None:
        return ttl_expire

    return min(ttl_expire, travel_expire)
И еще: чтобы активация/создание работали правильно с новыми датами, проверь, что в create_post_record() у тебя стоит:

expires_at = calculate_post_expires_at(ts, data.get("travel_date"), POST_TTL_DAYS)
а не старое:

expires_at = ts + days_to_seconds(POST_TTL_DAYS)
Если хочешь, следующим сообщением я могу сразу дать тебе цельный полный файл целиком, уже без обрывов и дублей, чтобы ты просто вставил и запустил.


дай мне полный цельный код


Thought for 18s
Вот полный цельный код бота с учетом всех 4 правок.

import asyncio
import html
import os
import re
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "/data/bot.db")

db_dir = os.path.dirname(DB_PATH)
if db_dir:
    os.makedirs(db_dir, exist_ok=True)

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "Poputchik_china_bot").lstrip("@")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

BUMP_PRICE_TEXT = os.getenv(
    "BUMP_PRICE_TEXT",
    "Поднятие объявления оплачивается вручную. После оплаты администратор поднимет ваше объявление выше в выдаче."
)

MAX_ACTIVE_POSTS_PER_USER = int(os.getenv("MAX_ACTIVE_POSTS_PER_USER", "5"))
MIN_SECONDS_BETWEEN_ACTIONS = int(os.getenv("MIN_SECONDS_BETWEEN_ACTIONS", "2"))
POST_TTL_DAYS = int(os.getenv("POST_TTL_DAYS", "14"))
COINCIDENCE_NOTIFY_LIMIT = int(os.getenv("COINCIDENCE_NOTIFY_LIMIT", "5"))
BUMP_PRICE_AMOUNT = int(os.getenv("BUMP_PRICE_AMOUNT", "10"))
BUMP_PRICE_CURRENCY = os.getenv("BUMP_PRICE_CURRENCY", "CNY")

router = Router()

TYPE_TRIP = "trip"
TYPE_PARCEL = "parcel"

STATUS_PENDING = "pending"
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"
STATUS_REJECTED = "rejected"
STATUS_EXPIRED = "expired"
STATUS_DELETED = "deleted"

DEAL_CONTACTED = "contacted"
DEAL_OFFERED = "offered"
DEAL_ACCEPTED = "accepted"
DEAL_COMPLETED_BY_OWNER = "completed_by_owner"
DEAL_COMPLETED_BY_REQUESTER = "completed_by_requester"
DEAL_COMPLETED = "completed"
DEAL_FAILED = "failed"
DEAL_CANCELLED = "cancelled"

MANUAL_COUNTRY = "🌍 Ввести другую страну"
MANUAL_CITY = "✏️ Ввести другой город"
MANUAL_WEIGHT = "✏️ Указать другой вес"
MANUAL_DATE = "✏️ Указать точную дату"
SKIP_VALUE = "-"

COUNTRY_OPTIONS = [
    ("🇨🇳 Китай", "Китай"),
    ("🇷🇺 Россия", "Россия"),
    ("🇺🇸 США", "США"),
    ("🇰🇿 Казахстан", "Казахстан"),
    ("🇺🇿 Узбекистан", "Узбекистан"),
    ("🇰🇬 Кыргызстан", "Кыргызстан"),
    ("🇹🇯 Таджикистан", "Таджикистан"),
    ("🇦🇿 Азербайджан", "Азербайджан"),
    ("🇦🇲 Армения", "Армения"),
    ("🇬🇪 Грузия", "Грузия"),
    ("🇧🇾 Беларусь", "Беларусь"),
    ("🇺🇦 Украина", "Украина"),
    ("🇲🇩 Молдова", "Молдова"),
    ("🇻🇳 Вьетнам", "Вьетнам"),
    ("🇹🇭 Таиланд", "Таиланд"),
]

COUNTRY_CITIES_RU = {
    "Китай": [
        "Шэньчжэнь", "Гуанчжоу", "Шанхай", "Пекин", "Ханчжоу",
        "Иу", "Гонконг", "Дунгуань", "Фошань", "Чжухай",
        "Сямынь", "Чэнду", "Чунцин", "Сучжоу", "Циндао",
        "Тяньцзинь", "Нинбо", "Ухань", "Нанкин", "Сиань"
    ],
    "Россия": [
        "Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург",
        "Казань", "Нижний Новгород", "Челябинск", "Самара",
        "Омск", "Ростов-на-Дону", "Уфа", "Красноярск",
        "Владивосток", "Хабаровск", "Иркутск"
    ],
    "США": [
        "Нью-Йорк", "Лос-Анджелес", "Чикаго", "Майами",
        "Хьюстон", "Сан-Франциско", "Лас-Вегас", "Орландо"
    ],
    "Казахстан": ["Алматы", "Астана", "Шымкент", "Караганда", "Актобе"],
    "Узбекистан": ["Ташкент", "Самарканд", "Бухара", "Наманган", "Андижан"],
    "Кыргызстан": ["Бишкек", "Ош", "Джалал-Абад", "Каракол"],
    "Таджикистан": ["Душанбе", "Худжанд", "Бохтар", "Куляб"],
    "Азербайджан": ["Баку", "Гянджа", "Сумгаит", "Ленкорань"],
    "Армения": ["Ереван", "Гюмри", "Ванадзор", "Абовян"],
    "Грузия": ["Тбилиси", "Батуми", "Кутаиси", "Рустави"],
    "Беларусь": ["Минск", "Гомель", "Гродно", "Брест", "Витебск"],
    "Украина": [
        "Киев", "Харьков", "Одесса", "Днепр", "Львов",
        "Запорожье", "Винница", "Ивано-Франковск"
    ],
    "Молдова": ["Кишинёв", "Бельцы", "Тирасполь", "Кагул"],
    "Вьетнам": ["Хошимин", "Ханой", "Дананг", "Хайфон", "Нячанг"],
    "Таиланд": ["Бангкок", "Паттайя", "Пхукет", "Чиангмай", "Самуи"],
}

POPULAR_WEIGHTS = [
    "0.5 кг", "1 кг", "2 кг", "3 кг",
    "5 кг", "10 кг", "20 кг", "Более 20 кг"
]

COUNTRY_ALIASES = {
    "китай": "Китай",
    "china": "Китай",
    "кнр": "Китай",
    "россия": "Россия",
    "russia": "Россия",
    "рф": "Россия",
    "сша": "США",
    "usa": "США",
    "united states": "США",
    "america": "США",
    "америка": "США",
    "казахстан": "Казахстан",
    "kazakhstan": "Казахстан",
    "узбекистан": "Узбекистан",
    "uzbekistan": "Узбекистан",
    "кыргызстан": "Кыргызстан",
    "киргизия": "Кыргызстан",
    "kyrgyzstan": "Кыргызстан",
    "таджикистан": "Таджикистан",
    "tajikistan": "Таджикистан",
    "азербайджан": "Азербайджан",
    "azerbaijan": "Азербайджан",
    "армения": "Армения",
    "armenia": "Армения",
    "грузия": "Грузия",
    "georgia": "Грузия",
    "беларусь": "Беларусь",
    "belarus": "Беларусь",
    "украина": "Украина",
    "ukraine": "Украина",
    "молдова": "Молдова",
    "moldova": "Молдова",
    "вьетнам": "Вьетнам",
    "vietnam": "Вьетнам",
    "таиланд": "Таиланд",
    "thailand": "Таиланд",
}

CITY_ALIASES = {
    "шэньчжэнь": "Шэньчжэнь",
    "шеньчжень": "Шэньчжэнь",
    "shen zhen": "Шэньчжэнь",
    "shenzhen": "Шэньчжэнь",
    "гуанчжоу": "Гуанчжоу",
    "guangzhou": "Гуанчжоу",
    "кантон": "Гуанчжоу",
    "шанхай": "Шанхай",
    "shanghai": "Шанхай",
    "пекин": "Пекин",
    "beijing": "Пекин",
    "иу": "Иу",
    "yiwu": "Иу",
    "гонконг": "Гонконг",
    "hong kong": "Гонконг",
    "hongkong": "Гонконг",
    "дунгуань": "Дунгуань",
    "dongguan": "Дунгуань",
    "фошань": "Фошань",
    "foshan": "Фошань",
    "чжухай": "Чжухай",
    "zhuhai": "Чжухай",
    "сямынь": "Сямынь",
    "xiamen": "Сямынь",
    "чэнду": "Чэнду",
    "chengdu": "Чэнду",
    "чунцин": "Чунцин",
    "chongqing": "Чунцин",
    "сучжоу": "Сучжоу",
    "suzhou": "Сучжоу",
    "циндао": "Циндао",
    "qingdao": "Циндао",
    "тяньцзинь": "Тяньцзинь",
    "tianjin": "Тяньцзинь",
    "нинбо": "Нинбо",
    "ningbo": "Нинбо",
    "москва": "Москва",
    "moscow": "Москва",
    "санкт-петербург": "Санкт-Петербург",
    "санкт петербург": "Санкт-Петербург",
    "питер": "Санкт-Петербург",
    "spb": "Санкт-Петербург",
    "saint petersburg": "Санкт-Петербург",
    "нью-йорк": "Нью-Йорк",
    "нью йорк": "Нью-Йорк",
    "new york": "Нью-Йорк",
    "лос-анджелес": "Лос-Анджелес",
    "лос анджелес": "Лос-Анджелес",
    "los angeles": "Лос-Анджелес",
    "чикаго": "Чикаго",
    "chicago": "Чикаго",
    "майами": "Майами",
    "miami": "Майами",
    "хьюстон": "Хьюстон",
    "houston": "Хьюстон",
    "сан-франциско": "Сан-Франциско",
    "сан франциско": "Сан-Франциско",
    "san francisco": "Сан-Франциско",
    "лас-вегас": "Лас-Вегас",
    "лас вегас": "Лас-Вегас",
    "las vegas": "Лас-Вегас",
    "орландо": "Орландо",
    "orlando": "Орландо",
    "алматы": "Алматы",
    "almaty": "Алматы",
    "астана": "Астана",
    "astana": "Астана",
    "ташкент": "Ташкент",
    "tashkent": "Ташкент",
    "бишкек": "Бишкек",
    "bishkek": "Бишкек",
    "киев": "Киев",
    "kyiv": "Киев",
    "одесса": "Одесса",
    "odesa": "Одесса",
    "львов": "Львов",
    "lviv": "Львов",
    "запорожье": "Запорожье",
    "zaporizhzhia": "Запорожье",
}

STEP_ORDER = [
    "from_country",
    "from_city",
    "to_country",
    "to_city",
    "travel_date",
    "weight",
    "description",
    "contact_note",
]

STEP_NUMBERS = {name: i + 1 for i, name in enumerate(STEP_ORDER)}


# -------------------------
# SERVICE HELPERS
# -------------------------

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


def normalize_free_text(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.replace("ё", "е")
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_country_input(value: str) -> str:
    raw = normalize_free_text(value)
    return COUNTRY_ALIASES.get(raw, value.strip().title())


def normalize_city_input(value: str) -> str:
    raw = normalize_free_text(value)
    return CITY_ALIASES.get(raw, value.strip().title())


def parse_weight_kg(value: Optional[str]) -> Optional[float]:
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


def format_date_ru(dt: datetime) -> str:
    return dt.strftime("%d.%m.%Y")


def make_date_range_text(days: int) -> str:
    start = datetime.now()
    end = start + timedelta(days=days)
    return f"{format_date_ru(start)} - {format_date_ru(end)}"


def extract_travel_end_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    text = value.strip()

    exact_dt = parse_date_loose(text)
    if exact_dt:
        return datetime(exact_dt.year, exact_dt.month, exact_dt.day, 23, 59, 59)

    m = re.match(r"^\s*(\d{2}\.\d{2}\.\d{4})\s*[-–—]\s*(\d{2}\.\d{2}\.\d{4})\s*$", text)
    if m:
        end_dt = parse_date_loose(m.group(2))
        if end_dt:
            return datetime(end_dt.year, end_dt.month, end_dt.day, 23, 59, 59)

    return None


def calculate_post_expires_at(created_ts: int, travel_date_text: Optional[str], post_ttl_days: int = 14) -> int:
    ttl_expire = created_ts + days_to_seconds(post_ttl_days)
    end_dt = extract_travel_end_datetime(travel_date_text)
    if not end_dt:
        return ttl_expire
    return min(ttl_expire, int(end_dt.timestamp()))


def bot_link(start_param: Optional[str] = None) -> str:
    if start_param:
        return f"https://t.me/{BOT_USERNAME}?start={start_param}"
    return f"https://t.me/{BOT_USERNAME}"


def post_deeplink(post_id: int) -> str:
    return bot_link(f"post_{post_id}")


def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str):
    cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


# -------------------------
# DB INIT
# -------------------------

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

        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            requester_user_id INTEGER NOT NULL,
            initiator_user_id INTEGER,
            status TEXT NOT NULL DEFAULT 'contacted',
            owner_confirmed INTEGER DEFAULT 0,
            requester_confirmed INTEGER DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            completed_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS bump_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            post_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            currency TEXT NOT NULL DEFAULT 'CNY',
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            paid_at INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_posts_search
        ON posts(post_type, status, from_country, to_country, created_at);

        CREATE INDEX IF NOT EXISTS idx_posts_user
        ON posts(user_id, status, created_at);

        CREATE INDEX IF NOT EXISTS idx_subscriptions_search
        ON route_subscriptions(post_type, from_country, to_country);

        CREATE INDEX IF NOT EXISTS idx_reviews_user
        ON reviews(reviewed_user_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_deals_owner
        ON deals(owner_user_id, status, created_at);

        CREATE INDEX IF NOT EXISTS idx_deals_requester
        ON deals(requester_user_id, status, created_at);
        """)

        ensure_column(conn, "users", "is_verified", "is_verified INTEGER DEFAULT 0")
        ensure_column(conn, "users", "is_banned", "is_banned INTEGER DEFAULT 0")
        ensure_column(conn, "users", "last_action_at", "last_action_at INTEGER DEFAULT 0")
        ensure_column(conn, "posts", "expires_at", "expires_at INTEGER")
        ensure_column(conn, "deals", "initiator_user_id", "initiator_user_id INTEGER")
        ensure_column(conn, "deals", "owner_confirmed", "owner_confirmed INTEGER DEFAULT 0")
        ensure_column(conn, "deals", "requester_confirmed", "requester_confirmed INTEGER DEFAULT 0")
        ensure_column(conn, "deals", "updated_at", "updated_at INTEGER DEFAULT 0")

        conn.execute("UPDATE deals SET updated_at = created_at WHERE updated_at IS NULL OR updated_at = 0")
        conn.execute("UPDATE deals SET status='contacted' WHERE status='pending'")


# -------------------------
# USER / PROFILE HELPERS
# -------------------------

def upsert_user(message_or_callback):
    user = message_or_callback.from_user
    with closing(connect_db()) as conn, conn:
        existing = conn.execute(
            "SELECT created_at FROM users WHERE user_id=?",
            (user.id,)
        ).fetchone()
        created_at = int(existing["created_at"]) if existing and existing["created_at"] else now_ts()

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
            created_at
        ))


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def is_user_banned(user_id: int) -> bool:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT is_banned FROM users WHERE user_id=?", (user_id,)).fetchone()
        return bool(row and row["is_banned"])


def ban_user(user_id: int):
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (user_id,))
        conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE user_id=? AND status IN ('active','pending','inactive')",
            (STATUS_INACTIVE, now_ts(), user_id)
        )


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


def user_service_days(user_id: int) -> int:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT created_at FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not row or not row["created_at"]:
            return 0
        return max(0, (now_ts() - int(row["created_at"])) // 86400)


def user_service_text(user_id: int) -> str:
    days = user_service_days(user_id)
    if days < 30:
        return f"{days} дн"
    if days < 365:
        months = max(1, days // 30)
        return f"{months} мес"
    years = max(1, days // 365)
    return f"{years} г"


def user_completed_deals_count(user_id: int) -> int:
    with closing(connect_db()) as conn:
        row = conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM deals
            WHERE status='completed'
              AND (owner_user_id=? OR requester_user_id=?)
        """, (user_id, user_id)).fetchone()
        return int(row["cnt"] or 0)


def is_user_verified(user_id: int) -> bool:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT is_verified FROM users WHERE user_id=?", (user_id,)).fetchone()
        return bool(row and row["is_verified"])


def reviews_word(n: int) -> str:
    n = abs(n) % 100
    n1 = n % 10
    if 11 <= n <= 19:
        return "отзывов"
    if n1 == 1:
        return "отзыв"
    if 2 <= n1 <= 4:
        return "отзыва"
    return "отзывов"


def format_rating_line(user_id: int) -> Optional[str]:
    avg_rating, cnt = user_rating_summary(user_id)
    if cnt <= 0:
        return None
    stars = "⭐" * max(1, min(5, round(avg_rating)))
    return f"{stars} {avg_rating:.1f} ({cnt} {reviews_word(cnt)})"


def get_username_by_user_id(user_id: int) -> Optional[str]:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT username FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row["username"] if row and row["username"] else None


def format_user_ref(user_id: int) -> str:
    username = get_username_by_user_id(user_id)
    return f"@{html.escape(username)}" if username else f"USER_ID {user_id}"


def get_user_reviews(user_id: int, limit: int = 10):
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT r.rating, r.text, r.created_at, u.username, u.full_name
            FROM reviews r
            LEFT JOIN users u ON u.user_id = r.reviewer_user_id
            WHERE r.reviewed_user_id=?
            ORDER BY r.created_at DESC
            LIMIT ?
        """, (user_id, limit)).fetchall()


# -------------------------
# TEXT FORMATTING
# -------------------------

def short_post_type(post_type: str) -> str:
    return "✈️ Попутчик" if post_type == TYPE_TRIP else "📦 Посылка"


def format_deal_status(status: str) -> str:
    mapping = {
        DEAL_CONTACTED: "контакт начат",
        DEAL_OFFERED: "предложена",
        DEAL_ACCEPTED: "принята",
        DEAL_COMPLETED_BY_OWNER: "подтвердил владелец",
        DEAL_COMPLETED_BY_REQUESTER: "подтвердил откликнувшийся",
        DEAL_COMPLETED: "завершена",
        DEAL_FAILED: "не договорились",
        DEAL_CANCELLED: "отменена",
    }
    return mapping.get(status, status)


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


def form_header(post_type: str, step: int, total_steps: int = 8) -> str:
    title = "📦 Отправить посылку" if post_type == TYPE_PARCEL else "✈️ Взять посылку"
    return (
        f"{title}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"Шаг {step} / {total_steps}\n"
        f"━━━━━━━━━━━━━━\n\n"
    )


def form_text(post_type: str, step: int, prompt: str, total_steps: int = 8) -> str:
    return form_header(post_type, step, total_steps) + prompt


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
    completed_deals = user_completed_deals_count(owner_user_id)
    service_text = user_service_text(owner_user_id)

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

    lines.append("")
    lines.append("<b>👤 Профиль пользователя</b>")

    if rating_line:
        lines.append(f"⭐ <b>Рейтинг:</b> {rating_line}")
    else:
        lines.append("⭐ <b>Рейтинг:</b> пока нет отзывов")

    lines.append(f"📦 <b>Передач:</b> {completed_deals}")
    lines.append(f"📅 <b>В сервисе:</b> {service_text}")

    lines.append("")
    lines.append(f"<b>ID объявления:</b> {row['id']}")

    if for_channel:
        lines.append("Откройте бот, чтобы добавить своё объявление или найти совпадения.")
    else:
        owner = row["username"] if "username" in row.keys() else None
        if owner:
            lines.append(f"<b>Telegram:</b> @{html.escape(owner)}")

    return "\n".join(lines)


# -------------------------
# KEYBOARDS
# -------------------------

def chunk_buttons(items: List[tuple], prefix: str, per_row: int = 2):
    rows = []
    row = []
    for label, value in items:
        row.append(InlineKeyboardButton(text=label, callback_data=f"{prefix}:{value}"))
        if len(row) == per_row:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def with_back(rows: List[List[InlineKeyboardButton]], include_back: bool = True):
    if include_back:
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def back_only_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")]]
    )


def countries_kb(prefix: str):
    rows = chunk_buttons(COUNTRY_OPTIONS, prefix, per_row=2)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def countries_select_kb(prefix: str, include_back: bool = False):
    rows = chunk_buttons(COUNTRY_OPTIONS, prefix, per_row=2)
    rows.append([InlineKeyboardButton(text=MANUAL_COUNTRY, callback_data=f"{prefix}:__manual__")])
    return with_back(rows, include_back=include_back)


def cities_select_kb(prefix: str, country: str, include_back: bool = True):
    cities = COUNTRY_CITIES_RU.get(country, [])
    rows = []
    row = []
    for city in cities:
        row.append(InlineKeyboardButton(text=city, callback_data=f"{prefix}:{city}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text=MANUAL_CITY, callback_data=f"{prefix}:__manual__")])
    rows.append([InlineKeyboardButton(text="Не важно", callback_data=f"{prefix}:__skip__")])
    return with_back(rows, include_back=include_back)


def weight_select_kb():
    items = [(w, w) for w in POPULAR_WEIGHTS]
    rows = chunk_buttons(items, "weightpick", per_row=2)
    rows.append([InlineKeyboardButton(text=MANUAL_WEIGHT, callback_data="weightpick:__manual__")])
    rows.append([InlineKeyboardButton(text="Не знаю", callback_data="weightpick:__skip__")])
    return with_back(rows, include_back=True)


def date_select_kb():
    rows = [
        [InlineKeyboardButton(text="В течение недели", callback_data="datepick:week")],
        [InlineKeyboardButton(text="В течение месяца", callback_data="datepick:month")],
        [InlineKeyboardButton(text="Указать точную дату", callback_data="datepick:manual")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def my_posts_kb(posts: List[sqlite3.Row]):
    rows = []
    for p in posts:
        label = f"{p['id']} • {('✈️' if p['post_type'] == TYPE_TRIP else '📦')} • {p['from_country']}→{p['to_country']} • {p['status']}"
        rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"mypost:{p['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Нет объявлений", callback_data="noop")]])


def deal_list_kb(deals: List[sqlite3.Row]):
    rows = []
    for d in deals:
        label = f"{d['id']} • post {d['post_id']} • {format_deal_status(d['status'])}"
        rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"mydeal:{d['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Нет сделок", callback_data="noop")]])


def post_actions_kb(post_id: int, status: str):
    share_url = f"https://t.me/share/url?url={post_deeplink(post_id)}"
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
            InlineKeyboardButton(text="📤 Поделиться", url=share_url)
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
    _, reviews_count = user_rating_summary(owner_id)

    rows = [
        [InlineKeyboardButton(text="✉️ Написать владельцу", callback_data=f"contact:{post_id}:{owner_id}")],
        [InlineKeyboardButton(text="🤝 Предложить сделку", callback_data=f"offer_deal:{post_id}:{owner_id}")]
    ]

    if reviews_count > 0:
        rows.append([
            InlineKeyboardButton(
                text=f"⭐ {reviews_count} {reviews_word(reviews_count)}",
                callback_data=f"user_reviews:{owner_id}"
            )
        ])

    rows.append([InlineKeyboardButton(text="⚠️ Пожаловаться", callback_data=f"complain:{post_id}")])
    rows.append([InlineKeyboardButton(
        text="📤 Поделиться",
        url=f"https://t.me/share/url?url={post_deeplink(post_id)}"
    )])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_post_kb(post_id: int, post_type: Optional[str] = None):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="✉️ Связаться с владельцем",
                url=bot_link(f"contact_{post_id}")
            )],
            [InlineKeyboardButton(
                text="🤝 Открыть объявление",
                url=post_deeplink(post_id)
            )],
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
                callback_data=f"popular:{row['from_country']}:{row['to_country']}"
            )
        ])
    return InlineKeyboardMarkup(
        inline_keyboard=buttons or [[InlineKeyboardButton(text="Пока пусто", callback_data="noop")]]
    )


def deal_open_kb(deal: sqlite3.Row, user_id: int) -> InlineKeyboardMarkup:
    rows = []

    if deal["status"] in (DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
        rows.append([InlineKeyboardButton(text="✅ Подтвердить завершение", callback_data=f"deal_confirm:{deal['id']}")])

    if deal["status"] == DEAL_COMPLETED:
        rows.append([InlineKeyboardButton(text="⭐ Оставить отзыв", callback_data=f"deal_review:{deal['id']}")])

    if deal["status"] in (DEAL_CONTACTED, DEAL_OFFERED, DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
        rows.append([InlineKeyboardButton(text="❌ Не договорились", callback_data=f"deal_fail_direct:{deal['id']}")])

    if not rows:
        rows = [[InlineKeyboardButton(text="Ок", callback_data="noop")]]

    return InlineKeyboardMarkup(inline_keyboard=rows)


def main_menu(user_id: Optional[int] = None):
    keyboard = [
        [KeyboardButton(text="✈️ Взять посылку"), KeyboardButton(text="📦 Отправить посылку")],
        [KeyboardButton(text="🔎 Найти совпадения"), KeyboardButton(text="📋 Мои объявления")],
        [KeyboardButton(text="🤝 Мои сделки"), KeyboardButton(text="🔥 Популярные маршруты")],
        [KeyboardButton(text="🆕 Новые объявления"), KeyboardButton(text="🔔 Подписки")],
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="💰 Поднять объявление")],
        [KeyboardButton(text="🆘 Жалоба"), KeyboardButton(text="ℹ️ Помощь")],
    ]
    if user_id is not None and is_admin(user_id):
        keyboard.append([KeyboardButton(text="👨‍💼 Админка")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


# -------------------------
# FSM STATES
# -------------------------

class CreatePost(StatesGroup):
    from_country = State()
    from_country_manual = State()

    from_city = State()
    from_city_manual = State()

    to_country = State()
    to_country_manual = State()

    to_city = State()
    to_city_manual = State()

    travel_date = State()
    travel_date_manual = State()

    weight = State()
    weight_manual = State()

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


# -------------------------
# CREATE FLOW HELPERS
# -------------------------

async def render_create_step(target_step: str, target_message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if target_step == "from_country":
        await state.set_state(CreatePost.from_country)
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], "Выберите страну отправления"),
            reply_markup=countries_select_kb("from_country_pick", include_back=False)
        )
        return

    if target_step == "from_city":
        await state.set_state(CreatePost.from_city)
        country = data.get("from_country", "")
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], f"Выберите город отправления в стране {country}"),
            reply_markup=cities_select_kb("from_city_pick", country, include_back=True)
        )
        return

    if target_step == "to_country":
        await state.set_state(CreatePost.to_country)
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], "Выберите страну назначения"),
            reply_markup=countries_select_kb("to_country_pick", include_back=True)
        )
        return

    if target_step == "to_city":
        await state.set_state(CreatePost.to_city)
        country = data.get("to_country", "")
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], f"Выберите город назначения в стране {country}"),
            reply_markup=cities_select_kb("to_city_pick", country, include_back=True)
        )
        return

    if target_step == "travel_date":
        await state.set_state(CreatePost.travel_date)
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], "Выберите дату поездки / отправки"),
            reply_markup=date_select_kb()
        )
        return

    if target_step == "weight":
        await state.set_state(CreatePost.weight)
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], "Выберите вес или объём"),
            reply_markup=weight_select_kb()
        )
        return

    if target_step == "description":
        await state.set_state(CreatePost.description)
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], "Опишите объявление подробно\nЧто нужно передать / сколько места есть / условия"),
            reply_markup=back_only_kb()
        )
        return

    if target_step == "contact_note":
        await state.set_state(CreatePost.contact_note)
        await target_message.answer(
            form_text(
                post_type,
                STEP_NUMBERS[target_step],
                "Введите дополнительный контакт или примечание\nНапример: WeChat ID / только текст / без звонков\nЕсли не нужно — напишите -"
            ),
            reply_markup=back_only_kb()
        )
        return


def get_current_create_step_name(state_name: Optional[str]) -> Optional[str]:
    if not state_name:
        return None

    mapping = {
        CreatePost.from_country.state: "from_country",
        CreatePost.from_country_manual.state: "from_country",
        CreatePost.from_city.state: "from_city",
        CreatePost.from_city_manual.state: "from_city",
        CreatePost.to_country.state: "to_country",
        CreatePost.to_country_manual.state: "to_country",
        CreatePost.to_city.state: "to_city",
        CreatePost.to_city_manual.state: "to_city",
        CreatePost.travel_date.state: "travel_date",
        CreatePost.travel_date_manual.state: "travel_date",
        CreatePost.weight.state: "weight",
        CreatePost.weight_manual.state: "weight",
        CreatePost.description.state: "description",
        CreatePost.contact_note.state: "contact_note",
    }
    return mapping.get(state_name)


CREATE_STEP_CLEANUP_KEYS = {
    "from_country": ["from_country", "from_city", "to_country", "to_city", "travel_date", "weight_kg", "description", "contact_note"],
    "from_city": ["from_city", "to_country", "to_city", "travel_date", "weight_kg", "description", "contact_note"],
    "to_country": ["to_country", "to_city", "travel_date", "weight_kg", "description", "contact_note"],
    "to_city": ["to_city", "travel_date", "weight_kg", "description", "contact_note"],
    "travel_date": ["travel_date", "description", "contact_note"],
    "weight": ["weight_kg", "description", "contact_note"],
    "description": ["description", "contact_note"],
    "contact_note": ["contact_note"],
}


async def clear_step_data_from(state: FSMContext, target_step: str):
    data = await state.get_data()
    for key in CREATE_STEP_CLEANUP_KEYS.get(target_step, []):
        data.pop(key, None)
    await state.set_data(data)


# -------------------------
# POST / DEAL DATA HELPERS
# -------------------------

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
            SELECT from_country, to_country, COUNT(*) AS cnt
            FROM posts
            WHERE status='active'
              AND (expires_at IS NULL OR expires_at > ?)
            GROUP BY from_country, to_country
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


def search_route_posts_all(from_country: str, to_country: str, limit: int = 20) -> List[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.from_country=?
              AND p.to_country=?
              AND p.status='active'
              AND (p.expires_at IS NULL OR p.expires_at > ?)
            ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
            LIMIT ?
        """, (from_country, to_country, now_ts(), limit)).fetchall()


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


def create_post_record(data: dict, user_id: int) -> int:
    ts = now_ts()
    expires_at = calculate_post_expires_at(ts, data.get("travel_date"), POST_TTL_DAYS)

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
        return int(cur.lastrowid)


def add_route_subscription(user_id: int, post_type: str, from_country: str, to_country: str):
    with closing(connect_db()) as conn, conn:
        exists = conn.execute("""
            SELECT id FROM route_subscriptions
            WHERE user_id=? AND post_type=? AND from_country=? AND to_country=?
            LIMIT 1
        """, (user_id, post_type, from_country, to_country)).fetchone()
        if exists:
            return
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


def delete_subscription(user_id: int, sub_id: int) -> bool:
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("DELETE FROM route_subscriptions WHERE id=? AND user_id=?", (sub_id, user_id))
        return cur.rowcount > 0


def reserve_coincidence_notification(post_a_id: int, post_b_id: int) -> bool:
    a, b = sorted([post_a_id, post_b_id])
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("""
            INSERT OR IGNORE INTO coincidence_notifications (post_a_id, post_b_id, created_at)
            VALUES (?, ?, ?)
        """, (a, b, now_ts()))
        return cur.rowcount > 0


def calculate_coincidence_score(source_row, candidate_row: sqlite3.Row) -> Tuple[int, List[str]]:
    score = 40
    notes: List[str] = []

    source_from_city = normalize_text(source_row["from_city"])
    candidate_from_city = normalize_text(candidate_row["from_city"])
    source_to_city = normalize_text(source_row["to_city"])
    candidate_to_city = normalize_text(candidate_row["to_city"])

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

    source_date = extract_travel_end_datetime(source_row["travel_date"])
    candidate_date = extract_travel_end_datetime(candidate_row["travel_date"])
    if source_date and candidate_date:
        days_diff = abs((source_date.date() - candidate_date.date()).days)
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

    source_weight = parse_weight_kg(source_row["weight_kg"])
    candidate_weight = parse_weight_kg(candidate_row["weight_kg"])

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
    source_row=None,
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

        coincidence_type = "strong" if score >= 75 else "good" if score >= 55 else "possible"
        results.append({
            "row": row,
            "score": score,
            "notes": notes,
            "type": coincidence_type
        })

    results.sort(key=lambda x: (x["score"], x["row"]["bumped_at"] or x["row"]["created_at"]), reverse=True)
    return results[:limit]


def search_posts_inline(query: str, limit: int = 10) -> List[sqlite3.Row]:
    q = f"%{query.strip().lower()}%"
    with closing(connect_db()) as conn:
        if query.strip():
            return conn.execute("""
                SELECT p.*, u.username, u.full_name
                FROM posts p
                LEFT JOIN users u ON u.user_id = p.user_id
                WHERE p.status='active'
                  AND (p.expires_at IS NULL OR p.expires_at > ?)
                  AND (
                        lower(p.from_country) LIKE ?
                     OR lower(COALESCE(p.from_city, '')) LIKE ?
                     OR lower(p.to_country) LIKE ?
                     OR lower(COALESCE(p.to_city, '')) LIKE ?
                     OR lower(COALESCE(p.description, '')) LIKE ?
                     OR lower(COALESCE(p.travel_date, '')) LIKE ?
                  )
                ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
                LIMIT ?
            """, (now_ts(), q, q, q, q, q, q, limit)).fetchall()

        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.status='active'
              AND (p.expires_at IS NULL OR p.expires_at > ?)
            ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
            LIMIT ?
        """, (now_ts(), limit)).fetchall()


def ensure_deal(post_id: int, owner_user_id: int, requester_user_id: int, initiator_user_id: int) -> int:
    with closing(connect_db()) as conn, conn:
        row = conn.execute("""
            SELECT id
            FROM deals
            WHERE post_id=? AND owner_user_id=? AND requester_user_id=?
            ORDER BY id DESC
            LIMIT 1
        """, (post_id, owner_user_id, requester_user_id)).fetchone()

        if row:
            return int(row["id"])

        cur = conn.execute("""
            INSERT INTO deals (
                post_id,
                owner_user_id,
                requester_user_id,
                initiator_user_id,
                status,
                owner_confirmed,
                requester_confirmed,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
        """, (
            post_id,
            owner_user_id,
            requester_user_id,
            initiator_user_id,
            DEAL_CONTACTED,
            now_ts(),
            now_ts(),
        ))
        return int(cur.lastrowid)


def get_deal(deal_id: int) -> Optional[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()


def list_user_deals(user_id: int) -> List[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT *
            FROM deals
            WHERE owner_user_id=? OR requester_user_id=?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 30
        """, (user_id, user_id)).fetchall()


def mark_deal_failed(post_id: int, user_id: int) -> bool:
    with closing(connect_db()) as conn, conn:
        row = conn.execute("""
            SELECT id
            FROM deals
            WHERE post_id=?
              AND (owner_user_id=? OR requester_user_id=?)
              AND status IN (?, ?, ?, ?, ?)
            ORDER BY id DESC
            LIMIT 1
        """, (
            post_id, user_id, user_id,
            DEAL_CONTACTED, DEAL_OFFERED, DEAL_ACCEPTED,
            DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER
        )).fetchone()

        if not row:
            return False

        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), row["id"]))
        return True


def create_bump_order(user_id: int, post_id: int, amount: int = BUMP_PRICE_AMOUNT, currency: str = BUMP_PRICE_CURRENCY) -> int:
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("""
            INSERT INTO bump_orders (user_id, post_id, amount, currency, status, created_at)
            VALUES (?, ?, ?, ?, 'pending', ?)
        """, (user_id, post_id, amount, currency, now_ts()))
        return int(cur.lastrowid)


# -------------------------
# CHANNEL / NOTIFY HELPERS
# -------------------------

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

    for item in coincidences:
        row = item["row"]
        score = item["score"]
        notes = item["notes"]

        if not reserve_coincidence_notification(new_row["id"], row["id"]):
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
                "🔔 По вашей подписке появилось новое объявление:\n\n" + post_text(row),
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

                if not reserve_coincidence_notification(row["id"], target["id"]):
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

    except Exception as e:
        print(f"GLOBAL COINCIDENCE SCAN ERROR: {e}")


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


# -------------------------
# FLOWS
# -------------------------

async def begin_create(message: Message, state: FSMContext, post_type: str):
    upsert_user(message)

    if is_user_banned(message.from_user.id):
        await message.answer(
            "⛔ Ваш аккаунт ограничен. Если это ошибка — свяжитесь с администратором.",
            reply_markup=main_menu(message.from_user.id)
        )
        return

    spam_error = anti_spam_check(message.from_user.id)
    if spam_error:
        await message.answer(spam_error, reply_markup=main_menu(message.from_user.id))
        return

    if active_post_count(message.from_user.id) >= MAX_ACTIVE_POSTS_PER_USER:
        await message.answer(
            f"У вас уже слишком много объявлений. Лимит: {MAX_ACTIVE_POSTS_PER_USER}. "
            "Удалите или деактивируйте старые объявления.",
            reply_markup=main_menu(message.from_user.id)
        )
        return

    await state.clear()
    await state.update_data(post_type=post_type)
    await state.set_state(CreatePost.from_country)

    await message.answer(
        form_text(post_type, 1, "Выберите страну отправления"),
        reply_markup=countries_select_kb("from_country_pick", include_back=False)
    )


def owner_only(callback: CallbackQuery, post_id: int) -> Optional[sqlite3.Row]:
    row = get_post(post_id)
    if not row or row["user_id"] != callback.from_user.id:
        return None
    return row


# -------------------------
# HANDLERS
# -------------------------

@router.inline_query()
async def inline_search_handler(inline_query: InlineQuery):
    query = (inline_query.query or "").strip()
    rows = search_posts_inline(query, limit=10)
    results = []

    for row in rows:
        title = f"{'✈️' if row['post_type'] == TYPE_TRIP else '📦'} {row['from_country']} → {row['to_country']}"
        if row["from_city"] or row["to_city"]:
            from_part = row["from_city"] or row["from_country"]
            to_part = row["to_city"] or row["to_country"]
            title = f"{'✈️' if row['post_type'] == TYPE_TRIP else '📦'} {from_part} → {to_part}"

        description_parts = []
        if row["travel_date"]:
            description_parts.append(f"Дата: {row['travel_date']}")
        if row["weight_kg"]:
            description_parts.append(f"Вес: {row['weight_kg']}")
        if row["description"]:
            description_parts.append(row["description"][:80])

        description = " | ".join(description_parts)[:200] or "Открыть объявление"
        text = f"{post_text(row)}\n\n🤖 Открыть в боте: {post_deeplink(row['id'])}"

        results.append(
            InlineQueryResultArticle(
                id=str(row["id"]),
                title=title[:256],
                description=description,
                input_message_content=InputTextMessageContent(
                    message_text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                ),
                reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"]),
            )
        )

    if not results:
        results = [
            InlineQueryResultArticle(
                id="no_results",
                title="Ничего не найдено",
                description="Попробуйте: Китай Россия, Шэньчжэнь Москва, посылка, попутчик",
                input_message_content=InputTextMessageContent(
                    message_text=(
                        "Ничего не найдено.\n\n"
                        f"Открой бота и создай объявление: {bot_link()}"
                    ),
                    disable_web_page_preview=True,
                ),
            )
        ]

    await inline_query.answer(results, cache_time=1, is_personal=True)


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    upsert_user(message)
    await state.clear()

    if is_user_banned(message.from_user.id):
        await message.answer(
            "⛔ Ваш аккаунт ограничен из-за жалоб пользователей.\n"
            "Если это ошибка — свяжитесь с администратором."
        )
        return

    start_arg = ""
    if message.text and " " in message.text:
        start_arg = message.text.split(" ", 1)[1].strip()

    if start_arg == "parcel":
        await begin_create(message, state, TYPE_PARCEL)
        return

    if start_arg == "trip":
        await begin_create(message, state, TYPE_TRIP)
        return

    if start_arg.startswith("contact_"):
        post_id_str = start_arg.replace("contact_", "", 1)
        if post_id_str.isdigit():
            row = get_post(int(post_id_str))

            if row and row["status"] == STATUS_ACTIVE:
                if row["user_id"] == message.from_user.id:
                    await message.answer("Это ваше объявление.", reply_markup=main_menu(message.from_user.id))
                    return

                deal_id = ensure_deal(
                    post_id=row["id"],
                    owner_user_id=row["user_id"],
                    requester_user_id=message.from_user.id,
                    initiator_user_id=message.from_user.id
                )

                await state.set_state(ContactFlow.message_text)
                await state.update_data(
                    post_id=row["id"],
                    target_user_id=row["user_id"],
                    deal_id=deal_id
                )

                await message.answer(
                    "✉️ Вы открыли связь с владельцем объявления:\n\n"
                    f"{post_text(row)}\n\n"
                    "Напишите сообщение, и я перешлю его владельцу."
                )
            else:
                await message.answer("Объявление не найдено или уже неактивно.")
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

    text = (
        "👋 <b>Привет.</b>\n\n"
        "Это <b>Попутчик Китай</b> — бот для передачи посылок через попутчиков.\n\n"
        "<b>Здесь можно отправить свою посылку или взять чужую по маршруту.</b>\n\n"
        "🔎 <b>Подпишись на канал с объявлениями:</b>\n"
        "t.me/china_poputchik\n\n"
        "Я сам ищу совпадения и уведомляю о них.\n\n"
        "<b>Нажми нужную кнопку в меню и заполни заявку.</b>\n\n"
        "⬇️ <b>Выберите действие в меню ниже</b>"
    )

    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.message(StateFilter("*"), Command("new_trip"))
@router.message(StateFilter("*"), F.text == "✈️ Взять посылку")
async def add_trip(message: Message, state: FSMContext):
    await state.clear()
    await begin_create(message, state, TYPE_TRIP)


@router.message(StateFilter("*"), Command("new_parcel"))
@router.message(StateFilter("*"), F.text == "📦 Отправить посылку")
async def add_parcel(message: Message, state: FSMContext):
    await state.clear()
    await begin_create(message, state, TYPE_PARCEL)


# -------------------------
# CREATE POST FLOW
# -------------------------

@router.callback_query(F.data == "create_back")
async def create_back_handler(callback: CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    step_name = get_current_create_step_name(current_state)

    if not step_name:
        await callback.answer("Назад недоступно", show_alert=True)
        return

    idx = STEP_ORDER.index(step_name)
    if idx == 0:
        await callback.answer("Это первый шаг", show_alert=True)
        return

    prev_step = STEP_ORDER[idx - 1]
    await clear_step_data_from(state, prev_step)
    await render_create_step(prev_step, callback.message, state)
    await callback.answer()


@router.callback_query(F.data.startswith("from_country_pick:"))
async def pick_from_country(callback: CallbackQuery, state: FSMContext):
    upsert_user(callback)
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.from_country_manual)
        await callback.message.answer(
            form_text(post_type, 1, "Введите страну отправления вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(from_country=value)
    await state.set_state(CreatePost.from_city)
    await callback.message.answer(
        form_text(post_type, 2, f"Выберите город отправления в стране {value}"),
        reply_markup=cities_select_kb("from_city_pick", value, include_back=True)
    )
    await callback.answer()


@router.message(CreatePost.from_country_manual)
async def from_country_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
    value = normalize_country_input(message.text.strip()[:80])

    await state.update_data(from_country=value)
    await state.set_state(CreatePost.from_city)
    await message.answer(
        form_text(post_type, 2, f"Выберите город отправления в стране {value}"),
        reply_markup=cities_select_kb("from_city_pick", value, include_back=True)
    )


@router.callback_query(F.data.startswith("from_city_pick:"))
async def pick_from_city(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.from_city_manual)
        await callback.message.answer(
            form_text(post_type, 2, "Введите город отправления вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    if value == "__skip__":
        await state.update_data(from_city=None)
    else:
        await state.update_data(from_city=value)

    await state.set_state(CreatePost.to_country)
    await callback.message.answer(
        form_text(post_type, 3, "Выберите страну назначения"),
        reply_markup=countries_select_kb("to_country_pick", include_back=True)
    )
    await callback.answer()


@router.message(CreatePost.from_city_manual)
async def from_city_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    value = message.text.strip()
    await state.update_data(from_city=None if value == SKIP_VALUE else normalize_city_input(value[:80]))
    await state.set_state(CreatePost.to_country)
    await message.answer(
        form_text(post_type, 3, "Выберите страну назначения"),
        reply_markup=countries_select_kb("to_country_pick", include_back=True)
    )


@router.callback_query(F.data.startswith("to_country_pick:"))
async def pick_to_country(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.to_country_manual)
        await callback.message.answer(
            form_text(post_type, 3, "Введите страну назначения вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(to_country=value)
    await state.set_state(CreatePost.to_city)
    await callback.message.answer(
        form_text(post_type, 4, f"Выберите город назначения в стране {value}"),
        reply_markup=cities_select_kb("to_city_pick", value, include_back=True)
    )
    await callback.answer()


@router.message(CreatePost.to_country_manual)
async def to_country_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
    value = normalize_country_input(message.text.strip()[:80])

    await state.update_data(to_country=value)
    await state.set_state(CreatePost.to_city)
    await message.answer(
        form_text(post_type, 4, f"Выберите город назначения в стране {value}"),
        reply_markup=cities_select_kb("to_city_pick", value, include_back=True)
    )


@router.callback_query(F.data.startswith("to_city_pick:"))
async def pick_to_city(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.to_city_manual)
        await callback.message.answer(
            form_text(post_type, 4, "Введите город назначения вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    if value == "__skip__":
        await state.update_data(to_city=None)
    else:
        await state.update_data(to_city=value)

    await state.set_state(CreatePost.travel_date)
    await callback.message.answer(
        form_text(post_type, 5, "Выберите дату поездки / отправки"),
        reply_markup=date_select_kb()
    )
    await callback.answer()


@router.message(CreatePost.to_city_manual)
async def to_city_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    value = message.text.strip()
    await state.update_data(to_city=None if value == SKIP_VALUE else normalize_city_input(value[:80]))
    await state.set_state(CreatePost.travel_date)
    await message.answer(
        form_text(post_type, 5, "Выберите дату поездки / отправки"),
        reply_markup=date_select_kb()
    )


@router.callback_query(F.data.startswith("datepick:"))
async def pick_date(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "manual":
        await state.set_state(CreatePost.travel_date_manual)
        await callback.message.answer(
            form_text(post_type, 5, "Введите точную дату\nНапример: 15.03.2026"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    if value == "week":
        travel_date = make_date_range_text(7)
    elif value == "month":
        travel_date = make_date_range_text(30)
    else:
        await callback.answer("Неверный выбор", show_alert=True)
        return

    await state.update_data(travel_date=travel_date)
    await state.set_state(CreatePost.weight)
    await callback.message.answer(
        form_text(post_type, 6, "Выберите вес или объём"),
        reply_markup=weight_select_kb()
    )
    await callback.answer()


@router.message(CreatePost.travel_date_manual)
async def date_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    raw = message.text.strip()[:100]
    parsed = parse_date_loose(raw)
    if not parsed:
        await message.answer(
            "Не смог распознать дату.\nВведите в формате: 15.03.2026",
            reply_markup=back_only_kb()
        )
        return

    await state.update_data(travel_date=format_date_ru(parsed))
    await state.set_state(CreatePost.weight)
    await message.answer(
        form_text(post_type, 6, "Выберите вес или объём"),
        reply_markup=weight_select_kb()
    )


@router.callback_query(F.data.startswith("weightpick:"))
async def pick_weight(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.weight_manual)
        await callback.message.answer(
            form_text(post_type, 6, "Введите свой вес / объём\n

дай мне полный цельный код


Thought for 16s

ты не дописал - продолжи отсюда @router.callback_query(F.data.startswith("deal_confirm:"))
async def deal_confirm_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if deal["status"] not in (DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL

Вот продолжение с этого места до конца файла:

@router.callback_query(F.data.startswith("deal_confirm:"))
async def deal_confirm_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if deal["status"] not in (
        DEAL_ACCEPTED,
        DEAL_COMPLETED_BY_OWNER,
        DEAL_COMPLETED_BY_REQUESTER,
    ):
        await callback.answer("Эту сделку нельзя подтвердить", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        if callback.from_user.id == deal["owner_user_id"]:
            conn.execute("""
                UPDATE deals
                SET owner_confirmed=1, updated_at=?
                WHERE id=?
            """, (now_ts(), deal_id))
        elif callback.from_user.id == deal["requester_user_id"]:
            conn.execute("""
                UPDATE deals
                SET requester_confirmed=1, updated_at=?
                WHERE id=?
            """, (now_ts(), deal_id))
        else:
            await callback.answer("Нет доступа", show_alert=True)
            return

        refreshed = conn.execute(
            "SELECT * FROM deals WHERE id=?",
            (deal_id,)
        ).fetchone()

        if refreshed["owner_confirmed"] and refreshed["requester_confirmed"]:
            conn.execute("""
                UPDATE deals
                SET status=?, completed_at=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED, now_ts(), now_ts(), deal_id))
            final_status = DEAL_COMPLETED
        elif refreshed["owner_confirmed"]:
            conn.execute("""
                UPDATE deals
                SET status=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED_BY_OWNER, now_ts(), deal_id))
            final_status = DEAL_COMPLETED_BY_OWNER
        else:
            conn.execute("""
                UPDATE deals
                SET status=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED_BY_REQUESTER, now_ts(), deal_id))
            final_status = DEAL_COMPLETED_BY_REQUESTER

    other_user_id = (
        deal["requester_user_id"]
        if callback.from_user.id == deal["owner_user_id"]
        else deal["owner_user_id"]
    )

    if final_status == DEAL_COMPLETED:
        for uid in [deal["owner_user_id"], deal["requester_user_id"]]:
            try:
                await callback.bot.send_message(
                    uid,
                    f"✅ Сделка #{deal_id} завершена. Теперь вы можете оставить отзыв.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="⭐ Оставить отзыв", callback_data=f"deal_review:{deal_id}")]
                    ])
                )
            except Exception as e:
                print(f"DEAL COMPLETE SEND ERROR: {e}")

        await callback.message.answer("Сделка полностью завершена.")
    else:
        try:
            await callback.bot.send_message(
                other_user_id,
                f"📌 Вторая сторона подтвердила завершение сделки по объявлению ID {deal['post_id']}.\n\n"
                "Чтобы окончательно закрыть сделку, откройте МЕНЮ → '🤝 Мои сделки' "
                "и подтвердите завершение со своей стороны."
            )
        except Exception as e:
            print(f"DEAL CONFIRM NOTIFY ERROR: {e}")

        await callback.message.answer(
            "Ваше подтверждение сохранено. Ждем подтверждение второй стороны."
        )

    await callback.answer()


@router.callback_query(F.data.startswith("deal_review:"))
async def deal_review_handler(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if deal["status"] != DEAL_COMPLETED:
        await callback.answer(
            "Отзыв можно оставить только после завершенной сделки",
            show_alert=True
        )
        return

    if callback.from_user.id == deal["owner_user_id"]:
        reviewed_user_id = deal["requester_user_id"]
    elif callback.from_user.id == deal["requester_user_id"]:
        reviewed_user_id = deal["owner_user_id"]
    else:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.clear()
    await state.update_data(
        reviewed_user_id=reviewed_user_id,
        post_id=deal["post_id"],
        deal_id=deal_id
    )
    await state.set_state(ReviewFlow.rating)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1", callback_data="review_rating:1"),
            InlineKeyboardButton(text="2", callback_data="review_rating:2"),
            InlineKeyboardButton(text="3", callback_data="review_rating:3"),
        ],
        [
            InlineKeyboardButton(text="4", callback_data="review_rating:4"),
            InlineKeyboardButton(text="5", callback_data="review_rating:5"),
        ],
    ])

    await callback.message.answer(
        "Поставьте оценку пользователю от 1 до 5:",
        reply_markup=kb
    )
    await callback.answer()


@router.callback_query(F.data.startswith("review_rating:"))
async def review_rating_callback(callback: CallbackQuery, state: FSMContext):
    rating = int(callback.data.split(":")[1])
    await state.update_data(rating=rating)
    await state.set_state(ReviewFlow.text)
    await callback.message.answer("Напишите короткий отзыв или отправьте '-' если без текста.")
    await callback.answer()


@router.message(ReviewFlow.text)
async def review_finish(message: Message, state: FSMContext):
    data = await state.get_data()

    review_text = None if message.text.strip() == "-" else message.text.strip()[:500]
    reviewed_user_id = data["reviewed_user_id"]
    post_id = data["post_id"]
    rating = data["rating"]

    with closing(connect_db()) as conn, conn:
        try:
            conn.execute("""
                INSERT INTO reviews (
                    reviewer_user_id,
                    reviewed_user_id,
                    post_id,
                    rating,
                    text,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                message.from_user.id,
                reviewed_user_id,
                post_id,
                rating,
                review_text,
                now_ts()
            ))
        except sqlite3.IntegrityError:
            await message.answer(
                "Такой отзыв уже оставлен.",
                reply_markup=main_menu(message.from_user.id)
            )
            await state.clear()
            return

    await message.answer(
        "⭐ Спасибо! Отзыв сохранен.",
        reply_markup=main_menu(message.from_user.id)
    )
    await state.clear()


@router.callback_query(F.data.startswith("deal_fail_post:"))
async def deal_fail_post_handler(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    ok = mark_deal_failed(post_id, callback.from_user.id)
    if ok:
        await callback.message.answer("❌ Сделка отмечена как несостоявшаяся.")
    else:
        await callback.message.answer(
            "Не удалось найти активную сделку для этого объявления."
        )
    await callback.answer()


@router.callback_query(F.data.startswith("deal_fail_direct:"))
async def deal_fail_direct_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or (
        deal["owner_user_id"] != callback.from_user.id
        and deal["requester_user_id"] != callback.from_user.id
    ):
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), deal_id))

    await callback.message.answer("❌ Сделка отмечена как несостоявшаяся.")
    await callback.answer()


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

    row = get_post(post_id)
    if not row:
        await message.answer(
            "Объявление не найдено.",
            reply_markup=main_menu(message.from_user.id)
        )
        await state.clear()
        return

    owner_user_id = row["user_id"]

    with closing(connect_db()) as conn, conn:
        conn.execute(
            """
            INSERT INTO complaints (post_id, from_user_id, reason, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (post_id, message.from_user.id, message.text.strip()[:1000], now_ts())
        )

        complaints_count = conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM complaints c
            JOIN posts p ON p.id = c.post_id
            WHERE p.user_id = ?
        """, (owner_user_id,)).fetchone()["cnt"]

    if complaints_count >= 3:
        ban_user(owner_user_id)

    await message.answer("Жалоба отправлена.", reply_markup=main_menu(message.from_user.id))

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"Новая жалоба на объявление {post_id}:\n\n"
                f"Причина: {html.escape(message.text.strip())}\n\n"
                f"Всего жалоб на пользователя: {complaints_count}\n\n"
                + post_text(row)
            )
        except Exception:
            pass

    if complaints_count >= 3:
        try:
            await bot.send_message(
                owner_user_id,
                "⛔ Ваш аккаунт автоматически заблокирован, потому что на ваши объявления поступило 3 жалобы.\n"
                "Если это ошибка — свяжитесь с администратором."
            )
        except Exception:
            pass

    await state.clear()


@router.message(Command("admin"))
@router.message(F.text == "👨‍💼 Админка")
async def admin_menu_handler(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Админка доступна только администраторам.")
        return

    with closing(connect_db()) as conn:
        pending = conn.execute(
            "SELECT COUNT(*) AS c FROM posts WHERE status='pending'"
        ).fetchone()["c"]
        complaints = conn.execute(
            "SELECT COUNT(*) AS c FROM complaints"
        ).fetchone()["c"]

    await message.answer(
        f"Админка\n\n"
        f"На модерации: {pending}\n"
        f"Жалоб: {complaints}\n\n"
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
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.status='pending'
            ORDER BY p.created_at ASC
            LIMIT 20
        """).fetchall()

    if not rows:
        await message.answer("Нет объявлений на модерации.")
        return

    for row in rows:
        await message.answer(
            post_text(row),
            reply_markup=admin_post_actions_kb(row["id"])
        )


@router.callback_query(F.data.startswith("adminapprove:"))
async def admin_approve(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET status='active', updated_at=? WHERE id=?",
            (now_ts(), post_id)
        )

    row = get_post(post_id)
    await callback.message.answer(f"Объявление {post_id} одобрено.")

    if row:
        try:
            await bot.send_message(
                row["user_id"],
                f"Ваше объявление {post_id} одобрено и опубликовано."
            )
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
        conn.execute(
            "UPDATE posts SET status='rejected', updated_at=? WHERE id=?",
            (now_ts(), post_id)
        )

    row = get_post(post_id)
    await callback.message.answer(f"Объявление {post_id} отклонено.")

    if row:
        try:
            await bot.send_message(
                row["user_id"],
                f"Ваше объявление {post_id} отклонено модератором."
            )
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

    ban_user(row["user_id"])

    try:
        await bot.send_message(
            row["user_id"],
            "Ваш аккаунт был ограничен администратором."
        )
    except Exception:
        pass

    await callback.message.answer(
        f"Пользователь {row['user_id']} забанен, его объявления деактивированы."
    )
    await callback.answer("Готово")


@router.message(Command("admin_complaints"))
async def admin_complaints(message: Message):
    if not is_admin(message.from_user.id):
        return

    with closing(connect_db()) as conn:
        rows = conn.execute("""
            SELECT c.*, p.user_id as post_owner_id, p.post_type, p.from_country, p.to_country
            FROM complaints c
            LEFT JOIN posts p ON p.id = c.post_id
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
            f"Причина: {html.escape(row['reason'])}"
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
    ban_user(user_id)
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

    await message.answer(
        f"Статус проверенного у пользователя {user_id} снят."
    )


@router.message(F.text == "ℹ️ Помощь")
async def help_handler(message: Message):
    text = (
        "<b>Помощь</b>\n\n"
        "✈️ <b>Взять посылку</b> — если вы летите и можете что-то передать.\n"
        "📦 <b>Отправить посылку</b> — если вам нужно что-то передать.\n"
        "🔎 <b>Найти совпадения</b> — быстрый поиск подходящих объявлений.\n"
        "📋 <b>Мои объявления</b> — управление своими объявлениями.\n"
        "🤝 <b>Мои сделки</b> — ваши активные и завершенные сделки.\n"
        "🔔 <b>Подписки</b> — уведомления по нужным маршрутам.\n"
        "🆘 <b>Жалоба</b> — пожаловаться на объявление."
    )
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


# -------------------------
# RUN
# -------------------------

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
Если хочешь, я сейчас следующим сообщением соберу тебе вообще один полностью цельный файл от первой строки до последней, без разрывов.


Uploaded image
why its not deploying

It is not failing to deploy. It is stuck in queue.

In your screenshot, the newest deployments show:

“Deployment queued due to upstream GitHub issues”

That means Railway is waiting on GitHub’s side, not your bot code. Your last completed deploy above it says:

“Deployment successful”

So the situation is:

your previous version is still running

the new push has not started building yet

Railway is waiting because of a GitHub integration problem upstream

What to do:

Wait 5–15 minutes and refresh Railway.

Open the queued deployment and check whether the status changes from Queued to Building.

If it stays queued, try Redeploy manually from Railway.

If still stuck, disconnect and reconnect the GitHub repo in Railway.

Also check Railway status and GitHub status pages, because this is often a temporary platform-side issue.

If you want to verify whether the problem is only queue-related, open the latest queued item and compare:

if there are no build logs yet, it has not started

if build logs appear, then it moved past the GitHub queue issue and the next problem would be inside the code

From this screenshot, the main reason is: Railway cannot start the new deployment because of upstream GitHub issues.


does it mean i did somwthing wrong?

No — you didn’t do anything wrong. 👍

From your screenshot the key line is:

“Deployment queued due to upstream GitHub issues.”

That means:

Railway is waiting for GitHub

Your code is not the problem

The deployment has not even started building yet

Your bot is still running the previous successful version, which is this one in your screenshot:

“Deployment successful — Active.”

So everything is actually OK.

What usually causes this
This happens when:

GitHub API is slow

Railway can't fetch the new commit

Temporary GitHub outage

Too many builds in the queue

It normally fixes itself.

What you can do
Try these simple things:

1️⃣ Wait 5–10 minutes and refresh

Sometimes it starts automatically.

2️⃣ Click the three dots (⋮) → Redeploy

Force a new build.

3️⃣ If still queued

Push a tiny change to GitHub:

Example:

# redeploy
Then commit and push again.

Important
Your screenshot shows:

✅ Bot is ONLINE
✅ Last deployment succeeded
⏳ New deployment just waiting

So your code is not broken.

If you want, I can also show you one Railway setting that makes deployments 10× more stable for Telegram bots (almost nobody knows this trick).


Uploaded image
Uploaded image
нужно более детально прописать описание что пользователь может делать нажимая ев жти кнопки меню все - чтобы при нажатии в боте писао к примеру не только подписки на маршруты - а и описание что человек тут может делать - как для 6 летнего ребенка

Отличная идея. 👍
Чем проще и понятнее текст, тем больше людей реально будут пользоваться ботом. Нужно писать как будто для 6-летнего ребёнка: коротко, понятно, с примерами.

Я покажу тебе готовые тексты, которые можно вставить в код бота.

1️⃣ Найти совпадения 🔎
Сейчас у тебя просто:

Что ищем?

Лучше так:

🔎 Поиск совпадений

Здесь бот поможет найти людей,
которые летят по нужному маршруту
или хотят отправить посылку.

Например:

📦 Вы хотите отправить посылку из Китая в Россию.
Бот покажет людей, которые летят этим маршрутом.

✈️ Или вы летите и хотите взять посылку —
бот покажет людей, которым нужно отправить посылку.

👇 Выберите вариант:
Кнопки:

Ищу попутчика для посылки
Ищу посылку для попутчика
2️⃣ Подписки 🔔
Сейчас у тебя:

Подписки на маршруты

Лучше так:

🔔 Подписки на маршруты

Здесь можно настроить уведомления.

Например:

Вы ждёте посылку из Китая в Россию.

Вы можете подписаться на маршрут
Китай → Россия

И бот будет автоматически присылать
новые объявления по этому маршруту.

Это удобно — не нужно искать вручную.

👇 Что хотите сделать?
Кнопки:

🔔 Подписаться на маршрут
📋 Мои подписки
3️⃣ Статистика 📊
Сейчас сухо.

Лучше:

📊 Статистика сервиса

Здесь можно посмотреть,
как работает сервис.

Вы увидите:

👤 сколько людей пользуется ботом
📦 сколько посылок отправляют
✈️ сколько попутчиков летит
🔥 какой маршрут самый популярный

Это помогает понять,
насколько активно работает сервис.
4️⃣ Популярные маршруты 🔥
🔥 Популярные маршруты

Здесь показаны маршруты,
по которым чаще всего передают посылки.

Например:

Китай → Россия
Китай → Казахстан
Китай → Беларусь

Если вы летите по одному из них —
очень легко найти посылку
или попутчика.
5️⃣ Мои объявления 📋
📋 Мои объявления

Здесь находятся все ваши объявления.

Вы можете:

✏️ изменить объявление  
❌ удалить объявление  
📈 поднять объявление выше  
🔎 посмотреть совпадения  

Через этот раздел вы управляете
всеми своими объявлениями.
6️⃣ Мои сделки 🤝
🤝 Мои сделки

Здесь отображаются все сделки,
которые вы начали с другими людьми.

Например:

📦 вы отправляете посылку  
✈️ кто-то летит и берёт её  

Когда посылка передана —
оба человека подтверждают сделку.

После этого можно оставить отзыв ⭐
7️⃣ Взять посылку ✈️
✈️ Взять посылку

Нажмите эту кнопку,
если вы летите в другую страну
и готовы взять посылку с собой.

Вы создадите объявление,
в котором укажете:

🌍 откуда летите  
🌍 куда летите  
📅 дату поездки  
⚖️ сколько веса готовы взять  

После этого люди смогут
написать вам и договориться
о передаче посылки.
8️⃣ Отправить посылку 📦
📦 Отправить посылку

Нажмите эту кнопку,
если вам нужно передать посылку
в другую страну.

Вы создадите объявление
с маршрутом и описанием посылки.

После этого попутчики,
которые летят этим маршрутом,
смогут связаться с вами.
9️⃣ Помощь ℹ️
ℹ️ Как работает бот

1️⃣ Один человек летит
2️⃣ Другой хочет отправить посылку
3️⃣ Они находят друг друга
4️⃣ Договариваются о передаче

После передачи
оба подтверждают сделку
и оставляют отзыв.

Так формируется доверие.
💡 Очень важная фишка
В начале бота можно сделать очень понятный onboarding:

👋 Добро пожаловать!

Этот бот помогает
передавать посылки через попутчиков.

Пример:

📦 Вы хотите отправить посылку из Китая в Россию  
✈️ Кто-то летит этим маршрутом  

Вы находите друг друга
и договариваетесь.

👇 Выберите действие:
🚀 Если хочешь — я могу ещё сделать
1️⃣ идеальное стартовое сообщение /start
2️⃣ более красивое меню (UX как у крупных сервисов)
3️⃣ систему уровней доверия пользователей ⭐
4️⃣ автоматический поиск совпадений (очень мощная функция)

Это сделает бота в 3-5 раз удобнее.


как сейчас зайти в админку?

В твоём коде вход в админку сделан двумя способами.

1️⃣ Через команду
Самый простой способ.

Просто отправь боту:

/admin
Если твой Telegram ID есть в списке ADMIN_IDS, бот покажет админку.

Пример ответа:

Админка

На модерации: X
Жалоб: X

Команды:
/admin_pending
/admin_complaints
/admin_ban USER_ID
/admin_unban USER_ID
/admin_verify USER_ID
/admin_unverify USER_ID
2️⃣ Через кнопку меню
У тебя в коде есть:

👨‍💼 Админка
Но она работает только если пользователь админ.

В коде это проверяется так:

if not is_admin(message.from_user.id):
    await message.answer("Админка доступна только администраторам.")
3️⃣ Самое важное — ADMIN_IDS
В коде должен быть список админов:

ADMIN_IDS = [123456789]
Туда должен быть добавлен твой Telegram ID.

4️⃣ Как узнать свой Telegram ID
Напиши боту:

/start
или сделай временно в коде:

@router.message(Command("id"))
async def my_id(message: Message):
    await message.answer(f"Your ID: {message.from_user.id}")
5️⃣ Если админка не открывается
Значит твой ID не добавлен.

Нужно добавить:

ADMIN_IDS = [твoй_ID]
например:

ADMIN_IDS = [615234567]
После этого перезапусти бота.

⚡ Важный совет
Я рекомендую добавить красивую админку с кнопками, а не команды.

Например:

👨‍💼 Админка

📋 Объявления на модерации
⚠️ Жалобы
🚫 Забанить пользователя
✅ Проверенные пользователи
📊 Статистика
Это намного удобнее, особенно когда пользователей станет много.

Если хочешь — я покажу как сделать админку в 10 раз мощнее (как у маркетплейсов):

там будет:

управление пользователями

просмотр сделок

анти-скам система

быстрый бан

просмотр диалогов

Это очень усиливает доверие к сервису.


Uploaded image
сейчас также есть правка 2 и 3 . 2 - надо убрать кнопку не знаю 3.в моих обьявлениях если человек удаляет одно из них - нумерация не пересчитывется сейчас - например он удалил 4 - остались 1 3 5 + нумерация должна идти сверху вниз (1 вверху)

Да, это надо поправить.
Вот что именно изменить в коде.

1. Убрать кнопку «Не знаю» из веса
Найди функцию:

def weight_select_kb():
И замени её полностью на это:

def weight_select_kb():
    items = [(w, w) for w in POPULAR_WEIGHTS]
    rows = chunk_buttons(items, "weightpick", per_row=2)
    rows.append([InlineKeyboardButton(text=MANUAL_WEIGHT, callback_data="weightpick:__manual__")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
2. Чтобы в «Мои объявления» нумерация была красивой:
сверху вниз

1 вверху

после удаления пересчитывалась заново

Сейчас у тебя, скорее всего, в my_posts_handler и my_posts_kb используется id объявления как номер.
Из-за этого и получается 1 3 5.

Замени функцию my_posts_kb
Найди:

def my_posts_kb(posts: List[sqlite3.Row]):
И замени полностью на это:

def my_posts_kb(posts: List[sqlite3.Row]):
    rows = []
    total = len(posts)

    for index, p in enumerate(posts, start=1):
        type_icon = "✈️" if p["post_type"] == TYPE_TRIP else "📦"
        route = f"{p['from_country']}→{p['to_country']}"
        label = f"{index}. {type_icon} • {route} • {p['status']}"
        rows.append([
            InlineKeyboardButton(
                text=label[:64],
                callback_data=f"mypost:{p['id']}"
            )
        ])

    if not rows:
        rows = [[InlineKeyboardButton(text="Нет объявлений", callback_data="noop")]]

    return InlineKeyboardMarkup(inline_keyboard=rows)
И поправь сортировку в my_posts_handler
Найди этот кусок:

with closing(connect_db()) as conn:
    posts = conn.execute("""
        SELECT * FROM posts
        WHERE user_id=?
        ORDER BY id DESC
        LIMIT 30
    """, (message.from_user.id,)).fetchall()
И замени на:

with closing(connect_db()) as conn:
    posts = conn.execute("""
        SELECT * FROM posts
        WHERE user_id=?
        ORDER BY created_at ASC, id ASC
        LIMIT 30
    """, (message.from_user.id,)).fetchall()
Это даст:

старые объявления сверху

новые ниже

нумерация 1, 2, 3, 4 сверху вниз

3. Если хочешь, чтобы новые объявления были сверху, но всё равно шли как 1, 2, 3
Тогда используй не ASC, а DESC:

with closing(connect_db()) as conn:
    posts = conn.execute("""
        SELECT * FROM posts
        WHERE user_id=?
        ORDER BY created_at DESC, id DESC
        LIMIT 30
    """, (message.from_user.id,)).fetchall()
И тогда:

самое новое объявление будет наверху

оно будет номером 1

ниже 2, 3, 4

Обычно это удобнее.

4. Что лучше выбрать
Я бы советовал вот так:

ORDER BY created_at DESC, id DESC
Потому что пользователь обычно хочет видеть свои последние объявления сверху.

Готовый итог
weight_select_kb()
def weight_select_kb():
    items = [(w, w) for w in POPULAR_WEIGHTS]
    rows = chunk_buttons(items, "weightpick", per_row=2)
    rows.append([InlineKeyboardButton(text=MANUAL_WEIGHT, callback_data="weightpick:__manual__")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
my_posts_kb()
def my_posts_kb(posts: List[sqlite3.Row]):
    rows = []

    for index, p in enumerate(posts, start=1):
        type_icon = "✈️" if p["post_type"] == TYPE_TRIP else "📦"
        route = f"{p['from_country']}→{p['to_country']}"
        label = f"{index}. {type_icon} • {route} • {p['status']}"
        rows.append([
            InlineKeyboardButton(
                text=label[:64],
                callback_data=f"mypost:{p['id']}"
            )
        ])

    if not rows:
        rows = [[InlineKeyboardButton(text="Нет объявлений", callback_data="noop")]]

    return InlineKeyboardMarkup(inline_keyboard=rows)
my_posts_handler()
Вариант с новыми сверху:

@router.message(Command("my"))
@router.message(F.text == "📋 Мои объявления")
async def my_posts_handler(message: Message):
    upsert_user(message)
    with closing(connect_db()) as conn:
        posts = conn.execute("""
            SELECT * FROM posts
            WHERE user_id=?
            ORDER BY created_at DESC, id DESC
            LIMIT 30
        """, (message.from_user.id,)).fetchall()

    if not posts:
        await message.answer("У тебя пока нет объявлений.", reply_markup=main_menu(message.from_user.id))
        return

    await message.answer("Твои объявления:", reply_markup=my_posts_kb(posts))
Если хочешь, я могу сразу дать тебе готовый цельный блок кода для вставки, без поиска по файлу.


Почему сейчас кнопка Меню не содержит внутри всех кнопок которые бота есть в выпадаюшем списке?

Потому что это две разные вещи в Telegram.

У тебя сейчас есть:

1. Большая клавиатура внизу чата
Это ReplyKeyboardMarkup — именно она показывает кнопки вроде:

Взять посылку

Отправить посылку

Найти совпадения

Мои объявления

Мои сделки

Популярные маршруты

2. Кнопка “Меню” / выпадающий список команд
Это вообще не из ReplyKeyboardMarkup. Это список bot commands Telegram, то есть команды вида:

/start

/admin

/new_trip

/new_parcel

Если ты отдельно не задал все команды через BotFather или через код set_my_commands, Telegram сам не покажет там все кнопки из твоей нижней клавиатуры.

Почему так происходит
Кнопки снизу и меню — это разные механики:

нижняя клавиатура = ты рисуешь сам в коде

меню Telegram = отдельный список команд бота

Поэтому если у тебя внизу 10 кнопок, это не значит, что они автоматически появятся в кнопке “Меню”.

Как сделать, чтобы в “Меню” тоже были пункты
Нужно добавить команды бота.

Например, в main() после создания bot добавить:

from aiogram.types import BotCommand
И потом:

await bot.set_my_commands([
    BotCommand(command="start", description="Запустить бота"),
    BotCommand(command="new_trip", description="Взять посылку"),
    BotCommand(command="new_parcel", description="Отправить посылку"),
    BotCommand(command="find", description="Найти совпадения"),
    BotCommand(command="my", description="Мои объявления"),
    BotCommand(command="help", description="Помощь"),
    BotCommand(command="admin", description="Админка"),
])
Но есть важный момент
В выпадающее меню Telegram лучше добавлять только команды, а не все вообще кнопки подряд.

То есть туда хорошо идут:

/start

/new_trip

/new_parcel

/find

/my

/help

А такие вещи как:

Популярные маршруты

Статистика

Подписки

Мои сделки

лучше оставить в нижней клавиатуре, потому что это уже не “команды”, а обычные действия интерфейса.

Почему у тебя сейчас меню пустое или неполное
Скорее всего одна из причин:

1. Ты не задавал команды через set_my_commands
или

2. Ты задавал их раньше, но только часть
или

3. Ты настроил только клавиатуру внизу, а меню Telegram вообще отдельно не трогал

Что я советую
Сделай так:

внизу оставить все большие кнопки

в “Меню” добавить 5–7 главных команд

Это будет самый понятный вариант.

Если хочешь, я могу сразу дать тебе готовый блок кода для вставки в main(), чтобы кнопка “Меню” красиво заполнилась.


есть 3 правки - 1 Добавь описание при нажатие на кнопки меню
Убрать кнопку «Не знаю» из веса
Чтобы нумерация в моих объявлениях была красивой и новые были сверху.   напиши какой текст ты будешь добавлять в первой

Отлично. Я покажу точно какие тексты я предлагаю добавить, чтобы при нажатии на каждую кнопку меню бот сначала объяснял что происходит, а потом уже запускал функцию.

Текст будет очень простой, как ты просил — понятный даже ребёнку.

1️⃣ ✈️ Взять посылку
✈️ Взять посылку

Нажмите эту кнопку, если вы летите в другую страну
и готовы взять посылку с собой.

Как это работает:

1️⃣ Вы указываете откуда и куда летите  
2️⃣ Указываете дату поездки  
3️⃣ Указываете сколько веса можете взять  

После этого люди смогут написать вам
и передать посылку через вас.

Это помогает отправлять вещи быстрее и дешевле.
2️⃣ 📦 Отправить посылку
📦 Отправить посылку

Нажмите эту кнопку, если вам нужно отправить посылку
в другую страну через попутчика.

Вы создадите объявление и укажете:

📍 откуда отправляется посылка  
📍 куда нужно доставить  
📅 примерную дату  
⚖️ вес посылки  

После этого люди, которые летят этим маршрутом,
смогут связаться с вами.
3️⃣ 🔎 Найти совпадения
🔎 Найти совпадения

Бот поможет найти людей,
которые летят по нужному маршруту
или хотят отправить посылку.

Например:

📦 Вы хотите отправить посылку из Китая в Россию  
✈️ Бот покажет людей, которые летят этим маршрутом.

Или наоборот — если вы летите,
бот покажет людей, которым нужно отправить посылку.
4️⃣ 📋 Мои объявления
📋 Мои объявления

Здесь находятся все ваши объявления.

Вы можете:

✏️ посмотреть объявление  
🔎 найти совпадения  
❌ удалить объявление  
📈 поднять объявление выше  

Если вы удалите объявление —
оно исчезнет из поиска.
5️⃣ 🤝 Мои сделки
🤝 Мои сделки

Здесь отображаются сделки с другими пользователями.

Когда вы договорились о передаче посылки,
в этом разделе можно:

✅ подтвердить передачу  
❌ отметить что сделка не состоялась  
⭐ оставить отзыв пользователю  

Отзывы помогают другим людям
понимать кому можно доверять.
6️⃣ 🔥 Популярные маршруты
🔥 Популярные маршруты

Здесь показаны маршруты,
по которым чаще всего передают посылки.

Например:

Китай → Россия  
Китай → Казахстан  
Китай → Беларусь  

Если вы летите по одному из этих маршрутов,
вам будет легче найти посылку или попутчика.
7️⃣ 🔔 Подписки (если есть кнопка)
🔔 Подписки на маршруты

Здесь можно подписаться на маршрут.

Например:

Китай → Россия

Когда появится новое объявление
по этому маршруту — бот пришлёт вам уведомление.

Это удобно, если вы часто ждёте посылки
или регулярно летаете.
Почему это важно
Такие тексты:

сильно повышают доверие

люди не боятся нажимать кнопки

пользователи лучше понимают сервис

У маркетплейсов это называется onboarding UX.

Очень важный момент
Описание должно появляться ДО запуска действия.

Например:

Пользователь нажал:

📦 Отправить посылку
Бот сначала пишет описание
⬇
Потом начинает шаг 1.

Если хочешь — я могу еще показать одну очень мощную UX-фишку, которая увеличивает использование бота в 3-5 раз.


нажмите эту кнопку? Человек же видет это описание уже нажав на кнопку?

Ты абсолютно прав. 👍
Фраза «Нажмите эту кнопку» — логически неправильная, потому что пользователь уже нажал кнопку.

Лучше писать так, будто бот объясняет, что происходит дальше.

Я перепишу тексты правильно.

✈️ Взять посылку
Было плохо:

Нажмите эту кнопку...

Правильно:

✈️ Вы создаете объявление попутчика

Если вы летите в другую страну
и готовы взять посылку — здесь можно
создать объявление.

Бот попросит указать:

🌍 откуда вы летите  
🌍 куда летите  
📅 дату поездки  
⚖️ сколько веса можете взять

После этого люди смогут написать вам
и договориться о передаче посылки.
📦 Отправить посылку
📦 Создание объявления посылки

Здесь можно создать объявление
для отправки посылки через попутчика.

Бот попросит указать:

🌍 откуда отправляется посылка  
🌍 куда нужно доставить  
📅 примерную дату  
⚖️ вес посылки

После публикации попутчики,
которые летят этим маршрутом,
смогут связаться с вами.
🔎 Найти совпадения
🔎 Поиск совпадений

Бот поможет найти людей,
которые летят нужным маршрутом
или хотят отправить посылку.

Например:

📦 вы хотите отправить посылку
Китай → Россия

Бот покажет попутчиков,
которые летят этим маршрутом.
📋 Мои объявления
📋 Ваши объявления

Здесь находятся все объявления,
которые вы создали.

Вы можете:

🔎 посмотреть объявление  
✏️ изменить объявление  
❌ удалить объявление  
📈 поднять объявление выше  
🤝 посмотреть совпадения
🤝 Мои сделки
🤝 Ваши сделки

Здесь отображаются договоренности
с другими пользователями.

После передачи посылки
оба человека подтверждают сделку.

После этого можно оставить отзыв ⭐
🔥 Популярные маршруты
🔥 Популярные маршруты

Здесь показаны направления,
по которым чаще всего передают посылки.

Это помогает быстрее найти
попутчика или отправителя.
🔔 Подписки
🔔 Подписки на маршруты

Можно подписаться на нужный маршрут.

Когда появится новое объявление
по этому направлению —
бот пришлет уведомление.
Маленькая UX-фишка
Лучше начинать текст с того, что происходит сейчас:

Вы создаете объявление

Вы ищете совпадения

Здесь находятся ваши объявления

Здесь отображаются сделки

Так пользователю сразу понятно где он оказался.

Если хочешь, я могу еще показать 2 очень сильные UX-улучшения для твоего бота, которые делают его как настоящий маркетплейс.


ок- давй теперь мне готовый код всего бота с этими правками 


ок- давй теперь мне готовый код всего бота с этими правками - раздели его на 2 части  - чтобы ты не залагал 


Thought for 14s
Да — сделаем нормально. Ниже цельный код, но разделён на 2 части, чтобы ты мог спокойно копировать без лагов.

Как вставлять:

Создай файл bot.py

Вставь Часть 1

Сразу под ней вставь Часть 2

Сохрани и деплой

ЧАСТЬ 1 / 2
import asyncio
import html
import os
import re
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "/data/bot.db")

db_dir = os.path.dirname(DB_PATH)
if db_dir:
    os.makedirs(db_dir, exist_ok=True)

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "Poputchik_china_bot").lstrip("@")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

BUMP_PRICE_TEXT = os.getenv(
    "BUMP_PRICE_TEXT",
    "Поднятие объявления оплачивается вручную. После оплаты администратор поднимет ваше объявление выше в выдаче."
)

MAX_ACTIVE_POSTS_PER_USER = int(os.getenv("MAX_ACTIVE_POSTS_PER_USER", "5"))
MIN_SECONDS_BETWEEN_ACTIONS = int(os.getenv("MIN_SECONDS_BETWEEN_ACTIONS", "2"))
POST_TTL_DAYS = int(os.getenv("POST_TTL_DAYS", "14"))
COINCIDENCE_NOTIFY_LIMIT = int(os.getenv("COINCIDENCE_NOTIFY_LIMIT", "5"))
BUMP_PRICE_AMOUNT = int(os.getenv("BUMP_PRICE_AMOUNT", "10"))
BUMP_PRICE_CURRENCY = os.getenv("BUMP_PRICE_CURRENCY", "CNY")

router = Router()

TYPE_TRIP = "trip"
TYPE_PARCEL = "parcel"

STATUS_PENDING = "pending"
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"
STATUS_REJECTED = "rejected"
STATUS_EXPIRED = "expired"
STATUS_DELETED = "deleted"

DEAL_CONTACTED = "contacted"
DEAL_OFFERED = "offered"
DEAL_ACCEPTED = "accepted"
DEAL_COMPLETED_BY_OWNER = "completed_by_owner"
DEAL_COMPLETED_BY_REQUESTER = "completed_by_requester"
DEAL_COMPLETED = "completed"
DEAL_FAILED = "failed"
DEAL_CANCELLED = "cancelled"

MANUAL_COUNTRY = "🌍 Ввести другую страну"
MANUAL_CITY = "✏️ Ввести другой город"
MANUAL_WEIGHT = "✏️ Указать другой вес"
MANUAL_DATE = "✏️ Указать точную дату"
SKIP_VALUE = "-"

COUNTRY_OPTIONS = [
    ("🇨🇳 Китай", "Китай"),
    ("🇷🇺 Россия", "Россия"),
    ("🇺🇸 США", "США"),
    ("🇰🇿 Казахстан", "Казахстан"),
    ("🇺🇿 Узбекистан", "Узбекистан"),
    ("🇰🇬 Кыргызстан", "Кыргызстан"),
    ("🇹🇯 Таджикистан", "Таджикистан"),
    ("🇦🇿 Азербайджан", "Азербайджан"),
    ("🇦🇲 Армения", "Армения"),
    ("🇬🇪 Грузия", "Грузия"),
    ("🇧🇾 Беларусь", "Беларусь"),
    ("🇺🇦 Украина", "Украина"),
    ("🇲🇩 Молдова", "Молдова"),
    ("🇻🇳 Вьетнам", "Вьетнам"),
    ("🇹🇭 Таиланд", "Таиланд"),
]

COUNTRY_CITIES_RU = {
    "Китай": [
        "Шэньчжэнь", "Гуанчжоу", "Шанхай", "Пекин", "Ханчжоу",
        "Иу", "Гонконг", "Дунгуань", "Фошань", "Чжухай",
        "Сямынь", "Чэнду", "Чунцин", "Сучжоу", "Циндао",
        "Тяньцзинь", "Нинбо", "Ухань", "Нанкин", "Сиань"
    ],
    "Россия": [
        "Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург",
        "Казань", "Нижний Новгород", "Челябинск", "Самара",
        "Омск", "Ростов-на-Дону", "Уфа", "Красноярск",
        "Владивосток", "Хабаровск", "Иркутск"
    ],
    "США": [
        "Нью-Йорк", "Лос-Анджелес", "Чикаго", "Майами",
        "Хьюстон", "Сан-Франциско", "Лас-Вегас", "Орландо"
    ],
    "Казахстан": ["Алматы", "Астана", "Шымкент", "Караганда", "Актобе"],
    "Узбекистан": ["Ташкент", "Самарканд", "Бухара", "Наманган", "Андижан"],
    "Кыргызстан": ["Бишкек", "Ош", "Джалал-Абад", "Каракол"],
    "Таджикистан": ["Душанбе", "Худжанд", "Бохтар", "Куляб"],
    "Азербайджан": ["Баку", "Гянджа", "Сумгаит", "Ленкорань"],
    "Армения": ["Ереван", "Гюмри", "Ванадзор", "Абовян"],
    "Грузия": ["Тбилиси", "Батуми", "Кутаиси", "Рустави"],
    "Беларусь": ["Минск", "Гомель", "Гродно", "Брест", "Витебск"],
    "Украина": [
        "Киев", "Харьков", "Одесса", "Днепр", "Львов",
        "Запорожье", "Винница", "Ивано-Франковск"
    ],
    "Молдова": ["Кишинёв", "Бельцы", "Тирасполь", "Кагул"],
    "Вьетнам": ["Хошимин", "Ханой", "Дананг", "Хайфон", "Нячанг"],
    "Таиланд": ["Бангкок", "Паттайя", "Пхукет", "Чиангмай", "Самуи"],
}

POPULAR_WEIGHTS = [
    "0.5 кг", "1 кг", "2 кг", "3 кг",
    "5 кг", "10 кг", "20 кг", "Более 20 кг"
]

COUNTRY_ALIASES = {
    "китай": "Китай",
    "china": "Китай",
    "кнр": "Китай",
    "россия": "Россия",
    "russia": "Россия",
    "рф": "Россия",
    "сша": "США",
    "usa": "США",
    "united states": "США",
    "america": "США",
    "америка": "США",
    "казахстан": "Казахстан",
    "kazakhstan": "Казахстан",
    "узбекистан": "Узбекистан",
    "uzbekistan": "Узбекистан",
    "кыргызстан": "Кыргызстан",
    "киргизия": "Кыргызстан",
    "kyrgyzstan": "Кыргызстан",
    "таджикистан": "Таджикистан",
    "tajikistan": "Таджикистан",
    "азербайджан": "Азербайджан",
    "azerbaijan": "Азербайджан",
    "армения": "Армения",
    "armenia": "Армения",
    "грузия": "Грузия",
    "georgia": "Грузия",
    "беларусь": "Беларусь",
    "belarus": "Беларусь",
    "украина": "Украина",
    "ukraine": "Украина",
    "молдова": "Молдова",
    "moldova": "Молдова",
    "вьетнам": "Вьетнам",
    "vietnam": "Вьетнам",
    "таиланд": "Таиланд",
    "thailand": "Таиланд",
}

CITY_ALIASES = {
    "шэньчжэнь": "Шэньчжэнь",
    "шеньчжень": "Шэньчжэнь",
    "shen zhen": "Шэньчжэнь",
    "shenzhen": "Шэньчжэнь",
    "гуанчжоу": "Гуанчжоу",
    "guangzhou": "Гуанчжоу",
    "кантон": "Гуанчжоу",
    "шанхай": "Шанхай",
    "shanghai": "Шанхай",
    "пекин": "Пекин",
    "beijing": "Пекин",
    "иу": "Иу",
    "yiwu": "Иу",
    "гонконг": "Гонконг",
    "hong kong": "Гонконг",
    "hongkong": "Гонконг",
    "дунгуань": "Дунгуань",
    "dongguan": "Дунгуань",
    "фошань": "Фошань",
    "foshan": "Фошань",
    "чжухай": "Чжухай",
    "zhuhai": "Чжухай",
    "сямынь": "Сямынь",
    "xiamen": "Сямынь",
    "чэнду": "Чэнду",
    "chengdu": "Чэнду",
    "чунцин": "Чунцин",
    "chongqing": "Чунцин",
    "сучжоу": "Сучжоу",
    "suzhou": "Сучжоу",
    "циндао": "Циндао",
    "qingdao": "Циндао",
    "тяньцзинь": "Тяньцзинь",
    "tianjin": "Тяньцзинь",
    "нинбо": "Нинбо",
    "ningbo": "Нинбо",
    "москва": "Москва",
    "moscow": "Москва",
    "санкт-петербург": "Санкт-Петербург",
    "санкт петербург": "Санкт-Петербург",
    "питер": "Санкт-Петербург",
    "spb": "Санкт-Петербург",
    "saint petersburg": "Санкт-Петербург",
    "нью-йорк": "Нью-Йорк",
    "нью йорк": "Нью-Йорк",
    "new york": "Нью-Йорк",
    "лос-анджелес": "Лос-Анджелес",
    "лос анджелес": "Лос-Анджелес",
    "los angeles": "Лос-Анджелес",
    "чикаго": "Чикаго",
    "chicago": "Чикаго",
    "майами": "Майами",
    "miami": "Майами",
    "хьюстон": "Хьюстон",
    "houston": "Хьюстон",
    "сан-франциско": "Сан-Франциско",
    "сан франциско": "Сан-Франциско",
    "san francisco": "Сан-Франциско",
    "лас-вегас": "Лас-Вегас",
    "лас вегас": "Лас-Вегас",
    "las vegas": "Лас-Вегас",
    "орландо": "Орландо",
    "orlando": "Орландо",
    "алматы": "Алматы",
    "almaty": "Алматы",
    "астана": "Астана",
    "astana": "Астана",
    "ташкент": "Ташкент",
    "tashkent": "Ташкент",
    "бишкек": "Бишкек",
    "bishkek": "Бишкек",
    "киев": "Киев",
    "kyiv": "Киев",
    "одесса": "Одесса",
    "odesa": "Одесса",
    "львов": "Львов",
    "lviv": "Львов",
    "запорожье": "Запорожье",
    "zaporizhzhia": "Запорожье",
}

STEP_ORDER = [
    "from_country",
    "from_city",
    "to_country",
    "to_city",
    "travel_date",
    "weight",
    "description",
    "contact_note",
]
STEP_NUMBERS = {name: i + 1 for i, name in enumerate(STEP_ORDER)}

MENU_TEXTS = {
    "trip": (
        "✈️ <b>Вы создаете объявление попутчика</b>\n\n"
        "Если вы летите в другую страну и готовы взять посылку — здесь можно создать объявление.\n\n"
        "Бот попросит указать:\n"
        "🌍 откуда вы летите\n"
        "🌍 куда летите\n"
        "📅 дату поездки\n"
        "⚖️ сколько веса можете взять\n\n"
        "После этого люди смогут написать вам и договориться о передаче посылки."
    ),
    "parcel": (
        "📦 <b>Создание объявления посылки</b>\n\n"
        "Здесь можно создать объявление для отправки посылки через попутчика.\n\n"
        "Бот попросит указать:\n"
        "🌍 откуда отправляется посылка\n"
        "🌍 куда нужно доставить\n"
        "📅 примерную дату\n"
        "⚖️ вес посылки\n\n"
        "После публикации попутчики, которые летят этим маршрутом, смогут связаться с вами."
    ),
    "find": (
        "🔎 <b>Поиск совпадений</b>\n\n"
        "Бот поможет найти людей, которые летят нужным маршрутом или хотят отправить посылку.\n\n"
        "Например:\n"
        "📦 вы хотите отправить посылку Китай → Россия\n\n"
        "Бот покажет попутчиков, которые летят этим маршрутом."
    ),
    "my_posts": (
        "📋 <b>Ваши объявления</b>\n\n"
        "Здесь находятся все объявления, которые вы создали.\n\n"
        "Вы можете:\n"
        "🔎 посмотреть объявление\n"
        "❌ удалить объявление\n"
        "📈 поднять объявление выше\n"
        "🤝 посмотреть совпадения"
    ),
    "deals": (
        "🤝 <b>Ваши сделки</b>\n\n"
        "Здесь отображаются договоренности с другими пользователями.\n\n"
        "После передачи посылки оба человека подтверждают сделку.\n\n"
        "После этого можно оставить отзыв ⭐"
    ),
    "popular": (
        "🔥 <b>Популярные маршруты</b>\n\n"
        "Здесь показаны направления, по которым чаще всего передают посылки.\n\n"
        "Это помогает быстрее найти попутчика или отправителя."
    ),
    "subscriptions": (
        "🔔 <b>Подписки на маршруты</b>\n\n"
        "Можно подписаться на нужный маршрут.\n\n"
        "Когда появится новое объявление по этому направлению — бот пришлет уведомление."
    ),
    "stats": (
        "📊 <b>Статистика сервиса</b>\n\n"
        "Здесь можно посмотреть:\n"
        "👤 сколько людей пользуется ботом\n"
        "📦 сколько посылок сейчас в сервисе\n"
        "✈️ сколько попутчиков летит\n"
        "🔥 какой маршрут самый популярный"
    ),
}


# -------------------------
# SERVICE HELPERS
# -------------------------

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


def normalize_free_text(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.replace("ё", "е")
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_country_input(value: str) -> str:
    raw = normalize_free_text(value)
    return COUNTRY_ALIASES.get(raw, value.strip().title())


def normalize_city_input(value: str) -> str:
    raw = normalize_free_text(value)
    return CITY_ALIASES.get(raw, value.strip().title())


def parse_weight_kg(value: Optional[str]) -> Optional[float]:
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


def format_date_ru(dt: datetime) -> str:
    return dt.strftime("%d.%m.%Y")


def make_date_range_text(days: int) -> str:
    start = datetime.now()
    end = start + timedelta(days=days)
    return f"{format_date_ru(start)} - {format_date_ru(end)}"


def extract_travel_end_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    text = value.strip()
    exact_dt = parse_date_loose(text)
    if exact_dt:
        return datetime(exact_dt.year, exact_dt.month, exact_dt.day, 23, 59, 59)

    m = re.match(r"^\s*(\d{2}\.\d{2}\.\d{4})\s*[-–—]\s*(\d{2}\.\d{2}\.\d{4})\s*$", text)
    if m:
        end_dt = parse_date_loose(m.group(2))
        if end_dt:
            return datetime(end_dt.year, end_dt.month, end_dt.day, 23, 59, 59)

    return None


def calculate_post_expires_at(created_ts: int, travel_date_text: Optional[str], post_ttl_days: int = 14) -> int:
    ttl_expire = created_ts + days_to_seconds(post_ttl_days)
    end_dt = extract_travel_end_datetime(travel_date_text)
    if not end_dt:
        return ttl_expire
    return min(ttl_expire, int(end_dt.timestamp()))


def bot_link(start_param: Optional[str] = None) -> str:
    if start_param:
        return f"https://t.me/{BOT_USERNAME}?start={start_param}"
    return f"https://t.me/{BOT_USERNAME}"


def post_deeplink(post_id: int) -> str:
    return bot_link(f"post_{post_id}")


def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str):
    cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


# -------------------------
# DB INIT
# -------------------------

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

        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            requester_user_id INTEGER NOT NULL,
            initiator_user_id INTEGER,
            status TEXT NOT NULL DEFAULT 'contacted',
            owner_confirmed INTEGER DEFAULT 0,
            requester_confirmed INTEGER DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            completed_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS bump_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            post_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            currency TEXT NOT NULL DEFAULT 'CNY',
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            paid_at INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_posts_search
        ON posts(post_type, status, from_country, to_country, created_at);

        CREATE INDEX IF NOT EXISTS idx_posts_user
        ON posts(user_id, status, created_at);

        CREATE INDEX IF NOT EXISTS idx_subscriptions_search
        ON route_subscriptions(post_type, from_country, to_country);

        CREATE INDEX IF NOT EXISTS idx_reviews_user
        ON reviews(reviewed_user_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_deals_owner
        ON deals(owner_user_id, status, created_at);

        CREATE INDEX IF NOT EXISTS idx_deals_requester
        ON deals(requester_user_id, status, created_at);
        """)

        ensure_column(conn, "users", "is_verified", "is_verified INTEGER DEFAULT 0")
        ensure_column(conn, "users", "is_banned", "is_banned INTEGER DEFAULT 0")
        ensure_column(conn, "users", "last_action_at", "last_action_at INTEGER DEFAULT 0")
        ensure_column(conn, "posts", "expires_at", "expires_at INTEGER")
        ensure_column(conn, "deals", "initiator_user_id", "initiator_user_id INTEGER")
        ensure_column(conn, "deals", "owner_confirmed", "owner_confirmed INTEGER DEFAULT 0")
        ensure_column(conn, "deals", "requester_confirmed", "requester_confirmed INTEGER DEFAULT 0")
        ensure_column(conn, "deals", "updated_at", "updated_at INTEGER DEFAULT 0")

        conn.execute("UPDATE deals SET updated_at = created_at WHERE updated_at IS NULL OR updated_at = 0")
        conn.execute("UPDATE deals SET status='contacted' WHERE status='pending'")


# -------------------------
# USER / PROFILE HELPERS
# -------------------------

def upsert_user(message_or_callback):
    user = message_or_callback.from_user
    with closing(connect_db()) as conn, conn:
        existing = conn.execute(
            "SELECT created_at FROM users WHERE user_id=?",
            (user.id,)
        ).fetchone()
        created_at = int(existing["created_at"]) if existing and existing["created_at"] else now_ts()

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
            created_at
        ))


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def is_user_banned(user_id: int) -> bool:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT is_banned FROM users WHERE user_id=?", (user_id,)).fetchone()
        return bool(row and row["is_banned"])


def ban_user(user_id: int):
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (user_id,))
        conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE user_id=? AND status IN ('active','pending','inactive')",
            (STATUS_INACTIVE, now_ts(), user_id)
        )


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


def user_service_days(user_id: int) -> int:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT created_at FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not row or not row["created_at"]:
            return 0
        return max(0, (now_ts() - int(row["created_at"])) // 86400)


def user_service_text(user_id: int) -> str:
    days = user_service_days(user_id)
    if days < 30:
        return f"{days} дн"
    if days < 365:
        months = max(1, days // 30)
        return f"{months} мес"
    years = max(1, days // 365)
    return f"{years} г"


def user_completed_deals_count(user_id: int) -> int:
    with closing(connect_db()) as conn:
        row = conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM deals
            WHERE status='completed'
              AND (owner_user_id=? OR requester_user_id=?)
        """, (user_id, user_id)).fetchone()
        return int(row["cnt"] or 0)


def is_user_verified(user_id: int) -> bool:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT is_verified FROM users WHERE user_id=?", (user_id,)).fetchone()
        return bool(row and row["is_verified"])


def reviews_word(n: int) -> str:
    n = abs(n) % 100
    n1 = n % 10
    if 11 <= n <= 19:
        return "отзывов"
    if n1 == 1:
        return "отзыв"
    if 2 <= n1 <= 4:
        return "отзыва"
    return "отзывов"


def format_rating_line(user_id: int) -> Optional[str]:
    avg_rating, cnt = user_rating_summary(user_id)
    if cnt <= 0:
        return None
    stars = "⭐" * max(1, min(5, round(avg_rating)))
    return f"{stars} {avg_rating:.1f} ({cnt} {reviews_word(cnt)})"


def get_username_by_user_id(user_id: int) -> Optional[str]:
    with closing(connect_db()) as conn:
        row = conn.execute("SELECT username FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row["username"] if row and row["username"] else None


def format_user_ref(user_id: int) -> str:
    username = get_username_by_user_id(user_id)
    return f"@{html.escape(username)}" if username else f"USER_ID {user_id}"


def get_user_reviews(user_id: int, limit: int = 10):
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT r.rating, r.text, r.created_at, u.username, u.full_name
            FROM reviews r
            LEFT JOIN users u ON u.user_id = r.reviewer_user_id
            WHERE r.reviewed_user_id=?
            ORDER BY r.created_at DESC
            LIMIT ?
        """, (user_id, limit)).fetchall()


# -------------------------
# TEXT FORMATTING
# -------------------------

def short_post_type(post_type: str) -> str:
    return "✈️ Попутчик" if post_type == TYPE_TRIP else "📦 Посылка"


def format_deal_status(status: str) -> str:
    mapping = {
        DEAL_CONTACTED: "контакт начат",
        DEAL_OFFERED: "предложена",
        DEAL_ACCEPTED: "принята",
        DEAL_COMPLETED_BY_OWNER: "подтвердил владелец",
        DEAL_COMPLETED_BY_REQUESTER: "подтвердил откликнувшийся",
        DEAL_COMPLETED: "завершена",
        DEAL_FAILED: "не договорились",
        DEAL_CANCELLED: "отменена",
    }
    return mapping.get(status, status)


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


def form_header(post_type: str, step: int, total_steps: int = 8) -> str:
    title = "📦 Отправить посылку" if post_type == TYPE_PARCEL else "✈️ Взять посылку"
    return (
        f"{title}\n\n"
        f"━━━━━━━━━━━━━━\n"
        f"Шаг {step} / {total_steps}\n"
        f"━━━━━━━━━━━━━━\n\n"
    )


def form_text(post_type: str, step: int, prompt: str, total_steps: int = 8) -> str:
    return form_header(post_type, step, total_steps) + prompt


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
    completed_deals = user_completed_deals_count(owner_user_id)
    service_text = user_service_text(owner_user_id)

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

    lines.append("")
    lines.append("<b>👤 Профиль пользователя</b>")

    if rating_line:
        lines.append(f"⭐ <b>Рейтинг:</b> {rating_line}")
    else:
        lines.append("⭐ <b>Рейтинг:</b> пока нет отзывов")

    lines.append(f"📦 <b>Передач:</b> {completed_deals}")
    lines.append(f"📅 <b>В сервисе:</b> {service_text}")

    lines.append("")
    lines.append(f"<b>ID объявления:</b> {row['id']}")

    if for_channel:
        lines.append("Откройте бот, чтобы добавить своё объявление или найти совпадения.")
    else:
        owner = row["username"] if "username" in row.keys() else None
        if owner:
            lines.append(f"<b>Telegram:</b> @{html.escape(owner)}")

    return "\n".join(lines)


# -------------------------
# KEYBOARDS
# -------------------------

def chunk_buttons(items: List[tuple], prefix: str, per_row: int = 2):
    rows = []
    row = []
    for label, value in items:
        row.append(InlineKeyboardButton(text=label, callback_data=f"{prefix}:{value}"))
        if len(row) == per_row:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def with_back(rows: List[List[InlineKeyboardButton]], include_back: bool = True):
    if include_back:
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def back_only_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")]]
    )


def countries_kb(prefix: str):
    rows = chunk_buttons(COUNTRY_OPTIONS, prefix, per_row=2)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def countries_select_kb(prefix: str, include_back: bool = False):
    rows = chunk_buttons(COUNTRY_OPTIONS, prefix, per_row=2)
    rows.append([InlineKeyboardButton(text=MANUAL_COUNTRY, callback_data=f"{prefix}:__manual__")])
    return with_back(rows, include_back=include_back)


def cities_select_kb(prefix: str, country: str, include_back: bool = True):
    cities = COUNTRY_CITIES_RU.get(country, [])
    rows = []
    row = []
    for city in cities:
        row.append(InlineKeyboardButton(text=city, callback_data=f"{prefix}:{city}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text=MANUAL_CITY, callback_data=f"{prefix}:__manual__")])
    rows.append([InlineKeyboardButton(text="Не важно", callback_data=f"{prefix}:__skip__")])
    return with_back(rows, include_back=include_back)


def weight_select_kb():
    items = [(w, w) for w in POPULAR_WEIGHTS]
    rows = chunk_buttons(items, "weightpick", per_row=2)
    rows.append([InlineKeyboardButton(text=MANUAL_WEIGHT, callback_data="weightpick:__manual__")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def date_select_kb():
    rows = [
        [InlineKeyboardButton(text="В течение недели", callback_data="datepick:week")],
        [InlineKeyboardButton(text="В течение месяца", callback_data="datepick:month")],
        [InlineKeyboardButton(text="✏️ Указать точную дату", callback_data="datepick:manual")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def my_posts_kb(posts: List[sqlite3.Row]):
    rows = []
    for index, p in enumerate(posts, start=1):
        icon = "✈️" if p["post_type"] == TYPE_TRIP else "📦"
        label = f"{index}. {icon} • {p['from_country']}→{p['to_country']} • {p['status']}"
        rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"mypost:{p['id']}")])

    return InlineKeyboardMarkup(
        inline_keyboard=rows or [[InlineKeyboardButton(text="Нет объявлений", callback_data="noop")]]
    )


def deal_list_kb(deals: List[sqlite3.Row]):
    rows = []
    for d in deals:
        label = f"{d['id']} • post {d['post_id']} • {format_deal_status(d['status'])}"
        rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"mydeal:{d['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Нет сделок", callback_data="noop")]])


def post_actions_kb(post_id: int, status: str):
    share_url = f"https://t.me/share/url?url={post_deeplink(post_id)}"
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
            InlineKeyboardButton(text="📤 Поделиться", url=share_url)
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
    _, reviews_count = user_rating_summary(owner_id)

    rows = [
        [InlineKeyboardButton(text="✉️ Написать владельцу", callback_data=f"contact:{post_id}:{owner_id}")],
        [InlineKeyboardButton(text="🤝 Предложить сделку", callback_data=f"offer_deal:{post_id}:{owner_id}")]
    ]

    if reviews_count > 0:
        rows.append([
            InlineKeyboardButton(
                text=f"⭐ {reviews_count} {reviews_word(reviews_count)}",
                callback_data=f"user_reviews:{owner_id}"
            )
        ])

    rows.append([InlineKeyboardButton(text="⚠️ Пожаловаться", callback_data=f"complain:{post_id}")])
    rows.append([InlineKeyboardButton(text="📤 Поделиться", url=f"https://t.me/share/url?url={post_deeplink(post_id)}")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_post_kb(post_id: int, post_type: Optional[str] = None):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✉️ Связаться с владельцем", url=bot_link(f"contact_{post_id}"))],
            [InlineKeyboardButton(text="🤝 Открыть объявление", url=post_deeplink(post_id))],
            [InlineKeyboardButton(text="📤 Поделиться", url=f"https://t.me/share/url?url={post_deeplink(post_id)}")]
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
            InlineKeyboardButton(text=label[:64], callback_data=f"popular:{row['from_country']}:{row['to_country']}")
        ])
    return InlineKeyboardMarkup(
        inline_keyboard=buttons or [[InlineKeyboardButton(text="Пока пусто", callback_data="noop")]]
    )


def deal_open_kb(deal: sqlite3.Row, user_id: int) -> InlineKeyboardMarkup:
    rows = []

    if deal["status"] in (DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
        rows.append([InlineKeyboardButton(text="✅ Подтвердить завершение", callback_data=f"deal_confirm:{deal['id']}")])

    if deal["status"] == DEAL_COMPLETED:
        rows.append([InlineKeyboardButton(text="⭐ Оставить отзыв", callback_data=f"deal_review:{deal['id']}")])

    if deal["status"] in (DEAL_CONTACTED, DEAL_OFFERED, DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
        rows.append([InlineKeyboardButton(text="❌ Не договорились", callback_data=f"deal_fail_direct:{deal['id']}")])

    if not rows:
        rows = [[InlineKeyboardButton(text="Ок", callback_data="noop")]]

    return InlineKeyboardMarkup(inline_keyboard=rows)


def main_menu(user_id: Optional[int] = None):
    keyboard = [
        [KeyboardButton(text="✈️ Взять посылку"), KeyboardButton(text="📦 Отправить посылку")],
        [KeyboardButton(text="🔎 Найти совпадения"), KeyboardButton(text="📋 Мои объявления")],
        [KeyboardButton(text="🤝 Мои сделки"), KeyboardButton(text="🔥 Популярные маршруты")],
        [KeyboardButton(text="🆕 Новые объявления"), KeyboardButton(text="🔔 Подписки")],
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="💰 Поднять объявление")],
        [KeyboardButton(text="🆘 Жалоба"), KeyboardButton(text="ℹ️ Помощь")],
    ]
    if user_id is not None and is_admin(user_id):
        keyboard.append([KeyboardButton(text="👨‍💼 Админка")])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


# -------------------------
# FSM STATES
# -------------------------

class CreatePost(StatesGroup):
    from_country = State()
    from_country_manual = State()
    from_city = State()
    from_city_manual = State()
    to_country = State()
    to_country_manual = State()
    to_city = State()
    to_city_manual = State()
    travel_date = State()
    travel_date_manual = State()
    weight = State()
    weight_manual = State()
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


# -------------------------
# CREATE FLOW HELPERS
# -------------------------

async def render_create_step(target_step: str, target_message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if target_step == "from_country":
        await state.set_state(CreatePost.from_country)
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], "Выберите страну отправления"),
            reply_markup=countries_select_kb("from_country_pick", include_back=False)
        )
        return

    if target_step == "from_city":
        await state.set_state(CreatePost.from_city)
        country = data.get("from_country", "")
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], f"Выберите город отправления в стране {country}"),
            reply_markup=cities_select_kb("from_city_pick", country, include_back=True)
        )
        return

    if target_step == "to_country":
        await state.set_state(CreatePost.to_country)
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], "Выберите страну назначения"),
            reply_markup=countries_select_kb("to_country_pick", include_back=True)
        )
        return

    if target_step == "to_city":
        await state.set_state(CreatePost.to_city)
        country = data.get("to_country", "")
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], f"Выберите город назначения в стране {country}"),
            reply_markup=cities_select_kb("to_city_pick", country, include_back=True)
        )
        return

    if target_step == "travel_date":
        await state.set_state(CreatePost.travel_date)
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], "Выберите дату поездки / отправки"),
            reply_markup=date_select_kb()
        )
        return

    if target_step == "weight":
        await state.set_state(CreatePost.weight)
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], "Выберите вес или объём"),
            reply_markup=weight_select_kb()
        )
        return

    if target_step == "description":
        await state.set_state(CreatePost.description)
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], "Опишите объявление подробно\nЧто нужно передать / сколько места есть / условия"),
            reply_markup=back_only_kb()
        )
        return

    if target_step == "contact_note":
        await state.set_state(CreatePost.contact_note)
        await target_message.answer(
            form_text(
                post_type,
                STEP_NUMBERS[target_step],
                "Введите дополнительный контакт или примечание\nНапример: WeChat ID / только текст / без звонков\nЕсли не нужно — напишите -"
            ),
            reply_markup=back_only_kb()
        )
        return


def get_current_create_step_name(state_name: Optional[str]) -> Optional[str]:
    if not state_name:
        return None

    mapping = {
        CreatePost.from_country.state: "from_country",
        CreatePost.from_country_manual.state: "from_country",
        CreatePost.from_city.state: "from_city",
        CreatePost.from_city_manual.state: "from_city",
        CreatePost.to_country.state: "to_country",
        CreatePost.to_country_manual.state: "to_country",
        CreatePost.to_city.state: "to_city",
        CreatePost.to_city_manual.state: "to_city",
        CreatePost.travel_date.state: "travel_date",
        CreatePost.travel_date_manual.state: "travel_date",
        CreatePost.weight.state: "weight",
        CreatePost.weight_manual.state: "weight",
        CreatePost.description.state: "description",
        CreatePost.contact_note.state: "contact_note",
    }
    return mapping.get(state_name)


CREATE_STEP_CLEANUP_KEYS = {
    "from_country": ["from_country", "from_city", "to_country", "to_city", "travel_date", "weight_kg", "description", "contact_note"],
    "from_city": ["from_city", "to_country", "to_city", "travel_date", "weight_kg", "description", "contact_note"],
    "to_country": ["to_country", "to_city", "travel_date", "weight_kg", "description", "contact_note"],
    "to_city": ["to_city", "travel_date", "weight_kg", "description", "contact_note"],
    "travel_date": ["travel_date", "description", "contact_note"],
    "weight": ["weight_kg", "description", "contact_note"],
    "description": ["description", "contact_note"],
    "contact_note": ["contact_note"],
}


async def clear_step_data_from(state: FSMContext, target_step: str):
    data = await state.get_data()
    for key in CREATE_STEP_CLEANUP_KEYS.get(target_step, []):
        data.pop(key, None)
    await state.set_data(data)


# -------------------------
# POST / DEAL HELPERS
# -------------------------

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
            SELECT from_country, to_country, COUNT(*) AS cnt
            FROM posts
            WHERE status='active'
              AND (expires_at IS NULL OR expires_at > ?)
            GROUP BY from_country, to_country
            ORDER BY cnt DESC, MAX(COALESCE(bumped_at, created_at)) DESC
            LIMIT ?
        """, (now_ts(), limit)).fetchall()


def search_route_posts_all(from_country: str, to_country: str, limit: int = 20) -> List[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.from_country=?
              AND p.to_country=?
              AND p.status='active'
              AND (p.expires_at IS NULL OR p.expires_at > ?)
            ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
            LIMIT ?
        """, (from_country, to_country, now_ts(), limit)).fetchall()


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


def create_post_record(data: dict, user_id: int) -> int:
    ts = now_ts()
    expires_at = calculate_post_expires_at(ts, data.get("travel_date"), POST_TTL_DAYS)

    with closing(connect_db()) as conn, conn:
        cur = conn.execute("""
            INSERT INTO posts (
                user_id, post_type, from_country, from_city, to_country, to_city,
                travel_date, weight_kg, description, contact_note, status,
                is_anonymous_contact, channel_message_id, created_at, updated_at, bumped_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
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
        return int(cur.lastrowid)


def add_route_subscription(user_id: int, post_type: str, from_country: str, to_country: str):
    with closing(connect_db()) as conn, conn:
        exists = conn.execute("""
            SELECT id FROM route_subscriptions
            WHERE user_id=? AND post_type=? AND from_country=? AND to_country=?
            LIMIT 1
        """, (user_id, post_type, from_country, to_country)).fetchone()
        if exists:
            return
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


def delete_subscription(user_id: int, sub_id: int) -> bool:
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("DELETE FROM route_subscriptions WHERE id=? AND user_id=?", (sub_id, user_id))
        return cur.rowcount > 0


def reserve_coincidence_notification(post_a_id: int, post_b_id: int) -> bool:
    a, b = sorted([post_a_id, post_b_id])
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("""
            INSERT OR IGNORE INTO coincidence_notifications (post_a_id, post_b_id, created_at)
            VALUES (?, ?, ?)
        """, (a, b, now_ts()))
        return cur.rowcount > 0


def calculate_coincidence_score(source_row, candidate_row: sqlite3.Row) -> Tuple[int, List[str]]:
    score = 40
    notes: List[str] = []

    source_from_city = normalize_text(source_row["from_city"])
    candidate_from_city = normalize_text(candidate_row["from_city"])
    source_to_city = normalize_text(source_row["to_city"])
    candidate_to_city = normalize_text(candidate_row["to_city"])

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

    source_date = extract_travel_end_datetime(source_row["travel_date"])
    candidate_date = extract_travel_end_datetime(candidate_row["travel_date"])
    if source_date and candidate_date:
        days_diff = abs((source_date.date() - candidate_date.date()).days)
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

    source_weight = parse_weight_kg(source_row["weight_kg"])
    candidate_weight = parse_weight_kg(candidate_row["weight_kg"])

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
ЧАСТЬ 2 / 2
def get_coincidences(
    post_type: str,
    from_country: str,
    to_country: str,
    exclude_user_id: Optional[int] = None,
    source_row=None,
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

        coincidence_type = "strong" if score >= 75 else "good" if score >= 55 else "possible"
        results.append({
            "row": row,
            "score": score,
            "notes": notes,
            "type": coincidence_type
        })

    results.sort(key=lambda x: (x["score"], x["row"]["bumped_at"] or x["row"]["created_at"]), reverse=True)
    return results[:limit]


def search_posts_inline(query: str, limit: int = 10) -> List[sqlite3.Row]:
    q = f"%{query.strip().lower()}%"
    with closing(connect_db()) as conn:
        if query.strip():
            return conn.execute("""
                SELECT p.*, u.username, u.full_name
                FROM posts p
                LEFT JOIN users u ON u.user_id = p.user_id
                WHERE p.status='active'
                  AND (p.expires_at IS NULL OR p.expires_at > ?)
                  AND (
                        lower(p.from_country) LIKE ?
                     OR lower(COALESCE(p.from_city, '')) LIKE ?
                     OR lower(p.to_country) LIKE ?
                     OR lower(COALESCE(p.to_city, '')) LIKE ?
                     OR lower(COALESCE(p.description, '')) LIKE ?
                     OR lower(COALESCE(p.travel_date, '')) LIKE ?
                  )
                ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
                LIMIT ?
            """, (now_ts(), q, q, q, q, q, q, limit)).fetchall()

        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.status='active'
              AND (p.expires_at IS NULL OR p.expires_at > ?)
            ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
            LIMIT ?
        """, (now_ts(), limit)).fetchall()


def ensure_deal(post_id: int, owner_user_id: int, requester_user_id: int, initiator_user_id: int) -> int:
    with closing(connect_db()) as conn, conn:
        row = conn.execute("""
            SELECT id
            FROM deals
            WHERE post_id=? AND owner_user_id=? AND requester_user_id=?
            ORDER BY id DESC
            LIMIT 1
        """, (post_id, owner_user_id, requester_user_id)).fetchone()

        if row:
            return int(row["id"])

        cur = conn.execute("""
            INSERT INTO deals (
                post_id,
                owner_user_id,
                requester_user_id,
                initiator_user_id,
                status,
                owner_confirmed,
                requester_confirmed,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
        """, (
            post_id,
            owner_user_id,
            requester_user_id,
            initiator_user_id,
            DEAL_CONTACTED,
            now_ts(),
            now_ts(),
        ))
        return int(cur.lastrowid)


def get_deal(deal_id: int) -> Optional[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()


def list_user_deals(user_id: int) -> List[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT *
            FROM deals
            WHERE owner_user_id=? OR requester_user_id=?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 30
        """, (user_id, user_id)).fetchall()


def mark_deal_failed(post_id: int, user_id: int) -> bool:
    with closing(connect_db()) as conn, conn:
        row = conn.execute("""
            SELECT id
            FROM deals
            WHERE post_id=?
              AND (owner_user_id=? OR requester_user_id=?)
              AND status IN (?, ?, ?, ?, ?)
            ORDER BY id DESC
            LIMIT 1
        """, (
            post_id, user_id, user_id,
            DEAL_CONTACTED, DEAL_OFFERED, DEAL_ACCEPTED,
            DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER
        )).fetchone()

        if not row:
            return False

        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), row["id"]))
        return True


def create_bump_order(user_id: int, post_id: int, amount: int = BUMP_PRICE_AMOUNT, currency: str = BUMP_PRICE_CURRENCY) -> int:
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("""
            INSERT INTO bump_orders (user_id, post_id, amount, currency, status, created_at)
            VALUES (?, ?, ?, ?, 'pending', ?)
        """, (user_id, post_id, amount, currency, now_ts()))
        return int(cur.lastrowid)


# -------------------------
# CHANNEL / NOTIFY HELPERS
# -------------------------

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

    for item in coincidences:
        row = item["row"]
        score = item["score"]
        notes = item["notes"]

        if not reserve_coincidence_notification(new_row["id"], row["id"]):
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
                "🔔 По вашей подписке появилось новое объявление:\n\n" + post_text(row),
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

                if not reserve_coincidence_notification(row["id"], target["id"]):
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

    except Exception as e:
        print(f"GLOBAL COINCIDENCE SCAN ERROR: {e}")


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


# -------------------------
# FLOW HELPERS
# -------------------------

async def begin_create(message: Message, state: FSMContext, post_type: str):
    upsert_user(message)

    if is_user_banned(message.from_user.id):
        await message.answer(
            "⛔ Ваш аккаунт ограничен. Если это ошибка — свяжитесь с администратором.",
            reply_markup=main_menu(message.from_user.id)
        )
        return

    spam_error = anti_spam_check(message.from_user.id)
    if spam_error:
        await message.answer(spam_error, reply_markup=main_menu(message.from_user.id))
        return

    if active_post_count(message.from_user.id) >= MAX_ACTIVE_POSTS_PER_USER:
        await message.answer(
            f"У вас уже слишком много объявлений. Лимит: {MAX_ACTIVE_POSTS_PER_USER}. "
            "Удалите или деактивируйте старые объявления.",
            reply_markup=main_menu(message.from_user.id)
        )
        return

    await state.clear()
    await state.update_data(post_type=post_type)
    await state.set_state(CreatePost.from_country)

    await message.answer(
        form_text(post_type, 1, "Выберите страну отправления"),
        reply_markup=countries_select_kb("from_country_pick", include_back=False)
    )


def owner_only(callback: CallbackQuery, post_id: int) -> Optional[sqlite3.Row]:
    row = get_post(post_id)
    if not row or row["user_id"] != callback.from_user.id:
        return None
    return row


# -------------------------
# HANDLERS
# -------------------------

@router.inline_query()
async def inline_search_handler(inline_query: InlineQuery):
    query = (inline_query.query or "").strip()
    rows = search_posts_inline(query, limit=10)
    results = []

    for row in rows:
        title = f"{'✈️' if row['post_type'] == TYPE_TRIP else '📦'} {row['from_country']} → {row['to_country']}"
        if row["from_city"] or row["to_city"]:
            from_part = row["from_city"] or row["from_country"]
            to_part = row["to_city"] or row["to_country"]
            title = f"{'✈️' if row['post_type'] == TYPE_TRIP else '📦'} {from_part} → {to_part}"

        description_parts = []
        if row["travel_date"]:
            description_parts.append(f"Дата: {row['travel_date']}")
        if row["weight_kg"]:
            description_parts.append(f"Вес: {row['weight_kg']}")
        if row["description"]:
            description_parts.append(row["description"][:80])

        description = " | ".join(description_parts)[:200] or "Открыть объявление"
        text = f"{post_text(row)}\n\n🤖 Открыть в боте: {post_deeplink(row['id'])}"

        results.append(
            InlineQueryResultArticle(
                id=str(row["id"]),
                title=title[:256],
                description=description,
                input_message_content=InputTextMessageContent(
                    message_text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                ),
                reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"]),
            )
        )

    if not results:
        results = [
            InlineQueryResultArticle(
                id="no_results",
                title="Ничего не найдено",
                description="Попробуйте: Китай Россия, Шэньчжэнь Москва, посылка, попутчик",
                input_message_content=InputTextMessageContent(
                    message_text=f"Ничего не найдено.\n\nОткрой бота и создай объявление: {bot_link()}",
                    disable_web_page_preview=True,
                ),
            )
        ]

    await inline_query.answer(results, cache_time=1, is_personal=True)


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    upsert_user(message)
    await state.clear()

    if is_user_banned(message.from_user.id):
        await message.answer(
            "⛔ Ваш аккаунт ограничен из-за жалоб пользователей.\n"
            "Если это ошибка — свяжитесь с администратором."
        )
        return

    start_arg = ""
    if message.text and " " in message.text:
        start_arg = message.text.split(" ", 1)[1].strip()

    if start_arg == "parcel":
        await message.answer(MENU_TEXTS["parcel"], reply_markup=main_menu(message.from_user.id))
        await begin_create(message, state, TYPE_PARCEL)
        return

    if start_arg == "trip":
        await message.answer(MENU_TEXTS["trip"], reply_markup=main_menu(message.from_user.id))
        await begin_create(message, state, TYPE_TRIP)
        return

    if start_arg.startswith("contact_"):
        post_id_str = start_arg.replace("contact_", "", 1)
        if post_id_str.isdigit():
            row = get_post(int(post_id_str))
            if row and row["status"] == STATUS_ACTIVE:
                if row["user_id"] == message.from_user.id:
                    await message.answer("Это ваше объявление.", reply_markup=main_menu(message.from_user.id))
                    return

                deal_id = ensure_deal(
                    post_id=row["id"],
                    owner_user_id=row["user_id"],
                    requester_user_id=message.from_user.id,
                    initiator_user_id=message.from_user.id
                )

                await state.set_state(ContactFlow.message_text)
                await state.update_data(post_id=row["id"], target_user_id=row["user_id"], deal_id=deal_id)

                await message.answer(
                    "✉️ Вы открыли связь с владельцем объявления:\n\n"
                    f"{post_text(row)}\n\n"
                    "Напишите сообщение, и я перешлю его владельцу."
                )
            else:
                await message.answer("Объявление не найдено или уже неактивно.")
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

    text = (
        "👋 <b>Привет.</b>\n\n"
        "Это <b>Попутчик Китай</b> — бот для передачи посылок через попутчиков.\n\n"
        "<b>Здесь можно отправить свою посылку или взять чужую по маршруту.</b>\n\n"
        "🔎 <b>Подпишись на канал с объявлениями:</b>\n"
        "t.me/china_poputchik\n\n"
        "Я сам ищу совпадения и уведомляю о них.\n\n"
        "<b>Нажми нужную кнопку в меню и заполни заявку.</b>\n\n"
        "⬇️ <b>Выберите действие в меню ниже</b>"
    )

    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.message(StateFilter("*"), Command("new_trip"))
@router.message(StateFilter("*"), F.text == "✈️ Взять посылку")
async def add_trip(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(MENU_TEXTS["trip"], reply_markup=main_menu(message.from_user.id))
    await begin_create(message, state, TYPE_TRIP)


@router.message(StateFilter("*"), Command("new_parcel"))
@router.message(StateFilter("*"), F.text == "📦 Отправить посылку")
async def add_parcel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(MENU_TEXTS["parcel"], reply_markup=main_menu(message.from_user.id))
    await begin_create(message, state, TYPE_PARCEL)


@router.callback_query(F.data == "create_back")
async def create_back_handler(callback: CallbackQuery, state: FSMContext):
    current_state = await state.get_state()
    step_name = get_current_create_step_name(current_state)

    if not step_name:
        await callback.answer("Назад недоступно", show_alert=True)
        return

    idx = STEP_ORDER.index(step_name)
    if idx == 0:
        await callback.answer("Это первый шаг", show_alert=True)
        return

    prev_step = STEP_ORDER[idx - 1]
    await clear_step_data_from(state, prev_step)
    await render_create_step(prev_step, callback.message, state)
    await callback.answer()


@router.callback_query(F.data.startswith("from_country_pick:"))
async def pick_from_country(callback: CallbackQuery, state: FSMContext):
    upsert_user(callback)
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.from_country_manual)
        await callback.message.answer(
            form_text(post_type, 1, "Введите страну отправления вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(from_country=value)
    await state.set_state(CreatePost.from_city)
    await callback.message.answer(
        form_text(post_type, 2, f"Выберите город отправления в стране {value}"),
        reply_markup=cities_select_kb("from_city_pick", value, include_back=True)
    )
    await callback.answer()


@router.message(CreatePost.from_country_manual)
async def from_country_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
    value = normalize_country_input(message.text.strip()[:80])

    await state.update_data(from_country=value)
    await state.set_state(CreatePost.from_city)
    await message.answer(
        form_text(post_type, 2, f"Выберите город отправления в стране {value}"),
        reply_markup=cities_select_kb("from_city_pick", value, include_back=True)
    )


@router.callback_query(F.data.startswith("from_city_pick:"))
async def pick_from_city(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.from_city_manual)
        await callback.message.answer(
            form_text(post_type, 2, "Введите город отправления вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(from_city=None if value == "__skip__" else value)
    await state.set_state(CreatePost.to_country)
    await callback.message.answer(
        form_text(post_type, 3, "Выберите страну назначения"),
        reply_markup=countries_select_kb("to_country_pick", include_back=True)
    )
    await callback.answer()


@router.message(CreatePost.from_city_manual)
async def from_city_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
    value = message.text.strip()

    await state.update_data(from_city=None if value == SKIP_VALUE else normalize_city_input(value[:80]))
    await state.set_state(CreatePost.to_country)
    await message.answer(
        form_text(post_type, 3, "Выберите страну назначения"),
        reply_markup=countries_select_kb("to_country_pick", include_back=True)
    )


@router.callback_query(F.data.startswith("to_country_pick:"))
async def pick_to_country(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.to_country_manual)
        await callback.message.answer(
            form_text(post_type, 3, "Введите страну назначения вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(to_country=value)
    await state.set_state(CreatePost.to_city)
    await callback.message.answer(
        form_text(post_type, 4, f"Выберите город назначения в стране {value}"),
        reply_markup=cities_select_kb("to_city_pick", value, include_back=True)
    )
    await callback.answer()


@router.message(CreatePost.to_country_manual)
async def to_country_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
    value = normalize_country_input(message.text.strip()[:80])

    await state.update_data(to_country=value)
    await state.set_state(CreatePost.to_city)
    await message.answer(
        form_text(post_type, 4, f"Выберите город назначения в стране {value}"),
        reply_markup=cities_select_kb("to_city_pick", value, include_back=True)
    )


@router.callback_query(F.data.startswith("to_city_pick:"))
async def pick_to_city(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.to_city_manual)
        await callback.message.answer(
            form_text(post_type, 4, "Введите город назначения вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(to_city=None if value == "__skip__" else value)
    await state.set_state(CreatePost.travel_date)
    await callback.message.answer(
        form_text(post_type, 5, "Выберите дату поездки / отправки"),
        reply_markup=date_select_kb()
    )
    await callback.answer()


@router.message(CreatePost.to_city_manual)
async def to_city_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
    value = message.text.strip()

    await state.update_data(to_city=None if value == SKIP_VALUE else normalize_city_input(value[:80]))
    await state.set_state(CreatePost.travel_date)
    await message.answer(
        form_text(post_type, 5, "Выберите дату поездки / отправки"),
        reply_markup=date_select_kb()
    )


@router.callback_query(F.data.startswith("datepick:"))
async def pick_date(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "manual":
        await state.set_state(CreatePost.travel_date_manual)
        await callback.message.answer(
            form_text(post_type, 5, "Введите точную дату\nНапример: 15.03.2026"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    if value == "week":
        travel_date = make_date_range_text(7)
    elif value == "month":
        travel_date = make_date_range_text(30)
    else:
        await callback.answer("Неверный выбор", show_alert=True)
        return

    await state.update_data(travel_date=travel_date)
    await state.set_state(CreatePost.weight)
    await callback.message.answer(
        form_text(post_type, 6, "Выберите вес или объём"),
        reply_markup=weight_select_kb()
    )
    await callback.answer()


@router.message(CreatePost.travel_date_manual)
async def date_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    raw = message.text.strip()[:100]
    parsed = parse_date_loose(raw)
    if not parsed:
        await message.answer(
            "Не смог распознать дату.\nВведите в формате: 15.03.2026",
            reply_markup=back_only_kb()
        )
        return

    await state.update_data(travel_date=format_date_ru(parsed))
    await state.set_state(CreatePost.weight)
    await message.answer(
        form_text(post_type, 6, "Выберите вес или объём"),
        reply_markup=weight_select_kb()
    )


@router.callback_query(F.data.startswith("weightpick:"))
async def pick_weight(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if value == "__manual__":
        await state.set_state(CreatePost.weight_manual)
        await callback.message.answer(
            form_text(post_type, 6, "Введите свой вес / объём\nНапример: 7 кг"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(weight_kg=value)
    await state.set_state(CreatePost.description)
    await callback.message.answer(
        form_text(post_type, 7, "Опишите объявление подробно\nЧто нужно передать / сколько места есть / условия"),
        reply_markup=back_only_kb()
    )
    await callback.answer()


@router.message(CreatePost.weight_manual)
async def weight_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
    value = message.text.strip()

    if not value:
        await message.answer("Введите вес или объём.", reply_markup=back_only_kb())
        return

    await state.update_data(weight_kg=value[:50])
    await state.set_state(CreatePost.description)
    await message.answer(
        form_text(post_type, 7, "Опишите объявление подробно\nЧто нужно передать / сколько места есть / условия"),
        reply_markup=back_only_kb()
    )


@router.message(CreatePost.description)
async def enter_description(message: Message, state: FSMContext):
    desc = (message.text or "").strip()
    if len(desc) < 3:
        await message.answer("Описание слишком короткое. Напишите подробнее.", reply_markup=back_only_kb())
        return

    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    await state.update_data(description=desc[:1000])
    await state.set_state(CreatePost.contact_note)
    await message.answer(
        form_text(post_type, 8, "Введите дополнительный контакт или примечание\nНапример: WeChat ID / только текст / без звонков\nЕсли не нужно — напишите -"),
        reply_markup=back_only_kb()
    )


@router.message(CreatePost.contact_note)
async def finalize_post(message: Message, state: FSMContext, bot: Bot):
    try:
        upsert_user(message)

        if is_user_banned(message.from_user.id):
            await message.answer("⛔ Ваш аккаунт ограничен.", reply_markup=main_menu(message.from_user.id))
            await state.clear()
            return

        data = await state.get_data()
        data["contact_note"] = None if message.text.strip() == "-" else message.text.strip()[:200]

        post_id = create_post_record(data, message.from_user.id)
        row = get_post(post_id)

        await state.clear()

        if not row:
            await message.answer("Ошибка: объявление создалось некорректно. Попробуйте ещё раз.", reply_markup=main_menu(message.from_user.id))
            return

        await message.answer(
            "✅ Объявление создано.\n" + ("Оно отправлено на модерацию." if ADMIN_IDS else "Оно уже активно."),
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
                except Exception as e:
                    print(f"ADMIN NOTIFY ERROR: {e}")
        else:
            await safe_publish(bot, post_id)
            await notify_coincidence_users(bot, post_id)
            await notify_subscribers(bot, post_id)

    except Exception as e:
        print(f"FINALIZE_POST ERROR: {e}")
        await message.answer(
            f"Произошла ошибка при сохранении объявления: {html.escape(str(e))}",
            reply_markup=main_menu(message.from_user.id)
        )


@router.message(Command("my"))
@router.message(F.text == "📋 Мои объявления")
async def my_posts_handler(message: Message):
    upsert_user(message)
    await message.answer(MENU_TEXTS["my_posts"], reply_markup=main_menu(message.from_user.id))

    with closing(connect_db()) as conn:
        posts = conn.execute("""
            SELECT * FROM posts
            WHERE user_id=?
            ORDER BY created_at DESC, id DESC
            LIMIT 30
        """, (message.from_user.id,)).fetchall()

    if not posts:
        await message.answer("У тебя пока нет объявлений.", reply_markup=main_menu(message.from_user.id))
        return

    await message.answer("Твои объявления:", reply_markup=my_posts_kb(posts))


@router.callback_query(F.data.startswith("mypost:"))
async def open_my_post(callback: CallbackQuery):
    await callback.answer()
    try:
        post_id = int(callback.data.split(":")[1])
        row = get_post(post_id)

        if not row or row["user_id"] != callback.from_user.id:
            await callback.message.answer("Объявление не найдено.")
            return

        text = post_text(row)
        if len(text) > 4000:
            text = text[:3900] + "\n\n..."

        await callback.message.answer(text, reply_markup=post_actions_kb(post_id, row["status"]))

    except Exception as e:
        print(f"OPEN_MY_POST ERROR: {e}")
        await callback.message.answer("Не удалось открыть объявление.")


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


@router.callback_query(F.data.startswith("user_reviews:"))
async def show_user_reviews(callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    reviews = get_user_reviews(user_id, limit=10)

    if not reviews:
        await callback.message.answer("У пользователя пока нет отзывов.")
        await callback.answer()
        return

    avg_rating, cnt = user_rating_summary(user_id)

    lines = [
        "⭐ <b>Отзывы о пользователе</b>",
        f"Средний рейтинг: <b>{avg_rating:.1f}</b>",
        f"Всего отзывов: <b>{cnt}</b>",
        ""
    ]

    for r in reviews:
        stars = "⭐" * int(r["rating"])
        author = f"@{r['username']}" if r["username"] else (r["full_name"] or "Пользователь")
        text = r["text"] or "Без текста"

        lines.append(stars)
        lines.append(html.escape(text))
        lines.append(f"— {html.escape(author)}")
        lines.append("")

    result = "\n".join(lines)
    await callback.message.answer(result[:4000])
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

    if is_user_banned(callback.from_user.id):
        await callback.answer("Ваш аккаунт ограничен", show_alert=True)
        return

    new_status = STATUS_PENDING if ADMIN_IDS else STATUS_ACTIVE
    expires_at = calculate_post_expires_at(now_ts(), row["travel_date"], POST_TTL_DAYS)

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

    order_id = create_bump_order(callback.from_user.id, post_id)

    await callback.message.answer(
        f"💰 Поднятие объявления стоит {BUMP_PRICE_AMOUNT} {BUMP_PRICE_CURRENCY}.\n\n"
        "Оплатите через WeChat / Alipay и отправьте скрин администратору.\n"
        "После подтверждения оплаты объявление будет поднято выше.\n\n"
        f"ID заказа: <b>{order_id}</b>"
    )
    await callback.answer("Заявка создана")


@router.message(F.text == "💰 Поднять объявление")
async def bump_info(message: Message):
    await message.answer(
        f"{BUMP_PRICE_TEXT}\n\n"
        f"Стоимость: {BUMP_PRICE_AMOUNT} {BUMP_PRICE_CURRENCY}\n"
        "Открой 'Мои объявления' и нажми 'Поднять' у нужного объявления.",
        reply_markup=main_menu(message.from_user.id)
    )


@router.message(Command("admin_bump_paid"))
async def admin_bump_paid(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_bump_paid ORDER_ID")
        return

    order_id = int(parts[1])

    with closing(connect_db()) as conn, conn:
        order = conn.execute("SELECT * FROM bump_orders WHERE id=?", (order_id,)).fetchone()
        if not order:
            await message.answer("Заказ не найден.")
            return

        if order["status"] == "paid":
            await message.answer("Этот заказ уже подтвержден.")
            return

        conn.execute("""
            UPDATE bump_orders
            SET status='paid', paid_at=?
            WHERE id=?
        """, (now_ts(), order_id))

        conn.execute("""
            UPDATE posts
            SET bumped_at=?, updated_at=?
            WHERE id=?
        """, (now_ts(), now_ts(), order["post_id"]))

    try:
        await message.bot.send_message(
            order["user_id"],
            f"✅ Оплата по заказу {order_id} подтверждена.\n"
            "Ваше объявление поднято выше в поиске."
        )
    except Exception:
        pass

    await message.answer("Объявление поднято.")


@router.message(Command("find"))
@router.message(F.text == "🔎 Найти совпадения")
async def find_start(message: Message, state: FSMContext):
    upsert_user(message)
    await state.clear()
    await message.answer(MENU_TEXTS["find"], reply_markup=main_menu(message.from_user.id))

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
    await message.answer(MENU_TEXTS["popular"], reply_markup=main_menu(message.from_user.id))
    rows = get_popular_routes(10)
    if not rows:
        await message.answer("Пока нет активных маршрутов.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer("🔥 Популярные маршруты сейчас:", reply_markup=popular_routes_kb(rows))


@router.callback_query(F.data.startswith("popular:"))
async def popular_route_open(callback: CallbackQuery):
    await callback.answer()
    try:
        _, from_country, to_country = callback.data.split(":", 2)
        rows = search_route_posts_all(from_country, to_country, limit=20)

        if not rows:
            await callback.message.answer("По этому маршруту сейчас нет активных объявлений.")
            return

        trips = sum(1 for r in rows if r["post_type"] == TYPE_TRIP)
        parcels = sum(1 for r in rows if r["post_type"] == TYPE_PARCEL)

        await callback.message.answer(
            f"Маршрут: <b>{html.escape(from_country)} → {html.escape(to_country)}</b>\n"
            f"Найдено: <b>{len(rows)}</b>\n"
            f"✈️ Попутчиков: <b>{trips}</b>\n"
            f"📦 Посылок: <b>{parcels}</b>"
        )

        for row in rows:
            try:
                text = post_text(row)
                if len(text) > 4000:
                    text = text[:3900] + "\n\n..."
                await callback.message.answer(
                    text,
                    reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
                )
            except Exception as inner_e:
                print(f"POPULAR_ROUTE_SEND_ROW ERROR: {inner_e}")

    except Exception as e:
        print(f"POPULAR_ROUTE_OPEN ERROR: {e}")
        await callback.message.answer("Не удалось открыть маршрут.")


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
    await message.answer(MENU_TEXTS["stats"], reply_markup=main_menu(message.from_user.id))
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
    await message.answer(MENU_TEXTS["subscriptions"], reply_markup=main_menu(message.from_user.id))
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
        rows = []
        for s in subs:
            label = f"{s['id']} • {('✈️' if s['post_type'] == TYPE_TRIP else '📦')} • {s['from_country']}→{s['to_country']}"
            rows.append([InlineKeyboardButton(text=label[:64], callback_data=f"subdel:{s['id']}")])
        await callback.message.answer(
            "Ваши подписки. Нажмите на нужную, чтобы удалить:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
        )
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

    deal_id = ensure_deal(
        post_id=int(post_id),
        owner_user_id=int(owner_id),
        requester_user_id=callback.from_user.id,
        initiator_user_id=callback.from_user.id
    )

    await state.set_state(ContactFlow.message_text)
    await state.update_data(post_id=int(post_id), target_user_id=int(owner_id), deal_id=deal_id)
    await callback.message.answer("Напиши сообщение владельцу объявления. Я перешлю его через бота.")
    await callback.answer()


@router.message(ContactFlow.message_text)
async def relay_message(message: Message, state: FSMContext):
    data = await state.get_data()
    target_user_id = data["target_user_id"]
    post_id = data["post_id"]
    deal_id = data.get("deal_id")
    text = (message.text or "").strip()

    if not text:
        await message.answer("Сообщение не должно быть пустым.")
        return

    try:
        from_name = html.escape(message.from_user.full_name or "Пользователь")
        username_part = f" (@{html.escape(message.from_user.username)})" if message.from_user.username else ""
        safe_text = html.escape(text)

        await message.bot.send_message(
            target_user_id,
            f"Новое сообщение по объявлению ID {post_id}:\n\n"
            f"От: {from_name}{username_part}\n\n{safe_text}\n\n"
            "Чтобы ответить, напиши пользователю напрямую или дождись предложения сделки."
        )

        with closing(connect_db()) as conn, conn:
            conn.execute(
                "INSERT INTO dialogs (post_id, owner_user_id, requester_user_id, created_at) VALUES (?, ?, ?, ?)",
                (post_id, target_user_id, message.from_user.id, now_ts())
            )

            if deal_id:
                conn.execute("""
                    UPDATE deals
                    SET status=?, updated_at=?
                    WHERE id=? AND status NOT IN (?, ?, ?)
                """, (DEAL_CONTACTED, now_ts(), deal_id, DEAL_COMPLETED, DEAL_FAILED, DEAL_CANCELLED))

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🤝 Предложить сделку", callback_data=f"offer_deal:{post_id}:{target_user_id}")],
            [InlineKeyboardButton(text="❌ Не договорились", callback_data=f"deal_fail_post:{post_id}")]
        ])

        await message.answer(
            "Сообщение отправлено владельцу объявления.\n\n"
            "Следующий шаг — предложить сделку:",
            reply_markup=kb
        )

    except Exception:
        await message.answer("Не удалось отправить сообщение. Возможно, владелец еще не запускал бота.")

    await state.clear()


@router.callback_query(F.data.startswith("offer_deal:"))
async def offer_deal_handler(callback: CallbackQuery):
    _, post_id_str, owner_id_str = callback.data.split(":")
    post_id = int(post_id_str)
    owner_id = int(owner_id_str)
    requester_id = callback.from_user.id

    if owner_id == requester_id:
        await callback.answer("Это ваше объявление", show_alert=True)
        return

    row = get_post(post_id)
    if not row or row["status"] != STATUS_ACTIVE:
        await callback.answer("Объявление не найдено или неактивно", show_alert=True)
        return

    deal_id = ensure_deal(
        post_id=post_id,
        owner_user_id=owner_id,
        requester_user_id=requester_id,
        initiator_user_id=requester_id
    )

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_OFFERED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            owner_id,
            f"🤝 Вам предложили сделку по объявлению ID {post_id}.\n\n"
            f"Пользователь: {html.escape(callback.from_user.full_name or 'Пользователь')}"
            + (f" (@{html.escape(callback.from_user.username)})" if callback.from_user.username else ""),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Принять сделку", callback_data=f"deal_accept:{deal_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"deal_reject:{deal_id}")
                ]
            ])
        )
    except Exception:
        pass

    await callback.message.answer("Предложение сделки отправлено владельцу.")
    await callback.answer("Готово")


@router.callback_query(F.data.startswith("deal_accept:"))
async def deal_accept_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or deal["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_ACCEPTED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            deal["requester_user_id"],
            f"✅ Ваша сделка по объявлению ID {deal['post_id']} принята.\n\n"
            "Управление сделками происходит во вкладке МЕНЮ '🤝 Мои сделки'.\n"
            "Там вы сможете закрыть сделку и оставить отзыв.\n\n"
            "Для перехода — откройте МЕНЮ бота."
        )
    except Exception:
        pass

    await callback.message.answer("Сделка принята.")
    await callback.answer()


@router.callback_query(F.data.startswith("deal_reject:"))
async def deal_reject_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or deal["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), deal_id))

    try:
        await callback.bot.send_message(
            deal["requester_user_id"],
            f"❌ Ваша сделка по объявлению ID {deal['post_id']} отклонена."
        )
    except Exception:
        pass

    await callback.message.answer("Сделка отклонена.")
    await callback.answer()


@router.message(F.text == "🤝 Мои сделки")
async def my_deals_menu(message: Message):
    upsert_user(message)
    await message.answer(MENU_TEXTS["deals"], reply_markup=main_menu(message.from_user.id))

    deals = list_user_deals(message.from_user.id)
    if not deals:
        await message.answer("У вас пока нет сделок.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer("Ваши сделки:", reply_markup=deal_list_kb(deals))


@router.callback_query(F.data.startswith("mydeal:"))
async def open_my_deal(callback: CallbackQuery):
    await callback.answer()
    try:
        deal_id = int(callback.data.split(":")[1])
        deal = get_deal(deal_id)

        if not deal or (deal["owner_user_id"] != callback.from_user.id and deal["requester_user_id"] != callback.from_user.id):
            await callback.message.answer("Сделка не найдена.")
            return

        role = "владелец объявления" if callback.from_user.id == deal["owner_user_id"] else "откликнувшийся"
        other_user_id = deal["requester_user_id"] if callback.from_user.id == deal["owner_user_id"] else deal["owner_user_id"]

        text = (
            f"🤝 <b>Сделка #{deal['id']}</b>\n\n"
            f"Объявление ID: <b>{deal['post_id']}</b>\n"
            f"Статус: <b>{format_deal_status(deal['status'])}</b>\n"
            f"Ваша роль: <b>{role}</b>\n"
            f"Вторая сторона: <b>{format_user_ref(other_user_id)}</b>\n"
            f"Подтверждение владельца: <b>{'Да' if deal['owner_confirmed'] else 'Нет'}</b>\n"
            f"Подтверждение откликнувшегося: <b>{'Да' if deal['requester_confirmed'] else 'Нет'}</b>"
        )

        await callback.message.answer(text, reply_markup=deal_open_kb(deal, callback.from_user.id))

    except Exception as e:
        print(f"OPEN_MY_DEAL ERROR: {e}")
        await callback.message.answer("Не удалось открыть сделку.")


@router.callback_query(F.data.startswith("deal_confirm:"))
async def deal_confirm_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if deal["status"] not in (DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
        await callback.answer("Эту сделку нельзя подтвердить", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        if callback.from_user.id == deal["owner_user_id"]:
            conn.execute("""
                UPDATE deals
                SET owner_confirmed=1, updated_at=?
                WHERE id=?
            """, (now_ts(), deal_id))
        elif callback.from_user.id == deal["requester_user_id"]:
            conn.execute("""
                UPDATE deals
                SET requester_confirmed=1, updated_at=?
                WHERE id=?
            """, (now_ts(), deal_id))
        else:
            await callback.answer("Нет доступа", show_alert=True)
            return

        refreshed = conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,)).fetchone()

        if refreshed["owner_confirmed"] and refreshed["requester_confirmed"]:
            conn.execute("""
                UPDATE deals
                SET status=?, completed_at=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED, now_ts(), now_ts(), deal_id))
            final_status = DEAL_COMPLETED
        elif refreshed["owner_confirmed"]:
            conn.execute("""
                UPDATE deals
                SET status=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED_BY_OWNER, now_ts(), deal_id))
            final_status = DEAL_COMPLETED_BY_OWNER
        else:
            conn.execute("""
                UPDATE deals
                SET status=?, updated_at=?
                WHERE id=?
            """, (DEAL_COMPLETED_BY_REQUESTER, now_ts(), deal_id))
            final_status = DEAL_COMPLETED_BY_REQUESTER

    other_user_id = deal["requester_user_id"] if callback.from_user.id == deal["owner_user_id"] else deal["owner_user_id"]

    if final_status == DEAL_COMPLETED:
        for uid in [deal["owner_user_id"], deal["requester_user_id"]]:
            try:
                await callback.bot.send_message(
                    uid,
                    f"✅ Сделка #{deal_id} завершена. Теперь вы можете оставить отзыв.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="⭐ Оставить отзыв", callback_data=f"deal_review:{deal_id}")]
                    ])
                )
            except Exception as e:
                print(f"DEAL COMPLETE SEND ERROR: {e}")

        await callback.message.answer("Сделка полностью завершена.")
    else:
        try:
            await callback.bot.send_message(
                other_user_id,
                f"📌 Вторая сторона подтвердила завершение сделки по объявлению ID {deal['post_id']}.\n\n"
                "Чтобы окончательно закрыть сделку, откройте МЕНЮ → '🤝 Мои сделки' "
                "и подтвердите завершение со своей стороны."
            )
        except Exception as e:
            print(f"DEAL CONFIRM NOTIFY ERROR: {e}")

        await callback.message.answer("Ваше подтверждение сохранено. Ждем подтверждение второй стороны.")

    await callback.answer()


@router.callback_query(F.data.startswith("deal_review:"))
async def deal_review_handler(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if deal["status"] != DEAL_COMPLETED:
        await callback.answer("Отзыв можно оставить только после завершенной сделки", show_alert=True)
        return

    if callback.from_user.id == deal["owner_user_id"]:
        reviewed_user_id = deal["requester_user_id"]
    elif callback.from_user.id == deal["requester_user_id"]:
        reviewed_user_id = deal["owner_user_id"]
    else:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.clear()
    await state.update_data(reviewed_user_id=reviewed_user_id, post_id=deal["post_id"], deal_id=deal_id)
    await state.set_state(ReviewFlow.rating)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1", callback_data="review_rating:1"),
            InlineKeyboardButton(text="2", callback_data="review_rating:2"),
            InlineKeyboardButton(text="3", callback_data="review_rating:3"),
        ],
        [
            InlineKeyboardButton(text="4", callback_data="review_rating:4"),
            InlineKeyboardButton(text="5", callback_data="review_rating:5"),
        ],
    ])

    await callback.message.answer("Поставьте оценку пользователю от 1 до 5:", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("review_rating:"))
async def review_rating_callback(callback: CallbackQuery, state: FSMContext):
    rating = int(callback.data.split(":")[1])
    await state.update_data(rating=rating)
    await state.set_state(ReviewFlow.text)
    await callback.message.answer("Напишите короткий отзыв или отправьте '-' если без текста.")
    await callback.answer()


@router.message(ReviewFlow.text)
async def review_finish(message: Message, state: FSMContext):
    data = await state.get_data()

    review_text = None if message.text.strip() == "-" else message.text.strip()[:500]
    reviewed_user_id = data["reviewed_user_id"]
    post_id = data["post_id"]
    rating = data["rating"]

    with closing(connect_db()) as conn, conn:
        try:
            conn.execute("""
                INSERT INTO reviews (
                    reviewer_user_id,
                    reviewed_user_id,
                    post_id,
                    rating,
                    text,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                message.from_user.id,
                reviewed_user_id,
                post_id,
                rating,
                review_text,
                now_ts()
            ))
        except sqlite3.IntegrityError:
            await message.answer("Такой отзыв уже оставлен.", reply_markup=main_menu(message.from_user.id))
            await state.clear()
            return

    await message.answer("⭐ Спасибо! Отзыв сохранен.", reply_markup=main_menu(message.from_user.id))
    await state.clear()


@router.callback_query(F.data.startswith("deal_fail_post:"))
async def deal_fail_post_handler(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    ok = mark_deal_failed(post_id, callback.from_user.id)
    if ok:
        await callback.message.answer("❌ Сделка отмечена как несостоявшаяся.")
    else:
        await callback.message.answer("Не удалось найти активную сделку для этого объявления.")
    await callback.answer()


@router.callback_query(F.data.startswith("deal_fail_direct:"))
async def deal_fail_direct_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal or (deal["owner_user_id"] != callback.from_user.id and deal["requester_user_id"] != callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_FAILED, now_ts(), deal_id))

    await callback.message.answer("❌ Сделка отмечена как несостоявшаяся.")
    await callback.answer()


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

    row = get_post(post_id)
    if not row:
        await message.answer("Объявление не найдено.", reply_markup=main_menu(message.from_user.id))
        await state.clear()
        return

    owner_user_id = row["user_id"]

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "INSERT INTO complaints (post_id, from_user_id, reason, created_at) VALUES (?, ?, ?, ?)",
            (post_id, message.from_user.id, message.text.strip()[:1000], now_ts())
        )

        complaints_count = conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM complaints c
            JOIN posts p ON p.id = c.post_id
            WHERE p.user_id = ?
        """, (owner_user_id,)).fetchone()["cnt"]

    if complaints_count >= 3:
        ban_user(owner_user_id)

    await message.answer("Жалоба отправлена.", reply_markup=main_menu(message.from_user.id))

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"Новая жалоба на объявление {post_id}:\n\n"
                f"Причина: {html.escape(message.text.strip())}\n\n"
                f"Всего жалоб на пользователя: {complaints_count}\n\n"
                + post_text(row)
            )
        except Exception:
            pass

    if complaints_count >= 3:
        try:
            await bot.send_message(
                owner_user_id,
                "⛔ Ваш аккаунт автоматически заблокирован, потому что на ваши объявления поступило 3 жалобы.\n"
                "Если это ошибка — свяжитесь с администратором."
            )
        except Exception:
            pass

    await state.clear()


@router.message(Command("admin"))
@router.message(F.text == "👨‍💼 Админка")
async def admin_menu_handler(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Админка доступна только администраторам.")
        return

    with closing(connect_db()) as conn:
        pending = conn.execute("SELECT COUNT(*) AS c FROM posts WHERE status='pending'").fetchone()["c"]
        complaints = conn.execute("SELECT COUNT(*) AS c FROM complaints").fetchone()["c"]

    await message.answer(
        f"Админка\n\n"
        f"На модерации: {pending}\n"
        f"Жалоб: {complaints}\n\n"
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
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
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

    ban_user(row["user_id"])

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
            LEFT JOIN posts p ON p.id = c.post_id
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
            f"Причина: {html.escape(row['reason'])}"
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
    ban_user(user_id)
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


@router.message(F.text == "ℹ️ Помощь")
async def help_handler(message: Message):
    text = (
        "<b>Помощь</b>\n\n"
        "✈️ <b>Взять посылку</b> — если вы летите и можете что-то передать.\n"
        "📦 <b>Отправить посылку</b> — если вам нужно что-то передать.\n"
        "🔎 <b>Найти совпадения</b> — быстрый поиск подходящих объявлений.\n"
        "📋 <b>Мои объявления</b> — управление своими объявлениями.\n"
        "🤝 <b>Мои сделки</b> — ваши активные и завершенные сделки.\n"
        "🔔 <b>Подписки</b> — уведомления по нужным маршрутам.\n"
        "🆘 <b>Жалоба</b> — пожаловаться на объявление."
    )
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


# -------------------------
# RUN
# -------------------------

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN env var")

    init_db()

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await bot.set_my_commands([
        BotCommand(command="start", description="Запустить бота"),
        BotCommand(command="new_trip", description="Взять посылку"),
        BotCommand(command="new_parcel", description="Отправить посылку"),
        BotCommand(command="find", description="Найти совпадения"),
        BotCommand(command="my", description="Мои объявления"),
        BotCommand(command="admin", description="Админка"),
    ])

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    asyncio.create_task(expire_old_posts(bot))
    asyncio.create_task(global_coincidence_loop(bot))

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

