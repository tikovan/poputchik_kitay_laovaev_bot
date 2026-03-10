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
DISPUTE_RESPONSE_HOURS = int(os.getenv("DISPUTE_RESPONSE_HOURS", "48"))

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
DEAL_DISPUTE_OPEN = "dispute_open"
DEAL_DISPUTE_WAITING = "dispute_waiting"
DEAL_DISPUTE_RESOLVED = "dispute_resolved"

DISPUTE_OPEN = "open"
DISPUTE_WAITING_RESPONSE = "waiting_response"
DISPUTE_RESPONDED = "responded"
DISPUTE_EXPIRED = "expired"
DISPUTE_RESOLVED = "resolved"

MANUAL_COUNTRY = "🌍 Ввести другую страну"
MANUAL_CITY = "✏️ Ввести другой город"
MANUAL_WEIGHT = "✏️ Указать другой вес"
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
    "Украина": ["Киев", "Харьков", "Одесса", "Днепр", "Львов", "Запорожье", "Винница", "Ивано-Франковск"],
    "Молдова": ["Кишинёв", "Бельцы", "Тирасполь", "Кагул"],
    "Вьетнам": ["Хошимин", "Ханой", "Дананг", "Хайфон", "Нячанг"],
    "Таиланд": ["Бангкок", "Паттайя", "Пхукет", "Чиангмай", "Самуи"],
}

POPULAR_WEIGHTS = [
    "0.5 кг", "1 кг", "2 кг", "3 кг",
    "5 кг", "10 кг", "20 кг", "Более 20 кг"
]

COUNTRY_ALIASES = {
    "китай": "Китай", "china": "Китай", "кнр": "Китай",
    "россия": "Россия", "russia": "Россия", "рф": "Россия",
    "сша": "США", "usa": "США", "united states": "США", "america": "США", "америка": "США",
    "казахстан": "Казахстан", "kazakhstan": "Казахстан",
    "узбекистан": "Узбекистан", "uzbekistan": "Узбекистан",
    "кыргызстан": "Кыргызстан", "киргизия": "Кыргызстан", "kyrgyzstan": "Кыргызстан",
    "таджикистан": "Таджикистан", "tajikistan": "Таджикистан",
    "азербайджан": "Азербайджан", "azerbaijan": "Азербайджан",
    "армения": "Армения", "armenia": "Армения",
    "грузия": "Грузия", "georgia": "Грузия",
    "беларусь": "Беларусь", "belarus": "Беларусь",
    "украина": "Украина", "ukraine": "Украина",
    "молдова": "Молдова", "moldova": "Молдова",
    "вьетнам": "Вьетнам", "vietnam": "Вьетнам",
    "таиланд": "Таиланд", "thailand": "Таиланд",
}

CITY_ALIASES = {
    "шэньчжэнь": "Шэньчжэнь", "шеньчжень": "Шэньчжэнь", "shen zhen": "Шэньчжэнь", "shenzhen": "Шэньчжэнь",
    "гуанчжоу": "Гуанчжоу", "guangzhou": "Гуанчжоу", "кантон": "Гуанчжоу",
    "шанхай": "Шанхай", "shanghai": "Шанхай",
    "пекин": "Пекин", "beijing": "Пекин",
    "иу": "Иу", "yiwu": "Иу",
    "гонконг": "Гонконг", "hong kong": "Гонконг", "hongkong": "Гонконг",
    "дунгуань": "Дунгуань", "dongguan": "Дунгуань",
    "фошань": "Фошань", "foshan": "Фошань",
    "чжухай": "Чжухай", "zhuhai": "Чжухай",
    "сямынь": "Сямынь", "xiamen": "Сямынь",
    "чэнду": "Чэнду", "chengdu": "Чэнду",
    "чунцин": "Чунцин", "chongqing": "Чунцин",
    "сучжоу": "Сучжоу", "suzhou": "Сучжоу",
    "циндао": "Циндао", "qingdao": "Циндао",
    "тяньцзинь": "Тяньцзинь", "tianjin": "Тяньцзинь",
    "нинбо": "Нинбо", "ningbo": "Нинбо",
    "москва": "Москва", "moscow": "Москва",
    "санкт-петербург": "Санкт-Петербург", "санкт петербург": "Санкт-Петербург", "питер": "Санкт-Петербург", "spb": "Санкт-Петербург", "saint petersburg": "Санкт-Петербург",
    "нью-йорк": "Нью-Йорк", "нью йорк": "Нью-Йорк", "new york": "Нью-Йорк",
    "лос-анджелес": "Лос-Анджелес", "лос анджелес": "Лос-Анджелес", "los angeles": "Лос-Анджелес",
    "чикаго": "Чикаго", "chicago": "Чикаго",
    "майами": "Майами", "miami": "Майами",
    "хьюстон": "Хьюстон", "houston": "Хьюстон",
    "сан-франциско": "Сан-Франциско", "сан франциско": "Сан-Франциско", "san francisco": "Сан-Франциско",
    "лас-вегас": "Лас-Вегас", "лас вегас": "Лас-Вегас", "las vegas": "Лас-Вегас",
    "орландо": "Орландо", "orlando": "Орландо",
    "алматы": "Алматы", "almaty": "Алматы",
    "астана": "Астана", "astana": "Астана",
    "ташкент": "Ташкент", "tashkent": "Ташкент",
    "бишкек": "Бишкек", "bishkek": "Бишкек",
    "киев": "Киев", "kyiv": "Киев",
    "одесса": "Одесса", "odesa": "Одесса",
    "львов": "Львов", "lviv": "Львов",
    "запорожье": "Запорожье", "zaporizhzhia": "Запорожье",
}

STEP_ORDER = [
    "from_country",
    "from_city",
    "to_country",
    "to_city",
    "travel_date",
    "weight",
    "description",
    "photo_choice",
    "contact_note",
]
STEP_NUMBERS = {name: i + 1 for i, name in enumerate(STEP_ORDER)}

MAIN_MENU_TEXTS = {
    "✈️ Взять посылку",
    "📦 Отправить посылку",
    "🔎 Найти совпадения",
    "📋 Мои объявления",
    "🤝 Мои сделки",
    "🔥 Популярные маршруты",
    "🆕 Новые объявления",
    "🔔 Подписки",
    "📊 Статистика",
    "💰 Поднять объявление",
    "🆘 Жалоба",
    "ℹ️ Помощь",
    "👨‍💼 Админка",
}

MENU_TEXTS = {
    "trip": (
        "✈️ <b>Вы создаете объявление попутчика</b>\n\n"
        "Если вы летите и можете что-то взять — здесь можно создать объявление.\n\n"
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
        "⚖️ вес посылки\n"
        "🖼 при желании — фото посылки\n\n"
        "После публикации попутчики, которые летят этим маршрутом, смогут связаться с вами."
    ),
    "find": (
        "🔎 <b>Поиск совпадений</b>\n\n"
        "Бот поможет найти людей, которые летят нужным маршрутом или хотят отправить посылку."
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
        "После передачи посылки обе стороны подтверждают сделку.\n"
        "Если возникла проблема — можно открыть спор."
    ),
    "popular": (
        "🔥 <b>Популярные маршруты</b>\n\n"
        "Здесь показаны направления, по которым чаще всего передают посылки."
    ),
    "subscriptions": (
        "🔔 <b>Подписки на маршруты</b>\n\n"
        "Здесь можно выбрать, что именно вы хотите отслеживать:\n\n"
        "✈️ кто летит и может взять посылку\n"
        "📦 кто хочет передать свою посылку\n\n"
        "Когда появится новое подходящее объявление — бот сразу пришлет уведомление."
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

WELCOME_TEXT = (
    "👋 <b>Привет.</b>\n\n"
    "Это <b>Попутчик Китай</b> — бот для передачи посылок через попутчиков.\n\n"
    "<b>Здесь можно отправить свою посылку или взять чужую по маршруту.</b>\n\n"
    "🔎 <b>Обязательно подпишись на канал с объявлениями:</b>\n"
    "t.me/china_poputchik\n\n"
    "Я настолько крутой, что сам ищу тебе попутчика/посылку и уведомляю о них, пока ты пьешь свой лате!\n\n"
    "В правом углу поисковой строки есть квадратик с 4-мя кружочками — нажми — это твой центр управления ботом.\n\n"
    "⬇️ <b>Синяя кнопка МЕНЮ — это только лишь меню с базовыми командами бота</b>"
)


def now_ts() -> int:
    return int(time.time())


def days_to_seconds(days: int) -> int:
    return days * 24 * 60 * 60


def format_age(ts: int) -> str:
    diff = max(0, now_ts() - ts)
    if diff < 60:
        return "только что"
    if diff < 3600:
        return f"{diff // 60} мин назад"
    if diff < 86400:
        return f"{diff // 3600} ч назад"
    return f"{diff // 86400} дн назад"


def normalize_text(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def normalize_free_text(value: str) -> str:
    value = (value or "").strip().lower().replace("ё", "е")
    return re.sub(r"\s+", " ", value)


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
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
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

    exact_dt = parse_date_loose(value.strip())
    if exact_dt:
        return datetime(exact_dt.year, exact_dt.month, exact_dt.day, 23, 59, 59)

    m = re.match(r"^\s*(\d{2}\.\d{2}\.\d{4})\s*[-–—]\s*(\d{2}\.\d{2}\.\d{4})\s*$", value.strip())
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
            is_verified INTEGER DEFAULT 0,
            dispute_no_response_count INTEGER DEFAULT 0
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
            photo_file_id TEXT,
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

        CREATE TABLE IF NOT EXISTS disputes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id INTEGER NOT NULL,
            opened_by_user_id INTEGER NOT NULL,
            against_user_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            reason_text TEXT,
            response_text TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            response_deadline_at INTEGER NOT NULL,
            responded_at INTEGER
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

        CREATE INDEX IF NOT EXISTS idx_posts_search ON posts(post_type, status, from_country, to_country, created_at);
        CREATE INDEX IF NOT EXISTS idx_posts_user ON posts(user_id, status, created_at);
        CREATE INDEX IF NOT EXISTS idx_subscriptions_search ON route_subscriptions(post_type, from_country, to_country);
        CREATE INDEX IF NOT EXISTS idx_reviews_user ON reviews(reviewed_user_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_deals_owner ON deals(owner_user_id, status, created_at);
        CREATE INDEX IF NOT EXISTS idx_deals_requester ON deals(requester_user_id, status, created_at);
        """)

        ensure_column(conn, "users", "is_verified", "is_verified INTEGER DEFAULT 0")
        ensure_column(conn, "users", "is_banned", "is_banned INTEGER DEFAULT 0")
        ensure_column(conn, "users", "last_action_at", "last_action_at INTEGER DEFAULT 0")
        ensure_column(conn, "users", "dispute_no_response_count", "dispute_no_response_count INTEGER DEFAULT 0")
        ensure_column(conn, "posts", "expires_at", "expires_at INTEGER")
        ensure_column(conn, "posts", "photo_file_id", "photo_file_id TEXT")
        ensure_column(conn, "deals", "initiator_user_id", "initiator_user_id INTEGER")
        ensure_column(conn, "deals", "owner_confirmed", "owner_confirmed INTEGER DEFAULT 0")
        ensure_column(conn, "deals", "requester_confirmed", "requester_confirmed INTEGER DEFAULT 0")
        ensure_column(conn, "deals", "updated_at", "updated_at INTEGER DEFAULT 0")

        conn.execute("UPDATE deals SET updated_at = created_at WHERE updated_at IS NULL OR updated_at = 0")
        conn.execute("UPDATE deals SET status='contacted' WHERE status='pending'")


def upsert_user(message_or_callback):
    user = message_or_callback.from_user
    with closing(connect_db()) as conn, conn:
        existing = conn.execute("SELECT created_at FROM users WHERE user_id=?", (user.id,)).fetchone()
        created_at = int(existing["created_at"]) if existing and existing["created_at"] else now_ts()
        conn.execute("""
            INSERT INTO users (user_id, username, full_name, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name
        """, (user.id, user.username, (user.full_name or "")[:200], created_at))


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
        row = conn.execute("SELECT is_banned, last_action_at FROM users WHERE user_id=?", (user_id,)).fetchone()
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
        return float(row["avg_rating"] or 0), int(row["cnt"] or 0)


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
        return f"{max(1, days // 30)} мес"
    return f"{max(1, days // 365)} г"


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


def has_user_left_review_for_deal(deal: sqlite3.Row, reviewer_user_id: int) -> bool:
    reviewed_user_id = deal["requester_user_id"] if reviewer_user_id == deal["owner_user_id"] else deal["owner_user_id"]
    with closing(connect_db()) as conn:
        row = conn.execute("""
            SELECT 1 FROM reviews
            WHERE reviewer_user_id=? AND reviewed_user_id=? AND post_id=?
            LIMIT 1
        """, (reviewer_user_id, reviewed_user_id, deal["post_id"])).fetchone()
        return row is not None


def get_open_dispute_by_deal(deal_id: int) -> Optional[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT *
            FROM disputes
            WHERE deal_id=? AND status IN ('open', 'waiting_response', 'responded')
            ORDER BY id DESC
            LIMIT 1
        """, (deal_id,)).fetchone()


def create_dispute(deal_id: int, opened_by_user_id: int, against_user_id: int, reason_text: str) -> int:
    ts = now_ts()
    deadline = ts + DISPUTE_RESPONSE_HOURS * 3600
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("""
            INSERT INTO disputes (
                deal_id, opened_by_user_id, against_user_id,
                status, reason_text, created_at, updated_at, response_deadline_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            deal_id, opened_by_user_id, against_user_id,
            DISPUTE_WAITING_RESPONSE, reason_text, ts, ts, deadline
        ))
        return int(cur.lastrowid)


def save_dispute_response(dispute_id: int, response_text: str):
    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE disputes
            SET response_text=?, status=?, responded_at=?, updated_at=?
            WHERE id=?
        """, (response_text, DISPUTE_RESPONDED, now_ts(), now_ts(), dispute_id))


def get_dispute(dispute_id: int) -> Optional[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("SELECT * FROM disputes WHERE id=?", (dispute_id,)).fetchone()


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
        DEAL_FAILED: "неуспешна",
        DEAL_CANCELLED: "отменена",
        DEAL_DISPUTE_OPEN: "спор открыт",
        DEAL_DISPUTE_WAITING: "ожидается ответ по спору",
        DEAL_DISPUTE_RESOLVED: "спор решен",
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


def form_header(post_type: str, step: int, total_steps: int = 9) -> str:
    title = "📦 Отправить посылку" if post_type == TYPE_PARCEL else "✈️ Взять посылку"
    return f"{title}\n\n━━━━━━━━━━━━━━\nШаг {step} / {total_steps}\n━━━━━━━━━━━━━━\n\n"


def form_text(post_type: str, step: int, prompt: str, total_steps: int = 9) -> str:
    return form_header(post_type, step, total_steps) + prompt


def post_text(row, for_channel: bool = False) -> str:
    route = html.escape(row["from_country"])
    if row["from_city"]:
        route += f", {html.escape(row['from_city'])}"
    route += " → " + html.escape(row["to_country"])
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

    if "photo_file_id" in row.keys() and row["photo_file_id"]:
        lines.append("<b>Фото посылки:</b> доступно по кнопке ниже")

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
       lines.append(
        "Откройте объявление и напишите пользователю.\n"
        "Возможно, ваша посылка уже почти в пути ✈️📦."
    )
    else:
        owner = row["username"] if "username" in row.keys() else None
        if owner:
            lines.append(f"<b>Telegram:</b> @{html.escape(owner)}")

    return "\n".join(lines)


def chunk_buttons(items: List[tuple], prefix: str, per_row: int = 2):
    rows, row = [], []
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
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")]])


def countries_kb(prefix: str):
    return InlineKeyboardMarkup(inline_keyboard=chunk_buttons(COUNTRY_OPTIONS, prefix, 2))


def countries_select_kb(prefix: str, include_back: bool = False):
    rows = chunk_buttons(COUNTRY_OPTIONS, prefix, 2)
    rows.append([InlineKeyboardButton(text=MANUAL_COUNTRY, callback_data=f"{prefix}:__manual__")])
    return with_back(rows, include_back)


def cities_select_kb(prefix: str, country: str, include_back: bool = True):
    cities = COUNTRY_CITIES_RU.get(country, [])
    rows, row = [], []
    for city in cities:
        row.append(InlineKeyboardButton(text=city, callback_data=f"{prefix}:{city}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text=MANUAL_CITY, callback_data=f"{prefix}:__manual__")])
    rows.append([InlineKeyboardButton(text="Не важно", callback_data=f"{prefix}:__skip__")])
    return with_back(rows, include_back)


def weight_select_kb():
    rows = chunk_buttons([(w, w) for w in POPULAR_WEIGHTS], "weightpick", 2)
    rows.append([InlineKeyboardButton(text=MANUAL_WEIGHT, callback_data="weightpick:__manual__")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def date_select_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="В течение недели", callback_data="datepick:week")],
        [InlineKeyboardButton(text="В течение месяца", callback_data="datepick:month")],
        [InlineKeyboardButton(text="✏️ Указать точную дату", callback_data="datepick:manual")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")],
    ])


def photo_choice_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📷 Добавить фото посылки", callback_data="photo_choice:add")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data="photo_choice:skip")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")],
    ])


def my_posts_kb(posts: List[sqlite3.Row]):
    rows = []
    for index, p in enumerate(posts, start=1):
        icon = "✈️" if p["post_type"] == TYPE_TRIP else "📦"
        label = f"{index}. {icon} • {p['from_country']}→{p['to_country']} • {p['status']}"
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
        [InlineKeyboardButton(text="📤 Поделиться", url=share_url)]
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
        [InlineKeyboardButton(text="🚫 Бан user", callback_data=f"adminbanpost:{post_id}")]
    ])


def public_post_kb(post_id: int, owner_id: int, post_type: Optional[str] = None):
    _, reviews_count = user_rating_summary(owner_id)
    row = get_post(post_id)

    rows = [
        [InlineKeyboardButton(text="✉️ Написать владельцу", callback_data=f"contact:{post_id}:{owner_id}")],
        [InlineKeyboardButton(text="🤝 Предложить сделку", callback_data=f"offer_deal:{post_id}:{owner_id}")]
    ]

    if row and row["photo_file_id"]:
        rows.append([InlineKeyboardButton(text="🖼 Посмотреть фото посылки", callback_data=f"viewphoto:{post_id}")])

    if reviews_count > 0:
        rows.append([InlineKeyboardButton(
            text=f"⭐ {reviews_count} {reviews_word(reviews_count)}",
            callback_data=f"user_reviews:{owner_id}"
        )])

    rows.append([InlineKeyboardButton(text="⚠️ Пожаловаться", callback_data=f"complain:{post_id}")])
    rows.append([InlineKeyboardButton(text="📤 Поделиться", url=f"https://t.me/share/url?url={post_deeplink(post_id)}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_post_kb(post_id: int, post_type: Optional[str] = None):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🤝 Открыть объявление, чтобы связаться",
                    url=post_deeplink(post_id)
                )
            ],
            [
                InlineKeyboardButton(
                    text="📤 Поделиться",
                    url=f"https://t.me/share/url?url={post_deeplink(post_id)}"
                )
            ],
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
        buttons.append([InlineKeyboardButton(text=label[:64], callback_data=f"popular:{row['from_country']}:{row['to_country']}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons or [[InlineKeyboardButton(text="Пока пусто", callback_data="noop")]])


def deal_open_kb(deal: sqlite3.Row, user_id: int) -> InlineKeyboardMarkup:
    rows = []

    if deal["status"] in (DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
        rows.append([InlineKeyboardButton(text="✅ Подтвердить завершение", callback_data=f"deal_confirm:{deal['id']}")])

    if deal["status"] == DEAL_COMPLETED and not has_user_left_review_for_deal(deal, user_id):
        rows.append([InlineKeyboardButton(text="⭐ Оставить отзыв", callback_data=f"deal_review:{deal['id']}")])

    if deal["status"] in (
    DEAL_ACCEPTED,
    DEAL_COMPLETED_BY_OWNER,
    DEAL_COMPLETED_BY_REQUESTER,
):
        rows.append([InlineKeyboardButton(text="📦 Посылка не доставлена", callback_data=f"deal_dispute_open:{deal['id']}")])

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
    photo_choice = State()
    photo_upload = State()
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


class DisputeFlow(StatesGroup):
    deal_id = State()
    reason = State()
    response = State()


def is_main_menu_text(text: str) -> bool:
    return (text or "").strip() in MAIN_MENU_TEXTS


async def block_menu_text_during_form(message: Message, state: FSMContext) -> bool:
    if is_main_menu_text(message.text):
        await message.answer(
            "Сейчас вы заполняете объявление.\n\n"
            "Сначала завершите заполнение или вернитесь назад.\n"
            "Кнопки меню не будут сохранены в объявление.",
            reply_markup=main_menu(message.from_user.id)
        )
        return True
    return False


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
            WHERE p.from_country=? AND p.to_country=?
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
                travel_date, weight_kg, description, contact_note, photo_file_id, status,
                is_anonymous_contact, channel_message_id, created_at, updated_at, bumped_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
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
            data.get("photo_file_id"),
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
            WHERE user_id=? AND post_type=? AND from_country=? AND to_country=? LIMIT 1
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

    trip_weight = source_weight if source_row["post_type"] == TYPE_TRIP else candidate_weight
    parcel_weight = candidate_weight if source_row["post_type"] == TYPE_TRIP else source_weight

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


def get_coincidences(post_type: str, from_country: str, to_country: str, exclude_user_id: Optional[int] = None, source_row=None, limit: int = 20) -> List[dict]:
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

    results = []
    for row in rows:
        score, notes = (45, ["Совпадает маршрут по странам"]) if source_row is None else calculate_coincidence_score(source_row, row)
        if score < 35:
            continue
        results.append({
            "row": row,
            "score": score,
            "notes": notes,
            "type": "strong" if score >= 75 else "good" if score >= 55 else "possible"
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
            SELECT id FROM deals
            WHERE post_id=? AND owner_user_id=? AND requester_user_id=?
            ORDER BY id DESC LIMIT 1
        """, (post_id, owner_user_id, requester_user_id)).fetchone()

        if row:
            return int(row["id"])

        cur = conn.execute("""
            INSERT INTO deals (
                post_id, owner_user_id, requester_user_id, initiator_user_id,
                status, owner_confirmed, requester_confirmed, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
        """, (
            post_id, owner_user_id, requester_user_id, initiator_user_id,
            DEAL_CONTACTED, now_ts(), now_ts()
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
            SELECT id FROM deals
            WHERE post_id=?
              AND (owner_user_id=? OR requester_user_id=?)
              AND status IN (?, ?, ?, ?, ?)
            ORDER BY id DESC LIMIT 1
        """, (
            post_id, user_id, user_id,
            DEAL_CONTACTED, DEAL_OFFERED, DEAL_ACCEPTED,
            DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER
        )).fetchone()

        if not row:
            return False

        conn.execute("UPDATE deals SET status=?, updated_at=? WHERE id=?", (DEAL_FAILED, now_ts(), row["id"]))
        return True


def create_bump_order(user_id: int, post_id: int, amount: int = BUMP_PRICE_AMOUNT, currency: str = BUMP_PRICE_CURRENCY) -> int:
    with closing(connect_db()) as conn, conn:
        cur = conn.execute("""
            INSERT INTO bump_orders (user_id, post_id, amount, currency, status, created_at)
            VALUES (?, ?, ?, ?, 'pending', ?)
        """, (user_id, post_id, amount, currency, now_ts()))
        return int(cur.lastrowid)


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
                            f"⌛ Ваше объявление ID {row['id']} истекло и скрыто.\nОткройте 'Мои объявления', чтобы активировать его снова.",
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


async def dispute_timeout_loop(bot: Bot):
    while True:
        try:
            with closing(connect_db()) as conn:
                disputes = conn.execute("""
                    SELECT *
                    FROM disputes
                    WHERE status='waiting_response'
                      AND response_deadline_at <= ?
                """, (now_ts(),)).fetchall()

            for dispute in disputes:
                with closing(connect_db()) as conn, conn:
                    conn.execute("UPDATE disputes SET status=?, updated_at=? WHERE id=?", (DISPUTE_EXPIRED, now_ts(), dispute["id"]))
                    conn.execute("UPDATE deals SET status=?, updated_at=? WHERE id=?", (DEAL_FAILED, now_ts(), dispute["deal_id"]))
                    conn.execute("""
                        UPDATE users
                        SET dispute_no_response_count = COALESCE(dispute_no_response_count, 0) + 1
                        WHERE user_id=?
                    """, (dispute["against_user_id"],))

                ban_user(dispute["against_user_id"])

                try:
                    await bot.send_message(
                        dispute["against_user_id"],
                        "⛔ Вы не ответили по спорной сделке в установленный срок.\nВаш аккаунт временно ограничен. Свяжитесь с администратором."
                    )
                except Exception:
                    pass

                try:
                    await bot.send_message(
                        dispute["opened_by_user_id"],
                        "⚠️ Вторая сторона не ответила по спору в установленный срок.\nАккаунт второй стороны ограничен, информация передана администратору."
                    )
                except Exception:
                    pass

                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(
                            admin_id,
                            f"⛔ Пользователь {dispute['against_user_id']} не ответил по спору #{dispute['deal_id']} в срок и был ограничен."
                        )
                    except Exception:
                        pass

        except Exception as e:
            print(f"DISPUTE TIMEOUT LOOP ERROR: {e}")

        await asyncio.sleep(600)


async def begin_create(message: Message, state: FSMContext, post_type: str):
    upsert_user(message)

    if is_user_banned(message.from_user.id):
        await message.answer("⛔ Ваш аккаунт ограничен. Если это ошибка — свяжитесь с администратором.", reply_markup=main_menu(message.from_user.id))
        return

    spam_error = anti_spam_check(message.from_user.id)
    if spam_error:
        await message.answer(spam_error, reply_markup=main_menu(message.from_user.id))
        return

    if active_post_count(message.from_user.id) >= MAX_ACTIVE_POSTS_PER_USER:
        await message.answer(
            f"У вас уже слишком много объявлений. Лимит: {MAX_ACTIVE_POSTS_PER_USER}. Удалите или деактивируйте старые объявления.",
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
        CreatePost.photo_choice.state: "photo_choice",
        CreatePost.photo_upload.state: "photo_choice",
        CreatePost.contact_note.state: "contact_note",
    }
    return mapping.get(state_name)


CREATE_STEP_CLEANUP_KEYS = {
    "from_country": ["from_country", "from_city", "to_country", "to_city", "travel_date", "weight_kg", "description", "photo_file_id", "contact_note"],
    "from_city": ["from_city", "to_country", "to_city", "travel_date", "weight_kg", "description", "photo_file_id", "contact_note"],
    "to_country": ["to_country", "to_city", "travel_date", "weight_kg", "description", "photo_file_id", "contact_note"],
    "to_city": ["to_city", "travel_date", "weight_kg", "description", "photo_file_id", "contact_note"],
    "travel_date": ["travel_date", "weight_kg", "description", "photo_file_id", "contact_note"],
    "weight": ["weight_kg", "description", "photo_file_id", "contact_note"],
    "description": ["description", "photo_file_id", "contact_note"],
    "photo_choice": ["photo_file_id", "contact_note"],
    "contact_note": ["contact_note"],
}


async def clear_step_data_from(state: FSMContext, target_step: str):
    data = await state.get_data()
    for key in CREATE_STEP_CLEANUP_KEYS.get(target_step, []):
        data.pop(key, None)
    await state.set_data(data)


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

    if target_step == "photo_choice":
        await state.set_state(CreatePost.photo_choice)
        await target_message.answer(
            form_text(post_type, STEP_NUMBERS[target_step], "Хотите добавить фото посылки? Это необязательно."),
            reply_markup=photo_choice_kb()
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
            "⛔ Ваш аккаунт ограничен из-за жалоб пользователей.\nЕсли это ошибка — свяжитесь с администратором."
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

    await message.answer(WELCOME_TEXT, reply_markup=main_menu(message.from_user.id))


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


@router.message(F.text == "🤝 Мои сделки")
async def my_deals_menu(message: Message):
    upsert_user(message)
    await message.answer(MENU_TEXTS["deals"], reply_markup=main_menu(message.from_user.id))
    deals = list_user_deals(message.from_user.id)
    if not deals:
        await message.answer("У вас пока нет сделок.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer("Ваши сделки:", reply_markup=deal_list_kb(deals))


@router.message(F.text == "🆘 Жалоба")
async def complaint_start(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(ComplaintFlow.post_id)
    await message.answer(
    "🆘 <b>Отправить жалобу</b>\n\n"
    "Если пользователь:\n"
    "• не отвечает\n"
    "• пытается обмануть\n"
    "• нарушает правила сервиса\n\n"
    "Вы можете отправить жалобу.\n\n"
    "Введите <b>ID объявления</b>, на которое хотите пожаловаться.\n"
    "ID указан в каждом объявлении."
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
    if await block_menu_text_during_form(message, state):
        return

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
    if await block_menu_text_during_form(message, state):
        return

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
    if await block_menu_text_during_form(message, state):
        return

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
    if await block_menu_text_during_form(message, state):
        return

    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
    value = message.text.strip()

    await state.update_data(to_city=None if value == SKIP_VALUE else normalize_city_input(value[:80]))
    await state.set_state(CreatePost.travel_date)
    await message.answer(
        form_text(post_type, 5, "Выберите дату поездки / отправки"),
        reply_markup=date_select_kb()
    )


@router.callback_query(F.data.startswith("complain:"))
async def complaint_from_button(callback: CallbackQuery, state: FSMContext):
    post_id = int(callback.data.split(":")[1])
    await state.clear()
    await state.set_state(ComplaintFlow.reason)
    await state.update_data(post_id=post_id)

    await callback.message.answer(
        f"🆘 <b>Жалоба на объявление {post_id}</b>\n\n"
        "Опишите причину жалобы.\n"
        "Например: не отвечает, подозрение на обман, некорректное объявление."
    )
    await callback.answer()
    

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
    if await block_menu_text_during_form(message, state):
        return

    raw = message.text.strip()[:100]
    parsed = parse_date_loose(raw)
    if not parsed:
        await message.answer("Не смог распознать дату.\nВведите в формате: 15.03.2026", reply_markup=back_only_kb())
        return

    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
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
    if await block_menu_text_during_form(message, state):
        return

    value = (message.text or "").strip()
    if len(value) < 1:
        await message.answer("Введите вес или объем.", reply_markup=back_only_kb())
        return

    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)
    await state.update_data(weight_kg=value[:50])
    await state.set_state(CreatePost.description)
    await message.answer(
        form_text(post_type, 7, "Опишите объявление подробно\nЧто нужно передать / сколько места есть / условия"),
        reply_markup=back_only_kb()
    )


@router.message(CreatePost.description)
async def enter_description(message: Message, state: FSMContext):
    if await block_menu_text_during_form(message, state):
        return

    desc = (message.text or "").strip()
    if len(desc) < 3:
        await message.answer("Описание слишком короткое. Напишите подробнее.", reply_markup=back_only_kb())
        return

    await state.update_data(description=desc[:1000])
    await render_create_step("photo_choice", message, state)


@router.callback_query(F.data.startswith("photo_choice:"))
async def photo_choice_handler(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":", 1)[1]
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if action == "add":
        await state.set_state(CreatePost.photo_upload)
        await callback.message.answer(
            form_text(post_type, 8, "Отправьте 1 фото посылки"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    if action == "skip":
        await state.update_data(photo_file_id=None)
        await state.set_state(CreatePost.contact_note)
        await callback.message.answer(
            form_text(
                post_type,
                9,
                "Введите дополнительный контакт или примечание\nНапример: WeChat ID / только текст / без звонков\nЕсли не нужно — напишите -"
            ),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await callback.answer("Неверная команда", show_alert=True)


@router.message(CreatePost.photo_upload, F.photo)
async def upload_parcel_photo(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    await state.update_data(photo_file_id=photo_id)
    await state.set_state(CreatePost.contact_note)
    await message.answer(
        form_text(
            post_type,
            9,
            "Фото сохранено.\n\nВведите дополнительный контакт или примечание\nНапример: WeChat ID / только текст / без звонков\nЕсли не нужно — напишите -"
        ),
        reply_markup=back_only_kb()
    )


@router.message(CreatePost.photo_upload)
async def upload_parcel_photo_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте именно фото или нажмите Назад.")


@router.message(CreatePost.contact_note)
async def finalize_post(message: Message, state: FSMContext, bot: Bot):
    if await block_menu_text_during_form(message, state):
        return

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
        await state.clear()


@router.message(Command("my"))
@router.message(F.text == "📋 Мои объявления")
async def my_posts_handler(message: Message):
    upsert_user(message)
    await message.answer(MENU_TEXTS["my_posts"], reply_markup=main_menu(message.from_user.id))

    with closing(connect_db()) as conn:
        posts = conn.execute("""
            SELECT * FROM posts
            WHERE user_id=? AND status != 'deleted'
            ORDER BY created_at DESC
            LIMIT 30
        """, (message.from_user.id,)).fetchall()

    if not posts:
        await message.answer("У вас пока нет объявлений.", reply_markup=main_menu(message.from_user.id))
        return

    await message.answer("📋 Ваши объявления:", reply_markup=my_posts_kb(posts))


@router.callback_query(F.data.startswith("deal_review:"))
async def deal_review_start(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if callback.from_user.id not in (deal["owner_user_id"], deal["requester_user_id"]):
        await callback.answer("Нет доступа", show_alert=True)
        return

    if deal["status"] != DEAL_COMPLETED:
        await callback.answer("Отзыв можно оставить только по завершенной сделке", show_alert=True)
        return

    if has_user_left_review_for_deal(deal, callback.from_user.id):
        await callback.answer("Вы уже оставили отзыв", show_alert=True)
        return

    reviewed_user_id = deal["requester_user_id"] if callback.from_user.id == deal["owner_user_id"] else deal["owner_user_id"]

    await state.clear()
    await state.set_state(ReviewFlow.rating)
    await state.update_data(
        deal_id=deal_id,
        reviewed_user_id=reviewed_user_id,
        post_id=deal["post_id"]
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1", callback_data="review_rating:1"),
            InlineKeyboardButton(text="2", callback_data="review_rating:2"),
            InlineKeyboardButton(text="3", callback_data="review_rating:3"),
            InlineKeyboardButton(text="4", callback_data="review_rating:4"),
            InlineKeyboardButton(text="5", callback_data="review_rating:5"),
        ]
    ])

    await callback.message.answer(
        "⭐ Выберите оценку пользователю от 1 до 5:",
        reply_markup=kb
    )
    await callback.answer()


@router.callback_query(F.data.startswith("review_rating:"))
async def review_rating_pick(callback: CallbackQuery, state: FSMContext):
    rating = int(callback.data.split(":")[1])

    if rating < 1 or rating > 5:
        await callback.answer("Неверная оценка", show_alert=True)
        return

    await state.update_data(rating=rating)
    await state.set_state(ReviewFlow.text)

    await callback.message.answer(
        "Напишите короткий отзыв.\n"
        "Если без текста — отправьте минус: -"
    )
    await callback.answer()


@router.message(ReviewFlow.text)
async def review_text_input(message: Message, state: FSMContext):
    data = await state.get_data()

    reviewed_user_id = data["reviewed_user_id"]
    post_id = data["post_id"]
    rating = data["rating"]

    text = (message.text or "").strip()
    if not text:
        await message.answer("Введите текст отзыва или '-'")
        return

    review_text = None if text == "-" else text[:500]

    try:
        with closing(connect_db()) as conn, conn:
            conn.execute("""
                INSERT INTO reviews (
                    reviewer_user_id, reviewed_user_id, post_id, rating, text, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                message.from_user.id,
                reviewed_user_id,
                post_id,
                rating,
                review_text,
                now_ts()
            ))

        await message.answer("✅ Отзыв сохранен.", reply_markup=main_menu(message.from_user.id))

    except sqlite3.IntegrityError:
        await message.answer("Вы уже оставили отзыв по этой сделке.", reply_markup=main_menu(message.from_user.id))

    await state.clear()


@router.callback_query(F.data.startswith("mypost:"))
async def open_my_post(callback: CallbackQuery):
    await callback.answer()
    try:
        post_id = int(callback.data.split(":")[1])
        row = get_post(post_id)

        if not row or row["user_id"] != callback.from_user.id or row["status"] == STATUS_DELETED:
            await callback.message.answer("Объявление не найдено.")
            return

        text = post_text(row)
        if len(text) > 4000:
            text = text[:3900] + "\n\n..."

        await callback.message.answer(text, reply_markup=post_actions_kb(post_id, row["status"]))
    except Exception as e:
        print(f"OPEN_MY_POST ERROR: {e}")
        await callback.message.answer("Не удалось открыть объявление.")


@router.callback_query(F.data.startswith("deal_confirm:"))
async def deal_confirm_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    user_id = callback.from_user.id
    if user_id not in (deal["owner_user_id"], deal["requester_user_id"]):
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        if user_id == deal["owner_user_id"]:
            owner_confirmed = 1
            requester_confirmed = deal["requester_confirmed"]
        else:
            owner_confirmed = deal["owner_confirmed"]
            requester_confirmed = 1

        if owner_confirmed and requester_confirmed:
            conn.execute("""
                UPDATE deals
                SET owner_confirmed=1, requester_confirmed=1, status=?, updated_at=?, completed_at=?
                WHERE id=?
            """, (DEAL_COMPLETED, now_ts(), now_ts(), deal_id))
            await callback.message.answer("✅ Сделка завершена. Теперь можно оставить отзыв.")
        else:
            new_status = DEAL_COMPLETED_BY_OWNER if user_id == deal["owner_user_id"] else DEAL_COMPLETED_BY_REQUESTER
            conn.execute("""
                UPDATE deals
                SET owner_confirmed=?, requester_confirmed=?, status=?, updated_at=?
                WHERE id=?
            """, (owner_confirmed, requester_confirmed, new_status, now_ts(), deal_id))
            await callback.message.answer("✅ Ваше подтверждение сохранено. Ждем подтверждение второй стороны.")

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
        conn.execute("UPDATE posts SET status=?, updated_at=? WHERE id=?", (STATUS_DELETED, now_ts(), post_id))

    await callback.message.answer("🗑 Объявление удалено.")
    await callback.answer()


@router.message(DisputeFlow.reason)
async def dispute_reason_input(message: Message, state: FSMContext):
    data = await state.get_data()
    deal_id = data["deal_id"]
    deal = get_deal(deal_id)

    if not deal:
        await message.answer("Сделка не найдена.", reply_markup=main_menu(message.from_user.id))
        await state.clear()
        return

    reason = (message.text or "").strip()
    if len(reason) < 3:
        await message.answer("Опишите причину подробнее.")
        return

    against_user_id = deal["requester_user_id"] if message.from_user.id == deal["owner_user_id"] else deal["owner_user_id"]
    dispute_id = create_dispute(deal_id, message.from_user.id, against_user_id, reason)

    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE deals SET status=?, updated_at=? WHERE id=?", (DEAL_DISPUTE_WAITING, now_ts(), deal_id))

    try:
        await message.bot.send_message(
            against_user_id,
            f"⚠️ По сделке #{deal_id} открыт спор.\n\n"
            f"Причина: {html.escape(reason)}\n\n"
            "Ответьте администратору или второй стороне как можно скорее."
        )
    except Exception:
        pass

    await message.answer(
        f"✅ Спор по сделке #{deal_id} открыт.",
        reply_markup=main_menu(message.from_user.id)
    )
    await state.clear()


@router.callback_query(F.data.startswith("deal_dispute_open:"))
async def deal_dispute_open_handler(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if callback.from_user.id not in (deal["owner_user_id"], deal["requester_user_id"]):
        await callback.answer("Нет доступа", show_alert=True)
        return

    existing = get_open_dispute_by_deal(deal_id)
    if existing:
        await callback.answer("По этой сделке уже открыт спор", show_alert=True)
        return

    await state.clear()
    await state.set_state(DisputeFlow.reason)
    await state.update_data(deal_id=deal_id)

    await callback.message.answer(
        "Опишите причину спора.\n"
        "Например: пользователь не вышел на связь, посылка не доставлена, обман."
    )
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
        conn.execute("UPDATE posts SET status=?, updated_at=? WHERE id=?", (STATUS_INACTIVE, now_ts(), post_id))

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
        "Откройте 'Мои объявления' и нажмите 'Поднять' у нужного объявления.",
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
        [InlineKeyboardButton(text="Ищу посылку", callback_data="findtype:trip")]
    ])
    await state.set_state(FindFlow.looking_for)
    await message.answer("Что ищем?", reply_markup=kb)


@router.callback_query(F.data.startswith("findtype:"))
async def find_type(callback: CallbackQuery, state: FSMContext):
    looking_for = callback.data.split(":")[1]
    await state.update_data(looking_for=looking_for)
    await state.set_state(FindFlow.from_country)
    await callback.message.answer("Выберите страну отправления:", reply_markup=countries_kb("findfrom"))
    await callback.answer()


@router.callback_query(F.data.startswith("findfrom:"))
async def find_from(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    await state.update_data(from_country=country)
    await state.set_state(FindFlow.to_country)
    await callback.message.answer("Выберите страну назначения:", reply_markup=countries_kb("findto"))
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


@router.callback_query(F.data.startswith("viewphoto:"))
async def view_photo_handler(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = get_post(post_id)

    if not row or not row["photo_file_id"]:
        await callback.answer("Фото не найдено", show_alert=True)
        return

    await callback.message.answer_photo(
        photo=row["photo_file_id"],
        caption=f"Фото посылки для объявления ID {post_id}"
    )
    await callback.answer()

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


@router.message(ComplaintFlow.post_id)
async def complaint_post_id_input(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    if not text.isdigit():
        await message.answer("Введите корректный ID объявления числом.")
        return

    post_id = int(text)
    row = get_post(post_id)
    if not row:
        await message.answer("Объявление с таким ID не найдено.")
        return

    await state.update_data(post_id=post_id)
    await state.set_state(ComplaintFlow.reason)
    await message.answer(
        f"🆘 <b>Жалоба на объявление {post_id}</b>\n\n"
        "Опишите причину жалобы.\n"
        "Например: не отвечает, подозрение на обман, некорректное объявление."
    )


@router.message(ComplaintFlow.reason)
async def complaint_reason_input(message: Message, state: FSMContext):
    reason = (message.text or "").strip()
    if len(reason) < 3:
        await message.answer("Опишите причину жалобы чуть подробнее.")
        return

    data = await state.get_data()
    post_id = data["post_id"]

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "INSERT INTO complaints (post_id, from_user_id, reason, created_at) VALUES (?, ?, ?, ?)",
            (post_id, message.from_user.id, reason[:1000], now_ts())
        )

    await state.clear()
    await message.answer(
        "✅ Жалоба отправлена администратору.",
        reply_markup=main_menu(message.from_user.id)
    )

    for admin_id in ADMIN_IDS:
        try:
            await message.bot.send_message(
                admin_id,
                f"🆘 Новая жалоба\n\n"
                f"Объявление ID: <b>{post_id}</b>\n"
                f"От пользователя: <b>{message.from_user.id}</b>\n\n"
                f"Причина:\n{html.escape(reason[:1000])}"
            )
        except Exception:
            pass


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
        [InlineKeyboardButton(text="✈️ Кто летит и может взять посылку", callback_data="subtype:trip")],
        [InlineKeyboardButton(text="📦 Кто хочет передать свою посылку", callback_data="subtype:parcel")]
    ])
    await state.set_state(SubscriptionFlow.looking_for)
    await callback.message.answer("Что отслеживать?", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("subtype:"))
async def sub_type(callback: CallbackQuery, state: FSMContext):
    post_type = callback.data.split(":")[1]
    await state.update_data(post_type=post_type)
    await state.set_state(SubscriptionFlow.from_country)
    await callback.message.answer("Выберите страну отправления:", reply_markup=countries_kb("subfrom"))
    await callback.answer()


@router.callback_query(F.data.startswith("mydeal:"))
async def open_my_deal(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if callback.from_user.id not in (deal["owner_user_id"], deal["requester_user_id"]):
        await callback.answer("Нет доступа", show_alert=True)
        return

    row = get_post(deal["post_id"])
    role = "владелец" if callback.from_user.id == deal["owner_user_id"] else "откликнувшийся"

    text = (
        f"🤝 <b>Сделка #{deal['id']}</b>\n\n"
        f"<b>ID объявления:</b> {deal['post_id']}\n"
        f"<b>Статус:</b> {format_deal_status(deal['status'])}\n"
        f"<b>Ваша роль:</b> {role}\n"
        f"<b>Создана:</b> {format_age(deal['created_at'])}"
    )

    await callback.message.answer(text)

    if row:
        await callback.message.answer(
            post_text(row),
            reply_markup=deal_open_kb(deal, callback.from_user.id)
        )
    else:
        await callback.message.answer(
            "Объявление, связанное со сделкой, не найдено.",
            reply_markup=deal_open_kb(deal, callback.from_user.id)
        )

    await callback.answer()

@router.callback_query(F.data.startswith("subfrom:"))
async def sub_from(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    await state.update_data(from_country=country)
    await state.set_state(SubscriptionFlow.to_country)
    await callback.message.answer("Выберите страну назначения:", reply_markup=countries_kb("subto"))
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

@router.callback_query(F.data.startswith("user_reviews:"))
async def user_reviews_handler(callback: CallbackQuery):
    user_id = int(callback.data.split(":")[1])
    reviews = get_user_reviews(user_id, limit=10)

    if not reviews:
        await callback.answer("Отзывов пока нет", show_alert=True)
        return

    parts = ["⭐ <b>Отзывы о пользователе</b>\n"]
    for r in reviews:
        author = r["full_name"] or r["username"] or "Пользователь"
        text = r["text"] or "Без текста"
        parts.append(
            f"\n<b>{html.escape(author)}</b> — {'⭐' * int(r['rating'])}\n"
            f"{html.escape(text)}"
        )

    text = "\n".join(parts)
    if len(text) > 4000:
        text = text[:3900] + "\n\n..."

    await callback.message.answer(text)
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
    await callback.message.answer("Напишите сообщение владельцу объявления. Я перешлю его через бота.")
    await callback.answer()


@router.callback_query(F.data.startswith("reply_contact:"))
async def reply_contact_handler(callback: CallbackQuery, state: FSMContext):
    _, post_id, target_user_id, deal_id = callback.data.split(":")
    await state.set_state(ContactFlow.message_text)
    await state.update_data(
        post_id=int(post_id),
        target_user_id=int(target_user_id),
        deal_id=None if deal_id == "0" else int(deal_id),
    )
    await callback.message.answer("Напишите ответ — я отправлю его через бота.")
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

        reply_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="💬 Ответить через бота",
                callback_data=f"reply_contact:{post_id}:{message.from_user.id}:{deal_id or 0}"
            )]
        ])

        await message.bot.send_message(
            target_user_id,
            f"💬 Новое сообщение по объявлению ID {post_id}:\n\n"
            f"От: {from_name}{username_part}\n\n{safe_text}",
            reply_markup=reply_kb
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

        await message.answer("✅ Сообщение отправлено через бота.", reply_markup=main_menu(message.from_user.id))

    except Exception as e:
        print(f"RELAY MESSAGE ERROR: {e}")
        await message.answer("Не удалось отправить сообщение. Возможно, пользователь еще не запускал бота.")

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

@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN env var")

    init_db()

    bot = Bot(
        BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

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
    asyncio.create_task(dispute_timeout_loop(bot))

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
