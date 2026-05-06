import asyncio
import html
import os 
import re
import time
import logging
import aiosqlite
from functools import lru_cache
from contextlib import closing
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from  aiogram import Bot, Dispatcher, F, Router
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("poputchik_bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "/data/bot.db")


db_dir = os.path.dirname(DB_PATH)
if db_dir:
    os.makedirs(db_dir, exist_ok=True)

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "")
BOT_USERNAME = os.getenv("BOT_USERNAME", "Poputchik_china_bot").lstrip("@")
ONBOARDING_STICKER_ID = "CAACAgQAAxkBAAFI6-dp-0Jiiwi4sUztljUnJehUIqNSlAACiQ4AAg_xkFNJFDb0IBgWDjsE"
ADMIN_IDS = {474671704}
MODERATION_ENABLED = False


BUMP_PRICE_TEXT = os.getenv(
    "BUMP_PRICE_TEXT",
    "Поднятие объявления оплачивается вручную. После оплаты администратор поднимет ваше объявление выше в выдаче."
)
VERIFICATION_PRICE_AMOUNT = int(os.getenv("VERIFICATION_PRICE_AMOUNT", "50"))
VERIFICATION_PRICE_CURRENCY = os.getenv("VERIFICATION_PRICE_CURRENCY", "CNY")

VERIF_STATUS_AWAITING_PAYMENT = "awaiting_payment"
VERIF_STATUS_PAYMENT_REVIEW = "payment_review"
VERIF_STATUS_DOCS_PENDING = "docs_pending"
VERIF_STATUS_SELFIE_PENDING = "selfie_pending"
VERIF_STATUS_REVIEW_PENDING = "review_pending"
VERIF_STATUS_APPROVED = "approved"
VERIF_STATUS_REJECTED = "rejected"
VERIF_STATUS_PAYMENT_REJECTED = "payment_rejected"

MAX_ACTIVE_POSTS_PER_USER = int(os.getenv("MAX_ACTIVE_POSTS_PER_USER", "5"))
MIN_SECONDS_BETWEEN_ACTIONS = int(os.getenv("MIN_SECONDS_BETWEEN_ACTIONS", "2"))
POST_TTL_DAYS = int(os.getenv("POST_TTL_DAYS", "14"))
COINCIDENCE_NOTIFY_LIMIT = int(os.getenv("COINCIDENCE_NOTIFY_LIMIT", "5"))
BUMP_PRICE_AMOUNT = int(os.getenv("BUMP_PRICE_AMOUNT", "10"))
BUMP_PRICE_CURRENCY = os.getenv("BUMP_PRICE_CURRENCY", "CNY")
DISPUTE_RESPONSE_HOURS = int(os.getenv("DISPUTE_RESPONSE_HOURS", "48"))
AUTO_HIDE_COMPLAINTS_THRESHOLD = int(os.getenv("AUTO_HIDE_COMPLAINTS_THRESHOLD", "3"))
POSTS_PAGE_SIZE = int(os.getenv("POSTS_PAGE_SIZE", "10"))
INLINE_PAGE_SIZE = int(os.getenv("INLINE_PAGE_SIZE", "10"))
MY_POSTS_PAGE_SIZE = int(os.getenv("MY_POSTS_PAGE_SIZE", "10"))
EXPIRE_WARN_DAYS = int(os.getenv("EXPIRE_WARN_DAYS", "3"))
MAX_POSTS_PER_10_MIN = int(os.getenv("MAX_POSTS_PER_10_MIN", "3"))
PROFILE_CACHE_TTL = int(os.getenv("PROFILE_CACHE_TTL", "300"))
SQLITE_BUSY_TIMEOUT_MS = int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "5000"))
MIN_SECONDS_BETWEEN_CHAT_MESSAGES = 3
MAX_CHAT_MESSAGES_PER_10_MIN = 20

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

# заявки на сделку (ещё НЕ сделка)
DEAL_REQUEST_PENDING = "pending"
DEAL_REQUEST_ACCEPTED = "accepted"
DEAL_REQUEST_DECLINED = "declined"

DISPUTE_OPEN = "open"
DISPUTE_WAITING_RESPONSE = "waiting_response"
DISPUTE_RESPONDED = "responded"
DISPUTE_EXPIRED = "expired"
DISPUTE_RESOLVED = "resolved"
DISPUTE_CLOSED_UNRESOLVED = "closed_unresolved"

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
        "Омск", "Ростов-на-Дону", "Улан-Удэ", "Красноярск",
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
    "delivery_date",  
    "weight",
    "description",
    "photo_choice",
    "contact",         
]

STEP_NUMBERS = {name: i + 1 for i, name in enumerate(STEP_ORDER)}

MAIN_MENU_TEXTS = {
    "✈️ Взять посылку",
    "📦 Отправить посылку",
    "🚀 Быстрая доставка (карго)",
    "🔎 Найти совпадения",
    "📋 Мои объявления",
    "🤝 Мои сделки",
    "🔥 Популярные маршруты",
    "🆕 Новые объявления",
    "🔔 Подписки",
    "📊 Статистика",
    "💰 Поднять объявление",
    "🛂 Верификация аккаунта",
    "🚩 Жалоба / Баг / Поддержка",
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

EDIT_FIELD_PROMPTS = {
    "description": "Введите новое описание:",
    "contact_note": "Введите новые контактные данные:",
    "weight_kg": "Выберите новый вес:",
}

WELCOME_TEXT = (
    "👋 <b>Привет.</b>\n\n"
    "Это <b>Попутчик Китай</b> — сервис для передачи посылок <b>из Китая и в Китай</b> через попутчиков.\n\n"
    "<b>Здесь можно отправить свою посылку или взять чужую по маршруту.</b>\n\n"
    "🔎 <b>Обязательно подпишись на канал с объявлениями:</b>\n"
    "👉 <a href='https://t.me/china_poputchik'>Открыть канал</a>\n\n"
   "🤖 Я сам ищу для вас подходящие совпадения и уведомляю, когда они появляются.\n\n"
   "В правом углу поисковой строки есть квадратик с 4-мя кружочками — нажми — это твой центр управления ботом.\n\n"
    "⬇️ <b>Синяя кнопка МЕНЮ — это только лишь меню с базовыми командами бота</b>"
)
ONBOARDING_TEXTS = {
    1: (
        "👋 <b>Добро пожаловать в Попутчик Китай</b>\n\n"
        "Передавайте посылки через людей, которые уже летят нужным маршрутом.\n\n"
        "📦 Нужно отправить посылку?\n"
        "✈️ Летите и готовы помочь другим людям?\n\n"
        "Платформа соединяет пользователей\n"
        "по подходящим маршрутам.\n\n"
        "Вы создаете объявление —\n"
        "система <b>автоматически найдет и уведомит вас о совпадении.</b>"
    ),

    2: (
    "📱 <b>Раньше попутчиков искали вручную</b>\n\n"
    "Люди рассылали сообщения в WeChat-группы, знакомым и друзьям.\n"
    "Это занимало много времени и терпения\n"
    "и часто <b>не давало результата.</b>\n\n"
    "Попутчик Китай делает поиск <b>невероятно простым.</b>\n\n"
    "Система <b>сама находит пользователей с подходящими маршрутами, пока вы пьете свой лате.</b>"
    ),

    3: (
        "📢 <b>Все объявления публикуются в канале</b>\n\n"
        "Каждая поездка и каждая посылка\n"
        "автоматически публикуются в нашем канале.\n\n"
        "Это основной поток объявлений сервиса.\n\n"
        "ОБЯЗАТЕЛЬНО ПОДПИШИСЬ на канал,чтобы:\n\n"
        "🔔 моментально видеть новые маршруты\n"
        "⚡ писать пользователям первым\n"
        "📦 быстрее находить попутчиков\n\n"
        "👉 <a href='https://t.me/china_poputchik'>Открыть канал</a>"
    ),
}

def onboarding_next_kb(screen: int):
    rows = []

    # если НЕ последний экран
    if screen < max(ONBOARDING_TEXTS.keys()):
        rows.append([InlineKeyboardButton(text="➡️ Далее", callback_data=f"onboarding_next:{screen}")])
        rows.append([InlineKeyboardButton(text="⏭ Пропустить", callback_data="onboarding_skip")])

    else:
        # последний экран → показываем финал
        rows.append([InlineKeyboardButton(text="✈️ Я лечу", callback_data="onboarding_action:trip")])
        rows.append([InlineKeyboardButton(text="📦 Отправить посылку", callback_data="onboarding_action:parcel")])
        rows.append([InlineKeyboardButton(text="🔎 Смотреть объявления", callback_data="onboarding_action:browse")])
        rows.append([InlineKeyboardButton(text="📢 Открыть канал", url="https://t.me/china_poputchik")])

    return InlineKeyboardMarkup(inline_keyboard=rows)
    

def now_ts() -> int:
    return int(time.time())


async def smart_form_answer(target, text: str, reply_markup=None):
    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=reply_markup)
        await target.answer()
    else:
        await target.answer(text, reply_markup=reply_markup)


DEAL_CONFIRM_DELAY_HOURS = 24


def can_confirm_deal_now(deal: aiosqlite.Row) -> bool:
    created_at = int(deal["created_at"] or 0)
    return now_ts() >= created_at + DEAL_CONFIRM_DELAY_HOURS * 3600


def time_left_until_deal_confirm(deal: aiosqlite.Row) -> str:
    created_at = int(deal["created_at"] or 0)
    unlock_at = created_at + DEAL_CONFIRM_DELAY_HOURS * 3600
    diff = unlock_at - now_ts()

    if diff <= 0:
        return "0 мин"

    hours = diff // 3600
    minutes = (diff % 3600) // 60

    if hours > 0:
        return f"{hours} ч {minutes} мин"
    return f"{minutes} мин"


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


class SafeDBConnection:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        await self.conn.close()

    def __getattr__(self, name):
        return getattr(self.conn, name)


async def connect_db():
    conn = await aiosqlite.connect(
        DB_PATH,
        timeout=max(5, SQLITE_BUSY_TIMEOUT_MS // 1000)
    )
    conn.row_factory = aiosqlite.Row

    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    await conn.execute("PRAGMA synchronous=NORMAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.execute("PRAGMA temp_store=MEMORY")
    await conn.execute("PRAGMA cache_size=-64000")

    return SafeDBConnection(conn)
    

def invalidate_user_profile_cache(user_id: Optional[int] = None):
    if user_id is None:
        _PROFILE_CACHE.clear()
        return
    _PROFILE_CACHE.pop(user_id, None)


_PROFILE_CACHE: dict[int, tuple[int, dict]] = {}


def cache_user_profile(user_id: int, payload: dict):
    _PROFILE_CACHE[user_id] = (now_ts(), payload)


def get_cached_user_profile(user_id: int) -> Optional[dict]:
    item = _PROFILE_CACHE.get(user_id)
    if not item:
        return None
    ts_cached, payload = item
    if now_ts() - ts_cached > PROFILE_CACHE_TTL:
        _PROFILE_CACHE.pop(user_id, None)
        return None
    return payload


async def run_db_write(query: str, params: tuple = ()):
    last_error = None
    for _ in range(3):
        try:
            async with await connect_db() as conn:
                cur = await conn.execute(query, params)
                await conn.commit()  # ← don't forget commit for writes!
                return cur
        except aiosqlite.OperationalError as e:
            last_error = e
            if "locked" not in str(e).lower():
                raise
            await asyncio.sleep(0.15)
    if last_error:
        raise last_error


async def can_send_chat_message(user_id: int) -> tuple[bool, Optional[str]]:
    async with await connect_db() as conn:
        cur = await conn.execute(
            "SELECT last_chat_message_at, chat_message_count_10min FROM users WHERE user_id=?",
            (user_id,)
        )
        row = await cur.fetchone()

        if not row:
            return True, None

        last_msg_at = int(row["last_chat_message_at"] or 0)
        msg_count = int(row["chat_message_count_10min"] or 0)
        now = now_ts()

        if now - last_msg_at > 600:
            msg_count = 0

        if now - last_msg_at < MIN_SECONDS_BETWEEN_CHAT_MESSAGES:
            wait = MIN_SECONDS_BETWEEN_CHAT_MESSAGES - (now - last_msg_at)
            return False, f"Слишком быстро. Подождите {wait} сек."

        if msg_count >= MAX_CHAT_MESSAGES_PER_10_MIN:
            return False, f"Лимит сообщений: {MAX_CHAT_MESSAGES_PER_10_MIN} в 10 минут."

        await conn.execute(
            "UPDATE users SET last_chat_message_at=?, chat_message_count_10min=? WHERE user_id=?",
            (now, msg_count + 1, user_id)
        )
        await conn.commit()

        return True, None
        

def go_my_deals_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🤝 Мои сделки",
                callback_data="back:my_deals"
            )
        ]
    ])


def admin_complaint_actions_kb(complaint_id: int, post_id: int, owner_user_id: Optional[int]):
    rows = [
        [InlineKeyboardButton(text="📄 Открыть объявление", callback_data=f"admincomplaint_openpost:{post_id}")],
        [InlineKeyboardButton(text="❌ Скрыть объявление", callback_data=f"admincomplaint_hidepost:{post_id}")],
        [InlineKeyboardButton(text="✅ Жалоба обработана", callback_data=f"admincomplaint_done:{complaint_id}")]
    ]

    if owner_user_id:
        rows.insert(
            2,
            [InlineKeyboardButton(text="🚫 Бан владельца", callback_data=f"admincomplaint_banuser:{owner_user_id}")]
        )

    return InlineKeyboardMarkup(inline_keyboard=rows)
    

async def ensure_column(conn, table: str, column: str, ddl: str):
    cur = await conn.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    cols = [r["name"] for r in rows]

    if column not in cols:
        await conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


async def init_db():
    async with await connect_db() as conn:
        await conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            created_at INTEGER NOT NULL,
            last_action_at INTEGER DEFAULT 0,
            is_banned INTEGER DEFAULT 0,
            is_verified INTEGER DEFAULT 0,
            dispute_no_response_count INTEGER DEFAULT 0,
            onboarding_completed INTEGER DEFAULT 0
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

        CREATE TABLE IF NOT EXISTS deal_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            requester_user_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_deal_requests_owner
        ON deal_requests(owner_user_id, status, created_at);

        CREATE INDEX IF NOT EXISTS idx_deal_requests_requester
        ON deal_requests(requester_user_id, status, created_at);

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

        CREATE TABLE IF NOT EXISTS verification_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'awaiting_payment',
            payment_amount INTEGER NOT NULL DEFAULT 0,
            payment_currency TEXT NOT NULL DEFAULT 'CNY',
            passport_photo_file_id TEXT,
            selfie_photo_file_id TEXT,
            rejection_reason TEXT,
            admin_user_id INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            paid_at INTEGER,
            reviewed_at INTEGER
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

        await conn.executescript("""
        CREATE TABLE IF NOT EXISTS chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            deal_id INTEGER,
            from_user_id INTEGER NOT NULL,
            to_user_id INTEGER NOT NULL,
            message_text TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            is_read INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS cargo_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            from_place TEXT NOT NULL,
            to_place TEXT NOT NULL,
            weight TEXT,
            cargo_desc TEXT NOT NULL,
            contact TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cargo_lead_access (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            cargo_user_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE(lead_id, cargo_user_id)
        );

        CREATE TABLE IF NOT EXISTS user_blacklist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            blocked_user_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            UNIQUE(user_id, blocked_user_id)
        );
        """)

        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_cargo_leads_created
        ON cargo_leads(created_at)
        """)

        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_cargo_leads_status
        ON cargo_leads(status)
        """)

        await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_cargo_lead_access_lead
        ON cargo_lead_access(lead_id)
        """)


        # ---- ensure columns ----
        await ensure_column(conn, "users", "failed_dispute_count", "failed_dispute_count INTEGER DEFAULT 0")
        await ensure_column(conn, "users", "review_status", "review_status TEXT DEFAULT 'clear'")
        await ensure_column(conn, "users", "review_requested_at", "review_requested_at INTEGER")
        await ensure_column(conn, "users", "review_admin_id", "review_admin_id INTEGER")
        await ensure_column(conn, "users", "is_cargo", "is_cargo INTEGER DEFAULT 0")
        await ensure_column(conn, "users", "cargo_company_name", "cargo_company_name TEXT")
        await ensure_column(conn, "users", "active_chat_target_user_id", "active_chat_target_user_id INTEGER")
        await ensure_column(conn, "users", "active_chat_post_id", "active_chat_post_id INTEGER")
        await ensure_column(conn, "users", "active_chat_deal_id", "active_chat_deal_id INTEGER")
        await ensure_column(conn, "users", "last_chat_message_at", "last_chat_message_at INTEGER DEFAULT 0")
        await ensure_column(conn, "users", "chat_message_count_10min", "chat_message_count_10min INTEGER DEFAULT 0")
        await ensure_column(conn, "users", "verified_at", "verified_at INTEGER")
        await ensure_column(conn, "users", "verification_type", "verification_type TEXT")

        await ensure_column(conn, "posts", "expire_warned_at", "expire_warned_at INTEGER")

        await ensure_column(conn, "route_subscriptions", "from_city", "from_city TEXT")
        await ensure_column(conn, "route_subscriptions", "to_city", "to_city TEXT")

        await ensure_column(conn, "cargo_leads", "photo_file_id", "photo_file_id TEXT")
        await ensure_column(conn, "cargo_leads", "delivery_date", "delivery_date TEXT")

        await conn.commit()


async def db_fetchone(query: str, params: tuple = ()):
    async with await connect_db() as conn:
        cur = await conn.execute(query, params)
        return await cur.fetchone()


async def db_fetchall(query: str, params: tuple = ()):
    async with await connect_db() as conn:
        cur = await conn.execute(query, params)
        return await cur.fetchall()


async def db_execute(query: str, params: tuple = ()):
    async with await connect_db() as conn:
        cur = await conn.execute(query, params)
        await conn.commit()
        return cur


async def upsert_user(message_or_callback):
    user = message_or_callback.from_user
    existing = await db_fetchone("SELECT created_at FROM users WHERE user_id=?", (user.id,))
    created_at = int(existing["created_at"]) if existing else now_ts()
    await db_execute("""
        INSERT INTO users (user_id, username, full_name, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            full_name=excluded.full_name
    """, (user.id, user.username, (user.full_name or "")[:200], created_at))


async def is_onboarding_completed(user_id: int) -> bool:
    row = await db_fetchone(
        "SELECT onboarding_completed FROM users WHERE user_id=?",
        (user_id,)
    )
    return bool(row and row["onboarding_completed"])


async def set_onboarding_completed(user_id: int):
    await db_execute(
        "UPDATE users SET onboarding_completed=1 WHERE user_id=?",
        (user_id,)
    )


async def get_recent_posts(limit: int = 10, offset: int = 0):
    return await db_fetchall("""
        SELECT p.*, u.username, u.full_name, COALESCE(u.is_verified, 0) AS is_verified
        FROM posts p
        LEFT JOIN users u ON u.user_id = p.user_id
        WHERE p.status='active'
          AND (p.expires_at IS NULL OR p.expires_at > ?)
        ORDER BY COALESCE(u.is_verified, 0) DESC, COALESCE(p.bumped_at, p.created_at) DESC
        LIMIT ? OFFSET ?
    """, (now_ts(), limit, offset))


async def count_recent_posts() -> int:
    row = await db_fetchone("""
        SELECT COUNT(*) AS c
        FROM posts
        WHERE status='active'
          AND (expires_at IS NULL OR expires_at > ?)
    """, (now_ts(),))
    return int(row["c"] or 0)
    

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def is_user_banned(user_id: int) -> bool:
    row = await db_fetchone(
        "SELECT is_banned FROM users WHERE user_id=?",
        (user_id,)
    )
    return bool(row and row["is_banned"])


async def ban_user(user_id: int):
    async with await connect_db() as conn:
        await conn.execute(
            "UPDATE users SET is_banned=1 WHERE user_id=?",
            (user_id,)
        )
        await conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE user_id=? AND status IN ('active','pending','inactive')",
            (STATUS_INACTIVE, now_ts(), user_id)
        )
        await conn.commit()

    invalidate_user_profile_cache(user_id)


async def unban_user(user_id: int):
    await db_execute(
        "UPDATE users SET is_banned=0 WHERE user_id=?",
        (user_id,)
    )
    invalidate_user_profile_cache(user_id)


async def unhold_user(user_id: int):
    await db_execute("""
        UPDATE users
        SET review_status='clear',
            review_requested_at=NULL,
            review_admin_id=NULL
        WHERE user_id=?
    """, (user_id,))
    invalidate_user_profile_cache(user_id)


async def hide_user_posts_from_channel(bot: Bot, user_id: int):
    rows = await db_fetchall("""
        SELECT p.*, u.username, u.full_name
        FROM posts p
        LEFT JOIN users u ON u.user_id = p.user_id
        WHERE p.user_id=?
          AND p.channel_message_id IS NOT NULL
    """, (user_id,))

    for row in rows:
        await remove_post_from_channel(bot, row)

    await db_execute(
        "UPDATE posts SET channel_message_id=NULL WHERE user_id=? AND channel_message_id IS NOT NULL",
        (user_id,)
    )


async def ban_user_with_cleanup(bot: Bot, user_id: int):
    await hide_user_posts_from_channel(bot, user_id)
    await ban_user(user_id)

async def hold_user_with_cleanup(bot: Bot, user_id: int, admin_id: int):
    await hide_user_posts_from_channel(bot, user_id)

    async with await connect_db() as conn:
        await conn.execute("""
            UPDATE users
            SET review_status='hold',
                review_requested_at=?,
                review_admin_id=?
            WHERE user_id=?
        """, (now_ts(), admin_id, user_id))

        await conn.execute("""
            UPDATE posts
            SET status=?, updated_at=?
            WHERE user_id=? AND status IN ('active','pending','inactive')
        """, (STATUS_INACTIVE, now_ts(), user_id))

        await conn.commit()

    invalidate_user_profile_cache(user_id)

    text = (
        "⚠️ <b>Ваш аккаунт временно ограничен для проверки.</b>\n\n"
        "Это стандартная мера безопасности сервиса.\n\n"
        "Чтобы продолжить использование, отправьте прямо сюда в бот:\n\n"
        "1. Короткое селфи-видео до 5 секунд\n"
        "— лицо должно совпадать с вашей аватаркой\n\n"
        "2. Контакт для связи\n"
        "— желательно WeChat ID\n\n"
        "После проверки доступ может быть восстановлен."
    )

    try:
        await bot.send_message(user_id, text)
    except Exception as e:
        logger.warning("Не удалось отправить HOLD сообщение пользователю %s: %s", user_id, e)


async def anti_spam_check(user_id: int) -> Optional[str]:
    async with await connect_db() as conn:
        cur = await conn.execute(
            "SELECT is_banned, last_action_at, review_status FROM users WHERE user_id=?",
            (user_id,)
        )
        row = await cur.fetchone()

        if not row:
            return None

        if row["is_banned"]:
            return "Ваш аккаунт ограничен администратором."

        if row["review_status"] == "hold":
            return "Ваш аккаунт временно ограничен для проверки. Отправьте сюда в бот селфи-видео до 5 секунд и WeChat ID."

        last_action_at = row["last_action_at"] or 0

        if now_ts() - last_action_at < MIN_SECONDS_BETWEEN_ACTIONS:
            return "Слишком быстро. Подождите пару секунд и попробуйте снова."

        await conn.execute(
            "UPDATE users SET last_action_at=? WHERE user_id=?",
            (now_ts(), user_id)
        )
        await conn.commit()

    return None


async def active_post_count(user_id: int) -> int:
    row = await db_fetchone(
        "SELECT COUNT(*) AS c FROM posts WHERE user_id=? AND status IN ('pending','active')",
        (user_id,)
    )
    return int(row["c"] or 0)


async def get_user_post_limit(user_id: int) -> int:
    if user_id in ADMIN_IDS:
        return 999
    if await is_user_verified(user_id):
        return MAX_ACTIVE_POSTS_PER_USER * 2
    return MAX_ACTIVE_POSTS_PER_USER * 5


async def user_rating_summary(user_id: int) -> Tuple[float, int]:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT AVG(rating) AS avg_rating, COUNT(*) AS cnt
            FROM reviews
            WHERE reviewed_user_id=?
        """, (user_id,))
        row = await cur.fetchone()

        return float(row["avg_rating"] or 0), int(row["cnt"] or 0)


async def user_service_days(user_id: int) -> int:
    row = await db_fetchone(
        "SELECT created_at FROM users WHERE user_id=?",
        (user_id,)
    )
    if not row or not row["created_at"]:
        return 0
    return max(0, (now_ts() - int(row["created_at"])) // 86400)


async def user_service_text(user_id: int) -> str:
    days = await user_service_days(user_id)

    if days < 30:
        return f"{days} дн"
    if days < 365:
        return f"{max(1, days // 30)} мес"
    return f"{max(1, days // 365)} г"


async def get_user_profile_short(user_id: int) -> dict:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT
                u.is_verified,
                COALESCE(u.is_cargo, 0) AS is_cargo,
                u.failed_dispute_count,
                u.dispute_no_response_count,
                u.created_at,
                AVG(r.rating) AS avg_rating,
                COUNT(r.id) AS reviews_count,
                (
                    SELECT COUNT(*)
                    FROM deals d
                    WHERE d.status='completed'
                      AND (d.owner_user_id=u.user_id OR d.requester_user_id=u.user_id)
                ) AS completed_deals
            FROM users u
            LEFT JOIN reviews r ON r.reviewed_user_id = u.user_id
            WHERE u.user_id=?
            GROUP BY u.user_id
        """, (user_id,))
        row = await cur.fetchone()

    if not row:
        return {
            "verified": False,
            "is_cargo": False,
            "has_warning": False,
            "rating_line": None,
            "completed_deals": 0,
            "service_text": "0 дн",
        }

    avg_rating = float(row["avg_rating"] or 0)
    reviews_count = int(row["reviews_count"] or 0)

    rating_line = None
    if reviews_count > 0:
        stars = "⭐" * max(1, min(5, round(avg_rating)))
        rating_line = f"{stars} {avg_rating:.1f} ({reviews_count} {reviews_word(reviews_count)})"

    created_at = int(row["created_at"] or now_ts())
    days = max(0, (now_ts() - created_at) // 86400)

    if days < 30:
        service_text = f"{days} дн"
    elif days < 365:
        service_text = f"{max(1, days // 30)} мес"
    else:
        service_text = f"{max(1, days // 365)} г"

    return {
        "verified": bool(row["is_verified"]),
        "is_cargo": bool(row["is_cargo"]),
        "has_warning": (
            int(row["failed_dispute_count"] or 0) > 0
            or int(row["dispute_no_response_count"] or 0) > 0
        ),
        "rating_line": rating_line,
        "completed_deals": int(row["completed_deals"] or 0),
        "service_text": service_text,
    }
    

async def get_user_profile_short_cached(user_id: int) -> dict:
    cached = get_cached_user_profile(user_id)
    if cached is not None:
        return cached

    payload = await get_user_profile_short(user_id)
    cache_user_profile(user_id, payload)
    return payload
    

async def user_completed_deals_count(user_id: int) -> int:
    row = await db_fetchone("""
        SELECT COUNT(*) AS cnt
        FROM deals
        WHERE status='completed'
          AND (owner_user_id=? OR requester_user_id=?)
    """, (user_id, user_id))
    return int(row["cnt"] or 0)


async def user_has_warning_badge(user_id: int) -> bool:
    row = await db_fetchone("""
        SELECT failed_dispute_count, dispute_no_response_count
        FROM users
        WHERE user_id=?
    """, (user_id,))

    if not row:
        return False

    return (
        int(row["failed_dispute_count"] or 0) > 0
        or int(row["dispute_no_response_count"] or 0) > 0
    )


async def is_user_verified(user_id: int) -> bool:
    row = await db_fetchone(
        "SELECT is_verified FROM users WHERE user_id=?",
        (user_id,)
    )
    return bool(row and row["is_verified"])


def sort_posts_with_verified_priority(rows: List[aiosqlite.Row]) -> List[aiosqlite.Row]:
    def sort_key(row):
        verified = int(row["is_verified"] or 0)
        bumped_or_created = row["bumped_at"] or row["created_at"] or 0
        return (verified, bumped_or_created)

    return sorted(rows, key=sort_key, reverse=True)


async def find_user_by_username(username: str):
    username = username.lstrip("@").strip().lower()
    row = await db_fetchone("""
        SELECT user_id, username, full_name, is_banned, created_at
        FROM users
        WHERE LOWER(COALESCE(username, '')) = ?
        LIMIT 1
    """, (username,))
    return row


async def find_user_by_id(user_id: int):
    return await db_fetchone("""
        SELECT user_id, username, full_name, is_banned, created_at
        FROM users
        WHERE user_id = ?
        LIMIT 1
    """, (user_id,))


async def search_users(query: str, limit: int = 10):
    q = f"%{query.strip().lower()}%"
    return await db_fetchall("""
        SELECT user_id, username, full_name, is_banned, created_at
        FROM users
        WHERE LOWER(COALESCE(username, '')) LIKE ?
           OR LOWER(COALESCE(full_name, '')) LIKE ?
           OR CAST(user_id AS TEXT) LIKE ?
        ORDER BY created_at DESC
        LIMIT ?
    """, (q, q, q, limit))
    

async def build_admin_user_profile_text(user_id: int) -> Optional[str]:
    profile = await get_user_profile_full(user_id)
    user = profile["user"]

    if not user:
        return None

    avg_rating, reviews_count = await user_rating_summary(user_id)
    
    return (
        f"👤 <b>Профиль пользователя</b>\n\n"
        f"<b>USER_ID:</b> {user_id}\n"
        f"<b>Username:</b> @{html.escape(user['username']) if user['username'] else 'нет'}\n"
        f"<b>Имя:</b> {html.escape(user['full_name'] or 'не указано')}\n"
        f"<b>Верификация:</b> {'да' if user['is_verified'] else 'нет'}\n"
        f"<b>Бан:</b> {'да' if user['is_banned'] else 'нет'}\n"
        f"<b>Карго-партнер:</b> {'да' if user['is_cargo'] else 'нет'}\n"
        f"<b>Объявлений всего:</b> {profile['posts_count']}\n"
        f"<b>Активных объявлений:</b> {profile['active_posts']}\n"
        f"<b>Завершенных сделок:</b> {profile['completed_deals']}\n"
        f"<b>Жалоб на пользователя:</b> {profile['complaints_received']}\n"
        f"<b>Рейтинг:</b> {avg_rating:.1f} ({reviews_count} {reviews_word(reviews_count)})\n"
    )
    

async def show_user_posts(target, user_id: int):
    posts = await db_fetchall("""
        SELECT * FROM posts
        WHERE user_id=? AND status != 'deleted'
        ORDER BY created_at DESC
        LIMIT 30
    """, (user_id,))

    if not posts:
        await target.answer("У вас пока нет объявлений.")
        return

    await target.answer(
        "📋 Ваши объявления:",
        reply_markup=my_posts_kb(posts)
    )
    

async def show_user_deals_sections(target, user_id: int, include_descriptions: bool = False):
    deals = await list_user_deals(user_id)

    if not deals:
        await target.answer("У вас пока нет сделок.")
        return

    in_progress, disputes, finished = split_deals_by_sections(deals)

    if in_progress:
        text = "🟢 <b>Сделки в процессе</b>"
        if include_descriptions:
            text += "\nЗдесь сделки, по которым сейчас идёт передача или ожидание подтверждения."
        await target.answer(text, reply_markup=await deal_section_kb(in_progress))

    if disputes:
        text = "⚖️ <b>Споры</b>"
        if include_descriptions:
            text += "\nЗдесь сделки, по которым открыт спор или ожидается решение."
        await target.answer(text, reply_markup=await deal_section_kb(disputes))

    if finished:
        text = "✅ <b>Завершённые и закрытые</b>"
        if include_descriptions:
            text += "\nЗдесь завершённые, неуспешные и отменённые сделки."
        await target.answer(text, reply_markup=await deal_section_kb(finished))
        

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


async def format_rating_line(user_id: int) -> Optional[str]:
    avg_rating, cnt = await user_rating_summary(user_id)

    if cnt <= 0:
        return None

    stars = "⭐" * max(1, min(5, round(avg_rating)))
    return f"{stars} {avg_rating:.1f} ({cnt} {reviews_word(cnt)})"


async def get_user_reviews(user_id: int, limit: int = 10):
    return await db_fetchall("""
        SELECT r.rating, r.text, r.created_at, u.username, u.full_name
        FROM reviews r
        LEFT JOIN users u ON u.user_id = r.reviewer_user_id
        WHERE r.reviewed_user_id=?
        ORDER BY r.created_at DESC
        LIMIT ?
    """, (user_id, limit))


async def get_username_by_user_id(user_id: int) -> Optional[str]:
    row = await db_fetchone(
        "SELECT username FROM users WHERE user_id=?",
        (user_id,)
    )
    return row["username"] if row and row["username"] else None


async def has_user_left_review_for_deal(deal: aiosqlite.Row, reviewer_user_id: int) -> bool:
    reviewed_user_id = (
        deal["requester_user_id"]
        if reviewer_user_id == deal["owner_user_id"]
        else deal["owner_user_id"]
    )

    row = await db_fetchone("""
        SELECT 1 FROM reviews
        WHERE reviewer_user_id=? AND reviewed_user_id=? AND post_id=?
        LIMIT 1
    """, (reviewer_user_id, reviewed_user_id, deal["post_id"]))

    return row is not None


async def get_open_dispute_by_deal(deal_id: int) -> Optional[aiosqlite.Row]:
    return await db_fetchone("""
        SELECT *
        FROM disputes
        WHERE deal_id=? AND status IN (?, ?, ?)
        ORDER BY id DESC
        LIMIT 1
    """, (
        deal_id,
        DISPUTE_OPEN,
        DISPUTE_WAITING_RESPONSE,
        DISPUTE_RESPONDED
    ))


async def create_dispute(deal_id: int, opened_by_user_id: int, against_user_id: int, reason_text: str) -> int:
    ts = now_ts()
    deadline = ts + DISPUTE_RESPONSE_HOURS * 3600

    async with await connect_db() as conn:
        cur = await conn.execute("""
            INSERT INTO disputes (
                deal_id, opened_by_user_id, against_user_id,
                status, reason_text, created_at, updated_at, response_deadline_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            deal_id, opened_by_user_id, against_user_id,
            DISPUTE_WAITING_RESPONSE, reason_text, ts, ts, deadline
        ))
        await conn.commit()
        return int(cur.lastrowid)


async def save_dispute_response(dispute_id: int, response_text: str):
    await db_execute("""
        UPDATE disputes
        SET response_text=?, status=?, responded_at=?, updated_at=?
        WHERE id=?
    """, (response_text, DISPUTE_RESPONDED, now_ts(), now_ts(), dispute_id))


async def get_dispute(dispute_id: int) -> Optional[aiosqlite.Row]:
    return await db_fetchone(
        "SELECT * FROM disputes WHERE id=?",
        (dispute_id,)
    )


def short_post_type(post_type: str) -> str:
    return "✈️ Попутчик" if post_type == TYPE_TRIP else "📦 Посылка"


async def ensure_deal_request(post_id: int, owner_user_id: int, requester_user_id: int) -> tuple[int, bool]:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT id
            FROM deal_requests
            WHERE post_id=? AND owner_user_id=? AND requester_user_id=? AND status=?
            ORDER BY id DESC
            LIMIT 1
        """, (post_id, owner_user_id, requester_user_id, DEAL_REQUEST_PENDING))
        row = await cur.fetchone()

        if row:
            return int(row["id"]), False

        cur = await conn.execute("""
            INSERT INTO deal_requests (
                post_id, owner_user_id, requester_user_id, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            post_id, owner_user_id, requester_user_id,
            DEAL_REQUEST_PENDING, now_ts(), now_ts()
        ))
        await conn.commit()
        return int(cur.lastrowid), True


async def get_deal_request(request_id: int) -> Optional[aiosqlite.Row]:
    return await db_fetchone("""
        SELECT *
        FROM deal_requests
        WHERE id=?
    """, (request_id,))


def format_deal_status(status: str) -> str:
    mapping = {
        DEAL_ACCEPTED: "сделка принята",
        DEAL_COMPLETED_BY_OWNER: "подтвердил владелец",
        DEAL_COMPLETED_BY_REQUESTER: "подтвердил откликнувшийся",
        DEAL_COMPLETED: "сделка завершена",
        DEAL_FAILED: "сделка неуспешна",
        DEAL_CANCELLED: "сделка отменена",
        DEAL_DISPUTE_OPEN: "спор активен",
        DEAL_DISPUTE_WAITING: "ожидается ответ по спору",
        DEAL_DISPUTE_RESOLVED: "спор решен",
        DEAL_REQUEST_PENDING: "заявка на сделку",
        DEAL_REQUEST_ACCEPTED: "заявка принята",
        DEAL_REQUEST_DECLINED: "заявка отклонена",
    }
    return mapping.get(status, status)


def format_post_status(status: str) -> str:
    mapping = {
        STATUS_ACTIVE: "активно",
        STATUS_INACTIVE: "неактивно",
        STATUS_PENDING: "на модерации",
        STATUS_REJECTED: "отклонено",
        STATUS_EXPIRED: "истекло",
        STATUS_DELETED: "удалено",
    }
    return mapping.get(status, status)


async def set_active_chat(user_id: int, target_user_id: int, post_id: int, deal_id: Optional[int] = None):
    await db_execute("""
        UPDATE users
        SET active_chat_target_user_id=?,
            active_chat_post_id=?,
            active_chat_deal_id=?
        WHERE user_id=?
    """, (target_user_id, post_id, deal_id, user_id))
    

async def get_active_chat(user_id: int) -> Optional[aiosqlite.Row]:
    return await db_fetchone("""
        SELECT active_chat_target_user_id, active_chat_post_id, active_chat_deal_id
        FROM users
        WHERE user_id=?
    """, (user_id,))


async def clear_active_chat(user_id: int):
    await db_execute("""
        UPDATE users
        SET active_chat_target_user_id=NULL,
            active_chat_post_id=NULL,
            active_chat_deal_id=NULL
        WHERE user_id=?
    """, (user_id,))


def deal_status_explanation(status: str, viewer_is_owner: bool) -> str:
    if status == DEAL_CONTACTED:
        return (
            "Контакт начат. Один из пользователей начал общение по объявлению.\n"
            "Теперь вы можете обсудить детали и договориться о передаче посылки."
        )

    if status == DEAL_OFFERED:
        return (
            "Сделка предложена и ожидает решения второй стороны.\n"
            "Когда она будет принята, появятся кнопки завершения сделки."
        )

    if status == DEAL_ACCEPTED:
        return (
            "Сделка принята обеими сторонами.\n"
            "После передачи посылки подтвердите завершение сделки."
        )

    if status == DEAL_COMPLETED_BY_OWNER:
        return (
            "Владелец объявления подтвердил завершение сделки.\n"
            "Ожидается подтверждение второй стороны."
        )

    if status == DEAL_COMPLETED_BY_REQUESTER:
        return (
            "Откликнувшийся пользователь подтвердил завершение.\n"
            "Ожидается подтверждение владельца объявления."
        )

    if status == DEAL_COMPLETED:
        return "Сделка завершена. Теперь можно оставить отзыв."

    if status == DEAL_FAILED:
        return "Сделка завершилась без результата."

    if status == DEAL_CANCELLED:
        return "Сделка была отменена."

    if status == DEAL_DISPUTE_WAITING:
        return "Открыт спор. Сейчас ожидается ответ второй стороны."

    if status == DEAL_DISPUTE_OPEN:
        return "Спор активен. Ожидается решение первой стороны."

    if status == DEAL_DISPUTE_RESOLVED:
        return "Спор решен."

    return "Статус сделки обновлен."
    

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


async def post_text(row, for_channel: bool = False) -> str:
    if row is None:
        raise ValueError("post_text получил None вместо объявления")

    if not hasattr(row, "keys"):
        raise ValueError("post_text получил неправильный объект: нет keys()")

    if "id" not in row.keys():
        raise ValueError("post_text получил неправильный объект: отсутствует id")

    route = html.escape(row["from_country"])
    if row["from_city"]:
        route += f", {html.escape(row['from_city'])}"
    route += " → " + html.escape(row["to_country"])
    if row["to_city"]:
        route += f", {html.escape(row['to_city'])}"

    owner_user_id = row["user_id"]
    profile = await get_user_profile_short_cached(owner_user_id)

    owner_username = row["username"] if "username" in row.keys() else None
    owner_full_name = row["full_name"] if "full_name" in row.keys() else None

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

    if "photo_file_id" in row.keys() and row["photo_file_id"]:
        lines.append("<b>Фото посылки:</b> доступно по кнопке ниже")

    lines.append("")
    lines.append("<b>👤 Профиль пользователя</b>")

    if owner_full_name:
        short_name = owner_full_name.strip().split()[0]
        lines.append(f"🪪 <b>Имя:</b> {html.escape(short_name)}")

    if owner_user_id in ADMIN_IDS:
        lines.append("🔰 <b>АДМИН СЕРВИСА</b> 🔰")

    status_parts = []

    if profile.get("is_cargo"):
        status_parts.append("🚀 Карго-партнер")

    if profile["has_warning"]:
        status_parts.append("⚠️ Были спорные сделки")

    if status_parts:
        lines.append(f"🏷 <b>Статус:</b> {' | '.join(status_parts)}")

    if profile["rating_line"]:
        lines.append(f"⭐ <b>Рейтинг:</b> {profile['rating_line']}")
    else:
        lines.append("⭐ <b>Рейтинг:</b> пока нет отзывов")

    lines.append(f"📦 <b>Передач:</b> {profile['completed_deals']}")
    lines.append(f"📅 <b>В сервисе:</b> {profile['service_text']}")

    lines.append("")
    lines.append(f"<b>ID объявления:</b> {row['id']}")

    lines.append("")
    lines.append("───────────────")

    if profile["verified"]:
       lines.append("🛂 <b>Паспорт верифицирован</b>")
    else:
       lines.append("🛂 <b>Паспорт не верифицирован</b>")
       lines.append("📈 <i>Верификация повышает доверие и увеличивает шанс отклика.</i>")

    if for_channel:
        lines.append("")
        lines.append(
            "Откройте объявление и напишите пользователю.\n"
            "Возможно, ваша посылка уже почти в пути ✈️📦."
        )
    else:
        if owner_username:
            lines.append(f"<b>Telegram:</b> @{html.escape(owner_username)}")

    return "\n".join(lines)
    

async def send_post_card(
    target,
    row,
    *,
    with_age: bool = False,
    prefix_text: Optional[str] = None,
    reply_markup=None
):
    text = await post_text(row)

    if prefix_text:
        text = f"{prefix_text}\n\n{text}"

    if with_age:
        text += f"\n\n<b>Добавлено:</b> {format_age(row['created_at'])}"

    if len(text) > 4000:
        text = text[:3900] + "\n\n..."

    kb = reply_markup if reply_markup is not None else await public_post_kb(
        row["id"],
        row["user_id"]
    )

    if hasattr(target, "answer"):
        await target.answer(text, reply_markup=kb)
        return

    if hasattr(target, "message") and hasattr(target.message, "answer"):
        await target.message.answer(text, reply_markup=kb)
        return

    raise ValueError("send_post_card получил неподдерживаемый target")


async def send_post_card_to_user(
    bot: Bot,
    user_id: int,
    row,
    *,
    with_age: bool = False,
    prefix_text: Optional[str] = None,
    reply_markup=None
):
    text = await post_text(row)

    if prefix_text:
        text = f"{prefix_text}\n\n{text}"

    if with_age:
        text += f"\n\n<b>Добавлено:</b> {format_age(row['created_at'])}"

    if len(text) > 4000:
        text = text[:3900] + "\n\n..."

    kb = reply_markup if reply_markup is not None else await public_post_kb(
        row["id"],
        row["user_id"]
    )

    await bot.send_message(
        user_id,
        text,
        reply_markup=kb
    )
    

async def show_onboarding_screen(target, screen: int):
    text = ONBOARDING_TEXTS.get(screen)

    if not text:
        await target.answer("Ошибка онбординга.")
        return

    kb = onboarding_next_kb(screen)

    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=kb)
    else:
        await target.answer(text, reply_markup=kb)
        

def chunk_buttons(items: List[tuple], prefix: str, per_row: int = 2):
    rows = []
    row = []

    for label, value in items:
        row.append(
            InlineKeyboardButton(
                text=label,
                callback_data=f"{prefix}:{value}"
            )
        )

        if len(row) == per_row:
            rows.append(row)
            row = []

    if row:
        rows.append(row)

    return rows


def with_back(rows: List[List[InlineKeyboardButton]], include_back: bool = True, back_callback: str = "create_back"):
    if include_back:
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def back_only_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")]
    ])

def cargo_back_only_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back:cargo")]
    ])
    

def countries_kb(prefix: str):
    return InlineKeyboardMarkup(inline_keyboard=chunk_buttons(COUNTRY_OPTIONS, prefix, 2))


def countries_select_kb(prefix: str, include_back: bool = False, back_callback: str = "create_back"):
    rows = chunk_buttons(COUNTRY_OPTIONS, prefix, 2)
    rows.append([InlineKeyboardButton(text=MANUAL_COUNTRY, callback_data=f"{prefix}:__manual__")])
    return with_back(rows, include_back, back_callback)
    

def cities_select_kb(prefix: str, country: str, include_back: bool = True, back_callback: str = "create_back"):
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
    return with_back(rows, include_back, back_callback)


def subscription_cities_kb(prefix: str, country: str):
    cities = COUNTRY_CITIES_RU.get(country, [])
    rows, row = [], []
    for city in cities:
        row.append(InlineKeyboardButton(text=city, callback_data=f"{prefix}:{city}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="Не важно", callback_data=f"{prefix}:__skip__")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def weight_select_kb():
    rows = chunk_buttons([(w, w) for w in POPULAR_WEIGHTS], "weightpick", 2)
    rows.append([InlineKeyboardButton(text=MANUAL_WEIGHT, callback_data="weightpick:__manual__")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def cargo_weight_select_kb():
    rows = []
    weights = ["0.5 кг", "1 кг", "2 кг", "3 кг", "5 кг", "10 кг", "20 кг", "Более 20 кг"]

    for i in range(0, len(weights), 2):
        row = []
        for w in weights[i:i+2]:
            row.append(
                InlineKeyboardButton(
                    text=w,
                    callback_data=f"cargo_weight:{w}"
                )
            )
        rows.append(row)

    rows.append([
        InlineKeyboardButton(
            text="✏️ Указать другой вес",
            callback_data="cargo_weight:__manual__"
        )
    ])

    rows.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back:cargo")
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def edit_weight_select_kb(post_id: int):
    rows = []
    row = []

    for weight in POPULAR_WEIGHTS:
        row.append(
            InlineKeyboardButton(
                text=weight,
                callback_data=f"editweightpick:{post_id}:{weight}"
            )
        )
        if len(row) == 2:
            rows.append(row)
            row = []

    if row:
        rows.append(row)

    rows.append([
        InlineKeyboardButton(
            text=MANUAL_WEIGHT,
            callback_data=f"editweightpick:{post_id}:__manual__"
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=f"editpost_back_to_fields:{post_id}"
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)
    

def date_select_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="В течение недели", callback_data="datepick:week")],
        [InlineKeyboardButton(text="В течение месяца", callback_data="datepick:month")],
        [InlineKeyboardButton(text="✏️ Указать точную дату", callback_data="datepick:manual")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")],
    ])


def cargo_date_select_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="В течение недели", callback_data="datepick:week")],
        [InlineKeyboardButton(text="В течение месяца", callback_data="datepick:month")],
        [InlineKeyboardButton(text="✏️ Указать точную дату", callback_data="datepick:manual")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back:cargo")],
    ])


def photo_choice_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📷 Добавить фото посылки", callback_data="photo_choice:add")],
        [InlineKeyboardButton(text="⏭ Пропустить", callback_data="photo_choice:skip")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="create_back")],
    ])


def cargo_photo_choice_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 Добавить фото", callback_data="cargo_photo_choice:add")],
        [InlineKeyboardButton(text="⏭ Без фото", callback_data="cargo_photo_choice:skip")],
    ])


def my_posts_kb(posts: List[aiosqlite.Row], offset: int = 0):
    rows = []

    for index, p in enumerate(posts, start=offset + 1):
        icon = "✈️" if p["post_type"] == TYPE_TRIP else "📦"
        status_text = format_post_status(p["status"])

        label = f"{index}. {icon} {p['from_country']} → {p['to_country']} · ID {p['id']} · {status_text}"

        rows.append([
            InlineKeyboardButton(
                text=label[:64],
                callback_data=f"mypost:{p['id']}"
            )
        ])

    return InlineKeyboardMarkup(
        inline_keyboard=rows or [[InlineKeyboardButton(text="Нет объявлений", callback_data="noop")]]
    )


def dispute_failed_opened_by_kb(deal_id: int):
    rows = [
        [
            InlineKeyboardButton(
                text="⭐ Оставить отзыв",
                callback_data=f"deal_review:{deal_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="🆘 Связаться с администратором",
                callback_data=f"contact_admin:{deal_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data="back:my_deals"
            )
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def dispute_failed_against_kb(deal_id: int):
    rows = [
        [
            InlineKeyboardButton(
                text="⭐ Оставить отзыв",
                callback_data=f"deal_review:{deal_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data="back:my_deals"
            )
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def deal_list_kb(deals: List[aiosqlite.Row]):
    rows = []

    for d in deals:
        if d["status"] == DEAL_ACCEPTED:
            status_icon = "🟢"
        elif d["status"] in (DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
            status_icon = "🟦"
        elif d["status"] == DEAL_COMPLETED:
            status_icon = "✅"
        elif d["status"] in (DEAL_FAILED, DEAL_CANCELLED):
            status_icon = "❌"
        elif d["status"] in (DEAL_DISPUTE_OPEN, DEAL_DISPUTE_WAITING, DEAL_DISPUTE_RESOLVED):
            status_icon = "⚖️"
        else:
            status_icon = "🤝"

        title = await deal_title(d)
        label = f"{status_icon} {title}"

        rows.append([
            InlineKeyboardButton(
                text=label[:64],
                callback_data=f"mydeal:{d['id']}"
            )
        ])

    if not rows:
        rows = [[InlineKeyboardButton(text="Нет сделок", callback_data="noop")]]

    return InlineKeyboardMarkup(inline_keyboard=rows)
    

def post_actions_kb(post_id: int, status: str):
    share_url = f"https://t.me/share/url?url={post_deeplink(post_id)}"
    rows = []

    if status == STATUS_ACTIVE:
        rows.append([
            InlineKeyboardButton(text="⏸ Деактивировать", callback_data=f"deactivate:{post_id}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete:{post_id}")
        ])
        rows.append([
            InlineKeyboardButton(text="🔼 Поднять", callback_data=f"bump:{post_id}"),
            InlineKeyboardButton(text="👀 Совпадения", callback_data=f"coincidences:{post_id}")
        ])
        rows.append([
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"editpost:{post_id}")
        ])
        rows.append([
            InlineKeyboardButton(text="📤 Поделиться", url=share_url)
        ])

    elif status in (STATUS_INACTIVE, STATUS_EXPIRED, STATUS_REJECTED):
        rows.append([
            InlineKeyboardButton(text="▶️ Активировать", callback_data=f"activate:{post_id}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete:{post_id}")
        ])
        rows.append([
            InlineKeyboardButton(text="👀 Совпадения", callback_data=f"coincidences:{post_id}")
        ])
        rows.append([
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"editpost:{post_id}")
        ])

    elif status == STATUS_PENDING:
        rows.append([
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete:{post_id}")
        ])

    else:
        rows.append([
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete:{post_id}")
        ])

    # 🛂 КНОПКА ВЕРИФИКАЦИИ — показывается при любом статусе объявления
    rows.append([
        InlineKeyboardButton(
            text="🛂 Верифицировать паспорт",
            callback_data="verify:info"
        )
    ])

    # ⬅️ КНОПКА НАЗАД
    rows.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back:my_posts")
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_post_actions_kb(post_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"adminapprove:{post_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"adminreject:{post_id}")
        ],
        [InlineKeyboardButton(text="🚫 Бан user", callback_data=f"adminbanpost:{post_id}")]
    ])


async def public_post_kb(post_id: int, owner_id: int):
    _, reviews_count = await user_rating_summary(owner_id)
    post = await get_post(post_id)

    rows = [
        [InlineKeyboardButton(text="✉️ Написать владельцу", callback_data=f"contact:{post_id}:{owner_id}")],
        [InlineKeyboardButton(text="🤝 Предложить сделку", callback_data=f"offer_deal_confirm:{post_id}:{owner_id}")]
    ]

    if post and post["photo_file_id"]:
        rows.append([
            InlineKeyboardButton(
                text="🖼 Посмотреть фото посылки",
                callback_data=f"viewphoto:{post_id}"
            )
        ])

    if reviews_count > 0:
        rows.append([
            InlineKeyboardButton(
                text=f"⭐ {reviews_count} {reviews_word(reviews_count)}",
                callback_data=f"user_reviews:{owner_id}"
            )
        ])

    rows.append([
        InlineKeyboardButton(
            text="⚠️ Пожаловаться",
            callback_data=f"complain:{post_id}"
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="🛂 Верифицировать паспорт",
            callback_data="verify:start"
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="📤 Поделиться",
            url=f"https://t.me/share/url?url={post_deeplink(post_id)}"
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data="back:new_posts"
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_offer_deal_kb(post_id: int, owner_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="✅ Да, отправить заявку",
                callback_data=f"offer_deal:{post_id}:{owner_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="❌ Отмена",
                callback_data="offer_deal_cancel"
            )
        ]
    ])
    

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


def verification_info_kb(is_verified_now: bool):
    rows = []
    if not is_verified_now:
        rows.append([InlineKeyboardButton(text="💳 Начать верификацию", callback_data="verify:start")])
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Ок", callback_data="noop")]])


def verification_pay_kb(request_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"verify:paid:{request_id}")]
    ])


def verification_upload_passport_kb(request_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📷 Загрузить паспорт", callback_data=f"verify:upload_passport:{request_id}")]
    ])


def verification_retry_kb(request_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📷 Отправить заново", callback_data=f"verify:retry:{request_id}")]
    ])


def admin_verification_payment_kb(request_id: int, user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить оплату", callback_data=f"admin_verif_pay_ok:{request_id}"),
            InlineKeyboardButton(text="❌ Отклонить оплату", callback_data=f"admin_verif_pay_no:{request_id}")
        ],
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data=f"admin_user:{user_id}")
        ]
    ])


async def get_user_profile_full(user_id: int) -> dict:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT *
            FROM users
            WHERE user_id=?
        """, (user_id,))
        user = await cur.fetchone()

        cur = await conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM posts
            WHERE user_id=?
        """, (user_id,))
        posts_count_row = await cur.fetchone()

        cur = await conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM posts
            WHERE user_id=? AND status='active'
        """, (user_id,))
        active_posts_row = await cur.fetchone()

        cur = await conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM deals
            WHERE status='completed'
              AND (owner_user_id=? OR requester_user_id=?)
        """, (user_id, user_id))
        completed_deals_row = await cur.fetchone()

        cur = await conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM complaints
            WHERE post_id IN (
                SELECT id FROM posts WHERE user_id=?
            )
        """, (user_id,))
        complaints_row = await cur.fetchone()

    return {
        "user": user,
        "posts_count": int(posts_count_row["cnt"] or 0),
        "active_posts": int(active_posts_row["cnt"] or 0),
        "completed_deals": int(completed_deals_row["cnt"] or 0),
        "complaints_received": int(complaints_row["cnt"] or 0),
    }
        

def admin_verification_review_kb(request_id: int, user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin_verif_ok:{request_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_verif_no:{request_id}")
        ],
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data=f"admin_user:{user_id}")
        ]
    ])


def admin_verification_list_kb(rows: List[aiosqlite.Row]):
    buttons = []
    for row in rows:
        label = f"{row['id']} • USER {row['user_id']} • {format_verification_status(row['status'])}"
        buttons.append([InlineKeyboardButton(text=label[:64], callback_data=f"admin_verif_open:{row['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons or [[InlineKeyboardButton(text="Пусто", callback_data="noop")]])


def admin_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📚 Все объявления", callback_data="admin:all_posts")],
        [InlineKeyboardButton(text="🛂 Верификации", callback_data="admin:verifications")],
        [InlineKeyboardButton(text="🆘 Последние жалобы", callback_data="admin:complaints")],
        [InlineKeyboardButton(text="👤 Пользователь", callback_data="admin:user_lookup")],
        [InlineKeyboardButton(text="💰 Заявки на поднятие", callback_data="admin:bump_orders")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
    ])


def popular_routes_kb(rows: List[aiosqlite.Row]):
    buttons = []
    for row in rows:
        label = f"{row['from_country']} → {row['to_country']} ({row['cnt']})"
        buttons.append([InlineKeyboardButton(text=label[:64], callback_data=f"popular:{row['from_country']}:{row['to_country']}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons or [[InlineKeyboardButton(text="Пока пусто", callback_data="noop")]])


async def deal_open_kb(deal: aiosqlite.Row, user_id: int) -> InlineKeyboardMarkup:
    rows = []

    viewer_is_owner = user_id == deal["owner_user_id"]
    other_user_id = deal["requester_user_id"] if viewer_is_owner else deal["owner_user_id"]

    owner_confirmed = int(deal["owner_confirmed"] or 0)
    requester_confirmed = int(deal["requester_confirmed"] or 0)

    user_confirmed = owner_confirmed if viewer_is_owner else requester_confirmed
    both_confirmed = owner_confirmed == 1 and requester_confirmed == 1

    # Если обе стороны подтвердили — только отзыв и назад
    if both_confirmed or deal["status"] == DEAL_COMPLETED:
        if not await has_user_left_review_for_deal(deal, user_id):
            rows.append([
                InlineKeyboardButton(
                    text="⭐ Оставить отзыв",
                    callback_data=f"deal_review:{deal['id']}"
                )
            ])

        rows.append([
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data="back:my_deals"
            )
        ])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    # Если спор
    if deal["status"] in (DEAL_DISPUTE_OPEN, DEAL_DISPUTE_WAITING):
        dispute = await get_open_dispute_by_deal(deal["id"])
        if dispute:
            rows.extend(dispute_actions_kb(dispute, user_id).inline_keyboard)

        rows.append([
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data="back:my_deals"
            )
        ])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    # Если сделка неуспешна / отменена
    if deal["status"] in (DEAL_FAILED, DEAL_CANCELLED):
        if not await has_user_left_review_for_deal(deal, user_id):
            rows.append([
                InlineKeyboardButton(
                    text="⭐ Оставить отзыв",
                    callback_data=f"deal_review:{deal['id']}"
                )
            ])

        rows.append([
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data="back:my_deals"
            )
        ])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    # Активная сделка
    if not user_confirmed:

        if can_confirm_deal_now(deal):

            rows.append([
                InlineKeyboardButton(
                    text="✅ Подтвердить завершение",
                    callback_data=f"deal_confirm:{deal['id']}"
                )
            ])

            rows.append([
                InlineKeyboardButton(
                    text="📦 Посылка не доставлена",
                    callback_data=f"deal_dispute_open:{deal['id']}"
                )
            ])

        else:

            rows.append([
                InlineKeyboardButton(
                    text=f"⏳Завершить сделку через  {time_left_until_deal_confirm(deal)}",
                    callback_data="noop"
                )
            ])

    rows.append([
        InlineKeyboardButton(
            text="💬 Написать в чат через бота",
            callback_data=f"reply_contact:{deal['post_id']}:{other_user_id}:{deal['id']}"
        )
    ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data="back:my_deals"
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)
    

async def get_active_deal_by_post(post_id: int) -> Optional[aiosqlite.Row]:
    return await db_fetchone("""
        SELECT *
        FROM deals
        WHERE post_id=?
          AND status IN (?, ?, ?, ?, ?)
        ORDER BY id DESC
        LIMIT 1
    """, (
        post_id,
        DEAL_ACCEPTED,
        DEAL_COMPLETED_BY_OWNER,
        DEAL_COMPLETED_BY_REQUESTER,
        DEAL_DISPUTE_OPEN,
        DEAL_DISPUTE_WAITING
    ))


def admin_deal_users_kb(owner_user_id: int, requester_user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="👤 Владелец",
                callback_data=f"admin_user_profile:{owner_user_id}"
            ),
            InlineKeyboardButton(
                text="👤 Заказчик",
                callback_data=f"admin_user_profile:{requester_user_id}"
            )
        ]
    ])
        

def dispute_actions_kb(dispute: aiosqlite.Row, viewer_user_id: int) -> InlineKeyboardMarkup:
    rows = []

    if dispute["status"] == DISPUTE_WAITING_RESPONSE and viewer_user_id == dispute["against_user_id"]:
        rows.append([
            InlineKeyboardButton(
                text="📩 Ответить по спору",
                callback_data=f"dispute_reply:{dispute['id']}"
            )
        ])

    if dispute["status"] == DISPUTE_RESPONDED and viewer_user_id == dispute["opened_by_user_id"]:
        rows.append([
            InlineKeyboardButton(
                text="✅ Решено",
                callback_data=f"dispute_resolve:{dispute['id']}"
            ),
            InlineKeyboardButton(
                text="❌ Не решено",
                callback_data=f"dispute_unresolved:{dispute['id']}"
            )
        ])

    if not rows:
        rows = [[InlineKeyboardButton(text="Ок", callback_data="noop")]]

    return InlineKeyboardMarkup(inline_keyboard=rows)


def main_menu(user_id: Optional[int] = None):
    keyboard = [
        [KeyboardButton(text="✈️ Взять посылку"), KeyboardButton(text="📦 Отправить посылку")],
        [KeyboardButton(text="🚀 Быстрая доставка (карго)")],
        [KeyboardButton(text="🔎 Найти совпадения"), KeyboardButton(text="📋 Мои объявления")],
        [KeyboardButton(text="🤝 Мои сделки"), KeyboardButton(text="🔔 Подписки")],
        [KeyboardButton(text="🆕 Новые объявления"), KeyboardButton(text="🔥 Популярные маршруты")],
        [KeyboardButton(text="💰 Поднять объявление"), KeyboardButton(text="🛂 Верификация аккаунта")],
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="🚩 Жалоба / Баг / Поддержка")],
        [KeyboardButton(text="ℹ️ Помощь")],
    ]

    if user_id is not None and is_admin(user_id):
        keyboard.append([KeyboardButton(text="👨‍💼 Админка")])

    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True
    )


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


class AdminFlow(StatesGroup):
    user_lookup = State()


class SupportFlow(StatesGroup):
    bug_text = State()
    help_text = State()


class ContactFlow(StatesGroup):
    message_text = State()


class SubscriptionFlow(StatesGroup):
    looking_for = State()
    from_country = State()
    from_city = State()
    to_country = State()
    to_city = State()


class ReviewFlow(StatesGroup):
    reviewed_user_id = State()
    post_id = State()
    rating = State()
    text = State()


class DisputeFlow(StatesGroup):
    deal_id = State()
    reason = State()
    response = State()


class OnboardingFlow(StatesGroup):
    screen_1 = State()
    screen_2 = State()
    screen_3 = State()
    screen_4 = State()
    screen_5 = State()
    screen_6 = State()


class AdminContactFlow(StatesGroup):
    message = State()

class VerificationFlow(StatesGroup):
    passport_photo = State()
    selfie_photo = State()

class CargoLeadFlow(StatesGroup):
    from_country = State()
    from_country_manual = State()
    from_city = State()
    from_city_manual = State()
    to_country = State()
    to_country_manual = State()
    to_city = State()
    to_city_manual = State()
    delivery_date = State()
    weight = State()
    weight_manual = State()
    description = State()
    photo_choice = State()
    photo_upload = State()
    contact = State()
    

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


async def get_post(post_id: int) -> Optional[aiosqlite.Row]:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.id=?
        """, (post_id,))
        return await cur.fetchone()


async def get_pending_posts(limit: int = 20):
    return await db_fetchall("""
        SELECT p.*, u.username, u.full_name
        FROM posts p
        LEFT JOIN users u ON u.user_id = p.user_id
        WHERE p.status=?
        ORDER BY p.created_at ASC
        LIMIT ?
    """, (STATUS_PENDING, limit))


async def get_recent_complaints(limit: int = 20):
    return await db_fetchall("""
        SELECT c.*, p.user_id AS post_owner_user_id
        FROM complaints c
        LEFT JOIN posts p ON p.id = c.post_id
        ORDER BY c.created_at DESC
        LIMIT ?
    """, (limit,))


async def get_pending_bump_orders(limit: int = 20):
    return await db_fetchall("""
        SELECT *
        FROM bump_orders
        WHERE status='pending'
        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,))


async def get_admin_posts(limit: int = 30):
    return await db_fetchall("""
        SELECT p.*, u.username, u.full_name
        FROM posts p
        LEFT JOIN users u ON u.user_id = p.user_id
        WHERE p.status != ?
        ORDER BY p.created_at DESC
        LIMIT ?
    """, (STATUS_DELETED, limit))
    

def support_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚩 Пожаловаться на объявление", callback_data="support:complaint")],
        [InlineKeyboardButton(text="🐞 Сообщить о баге", callback_data="support:bug")],
        [InlineKeyboardButton(text="🆘 Связаться с поддержкой", callback_data="support:help")],
    ])


def admin_posts_kb(rows: List[aiosqlite.Row]):
    buttons = []
    for row in rows:
        icon = "✈️" if row["post_type"] == TYPE_TRIP else "📦"
        status = row["status"]
        label = f"{row['id']} • {icon} • {row['from_country']}→{row['to_country']} • {status}"
        buttons.append([InlineKeyboardButton(text=label[:64], callback_data=f"adminpost:{row['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons or [[InlineKeyboardButton(text="Пусто", callback_data="noop")]])


def admin_post_manage_kb(post_id: int, owner_user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="❌ Скрыть", callback_data=f"admin_hide_post:{post_id}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"admin_delete_post:{post_id}")
        ],
        [
            InlineKeyboardButton(text="🚫 Бан владельца", callback_data=f"admin_ban_user:{owner_user_id}"),
            InlineKeyboardButton(text="👤 Профиль владельца", callback_data=f"admin_user:{owner_user_id}")
        ]
    ])


def admin_bump_orders_kb(order_id: int, post_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_bump_confirm:{order_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_bump_reject:{order_id}")
        ],
        [
            InlineKeyboardButton(text="📄 Открыть объявление", callback_data=f"adminpost:{post_id}")
        ]
    ])


def admin_user_actions_kb(user_id: int, is_verified: bool, is_banned: bool, is_cargo: bool = False):
    rows = []

    # Верификация
    if is_verified:
        rows.append([
            InlineKeyboardButton(
                text="↩️ Снять верификацию",
                callback_data=f"admin_user_unverify:{user_id}"
            )
        ])
    else:
        rows.append([
            InlineKeyboardButton(
                text="✅ Верифицировать",
                callback_data=f"admin_user_verify:{user_id}"
            )
        ])

    # HOLD (проверка)
    rows.append([
        InlineKeyboardButton(
            text="⚠️ На проверку",
            callback_data=f"admin_user_hold:{user_id}"
        ),
        InlineKeyboardButton(
            text="✅ Снять проверку",
            callback_data=f"admin_user_unhold:{user_id}"
        )
    ])

    # Карго-партнер
    if is_cargo:
        rows.append([
            InlineKeyboardButton(
                text="❌ Убрать карго",
                callback_data=f"admin_user_remove_cargo:{user_id}"
            )
        ])
    else:
        rows.append([
            InlineKeyboardButton(
                text="🚀 Сделать карго",
                callback_data=f"admin_user_make_cargo:{user_id}"
            )
        ])

    # Бан
    if is_banned:
        rows.append([
            InlineKeyboardButton(
                text="♻️ Разбанить",
                callback_data=f"admin_user_unban:{user_id}"
            )
        ])
    else:
        rows.append([
            InlineKeyboardButton(
                text="🚫 Забанить",
                callback_data=f"admin_user_ban:{user_id}"
            )
        ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def admin_stats_text() -> str:
    async with await connect_db() as conn:
        cur = await conn.execute("SELECT COUNT(*) AS c FROM users")
        users_count = (await cur.fetchone())["c"]

        cur = await conn.execute(
            "SELECT COUNT(*) AS c FROM posts WHERE status='active' AND (expires_at IS NULL OR expires_at > ?)",
            (now_ts(),)
        )
        active_posts = (await cur.fetchone())["c"]

        cur = await conn.execute("SELECT COUNT(*) AS c FROM posts WHERE status='pending'")
        pending_posts = (await cur.fetchone())["c"]

        cur = await conn.execute("SELECT COUNT(*) AS c FROM complaints")
        complaints_count = (await cur.fetchone())["c"]

        cur = await conn.execute("""
            SELECT COUNT(*) AS c
            FROM disputes
            WHERE status IN (?, ?, ?)
        """, (DISPUTE_OPEN, DISPUTE_WAITING_RESPONSE, DISPUTE_RESPONDED))
        disputes_open = (await cur.fetchone())["c"]

        cur = await conn.execute("""
            SELECT COUNT(*) AS c
            FROM bump_orders
            WHERE status='pending'
        """)
        bump_pending = (await cur.fetchone())["c"]

        cur = await conn.execute("""
            SELECT COUNT(*) AS c
            FROM verification_requests
            WHERE status IN (?, ?)
        """, (VERIF_STATUS_PAYMENT_REVIEW, VERIF_STATUS_REVIEW_PENDING))
        verif_pending = (await cur.fetchone())["c"]

    return (
        "👨‍💼 <b>Админка</b>\n\n"
        f"👤 Пользователей: <b>{users_count}</b>\n"
        f"📦 Активных объявлений: <b>{active_posts}</b>\n"
        f"⏳ На модерации: <b>{pending_posts}</b>\n"
        f"🆘 Жалоб: <b>{complaints_count}</b>\n"
        f"⚖️ Активных споров: <b>{disputes_open}</b>\n"
        f"💰 Заявок на поднятие: <b>{bump_pending}</b>\n"
        f"🛂 Верификаций на проверке: <b>{verif_pending}</b>\n"
    )


async def verify_user(user_id: int):
    await db_execute("""
        UPDATE users
        SET is_verified=1,
            verified_at=?,
            verification_type='passport'
        WHERE user_id=?
    """, (now_ts(), user_id))
    invalidate_user_profile_cache(user_id)


async def unverify_user(user_id: int):
    await db_execute("""
        UPDATE users
        SET is_verified=0,
            verified_at=NULL,
            verification_type=NULL
        WHERE user_id=?
    """, (user_id,))
    invalidate_user_profile_cache(user_id)
        

async def get_latest_verification_request(user_id: int) -> Optional[aiosqlite.Row]:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT *
            FROM verification_requests
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT 1
        """, (user_id,))
        return await cur.fetchone()


async def get_verification_request(request_id: int) -> Optional[aiosqlite.Row]:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT *
            FROM verification_requests
            WHERE id=?
            LIMIT 1
        """, (request_id,))
        return await cur.fetchone()


async def create_verification_request(user_id: int) -> int:
    ts = now_ts()

    async with await connect_db() as conn:
        cur = await conn.execute("""
            INSERT INTO verification_requests (
                user_id, status, payment_amount, payment_currency,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            user_id,
            VERIF_STATUS_AWAITING_PAYMENT,
            VERIFICATION_PRICE_AMOUNT,
            VERIFICATION_PRICE_CURRENCY,
            ts,
            ts
        ))
        await conn.commit()
        return int(cur.lastrowid)


async def users_had_recent_deal(user1: int, user2: int) -> bool:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT id
            FROM deals
            WHERE (
                (owner_user_id=? AND requester_user_id=?)
                OR
                (owner_user_id=? AND requester_user_id=?)
            )
            AND status = ?
            AND updated_at > ?
            LIMIT 1
        """, (
            user1, user2,
            user2, user1,
            DEAL_COMPLETED,
            now_ts() - 7 * 24 * 3600
        ))
        row = await cur.fetchone()

    return bool(row)


async def active_deals_count(user_id: int) -> int:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT COUNT(*) AS c
            FROM deals
            WHERE (owner_user_id=? OR requester_user_id=?)
              AND status IN (?, ?, ?, ?, ?)
        """, (
            user_id, user_id,
            DEAL_ACCEPTED,
            DEAL_COMPLETED_BY_OWNER,
            DEAL_COMPLETED_BY_REQUESTER,
            DEAL_DISPUTE_OPEN,
            DEAL_DISPUTE_WAITING
        ))
        row = await cur.fetchone()

    return int(row["c"] or 0)


async def set_verification_status(
    request_id: int,
    status: str,
    *,
    rejection_reason: Optional[str] = None,
    admin_user_id: Optional[int] = None,
    mark_paid: bool = False,
    mark_reviewed: bool = False
):
    async with await connect_db() as conn:
        cur = await conn.execute(
            "SELECT * FROM verification_requests WHERE id=?",
            (request_id,)
        )
        row = await cur.fetchone()

        if not row:
            return

        paid_at = row["paid_at"]
        reviewed_at = row["reviewed_at"]

        if mark_paid:
            paid_at = now_ts()
        if mark_reviewed:
            reviewed_at = now_ts()

        await conn.execute("""
            UPDATE verification_requests
            SET status=?,
                rejection_reason=?,
                admin_user_id=?,
                updated_at=?,
                paid_at=?,
                reviewed_at=?
            WHERE id=?
        """, (
            status,
            rejection_reason,
            admin_user_id,
            now_ts(),
            paid_at,
            reviewed_at,
            request_id
        ))

        await conn.commit()


async def save_verification_passport(request_id: int, photo_file_id: str):
    async with await connect_db() as conn:
        await conn.execute("""
            UPDATE verification_requests
            SET passport_photo_file_id=?,
                status=?,
                updated_at=?
            WHERE id=?
        """, (
            photo_file_id,
            VERIF_STATUS_SELFIE_PENDING,
            now_ts(),
            request_id
        ))
        await conn.commit()


async def save_verification_selfie(request_id: int, photo_file_id: str):
    async with await connect_db() as conn:
        await conn.execute("""
            UPDATE verification_requests
            SET selfie_photo_file_id=?,
                status=?,
                updated_at=?,
                rejection_reason=NULL
            WHERE id=?
        """, (
            photo_file_id,
            VERIF_STATUS_REVIEW_PENDING,
            now_ts(),
            request_id
        ))
        await conn.commit()


async def clear_verification_files(request_id: int):
    async with await connect_db() as conn:
        await conn.execute("""
            UPDATE verification_requests
            SET passport_photo_file_id=NULL,
                selfie_photo_file_id=NULL,
                updated_at=?
            WHERE id=?
        """, (now_ts(), request_id))
        await conn.commit()


async def list_pending_verification_requests(limit: int = 20):
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT *
            FROM verification_requests
            WHERE status IN (?, ?)
            ORDER BY updated_at DESC
            LIMIT ?
        """, (
            VERIF_STATUS_PAYMENT_REVIEW,
            VERIF_STATUS_REVIEW_PENDING,
            limit
        ))
        return await cur.fetchall()

def format_verification_status(status: str) -> str:
    mapping = {
        VERIF_STATUS_AWAITING_PAYMENT: "ожидает оплаты",
        VERIF_STATUS_PAYMENT_REVIEW: "оплата на проверке",
        VERIF_STATUS_DOCS_PENDING: "ожидает фото паспорта",
        VERIF_STATUS_SELFIE_PENDING: "ожидает селфи с паспортом",
        VERIF_STATUS_REVIEW_PENDING: "документы на проверке",
        VERIF_STATUS_APPROVED: "верификация одобрена",
        VERIF_STATUS_REJECTED: "документы отклонены",
        VERIF_STATUS_PAYMENT_REJECTED: "оплата отклонена",
    }
    return mapping.get(status, status)


async def search_posts_inline(
    query: str,
    limit: int = 10,
    offset: int = 0,
    post_type: Optional[str] = None,
    from_country: Optional[str] = None,
    to_country: Optional[str] = None
) -> List[aiosqlite.Row]:
    q = f"%{query.strip().lower()}%"
    sql = [
        """
        SELECT p.*, u.username, u.full_name, COALESCE(u.is_verified, 0) AS is_verified
        FROM posts p
        LEFT JOIN users u ON u.user_id = p.user_id
        WHERE p.status='active'
          AND (p.expires_at IS NULL OR p.expires_at > ?)
        """
    ]
    params = [now_ts()]

    if query.strip():
        sql.append("""
          AND (
                lower(p.from_country) LIKE ?
             OR lower(COALESCE(p.from_city, '')) LIKE ?
             OR lower(p.to_country) LIKE ?
             OR lower(COALESCE(p.to_city, '')) LIKE ?
             OR lower(COALESCE(p.description, '')) LIKE ?
             OR lower(COALESCE(p.travel_date, '')) LIKE ?
             OR lower(COALESCE(p.weight_kg, '')) LIKE ?
          )
        """)
        params.extend([q, q, q, q, q, q, q])

    if post_type:
        sql.append(" AND p.post_type=? ")
        params.append(post_type)

    if from_country:
        sql.append(" AND p.from_country=? ")
        params.append(from_country)

    if to_country:
        sql.append(" AND p.to_country=? ")
        params.append(to_country)

    sql.append("""
        ORDER BY COALESCE(u.is_verified, 0) DESC,
                 COALESCE(p.bumped_at, p.created_at) DESC
        LIMIT ? OFFSET ?
    """)
    params.extend([limit, offset])

    async with await connect_db() as conn:
        cur = await conn.execute("".join(sql), tuple(params))
        return await cur.fetchall()


async def count_search_posts(
    query: str = "",
    post_type: Optional[str] = None,
    from_country: Optional[str] = None,
    to_country: Optional[str] = None
) -> int:
    q = f"%{query.strip().lower()}%"
    sql = [
        """
        SELECT COUNT(*) AS c
        FROM posts p
        WHERE p.status='active'
          AND (p.expires_at IS NULL OR p.expires_at > ?)
        """
    ]
    params = [now_ts()]

    if query.strip():
        sql.append("""
          AND (
                lower(p.from_country) LIKE ?
             OR lower(COALESCE(p.from_city, '')) LIKE ?
             OR lower(p.to_country) LIKE ?
             OR lower(COALESCE(p.to_city, '')) LIKE ?
             OR lower(COALESCE(p.description, '')) LIKE ?
             OR lower(COALESCE(p.travel_date, '')) LIKE ?
             OR lower(COALESCE(p.weight_kg, '')) LIKE ?
          )
        """)
        params.extend([q, q, q, q, q, q, q])

    if post_type:
        sql.append(" AND p.post_type=? ")
        params.append(post_type)

    if from_country:
        sql.append(" AND p.from_country=? ")
        params.append(from_country)

    if to_country:
        sql.append(" AND p.to_country=? ")
        params.append(to_country)

    async with await connect_db() as conn:
        cur = await conn.execute("".join(sql), tuple(params))
        row = await cur.fetchone()
        return int(row["c"] or 0)


async def get_popular_routes(limit: int = 10) -> List[aiosqlite.Row]:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT from_country, to_country, COUNT(*) AS cnt
            FROM posts
            WHERE status='active'
              AND (expires_at IS NULL OR expires_at > ?)
            GROUP BY from_country, to_country
            ORDER BY cnt DESC, MAX(COALESCE(bumped_at, created_at)) DESC
            LIMIT ?
        """, (now_ts(), limit))
        return await cur.fetchall()


async def search_route_posts_all(
    from_country: str,
    to_country: str,
    limit: int = 20,
    offset: int = 0,
    from_city: Optional[str] = None,
    to_city: Optional[str] = None,
    post_type: Optional[str] = None
) -> List[aiosqlite.Row]:
    sql = [
        """
            SELECT p.*, u.username, u.full_name, COALESCE(u.is_verified, 0) AS is_verified
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.from_country=? AND p.to_country=?
              AND p.status='active'
              AND (p.expires_at IS NULL OR p.expires_at > ?)
        """
    ]
    params = [from_country, to_country, now_ts()]

    if from_city:
        sql.append(" AND COALESCE(p.from_city, '')=? ")
        params.append(from_city)

    if to_city:
        sql.append(" AND COALESCE(p.to_city, '')=? ")
        params.append(to_city)

    if post_type:
        sql.append(" AND p.post_type=? ")
        params.append(post_type)

    sql.append("""
        ORDER BY COALESCE(u.is_verified, 0) DESC,
                 COALESCE(p.bumped_at, p.created_at) DESC
        LIMIT ? OFFSET ?
    """)
    params.extend([limit, offset])

    async with await connect_db() as conn:
        cur = await conn.execute("".join(sql), tuple(params))
        return await cur.fetchall()


async def service_stats() -> aiosqlite.Row:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT
                (SELECT COUNT(*) FROM users) AS users_count,
                (SELECT COUNT(*) FROM posts WHERE status='active' AND (expires_at IS NULL OR expires_at > ?)) AS active_posts,
                (SELECT COUNT(*) FROM posts WHERE status='active' AND post_type='trip' AND (expires_at IS NULL OR expires_at > ?)) AS active_trips,
                (SELECT COUNT(*) FROM posts WHERE status='active' AND post_type='parcel' AND (expires_at IS NULL OR expires_at > ?)) AS active_parcels
        """, (now_ts(), now_ts(), now_ts()))
        return await cur.fetchone()


async def top_route() -> Optional[aiosqlite.Row]:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT from_country, to_country, COUNT(*) AS cnt
            FROM posts
            WHERE status='active'
              AND (expires_at IS NULL OR expires_at > ?)
            GROUP BY from_country, to_country
            ORDER BY cnt DESC
            LIMIT 1
        """, (now_ts(),))
        return await cur.fetchone()


async def create_post_record(data: dict, user_id: int) -> int:
    ts = now_ts()
    expires_at = calculate_post_expires_at(
        ts,
        data.get("travel_date"),
        POST_TTL_DAYS
    )

    async with await connect_db() as conn:
        cur = await conn.execute("""
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
            STATUS_PENDING if MODERATION_ENABLED else STATUS_ACTIVE,
            1,
            ts,
            ts,
            ts,
            expires_at
        ))
        await conn.commit()
        return int(cur.lastrowid)


async def update_post_record(post_id: int, user_id: int, updates: dict) -> bool:
    allowed = {
        "from_country", "from_city", "to_country", "to_city",
        "travel_date", "weight_kg", "description", "contact_note", "photo_file_id"
    }

    payload = {k: v for k, v in updates.items() if k in allowed}
    if not payload:
        return False

    payload["updated_at"] = now_ts()

    async with await connect_db() as conn:
        if "travel_date" in payload:
            cur = await conn.execute(
                "SELECT created_at FROM posts WHERE id=? AND user_id=?",
                (post_id, user_id)
            )
            row = await cur.fetchone()

            if not row:
                return False

            payload["expires_at"] = calculate_post_expires_at(
                int(row["created_at"] or now_ts()),
                payload.get("travel_date"),
                POST_TTL_DAYS
            )

        sets = ", ".join(f"{key}=?" for key in payload.keys())
        params = list(payload.values()) + [post_id, user_id]

        cur = await conn.execute(
            f"UPDATE posts SET {sets} WHERE id=? AND user_id=?",
            tuple(params)
        )
        await conn.commit()

        return cur.rowcount > 0


async def user_post_create_rate_limited(user_id: int) -> bool:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT COUNT(*) AS c
            FROM posts
            WHERE user_id=? AND created_at>=?
        """, (user_id, now_ts() - 600))
        row = await cur.fetchone()

        return int(row["c"] or 0) >= MAX_POSTS_PER_10_MIN


async def add_route_subscription(
    user_id: int,
    post_type: str,
    from_country: str,
    to_country: str,
    from_city: Optional[str] = None,
    to_city: Optional[str] = None
):
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT id FROM route_subscriptions
            WHERE user_id=?
              AND post_type=?
              AND from_country=?
              AND COALESCE(from_city, '')=COALESCE(?, '')
              AND to_country=?
              AND COALESCE(to_city, '')=COALESCE(?, '')
            LIMIT 1
        """, (user_id, post_type, from_country, from_city, to_country, to_city))
        exists = await cur.fetchone()

        if exists:
            return

        await conn.execute("""
            INSERT INTO route_subscriptions (
                user_id, post_type, from_country, from_city,
                to_country, to_city, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, post_type, from_country, from_city, to_country, to_city, now_ts()))

        await conn.commit()


async def list_route_subscriptions(user_id: int) -> List[aiosqlite.Row]:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT * FROM route_subscriptions
            WHERE user_id=?
            ORDER BY created_at DESC
            LIMIT 20
        """, (user_id,))
        return await cur.fetchall()


async def delete_subscription(user_id: int, sub_id: int) -> bool:
    async with await connect_db() as conn:
        cur = await conn.execute(
            "DELETE FROM route_subscriptions WHERE id=? AND user_id=?",
            (sub_id, user_id)
        )
        await conn.commit()
        return cur.rowcount > 0


async def is_user_blocked(user_id: int, blocked_user_id: int) -> bool:
    async with await connect_db() as conn:
        cur = await conn.execute(
            "SELECT 1 FROM user_blacklist WHERE user_id=? AND blocked_user_id=? LIMIT 1",
            (user_id, blocked_user_id)
        )
        row = await cur.fetchone()
        return row is not None


async def add_user_to_blacklist(user_id: int, blocked_user_id: int) -> bool:
    if user_id == blocked_user_id:
        return False

    async with await connect_db() as conn:
        try:
            await conn.execute("""
                INSERT INTO user_blacklist(user_id, blocked_user_id, created_at)
                VALUES (?, ?, ?)
            """, (user_id, blocked_user_id, now_ts()))
            await conn.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


async def remove_user_from_blacklist(user_id: int, blocked_user_id: int) -> bool:
    async with await connect_db() as conn:
        cur = await conn.execute(
            "DELETE FROM user_blacklist WHERE user_id=? AND blocked_user_id=?",
            (user_id, blocked_user_id)
        )
        await conn.commit()
        return cur.rowcount > 0


async def save_chat_message(
    post_id: int,
    from_user_id: int,
    to_user_id: int,
    message_text: str,
    deal_id: Optional[int] = None
):
    async with await connect_db() as conn:
        await conn.execute("""
            INSERT INTO chat_messages(
                post_id, deal_id, from_user_id, to_user_id,
                message_text, created_at, is_read
            )
            VALUES (?, ?, ?, ?, ?, ?, 0)
        """, (post_id, deal_id, from_user_id, to_user_id, message_text, now_ts()))
        await conn.commit()


async def unread_chat_count(user_id: int) -> int:
    async with await connect_db() as conn:
        cur = await conn.execute(
            "SELECT COUNT(*) AS c FROM chat_messages WHERE to_user_id=? AND is_read=0",
            (user_id,)
        )
        row = await cur.fetchone()
        return int(row["c"] or 0)


async def mark_chat_read(user_id: int, partner_user_id: int, post_id: int):
    async with await connect_db() as conn:
        await conn.execute("""
            UPDATE chat_messages
            SET is_read=1
            WHERE to_user_id=?
              AND from_user_id=?
              AND post_id=?
              AND is_read=0
        """, (user_id, partner_user_id, post_id))
        await conn.commit()


async def get_chat_history(
    user_a: int,
    user_b: int,
    post_id: int,
    limit: int = 20
) -> List[aiosqlite.Row]:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT * FROM chat_messages
            WHERE post_id=?
              AND (
                    (from_user_id=? AND to_user_id=?)
                 OR (from_user_id=? AND to_user_id=?)
              )
            ORDER BY created_at DESC
            LIMIT ?
        """, (post_id, user_a, user_b, user_b, user_a, limit))
        return await cur.fetchall()


async def reserve_coincidence_notification(post_a_id: int, post_b_id: int) -> bool:
    a, b = sorted([post_a_id, post_b_id])

    async with await connect_db() as conn:
        try:
            await conn.execute("""
                INSERT INTO coincidence_notifications (post_a_id, post_b_id, created_at)
                VALUES (?, ?, ?)
            """, (a, b, now_ts()))
            await conn.commit()
            return True
        except aiosqlite.IntegrityError:
            return False


def calculate_coincidence_score(source_row, candidate_row: aiosqlite.Row) -> Tuple[int, List[str]]:
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


async def get_coincidences(
    post_type: str,
    from_country: str,
    to_country: str,
    exclude_user_id: Optional[int] = None,
    source_row=None,
    limit: int = 20
) -> List[dict]:
    target_type = TYPE_PARCEL if post_type == TYPE_TRIP else TYPE_TRIP

    query = """
        SELECT p.*, u.username, u.full_name, COALESCE(u.is_verified, 0) AS is_verified
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

    rows = await db_fetchall(query, tuple(params))

    results = []
    for row in rows:
        score, notes = (
            (45, ["Совпадает маршрут по странам"])
            if source_row is None
            else calculate_coincidence_score(source_row, row)
        )

        if score < 35:
            continue

        results.append({
            "row": row,
            "score": score,
            "notes": notes,
            "type": "strong" if score >= 75 else "good" if score >= 55 else "possible"
        })

    results.sort(
        key=lambda x: (
            x["score"],
            int(x["row"]["is_verified"] or 0),
            x["row"]["bumped_at"] or x["row"]["created_at"] or 0
        ),
        reverse=True
    )

    return results[:limit]


async def ensure_deal(
    post_id: int,
    owner_user_id: int,
    requester_user_id: int,
    initiator_user_id: int
) -> int:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT id FROM deals
            WHERE post_id=? AND owner_user_id=? AND requester_user_id=?
            ORDER BY id DESC LIMIT 1
        """, (post_id, owner_user_id, requester_user_id))
        row = await cur.fetchone()

        if row:
            return int(row["id"])

        ts = now_ts()

        cur = await conn.execute("""
            INSERT INTO deals (
                post_id, owner_user_id, requester_user_id, initiator_user_id,
                status, owner_confirmed, requester_confirmed, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
        """, (
            post_id,
            owner_user_id,
            requester_user_id,
            initiator_user_id,
            DEAL_CONTACTED,
            ts,
            ts
        ))
        await conn.commit()
        return int(cur.lastrowid)


async def get_deal(deal_id: int) -> Optional[aiosqlite.Row]:
    async with await connect_db() as conn:
        cur = await conn.execute(
            "SELECT * FROM deals WHERE id=?",
            (deal_id,)
        )
        return await cur.fetchone()


async def list_user_deals(user_id: int) -> List[aiosqlite.Row]:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT *
            FROM deals
            WHERE owner_user_id=? OR requester_user_id=?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 30
        """, (user_id, user_id))
        return await cur.fetchall()


async def deal_title(deal: aiosqlite.Row) -> str:
    post = await get_post(deal["post_id"])

    if not post:
        return f"Сделка #{deal['id']}"

    route = post["from_country"] or ""

    if post["from_city"]:
        route += f", {post['from_city']}"

    route += " → "
    route += post["to_country"] or ""

    if post["to_city"]:
        route += f", {post['to_city']}"

    return route if route.strip() else f"Сделка #{deal['id']}"


def post_route_title(row: aiosqlite.Row) -> str:
    route = row["from_country"] or ""

    if row["from_city"]:
        route += f", {row['from_city']}"

    route += " → "
    route += row["to_country"] or ""

    if row["to_city"]:
        route += f", {row['to_city']}"

    return route


def extract_chat_ref_from_message(text: Optional[str]) -> Optional[tuple[int, int, Optional[int]]]:
    if not text:
        return None

    m = re.search(r"chat_ref:(\d+):(\d+):(\d+)", text)
    if not m:
        return None

    post_id = int(m.group(1))
    target_user_id = int(m.group(2))
    raw_deal_id = int(m.group(3))
    deal_id = None if raw_deal_id == 0 else raw_deal_id

    return post_id, target_user_id, deal_id


def split_deals_by_sections(deals: List[aiosqlite.Row]):
    in_progress = []
    disputes = []
    finished = []

    for d in deals:
        status = d["status"]

        if status in (
            DEAL_ACCEPTED,
            DEAL_COMPLETED_BY_OWNER,
            DEAL_COMPLETED_BY_REQUESTER,
        ):
            in_progress.append(d)

        elif status in (
            DEAL_DISPUTE_OPEN,
            DEAL_DISPUTE_WAITING,
            DEAL_DISPUTE_RESOLVED,
        ):
            disputes.append(d)

        elif status in (
            DEAL_COMPLETED,
            DEAL_FAILED,
            DEAL_CANCELLED,
        ):
            finished.append(d)

    return in_progress, disputes, finished


async def deal_section_kb(deals: List[aiosqlite.Row]) -> InlineKeyboardMarkup:
    rows = []

    for d in deals:
        if d["status"] == DEAL_ACCEPTED:
            icon = "🟢"
        elif d["status"] in (DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
            icon = "🟦"
        elif d["status"] in (DEAL_DISPUTE_OPEN, DEAL_DISPUTE_WAITING, DEAL_DISPUTE_RESOLVED):
            icon = "⚖️"
        elif d["status"] == DEAL_COMPLETED:
            icon = "✅"
        else:
            icon = "❌"

        title = await deal_title(d)
        label = f"{icon} {title}"

        rows.append([
            InlineKeyboardButton(
                text=label[:64],
                callback_data=f"mydeal:{d['id']}"
            )
        ])

    if not rows:
        rows = [[InlineKeyboardButton(text="Пусто", callback_data="noop")]]

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def mark_deal_failed(post_id: int, user_id: int) -> bool:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT id FROM deals
            WHERE post_id=?
              AND (owner_user_id=? OR requester_user_id=?)
              AND status IN (?, ?, ?, ?, ?)
            ORDER BY id DESC LIMIT 1
        """, (
            post_id, user_id, user_id,
            DEAL_CONTACTED, DEAL_OFFERED, DEAL_ACCEPTED,
            DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER
        ))

        row = await cur.fetchone()

        if not row:
            return False

        await conn.execute(
            "UPDATE deals SET status=?, updated_at=? WHERE id=?",
            (DEAL_FAILED, now_ts(), row["id"])
        )

        await conn.commit()
        return True


def contact_admin_kb(deal_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🆘 Связаться с администратором",
                    callback_data=f"contact_admin:{deal_id}"
                )
            ]
        ]
    )


async def create_bump_order(
    user_id: int,
    post_id: int,
    amount: int = BUMP_PRICE_AMOUNT,
    currency: str = BUMP_PRICE_CURRENCY
) -> int:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            INSERT INTO bump_orders (user_id, post_id, amount, currency, status, created_at)
            VALUES (?, ?, ?, ?, 'pending', ?)
        """, (user_id, post_id, amount, currency, now_ts()))

        await conn.commit()
        return int(cur.lastrowid)


async def publish_to_channel(bot: Bot, post_id: int):
    """Publish post to channel. Call with 'await'."""
    if not CHANNEL_USERNAME:
        return

    row = await get_post(post_id)
    if not row or row["status"] != STATUS_ACTIVE:
        return

    msg = await bot.send_message(
        CHANNEL_USERNAME,
       await post_text(row, for_channel=True),
        reply_markup=channel_post_kb(post_id, row["post_type"])
    )

    async with await connect_db() as conn:
        await conn.execute(
            "UPDATE posts SET channel_message_id=? WHERE id=?",
            (msg.message_id, post_id)
        )
        await conn.commit()


async def set_user_cargo(user_id: int, is_cargo: bool):
    await db_execute("""
        UPDATE users
        SET is_cargo=?
        WHERE user_id=?
    """, (1 if is_cargo else 0, user_id))

    invalidate_user_profile_cache(user_id)
    

async def is_cargo_user(user_id: int) -> bool:
    row = await db_fetchone("""
        SELECT COALESCE(is_cargo, 0) AS is_cargo
        FROM users
        WHERE user_id=?
    """, (user_id,))

    return bool(row and row["is_cargo"])


async def get_cargo_users():
    return await db_fetchall("""
        SELECT user_id, username, full_name
        FROM users
        WHERE is_cargo=1 AND is_banned=0
    """)


async def create_cargo_lead(
    user_id: int,
    from_place: str,
    to_place: str,
    delivery_date: str,
    weight: str,
    cargo_desc: str,
    contact: str,
    photo_file_id: Optional[str] = None
) -> int:
    async with await connect_db() as conn:
        cur = await conn.execute("""
            INSERT INTO cargo_leads (
                user_id, from_place, to_place, delivery_date,
                weight, cargo_desc, contact, photo_file_id, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user_id,
            from_place,
            to_place,
            delivery_date,
            weight,
            cargo_desc,
            contact,
            photo_file_id,
            now_ts()
        ))
        await conn.commit()
        return int(cur.lastrowid)


async def get_cargo_lead(lead_id: int):
    return await db_fetchone("""
        SELECT cl.*, u.username, u.full_name
        FROM cargo_leads cl
        LEFT JOIN users u ON u.user_id = cl.user_id
        WHERE cl.id=?
    """, (lead_id,))


def format_cargo_delivery_date(value: Optional[str]) -> str:
    if not value:
        return "не указано"

    value = str(value).strip()

    if re.match(r"^\d{2}\.\d{2}\.\d{4}$", value):
        return f"до {html.escape(value)}"

    return html.escape(value)


def cargo_lead_preview_text(lead) -> str:
    photo_line = ""
    if "photo_file_id" in lead.keys() and lead["photo_file_id"]:
        photo_line = "\n<b>Фото:</b> прикреплено, доступно по кнопке ниже"

    return (
        "🚚 <b>Новая заявка на доставку</b>\n\n"
        f"<b>Заявка:</b> #{lead['id']}\n"
        f"<b>Маршрут:</b> {html.escape(lead['from_place'])} → {html.escape(lead['to_place'])}\n"
        f"<b>Когда нужно доставить:</b> {format_cargo_delivery_date(lead['delivery_date'])}\n"
        f"<b>Вес/объем:</b> {html.escape(lead['weight'] or 'не указан')}\n"
        f"<b>Груз:</b> {html.escape(lead['cargo_desc'] or 'не указано')}"
        f"{photo_line}\n\n"
        "🔒 <b>Контакт клиента скрыт.</b>\n"
        "Нажмите кнопку ниже, чтобы получить контакт."
    )
    

def cargo_lead_contact_text(lead) -> str:
    photo_line = ""
    if "photo_file_id" in lead.keys() and lead["photo_file_id"]:
        photo_line = "\n<b>Фото:</b> прикреплено, доступно по кнопке ниже"

    return (
        "📩 <b>Контакт клиента</b>\n\n"
        f"<b>Заявка:</b> #{lead['id']}\n"
        f"<b>Маршрут:</b> {html.escape(lead['from_place'])} → {html.escape(lead['to_place'])}\n"
        f"<b>Когда нужно доставить:</b> {format_cargo_delivery_date(lead['delivery_date'])}\n"
        f"<b>Вес/объем:</b> {html.escape(lead['weight'] or 'не указан')}\n"
        f"<b>Груз:</b> {html.escape(lead['cargo_desc'] or 'не указано')}"
        f"{photo_line}\n\n"
        f"<b>Контакт:</b> {html.escape(lead['contact'])}"
    )
    

def cargo_lead_kb(lead):
    lead_id = lead["id"]
    rows = [[InlineKeyboardButton(
        text="📩 Получить контакт клиента",
        callback_data=f"cargo_get_contact:{lead_id}"
    )]]

    if lead["photo_file_id"]:
        rows.append([InlineKeyboardButton(
            text="🖼 Посмотреть фото",
            callback_data=f"cargo_view_photo:{lead_id}"
        )])

    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data.startswith("cargo_view_photo:"))
async def cargo_view_photo_handler(callback: CallbackQuery):
    lead_id = int(callback.data.split(":", 1)[1])
    lead = await get_cargo_lead(lead_id)

    if not lead or not lead["photo_file_id"]:
        await callback.answer("Фото не найдено", show_alert=True)
        return

    await callback.message.answer_photo(
        photo=lead["photo_file_id"],
        caption=f"🖼 Фото груза по заявке #{lead_id}"
    )
    await callback.answer()


CARGO_STEP_BACK = {
    "from_country": None,
    "from_country_manual": "from_country",

    "from_city": "from_country",
    "from_city_manual": "from_city",

    "to_country": "from_city",
    "to_country_manual": "to_country",

    "to_city": "to_country",
    "to_city_manual": "to_city",

    "delivery_date": "to_city",

    "weight": "delivery_date",
    "weight_manual": "weight",

    "description": "weight",
    "photo_choice": "description",
    "photo_upload": "photo_choice",
    "contact": "photo_choice",
}


@router.callback_query(F.data == "back:cargo")
async def cargo_back(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    current_state = await state.get_state()

    if not current_state:
        return

    current_step = current_state.split(":")[-1] if ":" in current_state else current_state
    prev_step = CARGO_STEP_BACK.get(current_step)

    if not prev_step:
        return

    await render_cargo_step(prev_step, callback.message, state)
    

async def remove_post_from_channel(bot: Bot, row):
    if not CHANNEL_USERNAME:
        return

    channel_message_id = row["channel_message_id"] if row and "channel_message_id" in row.keys() else None

    if not channel_message_id:
        return

    try:
        await bot.delete_message(CHANNEL_USERNAME, channel_message_id)

    except Exception as e:
        error_text = str(e).lower()

        if (
            "message can't be deleted" in error_text
            or "message to delete not found" in error_text
            or "message identifier is not specified" in error_text
        ):
            logger.warning(
                "CHANNEL DELETE SKIPPED: post_id=%s message_id=%s reason=%s",
                row["id"] if row and "id" in row.keys() else None,
                channel_message_id,
                e
            )
        else:
            logger.exception("CHANNEL DELETE ERROR: %s", e)

    try:
        await db_execute(
            "UPDATE posts SET channel_message_id=NULL WHERE id=?",
            (row["id"],)
        )
    except Exception as e:
        logger.exception("CHANNEL MESSAGE ID CLEAR ERROR: %s", e)


async def try_update_channel_post(bot: Bot, post_id: int):
    row = await get_post(post_id)
    if not row:
        return

    channel_message_id = row["channel_message_id"]

    if not channel_message_id or not CHANNEL_USERNAME:
        return

    try:
        await bot.edit_message_text(
            chat_id=CHANNEL_USERNAME,
            message_id=channel_message_id,
            text=await post_text(row, for_channel=True),
            reply_markup=channel_post_kb(post_id, row["post_type"])
        )
    except Exception as e:
        logger.exception(f"CHANNEL UPDATE ERROR: {e}")
        

async def notify_coincidence_users(bot: Bot, new_post_id: int):
    new_row = await get_post(new_post_id)
    if not new_row or new_row["status"] != STATUS_ACTIVE:
        return

    coincidences = await get_coincidences(
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

        if not await reserve_coincidence_notification(new_row["id"], row["id"]):
               continue

        intro = format_coincidence_badges(score, notes)

        try:
            await send_post_card_to_user(
                bot,
                new_row["user_id"],
                row,
                prefix_text=f"🔔 Найдено новое совпадение!\n\n{intro}"
            )
        except Exception as e:
            logger.exception("COINCIDENCE SEND A ERROR: %s", e)

        try:
            await send_post_card_to_user(
                bot,
                row["user_id"],
                new_row,
                prefix_text=f"🔔 Найдено новое совпадение!\n\n{intro}"
            )
        except Exception as e:
            logger.exception("COINCIDENCE SEND B ERROR: %s", e)


async def notify_subscribers(bot: Bot, post_id: int):
    row = await get_post(post_id)
    if not row or row["status"] != STATUS_ACTIVE:
        return

    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT * FROM route_subscriptions
            WHERE post_type=?
              AND from_country=?
              AND to_country=?
              AND user_id != ?
              AND (from_city IS NULL OR from_city='' OR from_city=COALESCE(?, ''))
              AND (to_city IS NULL OR to_city='' OR to_city=COALESCE(?, ''))
            ORDER BY created_at DESC
            LIMIT 50
        """, (
            row["post_type"],
            row["from_country"],
            row["to_country"],
            row["user_id"],
            row["from_city"],
            row["to_city"]
        ))

        subscribers = await cur.fetchall()

    for sub in subscribers:
        try:
            await send_post_card_to_user(
                bot,
                sub["user_id"],
                row,
                prefix_text="🔔 По вашей подписке появилось новое объявление:"
            )
        except Exception as e:
            logger.exception("SUBSCRIBER SEND ERROR: %s", e)
            

async def notify_cargo_users(bot: Bot, lead_id: int):
    lead = await get_cargo_lead(lead_id)
    if not lead:
        return

    cargo_users = await get_cargo_users()

    # ---- АДМИН ----
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                "🆕 <b> Вам пришел новый карго-лид</b>\n\n" + cargo_lead_contact_text(lead),
                reply_markup=cargo_lead_kb(lead_id)
            )
        except Exception as e:
            logger.warning("Не удалось отправить cargo лид админу %s: %s", admin_id, e)

    # ---- КАРГО ----
    for cargo in cargo_users:
        try:
            await bot.send_message(
                cargo["user_id"],
                cargo_lead_preview_text(lead),
                reply_markup=cargo_lead_kb(lead_id)
            )
        except Exception as e:
            logger.warning("Не удалось отправить cargo лид карго %s: %s", cargo["user_id"], e)
            

async def run_global_coincidence_scan(bot: Bot):
    try:
        async with await connect_db() as conn:
            cur = await conn.execute("""
                SELECT p.*, u.username, u.full_name
                FROM posts p
                LEFT JOIN users u ON u.user_id = p.user_id
                WHERE p.status='active'
                  AND (p.expires_at IS NULL OR p.expires_at > ?)
                ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
                LIMIT 300
            """, (now_ts(),))

            rows = await cur.fetchall()

        for row in rows:
            coincidences = await get_coincidences(
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

                if not await reserve_coincidence_notification(row["id"], target["id"]):
                    continue

                intro = format_coincidence_badges(score, notes)

                try:
                    await send_post_card_to_user(
                        bot,
                        row["user_id"],
                        target,
                        prefix_text=f"🔔 Найдено новое совпадение!\n\n{intro}"
                    )
                except Exception as e:
                    logger.exception("GLOBAL COINCIDENCE SEND A ERROR: %s", e)

                try:
                    await send_post_card_to_user(
                        bot,
                        target["user_id"],
                        row,
                        prefix_text=f"🔔 Найдено новое совпадение!\n\n{intro}"
                    )
                except Exception as e:
                    logger.exception("GLOBAL COINCIDENCE SEND B ERROR: %s", e)

    except Exception as e:
        logger.exception("GLOBAL COINCIDENCE SCAN ERROR: %s", e)


async def expire_old_posts(bot: Bot):
    while True:
        try:
            async with await connect_db() as conn:
                cur = await conn.execute("""
                    SELECT p.*, u.username, u.full_name
                    FROM posts p
                    LEFT JOIN users u ON u.user_id = p.user_id
                    WHERE p.status IN ('active','inactive')
                      AND p.expires_at IS NOT NULL
                      AND p.expires_at <= ?
                    LIMIT 50
                """, (now_ts(),))
                rows = await cur.fetchall()  # ← ИСПРАВЛЕНО: отдельная строка

            if rows:  # ← ИСПРАВЛЕНО: if rows (было if row — опечатка!)
                for row in rows:
                    await remove_post_from_channel(bot, row)

                async with await connect_db() as conn:
                    for row in rows:
                        await conn.execute(
                            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
                            (STATUS_EXPIRED, now_ts(), row["id"])
                        )
                    await conn.commit()  # ← ДОБАВЛЕНО: commit после всех UPDATE

                for row in rows:
                    try:
                        await bot.send_message(
                            row["user_id"],
                            f"⌛ Ваше объявление ID {row['id']} истекло и скрыто.\nОткройте 'Мои объявления', чтобы активировать его снова.",
                            reply_markup=main_menu(row["user_id"])
                        )
                    except Exception as e:
                        logger.exception("EXPIRE USER NOTIFY ERROR: %s", e)
        except Exception as e:
            logger.exception("EXPIRE LOOP ERROR: %s", e)

        await asyncio.sleep(300)


async def global_coincidence_loop(bot: Bot):
    while True:
        await run_global_coincidence_scan(bot)
        await asyncio.sleep(300)


async def dispute_timeout_loop(bot: Bot):
    while True:
        try:
            async with await connect_db() as conn:

                cur = await conn.execute("""
                    SELECT *
                    FROM disputes
                    WHERE status='waiting_response'
                      AND response_deadline_at <= ?
                """, (now_ts(),))

                disputes = await cur.fetchall()

                if not disputes:
                    await asyncio.sleep(600)
                    continue

                for dispute in disputes:
                    deal = await get_deal(dispute["deal_id"])

                    await conn.execute(
                        "UPDATE disputes SET status=?, updated_at=? WHERE id=?",
                        (DISPUTE_EXPIRED, now_ts(), dispute["id"])
                    )

                    await conn.execute(
                        "UPDATE deals SET status=?, updated_at=? WHERE id=?",
                        (DEAL_FAILED, now_ts(), dispute["deal_id"])
                    )

                    await conn.execute("""
                        UPDATE users
                        SET dispute_no_response_count = COALESCE(dispute_no_response_count, 0) + 1
                        WHERE user_id=?
                    """, (dispute["against_user_id"],))

                await conn.commit()  # 🔥 ОДИН commit на всё

            # --- ВНЕ БД (важно!) ---
            for dispute in disputes:
                deal = await get_deal(dispute["deal_id"])

                await ban_user_with_cleanup(bot, dispute["against_user_id"])

                try:
                    await bot.send_message(
                        dispute["against_user_id"],
                        "⛔ Вы не ответили по спору в установленный срок.\n"
                        f"Срок ответа: {DISPUTE_RESPONSE_HOURS} часов.\n\n"
                        "Сделка признана неуспешной.\n"
                        "Ваш аккаунт временно ограничен.\n"
                        "Если произошла ошибка — свяжитесь с администратором.",
                        reply_markup=dispute_failed_against_kb(deal["id"]) if deal else None
                    )
                except Exception as e:
                    logger.exception("DISPUTE TIMEOUT TARGET ERROR: %s", e)

                try:
                    await bot.send_message(
                        dispute["opened_by_user_id"],
                        "⚠️ Вторая сторона не ответила по спору в установленный срок.\n"
                        f"Срок ожидания: {DISPUTE_RESPONSE_HOURS} часов.\n\n"
                        "Спор закрыт автоматически.\n"
                        "Сделка признана неуспешной.",
                        reply_markup=dispute_failed_opened_by_kb(deal["id"]) if deal else None
                    )
                except Exception as e:
                    logger.exception("DISPUTE TIMEOUT OPENER ERROR: %s", e)

                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(
                            admin_id,
                            f"⛔ Пользователь {dispute['against_user_id']} не ответил "
                            f"по спору сделки #{dispute['deal_id']} и был ограничен."
                        )
                    except Exception:
                        pass

        except Exception as e:
            logger.exception("DISPUTE TIMEOUT LOOP ERROR: %s", e)

        await asyncio.sleep(600)
        

async def begin_create(message: Message, state: FSMContext, post_type: str):
    await upsert_user(message)

    if await is_user_banned(message.from_user.id):
        await message.answer(
            "⛔ Ваш аккаунт ограничен. Если это ошибка — свяжитесь с администратором.",
            reply_markup=main_menu(message.from_user.id)
        )
        return

    spam_error = await anti_spam_check(message.from_user.id)
    if spam_error:
        await message.answer(
            spam_error,
            reply_markup=main_menu(message.from_user.id)
        )
        return

    if await user_post_create_rate_limited(message.from_user.id):
        await message.answer(
            "Вы слишком часто создаете объявления. Подождите немного и попробуйте снова.",
            reply_markup=main_menu(message.from_user.id)
        )
        return

    user_limit = await get_user_post_limit(message.from_user.id)
    if await active_post_count(message.from_user.id) >= user_limit:
        await message.answer(
            f"У вас уже слишком много объявлений. Лимит: {user_limit}. Удалите или деактивируйте старые объявления.",
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
    

async def owner_only(callback: CallbackQuery, post_id: int) -> Optional[aiosqlite.Row]:
    row = await get_post(post_id)
    if not row or row["user_id"] != callback.from_user.id:
        return None
    return row

def format_deadline_left(ts_value: Optional[int]) -> str:
    if not ts_value:
        return "не указан"
    diff = int(ts_value) - now_ts()
    if diff <= 0:
        return "время истекло"

    hours = diff // 3600
    minutes = (diff % 3600) // 60

    if hours > 0:
        return f"{hours} ч {minutes} мин"
    return f"{minutes} мин"


def format_dispute_status(status: str) -> str:
    mapping = {
        DISPUTE_OPEN: "открыт",
        DISPUTE_WAITING_RESPONSE: "ожидается ответ второй стороны",
        DISPUTE_RESPONDED: "ответ получен",
        DISPUTE_EXPIRED: "истек по времени",
        DISPUTE_RESOLVED: "решен",
        DISPUTE_CLOSED_UNRESOLVED: "закрыт без решения",
    }
    return mapping.get(status, status)


async def get_user_row(user_id: int):
    async with await connect_db() as conn:
        cur = await conn.execute("""
            SELECT user_id, username, full_name, created_at, is_banned, is_verified,
                   dispute_no_response_count, onboarding_completed
            FROM users
            WHERE user_id = ?
            LIMIT 1
        """, (user_id,))
        return await cur.fetchone()


async def set_user_ban_status(user_id: int, is_banned: int):
    async with await connect_db() as conn:
        await conn.execute("""
            UPDATE users
            SET is_banned = ?
            WHERE user_id = ?
        """, (is_banned, user_id))
        await conn.commit()


def fmt_ts(ts: int | None) -> str:
    if not ts:
        return "—"
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)


async def render_user_admin_card(user_row) -> str:
    if not user_row:
        return "Пользователь не найден."

    username_text = f"@{user_row['username']}" if user_row["username"] else "—"
    full_name_text = user_row["full_name"] or "—"
    banned_text = "Да 🚫" if user_row["is_banned"] else "Нет ✅"
    verified_text = "Да" if user_row["is_verified"] else "Нет"

    return (
        "👤 <b>Профиль пользователя</b>\n\n"
        f"ID: <code>{user_row['user_id']}</code>\n"
        f"Username: {username_text}\n"
        f"Имя: {full_name_text}\n"
        f"Забанен: {banned_text}\n"
        f"Верифицирован: {verified_text}\n"
        f"Onboarding: {user_row['onboarding_completed']}\n"
        f"Пропуски ответа по спорам: {user_row['dispute_no_response_count']}\n"
        f"Создан: {fmt_ts(user_row['created_at'])}"
    )


async def get_post_owner_user_id(post_id: int):
    row = await db_fetchone("""
        SELECT user_id
        FROM posts
        WHERE id = ?
        LIMIT 1
    """, (post_id,))
    return row["user_id"] if row else None


def admin_user_moderation_kb(target_user_id: int, is_banned: int):
    buttons = [
        [
            InlineKeyboardButton(
                text="👤 Профиль",
                callback_data=f"admin_user_profile:{target_user_id}"
            )
        ],
        [
            InlineKeyboardButton(
                text="✅ Разбанить" if is_banned else "🚫 Забанить",
                callback_data=f"admin_toggle_ban:{target_user_id}"
            )
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_contact_kb():
    rows = []

    for admin_id in ADMIN_IDS:
        rows.append([
            InlineKeyboardButton(
                text="🆘 Связаться с администратором",
                url=f"tg://user?id={admin_id}"
            )
        ])
        break

    return InlineKeyboardMarkup(inline_keyboard=rows or [[
        InlineKeyboardButton(text="Ок", callback_data="noop")
    ]])
    

def dispute_text(dispute: aiosqlite.Row) -> str:
    lines = [
        f"⚖️ <b>Спор по сделке #{dispute['deal_id']}</b>",
        f"<b>Статус:</b> {format_dispute_status(dispute['status'])}",
    ]

    if dispute["reason_text"]:
        lines.append(f"<b>Причина:</b> {html.escape(dispute['reason_text'])}")

    if dispute["response_text"]:
        lines.append(f"<b>Ответ второй стороны:</b> {html.escape(dispute['response_text'])}")

    if dispute["status"] in (DISPUTE_WAITING_RESPONSE, DISPUTE_OPEN):
        lines.append(f"<b>До авто-завершения:</b> {format_deadline_left(dispute['response_deadline_at'])}")

    return "\n".join(lines)


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
        CreatePost.travel_date.state: "delivery_date",       
        CreatePost.travel_date_manual.state: "delivery_date", 
        CreatePost.weight.state: "weight",
        CreatePost.weight_manual.state: "weight",
        CreatePost.description.state: "description",
        CreatePost.photo_choice.state: "photo_choice",
        CreatePost.photo_upload.state: "photo_choice",
        CreatePost.contact_note.state: "contact",            
    }
    return mapping.get(state_name)

CREATE_STEP_CLEANUP_KEYS = {
    "from_country": ["from_country", "from_city", "to_country", "to_city", "delivery_date", "weight_kg", "description", "photo_file_id", "contact_note"],
    "from_city": ["from_city", "to_country", "to_city", "delivery_date", "weight_kg", "description", "photo_file_id", "contact_note"],
    "to_country": ["to_country", "to_city", "delivery_date", "weight_kg", "description", "photo_file_id", "contact_note"],
    "to_city": ["to_city", "delivery_date", "weight_kg", "description", "photo_file_id", "contact_note"],
    "delivery_date": ["delivery_date", "weight_kg", "description", "photo_file_id", "contact_note"], 
    "weight": ["weight_kg", "description", "photo_file_id", "contact_note"],
    "description": ["description", "photo_file_id", "contact_note"],
    "photo_choice": ["photo_file_id", "contact_note"],
    "contact": ["contact_note"],  
}


async def clear_step_data_from(state: FSMContext, target_step: str):
    data = await state.get_data()
    for key in CREATE_STEP_CLEANUP_KEYS.get(target_step, []):
        data.pop(key, None)
    await state.set_data(data)


def cargo_form_text(step: int, prompt: str) -> str:
    return (
        "🚀 <b>Быстрая доставка (карго)</b>\n\n"
        "━━━━━━━━━━━━━━\n"
        f"Шаг {step} / 9\n"
        "━━━━━━━━━━━━━━\n\n"
        f"{prompt}"
    )


async def render_create_step(step: str, target, state: FSMContext):
    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    if step == "from_country":
        await state.set_state(CreatePost.from_country)
        await smart_form_answer(
            target,
            form_text(post_type, 1, "Выберите страну отправления"),
            reply_markup=countries_select_kb("from_country_pick")
        )

    elif step == "from_city":
        await state.set_state(CreatePost.from_city)
        await smart_form_answer(
            target,
            form_text(post_type, 2, f"Выберите город отправления в стране {data.get('from_country', '')}"),
            reply_markup=cities_select_kb("from_city_pick", data.get("from_country"), include_back=True)
        )

    elif step == "to_country":
        await state.set_state(CreatePost.to_country)
        await smart_form_answer(
            target,
            form_text(post_type, 3, "Выберите страну назначения"),
            reply_markup=countries_select_kb("to_country_pick")
        )

    elif step == "to_city":
        await state.set_state(CreatePost.to_city)
        await smart_form_answer(
            target,
            form_text(post_type, 4, f"Выберите город назначения в стране {data.get('to_country', '')}"),
            reply_markup=cities_select_kb("to_city_pick", data.get("to_country"), include_back=True)
        )

    elif step == "delivery_date":
        await state.set_state(CreatePost.travel_date)
        await smart_form_answer(
            target,
            form_text(post_type, 5, "Выберите дату поездки / отправки"),
            reply_markup=date_select_kb()
        )

    elif step == "weight":
        await state.set_state(CreatePost.weight)
        await smart_form_answer(
            target,
            form_text(post_type, 6, "Выберите вес или объём"),
            reply_markup=weight_select_kb()
        )

    elif step == "description":
        await state.set_state(CreatePost.description)
        await target.answer(
            form_text(post_type, 7, "Опишите посылку или что готовы взять"),
            reply_markup=back_only_kb()
        )

    elif step == "photo_choice":
        await state.set_state(CreatePost.photo_choice)
        await smart_form_answer(
            target,
            form_text(post_type, 8, "Хотите добавить фото посылки? Это необязательно."),
            reply_markup=photo_choice_kb()
        )

    elif step == "contact":
        await state.set_state(CreatePost.contact_note)
        await target.answer(
            form_text(post_type, 9, "Оставьте контакт для связи"),
            reply_markup=back_only_kb()
        )
        

async def render_cargo_step(target_step: str, target_message: Message, state: FSMContext):
    data = await state.get_data()

    if target_step == "from_country":
        await state.set_state(CargoLeadFlow.from_country)
        await target_message.answer(
            "🚀 <b>Быстрая доставка (карго)</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            "Шаг 1 / 9\n"
            "━━━━━━━━━━━━━━\n\n"
            "Выберите страну отправления",
            reply_markup=countries_select_kb("cargo_from_country", include_back=False)
        )
        return

    if target_step == "from_city":
        await state.set_state(CargoLeadFlow.from_city)
        country = data.get("from_country", "")
        await target_message.answer(
            "🚀 <b>Быстрая доставка (карго)</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            "Шаг 2 / 9\n"
            "━━━━━━━━━━━━━━\n\n"
            f"Выберите город отправления в стране {country}",
            reply_markup=cities_select_kb("cargo_from_city", country, include_back=True, back_callback="back:cargo")
        )
        return

    if target_step == "to_country":
        await state.set_state(CargoLeadFlow.to_country)
        await target_message.answer(
            "🚀 <b>Быстрая доставка (карго)</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            "Шаг 3 / 9\n"
            "━━━━━━━━━━━━━━\n\n"
            "Выберите страну назначения",
            reply_markup=countries_select_kb("cargo_to_country", include_back=True, back_callback="back:cargo")
        )
        return

    if target_step == "to_city":
        await state.set_state(CargoLeadFlow.to_city)
        country = data.get("to_country", "")
        await target_message.answer(
            "🚀 <b>Быстрая доставка (карго)</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            "Шаг 4 / 9\n"
            "━━━━━━━━━━━━━━\n\n"
            f"Выберите город назначения в стране {country}",
            reply_markup=cities_select_kb("cargo_to_city", country, include_back=True, back_callback="back:cargo")
        )
        return

    if target_step == "delivery_date":
        await state.set_state(CargoLeadFlow.delivery_date)
        await target_message.answer(
            "🚀 <b>Быстрая доставка (карго)</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            "Шаг 5 / 9\n"
            "━━━━━━━━━━━━━━\n\n"
            "Выберите желаемую дату отправки / доставки",
            reply_markup=cargo_date_select_kb()
        )
        return

    if target_step == "weight":
        await state.set_state(CargoLeadFlow.weight)
        await target_message.answer(
            "🚀 <b>Быстрая доставка (карго)</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            "Шаг 6 / 9\n"
            "━━━━━━━━━━━━━━\n\n"
            "Выберите вес или объём груза",
            reply_markup=cargo_weight_select_kb()
        )
        return

    if target_step == "description":
        await state.set_state(CargoLeadFlow.description)
        await target_message.answer(
            "🚀 <b>Быстрая доставка (карго)</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            "Шаг 7 / 9\n"
            "━━━━━━━━━━━━━━\n\n"
            "Опишите груз подробно\nЧто за товар / объём / упаковка / срочность",
            reply_markup=cargo_back_only_kb()
        )
        return

    if target_step == "photo_choice":
        await state.set_state(CargoLeadFlow.photo_choice)
        await target_message.answer(
            "🚀 <b>Быстрая доставка (карго)</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            "Шаг 8 / 9\n"
            "━━━━━━━━━━━━━━\n\n"
            "Хотите добавить фото груза? Это необязательно.",
            reply_markup=cargo_photo_choice_kb()
        )
        return

    if target_step == "contact":
        await state.set_state(CargoLeadFlow.contact)
        await target_message.answer(
            "🚀 <b>Быстрая доставка (карго)</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            "Шаг 9 / 9\n"
            "━━━━━━━━━━━━━━\n\n"
            "Укажите контакт для связи.\n\n"
            "Лучше Telegram или WeChat ID.",
            reply_markup=cargo_back_only_kb()
        )
        return
        

@router.callback_query(F.data.startswith("contact_admin:"))
async def contact_admin_handler(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split(":")[1])

    deal = await get_deal(deal_id)
    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    await state.set_state(AdminContactFlow.message)
    await state.update_data(deal_id=deal_id)

    await callback.message.answer(
        "✉️ Напишите сообщение администратору.\n\n"
        "Опишите проблему по сделке."
    )

    await callback.answer()


@router.message(F.text == "🤝 Мои сделки")
async def my_deals_menu(message: Message):
    await upsert_user(message)
    await message.answer(MENU_TEXTS["deals"], reply_markup=main_menu(message.from_user.id))
    await show_user_deals_sections(
        message,
        message.from_user.id,
        include_descriptions=True
    )


@router.message(Command("search_user"))
async def admin_find_handler(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /search_user username")
        return


@router.message(Command("ban"))
async def admin_ban_handler(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /ban 123456789")
        return

    arg = parts[1].strip()

    target_user_id = None

    if arg.isdigit():
        target_user_id = int(arg)
    else:
        row = await find_user_by_username(arg)
        if row:
            target_user_id = row["user_id"]

    if not target_user_id:
        await message.answer("Пользователь не найден.")
        return

    await ban_user_with_cleanup(message.bot, target_user_id)
    await message.answer(f"🚫 Пользователь <code>{target_user_id}</code> забанен.")


@router.message(Command("unban"))
async def admin_unban_handler(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /unban 123456789")
        return

    arg = parts[1].strip()

    target_user_id = None

    if arg.isdigit():
        target_user_id = int(arg)
    else:
        row = await find_user_by_username(arg)
        if row:
            target_user_id = row["user_id"]

    if not target_user_id:
        await message.answer("Пользователь не найден.")
        return

    await unban_user(target_user_id)
    await message.answer(f"✅ Пользователь <code>{target_user_id}</code> разбанен.")


@router.message(Command("id"))
async def admin_id_handler(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /id @username или /id 123456789")
        return

    arg = parts[1].strip()

    if arg.isdigit():
        row = await find_user_by_id(int(arg))
    else:
        row = await find_user_by_username(arg)

    if not row:
        await message.answer("Пользователь не найден.")
        return

    username_text = f"@{row['username']}" if row["username"] else "—"
    full_name_text = row["full_name"] or "—"
    banned_text = "Да" if row["is_banned"] else "Нет"

    await message.answer(
        f"👤 ID: <code>{row['user_id']}</code>\n"
        f"Username: {username_text}\n"
        f"Имя: {full_name_text}\n"
        f"Забанен: {banned_text}"
    )
    

@router.message(AdminContactFlow.message)
async def admin_contact_message(message: Message, state: FSMContext):
    data = await state.get_data()
    deal_id = data.get("deal_id")

    deal = await get_deal(deal_id)
    if not deal:
        await message.answer("Сделка не найдена.")
        await state.clear()
        return

    post = await get_post(deal["post_id"])
    route = ""
    if post:
        route = f"{post['from_country']}"
        if post["from_city"]:
            route += f", {post['from_city']}"
        route += " → "
        route += f"{post['to_country']}"
        if post["to_city"]:
            route += f", {post['to_city']}"

    username = f"@{message.from_user.username}" if message.from_user.username else "без username"

    text = (
        "⚠️ <b>Запрос администратору по сделке</b>\n\n"
        f"<b>Пользователь:</b> {username}\n"
        f"<b>ID пользователя:</b> {message.from_user.id}\n\n"
        f"<b>ID сделки:</b> {deal_id}\n"
        f"<b>ID объявления:</b> {deal['post_id']}\n"
        f"<b>Маршрут:</b> {route}\n\n"
        f"<b>Сообщение:</b>\n{html.escape(message.text or '')}"
    )

    for admin_id in ADMIN_IDS:
        try:
            await message.bot.send_message(
                admin_id,
                text,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="💬 Написать пользователю",
                                url=f"tg://user?id={message.from_user.id}"
                            )
                        ]
                    ]
                )
            )
        except Exception as e:
            logger.exception("ADMIN CONTACT ERROR: %s", e)

    await message.answer(
        "📩 Ваше сообщение отправлено администратору.\n"
        "Он свяжется с вами напрямую в Telegram."
    )
    await state.clear()


@router.message(F.text == "🚀 Быстрая доставка (карго)")
async def cargo_lead_start(message: Message, state: FSMContext):
    await upsert_user(message)

    spam = await anti_spam_check(message.from_user.id)
    if spam:
        await message.answer(spam)
        return

    await state.clear()

    await message.answer(
        "🚀 <b>Вы создаёте заявку на карго-доставку</b>\n\n"
        "Это не поиск попутчика, а заявка для проверенных карго-компаний, партнеров сервиса.\n\n"
        "Подходит для:\n"
        "⚡ срочной доставки (например образцы)\n"
        "💰 дорогих товаров\n"
        "📦 коммерческих грузов\n\n"
        "Представитель компании сможет связаться с вами и предложить условия."
    )

    await render_cargo_step("from_country", message, state)
    

@router.callback_query(F.data.startswith("cargo_from_country:"))
async def cargo_from_country(callback: CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        country = callback.data.split(":", 1)[1]

        if country == "__manual__":
            await state.set_state(CargoLeadFlow.from_country_manual)
            await callback.message.answer(
                cargo_form_text(1, "Введите страну отправления вручную"),
                reply_markup=cargo_back_only_kb()
            )
            return

        await state.update_data(from_country=country)
        await render_cargo_step("from_city", callback.message, state)

    except Exception as e:
        logger.exception("CARGO_FROM_COUNTRY ERROR: %s", e)
        await callback.answer("Ошибка. Попробуйте ещё раз.", show_alert=True)


@router.callback_query(F.data.startswith("cargo_from_city:"))
async def cargo_from_city(callback: CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        city = callback.data.split(":", 1)[1]

        if city == "__manual__":
            await state.set_state(CargoLeadFlow.from_city_manual)
            await callback.message.answer(
                cargo_form_text(2, "Введите город вручную"),
                reply_markup=cargo_back_only_kb()
            )
            return

        await state.update_data(from_city=city)
        await render_cargo_step("to_country", callback.message, state)

    except Exception as e:
        logger.exception("CARGO_FROM_CITY ERROR: %s", e)
        await callback.answer("Ошибка. Попробуйте ещё раз.", show_alert=True)


@router.callback_query(F.data.startswith("cargo_to_country:"))
async def cargo_to_country(callback: CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        country = callback.data.split(":", 1)[1]

        if country == "__manual__":
            await state.set_state(CargoLeadFlow.to_country_manual)
            await callback.message.answer(
                cargo_form_text(3, "Введите страну назначения"),
                reply_markup=cargo_back_only_kb()
            )
            return

        await state.update_data(to_country=country)
        await render_cargo_step("to_city", callback.message, state)

    except Exception as e:
        logger.exception("CARGO_TO_COUNTRY ERROR: %s", e)
        await callback.answer("Ошибка. Попробуйте ещё раз.", show_alert=True)


@router.callback_query(F.data.startswith("cargo_to_city:"))
async def cargo_to_city(callback: CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        city = callback.data.split(":", 1)[1]

        if city == "__manual__":
            await state.set_state(CargoLeadFlow.to_city_manual)
            await callback.message.answer(
                cargo_form_text(4, "Введите город назначения"),
                reply_markup=cargo_back_only_kb()
            )
            return

        await state.update_data(to_city=city)
        await render_cargo_step("delivery_date", callback.message, state)

    except Exception as e:
        logger.exception("CARGO_TO_CITY ERROR: %s", e)
        await callback.answer("Ошибка. Попробуйте ещё раз.", show_alert=True)


@router.callback_query(F.data.startswith("datepick:"), CargoLeadFlow.delivery_date)
async def cargo_delivery_date(callback: CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        value = callback.data.split(":", 1)[1]
        today = datetime.now()

        if value == "manual":
            await callback.message.answer(
                cargo_form_text(5, "Введите дату вручную.\n\nНапример: 15.05.2026"),
                reply_markup=cargo_back_only_kb()
            )
            return

        if value == "week":
            delivery_date = f"до {(today + timedelta(days=7)).strftime('%d.%m.%Y')}"
        elif value == "month":
            start = today.strftime("%d.%m.%Y")
            end = (today + timedelta(days=30)).strftime("%d.%m.%Y")
            delivery_date = f"{start} - {end}"
        else:
            delivery_date = value

        await state.update_data(delivery_date=delivery_date)
        await render_cargo_step("weight", callback.message, state)

    except Exception as e:
        logger.exception("CARGO_DELIVERY_DATE ERROR: %s", e)
        await callback.answer("Ошибка. Попробуйте ещё раз.", show_alert=True)


@router.callback_query(F.data.startswith("cargo_weight:"), CargoLeadFlow.weight)
async def cargo_weight(callback: CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        weight = callback.data.split(":", 1)[1]

        if weight == "__manual__":
            await state.set_state(CargoLeadFlow.weight_manual)
            await callback.message.answer(
                cargo_form_text(6, "Введите вес или объём вручную.\n\nНапример: 35 кг, 2 коробки, 0.5 куба"),
                reply_markup=cargo_back_only_kb()
            )
            return

        await state.update_data(weight=weight)
        await render_cargo_step("description", callback.message, state)

    except Exception as e:
        logger.exception("CARGO_WEIGHT ERROR: %s", e)
        await callback.answer("Ошибка. Попробуйте ещё раз.", show_alert=True)


@router.callback_query(F.data.startswith("cargo_photo_choice:"), CargoLeadFlow.photo_choice)
async def cargo_photo_choice(callback: CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        action = callback.data.split(":", 1)[1]

        if action == "add":
            await state.set_state(CargoLeadFlow.photo_upload)
            await callback.message.answer(
                cargo_form_text(8, "Отправьте одно фото груза."),
                reply_markup=cargo_back_only_kb()
            )
            return

        await state.update_data(photo_file_id=None)
        await render_cargo_step("contact", callback.message, state)

    except Exception as e:
        logger.exception("CARGO_PHOTO_CHOICE ERROR: %s", e)
        await callback.answer("Ошибка. Попробуйте ещё раз.", show_alert=True)


@router.message(CargoLeadFlow.delivery_date)
async def cargo_delivery_date_manual(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    if len(text) < 3:
        await message.answer("Введите дату.", reply_markup=cargo_back_only_kb())
        return

    await state.update_data(delivery_date=text)
    await render_cargo_step("weight", message, state)

@router.message(CargoLeadFlow.weight_manual)
async def cargo_weight_manual(message: Message, state: FSMContext):
    weight = (message.text or "").strip()

    if len(weight) < 1:
        await message.answer("Введите вес или объём.", reply_markup=cargo_back_only_kb())
        return

    await state.update_data(weight=weight)
    await render_cargo_step("description", message, state)


@router.message(CargoLeadFlow.from_country_manual)
async def cargo_from_country_manual(message: Message, state: FSMContext):
    await state.update_data(from_country=message.text.strip())
    await render_cargo_step("from_city", message, state)


@router.message(CargoLeadFlow.from_city_manual)
async def cargo_from_city_manual(message: Message, state: FSMContext):
    await state.update_data(from_city=message.text.strip())
    await render_cargo_step("to_country", message, state)


@router.message(CargoLeadFlow.to_country_manual)
async def cargo_to_country_manual(message: Message, state: FSMContext):
    await state.update_data(to_country=message.text.strip())
    await render_cargo_step("to_city", message, state)


@router.message(CargoLeadFlow.to_city_manual)
async def cargo_to_city_manual(message: Message, state: FSMContext):
    await state.update_data(to_city=message.text.strip())
    await render_cargo_step("delivery_date", message, state)
    

@router.message(CargoLeadFlow.description)
async def cargo_description(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    if len(text) < 3:
        await message.answer("Опишите груз чуть подробнее.")
        return

    await state.update_data(description=text[:1000])
    await render_cargo_step("photo_choice", message, state)


@router.message(CargoLeadFlow.photo_upload)
async def cargo_photo_upload(message: Message, state: FSMContext):
    if not message.photo:
        await message.answer(
            "Отправьте фото груза.",
            reply_markup=cargo_back_only_kb()
        )
        return

    await state.update_data(photo_file_id=message.photo[-1].file_id)
    await render_cargo_step("contact", message, state)


@router.message(CargoLeadFlow.contact)
async def cargo_contact(message: Message, state: FSMContext):
    contact = (message.text or "").strip()

    if len(contact) < 3:
        await message.answer("Укажите корректный контакт.")
        return

    data = await state.get_data()

    try:
        lead_id = await create_cargo_lead(
            user_id=message.from_user.id,
            from_place=f"{data.get('from_country', '')}, {data.get('from_city', '')}",
            to_place=f"{data.get('to_country', '')}, {data.get('to_city', '')}",
            delivery_date=data.get("delivery_date") or "",
            weight=data.get("weight") or "",
            cargo_desc=data.get("description") or "",
            contact=contact,
            photo_file_id=data.get("photo_file_id")
        )
    except Exception as e:
        logger.exception("CARGO SAVE ERROR: %s", e)
        await message.answer(f"❌ Ошибка при сохранении заявки: {e}")
        return

    await state.clear()

    await message.answer(
        "✅ Заявка создана.\n\n"
        "Мы передали её карго-партнерам. Представитель компании свяжется с вами с предложением.",
        reply_markup=main_menu(message.from_user.id)
    )

    try:
        await notify_cargo_users(message.bot, lead_id)
    except Exception as e:
        logger.exception("CARGO NOTIFY ERROR: %s", e)
    

@router.inline_query()
async def inline_search_handler(inline_query: InlineQuery):
    query = (inline_query.query or "").strip()
    offset = int(inline_query.offset or "0") if (inline_query.offset or "0").isdigit() else 0
    rows = await search_posts_inline(query, limit=INLINE_PAGE_SIZE, offset=offset)
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
                id=f"{row['id']}_{offset}",
                title=title[:256],
                description=description,
                input_message_content=InputTextMessageContent(
                    message_text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                ),
                reply_markup= await public_post_kb(row["id"], row["user_id"]),
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
    next_offset = str(offset + INLINE_PAGE_SIZE) if len(rows) == INLINE_PAGE_SIZE else ""
    await inline_query.answer(results, cache_time=1, is_personal=True, next_offset=next_offset)


# =========================
# ГЛОБАЛЬНОЕ МЕНЮ
# =========================


@router.message(StateFilter("*"), F.text == "🚩 Жалоба / Баг / Поддержка")
async def support_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Выберите, что хотите сделать:",
        reply_markup=support_menu_kb()
    )
    

@router.message(F.text.in_(MAIN_MENU_TEXTS))
async def global_main_menu_router(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    if text == "👨‍💼 Админка" and not is_admin(message.from_user.id):
        await message.answer("Нет доступа.")
        return

    await state.clear()

    if text == "✈️ Взять посылку":
        await add_trip(message, state)
        return

    if text == "📦 Отправить посылку":
        await add_parcel(message, state)
        return

    if text == "🔎 Найти совпадения":
        await find_start(message, state)
        return

    if text == "📋 Мои объявления":
        await my_posts_handler(message)
        return

    if text == "🤝 Мои сделки":
        await my_deals_menu(message)
        return

    if text == "🔥 Популярные маршруты":
        await popular_routes_handler(message)
        return

    if text == "🆕 Новые объявления":
        await recent_posts_handler(message)
        return

    if text == "🔔 Подписки":
        await subscriptions_menu(message)
        return

    if text == "📊 Статистика":
        await stats_handler(message)
        return

    if text == "💰 Поднять объявление":
        await bump_info(message)
        return

    if text == "🚩 Жалоба / Баг / Поддержка":
        await support_start(message, state)
        return

    if text == "ℹ️ Помощь":
        await help_handler(message)
        return

    if text == "👨‍💼 Админка":
        await admin_menu_handler(message)
        return

    if text == "🛂 Верификация аккаунта":
        await verification_menu_handler(message)
        return
        

@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    try:
        await upsert_user(message)
        await state.clear()

        if await is_user_banned(message.from_user.id):
            await message.answer(
                "⛔ Ваш аккаунт ограничен из-за жалоб пользователей.\nЕсли это ошибка — свяжитесь с администратором."
            )
            return

        start_arg = ""
        if message.text and " " in message.text:
            start_arg = message.text.split(" ", 1)[1].strip()

        print(f"START ARG = {start_arg!r}")

        if start_arg == "wechat":
            await message.answer(
                "👋 <b>Добро пожаловать в Попутчик Китай</b>\n\n"
                "Это сервис для передачи посылок через попутчиков.\n\n"
                "⬇️ Выберите действие в меню ниже.",
                reply_markup=main_menu(message.from_user.id)
            )
            return

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

            if not post_id_str.isdigit():
                await message.answer("Некорректная ссылка на объявление.")
                return

            row = await get_post(int(post_id_str))

            if not row or row["status"] != STATUS_ACTIVE:
                await message.answer("Объявление не найдено или уже неактивно.")
                return

            if row["user_id"] == message.from_user.id:
                await send_post_card(
                    message,
                    row,
                    prefix_text="📋 Это ваше объявление. Здесь можно управлять объявлением:",
                    reply_markup=post_actions_kb(row["id"], row["status"])
                )
                return

            await state.set_state(ContactFlow.message_text)
            await state.update_data(
                post_id=row["id"],
                target_user_id=row["user_id"],
                deal_id=None
            )

            await message.answer("✉️ Вы открыли связь с владельцем объявления:")
            await send_post_card(message, row)
            await message.answer("Напишите сообщение, и я перешлю его владельцу.")
            return

        if start_arg.startswith("post_"):
            post_id_str = start_arg.replace("post_", "", 1)

            if not post_id_str.isdigit():
                await message.answer("Некорректная ссылка на объявление.")
                return

            row = await get_post(int(post_id_str))

            if not row or row["status"] != STATUS_ACTIVE:
                await message.answer("Объявление не найдено или уже неактивно.")
                return

            if row["user_id"] == message.from_user.id:
                await send_post_card(
                    message,
                    row,
                    prefix_text="📋 <b>Это ваше объявление.</b>\n\nЗдесь можно управлять объявлением:",
                    reply_markup=post_actions_kb(row["id"], row["status"])
               )
                return

            await send_post_card(
                message,
                row,
                prefix_text="📤 Открыто объявление по ссылке:"
            )
            return

        if not await is_onboarding_completed(message.from_user.id):
            await state.set_state(OnboardingFlow.screen_1)

            await message.answer_sticker(ONBOARDING_STICKER_ID)

            await show_onboarding_screen(message, 1)
            return
        await message.answer(
            WELCOME_TEXT,
            reply_markup=main_menu(message.from_user.id)
        )

    except Exception as e:
        logger.exception("START HANDLER ERROR: %s", e)
        await message.answer("Произошла ошибка при запуске бота. Попробуйте еще раз.")
        

@router.callback_query(F.data.startswith("onboarding_next:"))
async def onboarding_next_handler(callback: CallbackQuery, state: FSMContext):
    try:
        current = int(callback.data.split(":")[1])
    except Exception:
        await callback.answer("Ошибка", show_alert=True)
        return

    next_screen = current + 1
    if next_screen > 6:
        next_screen = 6

    state_map = {
        1: OnboardingFlow.screen_1,
        2: OnboardingFlow.screen_2,
        3: OnboardingFlow.screen_3,
        4: OnboardingFlow.screen_4,
        5: OnboardingFlow.screen_5,
        6: OnboardingFlow.screen_6,
    }

    if next_screen == 6:
        await set_onboarding_completed(callback.from_user.id)

    await state.set_state(state_map[next_screen])
    await show_onboarding_screen(callback.message, next_screen)
    await callback.answer()

@router.callback_query(F.data.startswith("adminapprove:"))
async def admin_approve_post(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])
    row = await get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    conn = await connect_db()
    try:
        await conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (STATUS_ACTIVE, now_ts(), post_id)
        )
        await conn.commit()
    finally:
        await conn.close()

    row = await get_post(post_id)

    try:
        await callback.bot.send_message(
            row["user_id"],
            f"✅ Ваше объявление ID {post_id} одобрено и опубликовано."
        )
    except Exception as e:
        logger.exception("APPROVE USER NOTIFY ERROR: %s", e)

    await publish_to_channel(bot, post_id)
    await notify_coincidence_users(bot, post_id)
    await notify_subscribers(bot, post_id)

    await callback.message.answer(f"✅ Объявление {post_id} одобрено.")
    await callback.answer()


@router.callback_query(F.data.startswith("adminreject:"))
async def admin_reject_post(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])
    row = await get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    conn = await connect_db()
    try:
        await conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (STATUS_REJECTED, now_ts(), post_id)
        )
        await conn.commit()
    finally:
        await conn.close()

    try:
        await callback.bot.send_message(
            row["user_id"],
            f"❌ Ваше объявление ID {post_id} отклонено модератором."
        )
    except Exception as e:
        logger.exception("REJECT USER NOTIFY ERROR: %s", e)

    await callback.message.answer(f"❌ Объявление {post_id} отклонено.")
    await callback.answer()
    

@router.callback_query(F.data.startswith("admin_user_profile:"))
async def admin_user_profile_handler(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return

    target_user_id = int(callback.data.split(":")[1])
    user_row = await get_user_row(target_user_id)

    if not user_row:
        await callback.answer("Пользователь не найден", show_alert=True)
        return

    await callback.message.edit_text(
        await render_user_admin_card(user_row),
        reply_markup=admin_user_moderation_kb(
            target_user_id=target_user_id,
            is_banned=user_row["is_banned"]
        )
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_toggle_ban:"))
async def admin_toggle_ban_handler(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return

    target_user_id = int(callback.data.split(":")[1])
    user_row = await get_user_row(target_user_id)
    if not user_row:
        await callback.answer("Пользователь не найден", show_alert=True)
        return

    new_status = 0 if user_row["is_banned"] else 1
    if new_status == 1:
        await ban_user_with_cleanup(callback.bot, target_user_id)
    else:
        await unban_user(target_user_id)

    updated_row = await get_user_row(target_user_id)

    await callback.message.edit_text(
        await render_user_admin_card(updated_row),
        reply_markup=admin_user_moderation_kb(
            target_user_id=target_user_id,
            is_banned=updated_row["is_banned"]
        )
    )

    await callback.answer(
        "Пользователь разбанен" if new_status == 0 else "Пользователь забанен"
    )
    

@router.callback_query(F.data.startswith("adminbanpost:"))
async def admin_ban_post_owner(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])
    row = await get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    await ban_user_with_cleanup(callback.bot, row["user_id"])

    try:
        await callback.bot.send_message(
            row["user_id"],
            "⛔ Ваш аккаунт ограничен администратором."
        )
    except Exception:
        pass

    await callback.message.answer(
        f"⛔ Пользователь {row['user_id']} забанен, его объявления скрыты."
    )
    await callback.answer()
    

@router.callback_query(F.data == "onboarding_skip")
async def onboarding_skip_handler(callback: CallbackQuery, state: FSMContext):
    await set_onboarding_completed(callback.from_user.id)
    await state.clear()

    await callback.message.answer(
        "Онбординг пропущен. Основные функции доступны в меню ниже.",
        reply_markup=main_menu(callback.from_user.id)
    )
    await callback.answer()


@router.callback_query(F.data == "admin:stats")
async def admin_stats_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await callback.message.answer(
        await admin_stats_text(),
        reply_markup=admin_menu_kb()
    )
    await callback.answer()


CARGO_LEAD_CONTACT_LIMIT = 3


@router.callback_query(F.data.startswith("cargo_get_contact:"))
async def cargo_get_contact(callback: CallbackQuery):
    user_id = callback.from_user.id

    if not is_admin(user_id) and not await is_cargo_user(user_id):
        await callback.answer("Эта функция доступна только карго-партнерам.", show_alert=True)
        return

    lead_id = int(callback.data.split(":")[1])
    lead = await get_cargo_lead(lead_id)

    if not lead:
        await callback.answer("Заявка не найдена.", show_alert=True)
        return

    conn = await connect_db()
    try:
        cur = await conn.execute("""
            SELECT 1
            FROM cargo_lead_access
            WHERE lead_id=? AND cargo_user_id=?
            LIMIT 1
        """, (lead_id, user_id))
        already_opened = await cur.fetchone()

        if not already_opened and not is_admin(user_id):
            cur = await conn.execute("""
                SELECT COUNT(*) AS c
                FROM cargo_lead_access
                WHERE lead_id=?
            """, (lead_id,))
            row = await cur.fetchone()

            opened_count = int(row["c"] or 0)

            if opened_count >= CARGO_LEAD_CONTACT_LIMIT:
                await callback.answer(
                    "Контакт уже получили другие 3 карго-партнера. Лимит по этому лиду исчерпан.",
                    show_alert=True
                )
                return

        await conn.execute("""
            INSERT OR IGNORE INTO cargo_lead_access (
                lead_id, cargo_user_id, created_at
            )
            VALUES (?, ?, ?)
        """, (lead_id, user_id, now_ts()))

        await conn.commit()

    finally:
        await conn.close()

    await callback.message.answer(cargo_lead_contact_text(lead))
    await callback.answer("Контакт открыт")
    

@router.callback_query(F.data.startswith("onboarding_action:"))
async def onboarding_action_handler(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":")[1]

    await set_onboarding_completed(callback.from_user.id)
    await state.clear()

    if action == "trip":
        await state.update_data(post_type=TYPE_TRIP)
        await state.set_state(CreatePost.from_country)

        await callback.message.answer(
            MENU_TEXTS["trip"],
            reply_markup=main_menu(callback.from_user.id)
        )
        await callback.message.answer(
            form_text(TYPE_TRIP, 1, "Выберите страну отправления"),
            reply_markup=countries_select_kb("from_country_pick", include_back=False)
        )
        await callback.answer()
        return

    if action == "parcel":
        await state.update_data(post_type=TYPE_PARCEL)
        await state.set_state(CreatePost.from_country)

        await callback.message.answer(
            MENU_TEXTS["parcel"],
            reply_markup=main_menu(callback.from_user.id)
        )
        await callback.message.answer(
            form_text(TYPE_PARCEL, 1, "Выберите страну отправления"),
            reply_markup=countries_select_kb("from_country_pick", include_back=False)
        )
        await callback.answer()
        return

    if action == "browse":
        rows = await get_recent_posts(10)
        if not rows:
            await callback.message.answer(
                "Пока нет новых активных объявлений.",
                reply_markup=main_menu(callback.from_user.id)
            )
        else:
            await callback.message.answer(
                "🆕 Последние объявления:",
                reply_markup=main_menu(callback.from_user.id)
            )
            for row in rows:
                await send_post_card(callback.message, row, with_age=True)

        await callback.answer()
        return

    await callback.answer("Неизвестное действие", show_alert=True)
        

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
      

@router.message(F.text == "ℹ️ Помощь")
async def help_handler(message: Message):
    text = (
        "<b>Помощь</b>\n\n"
        "✈️ <b>Взять посылку</b> — если вы летите и можете что-то передать.\n"
        "📦 <b>Отправить посылку</b> — если вам нужно что-то передать.\n"
        "🚀 <b>Быстрая доставка (карго)</b> — для срочных, дорогих либо больших коммерческих грузов.\n"
        "🔎 <b>Найти совпадения</b> — быстрый поиск подходящих объявлений.\n"
        "📋 <b>Мои объявления</b> — управление своими объявлениями.\n"
        "🤝 <b>Мои сделки</b> — ваши активные и завершенные сделки.\n"
        "🔥 <b>Популярные маршруты</b> — самые активные направления сервиса.\n"
        "💰 <b>Поднять объявление</b> — получайте больше просмотров и откликов.\n"
        "🛂 <b>Верификация</b> — повышает доверие к вашему профилю через подтверждение личности .\n"
        "🔔 <b>Подписки</b> — уведомления по нужным маршрутам.\n"
        "🚩 <b>Пожаловаться</b> — сообщить о проблеме с объявлением или пользователем.\n\n"
        "<b>🔐 Безопасность</b>\n\n"
        "Перед сделкой рекомендуем:\n"
        "• обменяться WeChat\n"
        "• проверить историю аккаунта\n"
        "• убедиться, что человек реально связан с Китаем\n"
        "• не переводить предоплату незнакомым людям\n\n"
        "<b>Никогда не делайте предоплату незнакомому человеку.</b>"
    )
    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.message(Command("user"))
async def admin_user_command_handler(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Нет доступа.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        await message.answer("Использование: /user 123456789")
        return

    target_user_id = int(parts[1].strip())
    user_row = await get_user_row(target_user_id)
    
    if not user_row:
        await message.answer("Пользователь не найден.")
        return

    await message.answer(
        await render_user_admin_card(user_row),
        reply_markup=admin_user_moderation_kb(
            target_user_id=target_user_id,
            is_banned=user_row["is_banned"]
        )
    )


@router.message(F.text == "🛂 Верификация аккаунта")
async def verification_menu_handler(message: Message):
    await upsert_user(message)

    if await is_user_verified(message.from_user.id):
        await message.answer(
            "✅  <b>Ваш аккаунт уже верифицирован.</b>\n\n"
            "Статус: 🛂 Паспорт подтвержден",
            reply_markup=main_menu(message.from_user.id)
        )
        return

    req = await get_latest_verification_request(message.from_user.id)

    extra = ""
    if req:
        extra = f"\n\n<b>Текущий статус:</b> {format_verification_status(req['status'])}"
        if req["status"] == VERIF_STATUS_REJECTED and req["rejection_reason"]:
            extra += f"\n<b>Причина:</b> {html.escape(req['rejection_reason'])}"

    text = (
    "🛂 <b>Верификация аккаунта</b>\n\n"

    "<b>Что такое верификация?</b>\n"
    "Это подтверждение личности пользователя с помощью паспорта.\n"
    "Вы загружаете фото первой страницы паспорта и селфи с паспортом в руках, "
    "после чего администратор проверяет данные.\n\n"
    "• верификация подтверждает, что пользователь загрузил паспорт,\n"
    "который был проверен администратором.\n\n"
    "Она не является гарантией выполнения сделки.\n\n"

    "<b>Что вы получите:</b>\n" 
    "• ✅ бейдж проверенного пользователя\n"
    "• 📈 больше доверия к вашим объявлениям\n"
    "• 🔝 приоритет ваших объявлений в поиске и совпадениях\n"
    "• 🤝 <b>неограниченное количество активных сделок</b>\n"
    "  (у обычных пользователей лимит 2)\n"
    "• 📢 <b>неограниченное количество активных объявлений</b>\n"
    "  (у обычных пользователей лимит 5)\n"
    "• 🔒 больше безопасности для вас и других пользователей\n\n"

    "<b>Как проходит проверка:</b>\n"
    "1️⃣ Оплатите верификацию\n"
    "2️⃣ Загрузите фото первой страницы паспорта\n"
    "3️⃣ Сделайте селфи с паспортом в руках\n"
    "4️⃣ Администратор проверит данные\n"
    "5️⃣ После проверки вы получите статус проверенного пользователя\n\n"

    "<b>Стоимость:</b> 50 CNY\n\n"

    "⚠️ <b>Важно</b>\n"
    "Оплата производится <b>до проверки</b>.\n"
    "Если документы не прошли — деньги <b>не возвращаются</b>, "
    "но документы можно отправить повторно без дополнительной оплаты.\n\n"

    "🔐 <b>Конфиденциальность</b>\n"
    "Фото документов используются только для проверки личности "
    "и после финального решения сразу же очищаются из базы."
)

    await message.answer(
        text,
        reply_markup=verification_info_kb(False)
    )


@router.callback_query(F.data == "verify:start")
async def verify_start_handler(callback: CallbackQuery):
    if await is_user_verified(callback.from_user.id):
        await callback.answer("Ваш аккаунт уже верифицирован.", show_alert=True)
        return

    req = await get_latest_verification_request(callback.from_user.id)

    if req and req["status"] in (
        VERIF_STATUS_AWAITING_PAYMENT,
        VERIF_STATUS_PAYMENT_REVIEW,
        VERIF_STATUS_DOCS_PENDING,
        VERIF_STATUS_SELFIE_PENDING,
        VERIF_STATUS_REVIEW_PENDING,
    ):
        request_id = req["id"]
    else:
        request_id = await create_verification_request(callback.from_user.id)

    await callback.message.answer(
    f"💳 <b>Оплата верификации</b>\n\n"
    f"<b>Заявка:</b> #{request_id}\n"
    f"<b>Стоимость:</b> {VERIFICATION_PRICE_AMOUNT} {VERIFICATION_PRICE_CURRENCY}\n\n"
    f"Для совершения оплаты напишите администратору в WeChat:\n\n"
    f"👤 <b>WeChat:</b> tikovan\n\n"
    f"Напишите администратору:\n"
    f"<code>Оплата верификации #{request_id}</code>\n\n"
    f"После оплаты нажмите кнопку <b>«✅ Я оплатил»</b> ниже.",
    reply_markup=verification_pay_kb(request_id)
 )
    await callback.answer()


@router.callback_query(F.data.startswith("verify:paid:"))
async def verify_paid_handler(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    req = await get_verification_request(request_id)

    if not req or req["user_id"] != callback.from_user.id:
        await callback.answer("Заявка не найдена", show_alert=True)
        return

    if req["status"] not in (VERIF_STATUS_AWAITING_PAYMENT, VERIF_STATUS_PAYMENT_REJECTED):
        await callback.answer("Этот этап уже пройден.", show_alert=True)
        return

    await set_verification_status(
        request_id,
        VERIF_STATUS_PAYMENT_REVIEW,
        rejection_reason=None
    )

    for admin_id in ADMIN_IDS:
        try:
            await callback.bot.send_message(
                admin_id,
                f"💳 <b>Новая оплата верификации</b>\n\n"
                f"<b>Заявка:</b> {request_id}\n"
                f"<b>USER_ID:</b> {req['user_id']}\n"
                f"<b>Сумма:</b> {req['payment_amount']} {req['payment_currency']}",
                reply_markup=admin_verification_payment_kb(request_id, req["user_id"])
            )
        except Exception as e:
            logger.exception("VERIF PAYMENT ADMIN NOTIFY ERROR: %s", e)

    await callback.message.answer(
        "✅ Заявка на оплату отправлена администратору.\n"
        "После подтверждения оплаты вы сможете загрузить паспорт."
    )
    await callback.answer()


@router.callback_query(F.data.startswith("verify:upload_passport:"))
async def verify_upload_passport_handler(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    req = await get_verification_request(request_id)

    if not req or req["user_id"] != callback.from_user.id:
        await callback.answer("Заявка не найдена", show_alert=True)
        return

    if req["status"] not in (VERIF_STATUS_DOCS_PENDING, VERIF_STATUS_REJECTED):
        await callback.answer("Сейчас нельзя загрузить паспорт.", show_alert=True)
        return

    await state.clear()
    await state.update_data(verification_request_id=request_id)
    await state.set_state(VerificationFlow.passport_photo)

    await callback.message.answer(
        "📷 <b>Шаг 1/2</b>\n\n"
        "Отправьте фото паспорта / загранпаспорта / ID.\n\n"
        "Требования:\n"
        "• документ полностью в кадре\n"
        "• текст читаемый\n"
        "• без сильных бликов"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("verify:retry:"))
async def verify_retry_handler(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    req = await get_verification_request(request_id)

    if not req or req["user_id"] != callback.from_user.id:
        await callback.answer("Заявка не найдена", show_alert=True)
        return

    await set_verification_status(
        request_id,
        VERIF_STATUS_DOCS_PENDING,
        rejection_reason=None
    )
    await clear_verification_files(request_id)

    await state.clear()
    await state.update_data(verification_request_id=request_id)
    await state.set_state(VerificationFlow.passport_photo)

    await callback.message.answer(
        "📷 <b>Повторная отправка</b>\n\n"
        "Сначала отправьте фото паспорта заново."
    )
    await callback.answer()


@router.message(VerificationFlow.passport_photo, F.photo)
async def verification_passport_input(message: Message, state: FSMContext):
    data = await state.get_data()
    request_id = data.get("verification_request_id")
    req = await get_verification_request(request_id) if request_id else None

    if not req or req["user_id"] != message.from_user.id:
        await message.answer("Заявка не найдена.")
        await state.clear()
        return

    photo_id = message.photo[-1].file_id
    await save_verification_passport(request_id, photo_id)

    await state.set_state(VerificationFlow.selfie_photo)
    await message.answer(
        "📷 <b>Шаг 2/2</b>\n\n"
        "Теперь отправьте селфи, где вы держите документ рядом с лицом."
    )


@router.message(VerificationFlow.passport_photo)
async def verification_passport_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте именно фото паспорта.")


@router.message(VerificationFlow.selfie_photo, F.photo)
async def verification_selfie_input(message: Message, state: FSMContext):
    data = await state.get_data()
    request_id = data.get("verification_request_id")
    req = await get_verification_request(request_id) if request_id else None

    if not req or req["user_id"] != message.from_user.id:
        await message.answer("Заявка не найдена.")
        await state.clear()
        return

    photo_id = message.photo[-1].file_id
    await save_verification_selfie(request_id, photo_id)
    req = await get_verification_request(request_id)

    for admin_id in ADMIN_IDS:
        try:
            await message.bot.send_message(
                admin_id,
                f"🛡 <b>Новая заявка на проверку документов</b>\n\n"
                f"<b>Заявка:</b> {request_id}\n"
                f"<b>USER_ID:</b> {req['user_id']}\n"
                f"<b>Статус:</b> {format_verification_status(req['status'])}"
            )
            if req["passport_photo_file_id"]:
                await message.bot.send_photo(admin_id, req["passport_photo_file_id"], caption=f"Паспорт • заявка {request_id}")
            if req["selfie_photo_file_id"]:
                await message.bot.send_photo(
                    admin_id,
                    req["selfie_photo_file_id"],
                    caption=f"Селфи с документом • заявка {request_id}",
                    reply_markup=admin_verification_review_kb(request_id, req["user_id"])
                )
        except Exception as e:
            logger.exception("VERIF REVIEW ADMIN NOTIFY ERROR: %s", e)

    await message.answer(
        "✅ Документы отправлены на проверку.\n"
        "Мы уведомим вас после решения администратора."
    )
    await state.clear()


@router.message(VerificationFlow.selfie_photo)
async def verification_selfie_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте именно селфи с документом.")

@router.message(Command("admin_verify"))
async def admin_verify_user_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_verify USER_ID")
        return

    user_id = int(parts[1])
    await verify_user(user_id)
    await message.answer(f"✅ Пользователь {user_id} верифицирован.")


@router.message(Command("admin_unverify"))
async def admin_unverify_user_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_unverify USER_ID")
        return

    user_id = int(parts[1])
    await unverify_user(user_id)
    await message.answer(f"↩️ Верификация пользователя {user_id} снята.")


@router.message(Command("admin_ban"))
async def admin_ban_user_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_ban USER_ID")
        return

    user_id = int(parts[1])
    await ban_user_with_cleanup(message.bot, user_id)
    await message.answer(f"⛔ Пользователь {user_id} забанен.")
    

@router.callback_query(F.data == "create_back")
async def create_back_handler(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    current_state = await state.get_state()
    step_name = get_current_create_step_name(current_state)

    if not step_name:
        return

    idx = STEP_ORDER.index(step_name)
    if idx == 0:
        return

    prev_step = STEP_ORDER[idx - 1]
    await clear_step_data_from(state, prev_step)
    await render_create_step(prev_step, callback, state)
    

@router.callback_query(F.data.startswith("support:"))
async def support_router(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":")[1]

    if action == "complaint":
        await state.clear()
        await state.set_state(ComplaintFlow.post_id)
        await callback.message.answer(
            "🚩 <b>Жалоба на объявление</b>\n\n"
            "Введите <b>ID объявления</b>, на которое хотите пожаловаться.\n\n"
            "ID указан внизу каждого объявления."
        )
        await callback.answer()
        return

    if action == "bug":
        await state.clear()
        await state.set_state(SupportFlow.bug_text)
        await callback.message.answer(
            "🐞 <b>Сообщение о баге</b>\n\n"
            "Опишите проблему:\n"
            "• что вы нажали\n"
            "• что должно было произойти\n"
            "• что произошло на самом деле\n\n"
            "Можно одним сообщением."
        )
        await callback.answer()
        return

    if action == "help":
        await state.clear()
        await state.set_state(SupportFlow.help_text)
        await callback.message.answer(
            "🆘 <b>Связь с поддержкой</b>\n\n"
            "Напишите ваш вопрос или проблему одним сообщением."
        )
        await callback.answer()
        return

    await callback.answer("Неизвестное действие", show_alert=True)


@router.message(SupportFlow.bug_text)
async def support_bug_input(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите баг чуть подробнее.")
        return

    username = f"@{message.from_user.username}" if message.from_user.username else "без username"

    admin_text = (
        "🐞 <b>Новый баг-репорт</b>\n\n"
        f"<b>Пользователь:</b> {username}\n"
        f"<b>ID:</b> {message.from_user.id}\n"
        f"<b>Имя:</b> {html.escape(message.from_user.full_name or 'Без имени')}\n\n"
        f"<b>Описание:</b>\n{html.escape(text[:2000])}"
    )

    for admin_id in ADMIN_IDS:
        try:
            await message.bot.send_message(admin_id, admin_text)
        except Exception as e:
            logger.exception("BUG REPORT SEND ERROR: %s", e)

    await message.answer(
        "✅ Сообщение о баге отправлено.",
        reply_markup=main_menu(message.from_user.id)
    )
    await state.clear()


@router.message(StateFilter(None))
async def forward_hold_user_messages_to_admin(message: Message, state: FSMContext):

    current_state = await state.get_state()
    if current_state is not None:
        return

    user_id = message.from_user.id

    if is_admin(user_id):
        return

    conn = await connect_db()
    try:
        cur = await conn.execute("""
            SELECT review_status, username, full_name
            FROM users
            WHERE user_id=?
        """, (user_id,))
        row = await cur.fetchone()
    finally:
        await conn.close()

    if not row or row["review_status"] != "hold":
        return

    admin_text = (
        "⚠️ <b>Сообщение от пользователя на проверке</b>\n\n"
        f"<b>USER_ID:</b> {user_id}\n"
        f"<b>Username:</b> @{html.escape(row['username']) if row['username'] else 'нет'}\n"
        f"<b>Имя:</b> {html.escape(row['full_name'] or 'не указано')}\n\n"
        "Ниже переслано его сообщение."
    )

    for admin_id in ADMIN_IDS:
        try:
            await message.bot.send_message(admin_id, admin_text)
            await message.forward(admin_id)
        except Exception as e:
            logger.warning("Не удалось переслать HOLD сообщение админу %s: %s", admin_id, e)

    await message.answer(
        "✅ Данные отправлены администратору. После проверки доступ может быть восстановлен."
    )


@router.message(SupportFlow.help_text)
async def support_help_input(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите вопрос чуть подробнее.")
        return

    username = f"@{message.from_user.username}" if message.from_user.username else "без username"

    admin_text = (
        "🆘 <b>Новое обращение в поддержку</b>\n\n"
        f"<b>Пользователь:</b> {username}\n"
        f"<b>ID:</b> {message.from_user.id}\n"
        f"<b>Имя:</b> {html.escape(message.from_user.full_name or 'Без имени')}\n\n"
        f"<b>Сообщение:</b>\n{html.escape(text[:2000])}"
    )

    for admin_id in ADMIN_IDS:
        try:
            await message.bot.send_message(admin_id, admin_text)
        except Exception as e:
            logger.exception("SUPPORT SEND ERROR: %s", e)

    await message.answer(
        "✅ Ваше сообщение отправлено в поддержку.",
        reply_markup=main_menu(message.from_user.id)
    )
    await state.clear()
    

@router.callback_query(F.data.startswith("from_country_pick:"))
async def pick_from_country(callback: CallbackQuery, state: FSMContext):
    await upsert_user(callback)

    value = callback.data.split(":", 1)[1]

    if value == "__manual__":
        data = await state.get_data()
        post_type = data.get("post_type", TYPE_PARCEL)

        await state.set_state(CreatePost.from_country_manual)
        await callback.message.answer(
            form_text(post_type, 1, "Введите страну отправления вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(from_country=value)
    await render_create_step("from_city", callback, state)
    

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

    if value == "__manual__":
        data = await state.get_data()
        post_type = data.get("post_type", TYPE_PARCEL)

        await state.set_state(CreatePost.from_city_manual)
        await callback.message.answer(
            form_text(post_type, 2, "Введите город отправления вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(from_city=None if value == "__skip__" else value)
    await render_create_step("to_country", callback, state)


@router.callback_query(F.data.startswith("back:"))
async def back_router(callback: CallbackQuery):
    action = callback.data.split(":")[1]

    if action == "my_posts":
        conn = await connect_db()
        try:
            cur = await conn.execute("""
                SELECT * FROM posts
                WHERE user_id=? AND status != 'deleted'
                ORDER BY created_at DESC
                LIMIT 30
            """, (callback.from_user.id,))
            posts = await cur.fetchall()
        finally:
            await conn.close()

        if not posts:
            await callback.message.answer("У вас пока нет объявлений.")
        else:
            await callback.message.answer(
                "📋 Ваши объявления:",
                reply_markup=my_posts_kb(posts)
            )

    elif action == "my_deals":
        await show_user_deals_sections(callback.message, callback.from_user.id)

    elif action == "new_posts":
        await callback.message.answer("🆕 Новые объявления:")
        await render_recent_posts_page(callback.message, 0)

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


@router.callback_query(F.data.startswith("from_city_pick:"))
async def pick_from_city(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":", 1)[1]

    if value == "__manual__":
        data = await state.get_data()
        post_type = data.get("post_type", TYPE_PARCEL)

        await state.set_state(CreatePost.from_city_manual)
        await callback.message.answer(
            form_text(post_type, 2, "Введите город отправления вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(from_city=None if value == "__skip__" else value)
    await render_create_step("to_country", callback, state)


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

    if value == "__manual__":
        data = await state.get_data()
        post_type = data.get("post_type", TYPE_PARCEL)

        await state.set_state(CreatePost.to_city_manual)
        await callback.message.answer(
            form_text(post_type, 4, "Введите город назначения вручную"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(to_city=None if value == "__skip__" else value)
    await render_create_step("delivery_date", callback, state)
    

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

    if value == "manual":
        data = await state.get_data()
        post_type = data.get("post_type", TYPE_PARCEL)

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
    await render_create_step("weight", callback, state)


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

    if value == "__manual__":
        data = await state.get_data()
        post_type = data.get("post_type", TYPE_PARCEL)

        await state.set_state(CreatePost.weight_manual)
        await callback.message.answer(
            form_text(post_type, 6, "Введите вес / объём\nНапример: 7 кг"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    await state.update_data(weight_kg=value)
    await render_create_step("description", callback, state)


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
    desc = (message.text or "").strip()

    if len(desc) < 3:
        await message.answer(
            "Описание слишком короткое. Напишите подробнее.",
            reply_markup=back_only_kb()
        )
        return

    data = await state.get_data()
    post_type = data.get("post_type", TYPE_PARCEL)

    await state.update_data(description=desc[:1000])

    await state.set_state(CreatePost.photo_choice)
    await message.answer(
        form_text(post_type, 8, "Хотите добавить фото посылки? Это необязательно."),
        reply_markup=photo_choice_kb()
    )
    

@router.callback_query(F.data.startswith("photo_choice:"))
async def photo_choice_handler(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":", 1)[1]

    if action == "add":
        data = await state.get_data()
        post_type = data.get("post_type", TYPE_PARCEL)

        await state.set_state(CreatePost.photo_upload)
        await callback.message.answer(
            form_text(post_type, 8, "Отправьте 1 фото посылки"),
            reply_markup=back_only_kb()
        )
        await callback.answer()
        return

    if action == "skip":
        await state.update_data(photo_file_id=None)
        await render_create_step("contact", callback, state)
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
        await upsert_user(message)

        if await is_user_banned(message.from_user.id):
            await message.answer(
                "⛔ Ваш аккаунт ограничен.",
                reply_markup=main_menu(message.from_user.id)
            )
            await state.clear()
            return

        data = await state.get_data()
        data["contact_note"] = None if message.text.strip() == "-" else message.text.strip()[:200]

        post_id = await create_post_record(data, message.from_user.id)
        row = await get_post(post_id)

        await state.clear()

        if not row:
            await message.answer(
                "Ошибка: объявление создалось некорректно. Попробуйте ещё раз.",
                reply_markup=main_menu(message.from_user.id)
            )
            return

        await message.answer(
            "✅ Объявление создано.\n" +
            ("Оно отправлено на модерацию." if MODERATION_ENABLED else "Оно уже активно."),
            reply_markup=main_menu(message.from_user.id)
        )

        # ⬇️ ЕДИНЫЙ РЕНДЕР КАРТОЧКИ
        await send_post_card(
            message,
            row,
            reply_markup=post_actions_kb(post_id, row["status"])
        )

        if MODERATION_ENABLED and row["status"] == STATUS_PENDING:
            for admin_id in ADMIN_IDS:
                try:
                    await send_post_card_to_user(
                        bot,
                        admin_id,
                        row,
                        prefix_text="Новое объявление на модерации:",
                        reply_markup=admin_post_actions_kb(post_id)
                    )
                except Exception as e:
                    logger.exception("ADMIN NOTIFY ERROR: %s", e)
        else:
            await publish_to_channel(bot, post_id)
            await notify_coincidence_users(bot, post_id)
            await notify_subscribers(bot, post_id)

    except Exception as e:
        logger.exception("FINALIZE_POST ERROR: %s", e)
        await message.answer(
            f"Произошла ошибка при сохранении объявления: {html.escape(str(e))}",
            reply_markup=main_menu(message.from_user.id)
        )
        await state.clear()


@router.message(Command("my"))
@router.message(F.text == "📋 Мои объявления")
async def my_posts_handler(message: Message):
    await upsert_user(message)
    await message.answer(MENU_TEXTS["my_posts"], reply_markup=main_menu(message.from_user.id))
    await render_my_posts_page(message, message.from_user.id, 0)


@router.callback_query(F.data.startswith("deal_review:"))
async def deal_review_start(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split(":")[1])
    deal = await get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if callback.from_user.id not in (deal["owner_user_id"], deal["requester_user_id"]):
        await callback.answer("Нет доступа", show_alert=True)
        return

    if deal["status"] not in (DEAL_COMPLETED, DEAL_FAILED):
        await callback.answer("Отзыв можно оставить только по завершенной или неуспешной сделке", show_alert=True)
        return

    if await has_user_left_review_for_deal(deal, callback.from_user.id):
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
        conn = await connect_db()
        try:
            await conn.execute("""
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
            await conn.commit()
        finally:
            await conn.close()

        invalidate_user_profile_cache(reviewed_user_id)

        await message.answer("✅ Отзыв сохранен.", reply_markup=main_menu(message.from_user.id))

    except aiosqlite.IntegrityError:
        await message.answer("Вы уже оставили отзыв по этой сделке.", reply_markup=main_menu(message.from_user.id))

    await state.clear()
    

@router.callback_query(F.data.startswith("mypost:"))
async def open_my_post(callback: CallbackQuery):
    try:
        post_id = int(callback.data.split(":")[1])
        row = await get_post(post_id)

        if not row:
            await callback.answer("Объявление не найдено", show_alert=True)
            return

        if row["user_id"] != callback.from_user.id:
            await callback.answer("Нет доступа", show_alert=True)
            return

        if row["status"] == STATUS_DELETED:
            await callback.answer("Объявление уже удалено", show_alert=True)
            return

        await send_post_card(
            callback.message,
            row,
            reply_markup=post_actions_kb(post_id, row["status"])
        )

        await callback.answer()

    except Exception as e:
        logger.exception("OPEN_MY_POST ERROR: %s", e)
        await callback.answer("Не удалось открыть объявление", show_alert=True)
        

@router.callback_query(F.data.startswith("deal_confirm:"))
async def deal_confirm_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])

    conn = await connect_db()
    try:
        cur = await conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,))
        deal = await cur.fetchone()

        if not deal:
            await callback.answer("Сделка не найдена", show_alert=True)
            return

        user_id = callback.from_user.id

        if user_id not in (deal["owner_user_id"], deal["requester_user_id"]):
            await callback.answer("Нет доступа", show_alert=True)
            return

        if not can_confirm_deal_now(deal):
            await callback.answer(
                f"Подтвердить завершение можно через {time_left_until_deal_confirm(deal)}",
                show_alert=True
            )
            return

        owner_confirmed = int(deal["owner_confirmed"] or 0)
        requester_confirmed = int(deal["requester_confirmed"] or 0)

        # уже подтверждено
        if user_id == deal["owner_user_id"] and owner_confirmed == 1:
            cur = await conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,))
            fresh_deal = await cur.fetchone()

            await callback.message.edit_reply_markup(
                reply_markup=await deal_open_kb(fresh_deal, user_id)
            )

            await callback.answer("Вы уже подтвердили завершение")
            return

        if user_id == deal["requester_user_id"] and requester_confirmed == 1:
            cur = await conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,))
            fresh_deal = await cur.fetchone()

            await callback.message.edit_reply_markup(
                reply_markup=await deal_open_kb(fresh_deal, user_id)
            )

            await callback.answer("Вы уже подтвердили завершение")
            return

        # ставим подтверждение
        if user_id == deal["owner_user_id"]:
            owner_confirmed = 1
        else:
            requester_confirmed = 1

        both_confirmed = owner_confirmed == 1 and requester_confirmed == 1

        if both_confirmed:
            new_status = DEAL_COMPLETED
            completed_at = now_ts()
        elif user_id == deal["owner_user_id"]:
            new_status = DEAL_COMPLETED_BY_OWNER
            completed_at = deal["completed_at"]
        else:
            new_status = DEAL_COMPLETED_BY_REQUESTER
            completed_at = deal["completed_at"]

        await conn.execute("""
            UPDATE deals
            SET owner_confirmed=?,
                requester_confirmed=?,
                status=?,
                updated_at=?,
                completed_at=?
            WHERE id=?
        """, (
            owner_confirmed,
            requester_confirmed,
            new_status,
            now_ts(),
            completed_at,
            deal_id
        ))

        await conn.commit()

        cur = await conn.execute("SELECT * FROM deals WHERE id=?", (deal_id,))
        fresh_deal = await cur.fetchone()

    finally:
        await conn.close()

    # обновляем кнопки
    try:
        await callback.message.edit_reply_markup(
            reply_markup=await deal_open_kb(fresh_deal, callback.from_user.id)
        )
    except Exception as e:
        logger.exception("DEAL CONFIRM EDIT MARKUP ERROR: %s", e)

    # обновляем текст
    try:
        route = await deal_title(fresh_deal)

        role = "владелец объявления" if callback.from_user.id == fresh_deal["owner_user_id"] else "откликнувшийся пользователь"

        text = (
            f"🤝 <b>{html.escape(route)}</b>\n\n"
            f"<b>ID сделки:</b> {fresh_deal['id']}\n"
            f"<b>ID объявления:</b> {fresh_deal['post_id']}\n"
            f"<b>Ваша роль:</b> {role}\n"
            f"<b>Статус:</b> {format_deal_status(fresh_deal['status'])}"
        )

        if int(fresh_deal["owner_confirmed"] or 0) == 1 and int(fresh_deal["requester_confirmed"] or 0) == 1:
            text += "\n\n✅ Сделка завершена обеими сторонами.\nТеперь можно оставить отзыв."
        else:
            text += "\n\n✅ Ваше подтверждение сохранено.\nЖдем подтверждение второй стороны."

        await callback.message.edit_text(
            text,
            reply_markup=await deal_open_kb(fresh_deal, callback.from_user.id)
        )

    except Exception as e:
        logger.exception("DEAL CONFIRM EDIT TEXT ERROR: %s", e)

    # уведомление второй стороне
    other_user_id = (
        fresh_deal["requester_user_id"]
        if callback.from_user.id == fresh_deal["owner_user_id"]
        else fresh_deal["owner_user_id"]
    )

    try:
        both_confirmed = int(fresh_deal["owner_confirmed"] or 0) == 1 and int(fresh_deal["requester_confirmed"] or 0) == 1

        if both_confirmed:
            await callback.bot.send_message(
                other_user_id,
                "✅ Сделка завершена обеими сторонами.\nТеперь можно оставить отзыв.",
                reply_markup=await deal_open_kb(fresh_deal, other_user_id)
            )
        else:
            await callback.bot.send_message(
                other_user_id,
                f"📦 Пользователь подтвердил завершение сделки #{deal_id}.\n"
                "Откройте 'Мои сделки', чтобы подтвердить завершение со своей стороны.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="🤝 Мои сделки",
                                callback_data="back:my_deals"
                            )
                        ]
                    ]
                )
            )
    except Exception as e:
        logger.exception("DEAL CONFIRM NOTIFY ERROR: %s", e)
        

@router.callback_query(F.data.startswith("delete:"))
async def delete_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])

    row = await owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await remove_post_from_channel(callback.bot, row)

    conn = await connect_db()
    try:
        await conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (STATUS_DELETED, now_ts(), post_id)
        )
        await conn.commit()
    finally:
        await conn.close()

    await callback.message.answer("🗑 Объявление удалено.")
    await callback.answer()
    

@router.callback_query(F.data.startswith("deactivate:"))
async def deactivate_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])

    row = await owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await remove_post_from_channel(callback.bot, row)

    conn = await connect_db()
    try:
        await conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (STATUS_INACTIVE, now_ts(), post_id)
        )
        await conn.commit()
    finally:
        await conn.close()

    await callback.message.answer(f"Объявление {post_id} деактивировано.")
    await callback.answer()
    

@router.callback_query(F.data.startswith("activate:"))
async def activate_post(callback: CallbackQuery, bot: Bot):
    post_id = int(callback.data.split(":")[1])

    row = await owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    if await is_user_banned(callback.from_user.id):
        await callback.answer("Ваш аккаунт ограничен", show_alert=True)
        return

    new_status = STATUS_PENDING if MODERATION_ENABLED else STATUS_ACTIVE
    expires_at = calculate_post_expires_at(now_ts(), row["travel_date"], POST_TTL_DAYS)

    conn = await connect_db()
    try:
        await conn.execute(
            "UPDATE posts SET status=?, updated_at=?, expires_at=? WHERE id=?",
            (new_status, now_ts(), expires_at, post_id)
        )
        await conn.commit()
    finally:
        await conn.close()

    await callback.message.answer(
        f"Объявление {post_id} " + (
            "отправлено на повторную модерацию." if MODERATION_ENABLED else "активировано."
        )
    )

    if not MODERATION_ENABLED:
        await publish_to_channel(bot, post_id)
        await notify_coincidence_users(bot, post_id)
        await notify_subscribers(bot, post_id)

    await callback.answer()
    

@router.callback_query(F.data.startswith("bump:"))
async def bump_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])

    row = await owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    if row["status"] != STATUS_ACTIVE:
        await callback.answer("Поднимать можно только активное объявление", show_alert=True)
        return

    order_id = await create_bump_order(
        callback.from_user.id,
        post_id
    )

    await callback.message.answer(
        (
            f"💰 <b>Поднятие объявления</b>\n\n"
            f"Стоимость: <b>{BUMP_PRICE_AMOUNT} {BUMP_PRICE_CURRENCY}</b>\n\n"
            "Оплатите через WeChat / Alipay и отправьте скрин администратору.\n"
            "После подтверждения оплаты объявление будет поднято выше.\n\n"
            f"🆔 ID заказа: <b>{order_id}</b>"
        ),
        parse_mode=ParseMode.HTML
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

    conn = await connect_db()
    try:
        cur = await conn.execute("SELECT * FROM bump_orders WHERE id=?", (order_id,))
        order = await cur.fetchone()

        if not order:
            await message.answer("Заказ не найден.")
            return

        if order["status"] == "paid":
            await message.answer("Этот заказ уже подтвержден.")
            return

        await conn.execute("""
            UPDATE bump_orders
            SET status='paid', paid_at=?
            WHERE id=?
        """, (now_ts(), order_id))

        await conn.execute("""
            UPDATE posts
            SET bumped_at=?, updated_at=?
            WHERE id=?
        """, (now_ts(), now_ts(), order["post_id"]))

        await conn.commit()

    finally:
        await conn.close()

    try:
        await message.bot.send_message(
            order["user_id"],
            f"✅ Оплата по заказу {order_id} подтверждена.\n"
            "Ваше объявление поднято выше в поиске."
        )
    except Exception as e:
        logger.exception("BUMP PAID USER NOTIFY ERROR: %s", e)

    await message.answer("Объявление поднято.")


@router.message(Command("find"))
@router.message(F.text == "🔎 Найти совпадения")
async def find_start(message: Message, state: FSMContext):
    await upsert_user(message)
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

    coincidences = await get_coincidences(
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

            await send_post_card(callback.message, row, prefix_text=intro)

    await callback.answer()


@router.callback_query(F.data.startswith("viewphoto:"))
async def view_photo_handler(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = await get_post(post_id)

    if not row or not row["photo_file_id"]:
        await callback.answer("Фото не найдено", show_alert=True)
        return

    await callback.message.answer_photo(
        photo=row["photo_file_id"],
        caption=f"Фото посылки для объявления ID {post_id}"
    )
    await callback.answer()

@router.callback_query(F.data.startswith("coincidences:"))
async def coincidences_for_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = await owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    coincidences = await get_coincidences(
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

            await send_post_card(callback.message, found_row, prefix_text=intro)

    await callback.answer()


@router.message(F.text == "🔥 Популярные маршруты")
async def popular_routes_handler(message: Message):
    await message.answer(MENU_TEXTS["popular"], reply_markup=main_menu(message.from_user.id))
    rows = await get_popular_routes(10)
    if not rows:
        await message.answer("Пока нет активных маршрутов.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer("🔥 Популярные маршруты сейчас:", reply_markup=popular_routes_kb(rows))


@router.callback_query(F.data.startswith("popular:"))
async def popular_route_open(callback: CallbackQuery):
    await callback.answer()
    try:
        _, from_country, to_country = callback.data.split(":", 2)
        rows = await search_route_posts_all(from_country, to_country, limit=20)

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
                await send_post_card(callback.message, row)
            except Exception as inner_e:
                print(f"POPULAR_ROUTE_SEND_ROW ERROR: {inner_e}")

    except Exception as e:
        logger.exception("POPULAR_ROUTE_OPEN ERROR: %s", e)
        await callback.message.answer("Не удалось открыть маршрут.")


@router.message(F.text == "🆕 Новые объявления")
async def recent_posts_handler(message: Message):
    rows = await get_recent_posts(10)
    if not rows:
        await message.answer("Пока нет новых активных объявлений.", reply_markup=main_menu(message.from_user.id))
        return
    await message.answer("🆕 Последние объявления:")
    for row in rows:
        await send_post_card(message, row, with_age=True)


@router.message(ComplaintFlow.post_id)
async def complaint_post_id_input(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    if not text.isdigit():
        await message.answer("Введите корректный ID объявления числом.")
        return

    post_id = int(text)
    row = await get_post(post_id)
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

    conn = await connect_db()
    try:
        cur = await conn.execute(
            "SELECT 1 FROM complaints WHERE post_id=? AND from_user_id=? LIMIT 1",
            (post_id, message.from_user.id)
        )
        existing = await cur.fetchone()

        if existing:
            await state.clear()
            await message.answer(
                "Вы уже отправляли жалобу на это объявление.",
                reply_markup=main_menu(message.from_user.id)
            )
            return

        await conn.execute(
            "INSERT INTO complaints (post_id, from_user_id, reason, created_at) VALUES (?, ?, ?, ?)",
            (post_id, message.from_user.id, reason[:1000], now_ts())
        )

        cur = await conn.execute(
            "SELECT COUNT(*) AS c FROM complaints WHERE post_id=?",
            (post_id,)
        )
        count_row = await cur.fetchone()
        complaints_count = int(count_row["c"] or 0)

        cur = await conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.id=?
        """, (post_id,))
        row = await cur.fetchone()

        auto_hidden = False
        if row and row["status"] == STATUS_ACTIVE and complaints_count >= AUTO_HIDE_COMPLAINTS_THRESHOLD:
            await conn.execute(
                "UPDATE posts SET status=?, updated_at=? WHERE id=?",
                (STATUS_INACTIVE, now_ts(), post_id)
            )
            auto_hidden = True

        await conn.commit()

    finally:
        await conn.close()

    await state.clear()

    if auto_hidden:
        try:
            await remove_post_from_channel(message.bot, row)
        except Exception as e:
            logger.exception("AUTO HIDE CHANNEL REMOVE ERROR: %s", e)

        try:
            await message.bot.send_message(
                row["user_id"],
                f"⚠️ Ваше объявление ID {post_id} временно скрыто автоматически, "
                f"так как набрало {complaints_count} жалобы.\n"
                "Если это ошибка — свяжитесь с администратором."
            )
        except Exception as e:
            logger.exception("AUTO HIDE OWNER NOTIFY ERROR: %s", e)

        await message.answer(
            "✅ Жалоба отправлена.\n"
            "Объявление автоматически скрыто и отправлено на проверку администратору.",
            reply_markup=main_menu(message.from_user.id)
        )
    else:
        await message.answer(
            "✅ Жалоба отправлена администратору.",
            reply_markup=main_menu(message.from_user.id)
        )

    for admin_id in ADMIN_IDS:
        try:
            admin_text = (
                f"🆘 Новая жалоба\n\n"
                f"Объявление ID: <b>{post_id}</b>\n"
                f"От пользователя: <b>{message.from_user.id}</b>\n"
                f"Всего жалоб по объявлению: <b>{complaints_count}</b>\n"
            )

            if auto_hidden:
                admin_text += "\n⚠️ <b>Объявление автоматически скрыто.</b>\n"

            admin_text += f"\nПричина:\n{html.escape(reason[:1000])}"

            await message.bot.send_message(admin_id, admin_text)
        except Exception as e:
            logger.exception("ADMIN COMPLAINT NOTIFY ERROR: %s", e)


@router.message(F.text == "📊 Статистика")
async def stats_handler(message: Message):
    await message.answer(MENU_TEXTS["stats"], reply_markup=main_menu(message.from_user.id))

    stats = await service_stats()
    top = await top_route()

    text = (
        "📊 <b>Статистика сервиса</b>\n\n"
        f"Пользователей: <b>{stats['users_count']}</b>\n"
        f"Активных объявлений: <b>{stats['active_posts']}</b>\n"
        f"✈️ Попутчиков: <b>{stats['active_trips']}</b>\n"
        f"📦 Посылок: <b>{stats['active_parcels']}</b>\n"
    )

    if top:
        text += (
            f"\nПопулярный маршрут:\n"
            f"<b>{top['from_country']} → {top['to_country']}</b> ({top['cnt']})"
        )

    await message.answer(text, reply_markup=main_menu(message.from_user.id))


@router.message(Command("admin"))
@router.message(F.text == "👨‍💼 Админка")
async def admin_menu_handler(message: Message):
    await upsert_user(message)

    if not is_admin(message.from_user.id):
        await message.answer("Нет доступа.", reply_markup=main_menu(message.from_user.id))
        return

    await message.answer(
        await admin_stats_text(),
        reply_markup=admin_menu_kb()
    )


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


@router.callback_query(F.data.startswith("subfrom:"))
async def sub_from(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    await state.update_data(from_country=country)
    await state.set_state(SubscriptionFlow.from_city)
    await callback.message.answer("Выберите город отправления:", reply_markup=subscription_cities_kb("subfromcity", country))
    await callback.answer()


@router.callback_query(F.data.startswith("subfromcity:"))
async def sub_from_city(callback: CallbackQuery, state: FSMContext):
    city = callback.data.split(":", 1)[1]
    await state.update_data(from_city=None if city == "__skip__" else city)
    await state.set_state(SubscriptionFlow.to_country)
    await callback.message.answer("Выберите страну назначения:", reply_markup=countries_kb("subto"))
    await callback.answer()


@router.callback_query(F.data.startswith("subto:"))
async def sub_to(callback: CallbackQuery, state: FSMContext):
    country = callback.data.split(":", 1)[1]
    await state.update_data(to_country=country)
    await state.set_state(SubscriptionFlow.to_city)
    await callback.message.answer("Выберите город назначения:", reply_markup=subscription_cities_kb("subtocity", country))
    await callback.answer()


@router.callback_query(F.data.startswith("subtocity:"))
async def sub_to_city(callback: CallbackQuery, state: FSMContext):
    city = callback.data.split(":", 1)[1]
    await state.update_data(to_city=None if city == "__skip__" else city)
    data = await state.get_data()
    await add_route_subscription(
        callback.from_user.id,
        data["post_type"],
        data["from_country"],
        data["to_country"],
        data.get("from_city"),
        data.get("to_city"),
    )
    await state.clear()
    await callback.message.answer(
        f"✅ Подписка сохранена: {data['from_country']} → {data['to_country']}\n"
        "Бот будет присылать новые подходящие объявления.",
        reply_markup=main_menu(callback.from_user.id)
    )
    await callback.answer()


@router.callback_query(F.data == "sub:list")
async def sub_list(callback: CallbackQuery):
    subs = await list_route_subscriptions(callback.from_user.id)
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
    reviews = await get_user_reviews(user_id, limit=10)

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
    ok = await delete_subscription(callback.from_user.id, sub_id)
    await callback.answer("Подписка удалена" if ok else "Не найдено", show_alert=True)


@router.callback_query(F.data.startswith("contact:"))
async def contact_owner(callback: CallbackQuery, state: FSMContext):
    _, post_id, owner_id = callback.data.split(":")
    post_id = int(post_id)
    owner_id = int(owner_id)

    if owner_id == callback.from_user.id:
        await callback.answer("Это ваше объявление", show_alert=True)
        return

    if await is_user_blocked(owner_id, callback.from_user.id) or await is_user_blocked(callback.from_user.id, owner_id):
        await callback.answer("Диалог недоступен", show_alert=True)
        return

    await state.set_state(ContactFlow.message_text)
    await state.update_data(
        post_id=post_id,
        target_user_id=owner_id,
        deal_id=None
    )

    await set_active_chat(
        user_id=callback.from_user.id,
        target_user_id=owner_id,
        post_id=post_id,
        deal_id=None
    )

    await callback.message.answer(
        "💬 <b>Чат с владельцем объявления открыт.</b>\n\n"
        "Просто напишите сообщение — я отправлю его через бота.\n"
        "Дальше вы можете продолжать переписку здесь без повторных нажатий.\n\n"
        "⚠️ Никогда не переводите предоплату незнакомым людям."
    )

    await callback.answer()
    

@router.callback_query(F.data == "admin:all_posts")
async def admin_all_posts_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    rows = await get_admin_posts(30)
    if not rows:
        await callback.message.answer("Объявлений пока нет.")
        await callback.answer()
        return

    await callback.message.answer(
        f"📚 Всего показано объявлений: {len(rows)}",
        reply_markup=admin_posts_kb(rows)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adminpost:"))
async def admin_open_post(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])
    row = await get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    await send_post_card(
    callback.message,
    row,
    reply_markup=admin_post_manage_kb(post_id, row["user_id"])
)
    await callback.answer()


@router.callback_query(F.data.startswith("admin_hide_post:"))
async def admin_hide_post_direct(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])
    row = await get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    await remove_post_from_channel(callback.bot, row)

    conn = await connect_db()
    try:
        await conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (STATUS_INACTIVE, now_ts(), post_id)
        )
        await conn.commit()
    finally:
        await conn.close()

    try:
        await callback.bot.send_message(
            row["user_id"],
            f"⚠️ Ваше объявление ID {post_id} скрыто администратором."
        )
    except Exception as e:
        logger.exception("ADMIN HIDE POST USER NOTIFY ERROR: %s", e)

    await callback.message.answer(f"❌ Объявление {post_id} скрыто.")
    await callback.answer()
    

@router.callback_query(F.data.startswith("admin_delete_post:"))
async def admin_delete_post_direct(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])
    row = await get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    await remove_post_from_channel(callback.bot, row)

    conn = await connect_db()
    try:
        await conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (STATUS_DELETED, now_ts(), post_id)
        )
        await conn.commit()
    finally:
        await conn.close()

    try:
        await callback.bot.send_message(
            row["user_id"],
            f"🗑 Ваше объявление ID {post_id} удалено администратором."
        )
    except Exception as e:
        logger.exception("ADMIN DELETE POST USER NOTIFY ERROR: %s", e)

    await callback.message.answer(f"🗑 Объявление {post_id} удалено.")
    await callback.answer()
    

@router.callback_query(F.data.startswith("admin_ban_user:"))
async def admin_ban_user_direct(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    await ban_user_with_cleanup(callback.bot, user_id)

    try:
        await callback.bot.send_message(
            user_id,
            "⛔ Ваш аккаунт ограничен администратором."
        )
    except Exception:
        pass

    await callback.message.answer(f"🚫 Пользователь {user_id} забанен.")
    await callback.answer()


@router.callback_query(F.data == "admin:user_lookup")
async def admin_user_lookup_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.clear()
    await state.set_state(AdminFlow.user_lookup)
    await callback.message.answer("Введите USER_ID, @username или имя пользователя:")
    await callback.answer()


@router.message(AdminFlow.user_lookup)
async def admin_user_lookup_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    query = (message.text or "").strip()

    if not query:
        await message.answer("Введите USER_ID, @username или имя пользователя.")
        return

    user = None

    # 1. По USER_ID
    if query.isdigit():
        user = await find_user_by_id(int(query))

    # 2. По @username
    elif query.startswith("@"):
        user = await find_user_by_username(query)

    # 3. По имени / username без @ / части имени
    else:
        users = await search_users(query, limit=5)

        if len(users) == 1:
            user = users[0]

        elif len(users) > 1:
            lines = ["🔎 Найдено несколько пользователей:\n"]

            for u in users:
                username = f"@{html.escape(u['username'])}" if u["username"] else "без username"
                full_name = html.escape(u["full_name"] or "без имени")

                lines.append(
                    f"👤 {full_name} — {username}\n"
                    f"ID: <code>{u['user_id']}</code>\n"
                )

            await message.answer("\n".join(lines))
            return

    if not user:
        await message.answer("❌ Пользователь не найден. Попробуйте USER_ID или @username.")
        return

    user_id = int(user["user_id"])

    profile = await get_user_profile_full(user_id)
    profile_user = profile["user"]

    if not profile_user:
        await message.answer("Пользователь не найден.")
        await state.clear()
        return

    text = await build_admin_user_profile_text(user_id)

    await message.answer(
        text,
        reply_markup=admin_user_actions_kb(
            user_id,
            bool(profile_user["is_verified"]),
            bool(profile_user["is_banned"]),
            bool(profile_user["is_cargo"])
        )
    )

    await state.clear()


@router.callback_query(F.data.startswith("admin_user:"))
async def admin_open_user_profile(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    profile = await get_user_profile_full(user_id)
    user = profile["user"]

    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return

    text = await build_admin_user_profile_text(user_id)

    await callback.message.answer(
        text,
        reply_markup=admin_user_actions_kb(
            user_id,
            bool(user["is_verified"]),
            bool(user["is_banned"]),
            bool(user["is_cargo"])
        )
    )
    await callback.answer()
    

@router.callback_query(F.data.startswith("admin_user_verify:"))
async def admin_user_verify_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    await verify_user(user_id)
    await callback.message.answer(f"✅ Пользователь {user_id} верифицирован.")
    await callback.answer()
    

@router.callback_query(F.data.startswith("admin_user_unverify:"))
async def admin_user_unverify_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    await unverify_user(user_id)
    await callback.message.answer(f"↩️ Верификация пользователя {user_id} снята.")
    await callback.answer()


@router.callback_query(F.data == "verify:info")
async def verification_info_from_button(callback: CallbackQuery):
    await callback.answer()

    await verification_menu_handler(callback.message)


@router.callback_query(F.data.startswith("admin_user_ban:"))
async def admin_user_ban_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    await ban_user_with_cleanup(callback.bot, user_id)
    await callback.message.answer(f"🚫 Пользователь {user_id} забанен.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_user_unban:"))
async def admin_user_unban_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    await unban_user(user_id)
    await callback.message.answer(f"♻️ Пользователь {user_id} разбанен.")
    await callback.answer()

@router.callback_query(F.data.startswith("admin_user_make_cargo:"))
async def admin_user_make_cargo(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    await set_user_cargo(user_id, True)

    try:
        await callback.bot.send_message(
            user_id,
            "🚀 <b>Вам открыт статус карго-партнера.</b>\n\n"
            "Теперь вы будете получать заявки на карго доставку от пользователей сервиса.\n\n"
            "Как это работает:\n"
            "1. Пользователь оставляет заявку на карго доставку.\n"
            "2. Вы получаете ЛИД без контактной информации.\n"
            "3. Если заявка вам подходит — нажмите кнопку «📩 Получить контакт».\n\n"
            "Контакт получают ТОЛЬКО ПЕРВЫЕ 3 компании, которые быстрее других нажмут кнопку.\n"
            "Остальным доступ к заявке закрывается.\n\n"
            "Сейчас доступ к контактам ЛИДА открыт бесплатно.\n"
            "В будущем получение контакта будет платным."
            
        )
    except Exception as e:
        logger.warning("Не удалось отправить уведомление карго %s: %s", user_id, e)

    await callback.message.answer(f"🚚 Пользователь {user_id} теперь карго-партнер.")
    await callback.answer()

@router.callback_query(F.data.startswith("admin_user_remove_cargo:"))
async def admin_user_remove_cargo(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    await set_user_cargo(user_id, False)

    await callback.message.answer(f"❌ Пользователь {user_id} больше не карго-партнер.")
    await callback.answer()    

@router.callback_query(F.data.startswith("admin_user_hold:"))
async def admin_user_hold_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    await hold_user_with_cleanup(callback.bot, user_id, callback.from_user.id)

    await callback.message.answer(f"⚠️ Пользователь {user_id} поставлен на проверку.")
    await callback.answer()

@router.callback_query(F.data.startswith("admin_user_unhold:"))
async def admin_user_unhold_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    await unhold_user(user_id)

    try:
        await callback.bot.send_message(
            user_id,
            "✅ Проверка снята. Ваш аккаунт снова доступен. Ваши обьявления были деактивированы. Для активации или создания нового обьявления перейдите в Мои Обьявления."
        )
    except Exception as e:
        logger.warning("Не удалось отправить сообщение о снятии HOLD пользователю %s: %s", user_id, e)

    await callback.message.answer(f"✅ Проверка пользователя {user_id} снята.")
    await callback.answer()

@router.callback_query(F.data == "admin:complaints")
async def admin_complaints_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    complaints = await get_recent_complaints(20)
    if not complaints:
        await callback.message.answer("Жалоб пока нет.")
        await callback.answer()
        return

    for c in complaints:
        text = (
            f"🆘 <b>Жалоба #{c['id']}</b>\n\n"
            f"<b>Объявление ID:</b> {c['post_id']}\n"
            f"<b>От пользователя:</b> {c['from_user_id']}\n"
            f"<b>Владелец объявления:</b> {c['post_owner_user_id']}\n"
            f"<b>Когда:</b> {format_age(c['created_at'])}\n\n"
            f"<b>Причина:</b>\n{html.escape(c['reason'])}"
        )
        await callback.message.answer(
    text,
    reply_markup=admin_complaint_actions_kb(
        c["id"],
        c["post_id"],
        c["post_owner_user_id"]
    )
)

    await callback.answer()


@router.callback_query(F.data.startswith("admincomplaint_openpost:"))
async def admin_complaint_open_post(callback: CallbackQuery):
    try:
        if not is_admin(callback.from_user.id):
            await callback.answer("Нет доступа", show_alert=True)
            return

        post_id = int(callback.data.split(":")[1])
        row = await get_post(post_id)

        if not row:
            await callback.answer("Объявление не найдено", show_alert=True)
            return

        await send_post_card(
            callback.message,
            row,
            reply_markup=admin_post_actions_kb(post_id)
        )

        await callback.answer()

    except Exception as e:
        logger.exception("ADMIN COMPLAINT OPEN POST ERROR: %s", e)
        await callback.answer("Ошибка при открытии объявления", show_alert=True)


@router.callback_query(F.data.startswith("admincomplaint_hidepost:"))
async def admin_complaint_hide_post(callback: CallbackQuery):
    try:
        if not is_admin(callback.from_user.id):
            await callback.answer("Нет доступа", show_alert=True)
            return

        post_id = int(callback.data.split(":")[1])
        row = await get_post(post_id)

        if not row:
            await callback.answer("Объявление не найдено", show_alert=True)
            return

        await remove_post_from_channel(callback.bot, row)

        conn = await connect_db()
        try:
            await conn.execute(
                "UPDATE posts SET status=?, updated_at=? WHERE id=?",
                (STATUS_INACTIVE, now_ts(), post_id)
            )
            await conn.commit()
        finally:
            await conn.close()

        try:
            await callback.bot.send_message(
                row["user_id"],
                f"⚠️ Ваше объявление ID {post_id} скрыто администратором."
            )
        except Exception as e:
            logger.exception("ADMIN COMPLAINT HIDE USER NOTIFY ERROR: %s", e)

        await callback.message.answer(f"❌ Объявление {post_id} скрыто.")
        await callback.answer()

    except Exception as e:
        logger.exception("ADMIN COMPLAINT HIDE POST ERROR: %s", e)
        await callback.answer("Ошибка при скрытии объявления", show_alert=True)


@router.callback_query(F.data.startswith("admincomplaint_banuser:"))
async def admin_complaint_ban_user(callback: CallbackQuery):
    try:
        if not is_admin(callback.from_user.id):
            await callback.answer("Нет доступа", show_alert=True)
            return

        raw_user_id = callback.data.split(":")[1]
        if not raw_user_id.isdigit():
            await callback.answer("Некорректный user_id", show_alert=True)
            return

        user_id = int(raw_user_id)
        await ban_user_with_cleanup(callback.bot, user_id)

        try:
            await callback.bot.send_message(
                user_id,
                "⛔ Ваш аккаунт ограничен администратором."
            )
        except Exception:
            pass

        await callback.message.answer(f"🚫 Пользователь {user_id} забанен.")
        await callback.answer()

    except Exception as e:
        logger.exception("ADMIN COMPLAINT BAN USER ERROR: %s", e)
        await callback.answer("Ошибка при бане пользователя", show_alert=True)


@router.callback_query(F.data.startswith("admincomplaint_done:"))
async def admin_complaint_done(callback: CallbackQuery):
    try:
        if not is_admin(callback.from_user.id):
            await callback.answer("Нет доступа", show_alert=True)
            return

        complaint_id = int(callback.data.split(":")[1])

        conn = await connect_db()
        try:
            await conn.execute(
                "DELETE FROM complaints WHERE id=?",
                (complaint_id,)
            )
            await conn.commit()
        finally:
            await conn.close()

        await callback.message.answer(f"✅ Жалоба #{complaint_id} обработана.")
        await callback.answer()

    except Exception as e:
        logger.exception("ADMIN COMPLAINT DONE ERROR: %s", e)
        await callback.answer("Ошибка при обработке жалобы", show_alert=True)


@router.callback_query(F.data == "admin:bump_orders")
async def admin_bump_orders_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    orders = await get_pending_bump_orders(20)
    if not orders:
        await callback.message.answer("Нет заявок на поднятие.")
        await callback.answer()
        return

    for order in orders:
        text = (
            f"💰 <b>Заявка на поднятие #{order['id']}</b>\n\n"
            f"<b>Пользователь:</b> {order['user_id']}\n"
            f"<b>Объявление:</b> {order['post_id']}\n"
            f"<b>Сумма:</b> {order['amount']} {order['currency']}\n"
            f"<b>Статус:</b> {order['status']}"
        )
        await callback.message.answer(
            text,
            reply_markup=admin_bump_orders_kb(order["id"], order["post_id"])
        )

    await callback.answer()


@router.callback_query(F.data.startswith("admin_bump_confirm:"))
async def admin_bump_confirm_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    order_id = int(callback.data.split(":")[1])

    conn = await connect_db()
    try:
        cur = await conn.execute(
            "SELECT * FROM bump_orders WHERE id=?",
            (order_id,)
        )
        order = await cur.fetchone()

        if not order:
            await callback.answer("Заказ не найден", show_alert=True)
            return

        if order["status"] == "paid":
            await callback.answer("Уже подтверждено", show_alert=True)
            return

        await conn.execute(
            "UPDATE bump_orders SET status='paid', paid_at=? WHERE id=?",
            (now_ts(), order_id)
        )

        await conn.execute(
            "UPDATE posts SET bumped_at=?, updated_at=? WHERE id=?",
            (now_ts(), now_ts(), order["post_id"])
        )

        await conn.commit()

    finally:
        await conn.close()

    try:
        await callback.bot.send_message(
            order["user_id"],
            f"✅ Оплата по заказу {order_id} подтверждена.\nВаше объявление поднято выше."
        )
    except Exception as e:
        logger.exception("BUMP CONFIRM USER NOTIFY ERROR: %s", e)

    await callback.message.answer(f"✅ Заказ {order_id} подтвержден.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_bump_reject:"))
async def admin_bump_reject_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    order_id = int(callback.data.split(":")[1])

    conn = await connect_db()
    try:
        cur = await conn.execute(
            "SELECT * FROM bump_orders WHERE id=?",
            (order_id,)
        )
        order = await cur.fetchone()

        if not order:
            await callback.answer("Заказ не найден", show_alert=True)
            return

        await conn.execute(
            "UPDATE bump_orders SET status='rejected' WHERE id=?",
            (order_id,)
        )
        await conn.commit()

    finally:
        await conn.close()

    try:
        await callback.bot.send_message(
            order["user_id"],
            f"❌ Заявка на поднятие {order_id} отклонена."
        )
    except Exception as e:
        logger.exception("BUMP REJECT USER NOTIFY ERROR: %s", e)

    await callback.message.answer(f"❌ Заказ {order_id} отклонен.")
    await callback.answer()


@router.callback_query(F.data == "admin:verifications")
async def admin_verifications_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    rows = await list_pending_verification_requests(20)
    await callback.message.answer(
        "🛂 <b>Заявки на верификацию</b>",
        reply_markup=admin_verification_list_kb(rows)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_verif_open:"))
async def admin_verif_open_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    request_id = int(callback.data.split(":")[1])
    req = await get_verification_request(request_id)
    if not req:
        await callback.answer("Заявка не найдена", show_alert=True)
        return

    text = (
        f"🛂 <b>Заявка на верификацию</b>\n\n"
        f"<b>ID заявки:</b> {req['id']}\n"
        f"<b>USER_ID:</b> {req['user_id']}\n"
        f"<b>Статус:</b> {format_verification_status(req['status'])}\n"
        f"<b>Стоимость:</b> {req['payment_amount']} {req['payment_currency']}"
    )
    if req["rejection_reason"]:
        text += f"\n<b>Причина отклонения:</b> {html.escape(req['rejection_reason'])}"

    await callback.message.answer(text)

    if req["passport_photo_file_id"]:
        await callback.bot.send_photo(callback.from_user.id, req["passport_photo_file_id"], caption="Паспорт")
    if req["selfie_photo_file_id"]:
        await callback.bot.send_photo(callback.from_user.id, req["selfie_photo_file_id"], caption="Селфи с документом")

    if req["status"] == VERIF_STATUS_PAYMENT_REVIEW:
        await callback.message.answer(
            "Проверка оплаты:",
            reply_markup=admin_verification_payment_kb(req["id"], req["user_id"])
        )
    elif req["status"] == VERIF_STATUS_REVIEW_PENDING:
        await callback.message.answer(
            "Проверка документов:",
            reply_markup=admin_verification_review_kb(req["id"], req["user_id"])
        )

    await callback.answer()


@router.callback_query(F.data.startswith("admin_verif_pay_ok:"))
async def admin_verif_pay_ok_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    request_id = int(callback.data.split(":")[1])
    req = await get_verification_request(request_id)
    if not req:
        await callback.answer("Заявка не найдена", show_alert=True)
        return

    await set_verification_status(
        request_id,
        VERIF_STATUS_DOCS_PENDING,
        admin_user_id=callback.from_user.id,
        mark_paid=True
    )

    try:
        await callback.bot.send_message(
            req["user_id"],
            "✅ Оплата подтверждена.\n\n"
            "Теперь загрузите фото паспорта.",
            reply_markup=verification_upload_passport_kb(request_id)
        )
    except Exception as e:
        logger.exception("VERIF PAY OK USER NOTIFY ERROR: %s", e)

    await callback.message.answer(f"✅ Оплата по заявке {request_id} подтверждена.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_verif_pay_no:"))
async def admin_verif_pay_no_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    request_id = int(callback.data.split(":")[1])
    req = await get_verification_request(request_id)
    if not req:
        await callback.answer("Заявка не найдена", show_alert=True)
        return

    await set_verification_status(
        request_id,
        VERIF_STATUS_PAYMENT_REJECTED,
        rejection_reason="Оплата не подтверждена",
        admin_user_id=callback.from_user.id
    )

    try:
        await callback.bot.send_message(
            req["user_id"],
            "❌ Оплата верификации не подтверждена.\n"
            "Проверьте оплату и отправьте заявку снова."
        )
    except Exception as e:
        logger.exception("VERIF PAY NO USER NOTIFY ERROR: %s", e)

    await callback.message.answer(f"❌ Оплата по заявке {request_id} отклонена.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_verif_ok:"))
async def admin_verif_ok_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    request_id = int(callback.data.split(":")[1])
    req = await get_verification_request(request_id)
    if not req:
        await callback.answer("Заявка не найдена", show_alert=True)
        return

    await verify_user(req["user_id"])
    await set_verification_status(
        request_id,
        VERIF_STATUS_APPROVED,
        admin_user_id=callback.from_user.id,
        mark_reviewed=True
    )
    await clear_verification_files(request_id)

    try:
        await callback.bot.send_message(
            req["user_id"],
            "🎉 <b>Верификация одобрена!</b>\n\n"
            "Ваш аккаунт теперь отмечен как 🛂 Паспорт подтвержден."
        )
    except Exception as e:
        logger.exception("VERIF APPROVED USER NOTIFY ERROR: %s", e)

    await callback.message.answer(f"✅ Заявка {request_id} одобрена.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_verif_no:"))
async def admin_verif_no_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    request_id = int(callback.data.split(":")[1])
    req = await get_verification_request(request_id)
    if not req:
        await callback.answer("Заявка не найдена", show_alert=True)
        return

    await set_verification_status(
        request_id,
        VERIF_STATUS_REJECTED,
        rejection_reason="Документы не прошли проверку. Отправьте более четкие фото.",
        admin_user_id=callback.from_user.id,
        mark_reviewed=True
    )
    await clear_verification_files(request_id)

    try:
        await callback.bot.send_message(
            req["user_id"],
            "❌ Документы не прошли проверку.\n\n"
            "Вы можете отправить документы повторно без повторной оплаты.",
            reply_markup=verification_retry_kb(request_id)
        )
    except Exception as e:
        logger.exception("VERIF REJECTED USER NOTIFY ERROR: %s", e)

    await callback.message.answer(f"❌ Заявка {request_id} отклонена.")
    await callback.answer()


@router.callback_query(F.data.startswith("reply_contact:"))
async def reply_contact_handler(callback: CallbackQuery, state: FSMContext):
    try:
        _, post_id, target_user_id, deal_id = callback.data.split(":")

        post_id = int(post_id)
        target_user_id = int(target_user_id)
        deal_id = None if deal_id == "0" else int(deal_id)

        if target_user_id == callback.from_user.id:
            await callback.answer("Нельзя ответить самому себе", show_alert=True)
            return

        if await is_user_blocked(target_user_id, callback.from_user.id) or await is_user_blocked(callback.from_user.id, target_user_id):
            await callback.answer("Диалог недоступен", show_alert=True)
            return

        await mark_chat_read(callback.from_user.id, target_user_id, post_id)

        await state.clear()
        await state.set_state(ContactFlow.message_text)
        await state.update_data(
            post_id=post_id,
            target_user_id=target_user_id,
            deal_id=deal_id
        )

        await set_active_chat(
            user_id=callback.from_user.id,
            target_user_id=target_user_id,
            post_id=post_id,
            deal_id=deal_id
        )

        await callback.message.answer(
            "💬 Чат открыт.\n"
            "Теперь просто пишите сообщения сюда — я буду пересылать их собеседнику."
        )
        await callback.answer()

    except Exception as e:
        logger.exception("REPLY_CONTACT_HANDLER ERROR: %s", e)
        await callback.answer("Ошибка ответа", show_alert=True)
        

@router.message(ContactFlow.message_text)
async def relay_message(message: Message, state: FSMContext):
    data = await state.get_data()

    target_user_id = data.get("target_user_id")
    post_id = data.get("post_id")
    deal_id = data.get("deal_id")

    text = (message.text or "").strip()

    if not target_user_id or not post_id:
        await message.answer(
            "Ошибка диалога. Откройте объявление заново и начните переписку снова.",
            reply_markup=main_menu(message.from_user.id)
        )
        await state.clear()
        await clear_active_chat(message.from_user.id)
        return

    if not text:
        await message.answer("Сообщение не должно быть пустым.")
        return

    can_send, error_msg = await can_send_chat_message(message.from_user.id)
    if not can_send:
        await message.answer(f"⏳ {error_msg}")
        return

    if len(text) > 2000:
        await message.answer("Сообщение слишком длинное (макс. 2000 символов).")
        return

    if target_user_id == message.from_user.id:
        await message.answer("Нельзя отправить сообщение самому себе.")
        return

    if await is_user_blocked(target_user_id, message.from_user.id) or await is_user_blocked(message.from_user.id, target_user_id):
        await message.answer("Диалог недоступен.")
        return

    try:
        from_name = html.escape(message.from_user.full_name or "Пользователь")
        username_part = f" (@{html.escape(message.from_user.username)})" if message.from_user.username else ""
        safe_text = html.escape(text)

        reply_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="💬 Ответить через бота",
                    callback_data=f"reply_contact:{post_id}:{message.from_user.id}:{deal_id or 0}"
                )
            ]
        ])

        await message.bot.send_message(
            target_user_id,
            f"💬 <b>Новое сообщение по объявлению ID {post_id}</b>\n\n"
            f"<b>От:</b> {from_name}{username_part}\n\n"
            f"{safe_text}",
            reply_markup=reply_kb
        )

        conn = await connect_db()
        try:
            await conn.execute("""
                INSERT INTO dialogs (post_id, owner_user_id, requester_user_id, created_at)
                VALUES (?, ?, ?, ?)
            """, (post_id, target_user_id, message.from_user.id, now_ts()))
            await conn.commit()
        finally:
            await conn.close()

        await set_active_chat(
            user_id=message.from_user.id,
            target_user_id=target_user_id,
            post_id=post_id,
            deal_id=deal_id
        )

        await set_active_chat(
            user_id=target_user_id,
            target_user_id=message.from_user.id,
            post_id=post_id,
            deal_id=deal_id
        )

        await message.answer("✅ Сообщение отправлено.")

    except Exception as e:
        logger.exception("RELAY MESSAGE ERROR: %s", e)
        await message.answer(
            "Не удалось отправить сообщение. Возможно, пользователь еще не запускал бота.",
            reply_markup=main_menu(message.from_user.id)
        )
        await state.clear()
        await clear_active_chat(message.from_user.id)


@router.message(F.reply_to_message)
async def reply_to_contact_message(message: Message, state: FSMContext):
    reply_text = message.reply_to_message.text or message.reply_to_message.caption or ""
    chat_ref = extract_chat_ref_from_message(reply_text)

    if not chat_ref:
        return

    post_id, target_user_id, deal_id = chat_ref

    if target_user_id == message.from_user.id:
        await message.answer("Нельзя ответить самому себе.")
        return

    await state.set_state(ContactFlow.message_text)
    await state.update_data(
        post_id=post_id,
        target_user_id=target_user_id,
        deal_id=deal_id
    )

    await relay_message(message, state)


@router.message(StateFilter(None))
async def active_chat_fallback(message: Message, state: FSMContext):
    text = (message.text or "").strip()

    if not text:
        return

    if is_main_menu_text(text):
        await clear_active_chat(message.from_user.id)
        return

    if is_main_menu_text(text):
        await clear_active_chat(message.from_user.id)
    # Перенаправляем в глобальный обработчик меню
        await global_main_menu_router(message, state)
        return

    active_chat = await get_active_chat(message.from_user.id)
    if not active_chat:
        return

    target_user_id = active_chat["active_chat_target_user_id"]
    post_id = active_chat["active_chat_post_id"]
    deal_id = active_chat["active_chat_deal_id"]

    if not target_user_id or not post_id:
        await clear_active_chat(message.from_user.id)
        return

    await state.set_state(ContactFlow.message_text)
    await state.update_data(
        target_user_id=target_user_id,
        post_id=post_id,
        deal_id=deal_id
    )

    await relay_message(message, state)


@router.callback_query(F.data.startswith("offer_deal_confirm:"))
async def offer_deal_confirm_handler(callback: CallbackQuery):
    try:
        _, post_id_str, owner_id_str = callback.data.split(":")
        post_id = int(post_id_str)
        owner_id = int(owner_id_str)

        if owner_id == callback.from_user.id:
            await callback.answer("Это ваше объявление", show_alert=True)
            return

        row = await get_post(post_id)
        if not row or row["status"] != STATUS_ACTIVE:
            await callback.answer("Объявление не найдено или неактивно", show_alert=True)
            return

        await callback.message.answer(
            "⚠️ <b>Подтверждение</b>\n\n"
            "Вы собираетесь отправить владельцу объявления заявку на сделку.\n\n"
            "Если владелец примет её, объявление перейдет в раздел <b>🤝 Мои сделки</b>.\n\n"
            "Отправить заявку?",
            reply_markup=confirm_offer_deal_kb(post_id, owner_id)
        )
        await callback.answer()

    except Exception as e:
        logger.exception("OFFER_DEAL_CONFIRM_HANDLER ERROR: %s", e)
        await callback.answer("Ошибка", show_alert=True)


@router.callback_query(F.data == "offer_deal_cancel")
async def offer_deal_cancel_handler(callback: CallbackQuery):
    await callback.answer("Открытие сделки отменено")
    try:
        await callback.message.answer("❌ Открытие сделки отменено.")
    except Exception as e:
        logger.exception("OFFER_DEAL_CANCEL_HANDLER ERROR: %s", e)
        

@router.callback_query(F.data.startswith("offer_deal:"))
async def offer_deal_handler(callback: CallbackQuery):
    try:
        _, post_id_str, owner_id_str = callback.data.split(":")
        post_id = int(post_id_str)
        owner_id = int(owner_id_str)
        requester_id = callback.from_user.id

        if owner_id == requester_id:
            await callback.answer("Это ваше объявление", show_alert=True)
            return

        row = await get_post(post_id)
        if not row or row["status"] != STATUS_ACTIVE:
            await callback.answer("Объявление не найдено или неактивно", show_alert=True)
            return

        if not await is_user_verified(requester_id) and await active_deals_count(requester_id) >= 2:
            await callback.answer(
                "У обычных пользователей максимум 2 активные сделки. Пройдите верификацию, чтобы снять лимит.",
                show_alert=True
            )
            return

        if await users_had_recent_deal(owner_id, requester_id):
            await callback.answer(
                "Вы недавно уже совершали сделку с этим пользователем. "
                "Новые сделки между одними и теми же пользователями возможны через 7 дней.",
                show_alert=True
            )
            return

        request_id, is_new_request = await ensure_deal_request(
            post_id=post_id,
            owner_user_id=owner_id,
            requester_user_id=requester_id
        )

        if not is_new_request:
            await callback.answer(
                "Вы уже отправили заявку на эту сделку. Ожидайте ответа владельца.",
                show_alert=True
            )
            return

        route = post_route_title(row)

        try:
            await callback.bot.send_message(
                owner_id,
                f"🤝 Пользователь предложил открыть сделку по вашему объявлению:\n"
                f"<b>{html.escape(route)}</b> (ID {post_id}).\n\n"
                f"Пользователь: {html.escape(callback.from_user.full_name or 'Пользователь')}"
                + (f" (@{html.escape(callback.from_user.username)})" if callback.from_user.username else ""),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="✅ Принять",
                                callback_data=f"deal_request_accept:{request_id}"
                            ),
                            InlineKeyboardButton(
                                text="❌ Отклонить",
                                callback_data=f"deal_request_decline:{request_id}"
                            )
                        ]
                    ]
                )
            )
        except Exception as e:
            logger.exception("OFFER DEAL NOTIFY ERROR: %s", e)

        await callback.message.answer(
            "📨 Заявка на сделку отправлена владельцу объявления.\n"
            "Ожидайте подтверждения от второго участника."
        )
        await callback.answer("Готово")

    except Exception as e:
        logger.exception("OFFER_DEAL_HANDLER ERROR: %s", e)
        await callback.answer("Ошибка при создании заявки", show_alert=True)


@router.callback_query(F.data.startswith("deal_request_accept:"))
async def deal_request_accept_handler(callback: CallbackQuery):
    try:
        request_id = int(callback.data.split(":")[1])
        req = await get_deal_request(request_id)

        if not req or req["owner_user_id"] != callback.from_user.id:
            await callback.answer("Нет доступа", show_alert=True)
            return

        row = await get_post(req["post_id"])  # FIX
        if not row:
            await callback.answer("Объявление не найдено", show_alert=True)
            return

        existing_deal = await get_active_deal_by_post(req["post_id"])
        if existing_deal:
            await callback.answer("По этому объявлению уже есть активная сделка", show_alert=True)
            return

        conn = await connect_db()
        try:
            cur = await conn.execute("""
                SELECT * FROM deal_requests WHERE id=?
            """, (request_id,))
            req_db = await cur.fetchone()

            if not req_db:
                await callback.answer("Заявка не найдена", show_alert=True)
                return

            if req_db["status"] != DEAL_REQUEST_PENDING:
                await callback.answer("Эта заявка уже обработана", show_alert=True)
                return

            cur = await conn.execute("""
                SELECT id
                FROM deals
                WHERE post_id=?
                  AND status IN (?, ?, ?, ?, ?)
                LIMIT 1
            """, (
                req["post_id"],
                DEAL_ACCEPTED,
                DEAL_COMPLETED_BY_OWNER,
                DEAL_COMPLETED_BY_REQUESTER,
                DEAL_DISPUTE_OPEN,
                DEAL_DISPUTE_WAITING
            ))
            existing_deal_db = await cur.fetchone()

            if existing_deal_db:
                await callback.answer("По этому объявлению уже есть активная сделка", show_alert=True)
                return

            await conn.execute("""
                UPDATE deal_requests
                SET status=?, updated_at=?
                WHERE id=?
            """, (DEAL_REQUEST_ACCEPTED, now_ts(), request_id))

            cur = await conn.execute("""
                INSERT INTO deals (
                    post_id, owner_user_id, requester_user_id, initiator_user_id,
                    status, owner_confirmed, requester_confirmed, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
            """, (
                req["post_id"],
                req["owner_user_id"],
                req["requester_user_id"],
                req["requester_user_id"],
                DEAL_ACCEPTED,
                now_ts(),
                now_ts()
            ))
            deal_id = int(cur.lastrowid)

            await conn.execute("""
                UPDATE posts
                SET status=?, updated_at=?
                WHERE id=?
            """, (STATUS_INACTIVE, now_ts(), req["post_id"]))

            await conn.execute("""
                UPDATE deal_requests
                SET status=?, updated_at=?
                WHERE post_id=? AND id != ? AND status=?
            """, (
                DEAL_REQUEST_DECLINED,
                now_ts(),
                req["post_id"],
                request_id,
                DEAL_REQUEST_PENDING
            ))

            await conn.commit()

        finally:
            await conn.close()

        await remove_post_from_channel(callback.bot, row)

        route = post_route_title(row)

        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

        try:
            await callback.bot.send_message(
                req["requester_user_id"],
                f"✅ Ваша заявка принята.\n\n"
                f"Вы успешно открыли сделку по объявлению <b>{html.escape(route)}</b> (ID {req['post_id']}).\n\n"
                f"ID сделки: <b>{deal_id}</b>"
            )
        except Exception as e:
            logger.exception("DEAL ACCEPT NOTIFY ERROR: %s", e)

        await callback.message.answer(
            f"✅ Сделка открыта.\n\n"
            f"<b>{html.escape(route)}</b>\n"
            f"ID сделки: <b>{deal_id}</b>"
        )

        await callback.answer("Сделка открыта")

    except Exception as e:
        logger.exception("DEAL_REQUEST_ACCEPT_HANDLER ERROR: %s", e)
        await callback.answer("Ошибка при подтверждении сделки", show_alert=True)
    

@router.callback_query(F.data.startswith("deal_request_decline:"))
async def deal_request_decline_handler(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[1])
    req = await get_deal_request(request_id)

    if not req or req["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    conn = await connect_db()
    try:
        await conn.execute("""
            UPDATE deal_requests
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_REQUEST_DECLINED, now_ts(), request_id))
        await conn.commit()
    finally:
        await conn.close()

    try:
        await callback.bot.send_message(
            req["requester_user_id"],
            f"❌ Ваша заявка на сделку по объявлению ID {req['post_id']} отклонена.\n"
            "Само объявление остается активным."
        )
    except Exception as e:
        logger.exception("DEAL REQUEST DECLINE NOTIFY ERROR: %s", e)

    await callback.message.answer(
        "❌ Заявка отклонена.\n"
        "Объявление остается активным."
    )
    await callback.answer()


@router.callback_query(F.data.startswith("deal_accept:"))
async def deal_accept_handler(callback: CallbackQuery):
    deal_id = int(callback.data.split(":")[1])
    deal = await get_deal(deal_id)

    if not deal or deal["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    conn = await connect_db()
    try:
        await conn.execute("""
            UPDATE deals
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_ACCEPTED, now_ts(), deal_id))
        await conn.commit()
    finally:
        await conn.close()

    try:
        await callback.bot.send_message(
            deal["requester_user_id"],
            f"✅ Ваша сделка по объявлению ID {deal['post_id']} принята.\n\n"
            "Управление сделками происходит во вкладке МЕНЮ '🤝 Мои сделки'.\n"
            "Там вы сможете закрыть сделку и оставить отзыв.",
            reply_markup=go_my_deals_kb()
        )
    except Exception as e:
        logger.exception("DEAL ACCEPT NOTIFY ERROR: %s", e)

    await callback.message.answer(
        "✅ <b>Сделка принята.</b>\n\n"
        "🤝 Теперь вы можете договориться о передаче посылки.\n\n"
        "📱 <b>Управление сделками</b> происходит во вкладке:\n"
        "🤝 <b>Мои сделки</b>\n\n"
        "Там вы сможете:\n"
        "• посмотреть информацию по сделке\n"
        "• завершить сделку\n"
        "• оставить отзыв\n\n"
        "⚠️ <b>Важно</b>\n"
        "Никогда не переводите предоплату незнакомым людям.",
        reply_markup=go_my_deals_kb()
    )

    await callback.answer()
    
    
@router.callback_query(F.data.startswith("deal_dispute_open:"))
async def deal_dispute_open_handler(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split(":")[1])
    deal = await get_deal(deal_id)

    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    if callback.from_user.id not in (deal["owner_user_id"], deal["requester_user_id"]):
        await callback.answer("Нет доступа", show_alert=True)
        return

    if not can_confirm_deal_now(deal):
        await callback.answer(
            f"Открыть спор можно только через {DEAL_CONFIRM_DELAY_HOURS} часов после начала сделки.",
            show_alert=True
        )
        return

    existing = await get_open_dispute_by_deal(deal_id)
    if existing:
        await callback.message.answer(
            dispute_text(existing),
            reply_markup=dispute_actions_kb(existing, callback.from_user.id)
        )
        await callback.answer("Спор уже открыт")
        return

    other_user_id = (
        deal["requester_user_id"]
        if callback.from_user.id == deal["owner_user_id"]
        else deal["owner_user_id"]
    )

    await state.clear()
    await state.set_state(DisputeFlow.reason)
    await state.update_data(
        deal_id=deal_id,
        against_user_id=other_user_id
    )

    await callback.message.answer(
        "⚖️ <b>Открытие спора</b>\n\n"
        "Опишите проблему.\n"
        "Например:\n"
        "• пользователь не отвечает\n"
        "• посылка не доставлена\n"
        "• есть подозрение на обман\n\n"
        f"Вторая сторона должна ответить в течение <b>{DISPUTE_RESPONSE_HOURS} часов</b>."
    )
    await callback.answer()
    

@router.message(DisputeFlow.reason)
async def dispute_reason_input(message: Message, state: FSMContext):
    reason_text = (message.text or "").strip()
    if len(reason_text) < 3:
        await message.answer("Опишите проблему чуть подробнее.")
        return

    data = await state.get_data()
    deal_id = data["deal_id"]
    against_user_id = data["against_user_id"]

    dispute_id = await create_dispute(
        deal_id=deal_id,
        opened_by_user_id=message.from_user.id,
        against_user_id=against_user_id,
        reason_text=reason_text[:1500]
    )

    conn = await connect_db()
    try:
        await conn.execute(
            "UPDATE deals SET status=?, updated_at=? WHERE id=?",
            (DEAL_DISPUTE_WAITING, now_ts(), deal_id)
        )
        await conn.commit()
    finally:
        await conn.close()

    dispute = await get_dispute(dispute_id)

    try:
        await message.bot.send_message(
            against_user_id,
            "⚠️ По одной из ваших сделок открыт спор.\n\n"
            f"{dispute_text(dispute)}\n\n"
            "Пожалуйста, ответьте в установленный срок.",
            reply_markup=dispute_actions_kb(dispute, against_user_id)
        )
    except Exception as e:
        logger.exception("DISPUTE NOTIFY TARGET ERROR: %s", e)

    await message.answer(
        "✅ Спор открыт.\n\n"
        f"{dispute_text(dispute)}",
        reply_markup=dispute_actions_kb(dispute, message.from_user.id)
    )

    await state.clear()


@router.callback_query(F.data.startswith("dispute_reply:"))
async def dispute_reply_handler(callback: CallbackQuery, state: FSMContext):
    dispute_id = int(callback.data.split(":")[1])
    dispute = await get_dispute(dispute_id)

    if not dispute:
        await callback.answer("Спор не найден", show_alert=True)
        return

    if callback.from_user.id != dispute["against_user_id"]:
        await callback.answer("Нет доступа", show_alert=True)
        return

    if dispute["status"] != DISPUTE_WAITING_RESPONSE:
        await callback.answer("По этому спору уже нельзя ответить", show_alert=True)
        return

    await state.clear()
    await state.set_state(DisputeFlow.response)
    await state.update_data(dispute_id=dispute_id)

    await callback.message.answer(
        "📩 Напишите ваш ответ по спору.\n\n"
        "Опишите ситуацию подробно."
    )
    await callback.answer()


@router.message(DisputeFlow.response)
async def dispute_response_input(message: Message, state: FSMContext):
    response_text = (message.text or "").strip()
    if len(response_text) < 2:
        await message.answer("Ответ слишком короткий.")
        return

    data = await state.get_data()
    dispute_id = data["dispute_id"]
    dispute = await get_dispute(dispute_id)

    if not dispute:
        await message.answer("Спор не найден.")
        await state.clear()
        return

    await save_dispute_response(dispute_id, response_text[:1500])

    conn = await connect_db()
    try:
        await conn.execute(
            "UPDATE deals SET status=?, updated_at=? WHERE id=?",
            (DEAL_DISPUTE_OPEN, now_ts(), dispute["deal_id"])
        )
        await conn.commit()
    finally:
        await conn.close()

    updated_dispute = await get_dispute(dispute_id)

    try:
        await message.bot.send_message(
            dispute["opened_by_user_id"],
            "📩 Вторая сторона ответила по спору.\n\n"
            f"{dispute_text(updated_dispute)}\n\n"
            "Выберите, решена ли проблема.",
            reply_markup=dispute_actions_kb(updated_dispute, dispute["opened_by_user_id"])
        )
    except Exception as e:
        logger.exception("DISPUTE NOTIFY OPENER ERROR: %s", e)

    await message.answer(
        "✅ Ваш ответ отправлен.\n\n"
        "Теперь ожидаем решения первой стороны."
    )
    await state.clear()


@router.callback_query(F.data.startswith("dispute_resolve:"))
async def dispute_resolve_handler(callback: CallbackQuery):
    dispute_id = int(callback.data.split(":")[1])
    dispute = await get_dispute(dispute_id)

    if not dispute:
        await callback.answer("Спор не найден", show_alert=True)
        return

    if callback.from_user.id != dispute["opened_by_user_id"]:
        await callback.answer("Нет доступа", show_alert=True)
        return

    deal = await get_deal(dispute["deal_id"])
    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    conn = await connect_db()
    try:
        await conn.execute(
            "UPDATE disputes SET status=?, updated_at=? WHERE id=?",
            (DISPUTE_RESOLVED, now_ts(), dispute_id)
        )
        await conn.execute(
            "UPDATE deals SET status=?, owner_confirmed=1, requester_confirmed=1, updated_at=?, completed_at=? WHERE id=?",
            (DEAL_COMPLETED, now_ts(), now_ts(), dispute["deal_id"])
        )
        await conn.commit()
    finally:
        await conn.close()

    completed_deal = await get_deal(dispute["deal_id"])
    updated_dispute = await get_dispute(dispute_id)

    await callback.message.answer(
        "✅ <b>Спор решен</b>\n\n"
        "Сделка завершена по соглашению сторон.\n"
        "Теперь вы можете оставить отзыв о второй стороне.",
        reply_markup=await deal_open_kb(completed_deal, callback.from_user.id)
    )

    try:
        await callback.bot.send_message(
            dispute["against_user_id"],
            "✅ <b>Спор по сделке решен</b>\n\n"
            "Сделка завершена по соглашению сторон.\n"
            "Теперь вы можете оставить отзыв о второй стороне.",
            reply_markup=await deal_open_kb(completed_deal, dispute["against_user_id"])
        )
    except Exception as e:
        logger.exception("DISPUTE RESOLVE NOTIFY ERROR: %s", e)

    await callback.message.answer(
        f"✅ <b>Сделка завершена</b>\n\n{dispute_text(updated_dispute)}"
    )

    await callback.answer()
    

@router.callback_query(F.data.startswith("dispute_unresolved:"))
async def dispute_unresolved_handler(callback: CallbackQuery):
    dispute_id = int(callback.data.split(":")[1])
    dispute = await get_dispute(dispute_id)

    if not dispute:
        await callback.answer("Спор не найден", show_alert=True)
        return

    if callback.from_user.id != dispute["opened_by_user_id"]:
        await callback.answer("Нет доступа", show_alert=True)
        return

    deal = await get_deal(dispute["deal_id"])
    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    post = await get_post(deal["post_id"])
    route = post_route_title(post) if post else f"Объявление ID {deal['post_id']}"

    # ✅ ПРАВИЛЬНАЯ РАБОТА С БД
    conn = await connect_db()
    try:
        await conn.execute(
            "UPDATE disputes SET status=?, updated_at=? WHERE id=?",
            (DISPUTE_CLOSED_UNRESOLVED, now_ts(), dispute_id)
        )

        await conn.execute(
            "UPDATE deals SET status=?, updated_at=? WHERE id=?",
            (DEAL_FAILED, now_ts(), dispute["deal_id"])
        )

        await conn.execute("""
            UPDATE users
            SET failed_dispute_count = COALESCE(failed_dispute_count, 0) + 1
            WHERE user_id=?
        """, (dispute["against_user_id"],))

        await conn.commit()
    finally:
        await conn.close()

    invalidate_user_profile_cache(dispute["against_user_id"])

    failed_deal = await get_deal(dispute["deal_id"])

    await callback.message.answer(
        f"❌ <b>Спор по сделке закрыт без решения</b>\n\n"
        f"<b>Маршрут:</b> {html.escape(route)}\n"
        f"<b>ID сделки:</b> {deal['id']}\n\n"
        "Сделка признана неуспешной и завершена внутри бота.\n\n"
        "<b>Как сервис реагирует на такую ситуацию:</b>\n"
        "• пользователю добавляется отметка <b>«⚠️ Были спорные сделки»</b>\n"
        "• это влияет на доверие других пользователей\n"
        "• при системных нарушениях аккаунт может быть ограничен\n\n"
        "<b>Что можно сделать дальше:</b>\n"
        "• оставить отзыв\n"
        "• сохранить переписку\n"
        "• связаться с администратором\n"
        "• создать новое объявление",
        reply_markup=dispute_failed_opened_by_kb(failed_deal["id"])
    )

    try:
        await callback.bot.send_message(
            dispute["against_user_id"],
            f"❌ <b>Спор по сделке закрыт без решения</b>\n\n"
            f"<b>Маршрут:</b> {html.escape(route)}\n"
            f"<b>ID сделки:</b> {deal['id']}\n\n"
            "Сделка признана неуспешной.\n\n"
            "В профиле появится отметка <b>«⚠️ Были спорные сделки»</b>.\n\n"
            "<b>Что можно сделать дальше:</b>\n"
            "• оставить отзыв\n"
            "• связаться с администратором",
            reply_markup=dispute_failed_against_kb(failed_deal["id"])
        )
    except Exception as e:
        logger.exception("DISPUTE UNRESOLVED NOTIFY ERROR: %s", e)

    await callback.answer()
    

@router.callback_query(F.data.startswith("mydeal:"))
async def open_my_deal(callback: CallbackQuery):
    try:
        deal_id = int(callback.data.split(":")[1])
        deal = await get_deal(deal_id)

        if not deal:
            await callback.answer("Сделка не найдена", show_alert=True)
            return

        if callback.from_user.id not in (
            deal["owner_user_id"],
            deal["requester_user_id"]
        ):
            await callback.answer("Нет доступа", show_alert=True)
            return

        route = await deal_title(deal)
        role = "владелец объявления" if callback.from_user.id == deal["owner_user_id"] else "откликнувшийся пользователь"

        text = (
            f"🤝 <b>{html.escape(route)}</b>\n\n"
            f"<b>ID сделки:</b> {deal['id']}\n"
            f"<b>ID объявления:</b> {deal['post_id']}\n"
            f"<b>Ваша роль:</b> {role}\n"
            f"<b>Статус:</b> {format_deal_status(deal['status'])}"
        )

        dispute = await get_open_dispute_by_deal(deal_id)
        if dispute:
            text += "\n\n" + dispute_text(dispute)
            kb = dispute_actions_kb(dispute, callback.from_user.id)
        else:
            kb = await deal_open_kb(deal, callback.from_user.id)

        await callback.message.answer(text, reply_markup=kb)
        await callback.answer()

    except Exception as e:
        logger.exception("DEAL OPEN ERROR: %s", e)
        await callback.answer("Ошибка открытия сделки", show_alert=True)
    



async def user_posts_page(
    user_id: int,
    limit: int = MY_POSTS_PAGE_SIZE,
    offset: int = 0
) -> List[aiosqlite.Row]:
    return await db_fetchall("""
        SELECT * FROM posts
        WHERE user_id=? AND status != 'deleted'
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    """, (user_id, limit, offset))


async def count_user_posts(user_id: int) -> int:
    row = await db_fetchone(
        "SELECT COUNT(*) AS c FROM posts WHERE user_id=? AND status != 'deleted'",
        (user_id,)
    )
    return int(row["c"] or 0)


def pager_kb(prefix: str, offset: int, page_size: int, total: int) -> InlineKeyboardMarkup:
    rows = []
    nav = []
    if offset > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{prefix}:{max(0, offset - page_size)}"))
    if offset + page_size < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{prefix}:{offset + page_size}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Ок", callback_data="noop")]])


async def render_recent_posts_page(target, offset: int = 0):
    rows = await get_recent_posts(POSTS_PAGE_SIZE, offset=offset)
    total = await count_recent_posts()
    if not rows:
        await target.answer("Пока нет новых объявлений.")
        return
    for row in rows:
        await send_post_card(target, row, with_age=True)
    if total > POSTS_PAGE_SIZE:
        await target.answer(f"Показано {offset + 1}-{offset + len(rows)} из {total}", reply_markup=pager_kb("recentpage", offset, POSTS_PAGE_SIZE, total))


async def render_my_posts_page(target, user_id: int, offset: int = 0):
    posts = await user_posts_page(user_id, MY_POSTS_PAGE_SIZE, offset)
    total = await count_user_posts(user_id)

    if not posts:
        text = "У вас пока нет объявлений."
        kb = main_menu(user_id)

        if isinstance(target, CallbackQuery):
            await target.message.edit_text(text)
            await target.answer()
        else:
            await target.answer(text, reply_markup=kb)
        return

    text = f"📋 <b>Ваши объявления</b>\n\nПоказано {offset + 1}-{offset + len(posts)} из {total}"

    post_kb = my_posts_kb(posts, offset)
    rows = post_kb.inline_keyboard

    if total > MY_POSTS_PAGE_SIZE:
        rows += pager_kb("mypostspage", offset, MY_POSTS_PAGE_SIZE, total).inline_keyboard

    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    if isinstance(target, CallbackQuery):
        await target.message.edit_text(text, reply_markup=kb)
        await target.answer()
    else:
        await target.answer(text, reply_markup=kb)
        

@router.callback_query(F.data.startswith("recentpage:"))
async def recent_page_callback(callback: CallbackQuery):
    offset = int(callback.data.split(":", 1)[1])
    await render_recent_posts_page(callback.message, offset)
    await callback.answer()


@router.callback_query(F.data.startswith("mypostspage:"))
async def my_posts_page_callback(callback: CallbackQuery):
    offset = int(callback.data.split(":")[1])
    await render_my_posts_page(callback, callback.from_user.id, offset)


@router.callback_query(F.data.startswith("editpost:"))
async def edit_post_entry(callback: CallbackQuery, state: FSMContext):
    post_id = int(callback.data.split(":", 1)[1])
    row = await owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    await state.update_data(edit_post_id=post_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Описание", callback_data=f"editfield:description:{post_id}")],
        [InlineKeyboardButton(text="Контакт", callback_data=f"editfield:contact_note:{post_id}")],
        [InlineKeyboardButton(text="Вес", callback_data=f"editfield:weight_kg:{post_id}")],
    ])
    await callback.message.answer("Выберите, что изменить:", reply_markup=kb)
    await callback.answer()


class EditPostFlow(StatesGroup):
    waiting_value = State()
    weight_pick = State()
    weight_manual = State()


@router.callback_query(F.data.startswith("editfield:"))
async def edit_post_field_pick(callback: CallbackQuery, state: FSMContext):
    _, field, post_id = callback.data.split(":")
    post_id = int(post_id)
    row = await owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.update_data(edit_post_id=post_id, edit_field=field)

    if field == "weight_kg":
        await state.set_state(EditPostFlow.weight_pick)
        await callback.message.answer(
            EDIT_FIELD_PROMPTS["weight_kg"],
            reply_markup=edit_weight_select_kb(post_id)
        )
        await callback.answer()
        return

    await state.set_state(EditPostFlow.waiting_value)
    await callback.message.answer(
        EDIT_FIELD_PROMPTS.get(field, "Введите новое значение:")
    )
    await callback.answer()


@router.message(EditPostFlow.weight_manual)
async def edit_post_weight_manual_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_id = data.get("edit_post_id")

    if not post_id:
        await state.clear()
        return

    value = (message.text or "").strip()
    if not value:
        await message.answer("Введите новый вес:")
        return

    ok = await update_post_record(post_id, message.from_user.id, {"weight_kg": value})
    await state.clear()

    if not ok:
        await message.answer("Не удалось обновить объявление.")
        return

    await try_update_channel_post(message.bot, post_id)

    row = await get_post(post_id)
    await message.answer("✅ Объявление обновлено.", reply_markup=main_menu(message.from_user.id))
    if row:
        await send_post_card(message, row, reply_markup=post_actions_kb(post_id, row["status"]))


@router.message(EditPostFlow.waiting_value)
async def edit_post_value_input(message: Message, state: FSMContext):
    data = await state.get_data()
    post_id = data.get("edit_post_id")
    field = data.get("edit_field")
    if not post_id or not field:
        await state.clear()
        return
    value = (message.text or "").strip()
    if field == "contact_note" and value == "-":
        value = None
    ok = await update_post_record(post_id, message.from_user.id, {field: value})
    await state.clear()
    if not ok:
        await message.answer("Не удалось обновить объявление.")
        return
    row = await get_post(post_id)
    await message.answer("✅ Объявление обновлено.", reply_markup=main_menu(message.from_user.id))
    if row:
        await send_post_card(message, row, reply_markup=post_actions_kb(post_id, row["status"]))


@router.callback_query(F.data.startswith("editweightpick:"))
async def edit_post_weight_pick(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    try:
        print("CLICK:", callback.data)

        parts = callback.data.split(":", 2)
        if len(parts) != 3:
            await callback.message.answer("Ошибка")
            return

        _, post_id_str, value = parts

        if not post_id_str.isdigit():
            await callback.message.answer("Ошибка ID")
            return

        post_id = int(post_id_str)

        row = await owner_only(callback, post_id)
        if not row:
            await callback.message.answer("Нет доступа")
            return

        if value == "__manual__":
            await state.set_state(EditPostFlow.weight_manual)
            await callback.message.answer("Введите новый вес:")
            return

        ok = await update_post_record(post_id, callback.from_user.id, {"weight_kg": value})
        if not ok:
            await callback.message.answer("Ошибка обновления")
            return

        await try_update_channel_post(callback.bot, post_id)

        updated_row = await get_post(post_id)

        await callback.message.answer("✅ Объявление обновлено")
        if updated_row:
            await send_post_card(
                callback.message,
                updated_row,
                reply_markup=post_actions_kb(post_id, updated_row["status"])
            )

    except Exception as e:
        print("EDIT WEIGHT ERROR:", e)
        await callback.message.answer(f"Ошибка: {e}")


@router.callback_query(F.data.startswith("editpost_back_to_fields:"))
async def editpost_back_to_fields(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":", 1)
    if len(parts) < 2 or not parts[1].isdigit():
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    post_id = int(parts[1])

    row = await owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return

    await state.clear()
    await state.update_data(edit_post_id=post_id)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Описание", callback_data=f"editfield:description:{post_id}")],
        [InlineKeyboardButton(text="Контакт", callback_data=f"editfield:contact_note:{post_id}")],
        [InlineKeyboardButton(text="Вес", callback_data=f"editfield:weight_kg:{post_id}")],
    ])

    await callback.message.answer("Выберите, что изменить:", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("blockuser:"))
async def block_user_callback(callback: CallbackQuery):
    target_user_id = int(callback.data.split(":", 1)[1])
    if await add_user_to_blacklist(callback.from_user.id, target_user_id):
        await callback.answer("Пользователь заблокирован", show_alert=True)
    else:
        await callback.answer("Не удалось выполнить действие", show_alert=True)


async def expire_soon_posts_notify(bot: Bot):
    while True:
        try:
            warn_before = now_ts() + EXPIRE_WARN_DAYS * 86400

            rows = await db_fetchall("""
                SELECT p.*, u.username, u.full_name
                FROM posts p
                LEFT JOIN users u ON u.user_id = p.user_id
                WHERE p.status IN ('active','inactive')
                  AND p.expires_at IS NOT NULL
                  AND p.expires_at <= ?
                  AND p.expires_at > ?
                  AND (p.expire_warned_at IS NULL OR p.expire_warned_at = 0)
                LIMIT 100
            """, (warn_before, now_ts()))

            for row in rows:
                try:
                    await bot.send_message(
                        row["user_id"],
                        f"⌛ Объявление ID {row['id']} скоро истечет. Откройте 'Мои объявления', чтобы активировать его снова.",
                        reply_markup=main_menu(row["user_id"])
                    )

                    await db_execute(
                        "UPDATE posts SET expire_warned_at=? WHERE id=?",
                        (now_ts(), row["id"])
                    )

                except Exception as e:
                    logger.exception("EXPIRE SOON NOTIFY ERROR: %s", e)

        except Exception as e:
            logger.exception("EXPIRE SOON LOOP ERROR: %s", e)

        await asyncio.sleep(3600)
        

@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery):
    await callback.answer()

@router.message(Command("test"))
async def test_handler(message: Message):
    await message.answer_sticker(ONBOARDING_STICKER_ID)


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Set BOT_TOKEN env var")

    await init_db()

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
    

    async def on_startup():
        asyncio.create_task(expire_old_posts(bot))
        asyncio.create_task(global_coincidence_loop(bot))
        asyncio.create_task(dispute_timeout_loop(bot))
        asyncio.create_task(expire_soon_posts_notify(bot))

    await on_startup()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
