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
ADMIN_IDS = {474671704}
MODERATION_ENABLED = False

POST_TTL_DAYS = 30
AUTO_HIDE_COMPLAINTS_THRESHOLD = 3

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
AUTO_HIDE_COMPLAINTS_THRESHOLD = int(os.getenv("AUTO_HIDE_COMPLAINTS_THRESHOLD", "3"))

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
ONBOARDING_TEXTS = {
    1: (
        "👋 <b>Добро пожаловать в Попутчик Китай</b>\n\n"
        "Сервис для передачи посылок с попутчиками.\n\n"
        "📦 Нужно отправить посылку?\n"
        "✈️ Летите и готовы помочь другим людям?\n\n"
        "Платформа соединяет пользователей\n"
        "по подходящим маршрутам.\n\n"
        "Вы создаете объявление —\n"
        "система <b>автоматически найдет и уведомит вас о совпадении.</b>"
    ),

    2: (
        "📱 <b>Раньше попутчиков искали вручную</b>\n\n"
        "Люди рассылали сообщения по\n\n"
        "WeChat-группам, знакомым и друзьям\n"
        "Это занимало много времени и терпения\n"
        "и часто <b>не давало результата.</b>\n\n"
        "Попутчик Китай делает поиск <b>невероятно простым.</b>\n\n"
        "Система <b>сама находит пользователей с подходящими маршрутами пока вы пьете свой лате.</b>"
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
        "👉 <b>Канал:</b> t.me/china_poputchik"
    ),

    4: (
        "🌍 <b>Примеры маршрутов пользователей</b>\n\n"
        "✈️ Китай, Шэньчжэнь → Россия, Москва\n"
        "📦 образцы тканей\n\n"
        "✈️ Китай, Гуанчжоу → Казахстан, Алматы\n"
        "📦 до 5 кг\n\n"
        "✈️ Китай, Шанхай → ОАЭ, Дубай\n"
        "📦 документы\n\n"
        "✈️ Китай, Пекин → Россия, Санкт-Петербург\n"
        "📦 личные вещи\n\n"
        "Каждый день появляются <b>новые объявления.</b>"
    ),

    5: (
        "🚀 <b>Что умеет Попутчик Китай</b>\n\n"
        "🤖 <b>Автоматический умный поиск совпадений</b>\n"
        "Система 24/7 анализирует маршруты и соединяет пользователей.\n\n"
        "⭐ <b>Отзывы и рейтинг</b>\n"
        "Можно видеть репутацию пользователей.\n\n"
        "🤝 <b>Система сделок</b>\n"
        "Позволяет фиксировать договоренности внутри бота.\n\n"
        "🔔 <b>Уведомления</b>\n"
        "Бот сообщает, когда появляется подходящее Вам обьявление."
    ),

    6: (
        "📱 <b>Навигация в боте</b>\n\n"
        "В Боте есть два типа кнопок с помощью которых происходит управление.\n\n"
        "⬜ <b>Кнопка с четырьмя кружками</b>\n"
        "<i>(в правом углу строки ввода сообщения)</i>\n\n"
        "Это <b>интерактивное меню действий</b>.\n\n"
        "Здесь находится основной функционал сервиса.\n"
        "Именно им Вы будете пользоваться чаще всего.\n\n"
        "Через эти кнопки можно быстро управлять вкладками:\n\n"
        "✈️ Взять посылку\n"
        "📦 Отправить посылку\n"
        "🔎 Найти совпадения\n"
        "📋 Мои объявления\n"
        "🤝 Мои сделки\n"
        "🔔 Подписки\n"
        "🆕 Новые объявления\n"
        "🔥 Популярные маршруты\n"
        "💰 Поднять объявление\n"
        "📊 Статистика\n"
        "🚩 Пожаловаться\n"
        "ℹ️ Помощь\n\n"
        "🟦 <b>Кнопка Menu</b>\n"
        "<i>(в левом углу строки ввода сообщения)</i>\n\n"
        "Открывает основные команды бота:\n\n"
        "• Запустить бот\n"
        "• Взять посылку\n"
        "• Отправить посылку\n"
        "• Найти совпадения\n"
        "• Мои объявления"
    ),
}


def onboarding_next_kb(screen: int):
    rows = []

    if screen == 3:
        rows.append([InlineKeyboardButton(text="📢 Открыть канал", url="https://t.me/china_poputchik")])

    rows.append([InlineKeyboardButton(text="➡️ Далее", callback_data=f"onboarding_next:{screen}")])
    rows.append([InlineKeyboardButton(text="⏭ Пропустить", callback_data="onboarding_skip")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def onboarding_finish_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✈️ Я лечу", callback_data="onboarding_action:trip")],
        [InlineKeyboardButton(text="📦 Отправить посылку", callback_data="onboarding_action:parcel")],
        [InlineKeyboardButton(text="🔎 Смотреть объявления", callback_data="onboarding_action:browse")],
    ])
    

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
        ensure_column(conn, "users", "failed_dispute_count", "failed_dispute_count INTEGER DEFAULT 0")
        ensure_column(conn, "posts", "expires_at", "expires_at INTEGER")
        ensure_column(conn, "posts", "photo_file_id", "photo_file_id TEXT")
        ensure_column(conn, "deals", "initiator_user_id", "initiator_user_id INTEGER")
        ensure_column(conn, "deals", "owner_confirmed", "owner_confirmed INTEGER DEFAULT 0")
        ensure_column(conn, "deals", "requester_confirmed", "requester_confirmed INTEGER DEFAULT 0")
        ensure_column(conn, "deals", "updated_at", "updated_at INTEGER DEFAULT 0")
        ensure_column(conn, "users", "onboarding_completed", "onboarding_completed INTEGER DEFAULT 0")

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


def is_onboarding_completed(user_id: int) -> bool:
    with closing(connect_db()) as conn:
        row = conn.execute(
            "SELECT onboarding_completed FROM users WHERE user_id=?",
            (user_id,)
        ).fetchone()
        return bool(row and row["onboarding_completed"])


def set_onboarding_completed(user_id: int):
    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE users SET onboarding_completed=1 WHERE user_id=?",
            (user_id,)
        )


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


def unban_user(user_id: int):
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_banned=0 WHERE user_id=?", (user_id,))


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


def user_has_warning_badge(user_id: int) -> bool:
    with closing(connect_db()) as conn:
        row = conn.execute("""
            SELECT failed_dispute_count, dispute_no_response_count
            FROM users
            WHERE user_id=?
        """, (user_id,)).fetchone()

    if not row:
        return False

    return (
        int(row["failed_dispute_count"] or 0) > 0
        or int(row["dispute_no_response_count"] or 0) > 0
    )


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
            WHERE deal_id=? AND status IN (?, ?, ?)
            ORDER BY id DESC
            LIMIT 1
        """, (
            deal_id,
            DISPUTE_OPEN,
            DISPUTE_WAITING_RESPONSE,
            DISPUTE_RESPONDED
        )).fetchone()


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


def ensure_deal_request(post_id: int, owner_user_id: int, requester_user_id: int) -> int:
    with closing(connect_db()) as conn, conn:
        row = conn.execute("""
            SELECT id
            FROM deal_requests
            WHERE post_id=? AND owner_user_id=? AND requester_user_id=? AND status=?
            ORDER BY id DESC
            LIMIT 1
        """, (post_id, owner_user_id, requester_user_id, DEAL_REQUEST_PENDING)).fetchone()

        if row:
            return int(row["id"])

        cur = conn.execute("""
            INSERT INTO deal_requests (
                post_id, owner_user_id, requester_user_id, status, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            post_id, owner_user_id, requester_user_id,
            DEAL_REQUEST_PENDING, now_ts(), now_ts()
        ))
        return int(cur.lastrowid)


def get_deal_request(request_id: int) -> Optional[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT *
            FROM deal_requests
            WHERE id=?
        """, (request_id,)).fetchone()
        

def format_deal_status(status: str) -> str:
    mapping = {

        # реальные статусы сделки
        DEAL_ACCEPTED: "сделка принята",
        DEAL_COMPLETED_BY_OWNER: "подтвердил владелец",
        DEAL_COMPLETED_BY_REQUESTER: "подтвердил откликнувшийся",
        DEAL_COMPLETED: "сделка завершена",
        DEAL_FAILED: "сделка неуспешна",
        DEAL_CANCELLED: "сделка отменена",
        DEAL_DISPUTE_OPEN: "спор активен",
        DEAL_DISPUTE_WAITING: "ожидается ответ по спору",
        DEAL_DISPUTE_RESOLVED: "спор решен",

        # статусы заявок на сделку (ещё НЕ сделка)
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


def post_text(row, for_channel: bool = False) -> str:
    route = html.escape(row["from_country"])
    if row["from_city"]:
        route += f", {html.escape(row['from_city'])}"
    route += " → " + html.escape(row["to_country"])
    if row["to_city"]:
        route += f", {html.escape(row['to_city'])}"

    owner_user_id = row["user_id"]
    verified_badge = " ✅ Проверенный" if is_user_verified(owner_user_id) else ""
    warning_badge = " ⚠️ Были спорные сделки" if user_has_warning_badge(owner_user_id) else ""
    rating_line = format_rating_line(owner_user_id)
    completed_deals = user_completed_deals_count(owner_user_id)
    service_text = user_service_text(owner_user_id)

    lines = [
        f"<b>{short_post_type(row['post_type'])}{verified_badge}{warning_badge}</b>",
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

async def show_onboarding_screen(target, screen: int):
    text = ONBOARDING_TEXTS[screen]
    kb = onboarding_finish_kb() if screen == 6 else onboarding_next_kb(screen)

    try:
        if hasattr(target, "edit_text"):
            await target.edit_text(text, reply_markup=kb)
        else:
            await target.answer(text, reply_markup=kb)
    except Exception as e:
        print(f"SHOW_ONBOARDING_SCREEN ERROR: {e}")
        if hasattr(target, "answer"):
            await target.answer(text, reply_markup=kb)


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
        status_text = format_post_status(p["status"])
        label = f"{index}. {icon} {p['from_country']} → {p['to_country']} • {status_text}"

        rows.append([
            InlineKeyboardButton(
                text=label[:64],
                callback_data=f"mypost:{p['id']}"
            )
        ])

    return InlineKeyboardMarkup(
        inline_keyboard=rows or [[InlineKeyboardButton(text="Нет объявлений", callback_data="noop")]]
    )


def deal_list_kb(deals: List[sqlite3.Row]):
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

        label = f"{status_icon} {deal_title(d)}"
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

    elif status == STATUS_PENDING:
        rows.append([
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete:{post_id}")
        ])

    else:
        rows.append([
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete:{post_id}")
        ])

    # ⬅️ КНОПКА НАЗАД
    rows.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back:my_posts")
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def dispute_failed_against_kb(deal_id: int):

    rows = [
        [
            InlineKeyboardButton(
                text="⭐ Оставить отзыв",
                callback_data=f"deal_review:{deal_id}"
            )
        ]
    ]

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


def admin_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📚 Все объявления", callback_data="admin:all_posts")],
        [InlineKeyboardButton(text="🆘 Последние жалобы", callback_data="admin:complaints")],
        [InlineKeyboardButton(text="👤 Пользователь", callback_data="admin:user_lookup")],
        [InlineKeyboardButton(text="💰 Заявки на поднятие", callback_data="admin:bump_orders")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
    ])


def popular_routes_kb(rows: List[sqlite3.Row]):
    buttons = []
    for row in rows:
        label = f"{row['from_country']} → {row['to_country']} ({row['cnt']})"
        buttons.append([InlineKeyboardButton(text=label[:64], callback_data=f"popular:{row['from_country']}:{row['to_country']}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons or [[InlineKeyboardButton(text="Пока пусто", callback_data="noop")]])


def deal_open_kb(deal: sqlite3.Row, user_id: int) -> InlineKeyboardMarkup:
    rows = []

    viewer_is_owner = user_id == deal["owner_user_id"]
    other_user_id = deal["requester_user_id"] if viewer_is_owner else deal["owner_user_id"]

    if deal["status"] in (DEAL_ACCEPTED, DEAL_COMPLETED_BY_OWNER, DEAL_COMPLETED_BY_REQUESTER):
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
        rows.append([
            InlineKeyboardButton(
                text="💬 Написать в чат через бота",
                callback_data=f"reply_contact:{deal['post_id']}:{other_user_id}:{deal['id']}"
            )
        ])

    elif deal["status"] == DEAL_COMPLETED:
        if not has_user_left_review_for_deal(deal, user_id):
            rows.append([
                InlineKeyboardButton(
                    text="⭐ Оставить отзыв",
                    callback_data=f"deal_review:{deal['id']}"
                )
            ])
        rows.append([
            InlineKeyboardButton(text="Ок", callback_data="noop")
        ])

    elif deal["status"] in (DEAL_DISPUTE_OPEN, DEAL_DISPUTE_WAITING):
        dispute = get_open_dispute_by_deal(deal["id"])

        if dispute:
            dispute_kb = dispute_actions_kb(dispute, user_id)
            rows.extend(dispute_kb.inline_keyboard)
        else:
            rows.append([
                InlineKeyboardButton(text="Ок", callback_data="noop")
            ])

    elif deal["status"] in (DEAL_FAILED, DEAL_CANCELLED):
        rows.append([
            InlineKeyboardButton(text="Ок", callback_data="noop")
        ])

    else:
        rows.append([
            InlineKeyboardButton(text="Ок", callback_data="noop")
        ])

    rows.append([
        InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data="back:my_deals"
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)
    

def get_active_deal_by_post(post_id: int) -> Optional[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
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
        )).fetchone()
        

def dispute_actions_kb(dispute: sqlite3.Row, viewer_user_id: int) -> InlineKeyboardMarkup:
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
        [KeyboardButton(text="🔎 Найти совпадения"), KeyboardButton(text="📋 Мои объявления")],
        [KeyboardButton(text="🤝 Мои сделки"), KeyboardButton(text="🔔 Подписки")],
        [KeyboardButton(text="🆕 Новые объявления"), KeyboardButton(text="🔥 Популярные маршруты")],
        [KeyboardButton(text="💰 Поднять объявление"), KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="🚩 Жалоба / Баг / Поддержка"), KeyboardButton(text="ℹ️ Помощь")],
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


class OnboardingFlow(StatesGroup):
    screen_1 = State()
    screen_2 = State()
    screen_3 = State()
    screen_4 = State()
    screen_5 = State()
    screen_6 = State()


class AdminContactFlow(StatesGroup):
    message = State()
    

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


def get_pending_posts(limit: int = 20):
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.status=?
            ORDER BY p.created_at ASC
            LIMIT ?
        """, (STATUS_PENDING, limit)).fetchall()


def get_recent_complaints(limit: int = 20):
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT c.*, p.user_id AS post_owner_user_id
            FROM complaints c
            LEFT JOIN posts p ON p.id = c.post_id
            ORDER BY c.created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()


def get_pending_bump_orders(limit: int = 20):
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT *
            FROM bump_orders
            WHERE status='pending'
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()


def get_admin_posts(limit: int = 30):
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.status != ?
            ORDER BY p.created_at DESC
            LIMIT ?
        """, (STATUS_DELETED, limit)).fetchall()


def support_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚩 Пожаловаться на объявление", callback_data="support:complaint")],
        [InlineKeyboardButton(text="🐞 Сообщить о баге", callback_data="support:bug")],
        [InlineKeyboardButton(text="🆘 Связаться с поддержкой", callback_data="support:help")],
    ])


def get_user_profile(user_id: int):
    with closing(connect_db()) as conn:
        user = conn.execute("""
            SELECT *
            FROM users
            WHERE user_id=?
        """, (user_id,)).fetchone()

        posts_count = conn.execute("""
            SELECT COUNT(*) AS c
            FROM posts
            WHERE user_id=? AND status != ?
        """, (user_id, STATUS_DELETED)).fetchone()["c"]

        active_posts = conn.execute("""
            SELECT COUNT(*) AS c
            FROM posts
            WHERE user_id=? AND status=?
        """, (user_id, STATUS_ACTIVE)).fetchone()["c"]

        completed_deals = conn.execute("""
            SELECT COUNT(*) AS c
            FROM deals
            WHERE status=? AND (owner_user_id=? OR requester_user_id=?)
        """, (DEAL_COMPLETED, user_id, user_id)).fetchone()["c"]

        complaints_received = conn.execute("""
            SELECT COUNT(*) AS c
            FROM complaints c
            LEFT JOIN posts p ON p.id = c.post_id
            WHERE p.user_id=?
        """, (user_id,)).fetchone()["c"]

        return {
            "user": user,
            "posts_count": int(posts_count or 0),
            "active_posts": int(active_posts or 0),
            "completed_deals": int(completed_deals or 0),
            "complaints_received": int(complaints_received or 0),
        }


def admin_posts_kb(rows: List[sqlite3.Row]):
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


def admin_user_actions_kb(user_id: int, is_verified: bool, is_banned: bool):
    rows = []

    if is_verified:
        rows.append([InlineKeyboardButton(text="↩️ Снять верификацию", callback_data=f"admin_user_unverify:{user_id}")])
    else:
        rows.append([InlineKeyboardButton(text="✅ Верифицировать", callback_data=f"admin_user_verify:{user_id}")])

    if is_banned:
        rows.append([InlineKeyboardButton(text="♻️ Разбанить", callback_data=f"admin_user_unban:{user_id}")])
    else:
        rows.append([InlineKeyboardButton(text="🚫 Забанить", callback_data=f"admin_user_ban:{user_id}")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_stats_text() -> str:
    with closing(connect_db()) as conn:
        users_count = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        active_posts = conn.execute(
            "SELECT COUNT(*) AS c FROM posts WHERE status='active'"
        ).fetchone()["c"]
        pending_posts = conn.execute(
            "SELECT COUNT(*) AS c FROM posts WHERE status='pending'"
        ).fetchone()["c"]
        complaints_count = conn.execute(
            "SELECT COUNT(*) AS c FROM complaints"
        ).fetchone()["c"]
        disputes_open = conn.execute("""
            SELECT COUNT(*) AS c
            FROM disputes
            WHERE status IN (?, ?, ?)
        """, (
            DISPUTE_OPEN,
            DISPUTE_WAITING_RESPONSE,
            DISPUTE_RESPONDED
        )).fetchone()["c"]
        bump_pending = conn.execute("""
            SELECT COUNT(*) AS c
            FROM bump_orders
            WHERE status='pending'
        """).fetchone()["c"]

    return (
        "👨‍💼 <b>Админка</b>\n\n"
        f"👤 Пользователей: <b>{users_count}</b>\n"
        f"📦 Активных объявлений: <b>{active_posts}</b>\n"
        f"⏳ На модерации: <b>{pending_posts}</b>\n"
        f"🆘 Жалоб: <b>{complaints_count}</b>\n"
        f"⚖️ Активных споров: <b>{disputes_open}</b>\n"
        f"💰 Заявок на поднятие: <b>{bump_pending}</b>"
    )


def verify_user(user_id: int):
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_verified=1 WHERE user_id=?", (user_id,))


def unverify_user(user_id: int):
    with closing(connect_db()) as conn, conn:
        conn.execute("UPDATE users SET is_verified=0 WHERE user_id=?", (user_id,))


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
            STATUS_PENDING if MODERATION_ENABLED else STATUS_ACTIVE,
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


def deal_title(deal: sqlite3.Row) -> str:
    post = get_post(deal["post_id"])

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


def split_deals_by_sections(deals: List[sqlite3.Row]):
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


def deal_section_kb(deals: List[sqlite3.Row]) -> InlineKeyboardMarkup:
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

        label = f"{icon} {deal_title(d)}"
        rows.append([
            InlineKeyboardButton(
                text=label[:64],
                callback_data=f"mydeal:{d['id']}"
            )
        ])

    if not rows:
        rows = [[InlineKeyboardButton(text="Пусто", callback_data="noop")]]

    return InlineKeyboardMarkup(inline_keyboard=rows)


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
                        "⛔ Вы не ответили по спору в установленный срок.\n"
                         f"Срок ответа: {DISPUTE_RESPONSE_HOURS} часов.\n"
                         "Ваш аккаунт временно ограничен. Свяжитесь с администратором."
                    )
                except Exception:
                    pass

                try:
                    await bot.send_message(
                        dispute["opened_by_user_id"],
                        "⚠️ Вторая сторона не ответила по спору в установленный срок.\n"
                        f"Срок ожидания: {DISPUTE_RESPONSE_HOURS} часов.\n"
                        "Спор закрыт автоматически, аккаунт второй стороны ограничен."
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
    

def dispute_text(dispute: sqlite3.Row) -> str:
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
        

@router.callback_query(F.data.startswith("contact_admin:"))
async def contact_admin_handler(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split(":")[1])

    deal = get_deal(deal_id)
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


@router.message(AdminContactFlow.message)
async def admin_contact_message(message: Message, state: FSMContext):
    data = await state.get_data()
    deal_id = data.get("deal_id")

    deal = get_deal(deal_id)
    post = get_post(deal["post_id"]) if deal else None

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
        f"<b>Сообщение:</b>\n{message.text}"
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
            print(f"ADMIN CONTACT ERROR: {e}")

    await message.answer(
        "📩 Ваше сообщение отправлено администратору.\n"
        "Он свяжется с вами напрямую в Telegram."
    )

    await state.clear()


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

    if text == "🚩 Пожаловаться":
        await complaint_start(message, state)
        return

    if text == "ℹ️ Помощь":
        await help_handler(message)
        return

    if text == "👨‍💼 Админка":
        await admin_menu_handler(message)
        return
        

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

    # ---------- deep links сначала ----------

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
                    await message.answer(
                        "Это ваше объявление.",
                        reply_markup=main_menu(message.from_user.id)
                    )
                    return

                await state.set_state(ContactFlow.message_text)

                await state.update_data(
                    post_id=row["id"],
                    target_user_id=row["user_id"],
                    deal_id=None
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

    # ---------- только теперь онбординг ----------

    if not is_onboarding_completed(message.from_user.id):
        await state.set_state(OnboardingFlow.screen_1)
        await show_onboarding_screen(message, 1)
        return

    await message.answer(
        WELCOME_TEXT,
        reply_markup=main_menu(message.from_user.id)
    )
    

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
        set_onboarding_completed(callback.from_user.id)

    await state.set_state(state_map[next_screen])
    await show_onboarding_screen(callback.message, next_screen)
    await callback.answer()

@router.callback_query(F.data.startswith("adminapprove:"))
async def admin_approve_post(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])
    row = get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (STATUS_ACTIVE, now_ts(), post_id)
        )

    try:
        await callback.bot.send_message(
            row["user_id"],
            f"✅ Ваше объявление ID {post_id} одобрено и опубликовано."
        )
    except Exception:
        pass

    await safe_publish(bot, post_id)
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
    row = get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (STATUS_REJECTED, now_ts(), post_id)
        )

    try:
        await callback.bot.send_message(
            row["user_id"],
            f"❌ Ваше объявление ID {post_id} отклонено модератором."
        )
    except Exception:
        pass

    await callback.message.answer(f"❌ Объявление {post_id} отклонено.")
    await callback.answer()


@router.callback_query(F.data.startswith("adminbanpost:"))
async def admin_ban_post_owner(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])
    row = get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    ban_user(row["user_id"])

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
    set_onboarding_completed(callback.from_user.id)
    await state.clear()

    await callback.message.answer(
        "Онбординг пропущен. Основные функции доступны в меню ниже.",
        reply_markup=main_menu(callback.from_user.id)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("onboarding_action:"))
async def onboarding_action_handler(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split(":")[1]

    set_onboarding_completed(callback.from_user.id)
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
        rows = get_recent_posts(10)
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
                await callback.message.answer(
                    f"{post_text(row)}\n\n<b>Добавлено:</b> {format_age(row['created_at'])}",
                    reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
                )

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


@router.message(F.text == "🤝 Мои сделки")
async def my_deals_menu(message: Message):
    upsert_user(message)
    await message.answer(MENU_TEXTS["deals"], reply_markup=main_menu(message.from_user.id))

    deals = list_user_deals(message.from_user.id)
    if not deals:
        await message.answer("У вас пока нет сделок.", reply_markup=main_menu(message.from_user.id))
        return

    in_progress, disputes, finished = split_deals_by_sections(deals)

    if in_progress:
        await message.answer(
            "🟢 <b>Сделки в процессе</b>\n"
            "Здесь сделки, по которым сейчас идёт передача или ожидание подтверждения.",
            reply_markup=deal_section_kb(in_progress)
        )

    if disputes:
        await message.answer(
            "⚖️ <b>Споры</b>\n"
            "Здесь сделки, по которым открыт спор или ожидается решение.",
            reply_markup=deal_section_kb(disputes)
        )

    if finished:
        await message.answer(
            "✅ <b>Завершённые и закрытые</b>\n"
            "Здесь завершённые, неуспешные и отменённые сделки.",
            reply_markup=deal_section_kb(finished)
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


@router.message(Command("admin_verify"))
async def admin_verify_user_cmd(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Использование: /admin_verify USER_ID")
        return

    user_id = int(parts[1])
    verify_user(user_id)
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
    unverify_user(user_id)
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
    ban_user(user_id)
    await message.answer(f"⛔ Пользователь {user_id} забанен.")
    

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
        f"<b>ID:</b> {message.from_user.id}\n\n"
        f"<b>Описание:</b>\n{html.escape(text[:2000])}"
    )

    for admin_id in ADMIN_IDS:
        try:
            await message.bot.send_message(admin_id, admin_text)
        except Exception as e:
            print(f"BUG REPORT SEND ERROR: {e}")

    await message.answer(
        "✅ Сообщение о баге отправлено. Спасибо.",
        reply_markup=main_menu(message.from_user.id)
    )
    await state.clear()


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
        f"<b>ID:</b> {message.from_user.id}\n\n"
        f"<b>Сообщение:</b>\n{html.escape(text[:2000])}"
    )

    for admin_id in ADMIN_IDS:
        try:
            await message.bot.send_message(admin_id, admin_text)
        except Exception as e:
            print(f"SUPPORT SEND ERROR: {e}")

    await message.answer(
        "✅ Ваше сообщение отправлено в поддержку.",
        reply_markup=main_menu(message.from_user.id)
    )
    await state.clear()
    

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


@router.callback_query(F.data.startswith("back:"))
async def back_router(callback: CallbackQuery):

    action = callback.data.split(":")[1]

    if action == "my_posts":

        posts = get_user_posts(callback.from_user.id)

        if not posts:
            await callback.message.answer("У вас пока нет объявлений.")
        else:
            await callback.message.answer(
                "📋 Ваши объявления:",
                reply_markup=my_posts_kb(posts)
            )

    elif action == "my_deals":

        deals = list_user_deals(callback.from_user.id)

        if not deals:
            await callback.message.answer("У вас пока нет сделок.")
        else:
            in_progress, disputes, finished = split_deals_by_sections(deals)

            if in_progress:
                await callback.message.answer(
                    "🟢 Сделки в процессе",
                    reply_markup=deal_section_kb(in_progress)
                )

            if disputes:
                await callback.message.answer(
                    "⚖️ Споры",
                    reply_markup=deal_section_kb(disputes)
                )

            if finished:
                await callback.message.answer(
                    "✅ Завершённые сделки",
                    reply_markup=deal_section_kb(finished)
                )

    elif action == "new_posts":

        posts = get_recent_posts(10)

        if not posts:
            await callback.message.answer("Новых объявлений пока нет.")
        else:
            await callback.message.answer("🆕 Новые объявления:")

            for row in posts:
                await callback.message.answer(
                    post_text(row),
                    reply_markup=public_post_kb(
                        row["id"],
                        row["user_id"],
                        row["post_type"]
                    )
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
    "✅ Объявление создано.\n" + ("Оно отправлено на модерацию." if MODERATION_ENABLED else "Оно уже активно."),
            reply_markup=main_menu(message.from_user.id)
        )

        await message.answer(post_text(row), reply_markup=post_actions_kb(post_id, row["status"]))

        if MODERATION_ENABLED and row["status"] == STATUS_PENDING:
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

    if deal["status"] not in (DEAL_COMPLETED, DEAL_FAILED):
        await callback.answer("Отзыв можно оставить только по завершенной или неуспешной сделке", show_alert=True)
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
    try:
        post_id = int(callback.data.split(":")[1])
        row = get_post(post_id)

        if not row:
            await callback.answer("Объявление не найдено", show_alert=True)
            return

        if row["user_id"] != callback.from_user.id:
            await callback.answer("Нет доступа", show_alert=True)
            return

        if row["status"] == STATUS_DELETED:
            await callback.answer("Объявление уже удалено", show_alert=True)
            return

        text = post_text(row)
        if len(text) > 4000:
            text = text[:3900] + "\n\n..."

        await callback.message.answer(
            text,
            reply_markup=post_actions_kb(post_id, row["status"])
        )
        await callback.answer()

    except Exception as e:
        print(f"OPEN_MY_POST ERROR: {e}")
        await callback.answer("Не удалось открыть объявление", show_alert=True)


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

    other_user_id = deal["requester_user_id"] if user_id == deal["owner_user_id"] else deal["owner_user_id"]

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

            completed_deal = get_deal(deal_id)

            await callback.message.answer(
                "✅ Сделка завершена. Теперь можно оставить отзыв.",
                reply_markup=deal_open_kb(completed_deal, callback.from_user.id)
            )

            try:
                await callback.bot.send_message(
                    other_user_id,
                    f"✅ Сделка #{deal_id} завершена обеими сторонами.\n"
                    "Теперь вы можете открыть 'Мои сделки' и оставить отзыв.",
                    reply_markup=deal_open_kb(completed_deal, other_user_id)
                )
            except Exception as e:
                print(f"DEAL COMPLETE NOTIFY ERROR: {e}")

        else:
            new_status = DEAL_COMPLETED_BY_OWNER if user_id == deal["owner_user_id"] else DEAL_COMPLETED_BY_REQUESTER
            conn.execute("""
                UPDATE deals
                SET owner_confirmed=?, requester_confirmed=?, status=?, updated_at=?
                WHERE id=?
            """, (owner_confirmed, requester_confirmed, new_status, now_ts(), deal_id))

            await callback.message.answer("✅ Ваше подтверждение сохранено. Ждем подтверждение второй стороны.")

            try:
                await callback.bot.send_message(
                    other_user_id,
                    f"📦 Пользователь подтвердил завершение сделки #{deal_id}.\n"
                    "Откройте 'Мои сделки', чтобы подтвердить завершение со своей стороны."
                )
            except Exception as e:
                print(f"DEAL PARTIAL CONFIRM NOTIFY ERROR: {e}")

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

    new_status = STATUS_PENDING if MODERATION_ENABLED else STATUS_ACTIVE
    expires_at = calculate_post_expires_at(now_ts(), row["travel_date"], POST_TTL_DAYS)

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET status=?, updated_at=?, expires_at=? WHERE id=?",
            (new_status, now_ts(), expires_at, post_id)
        )

    await callback.message.answer(
        f"Объявление {post_id} " + ("отправлено на повторную модерацию." if MODERATION_ENABLED else "активировано.")
    )

    if not MODERATION_ENABLED:
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
        # защита от повторной жалобы от одного и того же пользователя
        existing = conn.execute(
            "SELECT 1 FROM complaints WHERE post_id=? AND from_user_id=? LIMIT 1",
            (post_id, message.from_user.id)
        ).fetchone()

        if existing:
            await state.clear()
            await message.answer(
                "Вы уже отправляли жалобу на это объявление.",
                reply_markup=main_menu(message.from_user.id)
            )
            return

        conn.execute(
            "INSERT INTO complaints (post_id, from_user_id, reason, created_at) VALUES (?, ?, ?, ?)",
            (post_id, message.from_user.id, reason[:1000], now_ts())
        )

        complaints_count = conn.execute(
            "SELECT COUNT(*) AS c FROM complaints WHERE post_id=?",
            (post_id,)
        ).fetchone()["c"]

        row = conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.id=?
        """, (post_id,)).fetchone()

        auto_hidden = False
        if row and row["status"] == STATUS_ACTIVE and complaints_count >= AUTO_HIDE_COMPLAINTS_THRESHOLD:
            conn.execute(
                "UPDATE posts SET status=?, updated_at=? WHERE id=?",
                (STATUS_INACTIVE, now_ts(), post_id)
            )
            auto_hidden = True

    await state.clear()

    if auto_hidden:
        try:
            await remove_post_from_channel(message.bot, row)
        except Exception as e:
            print(f"AUTO HIDE CHANNEL REMOVE ERROR: {e}")

        try:
            await message.bot.send_message(
                row["user_id"],
                f"⚠️ Ваше объявление ID {post_id} временно скрыто автоматически, "
                f"так как набрало {complaints_count} жалобы.\n"
                "Если это ошибка — свяжитесь с администратором."
            )
        except Exception as e:
            print(f"AUTO HIDE OWNER NOTIFY ERROR: {e}")

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
            print(f"ADMIN COMPLAINT NOTIFY ERROR: {e}")


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


@router.message(Command("admin"))
@router.message(F.text == "👨‍💼 Админка")
async def admin_menu_handler(message: Message):
    upsert_user(message)

    if not is_admin(message.from_user.id):
        await message.answer("Нет доступа.", reply_markup=main_menu(message.from_user.id))
        return

    await message.answer(
        admin_stats_text(),
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
    post_id = int(post_id)
    owner_id = int(owner_id)

    if owner_id == callback.from_user.id:
        await callback.answer("Это ваше объявление", show_alert=True)
        return

    await state.set_state(ContactFlow.message_text)
    await state.update_data(
        post_id=post_id,
        target_user_id=owner_id,
        deal_id=None
    )

    await callback.message.answer(
        "✉️ <b>Напишите сообщение владельцу объявления.</b>\n"
        "Я перешлю его через бота.\n\n"
        "🔒 <b>Важно:</b>\n"
        "Никогда не переводите предоплату незнакомым людям.\n\n"
        "Перед сделкой рекомендуем проверить:\n"
        "• WeChat второго пользователя\n"
        "• историю аккаунта\n"
        "• связан ли человек с Китаем\n\n"
    )

    await callback.answer()
@router.callback_query(F.data == "admin:stats")
async def admin_stats_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await callback.message.answer(admin_stats_text(), reply_markup=admin_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "admin:all_posts")
async def admin_all_posts_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    rows = get_admin_posts(30)
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
    row = get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    text = post_text(row)
    if len(text) > 4000:
        text = text[:3900] + "\n\n..."

    await callback.message.answer(
        text,
        reply_markup=admin_post_manage_kb(post_id, row["user_id"])
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_hide_post:"))
async def admin_hide_post_direct(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])
    row = get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    await remove_post_from_channel(callback.bot, row)

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (STATUS_INACTIVE, now_ts(), post_id)
        )

    try:
        await callback.bot.send_message(
            row["user_id"],
            f"⚠️ Ваше объявление ID {post_id} скрыто администратором."
        )
    except Exception:
        pass

    await callback.message.answer(f"❌ Объявление {post_id} скрыто.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_delete_post:"))
async def admin_delete_post_direct(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    post_id = int(callback.data.split(":")[1])
    row = get_post(post_id)

    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    await remove_post_from_channel(callback.bot, row)

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE posts SET status=?, updated_at=? WHERE id=?",
            (STATUS_DELETED, now_ts(), post_id)
        )

    try:
        await callback.bot.send_message(
            row["user_id"],
            f"🗑 Ваше объявление ID {post_id} удалено администратором."
        )
    except Exception:
        pass

    await callback.message.answer(f"🗑 Объявление {post_id} удалено.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_ban_user:"))
async def admin_ban_user_direct(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    ban_user(user_id)

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
    await callback.message.answer("Введите USER_ID пользователя:")
    await callback.answer()


@router.message(AdminFlow.user_lookup)
async def admin_user_lookup_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return

    text = (message.text or "").strip()
    if not text.isdigit():
        await message.answer("Введите корректный USER_ID числом.")
        return

    user_id = int(text)
    profile = get_user_profile(user_id)
    user = profile["user"]

    if not user:
        await message.answer("Пользователь не найден.")
        await state.clear()
        return

    avg_rating, reviews_count = user_rating_summary(user_id)

    text = (
        f"👤 <b>Профиль пользователя</b>\n\n"
        f"<b>USER_ID:</b> {user_id}\n"
        f"<b>Username:</b> @{html.escape(user['username']) if user['username'] else 'нет'}\n"
        f"<b>Имя:</b> {html.escape(user['full_name'] or 'не указано')}\n"
        f"<b>Верификация:</b> {'да' if user['is_verified'] else 'нет'}\n"
        f"<b>Бан:</b> {'да' if user['is_banned'] else 'нет'}\n"
        f"<b>Объявлений всего:</b> {profile['posts_count']}\n"
        f"<b>Активных объявлений:</b> {profile['active_posts']}\n"
        f"<b>Завершенных сделок:</b> {profile['completed_deals']}\n"
        f"<b>Жалоб на пользователя:</b> {profile['complaints_received']}\n"
        f"<b>Рейтинг:</b> {avg_rating:.1f} ({reviews_count} {reviews_word(reviews_count)})\n"
    )

    await message.answer(
        text,
        reply_markup=admin_user_actions_kb(user_id, bool(user["is_verified"]), bool(user["is_banned"]))
    )
    await state.clear()


@router.callback_query(F.data.startswith("admin_user:"))
async def admin_open_user_profile(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    profile = get_user_profile(user_id)
    user = profile["user"]

    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return

    avg_rating, reviews_count = user_rating_summary(user_id)

    text = (
        f"👤 <b>Профиль пользователя</b>\n\n"
        f"<b>USER_ID:</b> {user_id}\n"
        f"<b>Username:</b> @{html.escape(user['username']) if user['username'] else 'нет'}\n"
        f"<b>Имя:</b> {html.escape(user['full_name'] or 'не указано')}\n"
        f"<b>Верификация:</b> {'да' if user['is_verified'] else 'нет'}\n"
        f"<b>Бан:</b> {'да' if user['is_banned'] else 'нет'}\n"
        f"<b>Объявлений всего:</b> {profile['posts_count']}\n"
        f"<b>Активных объявлений:</b> {profile['active_posts']}\n"
        f"<b>Завершенных сделок:</b> {profile['completed_deals']}\n"
        f"<b>Жалоб на пользователя:</b> {profile['complaints_received']}\n"
        f"<b>Рейтинг:</b> {avg_rating:.1f} ({reviews_count} {reviews_word(reviews_count)})\n"
    )

    await callback.message.answer(
        text,
        reply_markup=admin_user_actions_kb(user_id, bool(user["is_verified"]), bool(user["is_banned"]))
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin_user_verify:"))
async def admin_user_verify_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    verify_user(user_id)
    await callback.message.answer(f"✅ Пользователь {user_id} верифицирован.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_user_unverify:"))
async def admin_user_unverify_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    unverify_user(user_id)
    await callback.message.answer(f"↩️ Верификация пользователя {user_id} снята.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_user_ban:"))
async def admin_user_ban_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    ban_user(user_id)
    await callback.message.answer(f"🚫 Пользователь {user_id} забанен.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_user_unban:"))
async def admin_user_unban_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    unban_user(user_id)
    await callback.message.answer(f"♻️ Пользователь {user_id} разбанен.")
    await callback.answer()

@router.callback_query(F.data == "admin:complaints")
async def admin_complaints_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    complaints = get_recent_complaints(20)
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
        row = get_post(post_id)

        if not row:
            await callback.answer("Объявление не найдено", show_alert=True)
            return

        await callback.message.answer(
            post_text(row),
            reply_markup=admin_post_actions_kb(post_id)
        )
        await callback.answer()

    except Exception as e:
        print(f"ADMIN COMPLAINT OPEN POST ERROR: {e}")
        await callback.answer("Ошибка при открытии объявления", show_alert=True)


@router.callback_query(F.data.startswith("admincomplaint_hidepost:"))
async def admin_complaint_hide_post(callback: CallbackQuery):
    try:
        if not is_admin(callback.from_user.id):
            await callback.answer("Нет доступа", show_alert=True)
            return

        post_id = int(callback.data.split(":")[1])
        row = get_post(post_id)

        if not row:
            await callback.answer("Объявление не найдено", show_alert=True)
            return

        await remove_post_from_channel(callback.bot, row)

        with closing(connect_db()) as conn, conn:
            conn.execute(
                "UPDATE posts SET status=?, updated_at=? WHERE id=?",
                (STATUS_INACTIVE, now_ts(), post_id)
            )

        try:
            await callback.bot.send_message(
                row["user_id"],
                f"⚠️ Ваше объявление ID {post_id} скрыто администратором."
            )
        except Exception:
            pass

        await callback.message.answer(f"❌ Объявление {post_id} скрыто.")
        await callback.answer()

    except Exception as e:
        print(f"ADMIN COMPLAINT HIDE POST ERROR: {e}")
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
        ban_user(user_id)

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
        print(f"ADMIN COMPLAINT BAN USER ERROR: {e}")
        await callback.answer("Ошибка при бане пользователя", show_alert=True)


@router.callback_query(F.data.startswith("admincomplaint_done:"))
async def admin_complaint_done(callback: CallbackQuery):
    try:
        if not is_admin(callback.from_user.id):
            await callback.answer("Нет доступа", show_alert=True)
            return

        complaint_id = int(callback.data.split(":")[1])

        with closing(connect_db()) as conn, conn:
            conn.execute("DELETE FROM complaints WHERE id=?", (complaint_id,))

        await callback.message.answer(f"✅ Жалоба #{complaint_id} обработана.")
        await callback.answer()

    except Exception as e:
        print(f"ADMIN COMPLAINT DONE ERROR: {e}")
        await callback.answer("Ошибка при обработке жалобы", show_alert=True)


@router.callback_query(F.data == "admin:bump_orders")
async def admin_bump_orders_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    orders = get_pending_bump_orders(20)
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

    with closing(connect_db()) as conn, conn:
        order = conn.execute("SELECT * FROM bump_orders WHERE id=?", (order_id,)).fetchone()
        if not order:
            await callback.answer("Заказ не найден", show_alert=True)
            return

        if order["status"] == "paid":
            await callback.answer("Уже подтверждено", show_alert=True)
            return

        conn.execute(
            "UPDATE bump_orders SET status='paid', paid_at=? WHERE id=?",
            (now_ts(), order_id)
        )
        conn.execute(
            "UPDATE posts SET bumped_at=?, updated_at=? WHERE id=?",
            (now_ts(), now_ts(), order["post_id"])
        )

    try:
        await callback.bot.send_message(
            order["user_id"],
            f"✅ Оплата по заказу {order_id} подтверждена.\nВаше объявление поднято выше."
        )
    except Exception:
        pass

    await callback.message.answer(f"✅ Заказ {order_id} подтвержден.")
    await callback.answer()


@router.callback_query(F.data.startswith("admin_bump_reject:"))
async def admin_bump_reject_btn(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    order_id = int(callback.data.split(":")[1])

    with closing(connect_db()) as conn, conn:
        order = conn.execute("SELECT * FROM bump_orders WHERE id=?", (order_id,)).fetchone()
        if not order:
            await callback.answer("Заказ не найден", show_alert=True)
            return

        conn.execute(
            "UPDATE bump_orders SET status='rejected' WHERE id=?",
            (order_id,)
        )

    try:
        await callback.bot.send_message(
            order["user_id"],
            f"❌ Заявка на поднятие {order_id} отклонена."
        )
    except Exception:
        pass

    await callback.message.answer(f"❌ Заказ {order_id} отклонен.")
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

        await state.clear()
        await state.set_state(ContactFlow.message_text)
        await state.update_data(
            post_id=post_id,
            target_user_id=target_user_id,
            deal_id=deal_id
        )

        await callback.message.answer(
            "💬 Напишите сообщение — я отправлю его собеседнику через бота."
        )
        await callback.answer()

    except Exception as e:
        print(f"REPLY_CONTACT_HANDLER ERROR: {e}")
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
        return

    if not text:
        await message.answer("Сообщение не должно быть пустым.")
        return

    if target_user_id == message.from_user.id:
        await message.answer("Нельзя отправить сообщение самому себе.")
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

        with closing(connect_db()) as conn, conn:
            # просто логируем факт сообщения
            conn.execute(
                """
                INSERT INTO dialogs (post_id, owner_user_id, requester_user_id, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (post_id, target_user_id, message.from_user.id, now_ts())
            )

        await message.answer(
            "✅ Сообщение отправлено.",
            reply_markup=main_menu(message.from_user.id)
        )

    except Exception as e:
        print(f"RELAY MESSAGE ERROR: {e}")
        await message.answer(
            "Не удалось отправить сообщение. Возможно, пользователь ещё не запускал бот.",
            reply_markup=main_menu(message.from_user.id)
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

    request_id = ensure_deal_request(
        post_id=post_id,
        owner_user_id=owner_id,
        requester_user_id=requester_id
    )

    try:
        await callback.bot.send_message(
            owner_id,
            f"🤝 Пользователь предложил перейти в сделку по объявлению ID {post_id}.\n\n"
            f"Пользователь: {html.escape(callback.from_user.full_name or 'Пользователь')}"
            + (f" (@{html.escape(callback.from_user.username)})" if callback.from_user.username else ""),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Принять", callback_data=f"deal_request_accept:{request_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"deal_request_decline:{request_id}")
                ]
            ])
        )
    except Exception:
        pass

    await callback.message.answer("Заявка на сделку отправлена владельцу.")
    await callback.answer("Готово")


@router.callback_query(F.data.startswith("deal_request_accept:"))
async def deal_request_accept_handler(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[1])
    req = get_deal_request(request_id)

    if not req or req["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    row = get_post(req["post_id"])
    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return

    # если по объявлению уже есть активная сделка — вторую создавать нельзя
    existing_deal = get_active_deal_by_post(req["post_id"])
    if existing_deal:
        await callback.answer("По этому объявлению уже есть активная сделка", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        # повторно читаем заявку внутри транзакции
        req_db = conn.execute("""
            SELECT *
            FROM deal_requests
            WHERE id=?
        """, (request_id,)).fetchone()

        if not req_db:
            await callback.answer("Заявка не найдена", show_alert=True)
            return

        if req_db["status"] != DEAL_REQUEST_PENDING:
            await callback.answer("Эта заявка уже обработана", show_alert=True)
            return

        # ещё раз проверяем сделку внутри транзакции
        existing_deal_db = conn.execute("""
            SELECT id
            FROM deals
            WHERE post_id=?
              AND status IN (?, ?, ?, ?, ?)
            ORDER BY id DESC
            LIMIT 1
        """, (
            req["post_id"],
            DEAL_ACCEPTED,
            DEAL_COMPLETED_BY_OWNER,
            DEAL_COMPLETED_BY_REQUESTER,
            DEAL_DISPUTE_OPEN,
            DEAL_DISPUTE_WAITING
        )).fetchone()

        if existing_deal_db:
            await callback.answer("По этому объявлению уже есть активная сделка", show_alert=True)
            return

        conn.execute("""
            UPDATE deal_requests
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_REQUEST_ACCEPTED, now_ts(), request_id))

        cur = conn.execute("""
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

        conn.execute("""
            UPDATE posts
            SET status=?, updated_at=?
            WHERE id=?
        """, (STATUS_INACTIVE, now_ts(), req["post_id"]))

        # все остальные pending-заявки по этому объявлению автоматически закрываем
        conn.execute("""
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

    await remove_post_from_channel(callback.bot, row)

    try:
        await callback.bot.send_message(
            req["requester_user_id"],
            f"✅ Ваша заявка принята. Сделка создана по объявлению ID {req['post_id']}.\n\n"
            "Теперь она доступна во вкладке '🤝 Мои сделки'."
        )
    except Exception:
        pass

    await callback.message.answer(
        f"✅ Сделка создана.\n"
        f"Объявление #{req['post_id']} снято из канала и переведено в процесс сделки.\n"
        f"ID сделки: {deal_id}"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("deal_request_decline:"))
async def deal_request_decline_handler(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[1])
    req = get_deal_request(request_id)

    if not req or req["owner_user_id"] != callback.from_user.id:
        await callback.answer("Нет доступа", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute("""
            UPDATE deal_requests
            SET status=?, updated_at=?
            WHERE id=?
        """, (DEAL_REQUEST_DECLINED, now_ts(), request_id))

    try:
        await callback.bot.send_message(
            req["requester_user_id"],
            f"❌ Ваша заявка на сделку по объявлению ID {req['post_id']} отклонена.\n"
            "Само объявление остается активным."
        )
    except Exception:
        pass

    await callback.message.answer(
        "❌ Заявка отклонена.\n"
        "Объявление остается активным."
    )
    await callback.answer()


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
        "Никогда не переводите предоплату незнакомым людям."
    )

    await callback.answer()

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
        await callback.message.answer(
            dispute_text(existing),
            reply_markup=dispute_actions_kb(existing, callback.from_user.id)
        )
        await callback.answer("Спор уже открыт")
        return

    other_user_id = deal["requester_user_id"] if callback.from_user.id == deal["owner_user_id"] else deal["owner_user_id"]

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

    dispute_id = create_dispute(
        deal_id=deal_id,
        opened_by_user_id=message.from_user.id,
        against_user_id=against_user_id,
        reason_text=reason_text[:1500]
    )

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE deals SET status=?, updated_at=? WHERE id=?",
            (DEAL_DISPUTE_WAITING, now_ts(), deal_id)
        )

    dispute = get_dispute(dispute_id)

    try:
        await message.bot.send_message(
            against_user_id,
            "⚠️ По одной из ваших сделок открыт спор.\n\n"
            f"{dispute_text(dispute)}\n\n"
            "Пожалуйста, ответьте в установленный срок.",
            reply_markup=dispute_actions_kb(dispute, against_user_id)
        )
    except Exception as e:
        print(f"DISPUTE NOTIFY TARGET ERROR: {e}")

    await message.answer(
        "✅ Спор открыт.\n\n"
        f"{dispute_text(dispute)}",
        reply_markup=dispute_actions_kb(dispute, message.from_user.id)
    )
    await state.clear()


@router.callback_query(F.data.startswith("dispute_reply:"))
async def dispute_reply_handler(callback: CallbackQuery, state: FSMContext):
    dispute_id = int(callback.data.split(":")[1])
    dispute = get_dispute(dispute_id)

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
    dispute = get_dispute(dispute_id)

    if not dispute:
        await message.answer("Спор не найден.")
        await state.clear()
        return

    save_dispute_response(dispute_id, response_text[:1500])

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE deals SET status=?, updated_at=? WHERE id=?",
            (DEAL_DISPUTE_OPEN, now_ts(), dispute["deal_id"])
        )

    updated_dispute = get_dispute(dispute_id)

    try:
        await message.bot.send_message(
            dispute["opened_by_user_id"],
            "📩 Вторая сторона ответила по спору.\n\n"
            f"{dispute_text(updated_dispute)}\n\n"
            "Выберите, решена ли проблема.",
            reply_markup=dispute_actions_kb(updated_dispute, dispute["opened_by_user_id"])
        )
    except Exception as e:
        print(f"DISPUTE NOTIFY OPENER ERROR: {e}")

    await message.answer(
        "✅ Ваш ответ отправлен.\n\n"
        "Теперь ожидаем решения первой стороны."
    )
    await state.clear()


@router.callback_query(F.data.startswith("dispute_resolve:"))
async def dispute_resolve_handler(callback: CallbackQuery):
    dispute_id = int(callback.data.split(":")[1])
    dispute = get_dispute(dispute_id)

    if not dispute:
        await callback.answer("Спор не найден", show_alert=True)
        return

    if callback.from_user.id != dispute["opened_by_user_id"]:
        await callback.answer("Нет доступа", show_alert=True)
        return

    deal = get_deal(dispute["deal_id"])
    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE disputes SET status=?, updated_at=? WHERE id=?",
            (DISPUTE_RESOLVED, now_ts(), dispute_id)
        )
        conn.execute(
            "UPDATE deals SET status=?, owner_confirmed=1, requester_confirmed=1, updated_at=?, completed_at=? WHERE id=?",
            (DEAL_COMPLETED, now_ts(), now_ts(), dispute["deal_id"])
        )

    completed_deal = get_deal(dispute["deal_id"])
    updated_dispute = get_dispute(dispute_id)

    await callback.message.answer(
        "✅ <b>Спор решен</b>\n\n"
        "Сделка завершена по соглашению сторон.\n"
        "Теперь вы можете оставить отзыв о второй стороне.",
        reply_markup=deal_open_kb(completed_deal, callback.from_user.id)
    )

    try:
        await callback.bot.send_message(
            dispute["against_user_id"],
            "✅ <b>Спор по сделке решен</b>\n\n"
            "Сделка завершена по соглашению сторон.\n"
            "Теперь вы можете оставить отзыв о второй стороне.",
            reply_markup=deal_open_kb(completed_deal, dispute["against_user_id"])
        )
    except Exception as e:
        print(f"DISPUTE RESOLVE NOTIFY ERROR: {e}")

    await callback.message.answer(
        f"✅ <b>Сделка завершена</b>\n\n{dispute_text(updated_dispute)}"
    )

    await callback.answer()
    

@router.callback_query(F.data.startswith("dispute_unresolved:"))
async def dispute_unresolved_handler(callback: CallbackQuery):
    dispute_id = int(callback.data.split(":")[1])
    dispute = get_dispute(dispute_id)

    if not dispute:
        await callback.answer("Спор не найден", show_alert=True)
        return

    if callback.from_user.id != dispute["opened_by_user_id"]:
        await callback.answer("Нет доступа", show_alert=True)
        return

    deal = get_deal(dispute["deal_id"])
    if not deal:
        await callback.answer("Сделка не найдена", show_alert=True)
        return

    with closing(connect_db()) as conn, conn:
        conn.execute(
            "UPDATE disputes SET status=?, updated_at=? WHERE id=?",
            (DISPUTE_CLOSED_UNRESOLVED, now_ts(), dispute_id)
        )

        conn.execute(
            "UPDATE deals SET status=?, updated_at=? WHERE id=?",
            (DEAL_FAILED, now_ts(), dispute["deal_id"])
        )

        # Санкция только стороне, против которой был спор
        conn.execute("""
            UPDATE users
            SET failed_dispute_count = COALESCE(failed_dispute_count, 0) + 1
            WHERE user_id=?
        """, (dispute["against_user_id"],))

    updated_dispute = get_dispute(dispute_id)

    # Уведомление потерпевшему / открывшему спор
    await callback.message.answer(
        "❌ <b>Спор закрыт без решения</b>\n\n"
        "Сделка признана неуспешной и завершена внутри бота.\n\n"
        "<b>Как сервис реагирует на такую ситуацию:</b>\n"
        "• пользователю, против которого спор закрыт без решения, в профиле добавляется отметка "
        "<b>«⚠️ Были спорные сделки»</b>\n"
        "• это влияет на доверие других пользователей к его профилю\n"
        "• если пользователь системно игнорирует споры и не отвечает в срок — сервис ограничивает его аккаунт\n\n"
        "Что можно сделать дальше:\n"
        "• оставить отзыв о второй стороне\n"
        "• сохранить переписку и детали ситуации\n"
        "• при необходимости связаться с администратором\n"
        "• если отправка или получение всё ещё актуальны — создать новое объявление",
        reply_markup=dispute_failed_opened_by_kb(deal["id"])
    )

    # Уведомление второй стороне / стороне, против которой был спор
    try:
        await callback.bot.send_message(
            dispute["against_user_id"],
            "❌ <b>Спор по сделке закрыт без решения</b>\n\n"
            "Сделка признана неуспешной и завершена внутри бота.\n\n"
            "В вашем профиле будет отображаться отметка "
            "<b>«⚠️ Были спорные сделки»</b>, так как спор был закрыт без решения не в вашу пользу.\n"
            "Если пользователь системно игнорирует споры и не отвечает в срок — сервис ограничивает аккаунт.\n\n"
            "Что можно сделать дальше:\n"
            "• оставить отзыв о второй стороне\n"
            "• при необходимости связаться с администратором",
            reply_markup=dispute_failed_against_kb(deal["id"])
        )
    except Exception as e:
        print(f"DISPUTE UNRESOLVED NOTIFY ERROR: {e}")

    await callback.message.answer(
        f"❌ <b>Сделка завершилась неуспешно</b>\n\n{dispute_text(updated_dispute)}"
    )

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


@router.callback_query(F.data.startswith("mydeal:"))
async def open_my_deal(callback: CallbackQuery):
    try:
        deal_id = int(callback.data.split(":")[1])
        deal = get_deal(deal_id)

        if not deal:
            await callback.answer("Сделка не найдена", show_alert=True)
            return

        if callback.from_user.id not in (
            deal["owner_user_id"],
            deal["requester_user_id"]
        ):
            await callback.answer("Нет доступа", show_alert=True)
            return

        route = deal_title(deal)
        role = "владелец объявления" if callback.from_user.id == deal["owner_user_id"] else "откликнувшийся пользователь"

        text = (
            f"🤝 <b>{html.escape(route)}</b>\n\n"
            f"<b>ID сделки:</b> {deal['id']}\n"
            f"<b>ID объявления:</b> {deal['post_id']}\n"
            f"<b>Ваша роль:</b> {role}\n"
            f"<b>Статус:</b> {format_deal_status(deal['status'])}"
        )

        dispute = get_open_dispute_by_deal(deal_id)
        if dispute:
            text += "\n\n" + dispute_text(dispute)
            kb = dispute_actions_kb(dispute, callback.from_user.id)
        else:
            kb = deal_open_kb(deal, callback.from_user.id)

        await callback.message.answer(text, reply_markup=kb)
        await callback.answer()

    except Exception as e:
        print(f"DEAL OPEN ERROR: {e}")
        await callback.answer("Ошибка открытия сделки", show_alert=True)
    

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
