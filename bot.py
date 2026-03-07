import asyncio
import html
import os
import sqlite3
import time
from contextlib import closing
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
BOT_USERNAME = os.getenv("BOT_USERNAME", "poputchik_kitay_laovaev_bot").lstrip("@")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
BUMP_PRICE_TEXT = os.getenv(
    "BUMP_PRICE_TEXT",
    "Площадка не принимает оплату. Пользователи договариваются между собой напрямую."
)
MAX_ACTIVE_POSTS_PER_USER = int(os.getenv("MAX_ACTIVE_POSTS_PER_USER", "10"))
MIN_SECONDS_BETWEEN_ACTIONS = int(os.getenv("MIN_SECONDS_BETWEEN_ACTIONS", "3"))
POST_TTL_DAYS = int(os.getenv("POST_TTL_DAYS", "14"))
MATCH_NOTIFY_LIMIT = int(os.getenv("MATCH_NOTIFY_LIMIT", "5"))

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


def bot_link(start_param: Optional[str] = None) -> str:
    if start_param:
        return f"https://t.me/{BOT_USERNAME}?start={start_param}"
    return f"https://t.me/{BOT_USERNAME}"


def share_post_link(post_id: int) -> str:
    return bot_link(f"post_{post_id}")


def format_ts_ago(ts: int) -> str:
    diff = max(0, now_ts() - ts)
    if diff < 60:
        return "только что"
    if diff < 3600:
        mins = diff // 60
        return f"{mins} мин. назад"
    if diff < 86400:
        hrs = diff // 3600
        return f"{hrs} ч. назад"
    days = diff // 86400
    return f"{days} дн. назад"


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

        CREATE TABLE IF NOT EXISTS match_notifications (
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
        [KeyboardButton(text="🔥 Популярные маршруты"), KeyboardButton(text="🆕 Новые объявления")],
        [KeyboardButton(text="🔔 Подписки"), KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="🆘 Жалоба"), KeyboardButton(text="⭐ Оставить отзыв")],
        [KeyboardButton(text="💰 Поднять объявление"), KeyboardButton(text="ℹ️ Помощь")],
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
            InlineKeyboardButton(text="👀 Совпадения", callback_data=f"matches:{post_id}")
        ],
        [
            InlineKeyboardButton(text="📤 Поделиться", callback_data=f"share:{post_id}")
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
    second_button_text = "📦 Мне нужно отправить"
    second_button_url = bot_link("parcel")
    if post_type == TYPE_PARCEL:
        second_button_text = "✈️ Могу взять"
        second_button_url = bot_link("trip")

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Открыть бота", url=bot_link())],
            [InlineKeyboardButton(text=second_button_text, url=second_button_url)],
            [InlineKeyboardButton(text="📤 Поделиться", url=share_post_link(post_id))]
        ]
    )


def subscription_actions_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔔 Подписаться на маршрут", callback_data="sub:new")],
        [InlineKeyboardButton(text="📋 Мои подписки", callback_data="sub:list")]
    ])


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
                is_anonymous_contact, channel_message_id, created_at, updated_at,
                bumped_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user_id, data["post_type"], data["from_country"], data.get("from_city"),
            data["to_country"], data.get("to_city"), data.get("travel_date"),
            data.get("weight_kg"), data["description"], data.get("contact_note"),
            STATUS_PENDING if ADMIN_IDS else STATUS_ACTIVE, None, ts, ts, ts, expires_at
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
            reply_markup=public_post_kb(post_id, row["user_id"], row["post_type"])
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


def search_matches(post_type: str, from_country: str, to_country: str, exclude_user_id: Optional[int] = None):
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
    params = [target_type, from_country, to_country, now_ts()]
    if exclude_user_id is not None:
        query += " AND p.user_id != ?"
        params.append(exclude_user_id)
    query += " ORDER BY COALESCE(p.bumped_at, p.created_at) DESC LIMIT 20"

    with closing(connect_db()) as conn:
        return conn.execute(query, params).fetchall()


def match_already_notified(post_a_id: int, post_b_id: int) -> bool:
    a, b = sorted([post_a_id, post_b_id])
    with closing(connect_db()) as conn:
        row = conn.execute("""
            SELECT id FROM match_notifications
            WHERE post_a_id=? AND post_b_id=?
        """, (a, b)).fetchone()
        return row is not None


def mark_match_notified(post_a_id: int, post_b_id: int):
    a, b = sorted([post_a_id, post_b_id])
    with closing(connect_db()) as conn, conn:
        conn.execute("""
            INSERT OR IGNORE INTO match_notifications (post_a_id, post_b_id, created_at)
            VALUES (?, ?, ?)
        """, (a, b, now_ts()))


async def notify_match_users(bot: Bot, new_post_id: int):
    new_row = get_post(new_post_id)
    if not new_row or new_row["status"] != STATUS_ACTIVE:
        return

    matches = search_matches(
        new_row["post_type"],
        new_row["from_country"],
        new_row["to_country"],
        exclude_user_id=new_row["user_id"]
    )

    if not matches:
        return

    for match in matches[:MATCH_NOTIFY_LIMIT]:
        if match_already_notified(new_row["id"], match["id"]):
            continue

        try:
            await bot.send_message(
                new_row["user_id"],
                "🔔 Найдено совпадение по вашему объявлению!\n\n"
                + post_text(match),
                reply_markup=public_post_kb(match["id"], match["user_id"], match["post_type"])
            )
        except Exception:
            pass

        try:
            await bot.send_message(
                match["user_id"],
                "🔔 Появилось новое совпадение по вашему маршруту!\n\n"
                + post_text(new_row),
                reply_markup=public_post_kb(new_row["id"], new_row["user_id"], new_row["post_type"])
            )
        except Exception:
            pass

        mark_match_notified(new_row["id"], match["id"])


async def notify_subscribers(bot: Bot, post_id: int):
    row = get_post(post_id)
    if not row or row["status"] != STATUS_ACTIVE:
        return

    target_type = row["post_type"]
    with closing(connect_db()) as conn:
        subscribers = conn.execute("""
            SELECT * FROM route_subscriptions
            WHERE post_type=? AND from_country=? AND to_country=? AND user_id != ?
            ORDER BY created_at DESC
            LIMIT 50
        """, (target_type, row["from_country"], row["to_country"], row["user_id"])).fetchall()

    for sub in subscribers:
        try:
            await bot.send_message(
                sub["user_id"],
                "🔔 По вашей подписке появилось новое объявление:\n\n"
                + post_text(row),
                reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
            )
        except Exception:
            pass


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
                    except Exception:
                        pass
        except Exception as e:
            print(f"EXPIRE LOOP ERROR: {e}")

        await asyncio.sleep(300)


def get_recent_posts(limit: int = 10) -> List[sqlite3.Row]:
    with closing(connect_db()) as conn:
        return conn.execute("""
            SELECT p.*, u.username, u.full_name
            FROM posts p
            LEFT JOIN users u ON u.user_id = p.user_id
            WHERE p.status='active'
              AND (p.expires_at IS NULL OR p.expires_at > ?)
            ORDER BY COALESCE(p.bumped_at, p.created_at) DESC
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
            ORDER BY cnt DESC, MAX(created_at) DESC
            LIMIT ?
        """, (now_ts(), limit)).fetchall()


def service_stats() -> dict:
    with closing(connect_db()) as conn:
        users = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        active_posts = conn.execute("""
            SELECT COUNT(*) AS c FROM posts
            WHERE status='active' AND (expires_at IS NULL OR expires_at > ?)
        """, (now_ts(),)).fetchone()["c"]
        trips = conn.execute("""
            SELECT COUNT(*) AS c FROM posts
            WHERE status='active' AND post_type='trip' AND (expires_at IS NULL OR expires_at > ?)
        """, (now_ts(),)).fetchone()["c"]
        parcels = conn.execute("""
            SELECT COUNT(*) AS c FROM posts
            WHERE status='active' AND post_type='parcel' AND (expires_at IS NULL OR expires_at > ?)
        """, (now_ts(),)).fetchone()["c"]
        top_route = conn.execute("""
            SELECT from_country, to_country, COUNT(*) AS cnt
            FROM posts
            WHERE status='active' AND (expires_at IS NULL OR expires_at > ?)
            GROUP BY from_country, to_country
            ORDER BY cnt DESC
            LIMIT 1
        """, (now_ts(),)).fetchone()

    return {
        "users": int(users or 0),
        "active_posts": int(active_posts or 0),
        "trips": int(trips or 0),
        "parcels": int(parcels or 0),
        "top_route": top_route,
    }


@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    upsert_user(message)
    await state.clear()

    start_arg = ""
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) > 1:
        start_arg = parts[1].strip().lower()

    text = (
        "👋 Привет.\n\n"
        "Это <b>Попутчик Китай</b> — бот для передачи посылок через попутчиков.\n\n"
        "Здесь можно:\n"
        "• добавить поездку\n"
        "• добавить посылку\n"
        "• найти совпадения\n"
        "• подписаться на маршрут\n"
        "• смотреть новые объявления\n"
        "• делиться объявлениями\n"
        "• писать владельцу объявления\n"
        "• оставлять отзывы\n\n"
        "⚠️ Бот не принимает оплату и не выступает посредником.\n"
        "Пользователи договариваются напрямую."
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
                    "Открыто объявление по ссылке:",
                    reply_markup=main_menu(message.from_user.id)
                )
                await message.answer(
                    post_text(row),
                    reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
                )


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
    await message.answer("Дата поездки/отправки. Например: 15 марта 2026. Если дата не точная — напиши как удобно.")


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
                await notify_match_users(bot, post_id)
            except Exception as e:
                print(f"NOTIFY_MATCH_USERS ERROR: {e}")

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
        await notify_match_users(bot, post_id)
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


@router.callback_query(F.data.startswith("share:"))
async def share_post_handler(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = get_post(post_id)
    if not row:
        await callback.answer("Объявление не найдено", show_alert=True)
        return
    await callback.message.answer(
        f"📤 Ссылка на объявление:\n{share_post_link(post_id)}\n\n"
        "Её можно отправить в Telegram, WeChat или чаты.",
        reply_markup=main_menu(callback.from_user.id)
    )
    await callback.answer("Ссылка готова")


@router.message(F.text == "💰 Поднять объявление")
async def bump_info(message: Message):
    await message.answer(BUMP_PRICE_TEXT + "\n\nОткрой 'Мои объявления' и нажми 'Поднять' у нужного объявления.", reply_markup=main_menu(message.from_user.id))


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
    matches = search_matches(post_type, data["from_country"], country, exclude_user_id=callback.from_user.id)
    await state.clear()
    if not matches:
        await callback.message.answer("Совпадений пока нет.")
    else:
        await callback.message.answer(f"Найдено совпадений: {len(matches)}")
        for row in matches[:10]:
            await callback.message.answer(
                post_text(row),
                reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
            )
    await callback.answer()


@router.callback_query(F.data.startswith("matches:"))
async def matches_for_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    row = owner_only(callback, post_id)
    if not row:
        await callback.answer("Нет доступа", show_alert=True)
        return
    matches = search_matches(row["post_type"], row["from_country"], row["to_country"], exclude_user_id=callback.from_user.id)
    if not matches:
        await callback.message.answer("Совпадений пока нет.")
    else:
        await callback.message.answer(f"Найдено совпадений: {len(matches)}")
        for item in matches[:10]:
            await callback.message.answer(
                post_text(item),
                reply_markup=public_post_kb(item["id"], item["user_id"], item["post_type"])
            )
    await callback.answer()


@router.message(F.text == "🔥 Популярные маршруты")
async def popular_routes_handler(message: Message):
    routes = get_popular_routes(10)
    if not routes:
        await message.answer("Пока нет активных маршрутов.", reply_markup=main_menu(message.from_user.id))
        return

    text = ["🔥 <b>Популярные маршруты</b>\n"]
    for i, r in enumerate(routes, 1):
        text.append(f"{i}. {html.escape(r['from_country'])} → {html.escape(r['to_country'])} ({r['cnt']})")
    await message.answer("\n".join(text), reply_markup=main_menu(message.from_user.id))


@router.message(F.text == "🆕 Новые объявления")
async def new_posts_handler(message: Message):
    rows = get_recent_posts(10)
    if not rows:
        await message.answer("Пока нет новых активных объявлений.", reply_markup=main_menu(message.from_user.id))
        return

    await message.answer("🆕 <b>Новые объявления</b>", reply_markup=main_menu(message.from_user.id))
    for row in rows:
        await message.answer(
            post_text(row) + f"\n<b>Добавлено:</b> {format_ts_ago(row['created_at'])}",
            reply_markup=public_post_kb(row["id"], row["user_id"], row["post_type"])
        )


@router.message(F.text == "📊 Статистика")
async def stats_handler(message: Message):
    stats = service_stats()
    lines = [
        "📊 <b>Статистика сервиса</b>",
        f"Пользователей: {stats['users']}",
        f"Активных объявлений: {stats['active_posts']}",
        f"✈️ Попутчиков: {stats['trips']}",
        f"📦 Посылок: {stats['parcels']}",
    ]
    if stats["top_route"]:
        lines.append(
            f"🔥 Популярный маршрут: "
            f"{html.escape(stats['top_route']['from_country'])} → {html.escape(stats['top_route']['to_country'])} "
            f"({stats['top_route']['cnt']})"
        )
    await message.answer("\n".join(lines), reply_markup=main_menu(message.from_user.id))


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
    await notify_match_users(bot, post_id)
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

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
