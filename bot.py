"""
bot.py — Telegram Bot with full panel UI and extended admin controls.
- Auto‑approve toggle for join requests.
- Hierarchical roles: Superadmin → Admin → Subadmin.
- Message sequence addition via position‑first workflow + premium emoji warning.
- Daily morning/night scheduled messages + custom one‑time scheduling.
- Subadmins can approve requests, toggle auto‑approve, schedule messages,
  manage bot profile, create/send posts, reply to users, and broadcast,
  all with fine‑grained permissions.
- Broadcast runs in background → instant "Broadcast started" feedback;
  automatically removes blocked users. NO completion message is sent.
- Inline "Reply" button on forwarded user messages (only visible if allowed).
- New permissions: can_view_user_messages & can_reply_to_users.
- All datetime values stored as ISO strings.
"""

import asyncio
import logging
import sqlite3
import json
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from functools import partial

from dotenv import load_dotenv
import os

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    MessageEntity,
)
from telegram.error import TelegramError, Forbidden
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    filters,
    ContextTypes,
)

# ══════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════

load_dotenv()

BOT_TOKEN:       str = os.getenv("BOT_TOKEN", "")
SOURCE_CHAT_ID:  int = int(os.getenv("SOURCE_CHAT_ID", "0"))
ADMIN_ID:        int = int(os.getenv("ADMIN_ID", "0"))

DB_PATH:         str = "bot.db"

if not BOT_TOKEN:      raise ValueError("BOT_TOKEN not set in .env")
if not ADMIN_ID:       raise ValueError("ADMIN_ID not set in .env")

# ══════════════════════════════════════════════
# MINIMAL LOGGING CONFIGURATION
# ══════════════════════════════════════════════

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    level=logging.WARNING,
)

logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ══════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id    INTEGER PRIMARY KEY,
                first_seen DATE NOT NULL DEFAULT (DATE('now'))
            );
            CREATE TABLE IF NOT EXISTS subadmins (
                user_id  INTEGER PRIMARY KEY,
                added_at TIMESTAMP DEFAULT (DATETIME('now'))
            );
            CREATE TABLE IF NOT EXISTS state (
                user_id INTEGER PRIMARY KEY,
                action  TEXT NOT NULL,
                data    TEXT
            );
            CREATE TABLE IF NOT EXISTS pending_requests (
                user_id    INTEGER,
                chat_id    INTEGER,
                created_at TIMESTAMP DEFAULT (DATETIME('now')),
                PRIMARY KEY (user_id, chat_id)
            );
            CREATE TABLE IF NOT EXISTS subadmin_perms (
                user_id INTEGER PRIMARY KEY,
                can_stats INTEGER DEFAULT 1,
                can_manage_seq INTEGER DEFAULT 1,
                can_change_source INTEGER DEFAULT 0,
                can_set_post_button INTEGER DEFAULT 0,
                can_manage_subadmins INTEGER DEFAULT 0,
                can_test_sequence INTEGER DEFAULT 1,
                can_schedule INTEGER DEFAULT 1,
                can_approve_requests INTEGER DEFAULT 0,
                can_toggle_auto_approve INTEGER DEFAULT 0,
                can_manage_bot_profile INTEGER DEFAULT 0,
                can_create_post INTEGER DEFAULT 0,
                can_broadcast INTEGER DEFAULT 0,
                can_view_user_messages INTEGER DEFAULT 0,
                can_reply_to_users INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES subadmins(user_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS post_sequence (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                message_text TEXT,
                button_text TEXT,
                button_url TEXT
            );
            CREATE TABLE IF NOT EXISTS scheduled_daily (
                id INTEGER PRIMARY KEY CHECK (id IN (1,2)),
                time TEXT,
                msg_type TEXT,
                file_id TEXT,
                text TEXT,
                caption TEXT,
                chat_id INTEGER,
                enabled INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS scheduled_once (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                send_at TEXT,
                msg_type TEXT,
                file_id TEXT,
                text TEXT,
                caption TEXT,
                sent INTEGER DEFAULT 0
            );
        """)

        # --- Migration for messages table ---
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='messages'")
        if c.fetchone():
            c.execute("PRAGMA table_info(messages)")
            cols = [row[1] for row in c.fetchall()]
            if "msg_type" in cols or "message_id" not in cols:
                c.execute("DROP TABLE messages")
                logger.info("Dropped old messages table – recreating with new schema.")

        c.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                position   INTEGER NOT NULL UNIQUE,
                message_id INTEGER NOT NULL
            )
        """)

        # Add role column to subadmins if not exists
        try:
            c.execute("ALTER TABLE subadmins ADD COLUMN role TEXT DEFAULT 'subadmin'")
        except sqlite3.OperationalError:
            pass

        # --- Ensure ALL permission columns exist (for older databases) ---
        for col in PERMISSIONS:
            try:
                c.execute(f"ALTER TABLE subadmin_perms ADD COLUMN {col} INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass

        # --- Set proper defaults for any NULL permission column on existing subadmins ---
        for col, default in [
            ("can_stats", 1), ("can_manage_seq", 1), ("can_change_source", 0),
            ("can_set_post_button", 0), ("can_manage_subadmins", 0),
            ("can_test_sequence", 1), ("can_schedule", 1),
            ("can_approve_requests", 0), ("can_toggle_auto_approve", 0),
            ("can_manage_bot_profile", 0), ("can_create_post", 0),
            ("can_broadcast", 0), ("can_view_user_messages", 0),
            ("can_reply_to_users", 0)
        ]:
            c.execute(f"UPDATE subadmin_perms SET {col} = ? WHERE {col} IS NULL", (default,))

        # Default configs
        c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('source_chat_id', ?)",
                  (str(SOURCE_CHAT_ID),))
        c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('auto_approve', '0')")
        c.execute("INSERT OR IGNORE INTO post_sequence (id) VALUES (1)")
        c.execute("INSERT OR IGNORE INTO scheduled_daily (id, enabled) VALUES (1, 0)")
        c.execute("INSERT OR IGNORE INTO scheduled_daily (id, enabled) VALUES (2, 0)")

    logger.info("Database ready.")


# ── Users ──────────────────────────────────────

def db_upsert_user(user_id: int) -> bool:
    with get_conn() as conn:
        c = conn.cursor()
        return c.execute(
            "INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,)
        ).rowcount > 0

def db_total_users() -> int:
    with get_conn() as conn:
        c = conn.cursor()
        return c.execute("SELECT COUNT(*) FROM users").fetchone()[0]

def db_daily_users() -> int:
    with get_conn() as conn:
        c = conn.cursor()
        return c.execute(
            "SELECT COUNT(*) FROM users WHERE first_seen = ?",
            (date.today().isoformat(),)
        ).fetchone()[0]

def db_all_user_ids() -> list:
    with get_conn() as conn:
        c = conn.cursor()
        return [r["user_id"] for r in c.execute("SELECT user_id FROM users").fetchall()]

def db_remove_user(user_id: int) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))

def db_users_by_date_range(start: str = None, end: str = None) -> list:
    """Return user_ids filtered by first_seen (ISO strings)."""
    with get_conn() as conn:
        c = conn.cursor()
        if start and end:
            rows = c.execute(
                "SELECT user_id FROM users WHERE first_seen BETWEEN ? AND ?",
                (start, end)
            ).fetchall()
        elif start:
            rows = c.execute(
                "SELECT user_id FROM users WHERE first_seen >= ?", (start,)
            ).fetchall()
        else:
            rows = c.execute("SELECT user_id FROM users").fetchall()
        return [r["user_id"] for r in rows]


# ── Roles & Admins ─────────────────────────────

def is_main_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

def db_get_admin_role(user_id: int) -> str | None:
    with get_conn() as conn:
        c = conn.cursor()
        row = c.execute(
            "SELECT role FROM subadmins WHERE user_id = ?", (user_id,)
        ).fetchone()
        return row["role"] if row else None

def db_is_subadmin(user_id: int) -> bool:
    return db_get_admin_role(user_id) is not None

def db_is_admin(user_id: int) -> bool:
    return is_main_admin(user_id) or db_get_admin_role(user_id) == "admin"

def is_any_admin(user_id: int) -> bool:
    return is_main_admin(user_id) or db_is_subadmin(user_id)

def db_add_admin(user_id: int, role: str = "subadmin") -> bool:
    with get_conn() as conn:
        c = conn.cursor()
        try:
            c.execute(
                "INSERT INTO subadmins (user_id, role) VALUES (?, ?)",
                (user_id, role)
            )
            c.execute("INSERT INTO subadmin_perms (user_id) VALUES (?)", (user_id,))
            return True
        except sqlite3.IntegrityError:
            return False

def db_remove_subadmin(user_id: int) -> bool:
    with get_conn() as conn:
        c = conn.cursor()
        return c.execute(
            "DELETE FROM subadmins WHERE user_id = ?", (user_id,)
        ).rowcount > 0

def db_list_admins(role_filter: str = None) -> list:
    with get_conn() as conn:
        c = conn.cursor()
        if role_filter:
            return c.execute(
                "SELECT user_id, role FROM subadmins WHERE role = ?", (role_filter,)
            ).fetchall()
        return c.execute("SELECT user_id, role FROM subadmins").fetchall()

def db_get_all_admin_ids() -> list:
    ids = [ADMIN_ID]
    ids.extend(r["user_id"] for r in db_list_admins())
    return ids


# ── Subadmin Permissions ───────────────────────

PERMISSIONS = [
    "can_stats",
    "can_manage_seq",
    "can_change_source",
    "can_set_post_button",
    "can_manage_subadmins",
    "can_test_sequence",
    "can_schedule",
    "can_approve_requests",
    "can_toggle_auto_approve",
    "can_manage_bot_profile",
    "can_create_post",
    "can_broadcast",
    "can_view_user_messages",
    "can_reply_to_users",
]

PERM_DISPLAY = {
    "can_stats": "📊 Stats",
    "can_manage_seq": "📨 Manage Sequence",
    "can_change_source": "📡 Change Source",
    "can_set_post_button": "🔘 Post Button",
    "can_manage_subadmins": "👥 Manage Subadmins",
    "can_test_sequence": "📨 Test Sequence",
    "can_schedule": "⏰ Schedule Messages",
    "can_approve_requests": "✅ Approve Requests",
    "can_toggle_auto_approve": "🔄 Auto‑Approve Toggle",
    "can_manage_bot_profile": "🤖 Bot Profile",
    "can_create_post": "📝 Post Creator",
    "can_broadcast": "📢 Broadcast",
    "can_view_user_messages": "👁️ View User Messages",
    "can_reply_to_users": "↩️ Reply to Users",
}

def db_get_subadmin_perms(user_id: int) -> dict:
    with get_conn() as conn:
        c = conn.cursor()
        row = c.execute(
            "SELECT * FROM subadmin_perms WHERE user_id = ?", (user_id,)
        ).fetchone()
        if not row:
            return {}
        return {k: bool(row[k]) for k in row.keys() if k != "user_id"}

def db_set_subadmin_perm(user_id: int, perm: str, value: bool) -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            f"UPDATE subadmin_perms SET {perm} = ? WHERE user_id = ?",
            (int(value), user_id)
        )

def db_has_perm(user_id: int, perm: str) -> bool:
    if is_main_admin(user_id):
        return True
    perms = db_get_subadmin_perms(user_id)
    return perms.get(perm, False)


# ── Auto‑approve config ────────────────────────

def db_get_auto_approve() -> bool:
    with get_conn() as conn:
        c = conn.cursor()
        row = c.execute("SELECT value FROM config WHERE key = 'auto_approve'").fetchone()
        return row and row["value"] == "1"

def db_set_auto_approve(value: bool) -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "UPDATE config SET value = ? WHERE key = 'auto_approve'",
            ("1" if value else "0",)
        )


# ── Pending Join Requests ──────────────────────

def db_add_pending_request(user_id: int, chat_id: int) -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "INSERT OR IGNORE INTO pending_requests (user_id, chat_id) VALUES (?,?)",
            (user_id, chat_id)
        )

def db_get_pending_requests() -> list:
    with get_conn() as conn:
        c = conn.cursor()
        return c.execute(
            "SELECT user_id, chat_id FROM pending_requests"
        ).fetchall()

def db_clear_pending_requests() -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM pending_requests")


# ── Message sequence (message_id from source channel) ──

def db_add_message(message_id: int, position: int) -> bool:
    with get_conn() as conn:
        c = conn.cursor()
        try:
            c.execute(
                "INSERT INTO messages (message_id, position) VALUES (?, ?)",
                (message_id, position)
            )
            return True
        except sqlite3.IntegrityError:
            return False

def db_remove_message(message_id: int) -> bool:
    """Remove a message by its message_id."""
    with get_conn() as conn:
        c = conn.cursor()
        return c.execute(
            "DELETE FROM messages WHERE message_id = ?", (message_id,)
        ).rowcount > 0

def db_get_messages() -> list:
    with get_conn() as conn:
        c = conn.cursor()
        rows = c.execute("SELECT * FROM messages ORDER BY position ASC").fetchall()
        return [dict(row) for row in rows]

def db_reorder_message(message_id: int, new_position: int) -> bool:
    """Move a message (by message_id) to a new position."""
    with get_conn() as conn:
        c = conn.cursor()
        # temporarily move the target position out of the way
        c.execute(
            "UPDATE messages SET position = -1 WHERE position = ? AND message_id != ?",
            (new_position, message_id),
        )
        ok = c.execute(
            "UPDATE messages SET position = ? WHERE message_id = ?",
            (new_position, message_id),
        ).rowcount > 0
        c.execute("DELETE FROM messages WHERE position = -1")
        return ok


# ── Config ─────────────────────────────────────

def db_get_source_chat_id() -> int:
    with get_conn() as conn:
        c = conn.cursor()
        row = c.execute("SELECT value FROM config WHERE key = 'source_chat_id'").fetchone()
        return int(row["value"]) if row else SOURCE_CHAT_ID

def db_set_source_chat_id(chat_id: int) -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "UPDATE config SET value = ? WHERE key = 'source_chat_id'",
            (str(chat_id),)
        )


# ── Post‑sequence custom message ───────────────

def db_get_post_sequence() -> dict:
    with get_conn() as conn:
        c = conn.cursor()
        row = c.execute("SELECT message_text, button_text, button_url FROM post_sequence WHERE id = 1").fetchone()
        return dict(row) if row else {}

def db_set_post_sequence(message_text: str, button_text: str, button_url: str) -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "UPDATE post_sequence SET message_text = ?, button_text = ?, button_url = ? WHERE id = 1",
            (message_text, button_text, button_url)
        )


# ── Scheduled jobs ─────────────────────────────

def db_get_daily_job(job_id: int) -> dict:
    with get_conn() as conn:
        c = conn.cursor()
        row = c.execute("SELECT * FROM scheduled_daily WHERE id = ?", (job_id,)).fetchone()
        return dict(row) if row else {}

def db_set_daily_job(job_id: int, time_str: str, msg_type: str, file_id: str = None,
                     text: str = None, caption: str = None, chat_id: int = None) -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """UPDATE scheduled_daily SET time = ?, msg_type = ?, file_id = ?,
               text = ?, caption = ?, chat_id = ?, enabled = 1 WHERE id = ?""",
            (time_str, msg_type, file_id, text, caption, chat_id, job_id)
        )

def db_disable_daily_job(job_id: int) -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("UPDATE scheduled_daily SET enabled = 0 WHERE id = ?", (job_id,))

def db_add_once_job(chat_id: int, send_at: datetime, msg_type: str,
                    file_id: str = None, text: str = None, caption: str = None) -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """INSERT INTO scheduled_once (chat_id, send_at, msg_type, file_id, text, caption)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (chat_id, send_at.isoformat(), msg_type, file_id, text, caption)
        )

def db_get_pending_once_jobs(now: datetime) -> list:
    now_str = now.isoformat()
    with get_conn() as conn:
        c = conn.cursor()
        rows = c.execute(
            "SELECT * FROM scheduled_once WHERE sent = 0 AND send_at <= ?",
            (now_str,)
        ).fetchall()
        jobs = []
        for row in rows:
            job = dict(row)
            job["send_at"] = datetime.fromisoformat(job["send_at"])
            jobs.append(job)
        return jobs

def db_mark_once_sent(job_id: int) -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("UPDATE scheduled_once SET sent = 1 WHERE id = ?", (job_id,))


# ── State machine ──────────────────────────────

def db_set_state(user_id: int, action: str, data: str = "") -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO state (user_id, action, data) VALUES (?,?,?)",
            (user_id, action, data),
        )

def db_get_state(user_id: int) -> tuple:
    with get_conn() as conn:
        c = conn.cursor()
        row = c.execute(
            "SELECT action, data FROM state WHERE user_id = ?", (user_id,)
        ).fetchone()
        return (row["action"], row["data"]) if row else (None, None)

def db_clear_state(user_id: int) -> None:
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM state WHERE user_id = ?", (user_id,))


# ══════════════════════════════════════════════
# HELPER: CHECK FOR PREMIUM EMOJIS
# ══════════════════════════════════════════════

def has_premium_emoji(msg) -> bool:
    """Return True if the message contains any custom_emoji entities."""
    entities = list(msg.entities or []) + list(msg.caption_entities or [])
    for entity in entities:
        if entity.type == MessageEntity.CUSTOM_EMOJI:
            return True
    return False


# ══════════════════════════════════════════════
# ASYNC HELPER
# ══════════════════════════════════════════════

async def run(func, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(func, *args))


# ══════════════════════════════════════════════
# SEND STORED SCHEDULED MESSAGE (helper)
# ══════════════════════════════════════════════

async def send_stored_message(bot, chat_id: int, msg_data: dict) -> None:
    msg_type = msg_data["msg_type"]
    if msg_type == "text":
        await bot.send_message(chat_id, msg_data["text"])
    elif msg_type == "photo":
        await bot.send_photo(chat_id, msg_data["file_id"], caption=msg_data.get("caption"))
    elif msg_type == "video":
        await bot.send_video(chat_id, msg_data["file_id"], caption=msg_data.get("caption"))
    elif msg_type == "document":
        await bot.send_document(chat_id, msg_data["file_id"], caption=msg_data.get("caption"))
    elif msg_type == "audio":
        await bot.send_audio(chat_id, msg_data["file_id"], caption=msg_data.get("caption"))
    elif msg_type == "voice":
        await bot.send_voice(chat_id, msg_data["file_id"], caption=msg_data.get("caption"))
    else:
        logger.warning(f"Unknown message type: {msg_type}")


# ══════════════════════════════════════════════
# KEYBOARDS
# ══════════════════════════════════════════════

def admin_panel_kb() -> ReplyKeyboardMarkup:
    auto_status = "🔄 Auto‑Approve: " + ("ON ✅" if db_get_auto_approve() else "OFF ❌")
    return ReplyKeyboardMarkup(
        [
            ["📨 Test Sequence", "📊 Stats"],
            ["👑 Admins", "👥 Subadmins"],
            ["📨 Message Sequence", "✅ Approve All Requests"],
            ["📡 Change Source Channel", auto_status],
            ["🔘 Set Post Button", "🗑 Remove Post"],
            ["🌅 Schedule Morning", "🌙 Schedule Night"],
            ["⏰ Schedule Messages", "⚙️ Subadmin Permissions"],
            ["🤖 Bot Profile", "📝 Post Creator"],
            ["📢 Broadcast"],
        ],
        resize_keyboard=True,
    )

def subadmin_panel_kb(user_id: int) -> ReplyKeyboardMarkup:
    perms = db_get_subadmin_perms(user_id)
    role = db_get_admin_role(user_id)
    buttons = []
    if perms.get("can_test_sequence", False):
        buttons.append(["📨 Test Sequence"])
    if perms.get("can_stats", False):
        buttons.append(["📊 Stats"])
    if perms.get("can_manage_seq", False):
        buttons.append(["📨 Message Sequence"])
    if perms.get("can_approve_requests", False):
        buttons.append(["✅ Approve All Requests"])
    if perms.get("can_change_source", False):
        buttons.append(["📡 Change Source Channel"])
    if perms.get("can_toggle_auto_approve", False):
        auto_status = "🔄 Auto‑Approve: " + ("ON ✅" if db_get_auto_approve() else "OFF ❌")
        buttons.append([auto_status])
    if perms.get("can_set_post_button", False):
        buttons.append(["🔘 Set Post Button", "🗑 Remove Post"])
    if perms.get("can_schedule", False):
        buttons.append(["🌅 Schedule Morning", "🌙 Schedule Night"])
        buttons.append(["⏰ Schedule Messages"])
    if perms.get("can_manage_bot_profile", False):
        buttons.append(["🤖 Bot Profile"])
    if perms.get("can_create_post", False):
        buttons.append(["📝 Post Creator"])
    if perms.get("can_broadcast", False):
        buttons.append(["📢 Broadcast"])
    if role == "admin" and perms.get("can_manage_subadmins", False):
        buttons.append(["👥 Subadmins"])
    if not buttons:
        buttons = [["ℹ️ No permissions"]]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def bot_profile_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["✏️ Edit Name", "📝 Edit Description"],
            ["📝 Edit Short Description", "🖼 Set Profile Photo"],
            ["🔙 Back to Panel"],
        ],
        resize_keyboard=True,
    )

def sequence_panel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["➕ Add Message", "➖ Remove Message"],
            ["🔀 Reorder Message", "📄 List Messages"],
            ["🔙 Back to Panel"],
        ],
        resize_keyboard=True,
    )

def cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["❌ Cancel"]],
        resize_keyboard=True,
    )

def yes_no_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["✅ Yes", "❌ No"]],
        resize_keyboard=True,
    )

def staff_kb(user_id: int) -> ReplyKeyboardMarkup:
    return admin_panel_kb() if is_main_admin(user_id) else subadmin_panel_kb(user_id)


# ══════════════════════════════════════════════
# PANEL SENDER
# ══════════════════════════════════════════════

async def open_panel(update: Update, user_id: int, note: str = "") -> None:
    await run(db_clear_state, user_id)

    if is_main_admin(user_id):
        text = f"{note}\n\n👑 *SUPER ADMIN* — CHOOSE AN ACTION:" if note else "👑 *SUPER ADMIN* — CHOOSE AN ACTION:"
        kb   = admin_panel_kb()
    elif await run(db_is_subadmin, user_id):
        role = await run(db_get_admin_role, user_id)
        title = "ADMIN" if role == "admin" else "SUBADMIN"
        text = f"{note}\n\n🛠 *{title} PANEL* — CHOOSE AN ACTION:" if note else f"🛠 *{title} PANEL* — CHOOSE AN ACTION:"
        kb   = subadmin_panel_kb(user_id)
    else:
        return

    await update.message.reply_text(text.strip(), parse_mode="Markdown", reply_markup=kb)


# ══════════════════════════════════════════════
# /start
# ══════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return

    await run(db_upsert_user, user.id)
    await run(db_clear_state, user.id)

    if is_any_admin(user.id):
        await open_panel(update, user.id)
        return

    source_id = await run(db_get_source_chat_id)
    for row in await run(db_get_messages):
        try:
            await context.bot.copy_message(
                chat_id=user.id,
                from_chat_id=source_id,
                message_id=row["message_id"],
            )
        except Forbidden:
            logger.warning("User %s blocked the bot during /start sequence.", user.id)
            break
        except TelegramError as e:
            logger.error("Sequence error: %s", e)
        await asyncio.sleep(0)

    post = await run(db_get_post_sequence)
    if post.get("message_text"):
        kb = None
        if post.get("button_text") and post.get("button_url"):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(post["button_text"], url=post["button_url"])]])
        try:
            await context.bot.send_message(
                chat_id=user.id,
                text=post["message_text"],
                reply_markup=kb,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error("Failed to send post‑sequence message: %s", e)


# ══════════════════════════════════════════════
# JOIN REQUEST
# ══════════════════════════════════════════════

async def on_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    jr = update.chat_join_request
    if not jr:
        return
    user = jr.from_user
    if not user:
        return

    await run(db_upsert_user, user.id)

    source_id = await run(db_get_source_chat_id)
    for row in await run(db_get_messages):
        try:
            await context.bot.copy_message(
                chat_id=user.id,
                from_chat_id=source_id,
                message_id=row["message_id"],
            )
        except Forbidden:
            logger.warning("User %s blocked the bot — stopping sequence.", user.id)
            break
        except TelegramError as e:
            logger.error("Sequence error: %s", e)
        await asyncio.sleep(0)

    post = await run(db_get_post_sequence)
    if post.get("message_text"):
        kb = None
        if post.get("button_text") and post.get("button_url"):
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(post["button_text"], url=post["button_url"])]])
        try:
            await context.bot.send_message(
                chat_id=user.id,
                text=post["message_text"],
                reply_markup=kb,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error("Failed to send post‑sequence message: %s", e)

    if await run(db_get_auto_approve):
        try:
            await context.bot.approve_chat_join_request(chat_id=jr.chat.id, user_id=user.id)
            logger.info("Auto‑approved join request for %s", user.id)
        except Exception as e:
            logger.error("Auto‑approve failed: %s", e)
    else:
        await run(db_add_pending_request, user.id, jr.chat.id)


# ══════════════════════════════════════════════
# STATS
# ══════════════════════════════════════════════

async def _send_stats(update: Update, is_cb: bool = False) -> None:
    user = update.effective_user
    if not user:
        return
    if not is_any_admin(user.id):
        txt = "⛔ Admins only."
        if is_cb:
            await update.callback_query.answer(txt, show_alert=True)
        else:
            await update.message.reply_text(txt)
        return
    total = await run(db_total_users)
    daily = await run(db_daily_users)
    pending = len(await run(db_get_pending_requests))
    auto = "ON ✅" if await run(db_get_auto_approve) else "OFF ❌"
    text  = (
        "📊 *Bot Statistics*\n\n"
        f"👥 Total users:      `{total}`\n"
        f"🗓 Today's new users: `{daily}`\n"
        f"⏳ Pending approvals: `{pending}`\n"
        f"🔄 Auto‑approve:      `{auto}`"
    )
    if is_cb:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(text, parse_mode="Markdown")
    else:
        await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_stats(update, is_cb=False)

async def cb_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_stats(update, is_cb=True)


# ══════════════════════════════════════════════
# BACKGROUND SCHEDULER TASK
# ══════════════════════════════════════════════

async def scheduler_loop(bot):
    while True:
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        for job_id in (1, 2):
            job = await run(db_get_daily_job, job_id)
            if job and job["enabled"] and job["time"] == current_time:
                chat_id = job["chat_id"] or await run(db_get_source_chat_id)
                try:
                    await send_stored_message(bot, chat_id, job)
                except Exception as e:
                    logger.error(f"Daily job {job_id} failed: {e}")
        pending = await run(db_get_pending_once_jobs, now)
        for job in pending:
            try:
                await send_stored_message(bot, job["chat_id"], job)
                await run(db_mark_once_sent, job["id"])
            except Exception as e:
                logger.error(f"One‑time job {job['id']} failed: {e}")
        await asyncio.sleep(60)


# ══════════════════════════════════════════════
# BROADCAST BACKGROUND WORKER (SILENT)
# ══════════════════════════════════════════════

async def do_broadcast(bot, user_ids: list, msg_data: dict):
    """Send a message to all given user_ids in background.
    On Forbidden, remove the user from database automatically.
    No completion message is sent to the admin.
    """
    for uid in user_ids:
        try:
            await send_stored_message(bot, uid, msg_data)
        except Forbidden:
            await run(db_remove_user, uid)
        except Exception as e:
            logger.warning(f"Broadcast to {uid} failed: {e}")
        await asyncio.sleep(0.05)


# ══════════════════════════════════════════════
# FORWARD USER MESSAGES TO ADMINS (WITH REPLY BUTTON IF ALLOWED)
# ══════════════════════════════════════════════

async def forward_to_admins(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    user = update.effective_user
    if not user or not msg:
        return

    admin_ids = await run(db_get_all_admin_ids)
    for admin_id in admin_ids:
        if not db_has_perm(admin_id, "can_view_user_messages"):
            continue

        reply_markup = None
        if db_has_perm(admin_id, "can_reply_to_users"):
            reply_markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("↩️ Reply", callback_data=f"reply_{user.id}")
            ]])

        try:
            await msg.copy(chat_id=admin_id, reply_markup=reply_markup)
        except Exception as e:
            logger.error("Failed to copy to admin %s: %s", admin_id, e)


# ══════════════════════════════════════════════
# REPLY CALLBACK HANDLER
# ══════════════════════════════════════════════

async def reply_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    if not user:
        return

    if not db_has_perm(user.id, "can_reply_to_users"):
        await query.answer("⛔ You do not have permission to reply.", show_alert=True)
        return

    data = query.data
    if not data.startswith("reply_"):
        return

    target_user_id = int(data.split("_", 1)[1])

    await run(db_set_state, user.id, "awaiting_reply", str(target_user_id))
    await query.message.reply_text(
        f"✉️ *Replying to `{target_user_id}`.*\n"
        "Send your reply now (any text, photo, etc.).",
        parse_mode="Markdown",
        reply_markup=cancel_kb()
    )


# ══════════════════════════════════════════════
# PERMISSION CALLBACK HANDLERS
# ══════════════════════════════════════════════

async def subadmin_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    if not is_main_admin(user.id):
        await query.edit_message_text("⛔ Only main admin can manage permissions.")
        return

    subs = await run(db_list_admins)
    if not subs:
        await query.edit_message_text("ℹ️ No subadmins configured.")
        return

    keyboard = []
    for sub in subs:
        sid = sub["user_id"]
        role = sub["role"]
        label = f"👤 {sid} ({role.upper()})"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"perm_sub_{sid}")])
    keyboard.append([InlineKeyboardButton("🔙 Close", callback_data="perm_close")])

    await query.edit_message_text(
        "⚙️ *Select a subadmin to manage permissions:*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def subadmin_perm_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    if not is_main_admin(user.id):
        await query.edit_message_text("⛔ Only main admin can manage permissions.")
        return

    data = query.data
    if not data.startswith("perm_sub_"):
        return
    sub_id = int(data.split("_")[2])

    perms = await run(db_get_subadmin_perms, sub_id)
    if not perms:
        await query.edit_message_text(f"ℹ️ Subadmin `{sub_id}` not found or has no permissions.")
        return

    role = await run(db_get_admin_role, sub_id)
    keyboard = []
    for perm in PERMISSIONS:
        display = PERM_DISPLAY.get(perm, perm)
        status = "✅" if perms.get(perm, False) else "❌"
        keyboard.append([
            InlineKeyboardButton(
                f"{status} {display}",
                callback_data=f"perm_toggle_{sub_id}_{perm}"
            )
        ])
    keyboard.append([InlineKeyboardButton("🔙 Back to list", callback_data="perm_list")])
    keyboard.append([InlineKeyboardButton("🔙 Close", callback_data="perm_close")])

    await query.edit_message_text(
        f"⚙️ *Permissions for {role.upper()}* `{sub_id}`\n"
        "Tap a button to toggle ON/OFF.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def perm_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    if not is_main_admin(user.id):
        await query.edit_message_text("⛔ Only main admin can manage permissions.")
        return

    data = query.data
    if not data.startswith("perm_toggle_"):
        return
    parts = data.split("_")
    sub_id = int(parts[2])
    perm = "_".join(parts[3:])

    perms = await run(db_get_subadmin_perms, sub_id)
    if perm not in perms:
        await query.answer("Invalid permission.", show_alert=True)
        return

    new_val = not perms[perm]
    await run(db_set_subadmin_perm, sub_id, perm, new_val)

    perms = await run(db_get_subadmin_perms, sub_id)
    role = await run(db_get_admin_role, sub_id)
    keyboard = []
    for p in PERMISSIONS:
        display = PERM_DISPLAY.get(p, p)
        status = "✅" if perms.get(p, False) else "❌"
        keyboard.append([
            InlineKeyboardButton(
                f"{status} {display}",
                callback_data=f"perm_toggle_{sub_id}_{p}"
            )
        ])
    keyboard.append([InlineKeyboardButton("🔙 Back to list", callback_data="perm_list")])
    keyboard.append([InlineKeyboardButton("🔙 Close", callback_data="perm_close")])

    await query.edit_message_text(
        f"⚙️ *Permissions for {role.upper()}* `{sub_id}`\n"
        f"`{perm}` is now {'✅ ON' if new_val else '❌ OFF'}.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def perm_close_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    await query.delete_message()


# ══════════════════════════════════════════════
# UNIFIED MESSAGE HANDLER
# ══════════════════════════════════════════════

async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg  = update.message
    user = update.effective_user
    if not user or not msg:
        return

    uid   = user.id
    text  = (msg.text or "").strip()

    if not is_any_admin(uid):
        await forward_to_admins(update, context)
        return

    action, data = await run(db_get_state, uid)

    # ── Cancel ──
    if text == "❌ Cancel":
        if action and action.startswith("awaiting_postcreator_"):
            await run(db_clear_state, uid)
            await open_panel(update, uid, "↩️ Post creation cancelled.")
            return
        if action == "awaiting_reply":
            await run(db_clear_state, uid)
            await open_panel(update, uid, "↩️ Reply cancelled.")
            return
        if action == "awaiting_premium_confirm":
            await run(db_clear_state, uid)
            await _open_sequence_panel(update, uid, "↩️ Addition cancelled.")
            return
        if is_any_admin(uid):
            await open_panel(update, uid, "↩️ Cancelled.")
        else:
            await run(db_clear_state, uid)
            await msg.reply_text("↩️ Cancelled.", reply_markup=ReplyKeyboardRemove())
        return

    # ── Bot Profile submenu ──
    if text == "🤖 Bot Profile" and db_has_perm(uid, "can_manage_bot_profile"):
        await run(db_clear_state, uid)
        await msg.reply_text("🤖 *Bot Profile Management*", parse_mode="Markdown", reply_markup=bot_profile_kb())
        return

    if text == "✏️ Edit Name" and db_has_perm(uid, "can_manage_bot_profile"):
        await run(db_set_state, uid, "awaiting_bot_name")
        await msg.reply_text("Send the new bot name:", reply_markup=cancel_kb())
        return

    if text == "📝 Edit Description" and db_has_perm(uid, "can_manage_bot_profile"):
        await run(db_set_state, uid, "awaiting_bot_description")
        await msg.reply_text("Send the new bot description:", reply_markup=cancel_kb())
        return

    if text == "📝 Edit Short Description" and db_has_perm(uid, "can_manage_bot_profile"):
        await run(db_set_state, uid, "awaiting_bot_short_description")
        await msg.reply_text("Send the new short description:", reply_markup=cancel_kb())
        return

    if text == "🖼 Set Profile Photo" and db_has_perm(uid, "can_manage_bot_profile"):
        await run(db_set_state, uid, "awaiting_bot_photo")
        await msg.reply_text("Send a photo to set as profile picture:", reply_markup=cancel_kb())
        return

    if text == "🔙 Back to Panel":
        await open_panel(update, uid)
        return

    # ── Post Creator ──
    if text == "📝 Post Creator" and db_has_perm(uid, "can_create_post"):
        await run(db_set_state, uid, "awaiting_postcreator_content", json.dumps({}))
        await msg.reply_text(
            "📝 *Post Creator*\nSend the content (text, photo, video, audio, document, or voice).",
            parse_mode="Markdown", reply_markup=cancel_kb()
        )
        return

    # ── Broadcast ──
    if text == "📢 Broadcast" and db_has_perm(uid, "can_broadcast"):
        await run(db_set_state, uid, "awaiting_broadcast_target")
        await msg.reply_text(
            "📢 *Broadcast*\nChoose target:\n"
            "`today` – today's new users\n"
            "`week` – users from last 7 days\n"
            "`all` – every user in database",
            parse_mode="Markdown",
            reply_markup=cancel_kb()
        )
        return

    # ── State: Bot Name ──
    if action == "awaiting_bot_name" and db_has_perm(uid, "can_manage_bot_profile"):
        await run(db_clear_state, uid)
        try:
            await context.bot.set_my_name(text)
            reply = "✅ Bot name updated."
        except Exception as e:
            reply = f"❌ Failed: {e}"
        await msg.reply_text(reply, reply_markup=bot_profile_kb())
        return

    # ── State: Bot Description ──
    if action == "awaiting_bot_description" and db_has_perm(uid, "can_manage_bot_profile"):
        await run(db_clear_state, uid)
        try:
            await context.bot.set_my_description(text)
            reply = "✅ Bot description updated."
        except Exception as e:
            reply = f"❌ Failed: {e}"
        await msg.reply_text(reply, reply_markup=bot_profile_kb())
        return

    # ── State: Bot Short Description ──
    if action == "awaiting_bot_short_description" and db_has_perm(uid, "can_manage_bot_profile"):
        await run(db_clear_state, uid)
        try:
            await context.bot.set_my_short_description(text)
            reply = "✅ Short description updated."
        except Exception as e:
            reply = f"❌ Failed: {e}"
        await msg.reply_text(reply, reply_markup=bot_profile_kb())
        return

    # ── State: Bot Photo ──
    if action == "awaiting_bot_photo" and db_has_perm(uid, "can_manage_bot_profile"):
        await run(db_clear_state, uid)
        if not msg.photo:
            await msg.reply_text("❌ No photo found. Send a photo.", reply_markup=bot_profile_kb())
            return
        try:
            photo = msg.photo[-1]
            file = await context.bot.get_file(photo.file_id)
            photo_bytes = await file.download_as_bytearray()
            photo_data = bytes(photo_bytes)
            await context.bot.set_my_profile_photo(photo=photo_data)
            reply = "✅ Profile photo updated."
        except Exception as e:
            reply = f"❌ Failed: {e}"
        await msg.reply_text(reply, reply_markup=bot_profile_kb())
        return

    # ── Post Creator flow ──
    if action == "awaiting_postcreator_content" and db_has_perm(uid, "can_create_post"):
        payload = json.loads(data)
        msg_type = None
        file_id = None
        text_content = None
        caption = msg.caption

        if msg.text:
            msg_type = "text"
            text_content = msg.text
        elif msg.photo:
            msg_type = "photo"
            file_id = msg.photo[-1].file_id
        elif msg.video:
            msg_type = "video"
            file_id = msg.video.file_id
        elif msg.document:
            msg_type = "document"
            file_id = msg.document.file_id
        elif msg.audio:
            msg_type = "audio"
            file_id = msg.audio.file_id
        elif msg.voice:
            msg_type = "voice"
            file_id = msg.voice.file_id
        else:
            await msg.reply_text("❌ Unsupported type. Try again.", reply_markup=cancel_kb())
            return

        payload["msg_type"] = msg_type
        if file_id:
            payload["file_id"] = file_id
        if text_content:
            payload["text"] = text_content
        if caption:
            payload["caption"] = caption

        await run(db_set_state, uid, "awaiting_postcreator_button", json.dumps(payload))
        await msg.reply_text(
            "Now send the inline button:\nFormat: `Button text | Button URL`\nType `none` to skip.",
            parse_mode="Markdown", reply_markup=cancel_kb()
        )
        return

    if action == "awaiting_postcreator_button" and db_has_perm(uid, "can_create_post"):
        payload = json.loads(data)
        if text.lower() == "none":
            payload["button_text"] = ""
            payload["button_url"] = ""
        else:
            parts = text.split("|")
            if len(parts) != 2:
                await msg.reply_text("❌ Invalid format. Use: Button text | URL", reply_markup=cancel_kb())
                return
            payload["button_text"] = parts[0].strip()
            payload["button_url"] = parts[1].strip()

        await run(db_set_state, uid, "awaiting_postcreator_target", json.dumps(payload))
        await msg.reply_text(
            "Send target channel ID (or type `default` to use the source channel).",
            reply_markup=cancel_kb()
        )
        return

    if action == "awaiting_postcreator_target" and db_has_perm(uid, "can_create_post"):
        payload = json.loads(data)
        await run(db_clear_state, uid)
        try:
            if text.lower() == "default":
                target = await run(db_get_source_chat_id)
            else:
                target = int(text)
        except ValueError:
            await msg.reply_text("❌ Invalid chat ID.", reply_markup=cancel_kb())
            await run(db_set_state, uid, "awaiting_postcreator_target", json.dumps(payload))
            return

        msg_type = payload.get("msg_type")
        file_id = payload.get("file_id")
        txt = payload.get("text")
        caption = payload.get("caption")
        btn_text = payload.get("button_text")
        btn_url = payload.get("button_url")

        kb = None
        if btn_text and btn_url:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(btn_text, url=btn_url)]])

        try:
            if msg_type == "text":
                await context.bot.send_message(target, txt, reply_markup=kb)
            elif msg_type == "photo":
                await context.bot.send_photo(target, file_id, caption=caption, reply_markup=kb)
            elif msg_type == "video":
                await context.bot.send_video(target, file_id, caption=caption, reply_markup=kb)
            elif msg_type == "document":
                await context.bot.send_document(target, file_id, caption=caption, reply_markup=kb)
            elif msg_type == "audio":
                await context.bot.send_audio(target, file_id, caption=caption, reply_markup=kb)
            elif msg_type == "voice":
                await context.bot.send_voice(target, file_id, caption=caption, reply_markup=kb)
            else:
                await msg.reply_text("❌ Unknown message type in stored data.")
                return
            await msg.reply_text("✅ Post delivered successfully.", reply_markup=staff_kb(uid))
        except Exception as e:
            await msg.reply_text(f"❌ Failed to send: {e}", reply_markup=staff_kb(uid))
        return

    # ── Broadcast flow ──
    if action == "awaiting_broadcast_target" and db_has_perm(uid, "can_broadcast"):
        await run(db_clear_state, uid)
        target = text.lower()
        today = date.today().isoformat()
        if target == "today":
            user_ids = await run(db_users_by_date_range, today, today)
        elif target == "week":
            start = (date.today() - timedelta(days=7)).isoformat()
            user_ids = await run(db_users_by_date_range, start, today)
        elif target == "all":
            user_ids = await run(db_all_user_ids)
        else:
            await msg.reply_text("❌ Invalid choice. Use `today`, `week`, or `all`.", reply_markup=cancel_kb())
            await run(db_set_state, uid, "awaiting_broadcast_target")
            return

        if not user_ids:
            await msg.reply_text("ℹ️ No users found for that target.")
            await open_panel(update, uid)
            return

        await run(db_set_state, uid, "awaiting_broadcast_content", json.dumps({"target": target, "count": len(user_ids)}))
        await msg.reply_text(
            f"📢 *Broadcast to `{len(user_ids)}` users*\n"
            "Now send the message (text, photo, video, etc.)",
            parse_mode="Markdown",
            reply_markup=cancel_kb()
        )
        return

    if action == "awaiting_broadcast_content" and db_has_perm(uid, "can_broadcast"):
        stored = json.loads(data)
        target = stored["target"]
        today = date.today().isoformat()
        if target == "today":
            user_ids = await run(db_users_by_date_range, today, today)
        elif target == "week":
            start = (date.today() - timedelta(days=7)).isoformat()
            user_ids = await run(db_users_by_date_range, start, today)
        elif target == "all":
            user_ids = await run(db_all_user_ids)
        else:
            await run(db_clear_state, uid)
            await open_panel(update, uid, "❌ Invalid broadcast target.")
            return

        msg_type = None
        file_id = None
        text_content = None
        caption = msg.caption

        if msg.text:
            msg_type = "text"
            text_content = msg.text
        elif msg.photo:
            msg_type = "photo"
            file_id = msg.photo[-1].file_id
        elif msg.video:
            msg_type = "video"
            file_id = msg.video.file_id
        elif msg.document:
            msg_type = "document"
            file_id = msg.document.file_id
        elif msg.audio:
            msg_type = "audio"
            file_id = msg.audio.file_id
        elif msg.voice:
            msg_type = "voice"
            file_id = msg.voice.file_id
        else:
            await msg.reply_text("❌ Unsupported type.", reply_markup=cancel_kb())
            return

        msg_data = {
            "msg_type": msg_type,
            "file_id": file_id,
            "text": text_content,
            "caption": caption,
        }

        await run(db_clear_state, uid)
        await msg.reply_text(
            f"📢 *Broadcast started — running in background.* (`{len(user_ids)}` users).\n"
            "No further messages will be sent.",
            parse_mode="Markdown",
            reply_markup=staff_kb(uid)
        )
        asyncio.create_task(do_broadcast(context.bot, user_ids, msg_data))
        return

    # ── Reply state ──
    if action == "awaiting_reply":
        if not db_has_perm(uid, "can_reply_to_users"):
            await run(db_clear_state, uid)
            await open_panel(update, uid, "⛔ You don't have permission to reply.")
            return
        target_user_id = int(data)
        await run(db_clear_state, uid)
        try:
            await msg.copy(chat_id=target_user_id)
            await msg.reply_text(f"✅ Reply sent to `{target_user_id}`.", reply_markup=staff_kb(uid))
        except Exception as e:
            await msg.reply_text(f"❌ Failed to send reply: {e}", reply_markup=staff_kb(uid))
        return

    # ── Premium emoji confirmation (during add message) ──
    if action == "awaiting_premium_confirm":
        if text == "✅ Yes":
            # proceed with adding message despite premium emoji warning
            stored = json.loads(data)
            pos = stored["pos"]
            msg_data = stored["msg_data"]
            source_id = await run(db_get_source_chat_id)
            try:
                sent_msg = await _send_message_from_data(context.bot, source_id, msg_data)
                new_msg_id = sent_msg.message_id
            except Exception as e:
                logger.error("Failed to copy message to source channel: %s", e)
                await msg.reply_text("❌ Failed to copy message to source channel. Check bot permissions.")
                return
            ok = await run(db_add_message, new_msg_id, pos)
            if ok:
                reply = f"✅ Message added at position `{pos}` (ID: `{new_msg_id}`)."
            else:
                reply = "❌ Position already occupied. Use reorder to move existing message first."
            await run(db_clear_state, uid)
            await _open_sequence_panel(update, uid, reply)
        else:
            await run(db_clear_state, uid)
            await _open_sequence_panel(update, uid, "↩️ Addition cancelled.")
        return

    # ── State: awaiting add message position (step 1) ──
    if action == "awaiting_addmsg_pos":
        if not db_has_perm(uid, "can_manage_seq"):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        try:
            pos = int(text)
            if pos < 1:
                await msg.reply_text("❌ Position must be ≥ 1. Try again:", reply_markup=cancel_kb())
                return
            await run(db_set_state, uid, "awaiting_addmsg_msg", str(pos))
            await msg.reply_text(
                f"✅ Position set to `{pos}`.\n\n"
                "Now send the message you want to add to the sequence.\n"
                "It can be text, photo, video, document, etc.",
                parse_mode="Markdown",
                reply_markup=cancel_kb()
            )
        except ValueError:
            await msg.reply_text("❌ Please send a valid number for position.", reply_markup=cancel_kb())
        return

    # ── State: awaiting add message content (step 2) ──
    if action == "awaiting_addmsg_msg":
        if not db_has_perm(uid, "can_manage_seq"):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        try:
            pos = int(data)
        except (TypeError, ValueError):
            await run(db_clear_state, uid)
            await open_panel(update, uid, "❌ State error. Please try again.")
            return

        # Premium emoji check
        if has_premium_emoji(msg):
            msg_data = _extract_msg_data(msg)
            stored = json.dumps({"pos": pos, "msg_data": msg_data})
            await run(db_set_state, uid, "awaiting_premium_confirm", stored)
            await msg.reply_text(
                "⚠️ This message contains *premium emojis*.\n"
                "When copied to the source channel, they may be lost (replaced by regular emojis).\n\n"
                "Do you still want to add it?",
                parse_mode="Markdown",
                reply_markup=yes_no_kb()
            )
            return

        source_id = await run(db_get_source_chat_id)
        try:
            sent_msg = await msg.forward(chat_id=source_id)
            message_id = sent_msg.message_id
        except Exception as e:
            logger.error("Failed to forward message to source channel: %s", e)
            await run(db_clear_state, uid)
            await open_panel(update, uid, f"❌ Failed to forward message: {e}")
            return

        ok = await run(db_add_message, message_id, pos)
        if ok:
            reply = f"✅ Message added at position `{pos}` (ID: `{message_id}`)."
        else:
            reply = "❌ Duplicate position — use 🔀 Reorder to move an existing entry first."
        await run(db_clear_state, uid)
        await _open_sequence_panel(update, uid, reply)
        return

    # ── State: awaiting remove message (by message_id) ──
    if action == "awaiting_removemsg":
        if not db_has_perm(uid, "can_manage_seq"):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        await run(db_clear_state, uid)
        try:
            mid   = int(text)
            ok    = await run(db_remove_message, mid)
            reply = f"✅ Message `{mid}` removed." if ok else f"ℹ️ Message ID `{mid}` not found."
        except ValueError:
            reply = "❌ Invalid ID."
        await _open_sequence_panel(update, uid, reply)
        return

    # ── State: awaiting reorder message (message_id new_position) ──
    if action == "awaiting_reordermsg":
        if not db_has_perm(uid, "can_manage_seq"):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        await run(db_clear_state, uid)
        parts = text.split()
        try:
            mid, pos = int(parts[0]), int(parts[1])
            ok    = await run(db_reorder_message, mid, pos)
            reply = (
                f"✅ Message `{mid}` moved to position `{pos}`."
                if ok else
                f"ℹ️ Message ID `{mid}` not found."
            )
        except (ValueError, IndexError):
            reply = "❌ Invalid input. Expected: `<message_id> <new_position>` (two numbers)."
        await _open_sequence_panel(update, uid, reply)
        return

    # ── State: awaiting add admin (superadmin only) ──
    if action == "awaiting_add_admin":
        if not is_main_admin(uid):
            await open_panel(update, uid, "⛔ Only superadmin can add admins.")
            return
        await run(db_clear_state, uid)
        try:
            tid = int(text)
            if tid == ADMIN_ID:
                reply = "ℹ️ Cannot add the main admin."
            else:
                ok = await run(db_add_admin, tid, "admin")
                reply = f"✅ `{tid}` added as Admin." if ok else f"ℹ️ `{tid}` is already an admin/subadmin."
        except ValueError:
            reply = "❌ Invalid ID — please send a numeric Telegram user ID."
        await open_panel(update, uid, reply)
        return

    # ── State: awaiting add subadmin ──
    if action == "awaiting_add_subadmin":
        if not (is_main_admin(uid) or db_is_admin(uid)):
            await open_panel(update, uid, "⛔ You don't have permission to add subadmins.")
            return
        await run(db_clear_state, uid)
        try:
            tid = int(text)
            if tid == ADMIN_ID:
                reply = "ℹ️ Cannot add the main admin."
            else:
                ok = await run(db_add_admin, tid, "subadmin")
                reply = f"✅ `{tid}` added as Subadmin." if ok else f"ℹ️ `{tid}` is already an admin/subadmin."
        except ValueError:
            reply = "❌ Invalid ID."
        await open_panel(update, uid, reply)
        return

    # ── State: awaiting remove admin ──
    if action == "awaiting_remove_admin":
        if not is_main_admin(uid):
            await open_panel(update, uid, "⛔ Only superadmin can remove admins.")
            return
        await run(db_clear_state, uid)
        try:
            tid = int(text)
            if tid == ADMIN_ID:
                reply = "ℹ️ The main admin cannot be removed."
            else:
                ok = await run(db_remove_subadmin, tid)
                reply = f"✅ Admin/Subadmin `{tid}` removed." if ok else f"ℹ️ `{tid}` was not an admin/subadmin."
        except ValueError:
            reply = "❌ Invalid ID."
        await open_panel(update, uid, reply)
        return

    # ── State: awaiting remove subadmin ──
    if action == "awaiting_remove_subadmin":
        if not (is_main_admin(uid) or db_is_admin(uid)):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        await run(db_clear_state, uid)
        try:
            tid = int(text)
            if tid == ADMIN_ID:
                reply = "ℹ️ The main admin cannot be removed."
            else:
                ok = await run(db_remove_subadmin, tid)
                reply = f"✅ Subadmin `{tid}` removed." if ok else f"ℹ️ `{tid}` was not a subadmin."
        except ValueError:
            reply = "❌ Invalid ID."
        await open_panel(update, uid, reply)
        return

    # ── State: awaiting change source ──
    if action == "awaiting_change_source":
        if not db_has_perm(uid, "can_change_source"):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        await run(db_clear_state, uid)
        try:
            new_id = int(text)
            await run(db_set_source_chat_id, new_id)
            reply = f"✅ Source channel updated to `{new_id}`."
        except ValueError:
            reply = "❌ Invalid chat ID."
        await open_panel(update, uid, reply)
        return

    # ── State: awaiting set post button ──
    if action == "awaiting_set_post":
        if not db_has_perm(uid, "can_set_post_button"):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        await run(db_clear_state, uid)
        parts = text.split("|")
        msg_text = parts[0].strip()
        btn_text = parts[1].strip() if len(parts) > 1 else ""
        btn_url  = parts[2].strip() if len(parts) > 2 else ""
        await run(db_set_post_sequence, msg_text, btn_text, btn_url)
        reply = "✅ Post‑sequence message updated."
        await open_panel(update, uid, reply)
        return

    # ── State: awaiting remove post ──
    if action == "awaiting_remove_post_button":
        if not db_has_perm(uid, "can_set_post_button"):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        await run(db_clear_state, uid)
        if text.lower() != "yes":
            await open_panel(update, uid, "↩️ Removal cancelled.")
            return
        await run(db_set_post_sequence, "", "", "")
        await open_panel(update, uid, "✅ Entire post removed.")
        return

    # ── State: awaiting schedule time (morning/night) ──
    if action in ("awaiting_morning_time", "awaiting_night_time"):
        if not db_has_perm(uid, "can_schedule"):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        try:
            t = datetime.strptime(text, "%H:%M").time()
            time_str = t.strftime("%H:%M")
            job_id = 1 if action == "awaiting_morning_time" else 2
            await run(db_set_state, uid, f"awaiting_{'morning' if job_id==1 else 'night'}_content", time_str)
            await msg.reply_text(
                f"🕒 Time set to {time_str}. Now send the message to be sent daily.",
                reply_markup=cancel_kb()
            )
        except ValueError:
            await msg.reply_text("❌ Invalid time format. Use HH:MM (24-hour).", reply_markup=cancel_kb())
        return

    # ── State: awaiting schedule content (morning/night) ──
    if action in ("awaiting_morning_content", "awaiting_night_content"):
        if not db_has_perm(uid, "can_schedule"):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        job_id = 1 if action == "awaiting_morning_content" else 2
        time_str = data
        msg_type = None
        file_id = None
        text_content = None
        caption = msg.caption

        if msg.text:
            msg_type = "text"
            text_content = msg.text
        elif msg.photo:
            msg_type = "photo"
            file_id = msg.photo[-1].file_id
        elif msg.video:
            msg_type = "video"
            file_id = msg.video.file_id
        elif msg.document:
            msg_type = "document"
            file_id = msg.document.file_id
        elif msg.audio:
            msg_type = "audio"
            file_id = msg.audio.file_id
        elif msg.voice:
            msg_type = "voice"
            file_id = msg.voice.file_id
        else:
            await msg.reply_text("❌ Unsupported message type.")
            return

        chat_id = await run(db_get_source_chat_id)
        await run(db_set_daily_job, job_id, time_str, msg_type, file_id, text_content, caption, chat_id)
        await run(db_clear_state, uid)
        job_name = "Morning" if job_id == 1 else "Night"
        await open_panel(update, uid, f"✅ {job_name} message scheduled daily at {time_str}.")
        return

    # ── State: awaiting once datetime ──
    if action == "awaiting_once_datetime":
        if not db_has_perm(uid, "can_schedule"):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        try:
            dt = datetime.strptime(text, "%Y-%m-%d %H:%M")
            if dt < datetime.now():
                await msg.reply_text("❌ Date/time must be in the future.", reply_markup=cancel_kb())
                return
            await run(db_set_state, uid, "awaiting_once_content", dt.isoformat())
            await msg.reply_text(
                f"📅 Scheduled for {dt}. Now send the message.",
                reply_markup=cancel_kb()
            )
        except ValueError:
            await msg.reply_text("❌ Invalid format. Use: YYYY-MM-DD HH:MM", reply_markup=cancel_kb())
        return

    # ── State: awaiting once content ──
    if action == "awaiting_once_content":
        if not db_has_perm(uid, "can_schedule"):
            await open_panel(update, uid, "⛔ Permission denied.")
            return
        dt = datetime.fromisoformat(data)
        msg_type = None
        file_id = None
        text_content = None
        caption = msg.caption

        if msg.text:
            msg_type = "text"
            text_content = msg.text
        elif msg.photo:
            msg_type = "photo"
            file_id = msg.photo[-1].file_id
        elif msg.video:
            msg_type = "video"
            file_id = msg.video.file_id
        elif msg.document:
            msg_type = "document"
            file_id = msg.document.file_id
        elif msg.audio:
            msg_type = "audio"
            file_id = msg.audio.file_id
        elif msg.voice:
            msg_type = "voice"
            file_id = msg.voice.file_id
        else:
            await msg.reply_text("❌ Unsupported message type.")
            return

        chat_id = await run(db_get_source_chat_id)
        await run(db_add_once_job, chat_id, dt, msg_type, file_id, text_content, caption)
        await run(db_clear_state, uid)
        await open_panel(update, uid, f"✅ Message scheduled for {dt}.")
        return

    # ── Button: Test Sequence ──
    if text == "📨 Test Sequence":
        if not db_has_perm(uid, "can_test_sequence"):
            await msg.reply_text("⛔ Permission denied.")
            return
        rows = await run(db_get_messages)
        if not rows:
            await msg.reply_text("ℹ️ Sequence is empty.", reply_markup=staff_kb(uid))
            return
        source_id = await run(db_get_source_chat_id)
        await msg.reply_text("📨 Sending sequence to you now…")
        for row in rows:
            try:
                await context.bot.copy_message(
                    chat_id=uid,
                    from_chat_id=source_id,
                    message_id=row["message_id"]
                )
            except Exception as e:
                logger.error(f"Test sequence error: {e}")
                await msg.reply_text(f"⚠️ Failed to send one message: {e}")
            await asyncio.sleep(0.5)

        post = await run(db_get_post_sequence)
        if post.get("message_text"):
            kb = None
            if post.get("button_text") and post.get("button_url"):
                kb = InlineKeyboardMarkup([[InlineKeyboardButton(post["button_text"], url=post["button_url"])]])
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=post["message_text"],
                    reply_markup=kb,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error("Failed to send post‑sequence message in test: %s", e)
        return

    # ── Button: Stats ──
    if text == "📊 Stats":
        if not db_has_perm(uid, "can_stats"):
            await msg.reply_text("⛔ Permission denied.")
            return
        total = await run(db_total_users)
        daily = await run(db_daily_users)
        pending = len(await run(db_get_pending_requests))
        auto = "ON ✅" if await run(db_get_auto_approve) else "OFF ❌"
        await msg.reply_text(
            f"📊 *Bot Statistics*\n\n👥 Total: `{total}`\n🗓 Today: `{daily}`\n⏳ Pending: `{pending}`\n🔄 Auto‑approve: `{auto}`",
            parse_mode="Markdown",
            reply_markup=staff_kb(uid),
        )
        return

    # ── Admins / Subadmins ──
    if text == "👑 Admins" and is_main_admin(uid):
        rows = await run(db_list_admins, "admin")
        listing = "\n".join(f"• `{r['user_id']}`" for r in rows) if rows else "_No admins._"
        await msg.reply_text(
            f"👑 *Admin Management*\n\n{listing}",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardMarkup(
                [["➕ Add Admin", "➖ Remove Admin"], ["🔙 Back to Panel"]],
                resize_keyboard=True
            )
        )
        return

    if text == "➕ Add Admin" and is_main_admin(uid):
        await run(db_set_state, uid, "awaiting_add_admin")
        await msg.reply_text("👑 Send the *Telegram user ID* to add as Admin:", parse_mode="Markdown", reply_markup=cancel_kb())
        return

    if text == "➖ Remove Admin" and is_main_admin(uid):
        rows = await run(db_list_admins, "admin")
        if not rows:
            await msg.reply_text("ℹ️ No admins to remove.")
            return
        listing = "\n".join(f"• `{r['user_id']}`" for r in rows)
        await run(db_set_state, uid, "awaiting_remove_admin")
        await msg.reply_text(f"🟡 *Current Admins:*\n{listing}\n\nSend ID to remove:", parse_mode="Markdown", reply_markup=cancel_kb())
        return

    if text == "👥 Subadmins":
        if not (is_main_admin(uid) or db_is_admin(uid)):
            await msg.reply_text("⛔ Permission denied.")
            return
        rows = await run(db_list_admins) if is_main_admin(uid) else await run(db_list_admins, "subadmin")
        listing = "\n".join(f"• `{r['user_id']}`" for r in rows) if rows else "_No subadmins._"
        await msg.reply_text(
            f"👥 *Subadmin Management*\n\n{listing}",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardMarkup(
                [["➕ Add Subadmin", "➖ Remove Subadmin"], ["🔙 Back to Panel"]],
                resize_keyboard=True
            )
        )
        return

    if text == "➕ Add Subadmin":
        if not (is_main_admin(uid) or db_is_admin(uid)):
            await msg.reply_text("⛔ Permission denied.")
            return
        await run(db_set_state, uid, "awaiting_add_subadmin")
        await msg.reply_text("👤 Send the *Telegram user ID* to add as Subadmin:", parse_mode="Markdown", reply_markup=cancel_kb())
        return

    if text == "➖ Remove Subadmin":
        if not (is_main_admin(uid) or db_is_admin(uid)):
            await msg.reply_text("⛔ Permission denied.")
            return
        rows = await run(db_list_admins) if is_main_admin(uid) else await run(db_list_admins, "subadmin")
        if not rows:
            await msg.reply_text("ℹ️ No subadmins to remove.")
            return
        listing = "\n".join(f"• `{r['user_id']}`" for r in rows)
        await run(db_set_state, uid, "awaiting_remove_subadmin")
        await msg.reply_text(f"🟡 *Current Subadmins:*\n{listing}\n\nSend ID to remove:", parse_mode="Markdown", reply_markup=cancel_kb())
        return

    # ── Approve All Requests ──
    if text == "✅ Approve All Requests":
        if not db_has_perm(uid, "can_approve_requests"):
            await msg.reply_text("⛔ Permission denied.")
            return
        pending = await run(db_get_pending_requests)
        if not pending:
            await msg.reply_text("ℹ️ No pending requests.")
            return
        status = await msg.reply_text(f"⏳ Approving {len(pending)} requests…")
        approved = 0
        for req in pending:
            try:
                await context.bot.approve_chat_join_request(chat_id=req["chat_id"], user_id=req["user_id"])
                approved += 1
            except Exception as e:
                logger.error(f"Failed to approve: {e}")
            await asyncio.sleep(0.1)
        await run(db_clear_pending_requests)
        await status.edit_text(f"✅ Approved {approved} out of {len(pending)} requests.")
        await open_panel(update, uid)
        return

    # ── Change Source Channel ──
    if text == "📡 Change Source Channel":
        if not db_has_perm(uid, "can_change_source"):
            await msg.reply_text("⛔ Permission denied.")
            return
        current = await run(db_get_source_chat_id)
        await run(db_set_state, uid, "awaiting_change_source")
        await msg.reply_text(f"Current: `{current}`\nSend new channel ID:", parse_mode="Markdown", reply_markup=cancel_kb())
        return

    # ── Set / Remove Post ──
    if text == "🔘 Set Post Button":
        if not db_has_perm(uid, "can_set_post_button"):
            await msg.reply_text("⛔ Permission denied.")
            return
        await run(db_set_state, uid, "awaiting_set_post")
        await msg.reply_text("Send format:\n`Message text | Button text | Button URL`", parse_mode="Markdown", reply_markup=cancel_kb())
        return

    if text == "🗑 Remove Post":
        if not db_has_perm(uid, "can_set_post_button"):
            await msg.reply_text("⛔ Permission denied.")
            return
        await run(db_set_state, uid, "awaiting_remove_post_button")
        await msg.reply_text(
            "⚠️ This will remove the *entire* post (message + button).\nType `yes` to confirm:",
            parse_mode="Markdown", reply_markup=cancel_kb()
        )
        return

    # ── Auto‑Approve Toggle ──
    if text.startswith("🔄 Auto‑Approve:"):
        if not db_has_perm(uid, "can_toggle_auto_approve"):
            await msg.reply_text("⛔ Permission denied.")
            return
        current = await run(db_get_auto_approve)
        new_val = not current
        await run(db_set_auto_approve, new_val)
        await open_panel(update, uid, f"🔄 Auto‑approve is now {'ON ✅' if new_val else 'OFF ❌'}")
        return

    # ── Subadmin Permissions ──
    if text == "⚙️ Subadmin Permissions" and is_main_admin(uid):
        subs = await run(db_list_admins)
        if not subs:
            await msg.reply_text("ℹ️ No subadmins.")
            return
        keyboard = []
        for sub in subs:
            sid = sub["user_id"]
            role = sub["role"]
            keyboard.append([InlineKeyboardButton(f"👤 {sid} ({role.upper()})", callback_data=f"perm_sub_{sid}")])
        keyboard.append([InlineKeyboardButton("🔙 Close", callback_data="perm_close")])
        await msg.reply_text("⚙️ *Select subadmin:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # ── Schedule Morning / Night / Once ──
    if text == "🌅 Schedule Morning":
        if not db_has_perm(uid, "can_schedule"):
            await msg.reply_text("⛔ Permission denied.")
            return
        await run(db_set_state, uid, "awaiting_morning_time")
        await msg.reply_text("🌅 Send time (HH:MM):", reply_markup=cancel_kb())
        return

    if text == "🌙 Schedule Night":
        if not db_has_perm(uid, "can_schedule"):
            await msg.reply_text("⛔ Permission denied.")
            return
        await run(db_set_state, uid, "awaiting_night_time")
        await msg.reply_text("🌙 Send time (HH:MM):", reply_markup=cancel_kb())
        return

    if text == "⏰ Schedule Messages":
        if not db_has_perm(uid, "can_schedule"):
            await msg.reply_text("⛔ Permission denied.")
            return
        await run(db_set_state, uid, "awaiting_once_datetime")
        await msg.reply_text("📅 Send date/time (YYYY-MM-DD HH:MM):", parse_mode="Markdown", reply_markup=cancel_kb())
        return

    # ── Message Sequence Sub‑panel ──
    if text == "📨 Message Sequence":
        if not db_has_perm(uid, "can_manage_seq"):
            await msg.reply_text("⛔ Permission denied.")
            return
        await _open_sequence_panel(update, uid)
        return

    if text == "➕ Add Message" and db_has_perm(uid, "can_manage_seq"):
        await run(db_set_state, uid, "awaiting_addmsg_pos")
        await msg.reply_text("🔢 Enter the *position* (number) for the new message:", parse_mode="Markdown", reply_markup=cancel_kb())
        return

    if text == "➖ Remove Message" and db_has_perm(uid, "can_manage_seq"):
        rows = await run(db_get_messages)
        if not rows:
            await _open_sequence_panel(update, uid, "ℹ️ Sequence is empty.")
            return
        listing = "\n".join(f"  `{r['position']}.` msg\\_id `{r['message_id']}`" for r in rows)
        await run(db_set_state, uid, "awaiting_removemsg")
        await msg.reply_text(f"📋 *Current sequence:*\n{listing}\n\nSend the *message ID* to remove:", parse_mode="Markdown", reply_markup=cancel_kb())
        return

    if text == "🔀 Reorder Message" and db_has_perm(uid, "can_manage_seq"):
        rows = await run(db_get_messages)
        if not rows:
            await _open_sequence_panel(update, uid, "ℹ️ Sequence is empty.")
            return
        listing = "\n".join(f"  `{r['position']}.` msg\\_id `{r['message_id']}`" for r in rows)
        await run(db_set_state, uid, "awaiting_reordermsg")
        await msg.reply_text(f"📋 *Current sequence:*\n{listing}\n\nSend `<message_id> <new_position>`:", parse_mode="Markdown", reply_markup=cancel_kb())
        return

    if text == "📄 List Messages" and db_has_perm(uid, "can_manage_seq"):
        rows = await run(db_get_messages)
        if rows:
            body = "\n".join(f"  `{r['position']}.` msg\\_id `{r['message_id']}`" for r in rows)
        else:
            body = "_Sequence is empty._"
        await msg.reply_text(f"📋 *Message Sequence*\n\n{body}", parse_mode="Markdown", reply_markup=sequence_panel_kb())
        return

    # Fallback
    await open_panel(update, uid)


async def _open_sequence_panel(update: Update, uid: int, note: str = "") -> None:
    text = (f"{note}\n\n📨 *Message Sequence Panel*" if note else "📨 *Message Sequence Panel*")
    await update.message.reply_text(text.strip(), parse_mode="Markdown", reply_markup=sequence_panel_kb())


# ── Helper: extract message data for re‑sending ──
def _extract_msg_data(msg) -> dict:
    """Extract enough information to re-send a message via bot."""
    data = {}
    if msg.text:
        data["type"] = "text"
        data["text"] = msg.text
    elif msg.photo:
        data["type"] = "photo"
        data["file_id"] = msg.photo[-1].file_id
        data["caption"] = msg.caption
    elif msg.video:
        data["type"] = "video"
        data["file_id"] = msg.video.file_id
        data["caption"] = msg.caption
    elif msg.document:
        data["type"] = "document"
        data["file_id"] = msg.document.file_id
        data["caption"] = msg.caption
    elif msg.audio:
        data["type"] = "audio"
        data["file_id"] = msg.audio.file_id
        data["caption"] = msg.caption
    elif msg.voice:
        data["type"] = "voice"
        data["file_id"] = msg.voice.file_id
        data["caption"] = msg.caption
    else:
        data["type"] = "unknown"
    return data

async def _send_message_from_data(bot, chat_id: int, msg_data: dict):
    """Re‑send a message using extracted data. Returns the sent message."""
    t = msg_data["type"]
    if t == "text":
        return await bot.send_message(chat_id, msg_data["text"])
    elif t == "photo":
        return await bot.send_photo(chat_id, msg_data["file_id"], caption=msg_data.get("caption"))
    elif t == "video":
        return await bot.send_video(chat_id, msg_data["file_id"], caption=msg_data.get("caption"))
    elif t == "document":
        return await bot.send_document(chat_id, msg_data["file_id"], caption=msg_data.get("caption"))
    elif t == "audio":
        return await bot.send_audio(chat_id, msg_data["file_id"], caption=msg_data.get("caption"))
    elif t == "voice":
        return await bot.send_voice(chat_id, msg_data["file_id"], caption=msg_data.get("caption"))
    else:
        raise ValueError("Unknown message type")


# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════

def main() -> None:
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(ChatJoinRequestHandler(on_join_request))
    app.add_handler(CallbackQueryHandler(cb_stats, pattern="^stats$"))

    app.add_handler(CallbackQueryHandler(subadmin_list_callback, pattern="^perm_list$"))
    app.add_handler(CallbackQueryHandler(subadmin_perm_menu_callback, pattern="^perm_sub_"))
    app.add_handler(CallbackQueryHandler(perm_toggle_callback, pattern="^perm_toggle_"))
    app.add_handler(CallbackQueryHandler(perm_close_callback, pattern="^perm_close$"))
    app.add_handler(CallbackQueryHandler(reply_callback_handler, pattern="^reply_"))

    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, on_message))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(scheduler_loop(app.bot))

    logger.info("Bot started — polling…")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()