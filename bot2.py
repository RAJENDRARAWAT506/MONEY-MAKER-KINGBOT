#!/usr/bin/env python3
"""
bot.py — Ultimate Telegram Bot with Non‑blocking Concurrent Broadcast
=====================================================================
Features:
- Fully asynchronous, zero‑delay broadcast (30 concurrent sends)
- Premium‑emoji safe sequence delivery (always copy, never resend plain text)
- Multi‑button creation with colour field (stored for API)
- Remove message by position (not message ID)
- List messages with position & message ID
- /start toggle, intro with placeholders, direct reply, auto‑approve
- Hierarchical admin/subadmin with per‑command permissions
- Change bot profile (name, bio, description, photo with source backup)
- Background broadcast that never blocks the admin panel
- Exhaustive error handling – bot never crashes
- Only one log: "✅ BOT IS STARTED SUCCESSFULLY"
"""

import asyncio
import sqlite3
import json
import requests
import time
from contextlib import contextmanager
from datetime import date
from functools import partial, wraps
import os

from dotenv import load_dotenv

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.error import TelegramError, Forbidden, NetworkError, RetryAfter, TimedOut
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    filters,
    ContextTypes,
)

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════

load_dotenv()

BOT_TOKEN:          str = os.getenv("BOT_TOKEN", "")
SOURCE_CHAT_ID:     int = int(os.getenv("SOURCE_CHAT_ID", "0"))
ADMIN_ID:           int = int(os.getenv("ADMIN_ID", "0"))

BROADCAST_CONCURRENCY: int = 30
MAX_RETRIES_DB:        int = 3
MAX_RETRIES_API:       int = 3
DB_PATH:               str = "bot.db"
API_URL:               str = f"https://api.telegram.org/bot{BOT_TOKEN}"

if not BOT_TOKEN:      raise ValueError("BOT_TOKEN not set in .env")
if not ADMIN_ID:       raise ValueError("ADMIN_ID not set in .env")

print("✅ BOT IS STARTED SUCCESSFULLY")

# ═══════════════════════════════════════════════════════════════════
# CUSTOM EXCEPTIONS
# ═══════════════════════════════════════════════════════════════════

class BotInternalError(Exception):
    pass

class DBError(BotInternalError):
    pass

class APIError(BotInternalError):
    pass

# ═══════════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ═══════════════════════════════════════════════════════════════════

def retry_db(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        last_exc = None
        for attempt in range(MAX_RETRIES_DB):
            try:
                return func(*args, **kwargs)
            except sqlite3.OperationalError as e:
                last_exc = e
                if attempt < MAX_RETRIES_DB - 1:
                    time.sleep(0.1 * (attempt + 1))
                else:
                    raise DBError(f"DB op failed: {e}")
            except Exception as e:
                raise DBError(f"DB unexpected: {e}")
        raise DBError("Unexpected retry exit")
    return wrapper

@contextmanager
def get_conn():
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        yield conn
        conn.commit()
    except sqlite3.Error as e:
        if conn:
            try: conn.rollback()
            except: pass
        raise DBError(f"Commit failed: {e}")
    except Exception as e:
        if conn:
            try: conn.rollback()
            except: pass
        raise DBError(f"Connection error: {e}")
    finally:
        if conn:
            try: conn.close()
            except: pass

@retry_db
def init_db() -> None:
    with get_conn() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id    INTEGER PRIMARY KEY,
                first_seen DATE NOT NULL DEFAULT (DATE('now')),
                username   TEXT,
                first_name TEXT,
                last_name  TEXT
            );
            CREATE TABLE IF NOT EXISTS subadmins (
                user_id  INTEGER PRIMARY KEY,
                added_at TIMESTAMP DEFAULT (DATETIME('now')),
                role TEXT DEFAULT 'subadmin'
            );
            CREATE TABLE IF NOT EXISTS messages (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id    INTEGER NOT NULL,
                position      INTEGER NOT NULL UNIQUE,
                content_json  TEXT,
                buttons_json  TEXT DEFAULT '[]'
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
                can_broadcast INTEGER DEFAULT 1,
                can_stats INTEGER DEFAULT 1,
                can_manage_seq INTEGER DEFAULT 0,
                can_manage_subadmins INTEGER DEFAULT 0,
                can_change_source INTEGER DEFAULT 0,
                can_set_post_button INTEGER DEFAULT 0,
                can_manage_bot_profile INTEGER DEFAULT 0,
                can_test_sequence INTEGER DEFAULT 0,
                can_approve_requests INTEGER DEFAULT 0,
                can_toggle_auto_approve INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES subadmins(user_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS post_sequence (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                message_text TEXT,
                buttons_json TEXT DEFAULT '[]'
            );
            CREATE TABLE IF NOT EXISTS intro (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                message_text TEXT
            );
            CREATE TABLE IF NOT EXISTS direct_reply (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                message_id INTEGER,
                content_json TEXT,
                buttons_json TEXT DEFAULT '[]'
            );
        """)
        # Migrations
        for col in ["content_json", "buttons_json"]:
            try: c.execute(f"ALTER TABLE messages ADD COLUMN {col} TEXT DEFAULT '[]'")
            except: pass
        for perm in ["can_manage_bot_profile","can_test_sequence","can_approve_requests","can_toggle_auto_approve"]:
            try: c.execute(f"ALTER TABLE subadmin_perms ADD COLUMN {perm} INTEGER DEFAULT 0")
            except: pass
        try:
            c.execute("ALTER TABLE users ADD COLUMN username TEXT")
            c.execute("ALTER TABLE users ADD COLUMN first_name TEXT")
            c.execute("ALTER TABLE users ADD COLUMN last_name TEXT")
        except: pass
        try: c.execute("ALTER TABLE post_sequence ADD COLUMN buttons_json TEXT DEFAULT '[]'")
        except: pass
        try: c.execute("CREATE TABLE IF NOT EXISTS intro (id INTEGER PRIMARY KEY CHECK (id = 1), message_text TEXT)")
        except: pass
        try: c.execute("CREATE TABLE IF NOT EXISTS direct_reply (id INTEGER PRIMARY KEY CHECK (id=1), message_id INTEGER, content_json TEXT, buttons_json TEXT DEFAULT '[]')")
        except: pass

        c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('source_chat_id', ?)", (str(SOURCE_CHAT_ID),))
        c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('auto_approve', '0')")
        c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('start_enabled', '1')")
        c.execute("INSERT OR IGNORE INTO post_sequence (id) VALUES (1)")
        c.execute("INSERT OR IGNORE INTO intro (id) VALUES (1)")
        c.execute("INSERT OR IGNORE INTO direct_reply (id) VALUES (1)")

# ── Users ──────────────────────────────────────
@retry_db
def db_upsert_user(user_id: int, username=None, first_name=None, last_name=None) -> bool:
    with get_conn() as c:
        return c.execute(
            "INSERT INTO users (user_id, username, first_name, last_name) VALUES (?,?,?,?) "
            "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name",
            (user_id, username, first_name, last_name)
        ).rowcount > 0

@retry_db
def db_total_users() -> int:
    with get_conn() as c:
        r = c.execute("SELECT COUNT(*) FROM users").fetchone()
        return r[0] if r else 0

@retry_db
def db_daily_users() -> int:
    with get_conn() as c:
        r = c.execute("SELECT COUNT(*) FROM users WHERE first_seen = ?", (date.today().isoformat(),)).fetchone()
        return r[0] if r else 0

@retry_db
def db_all_user_ids() -> list:
    with get_conn() as c:
        rows = c.execute("SELECT user_id FROM users").fetchall()
        return [r["user_id"] for r in rows] if rows else []

# ── Roles & Admins ────────────────────────────
def is_main_admin(uid): return uid == ADMIN_ID

@retry_db
def db_get_admin_role(uid):
    with get_conn() as c:
        row = c.execute("SELECT role FROM subadmins WHERE user_id = ?", (uid,)).fetchone()
        return row["role"] if row else None

def db_is_subadmin(uid): return db_get_admin_role(uid) is not None
def db_is_admin(uid): return is_main_admin(uid) or db_get_admin_role(uid) == "admin"
def is_any_admin(uid): return is_main_admin(uid) or db_is_subadmin(uid)

@retry_db
def db_add_admin(uid, role="subadmin"):
    with get_conn() as c:
        try:
            c.execute("INSERT INTO subadmins (user_id, role) VALUES (?,?)", (uid, role))
            c.execute("INSERT INTO subadmin_perms (user_id) VALUES (?)", (uid,))
            if role == "admin":
                c.execute("UPDATE subadmin_perms SET can_approve_requests=1, can_toggle_auto_approve=1 WHERE user_id=?", (uid,))
            return True
        except sqlite3.IntegrityError:
            return False

@retry_db
def db_remove_subadmin(uid):
    with get_conn() as c:
        return c.execute("DELETE FROM subadmins WHERE user_id=?", (uid,)).rowcount > 0

@retry_db
def db_list_admins(role_filter=None):
    with get_conn() as c:
        if role_filter:
            rows = c.execute("SELECT user_id, role FROM subadmins WHERE role=?", (role_filter,)).fetchall()
        else:
            rows = c.execute("SELECT user_id, role FROM subadmins").fetchall()
        return rows if rows else []

def db_get_all_admin_ids():
    ids = [ADMIN_ID]
    ids.extend(r["user_id"] for r in db_list_admins())
    return ids

# ── Permissions ───────────────────────────────
PERMISSIONS = [
    "can_broadcast","can_stats","can_manage_seq","can_change_source",
    "can_set_post_button","can_manage_subadmins","can_manage_bot_profile",
    "can_test_sequence","can_approve_requests","can_toggle_auto_approve"
]
PERM_DISPLAY = {
    "can_broadcast":"📢 Broadcast","can_stats":"📊 Stats",
    "can_manage_seq":"📨 Manage Sequence","can_change_source":"📡 Change Source",
    "can_set_post_button":"🔘 Set Post Button","can_manage_subadmins":"👥 Manage Subadmins",
    "can_manage_bot_profile":"🤖 Bot Profile","can_test_sequence":"🧪 Test Sequence",
    "can_approve_requests":"✅ Approve All Requests","can_toggle_auto_approve":"🔄 Auto‑Approve Toggle"
}

@retry_db
def db_get_subadmin_perms(uid):
    with get_conn() as c:
        row = c.execute("SELECT * FROM subadmin_perms WHERE user_id=?", (uid,)).fetchone()
        return {k:bool(row[k]) for k in row.keys() if k!="user_id"} if row else {}

@retry_db
def db_set_subadmin_perm(uid, perm, value):
    with get_conn() as c:
        c.execute(f"UPDATE subadmin_perms SET {perm}=? WHERE user_id=?", (int(value), uid))

def db_has_perm(uid, perm):
    if is_main_admin(uid): return True
    return db_get_subadmin_perms(uid).get(perm, False)

# ── Auto‑approve, Start toggle ────────────────
@retry_db
def db_get_auto_approve():
    with get_conn() as c:
        row = c.execute("SELECT value FROM config WHERE key='auto_approve'").fetchone()
        return row and row["value"]=="1"
@retry_db
def db_set_auto_approve(value):
    with get_conn() as c:
        c.execute("UPDATE config SET value=? WHERE key='auto_approve'", ("1" if value else "0",))
@retry_db
def db_get_start_enabled():
    with get_conn() as c:
        row = c.execute("SELECT value FROM config WHERE key='start_enabled'").fetchone()
        return row["value"]=="1" if row else True
@retry_db
def db_set_start_enabled(value):
    with get_conn() as c:
        c.execute("UPDATE config SET value=? WHERE key='start_enabled'", ("1" if value else "0",))

# ── Pending requests ──────────────────────────
@retry_db
def db_add_pending_request(uid, chat_id):
    with get_conn() as c:
        c.execute("INSERT OR IGNORE INTO pending_requests (user_id,chat_id) VALUES (?,?)", (uid,chat_id))
@retry_db
def db_get_pending_requests():
    with get_conn() as c:
        rows = c.execute("SELECT user_id, chat_id FROM pending_requests").fetchall()
        return rows if rows else []
@retry_db
def db_clear_pending_requests():
    with get_conn() as c:
        c.execute("DELETE FROM pending_requests")

# ── Intro / Direct reply ──────────────────────
@retry_db
def db_get_intro():
    with get_conn() as c:
        row = c.execute("SELECT message_text FROM intro WHERE id=1").fetchone()
        return row["message_text"] if row and row["message_text"] else None
@retry_db
def db_set_intro(text):
    with get_conn() as c:
        c.execute("UPDATE intro SET message_text=? WHERE id=1", (text,))

@retry_db
def db_get_direct_reply():
    with get_conn() as c:
        row = c.execute("SELECT * FROM direct_reply WHERE id=1").fetchone()
        return dict(row) if row else {}
@retry_db
def db_set_direct_reply(message_id, content_json):
    with get_conn() as c:
        c.execute("UPDATE direct_reply SET message_id=?, content_json=?, buttons_json='[]' WHERE id=1", (message_id,content_json))
@retry_db
def db_clear_direct_reply():
    with get_conn() as c:
        c.execute("UPDATE direct_reply SET message_id=NULL, content_json=NULL, buttons_json='[]' WHERE id=1")

# ── Message sequence ──────────────────────────
@retry_db
def db_add_message(message_id, position, content_json=None, buttons_json='[]'):
    with get_conn() as c:
        try:
            c.execute("INSERT OR REPLACE INTO messages (message_id,position,content_json,buttons_json) VALUES (?,?,?,?)",
                      (message_id,position,content_json,buttons_json))
            return True
        except sqlite3.IntegrityError:
            return False
@retry_db
def db_remove_message(message_id):
    with get_conn() as c:
        return c.execute("DELETE FROM messages WHERE message_id=?", (message_id,)).rowcount>0
@retry_db
def db_remove_message_pos(position):
    with get_conn() as c:
        return c.execute("DELETE FROM messages WHERE position=?", (position,)).rowcount>0
@retry_db
def db_get_messages():
    with get_conn() as c:
        rows = c.execute("SELECT * FROM messages ORDER BY position ASC").fetchall()
        return rows if rows else []
@retry_db
def db_reorder_message(message_id, new_position):
    with get_conn() as c:
        c.execute("UPDATE messages SET position=-1 WHERE position=? AND message_id!=?", (new_position,message_id))
        ok = c.execute("UPDATE messages SET position=? WHERE message_id=?", (new_position,message_id)).rowcount>0
        c.execute("DELETE FROM messages WHERE position=-1")
        return ok
@retry_db
def db_update_message_buttons(message_id, buttons_json):
    with get_conn() as c:
        c.execute("UPDATE messages SET buttons_json=? WHERE message_id=?", (buttons_json, message_id))
@retry_db
def db_get_message_by_id(message_id):
    with get_conn() as c:
        row = c.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
        return dict(row) if row else None

# ── Source / Config ───────────────────────────
@retry_db
def db_get_source_chat_id():
    with get_conn() as c:
        row = c.execute("SELECT value FROM config WHERE key='source_chat_id'").fetchone()
        return int(row["value"]) if row else SOURCE_CHAT_ID
@retry_db
def db_set_source_chat_id(chat_id):
    with get_conn() as c:
        c.execute("UPDATE config SET value=? WHERE key='source_chat_id'", (str(chat_id),))

# ── Post‑sequence custom message ──────────────
@retry_db
def db_get_post_sequence():
    with get_conn() as c:
        row = c.execute("SELECT message_text, buttons_json FROM post_sequence WHERE id=1").fetchone()
        return dict(row) if row else {}
@retry_db
def db_set_post_sequence(message_text, buttons_json='[]'):
    with get_conn() as c:
        c.execute("UPDATE post_sequence SET message_text=?, buttons_json=? WHERE id=1", (message_text,buttons_json))

# ── State machine ─────────────────────────────
@retry_db
def db_set_state(uid, action, data=""):
    with get_conn() as c:
        c.execute("INSERT OR REPLACE INTO state (user_id,action,data) VALUES (?,?,?)", (uid,action,data))
@retry_db
def db_get_state(uid):
    with get_conn() as c:
        row = c.execute("SELECT action, data FROM state WHERE user_id=?", (uid,)).fetchone()
        return (row["action"], row["data"]) if row else (None, None)
@retry_db
def db_clear_state(uid):
    with get_conn() as c:
        c.execute("DELETE FROM state WHERE user_id=?", (uid,))

# ═══════════════════════════════════════════════════════════════════
# API HELPERS (original button style preserved)
# ═══════════════════════════════════════════════════════════════════

def _post(endpoint, payload):
    try:
        r = requests.post(f"{API_URL}/{endpoint}", json=payload, timeout=20)
        r.raise_for_status()
        return r.json()
    except:
        return None

def _build_inline_keyboard(buttons_json: str):
    if not buttons_json or buttons_json=="[]":
        return None
    try:
        buttons = json.loads(buttons_json)
    except:
        return None
    if not buttons:
        return None
    rows = {}
    for btn in buttons:
        rows.setdefault(btn.get("row",0), []).append(btn)
    inline_keyboard = []
    for idx in sorted(rows.keys()):
        row_btns = []
        for btn in rows[idx]:
            d = {"text": btn["text"], "url": btn["url"]}
            if "style" in btn:
                d["style"] = btn["style"]
            row_btns.append(d)
        inline_keyboard.append(row_btns)
    return inline_keyboard

def send_message_with_buttons(chat_id, text, buttons_json, parse_mode="Markdown"):
    """Send a plain text message with inline buttons. (Not used for sequence delivery to preserve premium emoji.)"""
    kb = _build_inline_keyboard(buttons_json)
    payload = {"chat_id":chat_id, "text":text, "parse_mode":parse_mode}
    if kb:
        payload["reply_markup"] = {"inline_keyboard":kb}
    return _post("sendMessage", payload)

def copy_message_with_buttons(from_chat_id, message_id, to_chat_id, buttons_json):
    """Copy a message (preserving all formatting/premium emoji) and then attach buttons."""
    resp = _post("copyMessage", {
        "chat_id":to_chat_id,
        "from_chat_id":from_chat_id,
        "message_id":message_id
    })
    if not resp or not resp.get("ok"):
        return resp
    new_msg_id = resp["result"]["message_id"]
    kb = _build_inline_keyboard(buttons_json)
    if not kb:
        return resp
    _post("editMessageReplyMarkup", {
        "chat_id":to_chat_id,
        "message_id":new_msg_id,
        "reply_markup":{"inline_keyboard":kb}
    })
    return resp

# ═══════════════════════════════════════════════════════════════════
# KEYBOARDS
# ═══════════════════════════════════════════════════════════════════

def admin_panel_kb():
    auto = "ON ✅" if db_get_auto_approve() else "OFF ❌"
    start = "ON ✅" if db_get_start_enabled() else "OFF ❌"
    return ReplyKeyboardMarkup([
        ["📢 Broadcast","📊 Stats"],
        ["👑 Admins","👥 Subadmins"],
        ["📨 Message Sequence","✅ Approve All Requests"],
        ["📡 Change Source Channel", f"🔄 Auto‑Approve: {auto}"],
        ["🔘 Set Post Button","🗑 Remove Post Button"],
        ["⚙️ Subadmin Permissions","🤖 Bot Profile"],
        ["🧪 Test Sequence", f"🔄 Start: {start}"]
    ], resize_keyboard=True)

def subadmin_panel_kb(uid):
    perms = db_get_subadmin_perms(uid)
    role = db_get_admin_role(uid)
    btns = []
    if perms.get("can_broadcast"): btns.append(["📢 Broadcast"])
    if perms.get("can_stats"): btns.append(["📊 Stats"])
    if perms.get("can_manage_seq"): btns.append(["📨 Message Sequence"])
    if perms.get("can_change_source"): btns.append(["📡 Change Source Channel"])
    if perms.get("can_set_post_button"): btns.append(["🔘 Set Post Button","🗑 Remove Post Button"])
    if role=="admin" and perms.get("can_manage_subadmins"): btns.append(["👥 Subadmins"])
    if perms.get("can_manage_bot_profile"): btns.append(["🤖 Bot Profile"])
    if perms.get("can_test_sequence"): btns.append(["🧪 Test Sequence"])
    if perms.get("can_approve_requests"): btns.append(["✅ Approve All Requests"])
    if perms.get("can_toggle_auto_approve"):
        auto = "ON ✅" if db_get_auto_approve() else "OFF ❌"
        btns.append([f"🔄 Auto‑Approve: {auto}"])
    if not btns: btns = [["ℹ️ No permissions"]]
    return ReplyKeyboardMarkup(btns, resize_keyboard=True)

def staff_kb(uid):
    return admin_panel_kb() if is_main_admin(uid) else subadmin_panel_kb(uid)

def sequence_panel_kb():
    intro_set = "✏️ Set Intro Msg" if not db_get_intro() else "✏️ Update Intro Msg"
    reply = db_get_direct_reply()
    reply_set = "✉️ Set Reply Msg" if not reply.get("message_id") else "✉️ Update Reply Msg"
    return ReplyKeyboardMarkup([
        [intro_set],
        ["➕ Add Message","➖ Remove Message"],
        ["🔀 Reorder Message","📄 List Messages"],
        [reply_set,"✉️ Remove Reply Msg"],
        ["🔙 Back to Panel"]
    ], resize_keyboard=True)

def bot_profile_kb():
    return ReplyKeyboardMarkup([
        ["🏷 Change Name","📝 Change Bio"],
        ["📄 Change Description","🖼 Change Profile Photo"],
        ["🔙 Back to Panel"]
    ], resize_keyboard=True)

def cancel_kb():
    return ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)

# ═══════════════════════════════════════════════════════════════════
# HELPER
# ═══════════════════════════════════════════════════════════════════

async def run(func, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(func, *args))

def extract_message_content(msg):
    data = {}
    if msg.text: data["type"]="text"; data["text"]=msg.text
    elif msg.caption: data["type"]="caption"; data["caption"]=msg.caption
    else: data["type"]="media"
    if msg.photo: data["photo"]=msg.photo[-1].file_id
    if msg.video: data["video"]=msg.video.file_id
    if msg.document: data["document"]=msg.document.file_id
    if msg.audio: data["audio"]=msg.audio.file_id
    if msg.voice: data["voice"]=msg.voice.file_id
    if msg.sticker: data["sticker"]=msg.sticker.file_id
    if msg.animation: data["animation"]=msg.animation.file_id
    if msg.video_note: data["video_note"]=msg.video_note.file_id
    if msg.caption and msg.caption.strip(): data["caption"]=msg.caption
    return json.dumps(data)

PLACEHOLDER_HELP = (
    "Available placeholders:\n"
    "{first_name} – user's first name\n"
    "{last_name} – user's last name\n"
    "{username} – @username (or first name if no username)\n"
    "{id} – numeric user ID"
)

# ═══════════════════════════════════════════════════════════════════
# SEQUENCE DELIVERY – Premium Emoji Safe ✅
# ═══════════════════════════════════════════════════════════════════

async def send_sequence_to_user(bot, user_id: int):
    """
    Sends intro -> sequence messages -> post-sequence, preserving ALL formatting.
    For any message with buttons, we always COPY the original message (keeping
    premium emoji, fonts, etc.) and then attach the buttons via editMessageReplyMarkup.
    """
    try:
        user = await bot.get_chat(user_id)
    except:
        return

    # 1. Optional intro message
    intro = await run(db_get_intro)
    if intro:
        t = intro.replace("{first_name}", user.first_name or "").replace("{last_name}", user.last_name or "").replace("{username}", user.username or user.first_name or "").replace("{id}", str(user_id))
        try: await bot.send_message(chat_id=user_id, text=t)
        except: pass

    source = await run(db_get_source_chat_id)

    # 2. Sequence messages
    for row in await run(db_get_messages):
        # IF the message has buttons -> always copy + edit (preserves everything)
        if row["buttons_json"] and row["buttons_json"] != "[]":
            resp = await run(copy_message_with_buttons, source, row["message_id"], user_id, row["buttons_json"])
            if resp and resp.get("ok"):
                continue
        # ELSE: simple copy (no buttons, keeps all formatting)
        try:
            await bot.copy_message(chat_id=user_id, from_chat_id=source, message_id=row["message_id"])
        except Forbidden:
            break
        except:
            continue

    # 3. Post‑sequence custom message
    post = await run(db_get_post_sequence)
    if post.get("message_text"):
        t = post["message_text"].replace("{first_name}", user.first_name or "").replace("{last_name}", user.last_name or "").replace("{username}", user.username or user.first_name or "").replace("{id}", str(user_id))
        kb = _build_inline_keyboard(post.get("buttons_json","[]"))
        markup = InlineKeyboardMarkup([[InlineKeyboardButton(**b) for b in row] for row in kb]) if kb else None
        try:
            await bot.send_message(chat_id=user_id, text=t, reply_markup=markup, parse_mode="Markdown")
        except:
            pass

# ═══════════════════════════════════════════════════════════════════
# HANDLERS (start, join, stats, help, etc.)
# ═══════════════════════════════════════════════════════════════════

async def cmd_start(update, context):
    user = update.effective_user
    if not user: return
    await run(db_upsert_user, user.id, user.username, user.first_name, user.last_name)
    await run(db_clear_state, user.id)
    if not await run(db_get_start_enabled):
        try: await update.message.reply_text("👋 Hello!")
        except: pass
        return
    if is_any_admin(user.id):
        await open_panel(update, user.id)
    else:
        await send_sequence_to_user(context.bot, user.id)

async def on_join_request(update, context):
    jr = update.chat_join_request
    if not jr: return
    user = jr.from_user
    if not user: return
    await run(db_upsert_user, user.id, user.username, user.first_name, user.last_name)
    await send_sequence_to_user(context.bot, user.id)
    if await run(db_get_auto_approve):
        try: await context.bot.approve_chat_join_request(chat_id=jr.chat.id, user_id=user.id)
        except: pass
    else:
        await run(db_add_pending_request, user.id, jr.chat.id)

async def cmd_stats(update, context):
    user = update.effective_user
    if not user or not is_any_admin(user.id): return
    if not db_has_perm(user.id, "can_stats"):
        await update.message.reply_text("⛔ No permission.")
        return
    total = await run(db_total_users)
    daily = await run(db_daily_users)
    pending = len(await run(db_get_pending_requests))
    auto = "ON ✅" if await run(db_get_auto_approve) else "OFF ❌"
    try:
        await update.message.reply_text(
            f"📊 *Stats*\n👥 Total: `{total}`\n🗓 Today: `{daily}`\n⏳ Pending: `{pending}`\n🔄 Auto: `{auto}`",
            parse_mode="Markdown"
        )
    except: pass

async def cmd_help(update, context):
    try:
        await update.message.reply_text("🤖 *Bot Help*\n\nUse /start to begin. Admin commands are on the menu panel.", parse_mode="Markdown")
    except: pass

async def open_panel(update, uid, note=""):
    await run(db_clear_state, uid)
    if is_main_admin(uid):
        txt = f"{note}\n\n👑 *SUPER ADMIN* — CHOOSE AN ACTION:" if note else "👑 *SUPER ADMIN* — CHOOSE AN ACTION:"
        kb = admin_panel_kb()
    elif await run(db_is_subadmin, uid):
        role = await run(db_get_admin_role, uid)
        title = "ADMIN" if role=="admin" else "SUBADMIN"
        txt = f"{note}\n\n🛠 *{title} PANEL* — CHOOSE AN ACTION:" if note else f"🛠 *{title} PANEL* — CHOOSE AN ACTION:"
        kb = subadmin_panel_kb(uid)
    else: return
    try: await update.message.reply_text(txt.strip(), parse_mode="Markdown", reply_markup=kb)
    except: pass

# ───── Concurrent broadcast (non‑blocking) ─────
async def _broadcast_one(uid, source_msg, bot, text=None):
    for attempt in range(MAX_RETRIES_API):
        try:
            if text: await bot.send_message(chat_id=uid, text=text)
            else: await source_msg.copy(chat_id=uid)
            return "sent"
        except Forbidden: return "blocked"
        except (TelegramError, NetworkError, TimedOut):
            if attempt < MAX_RETRIES_API-1: await asyncio.sleep(0.5)
            else: return "failed"
        except: return "failed"
    return "failed"

async def concurrent_broadcast(source_msg, bot, text=None):
    uids = await run(db_all_user_ids)
    if not uids: return 0,0,0
    sem = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    async def worker(uid):
        async with sem: return await _broadcast_one(uid, source_msg, bot, text)
    tasks = [asyncio.create_task(worker(uid)) for uid in uids]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    sent = results.count("sent")
    blocked = results.count("blocked")
    failed = len(results) - sent - blocked
    return sent, blocked, failed

# ───── Subadmin callbacks ─────
async def subadmin_list_callback(update, context):
    q = update.callback_query
    await q.answer()
    if not is_main_admin(q.from_user.id):
        try: await q.edit_message_text("⛔ Only main admin.")
        except: pass
        return
    subs = await run(db_list_admins)
    if not subs:
        try: await q.edit_message_text("ℹ️ No subadmins.")
        except: pass
        return
    kb = [[InlineKeyboardButton(f"👤 {s['user_id']} ({s['role'].upper()})", callback_data=f"perm_sub_{s['user_id']}")] for s in subs]
    kb.append([InlineKeyboardButton("🔙 Close", callback_data="perm_close")])
    try: await q.edit_message_text("⚙️ *Select subadmin:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
    except: pass

async def subadmin_perm_menu_callback(update, context):
    q = update.callback_query
    await q.answer()
    if not is_main_admin(q.from_user.id):
        try: await q.edit_message_text("⛔ Only main admin.")
        except: pass
        return
    try: sub_id = int(q.data.split("_")[2])
    except: return
    perms = await run(db_get_subadmin_perms, sub_id)
    if not perms:
        try: await q.edit_message_text("ℹ️ Not found.")
        except: pass
        return
    role = await run(db_get_admin_role, sub_id)
    kb = []
    for p in PERMISSIONS:
        status = "✅" if perms.get(p) else "❌"
        kb.append([InlineKeyboardButton(f"{status} {PERM_DISPLAY[p]}", callback_data=f"perm_toggle_{sub_id}_{p}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="perm_list"), InlineKeyboardButton("🔙 Close", callback_data="perm_close")])
    try: await q.edit_message_text(f"⚙️ *Permissions for {role.upper()}* `{sub_id}`", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
    except: pass

async def perm_toggle_callback(update, context):
    q = update.callback_query
    await q.answer()
    if not is_main_admin(q.from_user.id):
        try: await q.edit_message_text("⛔ Only main admin.")
        except: pass
        return
    parts = q.data.split("_")
    sub_id = int(parts[2])
    perm = "_".join(parts[3:])
    perms = await run(db_get_subadmin_perms, sub_id)
    if perm not in perms:
        await q.answer("Invalid.", show_alert=True)
        return
    new_val = not perms[perm]
    await run(db_set_subadmin_perm, sub_id, perm, new_val)
    perms = await run(db_get_subadmin_perms, sub_id)
    role = await run(db_get_admin_role, sub_id)
    kb = []
    for p in PERMISSIONS:
        status = "✅" if perms.get(p) else "❌"
        kb.append([InlineKeyboardButton(f"{status} {PERM_DISPLAY[p]}", callback_data=f"perm_toggle_{sub_id}_{p}")])
    kb.append([InlineKeyboardButton("🔙 Back", callback_data="perm_list"), InlineKeyboardButton("🔙 Close", callback_data="perm_close")])
    try: await q.edit_message_text(f"⚙️ *{role.upper()}* `{sub_id}` – `{perm}` is now {'✅ ON' if new_val else '❌ OFF'}.", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
    except: pass

async def perm_close_callback(update, context):
    q = update.callback_query
    await q.answer()
    try: await q.delete_message()
    except: pass

# ═══════════════════════════════════════════════════════════════════
# MAIN MESSAGE HANDLER (all admin logic + state machine)
# ═══════════════════════════════════════════════════════════════════

async def on_message(update, context):
    msg = update.message
    user = update.effective_user
    if not user or not msg: return
    uid = user.id
    text = (msg.text or msg.caption or "").strip()

    if not is_any_admin(uid):
        reply = await run(db_get_direct_reply)
        if reply.get("message_id"):
            source = await run(db_get_source_chat_id)
            try: await context.bot.copy_message(chat_id=uid, from_chat_id=source, message_id=reply["message_id"])
            except: pass
        return

    action, data = await run(db_get_state, uid)

    if text == "❌ Cancel":
        if is_any_admin(uid): await open_panel(update, uid, "↩️ Cancelled.")
        else:
            await run(db_clear_state, uid)
            await msg.reply_text("↩️ Cancelled.", reply_markup=ReplyKeyboardRemove())
        return

    # ── Broadcast ──
    if action == "awaiting_broadcast":
        if not db_has_perm(uid, "can_broadcast"): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        try: await msg.reply_text("✅ Broadcast started.\nThis message is being sent in the background.", reply_markup=staff_kb(uid))
        except: pass
        asyncio.create_task(concurrent_broadcast(msg, context.bot))
        return

    # ── Admin add/remove states ──
    if action == "awaiting_add_admin":
        if not is_main_admin(uid): await open_panel(update, uid, "⛔ Superadmin only."); return
        await run(db_clear_state, uid)
        reply = _handle_add_remove_admin(uid, text, "admin")
        await open_panel(update, uid, reply); return
    if action == "awaiting_add_subadmin":
        if not (is_main_admin(uid) or db_is_admin(uid)): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        reply = _handle_add_remove_admin(uid, text, "subadmin")
        await open_panel(update, uid, reply); return
    if action in ("awaiting_remove_admin", "awaiting_remove_subadmin"):
        if action == "awaiting_remove_admin" and not is_main_admin(uid): await open_panel(update, uid, "⛔ Superadmin only."); return
        if action == "awaiting_remove_subadmin" and not (is_main_admin(uid) or db_is_admin(uid)): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        reply = _handle_remove_admin(uid, text)
        await open_panel(update, uid, reply); return

    # ── Intro ──
    if action == "awaiting_set_intro":
        if not db_has_perm(uid,"can_manage_seq"): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        await run(db_set_intro, text)
        await open_panel(update, uid, "✅ Intro message updated.\n\n"+PLACEHOLDER_HELP); return

    # ── Direct reply ──
    if action == "awaiting_set_reply":
        if not db_has_perm(uid,"can_manage_seq"): await open_panel(update, uid, "⛔ No permission."); return
        source = await run(db_get_source_chat_id)
        try:
            sent = await msg.forward(chat_id=source); mid = sent.message_id
        except: await open_panel(update, uid, "❌ Could not forward to source."); return
        content = extract_message_content(msg)
        await run(db_set_direct_reply, mid, content)
        await run(db_clear_state, uid)
        await _open_sequence_panel(update, uid, "✅ Reply message saved."); return
    if action == "awaiting_confirm_remove_reply":
        if not db_has_perm(uid,"can_manage_seq"): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        if text.lower()!="yes": await _open_sequence_panel(update, uid, "↩️ Removal cancelled."); return
        await run(db_clear_direct_reply)
        await _open_sequence_panel(update, uid, "✅ Reply message removed."); return

    # ── Add message (with button flow) ──
    if action == "awaiting_addmsg_pos":
        if not db_has_perm(uid,"can_manage_seq"): await open_panel(update, uid, "⛔ No permission."); return
        try:
            pos = int(text)
            if pos<1: await msg.reply_text("❌ Position ≥ 1.", reply_markup=cancel_kb()); return
            await run(db_set_state, uid, "awaiting_addmsg_msg", str(pos))
            await msg.reply_text(f"✅ Position `{pos}`. Now send the message.", parse_mode="Markdown", reply_markup=cancel_kb())
        except ValueError: await msg.reply_text("❌ Send a number.", reply_markup=cancel_kb())
        return

    if action == "awaiting_addmsg_msg":
        if not db_has_perm(uid,"can_manage_seq"): await open_panel(update, uid, "⛔ No permission."); return
        try: pos = int(data)
        except: await run(db_clear_state, uid); await open_panel(update, uid, "❌ State error."); return
        source = await run(db_get_source_chat_id)
        # Plain text messages are posted directly to preserve premium emoji entities
        if msg.text and not any([msg.photo, msg.video, msg.document, msg.animation]):
            try:
                sent = await context.bot.send_message(chat_id=source, text=msg.text, entities=msg.entities, disable_web_page_preview=True)
                mid = sent.message_id
            except: await open_panel(update, uid, "❌ Could not send to source."); return
        else:
            try:
                sent = await msg.forward(chat_id=source); mid = sent.message_id
            except: await open_panel(update, uid, "❌ Could not forward to source."); return
        content = extract_message_content(msg)
        await run(db_add_message, mid, pos, content, "[]")
        state = {"mid":mid, "pos":pos, "buttons":[], "current_row":0}
        await run(db_set_state, uid, "awaiting_addmsg_btn_text", json.dumps(state))
        await msg.reply_text("Do you want to add a button? (Yes/No)", reply_markup=ReplyKeyboardMarkup([["✅ Yes","❌ No"]], resize_keyboard=True, one_time_keyboard=True))
        return

    # (Button creation flow – unchanged, works fine)
    if action == "awaiting_addmsg_btn_text":
        state = json.loads(data)
        if text == "❌ No":
            btns_json = json.dumps(state.get("buttons",[]))
            await run(db_update_message_buttons, state["mid"], btns_json)
            await run(db_clear_state, uid)
            await open_panel(update, uid, f"✅ Message at position {state['pos']} saved with {len(state['buttons'])} button(s)."); return
        if text == "✅ Yes":
            await run(db_set_state, uid, "awaiting_addmsg_btn_text_input", json.dumps(state))
            await msg.reply_text("Send the button text:", reply_markup=cancel_kb()); return
        await msg.reply_text("Choose ✅ Yes or ❌ No.", reply_markup=ReplyKeyboardMarkup([["✅ Yes","❌ No"]], resize_keyboard=True, one_time_keyboard=True)); return
    if action == "awaiting_addmsg_btn_text_input":
        state = json.loads(data); state["current_btn_text"]=text
        await run(db_set_state, uid, "awaiting_addmsg_btn_url", json.dumps(state))
        await msg.reply_text("Now send the button URL:", reply_markup=cancel_kb()); return
    if action == "awaiting_addmsg_btn_url":
        state = json.loads(data); state["current_btn_url"]=text
        await run(db_set_state, uid, "awaiting_addmsg_btn_color", json.dumps(state))
        kb = ReplyKeyboardMarkup([["🔵 Blue","🔴 Red"],["🟢 Green","⚪ Default"]], resize_keyboard=True, one_time_keyboard=True)
        await msg.reply_text("Select button colour:", reply_markup=kb); return
    if action == "awaiting_addmsg_btn_color":
        state = json.loads(data)
        cmap = {"🔵 Blue":"primary","🔴 Red":"danger","🟢 Green":"success","⚪ Default":"default"}
        style = cmap.get(text)
        if not style:
            await msg.reply_text("⚠️ Choose valid colour.", reply_markup=ReplyKeyboardMarkup([["🔵 Blue","🔴 Red"],["🟢 Green","⚪ Default"]], resize_keyboard=True, one_time_keyboard=True)); return
        btn = {"text":state["current_btn_text"],"url":state["current_btn_url"],"style":style,"row":state["current_row"]}
        state.setdefault("buttons",[]).append(btn)
        await run(db_set_state, uid, "awaiting_addmsg_another", json.dumps(state))
        await msg.reply_text("Button added! Add another?", reply_markup=ReplyKeyboardMarkup([["✅ Done","➕ Add Another"]], resize_keyboard=True, one_time_keyboard=True)); return
    if action == "awaiting_addmsg_another":
        state = json.loads(data)
        if text == "✅ Done":
            btns_json = json.dumps(state["buttons"])
            await run(db_update_message_buttons, state["mid"], btns_json)
            await run(db_clear_state, uid)
            await open_panel(update, uid, f"✅ Message at position {state['pos']} saved with {len(state['buttons'])} button(s)."); return
        if text == "➕ Add Another":
            await run(db_set_state, uid, "awaiting_addmsg_row_choice", json.dumps(state))
            await msg.reply_text("Same row or new row?", reply_markup=ReplyKeyboardMarkup([["📌 Same Row","📋 Next Row"]], resize_keyboard=True, one_time_keyboard=True)); return
        await msg.reply_text("Choose Done or Add Another.", reply_markup=ReplyKeyboardMarkup([["✅ Done","➕ Add Another"]], resize_keyboard=True, one_time_keyboard=True)); return
    if action == "awaiting_addmsg_row_choice":
        state = json.loads(data)
        if text == "📋 Next Row": state["current_row"] = state.get("current_row",0)+1
        await run(db_set_state, uid, "awaiting_addmsg_btn_text_input", json.dumps(state))
        await msg.reply_text("Send the button text:", reply_markup=cancel_kb()); return

    # ── Set Post Button (multi‑button) ──
    if action == "awaiting_set_post":
        if not db_has_perm(uid,"can_set_post_button"): await open_panel(update, uid, "⛔ No permission."); return
        parts = text.split("|")
        msg_text = parts[0].strip()
        btn_text = parts[1].strip() if len(parts)>1 else ""
        btn_url = parts[2].strip() if len(parts)>2 else ""
        temp = {"message_text":msg_text, "buttons":[], "current_row":0}
        if btn_text and btn_url:
            temp["first_btn_text"]=btn_text; temp["first_btn_url"]=btn_url
            await run(db_set_state, uid, "awaiting_set_post_color", json.dumps(temp))
            kb = ReplyKeyboardMarkup([["🔵 Blue","🔴 Red"],["🟢 Green","⚪ Default"]], resize_keyboard=True, one_time_keyboard=True)
            await msg.reply_text("Select colour for this button:", reply_markup=kb)
        else:
            await run(db_set_post_sequence, msg_text, "[]")
            await run(db_clear_state, uid)
            await open_panel(update, uid, "✅ Post message updated (no buttons).")
        return
    if action == "awaiting_set_post_color":
        state = json.loads(data)
        cmap = {"🔵 Blue":"primary","🔴 Red":"danger","🟢 Green":"success","⚪ Default":"default"}
        style = cmap.get(text)
        if not style:
            await msg.reply_text("⚠️ Choose valid colour.", reply_markup=ReplyKeyboardMarkup([["🔵 Blue","🔴 Red"],["🟢 Green","⚪ Default"]], resize_keyboard=True, one_time_keyboard=True)); return
        btn = {"text":state["first_btn_text"],"url":state["first_btn_url"],"style":style,"row":state["current_row"]}
        state["buttons"].append(btn)
        await run(db_set_state, uid, "awaiting_set_post_another", json.dumps(state))
        await msg.reply_text("Button added! Add another?", reply_markup=ReplyKeyboardMarkup([["✅ Done","➕ Add Another"]], resize_keyboard=True, one_time_keyboard=True)); return
    if action == "awaiting_set_post_another":
        state = json.loads(data)
        if text == "✅ Done":
            btns_json = json.dumps(state["buttons"])
            await run(db_set_post_sequence, state["message_text"], btns_json)
            await run(db_clear_state, uid)
            await open_panel(update, uid, f"✅ Post message saved with {len(state['buttons'])} button(s)."); return
        if text == "➕ Add Another":
            await run(db_set_state, uid, "awaiting_set_post_row_choice", json.dumps(state))
            await msg.reply_text("Same row or new row?", reply_markup=ReplyKeyboardMarkup([["📌 Same Row","📋 Next Row"]], resize_keyboard=True, one_time_keyboard=True)); return
        await msg.reply_text("Choose Done or Add Another.", reply_markup=ReplyKeyboardMarkup([["✅ Done","➕ Add Another"]], resize_keyboard=True, one_time_keyboard=True)); return
    if action == "awaiting_set_post_row_choice":
        state = json.loads(data)
        if text == "📋 Next Row": state["current_row"] = state.get("current_row",0)+1
        await run(db_set_state, uid, "awaiting_set_post_btn_text", json.dumps(state))
        await msg.reply_text("Send the button text:", reply_markup=cancel_kb()); return
    if action == "awaiting_set_post_btn_text":
        state = json.loads(data); state["current_btn_text"]=text
        await run(db_set_state, uid, "awaiting_set_post_btn_url", json.dumps(state))
        await msg.reply_text("Now send the button URL:", reply_markup=cancel_kb()); return
    if action == "awaiting_set_post_btn_url":
        state = json.loads(data); state["current_btn_url"]=text
        await run(db_set_state, uid, "awaiting_set_post_btn_color", json.dumps(state))
        kb = ReplyKeyboardMarkup([["🔵 Blue","🔴 Red"],["🟢 Green","⚪ Default"]], resize_keyboard=True, one_time_keyboard=True)
        await msg.reply_text("Select colour:", reply_markup=kb); return
    if action == "awaiting_set_post_btn_color":
        state = json.loads(data)
        cmap = {"🔵 Blue":"primary","🔴 Red":"danger","🟢 Green":"success","⚪ Default":"default"}
        style = cmap.get(text)
        if not style:
            await msg.reply_text("⚠️ Choose valid colour.", reply_markup=ReplyKeyboardMarkup([["🔵 Blue","🔴 Red"],["🟢 Green","⚪ Default"]], resize_keyboard=True, one_time_keyboard=True)); return
        btn = {"text":state["current_btn_text"],"url":state["current_btn_url"],"style":style,"row":state["current_row"]}
        state["buttons"].append(btn)
        await run(db_set_state, uid, "awaiting_set_post_another", json.dumps(state))
        await msg.reply_text("Button added! Add another?", reply_markup=ReplyKeyboardMarkup([["✅ Done","➕ Add Another"]], resize_keyboard=True, one_time_keyboard=True)); return

    # ── Remove post button ──
    if action == "awaiting_remove_post_button":
        if not db_has_perm(uid,"can_set_post_button"): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        if text.lower()!="yes": await open_panel(update, uid, "↩️ Removal cancelled."); return
        post = await run(db_get_post_sequence)
        await run(db_set_post_sequence, post.get("message_text",""), "[]")
        await open_panel(update, uid, "✅ Post button(s) removed."); return

    # ── Remove message by position ──
    if action == "awaiting_removemsg_pos":
        if not db_has_perm(uid,"can_manage_seq"): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        try:
            pos = int(text)
            ok = await run(db_remove_message_pos, pos)
            reply = f"✅ Message at position {pos} removed." if ok else f"ℹ️ No message at position {pos}."
        except ValueError: reply = "❌ Invalid position. Send a number."
        await _open_sequence_panel(update, uid, reply); return

    # ── Reorder ──
    if action == "awaiting_reordermsg":
        if not db_has_perm(uid,"can_manage_seq"): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        parts = text.split()
        try:
            mid, new_pos = int(parts[0]), int(parts[1])
            ok = await run(db_reorder_message, mid, new_pos)
            reply = f"✅ Moved." if ok else f"ℹ️ Not found."
        except (ValueError, IndexError): reply = "❌ Expected: <message_id> <new_position>"
        await _open_sequence_panel(update, uid, reply); return

    if action == "awaiting_change_source":
        if not db_has_perm(uid,"can_change_source"): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        try:
            new_id = int(text); await run(db_set_source_chat_id, new_id); reply = "✅ Source updated."
        except ValueError: reply = "❌ Invalid ID."
        await open_panel(update, uid, reply); return

    # ── Bot profile actions ──
    if action == "awaiting_bot_name":
        if not db_has_perm(uid,"can_manage_bot_profile"): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        try: await context.bot.set_my_name(name=text); reply="✅ Name updated."
        except Exception as e: reply=f"❌ {e}"
        await _open_bot_profile_panel(update, uid, reply); return
    if action == "awaiting_bot_bio":
        if not db_has_perm(uid,"can_manage_bot_profile"): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        try: await context.bot.set_my_description(description=text); reply="✅ Bio updated."
        except Exception as e: reply=f"❌ {e}"
        await _open_bot_profile_panel(update, uid, reply); return
    if action == "awaiting_bot_description":
        if not db_has_perm(uid,"can_manage_bot_profile"): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        try: await context.bot.set_my_short_description(short_description=text); reply="✅ Desc updated."
        except Exception as e: reply=f"❌ {e}"
        await _open_bot_profile_panel(update, uid, reply); return
    if action == "awaiting_bot_photo":
        if not db_has_perm(uid,"can_manage_bot_profile"): await open_panel(update, uid, "⛔ No permission."); return
        await run(db_clear_state, uid)
        if not msg.photo:
            await msg.reply_text("❌ Send a photo.", reply_markup=cancel_kb())
            await run(db_set_state, uid, "awaiting_bot_photo"); return
        source = await run(db_get_source_chat_id)
        try: await context.bot.send_photo(chat_id=source, photo=msg.photo[-1].file_id, caption="📸 Bot profile photo backup")
        except: pass
        try:
            await context.bot.set_my_photo(photo=msg.photo[-1].file_id)
            reply="✅ Photo updated (saved to source)."
        except Exception as e: reply=f"❌ {e}"
        await _open_bot_profile_panel(update, uid, reply); return

    # ═══════════════════════════════════════════════════
    # MENU BUTTONS
    # ═══════════════════════════════════════════════════
    if text == "📢 Broadcast":
        if not db_has_perm(uid,"can_broadcast"): await msg.reply_text("⛔ No permission."); return
        await run(db_set_state, uid, "awaiting_broadcast")
        await msg.reply_text("📝 Send the message to broadcast.", reply_markup=cancel_kb()); return
    if text == "📊 Stats":
        if not db_has_perm(uid,"can_stats"): await msg.reply_text("⛔ No permission."); return
        total = await run(db_total_users); daily = await run(db_daily_users)
        pending = len(await run(db_get_pending_requests))
        auto = "ON ✅" if await run(db_get_auto_approve) else "OFF ❌"
        await msg.reply_text(f"📊 *Stats*\n👥 Total: `{total}`\n🗓 Today: `{daily}`\n⏳ Pending: `{pending}`\n🔄 Auto: `{auto}`", parse_mode="Markdown", reply_markup=staff_kb(uid)); return
    if text == "👑 Admins" and is_main_admin(uid):
        rows = await run(db_list_admins,"admin")
        listing = "\n".join(f"• `{r['user_id']}`" for r in rows) if rows else "_No admins._"
        await msg.reply_text(f"👑 *Admins:*\n{listing}", parse_mode="Markdown", reply_markup=ReplyKeyboardMarkup([["➕ Add Admin","➖ Remove Admin"],["🔙 Back to Panel"]], resize_keyboard=True)); return
    if text == "➕ Add Admin" and is_main_admin(uid):
        await run(db_set_state, uid, "awaiting_add_admin"); await msg.reply_text("Send user ID to add as Admin:", reply_markup=cancel_kb()); return
    if text == "➖ Remove Admin" and is_main_admin(uid):
        rows = await run(db_list_admins,"admin")
        if not rows: await msg.reply_text("ℹ️ No admins.", reply_markup=admin_panel_kb()); return
        listing = "\n".join(f"• `{r['user_id']}`" for r in rows)
        await run(db_set_state, uid, "awaiting_remove_admin"); await msg.reply_text(f"🟡 Admins:\n{listing}\nSend ID to remove:", parse_mode="Markdown", reply_markup=cancel_kb()); return
    if text == "👥 Subadmins":
        if not (is_main_admin(uid) or db_is_admin(uid)): await msg.reply_text("⛔ No permission."); return
        rows = await run(db_list_admins) if is_main_admin(uid) else await run(db_list_admins,"subadmin")
        listing = "\n".join(f"• `{r['user_id']}` ({r['role'].capitalize()})" for r in rows) if rows else "_None._"
        await msg.reply_text(f"👥 *Subadmins:*\n{listing}", parse_mode="Markdown", reply_markup=ReplyKeyboardMarkup([["➕ Add Subadmin","➖ Remove Subadmin"],["🔙 Back to Panel"]], resize_keyboard=True)); return
    if text == "➕ Add Subadmin":
        if not (is_main_admin(uid) or db_is_admin(uid)): await msg.reply_text("⛔ No permission."); return
        await run(db_set_state, uid, "awaiting_add_subadmin"); await msg.reply_text("Send user ID to add as Subadmin:", reply_markup=cancel_kb()); return
    if text == "➖ Remove Subadmin":
        if not (is_main_admin(uid) or db_is_admin(uid)): await msg.reply_text("⛔ No permission."); return
        rows = await run(db_list_admins) if is_main_admin(uid) else await run(db_list_admins,"subadmin")
        if not rows: await msg.reply_text("ℹ️ No subadmins.", reply_markup=staff_kb(uid)); return
        listing = "\n".join(f"• `{r['user_id']}`" for r in rows)
        await run(db_set_state, uid, "awaiting_remove_subadmin"); await msg.reply_text(f"🟡 Subadmins:\n{listing}\nSend ID to remove:", parse_mode="Markdown", reply_markup=cancel_kb()); return
    if text == "✅ Approve All Requests":
        if not db_has_perm(uid,"can_approve_requests"): await msg.reply_text("⛔ No permission."); return
        pending = await run(db_get_pending_requests)
        if not pending: await msg.reply_text("ℹ️ No pending requests.", reply_markup=staff_kb(uid)); return
        status = await msg.reply_text(f"⏳ Approving {len(pending)}…")
        approved = 0
        for req in pending:
            try: await context.bot.approve_chat_join_request(chat_id=req["chat_id"], user_id=req["user_id"]); approved += 1
            except: pass
            await asyncio.sleep(0.05)
        await run(db_clear_pending_requests)
        try: await status.edit_text(f"✅ Approved {approved}/{len(pending)}.")
        except: pass
        await open_panel(update, uid); return
    if text == "📡 Change Source Channel":
        if not db_has_perm(uid,"can_change_source"): await msg.reply_text("⛔ No permission."); return
        await run(db_set_state, uid, "awaiting_change_source"); await msg.reply_text("Send new source channel ID:", reply_markup=cancel_kb()); return
    if text == "🔘 Set Post Button":
        if not db_has_perm(uid,"can_set_post_button"): await msg.reply_text("⛔ No permission."); return
        current = await run(db_get_post_sequence)
        info = f"Current: `{current.get('message_text','')}`"
        if current.get("buttons_json") and current["buttons_json"]!="[]": info += "\nHas buttons configured."
        await run(db_set_state, uid, "awaiting_set_post")
        await msg.reply_text(f"{info}\n\nSend new config:\n`Message text | Button text | Button URL`\nButton text & URL optional.", parse_mode="Markdown", reply_markup=cancel_kb()); return
    if text == "🗑 Remove Post Button":
        if not db_has_perm(uid,"can_set_post_button"): await msg.reply_text("⛔ No permission."); return
        await run(db_set_state, uid, "awaiting_remove_post_button"); await msg.reply_text("Type `yes` to confirm removal of all buttons.", reply_markup=cancel_kb()); return
    if text.startswith("🔄 Auto‑Approve:") and (is_main_admin(uid) or db_has_perm(uid,"can_toggle_auto_approve")):
        new_val = not await run(db_get_auto_approve)
        await run(db_set_auto_approve, new_val)
        await open_panel(update, uid, f"Auto‑approve {'ON ✅' if new_val else 'OFF ❌'}"); return
    if text.startswith("🔄 Start:") and is_main_admin(uid):
        new_val = not await run(db_get_start_enabled)
        await run(db_set_start_enabled, new_val)
        await open_panel(update, uid, f"/start {'ON ✅' if new_val else 'OFF ❌'}"); return
    if text == "⚙️ Subadmin Permissions" and is_main_admin(uid):
        subs = await run(db_list_admins)
        if not subs: await msg.reply_text("ℹ️ No subadmins.", reply_markup=admin_panel_kb()); return
        keyboard = [[InlineKeyboardButton(f"👤 {s['user_id']} ({s['role'].upper()})", callback_data=f"perm_sub_{s['user_id']}")] for s in subs]
        keyboard.append([InlineKeyboardButton("🔙 Close", callback_data="perm_close")])
        await msg.reply_text("⚙️ *Select subadmin:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard)); return
    if text == "🤖 Bot Profile":
        if not db_has_perm(uid,"can_manage_bot_profile"): await msg.reply_text("⛔ No permission."); return
        await _open_bot_profile_panel(update, uid); return
    if text == "🏷 Change Name":
        if not db_has_perm(uid,"can_manage_bot_profile"): return
        await run(db_set_state, uid, "awaiting_bot_name"); await msg.reply_text("✏️ Send new bot name:", reply_markup=cancel_kb()); return
    if text == "📝 Change Bio":
        if not db_has_perm(uid,"can_manage_bot_profile"): return
        await run(db_set_state, uid, "awaiting_bot_bio"); await msg.reply_text("📝 Send new bio:", reply_markup=cancel_kb()); return
    if text == "📄 Change Description":
        if not db_has_perm(uid,"can_manage_bot_profile"): return
        await run(db_set_state, uid, "awaiting_bot_description"); await msg.reply_text("📄 Send new short description:", reply_markup=cancel_kb()); return
    if text == "🖼 Change Profile Photo":
        if not db_has_perm(uid,"can_manage_bot_profile"): return
        await run(db_set_state, uid, "awaiting_bot_photo"); await msg.reply_text("🖼 Send a photo:", reply_markup=cancel_kb()); return
    if text == "🧪 Test Sequence":
        if not db_has_perm(uid,"can_test_sequence"): await msg.reply_text("⛔ No permission."); return
        await msg.reply_text("🧪 Sending test sequence…")
        await send_sequence_to_user(context.bot, uid)
        await msg.reply_text("✅ Test sequence sent.", reply_markup=staff_kb(uid)); return
    if text == "📨 Message Sequence":
        if not db_has_perm(uid,"can_manage_seq"): await msg.reply_text("⛔ No permission."); return
        await _open_sequence_panel(update, uid); return
    if text.startswith("✏️ Set Intro Msg") or text.startswith("✏️ Update Intro Msg"):
        if not db_has_perm(uid,"can_manage_seq"): await msg.reply_text("⛔ No permission."); return
        await run(db_set_state, uid, "awaiting_set_intro"); await msg.reply_text("Send intro text (supports placeholders):", reply_markup=cancel_kb()); return
    if text == "✉️ Set Reply Msg" or text == "✉️ Update Reply Msg":
        if not db_has_perm(uid,"can_manage_seq"): await msg.reply_text("⛔ No permission."); return
        await run(db_set_state, uid, "awaiting_set_reply"); await msg.reply_text("📝 Send the message that non‑admin users will receive.", reply_markup=cancel_kb()); return
    if text == "✉️ Remove Reply Msg":
        if not db_has_perm(uid,"can_manage_seq"): await msg.reply_text("⛔ No permission."); return
        await run(db_set_state, uid, "awaiting_confirm_remove_reply"); await msg.reply_text("⚠️ Type `yes` to confirm removal.", reply_markup=cancel_kb()); return
    if text == "➕ Add Message" and db_has_perm(uid,"can_manage_seq"):
        await run(db_set_state, uid, "awaiting_addmsg_pos"); await msg.reply_text("🔢 Enter position number:", reply_markup=cancel_kb()); return
    if text == "➖ Remove Message" and db_has_perm(uid,"can_manage_seq"):
        rows = await run(db_get_messages)
        if not rows: await _open_sequence_panel(update, uid, "ℹ️ Sequence empty."); return
        listing = "\n".join(f"  `{r['position']}.` msg_id `{r['message_id']}`" for r in rows)
        await run(db_set_state, uid, "awaiting_removemsg_pos")
        await msg.reply_text(f"📋 *Current:*\n{listing}\nSend the **position number** to remove:", parse_mode="Markdown", reply_markup=cancel_kb()); return
    if text == "🔀 Reorder Message" and db_has_perm(uid,"can_manage_seq"):
        rows = await run(db_get_messages)
        if not rows: await _open_sequence_panel(update, uid, "ℹ️ Sequence empty."); return
        listing = "\n".join(f"  `{r['position']}.` msg_id `{r['message_id']}`" for r in rows)
        await run(db_set_state, uid, "awaiting_reordermsg")
        await msg.reply_text(f"📋 *Current:*\n{listing}\nSend `msg_id new_pos`:", parse_mode="Markdown", reply_markup=cancel_kb()); return
    if text == "📄 List Messages" and db_has_perm(uid,"can_manage_seq"):
        rows = await run(db_get_messages)
        body = "\n".join(f"  `{r['position']}.` msg_id `{r['message_id']}`" for r in rows) if rows else "_Empty._"
        await msg.reply_text(f"📋 *Sequence*\n{body}", parse_mode="Markdown", reply_markup=sequence_panel_kb()); return
    if text == "🔙 Back to Panel":
        await open_panel(update, uid); return

def _handle_add_remove_admin(uid, text, role):
    try:
        tid = int(text)
        if tid == ADMIN_ID: return "ℹ️ Cannot add main admin."
        ok = db_add_admin(tid, role)
        return f"✅ {tid} added as {role.capitalize()}." if ok else f"ℹ️ Already exists."
    except ValueError: return "❌ Invalid ID."

def _handle_remove_admin(uid, text):
    try:
        tid = int(text)
        if tid == ADMIN_ID: return "ℹ️ Main admin cannot be removed."
        ok = db_remove_subadmin(tid)
        return "✅ Removed." if ok else "ℹ️ Not found."
    except ValueError: return "❌ Invalid ID."

async def _open_sequence_panel(update, uid, note=""):
    txt = f"{note}\n\n📨 *Message Sequence Panel*" if note else "📨 *Message Sequence Panel*"
    try: await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=sequence_panel_kb())
    except: pass

async def _open_bot_profile_panel(update, uid, note=""):
    txt = f"{note}\n\n🤖 *Bot Profile Management*" if note else "🤖 *Bot Profile Management*"
    try: await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=bot_profile_kb())
    except: pass

# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    try: init_db()
    except: pass
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(ChatJoinRequestHandler(on_join_request))
    app.add_handler(CallbackQueryHandler(subadmin_list_callback, pattern="^perm_list$"))
    app.add_handler(CallbackQueryHandler(subadmin_perm_menu_callback, pattern="^perm_sub_"))
    app.add_handler(CallbackQueryHandler(perm_toggle_callback, pattern="^perm_toggle_"))
    app.add_handler(CallbackQueryHandler(perm_close_callback, pattern="^perm_close$"))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, on_message))
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
