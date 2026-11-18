#!/usr/bin/env python3

import asyncio
import os
import json
import logging
import re
import sqlite3
import time
from collections import defaultdict, deque
from datetime import datetime
from zoneinfo import ZoneInfo

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ChatMemberHandler, filters
)
from telegram.constants import ParseMode, ChatMemberStatus
from telegram.error import RetryAfter
from admin_extras import AdminExtras, init_extras_db, TELETHON_OK as EX_TELETHON_OK
try:
    import miniapp
    MINIAPP_OK = True
except ImportError:
    MINIAPP_OK = False
try:
    import tunnel
    TUNNEL_OK = True
except ImportError:
    TUNNEL_OK = False


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)

ASSISTANT_API_ID = int(os.getenv("ASSISTANT_API_ID", "0") or 0)
ASSISTANT_USERNAME = os.getenv("ASSISTANT_USERNAME", "")
MINIAPP_ENABLED = os.getenv("MINIAPP_ENABLED", "1") == "1"
MINIAPP_PORT = int(os.getenv("PORT") or os.getenv("MINIAPP_PORT") or "8080")  # Railway يعطي PORT
MINIAPP_URL = os.getenv("MINIAPP_URL", "").strip() or (
    f"https://{os.getenv('RAILWAY_PUBLIC_DOMAIN')}" if os.getenv("RAILWAY_PUBLIC_DOMAIN") else "")
ASSISTANT_API_HASH = os.getenv("ASSISTANT_API_HASH", "").strip()
ASSISTANT_SESSION_STRING = os.getenv("ASSISTANT_SESSION_STRING", "").strip()

MEDIA_SPAM_LIMIT = 5
MEDIA_SPAM_WINDOW = 30
LONG_MSG_LIMIT = 150

# محاولات قراءة السجل الاداري: القيد لا يظهر في السجل فور وصول التعديل،
# ولهذا كان الحساب المساعد "لا يفيد" ويسقط دائما على اسم صاحب الرسالة.
ASSISTANT_LOG_LIMIT = 30
ASSISTANT_RETRY_DELAYS = (0.0, 1.0, 2.0, 4.0)
EMERGENCY_LOCK_SECS = 5 * 3600
BAN_LIMIT_COUNT = 3
BAN_LIMIT_WINDOW = 30

RESTORE_CHUNK_SIZE = 5
RESTORE_CHUNK_DELAY = 1.5
MAINTENANCE_INTERVAL = 6 * 3600
ADMIN_SYNC_INTERVAL = 15 * 60
SESSION_MONITOR_INTERVAL = 30   # فحص شبه فوري: هل اخرج مشرف جلسة البوت من (اجهزة حسابه)؟
MESSAGE_CACHE_MAX_AGE = 7 * 86400
OLD_MESSAGE_NOTIFY_THRESHOLD = 24 * 3600
SPAM_LOG_MAX_AGE = 30 * 86400
_API_MAX_RETRIES = 5
_API_CONCURRENT = 3


_api_semaphore: asyncio.Semaphore | None = None

def _get_semaphore() -> asyncio.Semaphore:
    global _api_semaphore
    if _api_semaphore is None:
        _api_semaphore = asyncio.Semaphore(_API_CONCURRENT)
    return _api_semaphore


async def _api_with_retry(coro_factory, max_retries: int = _API_MAX_RETRIES):
    sem = _get_semaphore()
    last_exc = None
    for attempt in range(max_retries):
        try:
            async with sem:
                return await coro_factory()
        except RetryAfter as e:
            wait = int(e.retry_after) + 2
            logger.warning(f"FloodWait {wait}s (محاولة {attempt+1}/{max_retries})")
            await asyncio.sleep(wait)
            last_exc = e
        except Exception:
            raise
    raise last_exc

_BAGHDAD = ZoneInfo("Asia/Baghdad")

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH") or (
    os.path.join(os.environ["RAILWAY_VOLUME_MOUNT_PATH"], "channel_manager.db")
    if os.getenv("RAILWAY_VOLUME_MOUNT_PATH") else "channel_manager.db")

SETTINGS_COLS = ("lock_messages", "lock_media", "lock_edits",
                 "delete_on_lock", "spam_media_enabled")
SETTINGS_DEFAULTS = {"lock_messages": 0, "lock_media": 0, "lock_edits": 1,
                     "delete_on_lock": 1, "spam_media_enabled": 1}

BAN_TYPES = {
    "tatweel": "منع التطويل",
    "long_msg": "منع الرسائل الطويلة",
    "links": "منع الروابط",
    "custom": "منع مخصص",
}

ALL_PERMS = {
    "can_change_info": "تغيير معلومات القناة",
    "can_manage_chat": "ادارة الرسائل",
    "can_post_messages": "نشر الرسائل",
    "can_edit_messages": "تحرير رسائل الاخرين",
    "can_delete_messages": "حذف رسائل الاخرين",
    "can_post_stories": "نشر القصص",
    "can_edit_stories": "تحرير قصص الاخرين",
    "can_delete_stories": "حذف قصص الاخرين",
    "can_restrict_members": "حظر المستخدمين",
    "can_invite_users": "اضافة مشتركين",
    "can_pin_messages": "ادارة الرسائل الخاصة",
    "can_manage_video_chats": "ادارة البثوث المباشرة",
    "can_promote_members": "اضافة مشرفين جدد",
}

CHANNEL_PERMS_BASE = {k for k in ALL_PERMS if not k.endswith("_stories")}
STORY_PERMS = {k for k in ALL_PERMS if k.endswith("_stories")}

DEFAULT_ADMIN_PERMS = {
    "can_change_info": False,
    "can_manage_chat": False,
    "can_post_messages": True,
    "can_edit_messages": True,
    "can_delete_messages": True,
    "can_post_stories": False,
    "can_edit_stories": False,
    "can_delete_stories": False,
    "can_restrict_members": False,
    "can_invite_users": False,
    "can_pin_messages": False,
    "can_manage_video_chats": True,
    "can_promote_members": False,
}

_TEXT = "__text__"

# منع الروابط: روابط قنوات/بوتات تيليجرام فقط (t.me وما يعادلها).
# tg:// و @username غير ممنوعين (اذا اردت منع @username لاحقا فذلك يشمل
# ذكر اي مستخدم/بوت في اي رسالة، وهو مقيّد جدا).
_TG_LINK_HOSTS = r'(?:t\.me|telegram\.me|telegram\.dog|telesco\.pe|tlgrm\.ru)'
LINK_PATTERN = re.compile(
    r'(?:https?://|www\.)?' + _TG_LINK_HOSTS + r'/[^\s]+',
    re.IGNORECASE,
)


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA temp_store=MEMORY")
        c.execute("PRAGMA mmap_size=30000000")

        c.execute("""CREATE TABLE IF NOT EXISTS channels (
            channel_id INTEGER PRIMARY KEY, channel_name TEXT,
            channel_user TEXT, added_by INTEGER, added_at INTEGER)""")

        c.execute("""CREATE TABLE IF NOT EXISTS channel_settings (
            channel_id INTEGER PRIMARY KEY,
            lock_messages   INTEGER DEFAULT 0,
            lock_media      INTEGER DEFAULT 0,
            lock_edits      INTEGER DEFAULT 1,
            delete_on_lock  INTEGER DEFAULT 1)""")

        try:
            c.execute("ALTER TABLE channel_settings ADD COLUMN delete_on_lock INTEGER DEFAULT 1")
        except sqlite3.OperationalError:
            pass

        try:
            c.execute("ALTER TABLE channel_settings ADD COLUMN spam_media_enabled INTEGER DEFAULT 1")
        except sqlite3.OperationalError:
            pass

        c.execute("""CREATE TABLE IF NOT EXISTS sub_owners (
            user_id INTEGER, channel_id INTEGER,
            added_by INTEGER, added_at INTEGER,
            PRIMARY KEY (user_id, channel_id))""")

        c.execute("""CREATE TABLE IF NOT EXISTS bot_admins (
            user_id INTEGER, channel_id INTEGER, permissions TEXT,
            promoted_by INTEGER, promoted_at INTEGER,
            PRIMARY KEY (user_id, channel_id))""")

        c.execute("""CREATE TABLE IF NOT EXISTS bot_demoted_admins (
            user_id INTEGER, channel_id INTEGER, permissions TEXT,
            demoted_by INTEGER, demoted_at INTEGER,
            PRIMARY KEY (user_id, channel_id))""")

        c.execute("""CREATE TABLE IF NOT EXISTS admin_snapshot (
            user_id INTEGER, channel_id INTEGER, permissions TEXT,
            PRIMARY KEY (user_id, channel_id))""")

        c.execute("""CREATE TABLE IF NOT EXISTS spam_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, channel_id INTEGER,
            spam_type TEXT, timestamp INTEGER)""")

        c.execute("""CREATE TABLE IF NOT EXISTS emergency_locks (
            channel_id INTEGER PRIMARY KEY, until INTEGER)""")

        c.execute("""CREATE TABLE IF NOT EXISTS ban_settings (
            channel_id INTEGER, ban_type TEXT, enabled INTEGER DEFAULT 0,
            PRIMARY KEY (channel_id, ban_type))""")

        c.execute("""CREATE TABLE IF NOT EXISTS ban_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER, item_type TEXT, item_value TEXT)""")

        c.execute("""CREATE TABLE IF NOT EXISTS admin_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, channel_id INTEGER,
            event_type TEXT,
            timestamp INTEGER)""")

        c.execute("""CREATE TABLE IF NOT EXISTS media_tracker (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, channel_id INTEGER,
            message_id INTEGER, file_unique_id TEXT,
            timestamp INTEGER)""")

        c.execute("""CREATE TABLE IF NOT EXISTS message_cache (
            channel_id INTEGER, message_id INTEGER,
            file_unique_id TEXT, cached_at INTEGER DEFAULT 0,
            PRIMARY KEY (channel_id, message_id))""")

        c.execute("""CREATE TABLE IF NOT EXISTS assistant_admins (
            channel_id INTEGER PRIMARY KEY,
            user_id INTEGER, added_by INTEGER, added_at INTEGER)""")

        # لقطة صلاحيات قفل الدردشة في جدول مستقل (لا تختلط مع bot_demoted_admins).
        c.execute("""CREATE TABLE IF NOT EXISTS lock_snapshot (
            user_id INTEGER, channel_id INTEGER, permissions TEXT,
            locked_at INTEGER,
            PRIMARY KEY (user_id, channel_id))""")

        # مشرفون منعهم المالك من الاستعادة التلقائية
        c.execute("""CREATE TABLE IF NOT EXISTS restore_excluded (
            user_id INTEGER, channel_id INTEGER, reason TEXT, excluded_at INTEGER,
            PRIMARY KEY (user_id, channel_id))""")

        # تفويضات (اجهزة) حساب كل مشرف التي رأيناها -> لكشف تسجيل دخول جديد
        c.execute("""CREATE TABLE IF NOT EXISTS admin_auth_seen (
            user_id INTEGER, auth_hash TEXT, seen_at INTEGER,
            PRIMARY KEY (user_id, auth_hash))""")

        try:
            c.execute("""INSERT OR IGNORE INTO lock_snapshot (user_id, channel_id, permissions, locked_at)
                         SELECT user_id, channel_id, permissions, demoted_at
                         FROM bot_demoted_admins WHERE demoted_by = -1""")
            c.execute("DELETE FROM bot_demoted_admins WHERE demoted_by = -1")
        except sqlite3.OperationalError:
            pass

        try:
            c.execute("ALTER TABLE message_cache ADD COLUMN cached_at INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass

        c.execute("SELECT channel_id FROM channels")
        for (cid,) in c.fetchall():
            for bt in BAN_TYPES:
                c.execute("INSERT OR IGNORE INTO ban_settings VALUES (?,?,0)", (cid, bt))
        conn.commit()
    init_extras_db(DB_PATH)


class TimedCache:
    def __init__(self, ttl=30):
        self._ttl = ttl
        self._data = {}

    def get(self, key, default=None):
        now = time.time()
        if key in self._data:
            val, exp = self._data[key]
            if now < exp:
                return val
            del self._data[key]
        return default

    def set(self, key, val, ttl=None):
        self._data[key] = (val, time.time() + (ttl or self._ttl))

    def delete(self, key):
        self._data.pop(key, None)

    def clear_prefix(self, prefix):
        for k in list(self._data):
            if k.startswith(prefix):
                del self._data[k]

    def mget(self, keys, default=None):
        return {k: self.get(k, default) for k in keys}

    def purge_expired(self):
        now = time.time()
        expired = [k for k, (val, exp) in self._data.items() if now >= exp]
        for k in expired:
            self._data.pop(k, None)
        return len(expired)


_cache = TimedCache(30)


def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


class DB:
    @staticmethod
    def add_channel(cid, name, username, added_by):
        with _db() as conn:
            conn.execute("INSERT OR REPLACE INTO channels VALUES (?,?,?,?,?)",
                         (cid, name, username, added_by, int(time.time())))
            conn.execute("INSERT OR IGNORE INTO channel_settings (channel_id) VALUES (?)", (cid,))
            for bt in BAN_TYPES:
                conn.execute("INSERT OR IGNORE INTO ban_settings VALUES (?,?,0)", (cid, bt))
            conn.commit()
        _cache.delete(f"get_channel:{cid}")
        _cache.delete(f"settings:{cid}")

    @staticmethod
    def remove_channel(cid):
        with _db() as conn:
            for tbl in ("channels", "channel_settings", "sub_owners", "bot_admins",
                        "admin_snapshot", "emergency_locks", "ban_settings", "ban_items",
                        "bot_demoted_admins", "admin_events", "media_tracker",
                        "message_cache", "spam_log", "assistant_admins",
                        "lock_snapshot", "restore_excluded", "admin_sessions"):
                conn.execute(f"DELETE FROM {tbl} WHERE channel_id=?", (cid,))
            conn.commit()
        _cache.delete(f"get_channel:{cid}")
        _cache.delete(f"settings:{cid}")
        _cache.delete(f"ban_settings:{cid}")
        _cache.delete(f"elock:{cid}")
        _cache.delete(f"sub_owners:{cid}")
        _cache.delete(f"snapshot:{cid}")
        _cache.delete(f"bot_demoted:{cid}")
        _cache.delete(f"lock_demoted:{cid}")
        _cache.delete(f"excluded:{cid}")
        _cache.clear_prefix(f"ban_items:{cid}")

    @staticmethod
    def get_channels(uid):
        with _db() as conn:
            c = conn.cursor()
            if uid == OWNER_ID:
                c.execute("SELECT * FROM channels ORDER BY added_at DESC")
            else:
                c.execute("""SELECT ch.* FROM channels ch
                    JOIN sub_owners so ON ch.channel_id=so.channel_id
                    WHERE so.user_id=? ORDER BY ch.added_at DESC""", (uid,))
            return c.fetchall()

    @staticmethod
    def set_assistant_admin(cid, uid, added_by):
        with _db() as conn:
            conn.execute("INSERT OR REPLACE INTO assistant_admins VALUES (?,?,?,?)",
                         (cid, uid, added_by, int(time.time())))
            conn.commit()

    @staticmethod
    def get_assistant_admin(cid):
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM assistant_admins WHERE channel_id=?", (cid,))
            row = c.fetchone()
            return row[0] if row else None

    @staticmethod
    def remove_assistant_admin(cid):
        with _db() as conn:
            conn.execute("DELETE FROM assistant_admins WHERE channel_id=?", (cid,))
            conn.commit()

    @staticmethod
    def get_all_channel_ids():
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT channel_id FROM channels")
            return [r[0] for r in c.fetchall()]

    @staticmethod
    def get_channel(cid):
        cached = _cache.get(f"get_channel:{cid}")
        if cached is not None:
            return cached
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM channels WHERE channel_id=?", (cid,))
            row = c.fetchone()
        _cache.set(f"get_channel:{cid}", row)
        return row

    @staticmethod
    def get_settings(cid, fresh=False):
        """يرجع dict باسماء الاعمدة.
        كان يرجع صفا من SELECT * ويُقرأ بالارقام s[1] s[5] s[6]، وهذا يكسر
        فورا عند اي تغيير في ترتيب الاعمدة.
        fresh=True يتجاوز الكاش - ضروري كي يرى كل مالك حالة القناة الحقيقية."""
        if not fresh:
            cached = _cache.get(f"settings:{cid}")
            if cached is not None:
                return cached
        cols = ", ".join(SETTINGS_COLS)
        with _db() as conn:
            c = conn.cursor()
            c.execute(f"SELECT {cols} FROM channel_settings WHERE channel_id=?", (cid,))
            row = c.fetchone()
            if row is None:
                c.execute("INSERT OR IGNORE INTO channel_settings (channel_id) VALUES (?)", (cid,))
                conn.commit()
                c.execute(f"SELECT {cols} FROM channel_settings WHERE channel_id=?", (cid,))
                row = c.fetchone()
        if row is None:
            result = dict(SETTINGS_DEFAULTS)
        else:
            result = {k: int(v or 0) for k, v in zip(SETTINGS_COLS, row)}
        _cache.set(f"settings:{cid}", result)
        return result

    @staticmethod
    def set_setting(cid, col, value):
        """كتابة قيمة محددة (وليس قلبا اعمى) - يرجع (القديمة, الجديدة)."""
        if col not in SETTINGS_COLS:
            raise ValueError(col)
        value = 1 if value else 0
        with _db() as conn:
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO channel_settings (channel_id) VALUES (?)", (cid,))
            c.execute(f"SELECT {col} FROM channel_settings WHERE channel_id=?", (cid,))
            row = c.fetchone()
            old = int(row[0] or 0) if row else 0
            c.execute(f"UPDATE channel_settings SET {col}=? WHERE channel_id=?", (value, cid))
            conn.commit()
        _cache.delete(f"settings:{cid}")
        return old, value

    @staticmethod
    def toggle_setting(cid, col):
        cur = DB.get_settings(cid, fresh=True).get(col, 0)
        _, new = DB.set_setting(cid, col, 0 if cur else 1)
        return new

    @staticmethod
    def add_sub_owner(uid, cid, added_by):
        with _db() as conn:
            conn.execute("INSERT OR REPLACE INTO sub_owners VALUES (?,?,?,?)",
                         (uid, cid, added_by, int(time.time())))
            conn.commit()
        _cache.delete(f"sub_owners:{cid}")

    @staticmethod
    def remove_sub_owner(uid, cid):
        with _db() as conn:
            conn.execute("DELETE FROM sub_owners WHERE user_id=? AND channel_id=?", (uid, cid))
            conn.commit()
        _cache.delete(f"sub_owners:{cid}")

    @staticmethod
    def is_authorized(uid, cid):
        if uid == OWNER_ID:
            return True
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM sub_owners WHERE user_id=? AND channel_id=?", (uid, cid))
            return c.fetchone() is not None

    @staticmethod
    def is_known_user(uid):
        if uid == OWNER_ID:
            return True
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM sub_owners WHERE user_id=? LIMIT 1", (uid,))
            return c.fetchone() is not None

    @staticmethod
    def get_sub_owners(cid):
        cached = _cache.get(f"sub_owners:{cid}")
        if cached is not None:
            return cached
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM sub_owners WHERE channel_id=?", (cid,))
            result = [r[0] for r in c.fetchall()]
        _cache.set(f"sub_owners:{cid}", result)
        return result

    @staticmethod
    def get_all_sub_owner_ids():
        """كل المالكين الفرعيين عبر كل القنوات (لارسال رابط اللوحة لهم)."""
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT DISTINCT user_id FROM sub_owners")
            return [r[0] for r in c.fetchall()]

    @staticmethod
    def save_admin(uid, cid, perms, promoted_by):
        with _db() as conn:
            conn.execute("INSERT OR REPLACE INTO bot_admins VALUES (?,?,?,?,?)",
                         (uid, cid, json.dumps(perms), promoted_by, int(time.time())))
            conn.commit()
        _cache.delete(f"admin_perms:{uid}:{cid}")

    @staticmethod
    def delete_admin(uid, cid):
        with _db() as conn:
            conn.execute("DELETE FROM bot_admins WHERE user_id=? AND channel_id=?", (uid, cid))
            conn.commit()
        _cache.delete(f"admin_perms:{uid}:{cid}")

    @staticmethod
    def get_admin_perms(uid, cid):
        cached = _cache.get(f"admin_perms:{uid}:{cid}")
        if cached is not None:
            return cached
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT permissions FROM bot_admins WHERE user_id=? AND channel_id=?", (uid, cid))
            row = c.fetchone()
            result = json.loads(row[0]) if row else None
        _cache.set(f"admin_perms:{uid}:{cid}", result)
        return result

    @staticmethod
    def clear_snapshot(cid):
        with _db() as conn:
            conn.execute("DELETE FROM admin_snapshot WHERE channel_id=?", (cid,))
            conn.commit()
        _cache.delete(f"snapshot:{cid}")

    @staticmethod
    def save_bot_demoted_admin(uid, cid, perms, demoted_by):
        """قائمة الاستعادة: للتنزيل التلقائي من البوت فقط.
        المستبعد بقرار المالك لا يدخلها ابدا.

        مهم: لا نُحدِّث لقطة موجودة بصلاحيات فارغة (كلها False). هذا يحدث حين
        يكون المشرف منزّلا اصلا وقت لقطة جديدة (مخالفة متكررة/تجريد شامل مكرر)،
        فتُتلَف اللقطة الاصلية ويُستعاد بلا صلاحيات. نحفظ الاصلية دائما."""
        if DB.is_restore_excluded(uid, cid):
            return False
        try:
            has_perms = any(bool(v) for v in (perms or {}).values())
        except Exception:
            has_perms = True
        with _db() as conn:
            if not has_perms:
                exists = conn.execute(
                    "SELECT 1 FROM bot_demoted_admins WHERE user_id=? AND channel_id=?",
                    (uid, cid)).fetchone()
                if exists:
                    return True   # ابقِ اللقطة الاصلية بصلاحياتها الحقيقية
            conn.execute("INSERT OR REPLACE INTO bot_demoted_admins VALUES (?,?,?,?,?)",
                         (uid, cid, json.dumps(perms), demoted_by, int(time.time())))
            conn.commit()
        _cache.delete(f"bot_demoted:{cid}")
        return True

    @staticmethod
    def get_bot_demoted_admins(cid):
        cached = _cache.get(f"bot_demoted:{cid}")
        if cached is not None:
            return cached
        with _db() as conn:
            c = conn.cursor()
            c.execute(
                "SELECT d.user_id, d.permissions FROM bot_demoted_admins d "
                "WHERE d.channel_id=? AND NOT EXISTS ("
                "  SELECT 1 FROM restore_excluded e "
                "  WHERE e.user_id=d.user_id AND e.channel_id=d.channel_id) "
                "ORDER BY d.demoted_at DESC",
                (cid,))
            result = [(r[0], json.loads(r[1])) for r in c.fetchall()]
        _cache.set(f"bot_demoted:{cid}", result)
        return result

    @staticmethod
    def get_demoted_admin_perms(uid, cid):
        if DB.is_restore_excluded(uid, cid):
            return None
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT permissions FROM bot_demoted_admins WHERE user_id=? AND channel_id=?",
                      (uid, cid))
            row = c.fetchone()
            return json.loads(row[0]) if row else None

    @staticmethod
    def clear_bot_demoted_admins(cid):
        with _db() as conn:
            conn.execute("DELETE FROM bot_demoted_admins WHERE channel_id=?", (cid,))
            conn.commit()
        _cache.delete(f"bot_demoted:{cid}")

    @staticmethod
    def save_lock_demoted_admin(uid, cid, perms):
        """INSERT OR IGNORE عمدا: اذا كانت الدردشة مقفولة اصلا فالصلاحيات
        الحالية مسحوبة، وحفظها فوق اللقطة الاصلية يفقد المشرف صلاحياته للابد."""
        with _db() as conn:
            conn.execute("INSERT OR IGNORE INTO lock_snapshot VALUES (?,?,?,?)",
                         (uid, cid, json.dumps(perms), int(time.time())))
            conn.commit()
        _cache.delete(f"lock_demoted:{cid}")

    @staticmethod
    def has_lock_snapshot(uid, cid):
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM lock_snapshot WHERE user_id=? AND channel_id=?", (uid, cid))
            return c.fetchone() is not None

    @staticmethod
    def get_lock_demoted_admins(cid):
        cached = _cache.get(f"lock_demoted:{cid}")
        if cached is not None:
            return cached
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id, permissions FROM lock_snapshot WHERE channel_id=?", (cid,))
            result = [(r[0], json.loads(r[1])) for r in c.fetchall()]
        _cache.set(f"lock_demoted:{cid}", result)
        return result

    @staticmethod
    def clear_lock_demoted_admins(cid):
        with _db() as conn:
            conn.execute("DELETE FROM lock_snapshot WHERE channel_id=?", (cid,))
            conn.commit()
        _cache.delete(f"lock_demoted:{cid}")

    @staticmethod
    def delete_lock_demoted_admin(uid, cid):
        with _db() as conn:
            conn.execute("DELETE FROM lock_snapshot WHERE user_id=? AND channel_id=?", (uid, cid))
            conn.commit()
        _cache.delete(f"lock_demoted:{cid}")

    @staticmethod
    def delete_bot_demoted_admin(uid, cid):
        with _db() as conn:
            conn.execute("DELETE FROM bot_demoted_admins WHERE user_id=? AND channel_id=?", (uid, cid))
            conn.commit()
        _cache.delete(f"bot_demoted:{cid}")

    # ---------- استبعاد من الاستعادة التلقائية ----------

    @staticmethod
    def exclude_from_restore(uid, cid, reason):
        now = int(time.time())
        with _db() as conn:
            conn.execute("INSERT OR REPLACE INTO restore_excluded VALUES (?,?,?,?)",
                         (uid, cid, reason, now))
            conn.execute("DELETE FROM bot_demoted_admins WHERE user_id=? AND channel_id=?", (uid, cid))
            conn.execute("DELETE FROM lock_snapshot WHERE user_id=? AND channel_id=?", (uid, cid))
            conn.execute("DELETE FROM admin_snapshot WHERE user_id=? AND channel_id=?", (uid, cid))
            conn.execute("DELETE FROM bot_admins WHERE user_id=? AND channel_id=?", (uid, cid))
            conn.commit()
        for k in ("bot_demoted", "lock_demoted", "snapshot", "excluded"):
            _cache.delete(f"{k}:{cid}")
        _cache.delete(f"admin_perms:{uid}:{cid}")

    @staticmethod
    def unexclude_from_restore(uid, cid):
        with _db() as conn:
            conn.execute("DELETE FROM restore_excluded WHERE user_id=? AND channel_id=?", (uid, cid))
            conn.commit()
        _cache.delete(f"excluded:{cid}")

    @staticmethod
    def get_restore_excluded(cid):
        cached = _cache.get(f"excluded:{cid}")
        if cached is not None:
            return cached
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id FROM restore_excluded WHERE channel_id=?", (cid,))
            result = {r[0] for r in c.fetchall()}
        _cache.set(f"excluded:{cid}", result)
        return result

    @staticmethod
    def is_restore_excluded(uid, cid):
        return uid in DB.get_restore_excluded(cid)

    @staticmethod
    def save_snapshot(uid, cid, perms):
        with _db() as conn:
            conn.execute("INSERT OR REPLACE INTO admin_snapshot VALUES (?,?,?)",
                         (uid, cid, json.dumps(perms)))
            conn.commit()
        _cache.delete(f"snapshot:{cid}")

    @staticmethod
    def delete_snapshot(uid, cid):
        with _db() as conn:
            conn.execute("DELETE FROM admin_snapshot WHERE user_id=? AND channel_id=?", (uid, cid))
            conn.commit()
        _cache.delete(f"snapshot:{cid}")

    @staticmethod
    def get_snapshot(cid):
        cached = _cache.get(f"snapshot:{cid}")
        if cached is not None:
            return cached
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id, permissions FROM admin_snapshot WHERE channel_id=?", (cid,))
            result = [(r[0], json.loads(r[1])) for r in c.fetchall()]
        _cache.set(f"snapshot:{cid}", result)
        return result

    @staticmethod
    def log_spam(uid, cid, spam_type):
        with _db() as conn:
            conn.execute("INSERT INTO spam_log (user_id,channel_id,spam_type,timestamp) VALUES (?,?,?,?)",
                         (uid, cid, spam_type, int(time.time())))
            conn.commit()

    @staticmethod
    def set_emergency_lock(cid, until):
        with _db() as conn:
            conn.execute("INSERT OR REPLACE INTO emergency_locks VALUES (?,?)", (cid, until))
            conn.commit()
        _cache.delete(f"elock:{cid}")

    @staticmethod
    def clear_emergency_lock(cid):
        with _db() as conn:
            conn.execute("DELETE FROM emergency_locks WHERE channel_id=?", (cid,))
            conn.commit()
        _cache.delete(f"elock:{cid}")

    @staticmethod
    def get_emergency_lock(cid):
        cached = _cache.get(f"elock:{cid}")
        if cached is not None:
            return cached
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT until FROM emergency_locks WHERE channel_id=?", (cid,))
            row = c.fetchone()
            result = row[0] if row else None
        _cache.set(f"elock:{cid}", result)
        return result

    @staticmethod
    def get_ban_settings(cid):
        cached = _cache.get(f"ban_settings:{cid}")
        if cached is not None:
            return cached
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT ban_type, enabled FROM ban_settings WHERE channel_id=?", (cid,))
            result = {bt: 0 for bt in BAN_TYPES}
            for ban_type, enabled in c.fetchall():
                result[ban_type] = enabled
        _cache.set(f"ban_settings:{cid}", result)
        return result

    @staticmethod
    def toggle_ban(cid, ban_type):
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT enabled FROM ban_settings WHERE channel_id=? AND ban_type=?", (cid, ban_type))
            row = c.fetchone()
            nv = 0 if (row and row[0]) else 1
            c.execute("INSERT OR REPLACE INTO ban_settings VALUES (?,?,?)", (cid, ban_type, nv))
            conn.commit()
        _cache.delete(f"ban_settings:{cid}")
        return nv

    @staticmethod
    def add_ban_item(cid, item_type, item_value):
        with _db() as conn:
            conn.execute("INSERT INTO ban_items (channel_id,item_type,item_value) VALUES (?,?,?)",
                         (cid, item_type, item_value))
            conn.commit()
        _cache.delete(f"ban_items:{cid}")

    @staticmethod
    def remove_ban_item(item_id, cid=None):
        with _db() as conn:
            if cid is None:
                row = conn.execute("SELECT channel_id FROM ban_items WHERE id=?", (item_id,)).fetchone()
                cid = row[0] if row else None
            conn.execute("DELETE FROM ban_items WHERE id=?", (item_id,))
            conn.commit()
        if cid:
            _cache.clear_prefix(f"ban_items:{cid}")

    @staticmethod
    def get_ban_items(cid, item_type=None):
        cache_key = f"ban_items:{cid}:{item_type or 'all'}"
        cached = _cache.get(cache_key)
        if cached is not None:
            return cached
        with _db() as conn:
            c = conn.cursor()
            if item_type:
                c.execute("SELECT id, item_type, item_value FROM ban_items WHERE channel_id=? AND item_type=?",
                           (cid, item_type))
            else:
                c.execute("SELECT id, item_type, item_value FROM ban_items WHERE channel_id=?", (cid,))
            result = c.fetchall()
        _cache.set(cache_key, result)
        return result

    @staticmethod
    def get_bot_admins(cid):
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id, permissions FROM bot_admins WHERE channel_id=?", (cid,))
            return [(r[0], json.loads(r[1])) for r in c.fetchall()]

    @staticmethod
    def get_channels_for_admin(uid):
        """كل القنوات المسجّل فيها هذا المشرف (لكشف اخراج الجلسة وتنزيله منها)."""
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT channel_id FROM bot_admins WHERE user_id=?", (uid,))
            return [r[0] for r in c.fetchall()]

    @staticmethod
    def get_seen_auth_hashes(uid):
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT auth_hash FROM admin_auth_seen WHERE user_id=?", (uid,))
            return {r[0] for r in c.fetchall()}

    @staticmethod
    def add_seen_auth_hashes(uid, hashes):
        if not hashes:
            return
        now = int(time.time())
        with _db() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO admin_auth_seen (user_id, auth_hash, seen_at) VALUES (?,?,?)",
                [(uid, str(h), now) for h in hashes])
            conn.commit()

    @staticmethod
    def add_admin_event(uid, cid, event_type):
        with _db() as conn:
            conn.execute("INSERT INTO admin_events (user_id, channel_id, event_type, timestamp) VALUES (?,?,?,?)",
                         (uid, cid, event_type, int(time.time())))
            conn.execute("""DELETE FROM admin_events WHERE id NOT IN (
                SELECT id FROM admin_events WHERE channel_id=? ORDER BY timestamp DESC LIMIT 200)""", (cid,))
            conn.commit()

    @staticmethod
    def get_recent_admin_events(cid, event_type, limit=50):
        with _db() as conn:
            c = conn.cursor()
            c.execute("""SELECT DISTINCT user_id FROM admin_events
                         WHERE channel_id=? AND event_type=?
                         ORDER BY timestamp DESC LIMIT ?""", (cid, event_type, limit))
            return [r[0] for r in c.fetchall()]

    @staticmethod
    def prune_media_tracker(cid, window=MEDIA_SPAM_WINDOW):
        cutoff = int(time.time()) - window
        with _db() as conn:
            conn.execute("DELETE FROM media_tracker WHERE channel_id=? AND timestamp<?", (cid, cutoff))
            conn.commit()

    @staticmethod
    def add_media_track(uid, cid, msg_id, fuid):
        with _db() as conn:
            conn.execute("INSERT INTO media_tracker (user_id,channel_id,message_id,file_unique_id,timestamp) VALUES (?,?,?,?,?)",
                         (uid, cid, msg_id, fuid or "", int(time.time())))
            conn.commit()

    @staticmethod
    def get_media_count(uid, cid, window=MEDIA_SPAM_WINDOW):
        cutoff = int(time.time()) - window
        with _db() as conn:
            c = conn.cursor()
            c.execute("""SELECT COUNT(*) FROM media_tracker
                         WHERE user_id=? AND channel_id=? AND timestamp>?""", (uid, cid, cutoff))
            return c.fetchone()[0]

    @staticmethod
    def get_media_messages(uid, cid, window=MEDIA_SPAM_WINDOW):
        cutoff = int(time.time()) - window
        with _db() as conn:
            c = conn.cursor()
            c.execute("""SELECT message_id FROM media_tracker
                         WHERE user_id=? AND channel_id=? AND timestamp>?
                         ORDER BY timestamp""", (uid, cid, cutoff))
            return [r[0] for r in c.fetchall()]

    @staticmethod
    def clear_media_tracker(cid=None, uid=None):
        with _db() as conn:
            if cid and uid:
                conn.execute("DELETE FROM media_tracker WHERE channel_id=? AND user_id=?", (cid, uid))
            elif cid:
                conn.execute("DELETE FROM media_tracker WHERE channel_id=?", (cid,))
            else:
                conn.execute("DELETE FROM media_tracker")
            conn.commit()

    @staticmethod
    def cache_message(cid, msg_id, fuid):
        with _db() as conn:
            conn.execute("INSERT OR REPLACE INTO message_cache VALUES (?,?,?,?)",
                         (cid, msg_id, fuid or _TEXT, int(time.time())))
            conn.commit()

    @staticmethod
    def get_cached_message(cid, msg_id):
        with _db() as conn:
            c = conn.cursor()
            c.execute("SELECT file_unique_id FROM message_cache WHERE channel_id=? AND message_id=?", (cid, msg_id))
            row = c.fetchone()
            return row[0] if row else None

    @staticmethod
    def prune_message_cache(max_age=7 * 86400):
        cutoff = int(time.time()) - max_age
        with _db() as conn:
            conn.execute("DELETE FROM message_cache WHERE cached_at<?", (cutoff,))
            conn.commit()

    @staticmethod
    def prune_spam_log(max_age=30 * 86400):
        cutoff = int(time.time()) - max_age
        with _db() as conn:
            conn.execute("DELETE FROM spam_log WHERE timestamp<?", (cutoff,))
            conn.commit()


class SpamTracker:

    def __init__(self):
        self._media = defaultdict(deque)
        self._anon = defaultdict(deque)

    def _clean(self, dq, window):
        cutoff = time.time() - window
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def _add(self, dq, msg_id, file_uid):
        dq.append((time.time(), msg_id, file_uid))
        if file_uid:
            matching = [x[1] for x in dq if x[2] == file_uid]
            if len(matching) >= 3:
                dq.clear()
                return True, matching
        if len(dq) >= MEDIA_SPAM_LIMIT:
            ids = [x[1] for x in dq]
            dq.clear()
            return True, ids
        return False, []

    def add_media(self, uid, cid, msg_id, file_uid=None):
        key = (uid, cid)
        dq = self._media[key]
        self._clean(dq, MEDIA_SPAM_WINDOW)
        return self._add(dq, msg_id, file_uid)

    def add_anon(self, cid, msg_id, file_uid=None):
        dq = self._anon[cid]
        self._clean(dq, MEDIA_SPAM_WINDOW)
        return self._add(dq, msg_id, file_uid)

    def reset_user(self, uid, cid):
        self._media.pop((uid, cid), None)

    def reset_anon(self, cid):
        self._anon.pop(cid, None)

    def purge_empty(self):
        for d in (self._media, self._anon):
            for k in [k for k, dq in d.items() if not dq]:
                del d[k]


spam = SpamTracker()


class BanTracker:
    def __init__(self):
        self._bans = defaultdict(deque)

    def add_ban(self, uid, cid):
        key = (uid, cid)
        dq = self._bans[key]
        now = time.time()
        cutoff = now - BAN_LIMIT_WINDOW
        while dq and dq[0] < cutoff:
            dq.popleft()
        dq.append(now)
        if len(dq) >= BAN_LIMIT_COUNT:
            dq.clear()
            return True
        return False

    def reset(self, uid, cid):
        self._bans.pop((uid, cid), None)

    def purge_empty(self):
        for k in [k for k, dq in self._bans.items() if not dq]:
            del self._bans[k]


ban_tracker = BanTracker()


msg_media_cache = {}

def ts_fmt(ts=None):
    dt = datetime.fromtimestamp(ts or time.time(), tz=_BAGHDAD)
    return f"{dt.day:02d}/{dt.month:02d} {dt.strftime('%I:%M %p')}"


def ts_fmt_full(ts=None):
    """تاريخ + ساعة + دقيقة كاملة بتوقيت العراق (للسجلّ)."""
    dt = datetime.fromtimestamp(ts or time.time(), tz=_BAGHDAD)
    return dt.strftime("%Y/%m/%d - %I:%M %p")


def ch_label(r):
    return r[1] or r[2] or str(r[0])


def user_mention_html(user):
    name = user.full_name if hasattr(user, "full_name") else str(user)
    uid = user.id if hasattr(user, "id") else user
    return f'<a href="tg://user?id={uid}">{name}</a>'


def get_msg_file_uid(msg):
    if msg.photo: return msg.photo[-1].file_unique_id
    if msg.video: return msg.video.file_unique_id
    if msg.document: return msg.document.file_unique_id
    if msg.animation: return msg.animation.file_unique_id
    if msg.audio: return msg.audio.file_unique_id
    if msg.sticker: return msg.sticker.file_unique_id
    return None


def cache_msg(cid, msg_id, msg):
    fuid = get_msg_file_uid(msg)
    val = fuid if fuid else _TEXT
    msg_media_cache[(cid, msg_id)] = val
    DB.cache_message(cid, msg_id, fuid)


def has_media(msg):
    return bool(msg.photo or msg.video or msg.document or msg.animation
                or msg.audio or msg.sticker)


def has_tatweel(text):
    return '\u0640' in text


def has_links(text):
    if not text:
        return False
    return bool(LINK_PATTERN.search(text))


def check_ban_violations(msg, bans, ban_items):
    text = msg.text or msg.caption or ""
    if text:
        if bans.get("tatweel") and has_tatweel(text):
            return "تطويل (ـ)"
        if bans.get("long_msg") and len(text) > LONG_MSG_LIMIT:
            return f"رسالة طويلة (+{LONG_MSG_LIMIT} حرف)"
        if bans.get("links") and has_links(text):
            return "روابط محظورة"
    if bans.get("custom") and ban_items:
        text_lower = text.lower() if text else ""
        for item_id, item_type, item_value in ban_items:
            if item_type == "word" and text_lower and item_value.lower() in text_lower:
                return f"كلمة محظورة: {item_value}"
    return None


async def safe_delete(bot, cid, msg_id):
    try:
        await bot.delete_message(cid, msg_id)
    except Exception:
        pass


async def delete_many(bot, cid, msg_ids):
    if not msg_ids:
        return
    results = await asyncio.gather(*[safe_delete(bot, cid, mid) for mid in msg_ids], return_exceptions=True)
    failed = [msg_ids[i] for i, r in enumerate(results) if isinstance(r, Exception)]
    if failed:
        await asyncio.gather(*[safe_delete(bot, cid, mid) for mid in failed], return_exceptions=True)


async def promote_with_stories(bot, cid, uid, perms):
    base = {k: v for k, v in perms.items() if k in CHANNEL_PERMS_BASE}
    stories = {k: v for k, v in perms.items() if k in STORY_PERMS}

    async def _try_promote():
        try:
            await bot.promote_chat_member(chat_id=cid, user_id=uid, **base, **stories)
        except RetryAfter:
            raise
        except Exception:
            await bot.promote_chat_member(chat_id=cid, user_id=uid, **base)

    await _api_with_retry(_try_promote)


_assistant_client = None
_assistant_lock = asyncio.Lock()

admin_extras_mgr = None


async def _on_login_promote(bot, cid, uid, name):
    """يُنادى بعد تسجيل الدخول لحساب مشرف جديد -> يرقّيه في القناة."""
    DB.unexclude_from_restore(uid, cid)
    await promote_with_stories(bot, cid, uid, DEFAULT_ADMIN_PERMS)
    DB.save_admin(uid, cid, DEFAULT_ADMIN_PERMS, 0)
    DB.add_admin_event(uid, cid, "promote")
    if MINIAPP_OK:
        try:
            await miniapp.attach_session(uid)
        except Exception:
            pass
    ch = DB.get_channel(cid)
    await notify_owners(bot, cid,
        "<b>ترقية مشرف جديد</b>\n\n"
        f"القناة: <b>{_safe_html_escape(ch_label(ch) if ch else str(cid))}</b>\n"
        f"المشرف: {_ulink(name, uid)}\n"
        "تم تسجيل الدخول لحسابه وترقيته مشرفًا.\n"
        f"الوقت: {ts_fmt()}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("لوحة القناة", callback_data=f"CH:{cid}", style="primary")]]))


async def _on_logout_demote(bot, cid, uid):
    """يُنادى عند اخراج جلسة مشرف من قناة -> يُنزَّل من تلك القناة ويُستبعد من
    الاستعادة فيها. لا يمس جلساته في القنوات الأخرى."""
    if DB.get_channel(cid) is None:
        return   # قناة غير موجودة (جلسة قديمة/مُرحّلة) - لا تنزيل ولا اشعار
    name = None
    try:
        m = await bot.get_chat_member(cid, uid)
        name = m.user.full_name
    except Exception:
        pass
    # queue_restore=False يستبعده من «استعادة الجميع» في هذه القناة
    await save_and_demote(bot, cid, uid, queue_restore=False, reason="session_removed")
    # افصل الأرشيف عن الحساب فقط اذا لم تبقَ له اي جلسة قناة اخرى
    if MINIAPP_OK and not _get_admin_extras().db.channels_of(uid):
        try:
            await miniapp.detach_session(uid)
        except Exception:
            pass
    ch = DB.get_channel(cid)
    await notify_owners(bot, cid,
        "<b>تنزيل مشرف - اخراج الجلسة</b>\n\n"
        f"القناة: <b>{_safe_html_escape(ch_label(ch) if ch else str(cid))}</b>\n"
        f"المشرف: {_ulink(name or 'مشرف', uid)}\n"
        "أُخرج البوت من حسابه، فنُزِّل من القناة ولن يُعاد رفعه مع «استعادة الجميع».\n"
        f"الوقت: {ts_fmt()}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("لوحة القناة", callback_data=f"CH:{cid}", style="primary")]]))


async def _on_new_login(uid, device, location, unconfirmed):
    """يُنادى عند دخول جهاز جديد الى حساب مشرف (تحديث خام مستقل عن اللغة)
    -> اشعار المالك ان لهذا المشرف تسجيل دخول جديد في حسابه."""
    bot = _sync_bot_ref.get("bot")
    if bot is None:
        return
    name = "مشرف"
    ch_name = None
    for cid in DB.get_channels_for_admin(uid):
        try:
            m = await bot.get_chat_member(cid, uid)
            name = m.user.full_name or name
        except Exception:
            pass
        ch = DB.get_channel(cid)
        ch_name = ch_label(ch) if ch else str(cid)
        break
    title = "تسجيل دخول جديد على حساب مشرف"
    if ch_name:
        title += " - " + _safe_html_escape(ch_name)
    lines = [f"<b>{title}</b>\n",
             f"المشرف: {_ulink(name, uid)} (<code>{uid}</code>)"]
    if device:
        lines.append(f"الجهاز: {_safe_html_escape(str(device))}")
    if location:
        lines.append(f"الموقع: {_safe_html_escape(str(location))}")
    lines.append(f"\nالوقت: {ts_fmt()}")
    try:
        await bot.send_message(OWNER_ID, "\n".join(lines), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning("new login notify uid=%s: %s", uid, e)


def _get_admin_extras():
    global admin_extras_mgr
    if admin_extras_mgr is None:
        admin_extras_mgr = AdminExtras(
            api_id=ASSISTANT_API_ID,
            api_hash=ASSISTANT_API_HASH,
            db_path=DB_PATH,
            assistant_client_provider=get_assistant_client,
            assistant_username=ASSISTANT_USERNAME,
            on_login_promote=_on_login_promote,
            on_logout_demote=_on_logout_demote,
        )
    return admin_extras_mgr


def assistant_configured():
    return bool(ASSISTANT_API_ID and ASSISTANT_API_HASH and ASSISTANT_SESSION_STRING)


async def get_assistant_client():
    global _assistant_client
    if not assistant_configured():
        return None, "الحساب المساعد غير مُهيأ (ASSISTANT_API_ID / ASSISTANT_API_HASH / ASSISTANT_SESSION_STRING فارغة في الكود)."
    async with _assistant_lock:
        if _assistant_client is not None:
            return _assistant_client, None
        try:
            from pyrogram import Client
        except ImportError:
            return None, "مكتبة pyrogram غير مثبتة على هذا السيرفر. نفّذ: pip install pyrogram tgcrypto"
        try:
            api_id = int(ASSISTANT_API_ID)
        except (TypeError, ValueError):
            return None, "ASSISTANT_API_ID يجب ان يكون رقماً صحيحاً بدون علامات تنصيص."
        try:
            client = Client(
                "assistant_session",
                api_id=api_id,
                api_hash=str(ASSISTANT_API_HASH).strip(),
                session_string=str(ASSISTANT_SESSION_STRING).strip(),
                in_memory=True,
            )
            await client.start()
        except Exception as e:
            logger.error(f"assistant: تعذر بدء الجلسة: {e}")
            return None, f"تعذر تشغيل جلسة الحساب المساعد: {e}"
        _assistant_client = client
        return _assistant_client, None


async def reset_assistant_client():
    global _assistant_client
    async with _assistant_lock:
        client, _assistant_client = _assistant_client, None
    if client is not None:
        try:
            await client.stop()
        except Exception:
            pass


async def ensure_assistant_can_read_last_seen():
    """يجعل «آخر ظهور» الحساب المساعد مرئيا للجميع. ضروري: تيليجرام يمنع من
    يُخفي آخر ظهوره من رؤية آخر ظهور الآخرين (قاعدة التبادل) — فحتى لو سمح
    المشرف للمساعد، لا يراه المساعد ما لم يُشارك ظهوره هو. نضبطه مرة عند البدء."""
    try:
        client, err = await get_assistant_client()
        if client is None:
            if err:
                logger.warning("assistant last-seen: %s", err)
            return
        from pyrogram.raw.functions.account import SetPrivacy
        from pyrogram.raw.types import (InputPrivacyKeyStatusTimestamp,
                                        InputPrivacyValueAllowAll)
        await client.invoke(SetPrivacy(
            key=InputPrivacyKeyStatusTimestamp(),
            rules=[InputPrivacyValueAllowAll()]))
        logger.info("assistant last-seen -> Everybody (كي يقرأ آخر ظهور المشرفين)")
    except Exception as e:
        logger.warning("set assistant last-seen privacy: %s", e)


def _flood_hint(e):
    import re as _re
    m = _re.search(r"(\d+)\s*second", str(e))
    secs = int(m.group(1)) if m else 0
    mins = max(1, (secs + 59) // 60)
    return ("تيليجرام يفرض انتظار %d دقيقة على الانضمام التلقائي (حد FLOOD_WAIT). "
            "الحل الفوري بلا انتظار: افتح القناة واضف @%s يدويا كعضو، ثم اضغط الزر مجددا."
            % (mins, ASSISTANT_USERNAME))


_ASSISTANT_PROMOTE_PERMS = {
    "can_change_info": False, "can_manage_chat": True, "can_post_messages": False,
    "can_edit_messages": False, "can_delete_messages": False, "can_post_stories": False,
    "can_edit_stories": False, "can_delete_stories": False, "can_restrict_members": False,
    "can_invite_users": False, "can_pin_messages": False, "can_manage_video_chats": False,
    "can_promote_members": False,
}


async def _assistant_is_member(bot, uid):
    async def _check(cid):
        try:
            m = await bot.get_chat_member(cid, uid)
            return m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER,
                                ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED)
        except Exception:
            return False
    return _check


async def add_assistant_to_channel(bot, cid, added_by):
    """يضيف الحساب المساعد للقناة عبر ثلاث استراتيجيات بالترتيب:

    1) اذا كانت القناة عامة: انضمام مباشر عبر اليوزر (الافكار: الفكرة الثانية).
    2) رابط دعوة بطلب انضمام (creates_join_request) + موافقة البوت على الطلب
       بمعرّف الحساب - هذا يتفادى INVITE_HASH_EXPIRED لان البوت هو من يوافق.
    3) رابط دعوة عادي لمرة واحدة (احتياط اخير).

    الفكرة الاولى (رابط من البوت) والثانية (عبر اليوزر) مطبّقتان معا، والثانية
    اولوية للقنوات العامة. عند FLOOD_WAIT نعطي رسالة واضحة بلا تكرار يفاقمه."""
    import pyrogram.errors as _pyerr
    try:
        client, err = await get_assistant_client()
        if client is None:
            return False, err
        me = await client.get_me()
        is_member = await _assistant_is_member(bot, me.id)

        if await is_member(cid):
            # موجود اصلا (اضفته يدويا مثلا) - رقّه فقط
            return await _promote_and_mark(bot, cid, me, added_by)

        uname = None
        try:
            chat = await bot.get_chat(cid)
            uname = getattr(chat, "username", None)
        except Exception:
            uname = None

        # --- (1) قناة عامة: انضمام مباشر عبر اليوزر ---
        if uname:
            try:
                await client.join_chat(uname)
                return await _promote_and_mark(bot, cid, me, added_by)
            except _pyerr.UserAlreadyParticipant:
                return await _promote_and_mark(bot, cid, me, added_by)
            except _pyerr.FloodWait as e:
                return False, _flood_hint(e)
            except Exception as e:
                logger.warning("assistant public-join cid=%s: %s" % (cid, e))

        # --- (2) رابط بطلب انضمام + موافقة البوت (الاكثر موثوقية) ---
        try:
            invite = await bot.create_chat_invite_link(cid, creates_join_request=True)
        except Exception as e:
            invite = None
            logger.warning("create request-link cid=%s: %s" % (cid, e))
        if invite is not None:
            requested = False
            try:
                await client.join_chat(invite.invite_link)
                # اذا انضم مباشرة (قناة بلا موافقة) اعتبره نجاحا
                if await is_member(cid):
                    return await _promote_and_mark(bot, cid, me, added_by)
                requested = True
            except _pyerr.InviteRequestSent:
                requested = True
            except _pyerr.UserAlreadyParticipant:
                return await _promote_and_mark(bot, cid, me, added_by)
            except _pyerr.FloodWait as e:
                return False, _flood_hint(e)
            except Exception as e:
                logger.warning("assistant request-join cid=%s: %s" % (cid, e))
            if requested:
                try:
                    await bot.approve_chat_join_request(cid, me.id)
                except Exception as e:
                    logger.warning("approve join-request cid=%s: %s" % (cid, e))
                if await is_member(cid):
                    return await _promote_and_mark(bot, cid, me, added_by)

        # --- (3) احتياط: رابط عادي لمرة واحدة ---
        try:
            invite2 = await bot.create_chat_invite_link(cid, member_limit=1,
                                                        creates_join_request=False)
        except Exception as e:
            return False, ("تعذر انشاء رابط دعوة (تأكد ان للبوت صلاحية «دعوة المستخدمين عبر رابط»). "
                           "او اضف @%s يدويا كعضو ثم اضغط الزر مجددا. (%s)"
                           % (ASSISTANT_USERNAME, e))
        try:
            await client.join_chat(invite2.invite_link)
        except _pyerr.UserAlreadyParticipant:
            pass
        except _pyerr.FloodWait as e:
            return False, _flood_hint(e)
        except Exception as e:
            try:
                await bot.revoke_chat_invite_link(cid, invite2.invite_link)
            except Exception:
                pass
            return False, ("تعذر انضمام الحساب المساعد تلقائيا. اضف @%s الى القناة يدويا "
                           "كعضو ثم اضغط الزر مجددا. (%s)" % (ASSISTANT_USERNAME, e))
        if await is_member(cid):
            return await _promote_and_mark(bot, cid, me, added_by)
        return False, ("تعذر تأكيد انضمام الحساب المساعد. اضف @%s يدويا ثم اضغط الزر مجددا."
                       % ASSISTANT_USERNAME)
    except Exception as e:
        logger.error("add_assistant_to_channel error: %s" % e)
        return False, str(e)


async def _promote_and_mark(bot, cid, me, added_by):
    try:
        await promote_with_stories(bot, cid, me.id, _ASSISTANT_PROMOTE_PERMS)
    except Exception as e:
        return False, ("انضم الحساب المساعد لكن تعذرت ترقيته كمشرف. تأكد ان للبوت "
                       "صلاحية «اضافة مشرفين». (%s)" % e)
    DB.set_assistant_admin(cid, me.id, added_by)
    return True, me


_editor_names = {}


async def get_editor_via_assistant(cid, message_id):
    """يرجع (user_id, name) لمن عدّل الرسالة فعلا، او None.

    ملاحظتان كانتا تعطّلان هذه الدالة تماما:
    1) اسم الوسيط الصحيح في pyrogram هو filters وليس filter، فكان النداء
       يرمي TypeError يُبتلع في except ويرجع None في كل مرة.
    2) قيد السجل الاداري لا يظهر لحظة وصول تحديث التعديل، فنعيد المحاولة.
    """
    if not DB.get_assistant_admin(cid):
        return None
    client, err = await get_assistant_client()
    if client is None:
        if err:
            logger.warning(f"assistant unavailable: {err}")
        return None
    try:
        from pyrogram.enums import ChatEventAction
        from pyrogram.types import ChatEventFilter
    except ImportError as e:
        logger.warning(f"assistant: pyrogram types missing: {e}")
        return None

    log_filter = ChatEventFilter(edited_messages=True)
    for delay in ASSISTANT_RETRY_DELAYS:
        if delay:
            await asyncio.sleep(delay)
        try:
            async for event in client.get_chat_event_log(
                    cid, filters=log_filter, limit=ASSISTANT_LOG_LIMIT):
                if event.action != ChatEventAction.MESSAGE_EDITED:
                    continue
                nm = event.new_message
                if not nm or nm.id != message_id:
                    continue
                u = event.user
                if not u:
                    return None
                name = " ".join(x for x in (u.first_name, u.last_name) if x).strip()
                name = name or (u.username and f"@{u.username}") or str(u.id)
                _editor_names[(cid, u.id)] = name
                return u.id, name
        except Exception as e:
            logger.warning(f"get_editor_via_assistant cid={cid} msg={message_id}: {e}")
            if "terminated" in str(e).lower() or "auth" in str(e).lower():
                await reset_assistant_client()
            return None
    return None


async def editor_mention(bot, cid, editor_uid, name=None):
    if not editor_uid:
        if not DB.get_assistant_admin(cid):
            return "غير معروف - فعّل الحساب المساعد في هذه القناة"
        return "غير معروف - تعذر قراءة السجل الاداري"
    name = name or _editor_names.get((cid, editor_uid))
    if not name:
        try:
            m = await bot.get_chat_member(cid, editor_uid)
            name = m.user.full_name or str(editor_uid)
        except Exception:
            name = str(editor_uid)
    return f'<a href="tg://user?id={editor_uid}">{_safe_html_escape(name)}</a>'


def _is_assistant_admin(cid, uid):
    """الحساب المساعد فوق كل العمليات: لا يُنزَّل ولا يُقفل ولا يُلمس باي عملية شاملة."""
    try:
        return uid is not None and uid == DB.get_assistant_admin(cid)
    except Exception:
        return False


async def do_demote(bot, cid, uid):
    if _is_assistant_admin(cid, uid):
        logger.info(f"do_demote: تخطّي الحساب المساعد cid={cid} uid={uid}")
        return
    base = {k: False for k in CHANNEL_PERMS_BASE}
    stories = {k: False for k in STORY_PERMS}

    async def _try_demote():
        try:
            await bot.promote_chat_member(chat_id=cid, user_id=uid, **base, **stories)
        except RetryAfter:
            raise
        except Exception:
            await bot.promote_chat_member(chat_id=cid, user_id=uid, **base)

    try:
        await _api_with_retry(_try_demote)
    except Exception as e:
        logger.warning(f"do_demote فشل نهائي uid={uid} cid={cid}: {e}")


async def save_and_demote(bot, cid, uid, queue_restore=True, reason="auto"):
    """
    queue_restore=True  -> تنزيل تلقائي (سبام/مخالفة): يدخل قائمة الاستعادة.
    queue_restore=False -> تنزيل بقرار المالك: يُستبعد من الاستعادة نهائيا
                           حتى لا يعود مع "استعادة الجميع" لاحقا.
    """
    if _is_assistant_admin(cid, uid):
        return   # الحساب المساعد لا يُنزَّل ابدا
    try:
        m = await bot.get_chat_member(cid, uid)
        perms = {k: bool(getattr(m, k, False)) for k in ALL_PERMS}
        if queue_restore:
            DB.save_admin(uid, cid, perms, 0)
            DB.save_bot_demoted_admin(uid, cid, perms, 0)
        DB.add_admin_event(uid, cid, "demote")
    except Exception:
        pass
    if not queue_restore:
        DB.exclude_from_restore(uid, cid, reason)
    await do_demote(bot, cid, uid)


async def _demote_in_chunks(bot, cid, admins_list):
    for i in range(0, len(admins_list), RESTORE_CHUNK_SIZE):
        chunk = admins_list[i:i + RESTORE_CHUNK_SIZE]
        tasks = []
        valid_admins = []
        for a in chunk:
            if a.user.is_bot or a.status == ChatMemberStatus.OWNER:
                continue
            perms = {k: bool(getattr(a, k, False)) for k in ALL_PERMS}
            DB.save_snapshot(a.user.id, cid, perms)
            DB.save_bot_demoted_admin(a.user.id, cid, perms, 0)
            DB.add_admin_event(a.user.id, cid, "demote")
            tasks.append(do_demote(bot, cid, a.user.id))
            valid_admins.append(a)
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for a, r in zip(valid_admins, results):
                if isinstance(r, Exception):
                    logger.warning(f"_demote_in_chunks: فشل تنزيل uid={a.user.id} cid={cid}: {r}")
        if i + RESTORE_CHUNK_SIZE < len(admins_list):
            await asyncio.sleep(RESTORE_CHUNK_DELAY)


async def snapshot_and_demote_all(bot, cid, exclude_uids=None):
    try:
        exclude = set(exclude_uids or ())
        admins = await bot.get_chat_administrators(cid)
        _asst = DB.get_assistant_admin(cid)
        to_demote = [a for a in admins if not a.user.is_bot
                     and a.status != ChatMemberStatus.OWNER and a.user.id != _asst
                     and a.user.id not in exclude]
        await _demote_in_chunks(bot, cid, to_demote)
        logger.info(f"snapshot_demote_all cid={cid} count={len(to_demote)} excluded={len(exclude)}")
    except Exception as e:
        logger.warning(f"snapshot_demote_all err cid={cid}: {e}")


_DEAD_MEMBER_ERRORS = ("user not found", "participant_id_invalid", "user_not_participant",
                       "user_id_invalid", "peer_id_invalid", "chat member not found",
                       "user is not a member")


async def restore_bot_demoted_admins(bot, cid):
    demoted = DB.get_bot_demoted_admins(cid)
    if not demoted:
        return 0, 0
    success = 0
    failed = 0
    for i in range(0, len(demoted), RESTORE_CHUNK_SIZE):
        chunk = demoted[i:i + RESTORE_CHUNK_SIZE]
        # تخطَّ من صار مشرفا اصلا (رُقّي يدويا خارج البوت): لا نعدّه فشلا،
        # ونزيله من قائمة الاستعادة حتى لا يعلق زره. كان فشل ترقيته يبقي
        # صفه في القائمة فيظل زر "استعادة المشرفين" ظاهرا بلا داعٍ.
        pending = []
        for uid, perms in chunk:
            try:
                m = await bot.get_chat_member(cid, uid)
                if m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                    DB.delete_bot_demoted_admin(uid, cid)
                    continue
            except Exception:
                pass
            pending.append((uid, perms))
        if not pending:
            continue
        results = await asyncio.gather(
            *[promote_with_stories(bot, cid, uid, perms) for uid, perms in pending],
            return_exceptions=True
        )
        for (uid, _perms), r in zip(pending, results):
            if isinstance(r, Exception):
                failed += 1
                logger.warning(f"restore_bot_demoted_admins: فشل استعادة uid={uid} cid={cid}: {r}")
                if any(x in str(r).lower() for x in _DEAD_MEMBER_ERRORS):
                    DB.delete_bot_demoted_admin(uid, cid)   # لم يعد عضوا
            else:
                success += 1
                DB.delete_bot_demoted_admin(uid, cid)
        if i + RESTORE_CHUNK_SIZE < len(demoted):
            await asyncio.sleep(RESTORE_CHUNK_DELAY)
    return success, failed


_LOCK_TOGGLED_PERMS = ("can_post_messages", "can_edit_messages", "can_delete_messages")


_chat_lock_guards = defaultdict(asyncio.Lock)


async def sync_chat_lock(bot, cid):
    """ينفّذ حالة القفل الحالية في قاعدة البيانات، وليس الحالة وقت الضغط.

    الحارس يمنع تداخل القفل والفتح - وهو سبب ضياع صلاحيات المشرفين نهائيا
    عندما يضغط اكثر من مالك على الزر في نفس الوقت او يضغط احدهم مرتين.
    """
    async with _chat_lock_guards[cid]:
        try:
            want_locked = bool(DB.get_settings(cid, fresh=True).get("lock_messages"))
            if want_locked:
                return await lock_all_admins(bot, cid)
            return await restore_message_perms_for_demoted(bot, cid)
        except Exception as e:
            logger.warning(f"sync_chat_lock cid={cid}: {e}")
            return 0


async def lock_all_admins(bot, cid):
    locked = 0
    try:
        admins = await bot.get_chat_administrators(cid)
        _asst = DB.get_assistant_admin(cid)
        to_lock = [a for a in admins if not a.user.is_bot
                   and a.status != ChatMemberStatus.OWNER and a.user.id != _asst]
        for i in range(0, len(to_lock), RESTORE_CHUNK_SIZE):
            chunk = to_lock[i:i + RESTORE_CHUNK_SIZE]
            tasks = []
            valid = []
            for a in chunk:
                perms = {k: bool(getattr(a, k, False)) for k in ALL_PERMS}
                if DB.has_lock_snapshot(a.user.id, cid):
                    # مقفول اصلا: لا نحفظ صلاحياته المسحوبة فوق لقطته الاصلية
                    if not any(perms[k] for k in _LOCK_TOGGLED_PERMS):
                        continue
                else:
                    DB.save_lock_demoted_admin(a.user.id, cid, perms)
                new_perms = dict(perms)
                for k in _LOCK_TOGGLED_PERMS:
                    new_perms[k] = False
                tasks.append(promote_with_stories(bot, cid, a.user.id, new_perms))
                valid.append(a)
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for a, r in zip(valid, results):
                    if isinstance(r, Exception):
                        logger.warning(f"lock_all_admins: فشل قفل uid={a.user.id} cid={cid}: {r}")
                    else:
                        locked += 1
            if i + RESTORE_CHUNK_SIZE < len(to_lock):
                await asyncio.sleep(RESTORE_CHUNK_DELAY)
        logger.info(f"lock_all_admins cid={cid} locked={locked}/{len(to_lock)}")
    except Exception as e:
        logger.warning(f"lock_all_admins err cid={cid}: {e}")
    return locked


async def restore_message_perms_for_demoted(bot, cid):
    demoted = DB.get_lock_demoted_admins(cid)
    if not demoted:
        return 0
    excluded = DB.get_restore_excluded(cid)
    success = 0
    for i in range(0, len(demoted), RESTORE_CHUNK_SIZE):
        chunk = demoted[i:i + RESTORE_CHUNK_SIZE]
        live = []
        for uid, perms in chunk:
            if uid in excluded:
                DB.delete_lock_demoted_admin(uid, cid)
            else:
                live.append((uid, perms))
        if not live:
            continue
        results = await asyncio.gather(
            *[promote_with_stories(bot, cid, uid, perms) for uid, perms in live],
            return_exceptions=True
        )
        for (uid, _perms), r in zip(live, results):
            if isinstance(r, Exception):
                logger.warning(f"restore_message_perms_for_demoted: فشل استعادة uid={uid} cid={cid}: {r}")
                if any(x in str(r).lower() for x in _DEAD_MEMBER_ERRORS):
                    DB.delete_lock_demoted_admin(uid, cid)
            else:
                success += 1
                DB.delete_lock_demoted_admin(uid, cid)
        if i + RESTORE_CHUNK_SIZE < len(demoted):
            await asyncio.sleep(RESTORE_CHUNK_DELAY)
    return success


async def notify_owners(bot, cid, text, reply_markup=None):
    targets = list({OWNER_ID} | set(DB.get_sub_owners(cid)))
    async def _send(uid):
        try:
            await bot.send_message(uid, text, parse_mode=ParseMode.HTML,
                                   disable_web_page_preview=True, reply_markup=reply_markup)
        except Exception:
            pass
    await asyncio.gather(*[_send(uid) for uid in targets], return_exceptions=True)


def _who_posted_label(msg, cid):
    """«باسم من» نُشرت الرسالة: مستخدم / القناة نفسها (توقيع مخفي) / قناة اخرى /
    توقيع نصّي / مجهول. للسجلّ واشعار التجريد."""
    if msg is None:
        return "غير معروف"
    u = getattr(msg, "from_user", None)
    if u and not getattr(u, "is_bot", False):
        return user_mention_html(u)
    sc = getattr(msg, "sender_chat", None)
    sig = getattr(msg, "author_signature", None)
    if sc is not None:
        title = _safe_html_escape(getattr(sc, "title", None) or "قناة")
        if _same_peer(sc, cid):
            base = f"باسم مخفي (القناة نفسها: <b>{title}</b>)"
        else:
            base = f"باسم قناة اخرى: <b>{title}</b>"
        if sig:
            base += f" — التوقيع: <b>{_safe_html_escape(sig)}</b>"
        return base
    if sig:
        return f"توقيع: <b>{_safe_html_escape(sig)}</b>"
    return "باسم مخفي (غير معروف)"


async def forward_violation_to_owners(bot, cid, msg, reason):
    """يحوّل الرسالة المخالفة نفسها للمالك والمالكين الفرعيين (كدليل) بعد التقرير،
    قبل حذفها. لا يُرسل اشعار حذف منفصل - تفاصيل الحذف مدموجة في اشعار القفل."""
    if msg is None or not getattr(msg, "message_id", None):
        return
    targets = list({OWNER_ID} | set(DB.get_sub_owners(cid)))

    async def _one(owner):
        try:
            await bot.forward_message(chat_id=owner, from_chat_id=cid,
                                      message_id=msg.message_id)
        except Exception:
            pass
    await asyncio.gather(*[_one(o) for o in targets], return_exceptions=True)


async def identify_sender(bot, msg, cid):
    if msg.from_user and not msg.from_user.is_bot:
        return msg.from_user.id, True
    # لا نُنزّل مشرفا بناء على التوقيع النصّي (author_signature): فهو قابل
    # للانتحال - يضبط محمد رتبته/توقيعه باسم "علي" ثم يخالف، فيُنزَّل علي ظلما.
    # نمرّر كل رسالة موقّعة/باسم القناة الى مسار المجهول الذي يحمي المشرف
    # المنتحَل باسمه ويجرّد الباقين ليقرّر المالك.
    return None, False


async def resolve_target_admin(bot, cid, sender_uid, identified):
    if not (identified and sender_uid):
        return None, True
    try:
        member = await bot.get_chat_member(cid, sender_uid)
    except Exception:
        return None, True
    if member.status == ChatMemberStatus.OWNER:
        return None, None
    if member.status != ChatMemberStatus.ADMINISTRATOR:
        return None, True
    return sender_uid, False


def _raw_peer(pid):
    """يحوّل معرّف قناة/كروب الى صيغته المجرّدة لمقارنة الهوية (يتجاهل بادئة -100)."""
    s = str(pid)
    return int(s[4:]) if s.startswith("-100") else abs(int(pid))


def _same_peer(sender_chat, cid):
    """هل الرسالة موقّعة باسم القناة نفسها (توقيع مخفي) وليست قناة اخرى؟"""
    tid = getattr(sender_chat, "id", None)
    if tid is None:
        return False
    try:
        return _raw_peer(tid) == _raw_peer(cid)
    except (TypeError, ValueError):
        return False


async def _anon_response(bot, cid, msg, reason):
    """مخالفة باسم مخفي/قناة: التقاط تقرير التواقيع -> احتواء فوري (تجريد
    الجميع) -> فحص عبر الجلسات عن مالك القناة الموقّع باسمها -> اعادة صلاحيات
    الجميع عداه (او ابقاؤهم منزّلين ان لم يوجد جانٍ) -> اشعاران: التقرير المستقل
    ثم اشعار القفل."""
    extras = _get_admin_extras()
    try:
        admins = await bot.get_chat_administrators(cid)
    except Exception:
        admins = []
    _asst = DB.get_assistant_admin(cid)
    admins = [a for a in admins if a.user.id != _asst]
    sender_chat = getattr(msg, "sender_chat", None) if msg else None
    _sig = getattr(msg, "author_signature", None) if msg else None
    ch = DB.get_channel(cid)
    name = _safe_html_escape(ch_label(ch) if ch else str(cid))

    # (1) التقط الأدلة (تقرير التواقيع + آخر ظهور + كاشف التوقيع) قبل التجريد،
    #     لان التجريد قد يُلغي إعداد «النشر باسم القناة» لدى الجاني.
    report = ""
    try:
        report = await extras.build_anon_report(bot, cid, msg, admins)
    except Exception as e:
        logger.warning("anon report cid=%s: %s" % (cid, e))
    culprit = None
    try:
        culprit = await extras.detect_culprit_by_signature(cid, sender_chat, admins)
    except Exception as e:
        logger.warning("anon culprit detect cid=%s: %s" % (cid, e))

    # (2) احتواء فوري: جرّد *كل* المشرفين (مع حفظ صلاحياتهم للاستعادة).
    await snapshot_and_demote_all(bot, cid)

    # (3) الفحص مشرف-مشرف عبر الجلسات: من يملك القناة/الكروب الموقّع باسمها؟
    if culprit is None:
        _names = []
        if sender_chat is not None and not _same_peer(sender_chat, cid):
            _t = getattr(sender_chat, "title", None)
            if _t:
                _names.append(_t)
        if _sig:
            _names.append(_sig)
        _uids = [a.user.id for a in admins if not getattr(a.user, "is_bot", False)]
        for _nm in _names:
            try:
                owner = await extras.find_impersonator_by_owned_channel(_nm, _uids)
            except Exception as e:
                logger.warning("owned-channel search cid=%s: %s" % (cid, e))
                owner = None
            if owner is not None:
                for a in admins:
                    if a.user.id == owner and a.status != ChatMemberStatus.OWNER:
                        culprit = (owner, a.user.full_name)
                        break
            if culprit:
                break
    if culprit is None and sender_chat is not None and not _same_peer(sender_chat, cid):
        try:
            creator_uid = await extras.channel_creator_id(sender_chat)
        except Exception as e:
            logger.warning("channel creator detect cid=%s: %s" % (cid, e))
            creator_uid = None
        if creator_uid is not None:
            for a in admins:
                if a.user.id == creator_uid and a.status != ChatMemberStatus.OWNER:
                    culprit = (creator_uid, a.user.full_name)
                    break

    # (4) اعادة صلاحيات الجميع عدا الجاني (ان وُجد).
    if culprit:
        cu_uid, cu_name = culprit
        DB.exclude_from_restore(cu_uid, cid, "anon_culprit")
        DB.delete_bot_demoted_admin(cu_uid, cid)   # يبقى منزّلا وخارج قائمة الاستعادة
        try:
            succ, _f = await restore_bot_demoted_admins(bot, cid)
        except Exception as e:
            logger.warning("restore-except cid=%s: %s" % (cid, e))
            succ = 0
        DB.unexclude_from_restore(cu_uid, cid)
        demote_note = ("تم تجريد " + _ulink(cu_name, cu_uid)
                       + " وحده — يملك القناة/الكروب الموقّع باسمها. "
                       + ("واستعادة %d مشرف." % succ if succ else "ولا مشرف آخر لاستعادته."))
        emk = _emergency_markup(cid, is_anon=False, demoted_uid=cu_uid)
    else:
        if sender_chat is None or _same_peer(sender_chat, cid):
            # توقيع مخفي (باسم القناة نفسها): لا قناة خارجية نبحث عن مالكها.
            demote_note = "تم تجريد جميع المشرفين"
        else:
            demote_note = ("تم تجريد جميع المشرفين (احتواءً) — تعذّر تحديد صاحب "
                           "القناة الموقّع باسمها عبر الجلسات. استعِدهم يدويا من الزر.")
        emk = _emergency_markup(cid, is_anon=True)

    DB.set_emergency_lock(cid, int(time.time()) + EMERGENCY_LOCK_SECS)
    DB.log_spam(0, cid, "مجهول: %s" % reason)

    # (5) الاشعارات: تقرير التواقيع (رسالة مستقلة بذاتها) ثم اشعار القفل.
    if report:
        await notify_owners(bot, cid,
            "<b>تقرير مخالفة باسم مخفي</b>\n"
            "القناة: <b>" + name + "</b>\n"
            "السبب: " + _safe_html_escape(str(reason)) + "\n"
            "الوقت: " + ts_fmt() + "\n\n" + report)
    sig_label = _who_posted_label(msg, cid)
    await notify_owners(bot, cid,
        "<b>اجراء طوارئ - " + name + "</b>\n" + demote_note + "\n\n"
        "السبب: " + _safe_html_escape(str(reason)) + "\n"
        "التوقيع كان: " + sig_label + "\n"
        "وقت الحذف: " + ts_fmt_full() + "\n"
        "قفل الطوارئ نشط. التقرير النشاط اعلاه.",
        reply_markup=emk)


async def _do_violation(bot, uid, cid, violation, is_anon, msg=None):
    if not is_anon and _is_assistant_admin(cid, uid):
        return   # الحساب المساعد لا يُعاقب
    ch = DB.get_channel(cid)
    name = ch_label(ch) if ch else str(cid)
    if is_anon:
        await _anon_response(bot, cid, msg, "انتهاك قائمة المنع: " + str(violation))
    else:
        try:
            member = await bot.get_chat_member(cid, uid)
            mention = user_mention_html(member.user)
            perms = {k: bool(getattr(member, k, False)) for k in ALL_PERMS}
            DB.save_bot_demoted_admin(uid, cid, perms, 0)
            DB.add_admin_event(uid, cid, "demote")
        except Exception:
            mention = f'<a href="tg://user?id={uid}">{uid}</a>'
        await do_demote(bot, cid, uid)
        DB.set_emergency_lock(cid, int(time.time()) + EMERGENCY_LOCK_SECS)
        DB.log_spam(uid, cid, f"مخالفة منع: {violation}")
        await notify_owners(bot, cid,
            f"<b>انتهاك قائمة المنع</b>\n\n"
            f"القناة: <b>{name}</b>\n"
            f"المستخدم: {mention}\n"
            f"المخالفة: {violation}\n"
            f"الوقت: {ts_fmt()}\n\n"
            f"تم تجريد المشرف.",
            reply_markup=_emergency_markup(cid, is_anon=False, demoted_uid=uid))


async def _do_spam(bot, uid, cid, msg_ids, reason, is_anon, msg=None):
    if not is_anon and _is_assistant_admin(cid, uid):
        await delete_many(bot, cid, msg_ids)
        return   # الحساب المساعد لا يُعاقب
    ch = DB.get_channel(cid)
    name = ch_label(ch) if ch else str(cid)
    await delete_many(bot, cid, msg_ids)
    if is_anon:
        spam.reset_anon(cid)
        DB.clear_media_tracker(cid=cid)
        await _anon_response(bot, cid, msg, str(reason) + " (حُذف " + str(len(msg_ids)) + " رسالة)")
    else:
        spam.reset_user(uid, cid)
        DB.clear_media_tracker(cid=cid, uid=uid)
        DB.log_spam(uid, cid, reason)
        try:
            member = await bot.get_chat_member(cid, uid)
            mention = user_mention_html(member.user)
            perms = {k: bool(getattr(member, k, False)) for k in ALL_PERMS}
            DB.save_bot_demoted_admin(uid, cid, perms, 0)
            DB.add_admin_event(uid, cid, "demote")
        except Exception:
            mention = f'<a href="tg://user?id={uid}">{uid}</a>'
        await do_demote(bot, cid, uid)
        DB.set_emergency_lock(cid, int(time.time()) + EMERGENCY_LOCK_SECS)
        await notify_owners(bot, cid,
            f"<b>تنبيه: سبام</b>\n\n"
            f"القناة: <b>{name}</b>\n"
            f"المستخدم: {mention}\n"
            f"السبب: {reason}\n"
            f"الرسائل المحذوفة: {len(msg_ids)}\n"
            f"الوقت: {ts_fmt()}",
            reply_markup=_emergency_markup(cid, is_anon=False, demoted_uid=uid))


def back_kb(callback_data, label="✘"):
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=callback_data, style="success")]])


async def channel_panel_text(bot, cid, ch):
    name = _safe_html_escape(ch_label(ch) if ch else cid)
    admins_count = "—"
    members_count = "—"
    try:
        admins = await bot.get_chat_administrators(cid)
        admins_count = str(len([a for a in admins if not a.user.is_bot]))
    except Exception:
        pass
    try:
        members_count = str(await bot.get_chat_member_count(cid))
    except Exception:
        pass
    lu = DB.get_emergency_lock(cid)
    ea = lu and lu > int(time.time())
    s = DB.get_settings(cid, fresh=True)
    pending = len(DB.get_bot_demoted_admins(cid))

    # الحالة مكتوبة في النص عمدا: كان نص اللوحة ثابتا لا يتغير عند اي تبديل،
    # فيرد تيليجرام "message is not modified" ولا يتغير شيء على الشاشة،
    # فيظن المستخدم ان الزر لا يعمل.
    state_lines = (
        f"{_lk(s.get('lock_messages'))} الدردشة   "
        f"{_lk(s.get('lock_media'))} الوسائط   "
        f"{_lk(s.get('lock_edits'))} التعديلات\n"
        f"{_lk(s.get('spam_media_enabled'))} حماية سبام الوسائط   "
        f"{_lk(s.get('delete_on_lock'))} حذف الرسائل عند القفل"
    )
    elock_line = ""
    if ea:
        elock_line = (
            f"\n<b>قفل الطوارئ نشط</b> - متبقي {int((lu - time.time()) / 60)} دقيقة\n"
            "اثناء قفل الطوارئ يُحذف كل ما يُنشر مهما كانت بقية الاعدادات،\n"
            "لذلك اوقفه اولا اذا بدا لك ان الاقفال لا تعمل.\n"
        )
    pending_line = f"\nمشرفون بانتظار الاستعادة: <b>{pending}</b>\n" if pending else ""
    return (
        "يتم حمايه القناه وفق قوانين وحمايه آلـ hell\n\n"
        "-------------------------------------\n"
        f"اسم القناة: {name}\n"
        f"عدد المشرفين: {admins_count}\n"
        f"عدد المشتركين: {members_count}\n"
        "-------------------------------------\n"
        f"{state_lines}\n"
        "-------------------------------------\n"
        f"{elock_line}{pending_line}\n"
        "إختر الاجراء لِحمايه القناه:"
    )


async def render_panel(q, bot, cid, uid):
    ch = DB.get_channel(cid)
    text = await channel_panel_text(bot, cid, ch)
    await try_edit_or_send(q, text, parse_mode=ParseMode.HTML,
                           reply_markup=kb_channel(cid, uid))


async def _display_name(bot, cid, tuid):
    cached = _cache.get(f"uname:{cid}:{tuid}")
    if cached:
        return cached
    name = _editor_names.get((cid, tuid)) or str(tuid)
    try:
        m = await bot.get_chat_member(cid, tuid)
        name = (m.user.full_name or str(tuid))
    except Exception:
        pass
    name = name[:25]
    _cache.set(f"uname:{cid}:{tuid}", name, ttl=600)
    return name


async def kb_restore_list(bot, cid):
    demoted = DB.get_bot_demoted_admins(cid)
    rows = []
    for tuid, _perms in demoted[:20]:
        name = await _display_name(bot, cid, tuid)
        rows.append([
            InlineKeyboardButton(name, callback_data=f"RONE:{cid}:{tuid}", style="success"),
            InlineKeyboardButton("لا ترفعه", callback_data=f"RDROP:{cid}:{tuid}", style="danger"),
        ])
    if not rows:
        rows.append([InlineKeyboardButton("لا يوجد مشرفون بانتظار الاستعادة", callback_data="NOOP")])
    else:
        rows.append([InlineKeyboardButton("استعادة الجميع",
                                          callback_data=f"RESTORE_ALL:{cid}", style="success")])
    rows.append([InlineKeyboardButton("رجوع", callback_data=f"CH:{cid}")])
    return InlineKeyboardMarkup(rows)



def _emergency_markup(cid, is_anon=False, demoted_uid=None):
    buttons = []
    if is_anon or not demoted_uid:
        buttons.append([InlineKeyboardButton("استعادة المشرفين",
                                             callback_data=f"RLIST:{cid}", style="success")])
    else:
        buttons.append([InlineKeyboardButton("استعادة صلاحيات المشرف",
                                             callback_data=f"RESTORE_ADMIN:{cid}:{demoted_uid}",
                                             style="success")])
        buttons.append([InlineKeyboardButton("لا ترفعه مرة اخرى",
                                             callback_data=f"RDROP:{cid}:{demoted_uid}",
                                             style="danger")])
    buttons.append([InlineKeyboardButton("ايقاف قفل الطوارئ",
                                         callback_data=f"STOP_ELOCK:{cid}", style="danger")])
    buttons.append([InlineKeyboardButton("لوحة القناة", callback_data=f"CH:{cid}", style="primary")])
    return InlineKeyboardMarkup(buttons)


def start_text(uid, name):
    safe_name = (name or "مستخدم").replace("<", "&lt;").replace(">", "&gt;").replace("&", "&amp;")
    return (
        f'مرحباً <a href="tg://user?id={uid}">{safe_name}</a> في بوت حماية القنوات الخاص بـ آلـ hell\n\n'
        f"قم باضافه القنوات وإحمِ قناتك من الزر ادناه:"
    )


def kb_main(uid):
    channels = DB.get_channels(uid)
    rows = []
    for ch in channels:
        lock = DB.get_emergency_lock(ch[0])
        prefix = "🔒 " if (lock and lock > int(time.time())) else ""
        rows.append([InlineKeyboardButton(f"{prefix}{ch_label(ch)}", callback_data=f"CH:{ch[0]}")])
    if uid == OWNER_ID:
        rows.append([InlineKeyboardButton("+ اضافة قناة", callback_data="ADD_CH", style="success")])
        rows.append([InlineKeyboardButton("ادارة المستخدمين", callback_data="MGMT_USERS")])
    if not channels:
        if uid == OWNER_ID:
            rows.append([InlineKeyboardButton("لا توجد قنوات", callback_data="ADD_CH")])
        else:
            rows.append([InlineKeyboardButton("لم يتم اسنادك الى اي قناة بعد", callback_data="NOOP")])
    return InlineKeyboardMarkup(rows)


async def send_start(target, uid, name):
    text = start_text(uid, name)
    markup = kb_main(uid)
    send_kwargs = {"reply_markup": markup}
    if hasattr(target, "_target_chat_id"):
        send_kwargs["chat_id"] = target._target_chat_id
    try:
        if hasattr(target, "edit_message_text"):
            try:
                await target.edit_message_text(text, parse_mode=ParseMode.HTML, **send_kwargs)
                return
            except Exception as e:
                err = str(e).lower()
                if "not modified" in err or "message is not modified" in err:
                    try:
                        await target.answer()
                    except Exception:
                        pass
                    return
        if hasattr(target, "message") and target.message:
            await target.message.reply_text(text, parse_mode=ParseMode.HTML, **send_kwargs)
            return
        if hasattr(target, "reply_text"):
            await target.reply_text(text, parse_mode=ParseMode.HTML, **send_kwargs)
            return
        if hasattr(target, "bot"):
            await target.bot.send_message(uid, text, parse_mode=ParseMode.HTML, **send_kwargs)
            return
    except Exception as e:
        logger.warning(f"send_primary err: {e}")
    try:
        if hasattr(target, "message") and target.message:
            await target.message.reply_text(text, **send_kwargs)
        elif hasattr(target, "reply_text"):
            await target.reply_text(text, **send_kwargs)
        elif hasattr(target, "bot"):
            await target.bot.send_message(uid, text, **send_kwargs)
    except Exception as e:
        logger.warning(f"send_fallback err: {e}")


TOGGLE_MAP = {
    "messages":  ("lock_messages", "الدردشة", "lock"),
    "media":     ("lock_media", "الوسائط", "lock"),
    "edits":     ("lock_edits", "التعديلات", "lock"),
    "delonlock": ("delete_on_lock", "حذف الرسائل عند قفل الدردشة", "lock"),
    "spammedia": ("spam_media_enabled", "حماية سبام الوسائط", "lock"),
}


def _lk(on):
    return "🔒" if on else "🔓"


def _style(on, mode="lock"):
    """lock: مقفول احمر / مفتوح اخضر.  guard: مفعّلة اخضر / معطّلة احمر."""
    if mode == "guard":
        return "success" if on else "danger"
    return "danger" if on else "success"


def kb_channel(cid, uid):
    # fresh=True: عده مالكين يستعملون البوت في آن واحد، والكاش كان يعرض
    # لاحدهم حاله قديمه فيبدو ان زر الآخر لم يعمل.
    s = DB.get_settings(cid, fresh=True)
    lu = DB.get_emergency_lock(cid)
    ea = lu and lu > int(time.time())
    demoted = DB.get_bot_demoted_admins(cid)

    def tgl(key):
        col, label, mode = TOGGLE_MAP[key]
        cur = 1 if s.get(col) else 0
        # الحالة المتوقعة داخل الزر: تمنع قلب الاعداد من شاشة قديمة
        return InlineKeyboardButton(f"{_lk(cur)} {label}",
                                    callback_data=f"TGL:{key}:{cid}:{cur}",
                                    style=_style(cur, mode))

    rows = [
        [InlineKeyboardButton("المشرفون", callback_data=f"ADMINS:{cid}", style="primary")],
        [tgl("spammedia")],
        [tgl("messages"), tgl("media"), tgl("edits")],
        [tgl("delonlock")],
        [InlineKeyboardButton("قائمة المنع", callback_data=f"BANS:{cid}", style="primary")],
    ]
    if DB.get_assistant_admin(cid):
        rows.append([InlineKeyboardButton("الحساب المساعد مُفعّل",
                                          callback_data=f"ASSISTANT_INFO:{cid}", style="success")])
    else:
        rows.append([InlineKeyboardButton("اضافه الحساب المساعد",
                                          callback_data=f"ADD_ASSISTANT:{cid}", style="danger")])
    if ea:
        rows.append([InlineKeyboardButton("ايقاف قفل الطوارئ",
                                          callback_data=f"STOP_ELOCK:{cid}", style="danger")])
    if demoted:
        rows.append([InlineKeyboardButton(f"استعادة المشرفين ({len(demoted)})",
                                          callback_data=f"RLIST:{cid}", style="success")])
    if uid == OWNER_ID:
        rows.append([InlineKeyboardButton("المالكون الفرعيون", callback_data=f"SUBOWNERS:{cid}", style="primary")])
        rows.append([InlineKeyboardButton("ازالة القناة", callback_data=f"DEL_CH:{cid}", style="danger")])
    rows.append([InlineKeyboardButton("رجوع", callback_data="BACK_MAIN")])
    return InlineKeyboardMarkup(rows)


async def kb_admins(bot, cid):
    rows = []
    try:
        admins = await bot.get_chat_administrators(cid)
        _asst = DB.get_assistant_admin(cid)
        for a in admins:
            if a.user.is_bot or a.user.id == _asst:
                continue
            if a.status == ChatMemberStatus.OWNER:
                rows.append([InlineKeyboardButton(f"{a.user.full_name[:25]} - المالك", callback_data="NOOP")])
            else:
                rows.append([InlineKeyboardButton(f"{a.user.full_name[:25]}", callback_data=f"ADM_DETAIL:{cid}:{a.user.id}")])
    except Exception:
        pass
    if not rows:
        rows.append([InlineKeyboardButton("لا يوجد مشرفون", callback_data="NOOP")])
    rows.append([InlineKeyboardButton("+ اضافة مشرف جديد", callback_data=f"PROMOTE_NEW:{cid}", style="success")])
    rows.append([InlineKeyboardButton("رجوع", callback_data=f"CH:{cid}")])
    return InlineKeyboardMarkup(rows)


def kb_admin_detail(cid, tuid):
    rows = [
        [InlineKeyboardButton("تعديل الصلاحيات", callback_data=f"EDIT_PERMS:{cid}:{tuid}")],
        [InlineKeyboardButton("تنزيل من الادارة", callback_data=f"DEMOTE:{cid}:{tuid}", style="danger")],
        [InlineKeyboardButton("تسجيل دخول لحساب المشرف", callback_data=f"ADMSESS:OPEN:{cid}:{tuid}", style="primary")],
        [InlineKeyboardButton("رجوع", callback_data=f"ADMINS:{cid}")],
    ]
    return InlineKeyboardMarkup(rows)


def kb_perms(cid, tuid, perms):
    rows = []
    keys = list(ALL_PERMS.items())
    for i in range(0, len(keys), 2):
        row = []
        for j in range(2):
            if i + j >= len(keys):
                break
            key, label = keys[i + j]
            cur = bool(perms.get(key))
            tick = "+" if cur else "-"
            row.append(InlineKeyboardButton(f"{label} {tick}",
                                            callback_data=f"PERM:{cid}:{tuid}:{key}",
                                            style=_style(cur, "guard")))
        rows.append(row)
    rows.append([
        InlineKeyboardButton("حفظ", callback_data=f"SAVE_PERMS:{cid}:{tuid}"),
        InlineKeyboardButton("رجوع", callback_data=f"ADM_DETAIL:{cid}:{tuid}"),
    ])
    return InlineKeyboardMarkup(rows)


def kb_sub_owners(cid):
    rows = []
    for uid in DB.get_sub_owners(cid):
        rows.append([
            InlineKeyboardButton(f"{uid}", callback_data="NOOP"),
            InlineKeyboardButton("X ازالة", callback_data=f"RM_SUB:{uid}:{cid}", style="danger"),
        ])
    if not rows:
        rows.append([InlineKeyboardButton("لا يوجد مالكون فرعيون", callback_data="NOOP")])
    rows.append([InlineKeyboardButton("+ اضافة مالك فرعي", callback_data=f"ADD_SUB:{cid}", style="success")])
    rows.append([InlineKeyboardButton("رجوع", callback_data=f"CH:{cid}")])
    return InlineKeyboardMarkup(rows)


def kb_bans(cid):
    bans = DB.get_ban_settings(cid)
    rows = []
    for ban_type, label in BAN_TYPES.items():
        cur = 1 if bans.get(ban_type) else 0
        cb = f"TGL_BAN:{cid}:{ban_type}:{cur}"
        st = _style(cur, "guard")
        if ban_type == "custom":
            count = len(DB.get_ban_items(cid, item_type="word"))
            rows.append([InlineKeyboardButton(f"{_lk(cur)} {label} ({count})", callback_data=cb, style=st)])
        else:
            rows.append([InlineKeyboardButton(f"{_lk(cur)} {label}", callback_data=cb, style=st)])
    rows.append([InlineKeyboardButton("ادارة الكلمات المحظورة", callback_data=f"CUSTOM_BANS:{cid}")])
    rows.append([InlineKeyboardButton("رجوع", callback_data=f"CH:{cid}")])
    return InlineKeyboardMarkup(rows)


def kb_custom_bans(cid):
    items = DB.get_ban_items(cid, item_type="word")
    rows = []
    for item_id, item_type, item_value in items:
        rows.append([
            InlineKeyboardButton(f"كلمة: {item_value[:25]}", callback_data="NOOP"),
            InlineKeyboardButton("X حذف", callback_data=f"RM_BAN_ITEM:{item_id}:{cid}", style="danger"),
        ])
    if not items:
        rows.append([InlineKeyboardButton("لا توجد كلمات محظورة", callback_data="NOOP")])
    rows.append([InlineKeyboardButton("+ اضافة كلمة محظورة", callback_data=f"ADD_BAN_WORD:{cid}", style="success")])
    rows.append([InlineKeyboardButton("رجوع", callback_data=f"BANS:{cid}")])
    return InlineKeyboardMarkup(rows)


async def error_handler(update, ctx):
    err = str(ctx.error)
    silent = ("not modified", "message_id_invalid", "message to edit not found",
              "query is too old", "bot was blocked", "user is deactivated",
              "chat not found", "can't access the chat", "have no rights",
              "not enough rights", "peer_id_invalid", "message not found",
              "can't demote", "user not found", "chat_write_forbidden",
              "need administrator rights", "method not found",
              "bot can't initiate conversation", "retry after",
              "too many requests", "flood control", "message is not modified",
              "message can't be deleted", "message to delete not found",
              "timed out", "network", "connection", "httpx")
    if any(x in err.lower() for x in silent):
        return
    logger.error(f"Update error: {ctx.error}")


async def safe_edit(q, text, **kwargs):
    try:
        await q.edit_message_text(text, **kwargs)
    except Exception as e:
        err = str(e).lower()
        if any(x in err for x in ("not modified", "message is not modified", "invalid", "not found")):
            return
        logger.warning(f"safe_edit err: {e}")


async def try_edit_or_send(q, text, **kwargs):
    try:
        await q.edit_message_text(text, **kwargs)
        return
    except Exception as e:
        err = str(e).lower()
        if "not modified" in err or "message is not modified" in err:
            return
    try:
        if q.message:
            await q.message.reply_text(text, **kwargs)
    except Exception as e:
        logger.warning(f"try_edit_or_send err: {e}")


def clear_flow(ctx):
    for k in list(ctx.user_data.keys()):
        if (k in ("flow", "flow_ch", "admsess_cid", "admsess_tuid", "admsess_promote")
                or k.startswith("perms:")):
            del ctx.user_data[k]


async def safe_answer(q, text=None, show_alert=False):
    try:
        if text is not None:
            await q.answer(text, show_alert=show_alert)
        else:
            await q.answer()
    except Exception:
        pass


def _parse_int(s):
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def _ulink(name, uid):
    """رابط باسم مدموج بلا اظهار المعرّف الرقمي (المعرّف داخل الرابط فقط)."""
    return '<a href="tg://user?id=%s">%s</a>' % (uid, _safe_html_escape(str(name or "مستخدم")))


def _ulink_user(user):
    if user is None:
        return "مستخدم"
    name = getattr(user, "full_name", None) or getattr(user, "first_name", None) or "مستخدم"
    return _ulink(name, getattr(user, "id", 0))


def _safe_html_escape(s):
    if s is None:
        return ""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def cmd_start(update, ctx):
    uid = update.effective_user.id
    if not DB.is_known_user(uid):
        return
    name = ""
    if update.effective_user:
        name = update.effective_user.full_name or ""
    clear_flow(ctx)
    await send_start(update, uid, name)


async def cb_handler(update, ctx):
    """تيليجرام يقبل اجابة واحدة فقط لكل ضغطة زر.
    كان بعض الفروع يجيب مرتين، فتضيع الرسالة الثانية بصمت وتبدو كأن
    الزر لم يفعل شيئا. الآن: اجابة واحدة مضمونة لكل ضغطة."""
    q = update.callback_query
    _answered = [False]

    async def ans(text=None, alert=False):
        if _answered[0]:
            return
        _answered[0] = True
        await safe_answer(q, text, alert)

    try:
        await _cb_body(update, ctx, ans)
    except Exception as e:
        logger.exception(f"callback error data={q.data!r}: {e}")
        await ans("حدث خطأ. حاول مرة اخرى.", alert=True)
    finally:
        await ans()


async def _cb_body(update, ctx, ans):
    q = update.callback_query
    uid = q.from_user.id
    d = q.data or ""
    bot = ctx.bot

    if not DB.is_known_user(uid):
        return

    if not d:
        return

    if d.startswith("ADMSESS:"):
        if uid != OWNER_ID:
            await ans("للمالك فقط", alert=True)
            return
        try:
            if await _get_admin_extras().handle_callback(q, ans, ctx):
                return
        except Exception as e:
            logger.exception(f"admin_extras callback err: {e}")
            await ans("حدث خطأ.", alert=True)
            return

    if d == "NOOP":
        await ans()
        return

    if d == "CANCEL_FLOW":
        await ans()
        clear_flow(ctx)
        name = ""
        try:
            name = q.from_user.full_name or ""
        except Exception:
            pass
        await send_start(q, uid, name)
        return

    if d == "BACK_MAIN":
        await ans()
        clear_flow(ctx)
        name = ""
        try:
            name = q.from_user.full_name or ""
        except Exception:
            pass
        await send_start(q, uid, name)
        return

    if d == "ADD_CH":
        await ans()
        if uid != OWNER_ID:
            return
        ctx.user_data["flow"] = "add_channel"
        await try_edit_or_send(q,
            "<b>اضافة قناة جديدة</b>\n\n"
            "1. اضف البوت كمشرف بكامل الصلاحيات\n"
            "2. ارسل ID القناة (مثال: <code>-1001234567890</code>)",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("الغاء", callback_data="CANCEL_FLOW", style="danger")]]))
        return

    if d == "MGMT_USERS":
        if uid != OWNER_ID:
            await ans("للمالك فقط", alert=True)
            return
        ctx.user_data["flow"] = "mgmt_users"
        await ans()
        await try_edit_or_send(q,
            "<b>ادارة المستخدمين</b>\n\n"
            "<code>add &lt;user_id&gt; &lt;channel_id&gt;</code>\n"
            "<code>remove &lt;user_id&gt; &lt;channel_id&gt;</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("الغاء", callback_data="CANCEL_FLOW", style="danger")]]))
        return

    if d.startswith("CH:"):
        cid = _parse_int(d[3:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        await render_panel(q, bot, cid, uid)
        return

    if d.startswith("TGL:"):
        parts = d.split(":")
        if len(parts) < 3:
            return
        setting = parts[1]
        cid = _parse_int(parts[2])
        expected = _parse_int(parts[3]) if len(parts) > 3 else None
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        entry = TOGGLE_MAP.get(setting)
        if not entry:
            return
        col, label, _mode = entry

        current = 1 if DB.get_settings(cid, fresh=True).get(col) else 0
        if expected is not None and current != expected:
            # مالك آخر غيّر الحالة بعد ان ظهرت لك اللوحة.
            # لا نقلب الاعداد بناء على شاشة قديمة.
            await ans(f"{label}: تم تغييره من مستخدم آخر. تم تحديث اللوحة.", alert=True)
            await render_panel(q, bot, cid, uid)
            return

        _, nv = DB.set_setting(cid, col, 0 if current else 1)

        if col == "lock_messages":
            if nv:
                await ans(f"{label}: مقفلة - جاري سحب صلاحيات النشر من المشرفين...")
            else:
                await ans(f"{label}: مفتوحة - جاري استعادة صلاحيات المشرفين...")
            asyncio.create_task(sync_chat_lock(ctx.bot, cid))
        elif col == "spam_media_enabled":
            await ans(f"{label}: {'مفعّلة' if nv else 'معطّلة'}")
        else:
            await ans(f"{label}: {'مقفل' if nv else 'مفتوح'}")
        await render_panel(q, bot, cid, uid)
        return

    if d.startswith("TGL_SPAM:"):
        # صيغة قديمة من ازرار سابقة: نحوّلها لزر سبام الوسائط الجديد
        parts = d.split(":")
        cid = _parse_int(parts[2]) if len(parts) > 2 else None
        if cid is None or not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        await ans("تم تحديث اللوحة.")
        await render_panel(q, bot, cid, uid)
        return

    if d.startswith("RLIST:"):
        cid = _parse_int(d[6:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        ch = DB.get_channel(cid)
        await try_edit_or_send(q,
            "<b>استعادة المشرفين - " + _safe_html_escape(ch_label(ch) if ch else str(cid)) + "</b>\n\n"
            "هؤلاء من نزّلهم البوت تلقائيا فقط.",
            parse_mode=ParseMode.HTML, reply_markup=await kb_restore_list(bot, cid))
        return

    if d.startswith("RONE:"):
        parts = d.split(":")
        if len(parts) < 3:
            return
        cid = _parse_int(parts[1])
        tuid = _parse_int(parts[2])
        if cid is None or tuid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        saved = DB.get_demoted_admin_perms(tuid, cid)
        if not saved:
            await ans("لم يعد هذا المشرف في قائمة الاستعادة.", alert=True)
        else:
            try:
                await promote_with_stories(bot, cid, tuid, saved)
                DB.delete_bot_demoted_admin(tuid, cid)
                await ans("تمت استعادة صلاحياته.", alert=True)
            except Exception as e:
                await ans(f"تعذرت الاستعادة: {e}", alert=True)
        await try_edit_or_send(q, "<b>استعادة المشرفين</b>", parse_mode=ParseMode.HTML,
                               reply_markup=await kb_restore_list(bot, cid))
        return

    if d.startswith("RDROP:"):
        parts = d.split(":")
        if len(parts) < 3:
            return
        cid = _parse_int(parts[1])
        tuid = _parse_int(parts[2])
        if cid is None or tuid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        DB.exclude_from_restore(tuid, cid, "manual")
        await ans("تمت ازالته من قائمة الاستعادة ولن يُرفع تلقائيا.", alert=True)
        await try_edit_or_send(q, "<b>استعادة المشرفين</b>", parse_mode=ParseMode.HTML,
                               reply_markup=await kb_restore_list(bot, cid))
        return

    if d.startswith("DEMOTE_ALL:"):
        cid = _parse_int(d[11:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        await ans("جاري تجريد جميع المشرفين...", alert=True)
        await snapshot_and_demote_all(bot, cid)
        DB.set_emergency_lock(cid, int(time.time()) + EMERGENCY_LOCK_SECS)
        ch = DB.get_channel(cid)
        await try_edit_or_send(q,
            f"<b>تم تجريد جميع المشرفين</b>\n\n"
            f"القناة: {_safe_html_escape(ch_label(ch) if ch else cid)}\n"
            f"الوقت: {ts_fmt()}",
            parse_mode=ParseMode.HTML,
            reply_markup=_emergency_markup(cid, is_anon=True))
        return

    if d.startswith("BANS:"):
        cid = _parse_int(d[5:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        ch = DB.get_channel(cid)
        await ans()
        await try_edit_or_send(q,
            f"<b>قائمة المنع - {_safe_html_escape(ch_label(ch) if ch else cid)}</b>\n\n"
            "فعل او اوقف اي منع\n"
            "اذا خالف مشرف اي قاعدة يجرد فورا.",
            parse_mode=ParseMode.HTML, reply_markup=kb_bans(cid))
        return

    if d.startswith("ADD_ASSISTANT:"):
        cid = _parse_int(d[14:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        if DB.get_assistant_admin(cid):
            await ans("الحساب المساعد مُفعّل بالفعل في هذه القناة.", alert=True)
            return
        if not assistant_configured():
            await ans("الحساب المساعد غير مُهيأ بعد. ضع ASSISTANT_API_ID / ASSISTANT_API_HASH / ASSISTANT_SESSION_STRING في اعلى الكود.", alert=True)
            return
        ch = DB.get_channel(cid)
        await ans()
        await try_edit_or_send(q,
            f"<b>اضافة الحساب المساعد - {_safe_html_escape(ch_label(ch) if ch else cid)}</b>\n\n"
            "سيقوم البوت برفع الحساب المساعد تلقائياً كمشرف في هذه القناة، "
            "لفتح جميع مميزات البوت",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("تأكيد الرفع", callback_data=f"ADD_ASSISTANT_CONFIRM:{cid}", style="success"),
                InlineKeyboardButton("رجوع", callback_data=f"CH:{cid}", style="danger"),
            ]]))
        return

    if d.startswith("ADD_ASSISTANT_CONFIRM:"):
        cid = _parse_int(d[22:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        if DB.get_assistant_admin(cid):
            await ans("الحساب المساعد مُفعّل بالفعل في هذه القناة.", alert=True)
            return
        await ans("جاري رفع الحساب المساعد...")
        ch = DB.get_channel(cid)
        name = _safe_html_escape(ch_label(ch) if ch else cid)
        try:
            ok, result = await add_assistant_to_channel(bot, cid, uid)
        except Exception as e:
            logger.error(f"ADD_ASSISTANT_CONFIRM unexpected error: {e}")
            ok, result = False, str(e)
        if ok:
            await try_edit_or_send(q,
                f"تم رفع الحساب المساعد بنجاح في <b>{name}</b>.\n\n"
                f"الحساب: {_safe_html_escape(getattr(result, 'first_name', '') or str(getattr(result, 'id', '')))}",
                parse_mode=ParseMode.HTML, reply_markup=back_kb(f"CH:{cid}"))
        else:
            await try_edit_or_send(q,
                f"تعذر رفع الحساب المساعد في <b>{name}</b>.\n\n"
                f"السبب: <code>{_safe_html_escape(str(result))}</code>",
                parse_mode=ParseMode.HTML, reply_markup=back_kb(f"CH:{cid}"))
        return

    if d.startswith("ASSISTANT_INFO:"):
        cid = _parse_int(d[15:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        assistant_uid = DB.get_assistant_admin(cid)
        if not assistant_uid:
            await ans("الحساب المساعد غير مُفعّل في هذه القناة.", alert=True)
            return
        ch = DB.get_channel(cid)
        name = _safe_html_escape(ch_label(ch) if ch else cid)
        await try_edit_or_send(q,
            f"<b>الحساب المساعد - {name}</b>\n\n"
            f"معرف الحساب: <code>{assistant_uid}</code>\n"
            f"يقوم هذا الحساب بمراقبة سجل الاحداث لتحديد هوية من يعدل الرسائل فعلياً.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("ازالة الحساب المساعد من القناة", callback_data=f"RM_ASSISTANT:{cid}", style="danger")],
                [InlineKeyboardButton("رجوع", callback_data=f"CH:{cid}")],
            ]))
        return

    if d.startswith("RM_ASSISTANT:"):
        cid = _parse_int(d[13:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        assistant_uid = DB.get_assistant_admin(cid)
        if assistant_uid:
            try:
                await bot.ban_chat_member(cid, assistant_uid)
                await bot.unban_chat_member(cid, assistant_uid)
            except Exception as e:
                logger.warning(f"RM_ASSISTANT kick error: {e}")
        DB.remove_assistant_admin(cid)
        await ans("تمت ازالة الحساب المساعد.", alert=True)
        ch = DB.get_channel(cid)
        text = await channel_panel_text(bot, cid, ch)
        await try_edit_or_send(q, text, parse_mode=ParseMode.HTML, reply_markup=kb_channel(cid, uid))
        return

    if d.startswith("TGL_BAN:"):
        parts = d.split(":")
        if len(parts) < 3:
            return
        cid = _parse_int(parts[1])
        ban_type = parts[2]
        expected = _parse_int(parts[3]) if len(parts) > 3 else None
        if cid is None or ban_type not in BAN_TYPES:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        label = BAN_TYPES.get(ban_type, ban_type)
        current = 1 if DB.get_ban_settings(cid).get(ban_type) else 0
        if expected is not None and current != expected:
            await ans(f"{label}: تم تغييره من مستخدم آخر. تم تحديث القائمة.", alert=True)
        else:
            nv = DB.toggle_ban(cid, ban_type)
            await ans(f"{label}: {'مفعل' if nv else 'معطل'}")
        try:
            await q.edit_message_reply_markup(reply_markup=kb_bans(cid))
        except Exception:
            pass
        return

    if d.startswith("CUSTOM_BANS:"):
        cid = _parse_int(d[12:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        ch = DB.get_channel(cid)
        await ans()
        await try_edit_or_send(q,
            f"<b>المنع المخصص - {_safe_html_escape(ch_label(ch) if ch else cid)}</b>\n\n"
            "يمكنك اضافة كلمات محظورة. عند وجود كلمة محظورة في رسالة مشرف يتم تجريده.",
            parse_mode=ParseMode.HTML, reply_markup=kb_custom_bans(cid))
        return

    if d.startswith("ADD_BAN_WORD:"):
        cid = _parse_int(d[13:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        ctx.user_data["flow"] = "add_ban_word"
        ctx.user_data["flow_ch"] = cid
        await ans()
        await try_edit_or_send(q, "ارسل الكلمة المحظورة:",
                               parse_mode=ParseMode.HTML,
                               reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("الغاء", callback_data="CANCEL_FLOW", style="danger")]]))
        return

    if d.startswith("RM_BAN_ITEM:"):
        parts = d.split(":")
        if len(parts) < 3:
            return
        item_id = _parse_int(parts[1])
        cid = _parse_int(parts[2])
        if item_id is None or cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        DB.remove_ban_item(item_id, cid)
        await ans("تم الحذف.", alert=True)
        try:
            await q.edit_message_reply_markup(reply_markup=kb_custom_bans(cid))
        except Exception:
            pass
        return

    if d.startswith("STOP_ELOCK:"):
        cid = _parse_int(d[11:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        DB.clear_emergency_lock(cid)
        spam.reset_anon(cid)
        DB.clear_media_tracker(cid=cid)
        ch = DB.get_channel(cid)
        demoted_list = DB.get_bot_demoted_admins(cid)
        btns = []
        if demoted_list:
            btns.append([InlineKeyboardButton(f"استعادة المشرفين ({len(demoted_list)})",
                                              callback_data=f"RLIST:{cid}", style="success")])
        btns.append([InlineKeyboardButton("رجوع للقناة", callback_data=f"CH:{cid}")])
        await ans()
        await try_edit_or_send(q,
            f"<b>تم ايقاف قفل الطوارئ</b>\n\n"
            f"القناة: {_safe_html_escape(ch_label(ch) if ch else cid)}\n"
            f"الوقت: {ts_fmt()}\n\n"
            f"القناة عادت للعمل الطبيعي.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(btns))
        return

    if d.startswith("RESTORE_ADMIN:"):
        parts = d.split(":")
        if len(parts) < 3:
            return
        cid = _parse_int(parts[1])
        tuid = _parse_int(parts[2])
        if cid is None or tuid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        if tuid == 0:
            demoted = DB.get_bot_demoted_admins(cid)
            if len(demoted) == 1:
                tuid = demoted[0][0]
            else:
                await ans("تعذر تحديد المشرف. استخدم استعادة جميع المشرفين.", alert=True)
                return
        # لا نرجع الى bot_admins عمدا: ذلك الجدول يحتفظ بكل من رُقّي يوما ما،
        # وكان يعيد رفع مشرفين نزّلهم المالك بنفسه.
        saved = DB.get_demoted_admin_perms(tuid, cid)
        if not saved:
            if DB.is_restore_excluded(tuid, cid):
                await ans("هذا المشرف مستبعد من الاستعادة (نزّلته بنفسك).", alert=True)
            else:
                await ans("لا توجد صلاحيات محفوظة لهذا المشرف.", alert=True)
            return
        try:
            await promote_with_stories(bot, cid, tuid, saved)
            DB.delete_bot_demoted_admin(tuid, cid)
            ch = DB.get_channel(cid)
            lu = DB.get_emergency_lock(cid)
            ea = lu and lu > int(time.time())
            btns = []
            if ea:
                btns.append([InlineKeyboardButton("ايقاف قفل الطوارئ", callback_data=f"STOP_ELOCK:{cid}", style="danger")])
            btns.append([InlineKeyboardButton("رجوع للقناة", callback_data=f"CH:{cid}")])
            await ans()
            await try_edit_or_send(q,
                f"<b>تم استعادة صلاحيات المشرف</b>\n\n"
                f"القناة: {_safe_html_escape(ch_label(ch) if ch else cid)}\n"
                f"الوقت: {ts_fmt()}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(btns))
        except Exception as e:
            await ans(f"خطأ: {e}", alert=True)
        return

    if d.startswith("RESTORE_ALL:"):
        cid = _parse_int(d[12:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        demoted = DB.get_bot_demoted_admins(cid)
        if not demoted:
            await ans("لا يوجد مشرفون نزّلهم البوت تلقائيا.", alert=True)
            return
        await ans(f"جاري استعادة {len(demoted)} مشرف...", alert=True)
        success, failed = await restore_bot_demoted_admins(bot, cid)
        ch = DB.get_channel(cid)
        fail_text = f"\nفشل: {failed}" if failed else ""
        lu = DB.get_emergency_lock(cid)
        ea = lu and lu > int(time.time())
        btns = []
        if ea:
            btns.append([InlineKeyboardButton("ايقاف قفل الطوارئ", callback_data=f"STOP_ELOCK:{cid}", style="danger")])
        btns.append([InlineKeyboardButton("رجوع للقناة", callback_data=f"CH:{cid}")])
        await ans()
        await try_edit_or_send(q,
            f"<b>تم استعادة {success} مشرف بنجاح</b>{fail_text}\n\n"
            f"القناة: {_safe_html_escape(ch_label(ch) if ch else cid)}\n"
            f"الوقت: {ts_fmt()}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(btns))
        return

    if d.startswith("DEL_CH:"):
        cid = _parse_int(d[7:])
        if cid is None:
            return
        if uid != OWNER_ID:
            await ans("للمالك فقط", alert=True)
            return
        DB.remove_channel(cid)
        await ans()
        await try_edit_or_send(q, "تمت ازالة القناة.", reply_markup=kb_main(uid))
        return

    if d.startswith("ADMINS:"):
        cid = _parse_int(d[7:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        ch = DB.get_channel(cid)
        await ans()
        await try_edit_or_send(q,
            f"<b>مشرفو {_safe_html_escape(ch_label(ch) if ch else cid)}</b>\n\n"
            f"اختر مشرفا لتعديل صلاحياته او لتنزيله.\n"
            f"المالك لا يمكن تعديل صلاحياته",
            parse_mode=ParseMode.HTML, reply_markup=await kb_admins(bot, cid))
        return

    if d.startswith("ADM_DETAIL:"):
        parts = d.split(":")
        if len(parts) < 3:
            return
        cid = _parse_int(parts[1])
        tuid = _parse_int(parts[2])
        if cid is None or tuid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        try:
            m = await bot.get_chat_member(cid, tuid)
            if m.status == ChatMemberStatus.OWNER:
                await ans("المالك لا يمكن تعديل صلاحياته", alert=True)
                return
            username = f"@{m.user.username}" if (m.user and m.user.username) else "-"
            full_name = _safe_html_escape(m.user.full_name if m.user else str(tuid))
            await ans()
            await try_edit_or_send(q,
                f"<b>{full_name}</b>\n"
                f"المعرف: {_safe_html_escape(username)}\n"
                f"الحالة: مشرف",
                parse_mode=ParseMode.HTML, reply_markup=kb_admin_detail(cid, tuid))
        except Exception as e:
            await ans(f"خطأ: {e}", alert=True)
        return

    if d.startswith("DEMOTE:"):
        parts = d.split(":")
        if len(parts) < 3:
            return
        cid = _parse_int(parts[1])
        tuid = _parse_int(parts[2])
        if cid is None or tuid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        # تنزيل بقرار المالك: يُستبعد من الاستعادة حتى لا يعود مع
        # «استعادة الجميع». للاعادة لاحقا: ارفعه يدويا كمشرف فيُزال الاستبعاد تلقائيا.
        await save_and_demote(bot, cid, tuid, queue_restore=False, reason="demote_by_owner")
        DB.delete_snapshot(tuid, cid)
        await ans("تم تنزيل المشرف، ولن يُعاد رفعه تلقائيا.", alert=True)
        ch = DB.get_channel(cid)
        await ans()
        await try_edit_or_send(q,
            f"<b>مشرفو {_safe_html_escape(ch_label(ch) if ch else cid)}</b>\n\nاختر مشرفا:",
            parse_mode=ParseMode.HTML, reply_markup=await kb_admins(bot, cid))
        return

    if d.startswith("PROMOTE_NEW:"):
        cid = _parse_int(d[12:])
        if cid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        # رقم هاتف (يبدأ بـ +) -> تسجيل دخول للحساب؛ ID (ارقام) -> رفع مباشر بلا جلسة.
        ctx.user_data["flow"] = "admsess_phone"
        ctx.user_data["admsess_cid"] = cid
        ctx.user_data["admsess_tuid"] = None
        ctx.user_data["admsess_promote"] = True
        await ans()
        await try_edit_or_send(q,
            "<b>اضافة مشرف جديد</b>\n\n"
            "لا يتم الرفع الا بعد تسجيل الدخول لحساب المشرف او رفعه عن طريق الـID\n"
            "ارسل رقم هاتف المشرف او الـID:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                "الغاء", callback_data=f"ADMSESS:CANCEL:{cid}:0", style="danger")]]))
        return

    if d.startswith("EDIT_PERMS:"):
        parts = d.split(":")
        if len(parts) < 3:
            return
        cid = _parse_int(parts[1])
        tuid = _parse_int(parts[2])
        if cid is None or tuid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        saved = DB.get_admin_perms(tuid, cid)
        if saved:
            perms = saved
        else:
            try:
                m = await bot.get_chat_member(cid, tuid)
                perms = {k: bool(getattr(m, k, False)) for k in ALL_PERMS}
            except Exception:
                perms = dict(DEFAULT_ADMIN_PERMS)
        ctx.user_data[f"perms:{cid}:{tuid}"] = perms
        await ans()
        await try_edit_or_send(q, "<b>تعديل الصلاحيات</b>\n\nانقر لتفعيل او ايقاف:",
                        parse_mode=ParseMode.HTML, reply_markup=kb_perms(cid, tuid, perms))
        return

    if d.startswith("PERM:"):
        parts = d.split(":", 3)
        if len(parts) < 4:
            return
        cid = _parse_int(parts[1])
        tuid = _parse_int(parts[2])
        perm_key = parts[3]
        if cid is None or tuid is None:
            return
        if perm_key not in ALL_PERMS:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        pkey = f"perms:{cid}:{tuid}"
        perms = ctx.user_data.get(pkey, {})
        perms[perm_key] = not perms.get(perm_key, False)
        ctx.user_data[pkey] = perms
        try:
            await q.edit_message_reply_markup(reply_markup=kb_perms(cid, tuid, perms))
        except Exception:
            pass
        return

    if d.startswith("SAVE_PERMS:"):
        parts = d.split(":")
        if len(parts) < 3:
            return
        cid = _parse_int(parts[1])
        tuid = _parse_int(parts[2])
        if cid is None or tuid is None:
            return
        if not DB.is_authorized(uid, cid):
            await ans("ليس لديك صلاحية", alert=True)
            return
        perms = ctx.user_data.get(f"perms:{cid}:{tuid}", DEFAULT_ADMIN_PERMS)
        try:
            await promote_with_stories(bot, cid, tuid, perms)
            DB.save_admin(tuid, cid, perms, uid)
            await ans("تم الحفظ بنجاح!", alert=True)
            name = str(tuid)
            try:
                m = await bot.get_chat_member(cid, tuid)
                name = m.user.full_name
            except Exception:
                pass
            ctx.user_data.pop(f"perms:{cid}:{tuid}", None)
            await ans()
            await try_edit_or_send(q,
                f"تم تحديث صلاحيات <b>{_safe_html_escape(name)}</b>",
                parse_mode=ParseMode.HTML, reply_markup=back_kb(f"ADM_DETAIL:{cid}:{tuid}"))
        except Exception as e:
            await ans(f"خطأ: {e}", alert=True)
        return

    if d.startswith("SUBOWNERS:"):
        cid = _parse_int(d[10:])
        if cid is None:
            return
        if uid != OWNER_ID:
            await ans("للمالك فقط", alert=True)
            return
        ch = DB.get_channel(cid)
        await ans()
        await try_edit_or_send(q,
            f"<b>المالكون الفرعيون - {_safe_html_escape(ch_label(ch) if ch else cid)}</b>\n\n"
            "المالكون الفرعيون يمكنهم ادارة القناة بكامل الصلاحيات.",
            parse_mode=ParseMode.HTML, reply_markup=kb_sub_owners(cid))
        return

    if d.startswith("ADD_SUB:"):
        cid = _parse_int(d[8:])
        if cid is None:
            return
        if uid != OWNER_ID:
            await ans("للمالك فقط", alert=True)
            return
        ctx.user_data["flow"] = "add_sub"
        ctx.user_data["flow_ch"] = cid
        await ans()
        await try_edit_or_send(q, "<b>➕ اضافة مالك فرعي</b>\n\nارسل ID المستخدم لاضافته كمالك فرعي:",
                               parse_mode=ParseMode.HTML,
                               reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("الغاء", callback_data="CANCEL_FLOW", style="danger")]]))
        return

    if d.startswith("RM_SUB:"):
        parts = d.split(":")
        if len(parts) < 3:
            return
        tuid = _parse_int(parts[1])
        cid = _parse_int(parts[2])
        if tuid is None or cid is None:
            return
        if uid != OWNER_ID:
            await ans("للمالك فقط", alert=True)
            return
        DB.remove_sub_owner(tuid, cid)
        await ans("تمت الازالة.", alert=True)
        try:
            await q.edit_message_reply_markup(reply_markup=kb_sub_owners(cid))
        except Exception:
            pass
        return

    await ans()


async def private_msg(update, ctx):
    # اضافة مشرف جديد: نميّز حسب رمز الدولة - رقم الهاتف يبدأ بـ + (تسجيل دخول
    # للحساب)، اما الـID فأرقام بلا + فنرفعه مباشرة عبر مسار "promote" (بلا جلسة،
    # فتنطبق عليه كل المميزات عدا ما يحتاج تسجيل دخول).
    _m = update.message
    if (_m and ctx.user_data.get("flow") == "admsess_phone"
            and ctx.user_data.get("admsess_promote")):
        _t = (_m.text or "").strip()
        if _t and not _t.startswith("+") and _t.lstrip("-").isdigit():
            ctx.user_data["flow"] = "promote"
            ctx.user_data["flow_ch"] = ctx.user_data.get("admsess_cid")
            for _k in ("admsess_cid", "admsess_tuid", "admsess_promote"):
                ctx.user_data.pop(_k, None)

    # امرر رسائل تدفق تسجيل الدخول للحساب المساعد اولاً
    try:
        if await _get_admin_extras().handle_private_message(update, ctx):
            return
    except Exception as e:
        logger.exception(f"admin_extras pm err: {e}")

    # --- المعالجه الاصليه ---
    if not update.message:
        return
    uid = update.effective_user.id if update.effective_user else 0
    if not DB.is_known_user(uid):
        return
    bot = ctx.bot
    name = (update.effective_user.full_name if update.effective_user else "") or ""
    text = (update.message.text or "").strip()
    flow = ctx.user_data.get("flow")

    if not flow:
        await send_start(update, uid, name)
        return

    if flow == "add_channel":
        if uid != OWNER_ID:
            clear_flow(ctx)
            return
        cid = _parse_int(text)
        if cid is None:
            await update.message.reply_text(
                "❌ ارسل ID القناة كرقم فقط.\n\n"
                "<b>مثال:</b> <code>-1001234567890</code>\n\n"
                "<b>طريقة الحصول على ID:</b>\n"
                "ارسل اي رسالة من قناتك الى @userinfobot",
                parse_mode=ParseMode.HTML)
            return
        try:
            chat = await bot.get_chat(cid)
        except Exception as e:
            await update.message.reply_text(
                f"❌ تعذر الوصول الى القناة.\n"
                f"<b>تأكد من:</b>\n"
                f"• صحة الـ ID\n"
                f"• البوت مضاف كعضو في القناة\n\n"
                f"<b>الخطأ:</b> <code>{_safe_html_escape(str(e))}</code>",
                parse_mode=ParseMode.HTML)
            return
        try:
            bm = await bot.get_chat_member(cid, bot.id)
            if bm.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                await update.message.reply_text(
                    "❌ البوت ليس مشرفا في القناة.\n\n"
                    "<b>الحل:</b> اضف البوت كمشرف في القناة (يكفي حذف الرسائل + تقييد + اضافة مشرفين) ثم اعد المحاولة.",
                    parse_mode=ParseMode.HTML)
                return
        except Exception as e:
            await update.message.reply_text(
                f"❌ تعذر التحقق من البوت في القناة.\n<b>الخطأ:</b> <code>{_safe_html_escape(str(e))}</code>",
                parse_mode=ParseMode.HTML)
            return
        try:
            um = await bot.get_chat_member(cid, uid)
            if um.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
                await update.message.reply_text(
                    "❌ يجب ان تكون مشرفا في القناة لتضيفها للبوت.",
                    parse_mode=ParseMode.HTML)
                return
        except Exception as e:
            await update.message.reply_text(
                f"❌ تعذر التحقق من صلاحياتك في القناة.\n"
                f"تأكد انك مشرف فيها.\n\n"
                f"<b>الخطأ:</b> <code>{_safe_html_escape(str(e))}</code>",
                parse_mode=ParseMode.HTML)
            return
        missing = []
        if not getattr(bm, "can_delete_messages", False):
            missing.append("حذف رسائل الاخرين")
        if not getattr(bm, "can_restrict_members", False):
            missing.append("حظر المستخدمين")
        if not getattr(bm, "can_promote_members", False):
            missing.append("اضافة مشرفين جدد")
        if not getattr(bm, "can_invite_users", False):
            missing.append("دعوة المستخدمين (لازمة لاضافة الحساب المساعد)")
        try:
            DB.add_channel(cid, chat.title, chat.username, uid)
            DB.add_sub_owner(uid, cid, uid)
        except Exception as e:
            await update.message.reply_text(f"❌ خطأ في حفظ القناة: <code>{_safe_html_escape(str(e))}</code>", parse_mode=ParseMode.HTML)
            return
        clear_flow(ctx)
        if missing:
            await update.message.reply_text(
                f"تمت اضافة <b>{_safe_html_escape(chat.title or 'القناة')}</b>\n\n"
                f"⚠️ لكن البوت ينقصه الصلاحيات التالية:\n• " + "\n• ".join(missing) +
                "\n\nفعل هذه الصلاحيات للبوت من اعدادات المشرفين في القناة لتعمل الحماية بشكل كامل.",
                parse_mode=ParseMode.HTML, reply_markup=back_kb("BACK_MAIN"))
        else:
            await update.message.reply_text(
                f"تمت اضافة <b>{_safe_html_escape(chat.title or 'القناة')}</b> بنجاح!\n\n"
                f"البوت جاهز لحماية القناة.",
                parse_mode=ParseMode.HTML, reply_markup=back_kb("BACK_MAIN"))
        return

    if flow == "promote":
        cid = ctx.user_data.get("flow_ch")
        if not isinstance(cid, int):
            await update.message.reply_text("❌ حدث خطأ. ابدأ من جديد.")
            clear_flow(ctx)
            await send_start(update, uid, name)
            return
        tuid = _parse_int(text)
        if tuid is None:
            await update.message.reply_text("❌ ارسل ID صحيحا (رقم فقط).")
            return
        try:
            await promote_with_stories(bot, cid, tuid, DEFAULT_ADMIN_PERMS)
            DB.save_admin(tuid, cid, DEFAULT_ADMIN_PERMS, uid)
            DB.add_admin_event(tuid, cid, "promote")
        except Exception as e:
            await update.message.reply_text(
                f"❌ تعذر ترقية المستخدم.\n"
                f"تأكد انه عضو في القناة.\n\n"
                f"<b>الخطأ:</b> <code>{_safe_html_escape(str(e))}</code>",
                parse_mode=ParseMode.HTML)
            return
        pname = str(tuid)
        try:
            m = await bot.get_chat_member(cid, tuid)
            pname = m.user.full_name or str(tuid)
        except Exception:
            pass
        clear_flow(ctx)
        await update.message.reply_text(
            f"تمت ترقية <b>{_safe_html_escape(pname)}</b> كمشرف بنجاح.\n\n"
            f"الصلاحيات الافتراضية:\n• نشر الرسائل\n• تعديل الرسائل\n• حذف الرسائل\n• ادارة البثوث المباشرة",
            parse_mode=ParseMode.HTML, reply_markup=back_kb(f"CH:{cid}"))
        return

    if flow == "mgmt_users" and uid == OWNER_ID:
        parts = text.split()
        if len(parts) != 3 or parts[0] not in ("add", "remove"):
            await update.message.reply_text(
                "❌ صيغة خاطئة.\n\n"
                "<b>للاضافة:</b> <code>add 123456789 -1001234567890</code>\n"
                "<b>للازالة:</b> <code>remove 123456789 -1001234567890</code>",
                parse_mode=ParseMode.HTML)
            return
        try:
            action = parts[0]
            tuid = int(parts[1])
            cid = int(parts[2])
        except ValueError:
            await update.message.reply_text(
                "❌ ارسل ارقام صحيحة.\n\n"
                "<b>مثال:</b> <code>add 123456789 -1001234567890</code>",
                parse_mode=ParseMode.HTML)
            return
        if action == "add":
            DB.add_sub_owner(tuid, cid, uid)
            msg = f"تمت اضافة <code>{tuid}</code> كمالك فرعي لقناة <code>{cid}</code>."
        else:
            DB.remove_sub_owner(tuid, cid)
            msg = f"تمت ازالة <code>{tuid}</code> من ملاك قناة <code>{cid}</code> الفرعيين."
        clear_flow(ctx)
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML, reply_markup=back_kb("BACK_MAIN"))
        return

    if flow == "add_sub" and uid == OWNER_ID:
        cid = ctx.user_data.get("flow_ch")
        if not isinstance(cid, int):
            await update.message.reply_text("❌ حدث خطأ. ابدأ من جديد.")
            clear_flow(ctx)
            await send_start(update, uid, name)
            return
        tuid = _parse_int(text)
        if tuid is None:
            await update.message.reply_text("❌ ارسل ID صحيحا (رقم).")
            return
        DB.add_sub_owner(tuid, cid, uid)
        clear_flow(ctx)
        await update.message.reply_text(
            f"تمت اضافة <code>{tuid}</code> كمالك فرعي.",
            parse_mode=ParseMode.HTML, reply_markup=back_kb(f"SUBOWNERS:{cid}"))
        return

    if flow == "add_ban_word":
        cid = ctx.user_data.get("flow_ch")
        if not isinstance(cid, int):
            await update.message.reply_text("❌ حدث خطأ. ابدأ من جديد.")
            clear_flow(ctx)
            await send_start(update, uid, name)
            return
        word = text.strip()
        if not word:
            await update.message.reply_text("❌ ارسل كلمة غير فارغة.")
            return
        if len(word) > 100:
            await update.message.reply_text("❌ الكلمة طويلة جدا (الحد 100 حرف).")
            return
        try:
            DB.add_ban_item(cid, "word", word)
            if not DB.get_ban_settings(cid).get("custom"):
                DB.toggle_ban(cid, "custom")
        except Exception as e:
            await update.message.reply_text(f"❌ خطأ: <code>{_safe_html_escape(str(e))}</code>", parse_mode=ParseMode.HTML)
            return
        ch = DB.get_channel(cid)
        clear_flow(ctx)
        await update.message.reply_text(
            f"تمت اضافة الكلمة المحظورة: <b>{_safe_html_escape(word)}</b>\n\n"
            f"القناة: {_safe_html_escape(ch_label(ch) if ch else str(cid))}",
            parse_mode=ParseMode.HTML, reply_markup=back_kb(f"CUSTOM_BANS:{cid}"))
        return

    clear_flow(ctx)
    await send_start(update, uid, name)


async def monitor_post(update, ctx):
    msg = update.channel_post
    if not msg:
        return
    cid = msg.chat.id
    bot = ctx.bot
    ch = DB.get_channel(cid)
    if not ch:
        return

    cache_msg(cid, msg.message_id, msg)

    lu = DB.get_emergency_lock(cid)
    if lu and int(time.time()) < lu:
        await safe_delete(bot, cid, msg.message_id)
        return

    s = DB.get_settings(cid)
    lock_msgs = s.get("lock_messages")
    lock_media = s.get("lock_media")
    delete_on_lock = s.get("delete_on_lock")
    spam_media_on = s.get("spam_media_enabled")
    is_media = has_media(msg)

    if lock_msgs:
        if delete_on_lock:
            await safe_delete(bot, cid, msg.message_id)
        return

    if lock_media and is_media:
        await safe_delete(bot, cid, msg.message_id)
        return

    sender_uid, identified = await identify_sender(bot, msg, cid)

    bans = DB.get_ban_settings(cid)
    if any(bans.values()):
        ban_items = DB.get_ban_items(cid) if bans.get("custom") else []
        violation = check_ban_violations(msg, bans, ban_items)
        if violation:
            target_uid, is_anon = await resolve_target_admin(bot, cid, sender_uid, identified)
            if is_anon is None:
                await safe_delete(bot, cid, msg.message_id)
                return
            # الترتيب: (1) حوّل الرسالة كدليل, (2) احذفها *فورا*, (3) احتواء
            # (تجريد الجميع) + فحص عبر الجلسات + استعادة الأبرياء + الاشعارات.
            await forward_violation_to_owners(bot, cid, msg, "انتهاك قائمة المنع: " + str(violation))
            await safe_delete(bot, cid, msg.message_id)
            await _do_violation(bot, target_uid or 0, cid, violation, is_anon=bool(is_anon), msg=msg)
            return

    if spam_media_on and has_media(msg):
        fuid = get_msg_file_uid(msg)
        DB.prune_media_tracker(cid)
        target_uid, is_anon = await resolve_target_admin(bot, cid, sender_uid, identified)
        if is_anon is None:
            return
        if is_anon:
            is_spam, ids = spam.add_anon(cid, msg.message_id, fuid)
            if is_spam:
                await _do_spam(bot, 0, cid, ids, "وسائط متكررة مجهولة", is_anon=True, msg=msg)
                return
        else:
            DB.add_media_track(target_uid, cid, msg.message_id, fuid)
            is_spam, ids = spam.add_media(target_uid, cid, msg.message_id, fuid)
            if is_spam:
                db_msgs = DB.get_media_messages(target_uid, cid)
                all_ids = list(dict.fromkeys(ids + db_msgs))
                await _do_spam(bot, target_uid, cid, all_ids[:20],
                               f"وسائط متكررة ({MEDIA_SPAM_LIMIT}+ في {MEDIA_SPAM_WINDOW}ث)", is_anon=False, msg=msg)
                return


async def _safe_task(coro, label):
    """مهمة خلفية لا تترك استثناء غير ملتقط."""
    try:
        await coro
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"background task {label}: {e}")


async def _edit_actor_followup(bot, cid, msg_id, change):
    """اشعار تعديل رسالة. التعديل لا يُنزّل مشرفا ابدا - فقط حذف + اشعار.
    اسم المعدِّل من سجل الادارة ان توفّر، والا "غير معروف". بلا اي معرّف رقمي."""
    ch = DB.get_channel(cid)
    name = _safe_html_escape(ch_label(ch) if ch else str(cid))
    editor_uid, editor_name, _trusted = await resolve_editor_by_id(cid, msg_id)
    mention = await editor_mention(bot, cid, editor_uid, editor_name)
    action_label = {
        "media_added":   "اضافة وسائط الى الرسالة",
        "media_changed": "تغيير الصورة/الوسائط المرفقة",
        "media_removed": "حذف الوسائط من الرسالة",
        "text_only":     "تعديل نص الرسالة",
        "unknown":       "تغيير الصورة/الوسائط المرفقة",
    }.get(change, "تعديل")
    await notify_owners(bot, cid,
        "<b>تعديل رسالة</b>\n\n"
        f"القناة: <b>{name}</b>\n"
        f"المعدِّل: {mention}\n"
        f"الاجراء: {action_label}\n"
        f"الوقت: {ts_fmt()}")


async def resolve_editor_by_id(cid, msg_id):
    """(uid, name, trusted) - trusted=True فقط اذا اتى من السجل الاداري."""
    found = await get_editor_via_assistant(cid, msg_id)
    if found:
        return found[0], found[1], True
    return None, None, False


async def monitor_edit(update, ctx):
    msg = update.edited_channel_post
    if not msg:
        return
    cid = msg.chat.id
    bot = ctx.bot
    ch = DB.get_channel(cid)
    if not ch:
        return

    is_old_message = False
    try:
        if msg.date:
            msg_ts = msg.date.timestamp() if hasattr(msg.date, "timestamp") else msg.date
            if (int(time.time()) - int(msg_ts)) > OLD_MESSAGE_NOTIFY_THRESHOLD:
                is_old_message = True
    except Exception:
        pass

    lu = DB.get_emergency_lock(cid)
    if lu and int(time.time()) < lu:
        await safe_delete(bot, cid, msg.message_id)
        return

    s = DB.get_settings(cid)
    lock_edits = bool(s.get("lock_edits"))

    bans = DB.get_ban_settings(cid)
    violation = None
    if any(bans.values()):
        ban_items = DB.get_ban_items(cid) if bans.get("custom") else []
        violation = check_ban_violations(msg, bans, ban_items)

    cached_val = msg_media_cache.get((cid, msg.message_id))
    if cached_val is None:
        cached_val = DB.get_cached_message(cid, msg.message_id)
    new = get_msg_file_uid(msg)

    if cached_val is None:
        change = "unknown"
    elif cached_val == _TEXT and new is not None:
        change = "media_added"
    elif cached_val != _TEXT and new is None:
        change = "media_removed"
    elif cached_val != _TEXT and new is not None and cached_val != new:
        change = "media_changed"
    else:
        change = "text_only"

    media_change = change in ("media_added", "media_changed", "media_removed")

    # الحذف مربوط بقفل التعديلات. "unknown" (رسالة بلا اصل محفوظ) تُحذف فقط
    # اذا صارت تحمل وسائط - الخطر المقصود هو تبديل/اضافة الصورة لا تصحيح كلمة.
    unknown_risky = change == "unknown" and new is not None
    should_delete = bool(violation) or (lock_edits and (media_change or unknown_risky))
    if should_delete:
        await safe_delete(bot, cid, msg.message_id)
        if MINIAPP_OK:
            try:
                miniapp.bump_edit_deleted(cid)
            except Exception:
                pass

    # اشعِر: عند اي حذف (حتى لرسالة قديمة سابقة للبوت)، او عند تغيير وسائط
    # لرسالة يعرفها البوت. الحذف يستحق اشعارا دائما - كان الاشعار يُفوَّت
    # سابقا للرسائل القديمة رغم حذفها.
    if should_delete or (media_change and not is_old_message):
        asyncio.create_task(
            _safe_task(_edit_actor_followup(bot, cid, msg.message_id, change),
                       f"edit_followup cid={cid} msg={msg.message_id}"))

    cache_msg(cid, msg.message_id, msg)


async def monitor_member_update(update, ctx):
    cmu = update.chat_member
    if not cmu:
        return
    cid = cmu.chat.id
    if not DB.get_channel(cid):
        return

    old_status = cmu.old_chat_member.status
    new_status = cmu.new_chat_member.status
    target_uid = cmu.new_chat_member.user.id
    actor = cmu.from_user
    bot_id = ctx.bot.id

    _cache.delete(f"admin_sig:{cid}")

    # ---- تنزيل مشرف من خارج البوت ----
    # كان البوت يبقيه في قائمة الاستعادة ويرفعه من جديد لاحقا رغم انك
    # نزّلته بنفسك. الآن: اي تنزيل لم ينفّذه البوت = قرار منك، فيُستبعد.
    if old_status == ChatMemberStatus.ADMINISTRATOR and new_status != ChatMemberStatus.ADMINISTRATOR:
        if actor and actor.id != bot_id and target_uid != DB.get_assistant_admin(cid):
            DB.exclude_from_restore(target_uid, cid, f"manual_by:{actor.id}")
            logger.info(f"manual demote cid={cid} uid={target_uid} by={actor.id} -> excluded")
            ch = DB.get_channel(cid)
            await notify_owners(ctx.bot, cid,
                f"<b>تنزيل مشرف من خارج البوت</b>\n\n"
                f"القناة: <b>{_safe_html_escape(ch_label(ch) if ch else str(cid))}</b>\n"
                f"المشرف: {_ulink_user(getattr(cmu.new_chat_member, 'user', None)) or _ulink('مشرف', target_uid)}\n"
                f"بواسطة: {_ulink_user(actor)}\n"
                f"الوقت: {ts_fmt()}\n\n"
                f"لن يُعيده البوت تلقائيا عند «استعادة جميع المشرفين». "
                f"ان اردت اعادته لاحقا ارفعه يدويا كمشرف وسيُزال الاستبعاد عنه تلقائيا.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("لوحة القناة", callback_data=f"CH:{cid}", style="primary")],
                ]))

    # ---- ترقية مشرف من خارج البوت ----
    if old_status != ChatMemberStatus.ADMINISTRATOR and new_status == ChatMemberStatus.ADMINISTRATOR:
        DB.unexclude_from_restore(target_uid, cid)
        DB.delete_bot_demoted_admin(target_uid, cid)
        if actor and actor.id != bot_id:
            # اذا كانت الدردشة مقفولة فالمشرف الجديد يجب ان يخضع للقفل ايضا
            if DB.get_settings(cid, fresh=True).get("lock_messages"):
                asyncio.create_task(sync_chat_lock(ctx.bot, cid))

    # ---- حظر مفرط ----
    if new_status not in (ChatMemberStatus.BANNED, ChatMemberStatus.RESTRICTED):
        return
    if old_status in (ChatMemberStatus.BANNED, ChatMemberStatus.RESTRICTED):
        return
    if not actor or actor.is_bot:
        return
    actor_uid = actor.id
    if _is_assistant_admin(cid, actor_uid):
        return   # الحساب المساعد لا يُعاقب
    try:
        member = await ctx.bot.get_chat_member(cid, actor_uid)
        if member.status != ChatMemberStatus.ADMINISTRATOR:
            return
    except Exception:
        return
    if ban_tracker.add_ban(actor_uid, cid):
        await save_and_demote(ctx.bot, cid, actor_uid, queue_restore=True, reason="ban_flood")
        try:
            m = await ctx.bot.get_chat_member(cid, actor_uid)
            mention = user_mention_html(m.user)
        except Exception:
            mention = f'<a href="tg://user?id={actor_uid}">{actor_uid}</a>'
        ch = DB.get_channel(cid)
        name = ch_label(ch) if ch else str(cid)
        await notify_owners(ctx.bot, cid,
            f"<b>تنبيه: حظر مفرط</b>\n\n"
            f"القناة: <b>{name}</b>\n"
            f"المشرف: {mention}\n"
            f"السبب: حظر اكثر من {BAN_LIMIT_COUNT} مستخدمين في {BAN_LIMIT_WINDOW}ث\n"
            f"الوقت: {ts_fmt()}",
            reply_markup=_emergency_markup(cid, is_anon=False, demoted_uid=actor_uid))


async def _maintenance_loop():
    while True:
        try:
            await asyncio.sleep(MAINTENANCE_INTERVAL)
            DB.prune_message_cache(MESSAGE_CACHE_MAX_AGE)
            DB.prune_spam_log(SPAM_LOG_MAX_AGE)
            if msg_media_cache:
                msg_media_cache.clear()
            purged = _cache.purge_expired()
            spam.purge_empty()
            ban_tracker.purge_empty()
            logger.info(f"maintenance: pruned message_cache, spam_log, {purged} cache entries, and empty spam/ban/flood trackers")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"maintenance error: {e}")


def _perms_from_member(member):
    return {k: bool(getattr(member, k, False)) for k in ALL_PERMS}


async def sync_channel_admins(bot, cid):
    try:
        real_admins = await _api_with_retry(lambda: bot.get_chat_administrators(cid))
    except Exception as e:
        logger.warning(f"admin_sync: get_chat_administrators failed cid={cid}: {e}")
        return
    real_by_id = {a.user.id: a for a in real_admins if not a.user.is_bot}
    chat_locked = bool(DB.get_settings(cid, fresh=True).get("lock_messages"))
    for uid, stored_perms in DB.get_bot_admins(cid):
        real = real_by_id.get(uid)
        if real is None or real.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            DB.delete_admin(uid, cid)
            DB.delete_snapshot(uid, cid)
            continue
        if real.status == ChatMemberStatus.OWNER:
            continue
        if chat_locked:
            # الدردشة مقفولة: الصلاحيات الظاهرة الآن مسحوبة، وحفظها يفسد
            # المرجع المحفوظ للمشرف.
            continue
        current_perms = _perms_from_member(real)
        if current_perms != stored_perms:
            DB.save_admin(uid, cid, current_perms, uid)

    # كان يمسح لقطة قفل الدردشة كاملة لمجرد غياب مشرف واحد، فيفقد كل
    # المشرفين صلاحياتهم عند فتح الدردشة. الآن نحذف الصف الخاص به فقط.
    for uid, _stored in DB.get_lock_demoted_admins(cid):
        if uid not in real_by_id:
            DB.delete_lock_demoted_admin(uid, cid)


async def _admin_sync_loop():
    while True:
        try:
            await asyncio.sleep(ADMIN_SYNC_INTERVAL)
            for cid in DB.get_all_channel_ids():
                await sync_channel_admins(_sync_bot_ref["bot"], cid)
            logger.info("admin_sync: completed permission sync pass")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"admin_sync_loop error: {e}")


async def _handle_dead_session(bot, extras, uid, cid):
    """جلسة (مشرف, قناة) اكتُشف انها أُخرجت: تنزيله من هذه القناة وحدها + اشعار +
    اسقاط جلسة هذه القناة فقط. تبقى جلساته في القنوات الأخرى سليمة (عزل تام)."""
    try:
        await _on_logout_demote(bot, cid, uid)
    except Exception as e:
        logger.warning(f"session_monitor demote cid={cid} uid={uid}: {e}")
    try:
        await extras.stop_one(uid, cid)
    except Exception:
        pass
    try:
        extras.db.delete(uid, cid)
    except Exception:
        pass
    # الأرشيف مربوط بعميل واحد للحساب (قد يكون عميل الجلسة الميتة). افصله؛
    # فان بقيت للحساب جلسة قناة اخرى اعِد ربطه عليها كي لا تتوقف الأرشفة.
    if MINIAPP_OK:
        try:
            await miniapp.detach_session(uid)
            if extras.db.channels_of(uid):
                await miniapp.attach_session(uid)
        except Exception:
            pass
    logger.info("session_monitor: uid=%s cid=%s session removed -> demoted from channel"
                % (uid, cid))


async def _check_new_logins(bot, extras, uid):
    """يكشف تسجيل دخول جديد على حساب المشرف عبر مقارنة قائمة تفويضاته الحالية
    بما رأيناه سابقا (مستقل عن اللغة تماما)."""
    try:
        auths = await extras.get_authorizations(uid)
    except Exception:
        auths = None
    if not auths:
        return
    known = DB.get_seen_auth_hashes(uid)
    if not known:
        # اول مرة: خُذ لقطة اساس بلا اشعار (كل الجلسات القائمة معروفة الآن)
        DB.add_seen_auth_hashes(uid, [h for h, _d, _l, _c in auths])
        return
    fresh = [(h, d, l, cur) for (h, d, l, cur) in auths if h not in known]
    if not fresh:
        return
    DB.add_seen_auth_hashes(uid, [h for h, _d, _l, _c in fresh])
    for _h, device, location, is_current in fresh:
        if is_current:
            continue   # لا نُشعر عن جلسة البوت نفسها
        if device and "eye of hell" in device.lower():
            continue   # جلسة انشأها البوت لقناة اخرى (جهاز eye of hell) - ليست دخولا للمشرف
        try:
            await _on_new_login(uid, device, location, True)
        except Exception as e:
            logger.warning("new-login notify uid=%s: %s" % (uid, e))


async def _ensure_last_seen(extras, uid):
    """يضبط استثناء (اخر ظهور) للحساب المساعد على حساب المشرف مرة واحدة، كي
    يظهر آخر ظهوره للبوت (على مستوى الحساب - اي جلسة تكفي)."""
    try:
        row = extras.db.get_any(uid)
        if row and row[-1]:   # last_seen_ok مضبوط اصلا
            return
        await extras._auto_last_seen_exception(uid)
    except Exception as e:
        logger.warning("ensure last-seen uid=%s: %s" % (uid, e))


async def _session_monitor_once(bot, extras):
    """تمريرة على كل جلسة (مشرف, قناة) على حدة: تُنزِّل من أُخرجت جلسة قناته منها
    وحدها. العمليات على مستوى الحساب (آخر ظهور/كشف دخول جديد) تُنفَّذ مرة لكل حساب.
    تعيد قائمة (uid, cid) التي عولجت."""
    handled = []
    alive_uids = set()
    for uid, cid in list(extras.db.all_pairs()):
        if uid == OWNER_ID:
            continue   # مالك القناة لا يُنزَّل
        alive = await extras.session_alive(uid, cid)
        if alive is False:
            await _handle_dead_session(bot, extras, uid, cid)
            handled.append((uid, cid))
        elif alive is True:
            alive_uids.add(uid)
    for uid in alive_uids:
        await _ensure_last_seen(extras, uid)
        await _check_new_logins(bot, extras, uid)
    return handled


async def _session_monitor_loop():
    """يكتشف اخراج/الغاء جلسة اي مشرف — حتى لو فعلها من (الأجهزة النشطة) في
    تيليجرام بلا ضغط زر داخل البوت — فيُشعِر المُلّاك ويُنزِّله من كل قنواته."""
    if admin_extras_mgr is None and not EX_TELETHON_OK:
        return
    await asyncio.sleep(15)   # امهال قصير ريثما تجهز الجلسات والأرشيف
    while True:
        try:
            await asyncio.sleep(SESSION_MONITOR_INTERVAL)
            bot = _sync_bot_ref["bot"]
            if bot is None:
                continue
            await _session_monitor_once(bot, _get_admin_extras())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"session_monitor_loop error: {e}")


_sync_bot_ref = {"bot": None}


async def _run():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start, filters=filters.ChatType.PRIVATE))
    app.add_handler(CallbackQueryHandler(cb_handler))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, private_msg))
    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST, monitor_post))
    app.add_handler(MessageHandler(filters.UpdateType.EDITED_CHANNEL_POST, monitor_edit))
    app.add_handler(ChatMemberHandler(monitor_member_update, ChatMemberHandler.CHAT_MEMBER))
    app.add_error_handler(error_handler)
    async with app:
        await app.start()
        await app.updater.start_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
        _sync_bot_ref["bot"] = app.bot
        maintenance_task = asyncio.create_task(_maintenance_loop())
        admin_sync_task = asyncio.create_task(_admin_sync_loop())
        session_monitor_task = asyncio.create_task(_session_monitor_loop())
        miniapp_runner = None
        if MINIAPP_ENABLED and MINIAPP_OK:
            try:
                _turl = ""
                if TUNNEL_OK and not MINIAPP_URL:
                    _turl = tunnel.start(port=MINIAPP_PORT)
                    if _turl:
                        globals()["MINIAPP_URL"] = _turl
                        logger.info(f"Mini App public URL: {_turl}")
                        # حدّث زر القائمة تلقائيا بالرابط الحالي (يعمل حتى لو
                        # تغيّر الرابط بين التشغيلات - المستخدم لا يلمس شيئا).
                        try:
                            from telegram import MenuButtonWebApp, WebAppInfo
                            await app.bot.set_chat_menu_button(
                                menu_button=MenuButtonWebApp(
                                    text="eye of hell", web_app=WebAppInfo(url=_turl)))
                            logger.info("تم تحديث زر القائمة بالرابط الحالي")
                        except Exception as e:
                            logger.warning(f"set menu button: {e}")
                if MINIAPP_URL and not _turl:
                    try:
                        from telegram import MenuButtonWebApp, WebAppInfo
                        await app.bot.set_chat_menu_button(
                            menu_button=MenuButtonWebApp(
                                text="eye of hell", web_app=WebAppInfo(url=MINIAPP_URL)))
                        logger.info(f"Mini App URL: {MINIAPP_URL}")
                    except Exception as e:
                        logger.warning(f"set menu button: {e}")
                async def _miniapp_admins(cid):
                    return await app.bot.get_chat_administrators(cid)
                async def _miniapp_owner_name():
                    ch = await app.bot.get_chat(OWNER_ID)
                    return (getattr(ch, "full_name", None)
                            or getattr(ch, "first_name", None)
                            or getattr(ch, "username", None) or "eye of hell")
                async def _miniapp_member_count(cid):
                    return await app.bot.get_chat_member_count(cid)
                miniapp.configure(
                    bot_token=BOT_TOKEN, db_path=DB_PATH, owner_id=OWNER_ID,
                    extras=_get_admin_extras(),
                    assistant_provider=get_assistant_client,
                    bot_admins_provider=_miniapp_admins,
                    owner_name_provider=_miniapp_owner_name,
                    member_count_provider=_miniapp_member_count)
                miniapp_runner = await miniapp.start(port=MINIAPP_PORT)
                await miniapp.start_archiver()
                # اجعل المساعد يستطيع قراءة آخر ظهور المشرفين (قاعدة التبادل)
                asyncio.create_task(_safe_task(
                    ensure_assistant_can_read_last_seen(), "assistant-lastseen"))
                # ارسل الرابط للمالك *وكل المالكين الفرعيين* مع زر فتح مباشر.
                # مهم: رابط Cloudflare يتغيّر كل تشغيل، فلو لم يصل الفرعيين
                # يفتحون رابطا قديما ميتا («لا يعمل»). لذا نرسله لهم جميعا.
                try:
                    from telegram import (WebAppInfo, InlineKeyboardButton,
                                          InlineKeyboardMarkup)
                    targets = {OWNER_ID} | set(DB.get_all_sub_owner_ids())
                    for _uid in targets:
                        try:
                            if MINIAPP_URL:
                                await app.bot.send_message(
                                    _uid,
                                    "لوحة <b>eye of hell</b> جاهزة.\n\n"
                                    "اضغط الزر ادناه لفتحها، وهي ايضا زر بجانب مربّع الكتابة.",
                                    parse_mode=ParseMode.HTML,
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                                        "فتح اللوحة", web_app=WebAppInfo(url=MINIAPP_URL))]]))
                            elif _uid == OWNER_ID:
                                await app.bot.send_message(
                                    _uid,
                                    "⚠️ الموقع يعمل محليا فقط ولم يُنشأ رابط عام.\n"
                                    "تأكد ان cloudflared.exe موجود بجانب البوت ثم اعد التشغيل.")
                        except Exception:
                            pass   # الفرعي الذي لم يبدأ محادثة البوت لا يمكن ارساله له
                except Exception as e:
                    logger.warning(f"send miniapp link: {e}")
            except Exception as e:
                logger.warning(f"miniapp start error: {e}")
        logger.info("Bot running")
        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            maintenance_task.cancel()
            admin_sync_task.cancel()
            session_monitor_task.cancel()
            for t in (maintenance_task, admin_sync_task, session_monitor_task):
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            try:
                if MINIAPP_OK and miniapp_runner is not None:
                    await miniapp.stop(miniapp_runner)
            except Exception:
                pass
            try:
                if TUNNEL_OK:
                    tunnel.stop()
            except Exception:
                pass
            try:
                await reset_assistant_client()
            except Exception:
                pass
            try:
                if admin_extras_mgr is not None:
                    await admin_extras_mgr.stop_all()
            except Exception:
                pass
            try:
                if app.updater.running:
                    await app.updater.stop()
            except Exception as e:
                logger.warning(f"updater stop error: {e}")
            try:
                await app.stop()
            except Exception as e:
                logger.warning(f"app stop error: {e}")


_SINGLE_INSTANCE_SOCK = None


def _acquire_single_instance():
    """يمنع تشغيل نسختين بنفس التوكن (سبب خطأ 409 Conflict).
    يربط مقبسا محليا على منفذ مشتق من التوكن؛ النسخه الثانيه تفشل بالربط
    فتخرج برساله واضحه بدل الانهيار الغامض. المقبس يُحرَّر تلقائيا عند
    انتهاء العمليه."""
    global _SINGLE_INSTANCE_SOCK
    import socket
    port = 20000 + (abs(hash(BOT_TOKEN)) % 20000)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # على ويندوز، عدم استعمال SO_REUSEADDR كي يفشل الربط لو كان محجوزا
        sock.bind(("127.0.0.1", port))
        sock.listen(1)
    except OSError:
        return False
    _SINGLE_INSTANCE_SOCK = sock
    return True


def main():
    if not _acquire_single_instance():
        logger.error(
            "نسخة اخرى من البوت تعمل بالفعل بنفس التوكن. "
            "اغلقها اولا (نسخة واحدة فقط مسموحة) ثم اعد التشغيل.")
        raise SystemExit(1)
    if not BOT_TOKEN or not OWNER_ID:
        logger.error("BOT_TOKEN و OWNER_ID مطلوبة في متغيرات البيئة (Railway Variables).")
        raise SystemExit(1)
    _here = os.path.dirname(os.path.abspath(__file__))
    _sdir = os.path.join(_here, "seed")
    if not os.path.isdir(_sdir) or not any(f.startswith("channel_manager.db.gz.part") for f in os.listdir(_sdir)):
        _sdir = _here   # القطع مرفوعة بجانب bot.py مباشرة
    _parts = sorted(f for f in os.listdir(_sdir)
                    if f.startswith("channel_manager.db.gz.part"))
    if not os.path.exists(DB_PATH) and _parts:
        import gzip, io, shutil
        os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
        _buf = io.BytesIO()
        for _f in _parts:
            with open(os.path.join(_sdir, _f), "rb") as _pf:
                shutil.copyfileobj(_pf, _buf)
        _buf.seek(0)
        with gzip.GzipFile(fileobj=_buf) as fi, open(DB_PATH + ".tmp", "wb") as fo:
            shutil.copyfileobj(fi, fo)
        os.replace(DB_PATH + ".tmp", DB_PATH)
        logger.info(f"تم استرجاع قاعدة البيانات الاولية الى {DB_PATH}")
    init_db()
    logger.info(f"DB ready: {DB_PATH}")
    asyncio.run(_run())


if __name__ == "__main__":
    main()
