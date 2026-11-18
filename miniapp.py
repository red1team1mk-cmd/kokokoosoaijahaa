# -*- coding: utf-8 -*-
"""
miniapp.py  --  Telegram Mini App لبوت حماية القنوات (eye of hell)
================================================================
يعمل *داخل* عملية البوت على نفس حلقة asyncio، فيشارك عملاء Telethon انفسهم
(عميل واحد لكل مشرف) عبر AdminExtras. هذا يمنع تعارض الجلسات نهائيا.

المرحلة الاولى: واجهة احمر/اسود + مصادقة initData + قائمة القنوات (حسب المالك
الفرعي) + قائمة المشرفين بالاسم والصورة.

المرحلة الثانية: ارشفة الرسائل الخاصة 24/7 لكل مشرف، كشف الحذف (X حمراء
للمحادثة المحذوفة و ✕ صغيرة للرسالة المحذوفة)، تحديث مباشر عبر SSE، مع
استثناء المجموعات >100 عضو والفيديو >=250MB.

التكامل من bot.py (داخل _run):
    miniapp.configure(bot_token=BOT_TOKEN, db_path=DB_PATH, owner_id=OWNER_ID,
                      extras=_get_admin_extras(),
                      assistant_provider=get_assistant_client)
    runner = await miniapp.start(port=MINIAPP_PORT)
    await miniapp.start_archiver()          # المرحلة الثانية
    ...
    await miniapp.stop(runner)
"""

import asyncio
import hashlib
import hmac
import html
import json
import logging
import os
import sqlite3
import threading
import time
from collections import defaultdict
from urllib.parse import parse_qsl

logger = logging.getLogger("miniapp")

try:
    from aiohttp import web
    AIOHTTP_OK = True
except ImportError:
    AIOHTTP_OK = False

# --- استثناءات الارشفة ---
GROUP_MEMBER_LIMIT = 100               # لا نأرشف مجموعات اكبر من هذا
VIDEO_MAX_BYTES = 250 * 1024 * 1024    # فيديو بهذا الحجم فاكثر لا يُحفظ
VIEW_VIDEO_MAX_BYTES = 80 * 1024 * 1024   # حدّ تشغيل الفيديو داخل الموقع مباشرة
# حذف الفيديو الضخم من المحادثة فعليا (اجراء لا رجعة فيه على حساب المشرف).
# افتراضيا لا نحذف رسائل حقيقية من محادثات الاخرين - نكتفي بعدم حفظها.
ARCHIVER_DELETE_BIG_VIDEOS = False

_cfg = {
    "bot_token": None, "db_path": None, "owner_id": None,
    "extras": None, "assistant_provider": None, "cache_dir": None,
    "bot_admins_provider": None, "owner_name_provider": None,
    "member_count_provider": None,
}


def configure(bot_token, db_path, owner_id, extras=None, assistant_provider=None,
              cache_dir=None, bot_admins_provider=None,
              owner_name_provider=None, member_count_provider=None):
    _cfg["bot_token"] = bot_token
    _cfg["db_path"] = db_path
    _cfg["owner_id"] = int(owner_id)
    _cfg["extras"] = extras
    _cfg["assistant_provider"] = assistant_provider
    _cfg["bot_admins_provider"] = bot_admins_provider
    _cfg["owner_name_provider"] = owner_name_provider
    _cfg["member_count_provider"] = member_count_provider
    _cfg["cache_dir"] = cache_dir or os.path.join(
        os.path.dirname(os.path.abspath(db_path or ".")), ".miniapp_cache")
    try:
        os.makedirs(_cfg["cache_dir"], exist_ok=True)
    except Exception:
        pass


# =========================================================================
#  قاعدة البيانات (ارشيف المحادثات المحذوفة)
# =========================================================================

def init_miniapp_db(db_path):
    with sqlite3.connect(db_path) as c:
        c.execute("PRAGMA journal_mode=WAL")
        # ترحيل لمرة واحدة: لو وُجد مخطط قديم (بلا peer_name/out) نُسقطه ونعيد
        # بناءه. الجداول ارشيفيه لا مراجع خارجيه لها.
        for tbl, col in (("miniapp_dialogs", "peer_name"), ("miniapp_messages", "out")):
            try:
                cols = [r[1] for r in c.execute(f"PRAGMA table_info({tbl})")]
                if cols and col not in cols:
                    c.execute(f"DROP TABLE {tbl}")
            except sqlite3.OperationalError:
                pass
        c.execute("""CREATE TABLE IF NOT EXISTS miniapp_dialogs (
            admin_uid    INTEGER,
            peer_id      INTEGER,
            peer_type    TEXT,
            peer_name    TEXT,
            peer_username TEXT,
            last_text    TEXT,
            last_ts      INTEGER DEFAULT 0,
            deleted      INTEGER DEFAULT 0,
            del_count    INTEGER DEFAULT 0,
            PRIMARY KEY (admin_uid, peer_id))""")
        c.execute("""CREATE TABLE IF NOT EXISTS miniapp_messages (
            admin_uid    INTEGER,
            peer_id      INTEGER,
            message_id   INTEGER,
            out          INTEGER DEFAULT 0,
            text         TEXT,
            media_kind   TEXT,
            ts           INTEGER,
            deleted      INTEGER DEFAULT 0,
            PRIMARY KEY (admin_uid, peer_id, message_id))""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_mm ON miniapp_messages(admin_uid, peer_id, ts)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_mm_mid ON miniapp_messages(admin_uid, message_id)")
        c.execute("""CREATE TABLE IF NOT EXISTS channel_counters (
            channel_id INTEGER PRIMARY KEY,
            edits_deleted INTEGER DEFAULT 0)""")
        c.commit()


def _db():
    con = sqlite3.connect(_cfg["db_path"])
    con.row_factory = sqlite3.Row
    return con


def bump_edit_deleted(cid, n=1):
    """يزيد عدّاد التعديلات الممسوحة (يُنادى من bot.py عند حذف تعديل)."""
    try:
        with _db() as con:
            con.execute(
                "INSERT INTO channel_counters (channel_id,edits_deleted) VALUES (?,?) "
                "ON CONFLICT(channel_id) DO UPDATE SET edits_deleted=edits_deleted+?",
                (cid, n, n))
            con.commit()
    except Exception:
        pass


def get_stats(uid):
    """احصائيات البوت الخاصة بقنوات هذا المستخدم."""
    chans = get_user_channels(uid)
    cids = [c["channel_id"] for c in chans]
    viol = edits = 0
    with _db() as con:
        if cids:
            qs = ",".join("?" for _ in cids)
            try:
                viol = con.execute(f"SELECT COUNT(*) FROM spam_log WHERE channel_id IN ({qs})",
                                   cids).fetchone()[0]
            except Exception:
                viol = 0
            try:
                r = con.execute(f"SELECT COALESCE(SUM(edits_deleted),0) FROM channel_counters "
                                f"WHERE channel_id IN ({qs})", cids).fetchone()
                edits = r[0] if r else 0
            except Exception:
                edits = 0
        try:
            archived = con.execute("SELECT COUNT(*) FROM miniapp_messages").fetchone()[0]
        except Exception:
            archived = 0
    return {"channels": len(cids), "violations": viol,
            "edits_deleted": edits, "archived": archived}


def get_channel_stats(cid):
    """احصائيات خاصة بقناة واحدة (تظهر داخل القناة عند الضغط)."""
    viol = edits = 0
    with _db() as con:
        try:
            viol = con.execute("SELECT COUNT(*) FROM spam_log WHERE channel_id=?",
                               (cid,)).fetchone()[0]
        except Exception:
            viol = 0
        try:
            r = con.execute("SELECT COALESCE(edits_deleted,0) FROM channel_counters "
                            "WHERE channel_id=?", (cid,)).fetchone()
            edits = r[0] if r else 0
        except Exception:
            edits = 0
    return {"violations": viol, "edits_deleted": edits}


# =========================================================================
#  التحقق من initData
# =========================================================================

def verify_init_data(init_data, bot_token, max_age=86400):
    if not init_data or not bot_token:
        return None
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    except Exception:
        return None
    recv_hash = pairs.pop("hash", None)
    if not recv_hash:
        return None
    dcs = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
    secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    calc = hmac.new(secret, dcs.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, recv_hash):
        return None
    if max_age:
        try:
            if time.time() - int(pairs.get("auth_date", "0")) > max_age:
                return None
        except (TypeError, ValueError):
            return None
    user = {}
    if "user" in pairs:
        try:
            user = json.loads(pairs["user"])
        except Exception:
            user = {}
    if not user.get("id"):
        return None
    return {"id": int(user["id"]),
            "name": ((user.get("first_name") or "") + " " + (user.get("last_name") or "")).strip()
                    or (user.get("username") or str(user["id"])),
            "username": user.get("username")}


def _viewer_from_init(init_data):
    viewer = verify_init_data(init_data, _cfg["bot_token"])
    if viewer:
        return viewer
    dev = os.environ.get("MINIAPP_DEV_USER")
    if dev:
        try:
            return {"id": int(dev), "name": "DEV", "username": None}
        except ValueError:
            pass
    return None


def _viewer_from_request(request):
    init_data = (request.headers.get("X-Telegram-Init-Data")
                 or request.headers.get("Authorization", "").replace("tma ", "").strip())
    return _viewer_from_init(init_data)


# =========================================================================
#  استعلامات القنوات/الصلاحيات/الجلسات
# =========================================================================

def get_user_channels(uid):
    with _db() as con:
        if uid == _cfg["owner_id"]:
            rows = con.execute(
                "SELECT channel_id, channel_name, channel_user FROM channels "
                "ORDER BY added_at DESC").fetchall()
        else:
            rows = con.execute(
                "SELECT ch.channel_id, ch.channel_name, ch.channel_user "
                "FROM channels ch JOIN sub_owners so ON ch.channel_id=so.channel_id "
                "WHERE so.user_id=? ORDER BY ch.added_at DESC", (uid,)).fetchall()
    return [dict(r) for r in rows]


def is_authorized_for(uid, cid):
    if uid == _cfg["owner_id"]:
        return True
    with _db() as con:
        r = con.execute("SELECT 1 FROM sub_owners WHERE user_id=? AND channel_id=?",
                        (uid, cid)).fetchone()
    return r is not None


def _has_session(uid):
    with _db() as con:
        return con.execute("SELECT 1 FROM admin_sessions WHERE user_id=?", (uid,)).fetchone() is not None


def _all_session_uids():
    with _db() as con:
        return [r[0] for r in con.execute(
            "SELECT DISTINCT user_id FROM admin_sessions").fetchall()]


def _assistant_admin_uid(cid):
    with _db() as con:
        r = con.execute("SELECT user_id FROM assistant_admins WHERE channel_id=?", (cid,)).fetchone()
    return r[0] if r else None


# =========================================================================
#  الحساب المساعد: مشرفون + صور
# =========================================================================

async def _assistant():
    prov = _cfg["assistant_provider"]
    if prov is None:
        return None
    try:
        client, _err = await prov()
        return client
    except Exception:
        return None


_admins_cache = {}   # cid -> (ts, [admins])


async def list_channel_admins(cid):
    """قائمة مشرفي القناة. نفضّل واجهة البوت (get_chat_administrators) لانها
    اضمن - البوت مشرف اصلا في القناة - ونرجع للمساعد عند غيابها. النتيجة
    تُخبّأ 120ث كي تعمل الصفحة فورا حتى لو تعثّرت الشبكة لحظيا."""
    asst_uid = _assistant_admin_uid(cid)
    out = []
    prov = _cfg.get("bot_admins_provider")
    if prov is not None:
        try:
            admins = await prov(cid)
            for a in admins:
                u = getattr(a, "user", None)
                if u is None or getattr(u, "is_bot", False) or u.id == asst_uid:
                    continue
                name = (getattr(u, "full_name", None)
                        or ((u.first_name or "") + " " + (u.last_name or "")).strip()
                        or (u.username or str(u.id)))
                out.append({"id": u.id, "name": name, "username": u.username,
                            "has_photo": True, "has_session": _has_session(u.id)})
            _admins_cache[cid] = (time.time(), out)
            return out
        except Exception as e:
            logger.warning("bot admins cid=%s: %s", cid, e)
    # احتياط: عبر الحساب المساعد
    client = await _assistant()
    if client is None:
        cached = _admins_cache.get(cid)
        return cached[1] if cached else []
    try:
        from pyrogram.enums import ChatMembersFilter
        async for m in client.get_chat_members(cid, filter=ChatMembersFilter.ADMINISTRATORS):
            u = getattr(m, "user", None)
            if u is None or getattr(u, "is_bot", False) or u.id == asst_uid:
                continue
            name = ((u.first_name or "") + " " + (u.last_name or "")).strip() or (u.username or str(u.id))
            out.append({"id": u.id, "name": name, "username": u.username,
                        "has_photo": bool(getattr(u, "photo", None)),
                        "has_session": _has_session(u.id)})
        _admins_cache[cid] = (time.time(), out)
    except Exception as e:
        logger.warning("list_channel_admins cid=%s: %s", cid, e)
        cached = _admins_cache.get(cid)
        if cached:
            return cached[1]
    return out


def _fresh(path):
    return (os.path.exists(path) and os.path.getsize(path) > 0
            and (time.time() - os.path.getmtime(path) < 6 * 3600))


async def _tg_download_photo(entity, path, prefer_uid=None):
    """ينزّل صورة ملف (مستخدم/قناة/مجموعة) عبر جلسات المشرفين (telethon):
    يجرّب الجلسة المفضّلة اولا (مثلا صاحب الحساب نفسه)، ثم بقيّة الجلسات حتى
    تنجح واحدة تعرف الكيان (مشرف يشارك القناة يستطيع تنزيل صورتها/صور زملائه)."""
    extras = _cfg["extras"]
    if extras is None:
        return False
    order = []
    if prefer_uid is not None:
        order.append(prefer_uid)
    try:
        order += [u for u in extras.db.all_ids() if u not in order]
    except Exception:
        pass
    tried = 0
    for u in order:
        if tried >= 4:      # لا نُثقل الشبكة بمحاولات كثيرة (استقرار على شبكة ضعيفة)
            break
        try:
            client = await asyncio.wait_for(extras.get_client(u), timeout=6)
        except Exception:
            client = None
        if client is None:
            continue
        tried += 1
        target = "me" if (u == prefer_uid and u == entity) else entity
        try:
            got = await asyncio.wait_for(
                client.download_profile_photo(target, file=path), timeout=8)
            if got and os.path.exists(path) and os.path.getsize(path) > 0:
                return True
        except Exception:
            continue
    return False


async def _assistant_download_chat_photo(entity, path):
    """احتياط للقنوات/المجموعات عبر الحساب المساعد (pyrogram): get_chat ثم file_id."""
    client = await _assistant()
    if client is None:
        return False
    try:
        chat = await client.get_chat(entity)
        ph = getattr(chat, "photo", None)
        fid = (getattr(ph, "big_file_id", None) or getattr(ph, "small_file_id", None)) if ph else None
        if fid:
            got = await client.download_media(fid, file_name=path)
            if got and os.path.exists(path) and os.path.getsize(path) > 0:
                return True
    except Exception:
        pass
    return False


async def _download_photo(kind, ident):
    """صورة قناة/مستخدم للموقع: جلسات المشرفين اولا (اضمن)، ثم الحساب المساعد."""
    cache = _cfg["cache_dir"]
    path = os.path.join(cache, f"{kind}_{ident}.jpg")
    if _fresh(path):
        return path if os.path.getsize(path) > 0 else None
    if await _tg_download_photo(ident, path):
        return path
    if await _assistant_download_chat_photo(ident, path):
        return path
    return path if (os.path.exists(path) and os.path.getsize(path) > 0) else None


LOGO_URL = "https://files.catbox.moe/27loox.jpeg"
_logo_lock = threading.Lock()


def _logo_path():
    return os.path.join(_cfg["cache_dir"] or ".", "logo.jpg")


def _download_logo_blocking():
    """تنزيل الشعار — يُشغَّل في خيط منفصل فقط (لا يُستدعى ابدا داخل حلقة asyncio،
    والا عطّل الموقع كله اذا كان المصدر بطيئا/محظورا)."""
    if not _logo_lock.acquire(blocking=False):
        return  # تنزيل جارٍ في خيط آخر
    try:
        path = _logo_path()
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return
        import urllib.request
        req = urllib.request.Request(LOGO_URL, headers={"User-Agent": "eye-of-hell"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = r.read()
        if data:
            tmp = path + ".part"
            with open(tmp, "wb") as f:
                f.write(data)
            os.replace(tmp, path)   # كتابة ذرّية
    except Exception as e:
        logger.warning("logo download: %s", e)
    finally:
        _logo_lock.release()


def _ensure_logo():
    """يطلق تنزيل الشعار في خيط خلفي — لا يعطّل الحلقة اطلاقا."""
    path = _logo_path()
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    threading.Thread(target=_download_logo_blocking, daemon=True).start()


def _letter_avatar_svg(text):
    ch = (text or "?").strip()[:1].upper() or "?"
    palette = ["#e11d2a", "#b81420", "#7a0d16", "#3a3a3a", "#c0392b"]
    color = palette[(ord(ch) if ch else 0) % len(palette)]
    safe = html.escape(ch)
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="120" height="120">'
            f'<rect width="120" height="120" rx="60" fill="{color}"/>'
            f'<text x="50%" y="50%" dy=".35em" text-anchor="middle" '
            f'font-family="Segoe UI,Arial,sans-serif" font-size="56" fill="#0a0a0a" '
            f'font-weight="700">{safe}</text></svg>').encode("utf-8")


# =========================================================================
#  ارشيف الرسائل: قراءة/كتابة
# =========================================================================

def _upsert_dialog(uid, pid, info, last_text, ts):
    with _db() as con:
        con.execute(
            "INSERT INTO miniapp_dialogs (admin_uid,peer_id,peer_type,peer_name,peer_username,last_text,last_ts) "
            "VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(admin_uid,peer_id) DO UPDATE SET "
            "peer_type=excluded.peer_type, peer_name=excluded.peer_name, "
            "peer_username=excluded.peer_username, last_text=excluded.last_text, "
            "last_ts=excluded.last_ts",
            (uid, pid, info["type"], info["name"], info.get("username"),
             last_text[:400] if last_text else "", ts))
        con.commit()


def _upsert_message(uid, pid, mid, out, text, media_kind, ts):
    with _db() as con:
        con.execute(
            "INSERT OR REPLACE INTO miniapp_messages "
            "(admin_uid,peer_id,message_id,out,text,media_kind,ts,deleted) "
            "VALUES (?,?,?,?,?,?,?,COALESCE((SELECT deleted FROM miniapp_messages "
            "WHERE admin_uid=? AND peer_id=? AND message_id=?),0))",
            (uid, pid, mid, out, text, media_kind, ts, uid, pid, mid))
        con.commit()


def _mark_deleted(uid, ids, pid=None):
    """يعلّم رسائل محذوفة؛ يرجع [(peer_id, message_id), ...] المتأثرة."""
    if not ids:
        return []
    qs = ",".join("?" for _ in ids)
    with _db() as con:
        if pid is not None:
            rows = con.execute(
                f"SELECT peer_id,message_id FROM miniapp_messages "
                f"WHERE admin_uid=? AND peer_id=? AND message_id IN ({qs}) AND deleted=0",
                (uid, pid, *ids)).fetchall()
        else:
            rows = con.execute(
                f"SELECT peer_id,message_id FROM miniapp_messages "
                f"WHERE admin_uid=? AND message_id IN ({qs}) AND deleted=0",
                (uid, *ids)).fetchall()
        affected = [(r[0], r[1]) for r in rows]
        for peer, mid in affected:
            con.execute("UPDATE miniapp_messages SET deleted=1 "
                        "WHERE admin_uid=? AND peer_id=? AND message_id=?", (uid, peer, mid))
        for peer in set(p for p, _ in affected):
            tot = con.execute("SELECT COUNT(*) FROM miniapp_messages WHERE admin_uid=? AND peer_id=?",
                              (uid, peer)).fetchone()[0]
            deld = con.execute("SELECT COUNT(*) FROM miniapp_messages WHERE admin_uid=? AND peer_id=? AND deleted=1",
                               (uid, peer)).fetchone()[0]
            con.execute("UPDATE miniapp_dialogs SET del_count=?, deleted=? WHERE admin_uid=? AND peer_id=?",
                        (deld, 1 if (tot > 0 and deld >= tot) else 0, uid, peer))
        con.commit()
    return affected


def get_archived_dialogs(uid):
    with _db() as con:
        rows = con.execute(
            "SELECT peer_id,peer_type,peer_name,peer_username,last_text,last_ts,deleted,del_count "
            "FROM miniapp_dialogs WHERE admin_uid=? ORDER BY last_ts DESC", (uid,)).fetchall()
    return [dict(r) for r in rows]


def _archived_messages_map(uid, pid, limit=160):
    with _db() as con:
        rows = con.execute(
            "SELECT message_id,out,text,media_kind,ts,deleted FROM miniapp_messages "
            "WHERE admin_uid=? AND peer_id=? "
            "ORDER BY ts DESC, message_id DESC LIMIT ?", (uid, pid, limit)).fetchall()
    return {r["message_id"]: dict(r) for r in rows}


async def get_dialog_messages_live(uid, pid, limit=80):
    """يدمج الرسائل الحيّة (من حساب المشرف عبر العميل المشترك) مع المؤرشفة،
    فتظهر المحادثة كاملة مثل server.py + الرسائل المحذوفة تُستعاد من الارشيف
    بعلامة X."""
    archived = _archived_messages_map(uid, pid)
    out = {}
    extras = _cfg["extras"]
    client = await extras.get_client(uid) if extras is not None else None
    if client is not None:
        try:
            msgs = await client.get_messages(pid, limit=limit)
            for m in msgs:
                kind, _sz = _media_of(m)
                out[m.id] = {
                    "id": m.id,
                    "out": 1 if getattr(m, "out", False) else 0,
                    "text": (getattr(m, "message", None) or ""),
                    "media": kind,
                    "has_media": bool(getattr(m, "media", None)),
                    "ts": int(m.date.timestamp()) if getattr(m, "date", None) else 0,
                    "deleted": 0,
                }
        except Exception as e:
            logger.warning("live messages uid=%s pid=%s: %s", uid, pid, e)
    for mid, a in archived.items():
        if mid in out:
            if a["deleted"]:
                out[mid]["deleted"] = 1
        else:
            out[mid] = {"id": mid, "out": a["out"], "text": a["text"] or "",
                        "media": a["media_kind"], "has_media": bool(a["media_kind"]),
                        "ts": a["ts"], "deleted": a["deleted"]}
    res = list(out.values())
    res.sort(key=lambda x: (x["ts"] or 0, x["id"]))
    if len(res) > limit * 2:
        res = res[-limit * 2:]
    return res


async def download_message_media(uid, pid, mid):
    """يحمّل صورة/مصغّر رسالة عبر عميل المشرف؛ يرجع مسار jpg او None."""
    cache = _cfg["cache_dir"]
    path = os.path.join(cache, f"media_{uid}_{pid}_{mid}.jpg")
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path
    extras = _cfg["extras"]
    client = await extras.get_client(uid) if extras is not None else None
    if client is None:
        return None
    try:
        msg = await client.get_messages(pid, ids=mid)
    except Exception:
        return None
    if not msg or not getattr(msg, "media", None):
        return None
    kind, _sz = _media_of(msg)
    try:
        if kind == "photo":
            got = await asyncio.wait_for(client.download_media(msg, file=path), timeout=25)
        else:
            # مصغّر فقط للفيديو/الملصق/المستند (نتفادى تحميل ملفات ضخمة)
            got = await asyncio.wait_for(client.download_media(msg, thumb=-1, file=path), timeout=20)
        if got and os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    except Exception as e:
        logger.warning("download media uid=%s pid=%s mid=%s: %s", uid, pid, mid, e)
    return None


async def download_message_video(uid, pid, mid):
    """يحمّل الفيديو/GIF الفعلي (بحدّ حجم) لتشغيله داخل الموقع؛ يرجع مسار الملف او None."""
    cache = _cfg["cache_dir"]
    path = os.path.join(cache, f"vid_{uid}_{pid}_{mid}.mp4")
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path
    extras = _cfg["extras"]
    client = await extras.get_client(uid) if extras is not None else None
    if client is None:
        return None
    try:
        msg = await client.get_messages(pid, ids=mid)
    except Exception:
        return None
    if not msg or not getattr(msg, "media", None):
        return None
    kind, size = _media_of(msg)
    if kind not in ("video", "gif"):
        return None
    if size and size > VIEW_VIDEO_MAX_BYTES:
        return None    # كبير جدا للتشغيل المباشر (يبقى المصغّر فقط)
    try:
        got = await asyncio.wait_for(client.download_media(msg, file=path), timeout=120)
        if got and os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    except Exception as e:
        logger.warning("download video uid=%s pid=%s mid=%s: %s", uid, pid, mid, e)
    return None


async def download_peer_avatar(uid, pid):
    """صورة الطرف الآخر عبر عميل المشرف، ثم اي جلسة اخرى تعرفه."""
    cache = _cfg["cache_dir"]
    path = os.path.join(cache, f"peer_{uid}_{pid}.jpg")
    if _fresh(path):
        return path if os.path.getsize(path) > 0 else None
    if await _tg_download_photo(pid, path, prefer_uid=uid):
        return path
    return None


# =========================================================================
#  SSE: اشتراكات التحديث المباشر
# =========================================================================

_subs = defaultdict(set)   # admin_uid -> {asyncio.Queue,...}


def _push(uid, event):
    for q in list(_subs.get(uid, ())):
        try:
            q.put_nowait(event)
        except Exception:
            pass


# =========================================================================
#  المؤرشِف: يستمع 24/7 لحسابات المشرفين عبر عملاء AdminExtras المشتركين
# =========================================================================

def _media_of(m):
    if not getattr(m, "media", None):
        return None, 0
    size = 0
    try:
        size = (m.file.size if m.file else 0) or 0
    except Exception:
        size = 0
    for attr, kind in (("photo", "photo"), ("video", "video"), ("voice", "voice"),
                       ("audio", "audio"), ("sticker", "sticker"), ("gif", "gif"),
                       ("document", "document")):
        if getattr(m, attr, None):
            return kind, size
    return "media", size


class Archiver:
    def __init__(self, extras):
        self.extras = extras
        self._attached = {}          # uid -> (client, h_new, h_del)
        self._peer_cache = {}        # (uid,pid) -> info dict
        self._sync_task = None
        self._stopped = False

    async def start(self):
        if self.extras is None:
            logger.warning("archiver: no extras (admin clients) - disabled")
            return
        await self._sync_once()
        self._sync_task = asyncio.create_task(self._sync_loop())
        logger.info("archiver started (%d admin sessions)", len(self._attached))

    async def stop(self):
        self._stopped = True
        if self._sync_task:
            self._sync_task.cancel()
        for uid in list(self._attached):
            await self._detach(uid)

    async def _sync_loop(self):
        while not self._stopped:
            try:
                await asyncio.sleep(180)
                await self._sync_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("archiver sync loop: %s", e)

    async def _sync_once(self):
        want = set(_all_session_uids())
        for uid in want - set(self._attached):
            await self._attach(uid)
        for uid in set(self._attached) - want:
            await self._detach(uid)

    async def _attach(self, uid):
        try:
            client = await self.extras.get_client(uid)
        except Exception as e:
            logger.warning("archiver get_client uid=%s: %s", uid, e)
            client = None
        if client is None or uid in self._attached:
            return
        from telethon import events

        async def _on_new(event, _uid=uid):
            await self._handle_new(_uid, event)

        async def _on_del(event, _uid=uid):
            await self._handle_del(_uid, event)

        client.add_event_handler(_on_new, events.NewMessage())
        client.add_event_handler(_on_del, events.MessageDeleted())
        # ملاحظة: كشف «تسجيل دخول جديد» يتم عبر account.getAuthorizations في
        # مراقب الجلسات (اشعار واحد كامل بالجهاز والموقع)، لا عبر تحديث خام هنا.
        self._attached[uid] = (client, _on_new, _on_del)
        logger.info("archiver attached uid=%s", uid)

    async def _detach(self, uid):
        info = self._attached.pop(uid, None)
        if not info:
            return
        client, h_new, h_del = info[0], info[1], info[2]
        try:
            client.remove_event_handler(h_new)
            client.remove_event_handler(h_del)
        except Exception:
            pass

    async def _peer_info(self, uid, event):
        pid = event.chat_id
        key = (uid, pid)
        cached = self._peer_cache.get(key)
        if cached and time.time() - cached["t"] < 300:
            return cached
        info = {"type": "user", "name": str(pid), "username": None,
                "archivable": True, "t": time.time()}
        try:
            from telethon.utils import get_display_name
            chat = await event.get_chat()
            info["name"] = get_display_name(chat) or str(pid)
            info["username"] = getattr(chat, "username", None)
            if event.is_private:
                info["type"] = "user"; info["archivable"] = True
            elif getattr(event, "is_channel", False) and not event.is_group:
                info["type"] = "channel"; info["archivable"] = False   # قناة بث - تجاهل
            else:
                info["type"] = "group"
                cnt = getattr(chat, "participants_count", None)
                info["archivable"] = (cnt is None) or (cnt <= GROUP_MEMBER_LIMIT)
        except Exception:
            pass
        self._peer_cache[key] = info
        return info

    async def _handle_new(self, uid, event):
        try:
            m = event.message
            if getattr(event, "is_channel", False) and not event.is_group:
                return
            info = await self._peer_info(uid, event)
            if not info["archivable"]:
                return
            pid = event.chat_id
            media_kind, size = _media_of(m)
            if media_kind == "video" and size >= VIDEO_MAX_BYTES:
                if ARCHIVER_DELETE_BIG_VIDEOS:
                    try:
                        await event.client.delete_messages(pid, [m.id])
                    except Exception:
                        pass
                return   # لا نحفظ الفيديو الضخم
            text = m.message or (f"[{media_kind}]" if media_kind else "")
            out = 1 if m.out else 0
            ts = int(m.date.timestamp()) if m.date else int(time.time())
            _upsert_message(uid, pid, m.id, out, text, media_kind, ts)
            _upsert_dialog(uid, pid, info, text, ts)
            _push(uid, {"type": "new", "peer": pid, "peer_name": info["name"],
                        "peer_type": info["type"], "message_id": m.id, "out": out,
                        "text": text[:300], "media": media_kind, "ts": ts})
        except Exception as e:
            logger.warning("archiver new uid=%s: %s", uid, e)

    async def _handle_del(self, uid, event):
        try:
            ids = list(getattr(event, "deleted_ids", None) or [])
            pid = getattr(event, "chat_id", None)
            affected = _mark_deleted(uid, ids, pid)
            for peer, mid in affected:
                _push(uid, {"type": "deleted", "peer": peer, "message_id": mid})
            for peer in set(p for p, _ in affected):
                _push(uid, {"type": "dialog_update", "peer": peer})
        except Exception as e:
            logger.warning("archiver del uid=%s: %s", uid, e)


_archiver = None


async def start_archiver():
    global _archiver
    if _archiver is None and _cfg["extras"] is not None:
        _archiver = Archiver(_cfg["extras"])
        await _archiver.start()
    return _archiver


async def stop_archiver():
    global _archiver
    if _archiver is not None:
        await _archiver.stop()
        _archiver = None


async def attach_session(uid):
    """ربط فوري لحساب مشرف جديد بالمؤرشِف (يُنادى عند تسجيل الدخول)."""
    if _archiver is not None:
        try:
            await _archiver._attach(uid)
        except Exception as e:
            logger.warning("attach_session uid=%s: %s", uid, e)


async def detach_session(uid):
    """فك ربط فوري عند اخراج جلسة مشرف."""
    if _archiver is not None:
        try:
            await _archiver._detach(uid)
        except Exception:
            pass


# =========================================================================
#  دمج المحادثات الحيّة (اسماء/معاينة) مع الارشيف (المحذوفات)
# =========================================================================

async def get_admin_dialogs(uid):
    """كل محادثات حساب المشرف: خاص + مجموعات + قنوات (حتى التي ليس مشرفا فيها),
    مثل تقسيمات server.py. المحذوفات تُعلَّم من الارشيف."""
    archived = {d["peer_id"]: d for d in get_archived_dialogs(uid)}
    extras = _cfg["extras"]
    if extras is not None:
        try:
            client = await extras.get_client(uid)
        except Exception:
            client = None
        if client is not None:
            try:
                from telethon.utils import get_display_name
                async for d in client.iter_dialogs(limit=200):
                    ent = d.entity
                    pid = ent.id
                    if d.is_user:
                        kind = "bot" if getattr(ent, "bot", False) else "user"
                    elif getattr(ent, "broadcast", False) and not getattr(ent, "megagroup", False):
                        kind = "channel"
                    else:
                        kind = "group"
                    row = archived.get(pid, {"peer_id": pid, "deleted": 0, "del_count": 0})
                    row["peer_name"] = get_display_name(ent) or str(pid)
                    row["peer_username"] = getattr(ent, "username", None)
                    row["peer_type"] = kind
                    if not row.get("last_text"):
                        row["last_text"] = (d.message.message if d.message else "") or ""
                    row["last_ts"] = int(d.date.timestamp()) if d.date else row.get("last_ts", 0)
                    archived[pid] = row
            except Exception as e:
                logger.warning("live dialogs uid=%s: %s", uid, e)
    result = list(archived.values())
    result.sort(key=lambda r: r.get("last_ts", 0) or 0, reverse=True)
    return result


async def get_admin_contacts(uid):
    """جهات اتصال حساب المشرف (تظهر اسفل الموقع كما في server.py)."""
    extras = _cfg["extras"]
    if extras is None:
        return []
    try:
        client = await extras.get_client(uid)
    except Exception:
        client = None
    if client is None:
        return []
    try:
        from telethon.tl.functions.contacts import GetContactsRequest
        res = await client(GetContactsRequest(hash=0))
        out = []
        for u in getattr(res, "users", []) or []:
            if getattr(u, "is_self", False) or getattr(u, "bot", False):
                continue
            name = (((u.first_name or "") + " " + (u.last_name or "")).strip()
                    or u.username or str(u.id))
            out.append({"peer": u.id, "name": name, "username": u.username})
        out.sort(key=lambda x: x["name"])
        return out
    except Exception as e:
        logger.warning("contacts uid=%s: %s", uid, e)
        return []


async def download_own_avatar(uid):
    """صورة المشرف نفسه عبر جلسته، ثم عبر جلسة مشرف آخر يشاركه قناة
    (كي تظهر صور المشرفين حتى بلا جلسة خاصة بهم)."""
    cache = _cfg["cache_dir"]
    path = os.path.join(cache, f"self_{uid}.jpg")
    if _fresh(path):
        return path if os.path.getsize(path) > 0 else None
    if await _tg_download_photo(uid, path, prefer_uid=uid):
        return path
    return None


# =========================================================================
#  معالجات HTTP
# =========================================================================

async def _h_index(request):
    return web.Response(text=INDEX_HTML, content_type="text/html", charset="utf-8")


async def _h_health(request):
    return web.json_response({"ok": True})


_dev_name_cache = {"name": None, "ts": 0}


async def _dev_name():
    """اسم المطوّر = اسم المالك الحالي في تيليجرام (ثابت لكل المستخدمين, يتحدّث تلقائيا)."""
    prov = _cfg.get("owner_name_provider")
    if prov is None:
        return _dev_name_cache["name"]
    now = time.time()
    if _dev_name_cache["name"] and (now - _dev_name_cache["ts"] < 120):
        return _dev_name_cache["name"]
    try:
        nm = await prov()
        if nm:
            _dev_name_cache["name"] = nm
            _dev_name_cache["ts"] = now
    except Exception as e:
        logger.warning("dev name: %s", e)
    return _dev_name_cache["name"]


async def _member_count(cid):
    prov = _cfg.get("member_count_provider")
    if prov is None:
        return None
    try:
        return await prov(cid)
    except Exception:
        return None


async def _h_channels(request):
    viewer = _viewer_from_request(request)
    if not viewer:
        return web.json_response({"error": "unauthorized"}, status=401)
    channels = get_user_channels(viewer["id"])
    return web.json_response({
        "viewer": viewer,
        "is_owner": viewer["id"] == _cfg["owner_id"],
        "dev_name": await _dev_name(),
        "stats": get_stats(viewer["id"]),
        "channels": [{"id": c["channel_id"],
                      "name": c["channel_name"] or c["channel_user"] or str(c["channel_id"]),
                      "username": c["channel_user"],
                      "avatar": f"/avatar/channel/{c['channel_id']}"} for c in channels],
    })


def _auth_cid(request):
    viewer = _viewer_from_request(request)
    if not viewer:
        return None, None
    try:
        cid = int(request.match_info["cid"])
    except (KeyError, ValueError):
        return viewer, None
    if not is_authorized_for(viewer["id"], cid):
        return viewer, False
    return viewer, cid


async def _h_admins(request):
    viewer, cid = _auth_cid(request)
    if not viewer:
        return web.json_response({"error": "unauthorized"}, status=401)
    if cid is None:
        return web.json_response({"error": "bad_channel"}, status=400)
    if cid is False:
        return web.json_response({"error": "forbidden"}, status=403)
    # اجلب المشرفين وعدد الاعضاء بالتوازي مع مهلة، كي لا تتعطّل الصفحة على
    # شبكة بطيئة (كانت النداءات المتسلسلة تتجاوز مهلة المتصفّح -> «تعذر جلب المشرفين»).
    async def _get_admins():
        try:
            return await asyncio.wait_for(list_channel_admins(cid), timeout=8)
        except Exception as e:
            logger.warning("admins timeout/err cid=%s: %s", cid, e)
            cached = _admins_cache.get(cid)
            return cached[1] if cached else []

    async def _get_members():
        try:
            return await asyncio.wait_for(_member_count(cid), timeout=3)
        except Exception:
            return None

    admins, mc = await asyncio.gather(_get_admins(), _get_members())
    stats = get_channel_stats(cid)
    stats["admins"] = len(admins)
    if mc is not None:
        stats["members"] = mc
    return web.json_response({"channel_id": cid, "count": len(admins), "stats": stats,
        "admins": [{"id": a["id"], "name": a["name"], "username": a["username"],
                    "has_session": a["has_session"],
                    "avatar": f"/avatar/user/{a['id']}"} for a in admins]})


async def _h_dialogs(request):
    viewer, cid = _auth_cid(request)
    if not viewer or cid in (None, False):
        return web.json_response({"error": "forbidden"}, status=403 if viewer else 401)
    uid = int(request.match_info["uid"])
    if not _has_session(uid):
        return web.json_response({"error": "no_session", "dialogs": []})
    dialogs, contacts = await asyncio.gather(
        get_admin_dialogs(uid), get_admin_contacts(uid))
    return web.json_response({"admin": uid,
        "dialogs": [{
            "peer": d["peer_id"],
            "name": d.get("peer_name") or str(d["peer_id"]),
            "username": d.get("peer_username"),
            "type": d.get("peer_type") or "user",
            "preview": (d.get("last_text") or "")[:80],
            "ts": d.get("last_ts") or 0,
            "deleted": bool(d.get("deleted")),
            "del_count": d.get("del_count") or 0,
        } for d in dialogs],
        "contacts": [{"peer": c["peer"], "name": c["name"],
                      "username": c.get("username")} for c in contacts]})


async def _h_messages(request):
    viewer, cid = _auth_cid(request)
    if not viewer or cid in (None, False):
        return web.json_response({"error": "forbidden"}, status=403 if viewer else 401)
    uid = int(request.match_info["uid"])
    peer = int(request.match_info["peer"])
    msgs = await get_dialog_messages_live(uid, peer)
    base = f"/api/channel/{cid}/admin/{uid}/media/{peer}/"
    vbase = f"/api/channel/{cid}/admin/{uid}/vmedia/{peer}/"
    def murl(m):
        if m.get("has_media") and not m.get("deleted") and m.get("media") in (
                "photo", "video", "gif", "sticker"):
            return base + str(m["id"])
        return None
    def vurl(m):
        if m.get("has_media") and not m.get("deleted") and m.get("media") in ("video", "gif"):
            return vbase + str(m["id"])
        return None
    return web.json_response({"admin": uid, "peer": peer, "messages": [{
        "id": m["id"], "out": bool(m["out"]), "text": m["text"] or "",
        "media": m["media"], "has_media": bool(m.get("has_media")),
        "media_url": murl(m), "video_url": vurl(m), "ts": m["ts"],
        "deleted": bool(m["deleted"]),
    } for m in msgs]})


def _auth_query(request):
    """مصادقة لطلبات الصور/الوسائط (GET) عبر i= في الاستعلام."""
    viewer = _viewer_from_init(request.rel_url.query.get("i", ""))
    if not viewer:
        return None, None
    try:
        cid = int(request.match_info["cid"])
    except (KeyError, ValueError):
        return viewer, None
    if not is_authorized_for(viewer["id"], cid):
        return viewer, False
    return viewer, cid


async def _h_media(request):
    viewer, cid = _auth_query(request)
    if not viewer or cid in (None, False):
        return web.Response(status=403 if viewer else 401)
    uid = int(request.match_info["uid"])
    peer = int(request.match_info["peer"])
    mid = int(request.match_info["mid"])
    path = await download_message_media(uid, peer, mid)
    if path and os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "rb") as f:
            data = f.read()
        return web.Response(body=data, content_type="image/jpeg",
                            headers={"Cache-Control": "private, max-age=86400"})
    return web.Response(status=404)


async def _h_vmedia(request):
    """يقدّم الفيديو الفعلي (قابل للتشغيل) مع دعم Range."""
    viewer, cid = _auth_query(request)
    if not viewer or cid in (None, False):
        return web.Response(status=403 if viewer else 401)
    uid = int(request.match_info["uid"])
    peer = int(request.match_info["peer"])
    mid = int(request.match_info["mid"])
    path = await download_message_video(uid, peer, mid)
    if not (path and os.path.exists(path) and os.path.getsize(path) > 0):
        return web.Response(status=404)
    # FileResponse يتكفّل بـ Range/206 (لازم iOS) ويبثّ الملف بلا تحميله كاملا
    # في الذاكرة ولا حجب حلقة asyncio اثناء تشغيل الفيديو.
    return web.FileResponse(path, headers={"Content-Type": "video/mp4",
                                           "Cache-Control": "private, max-age=86400"})


async def _h_send(request):
    """يرسل رسالة نصّية في محادثة عبر جلسة المشرف (حيث يملك صلاحية الكتابة)."""
    viewer, cid = _auth_cid(request)
    if not viewer or cid in (None, False):
        return web.json_response({"error": "forbidden"}, status=403 if viewer else 401)
    uid = int(request.match_info["uid"])
    peer = int(request.match_info["peer"])
    try:
        body = await request.json()
    except Exception:
        body = {}
    text = (body.get("text") or "").strip()
    if not text:
        return web.json_response({"error": "empty"}, status=400)
    text = text[:4096]
    extras = _cfg["extras"]
    client = await extras.get_client(uid) if extras is not None else None
    if client is None:
        return web.json_response({"error": "no_session"}, status=400)
    try:
        sent = await client.send_message(peer, text)
    except Exception as e:
        # غالبا لا صلاحيه كتابه (ChatWriteForbidden) او قيود اخرى
        return web.json_response({"error": "send_failed", "detail": str(e)[:140]})
    return web.json_response({"ok": True, "id": getattr(sent, "id", None),
                              "ts": int(sent.date.timestamp()) if getattr(sent, "date", None) else 0})


async def _h_uavatar(request):
    """صورة مشرف عبر جلسته (i= للمصادقة). fallback حرف."""
    viewer = _viewer_from_init(request.rel_url.query.get("i", ""))
    if not viewer:
        return web.Response(status=401)
    uid = int(request.match_info["uid"])
    label = request.rel_url.query.get("name", str(uid))
    try:
        path = await download_own_avatar(uid)
    except Exception:
        path = None
    if path and os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "rb") as f:
            data = f.read()
        return web.Response(body=data, content_type="image/jpeg",
                            headers={"Cache-Control": "private, max-age=3600"})
    return web.Response(body=_letter_avatar_svg(label), content_type="image/svg+xml",
                        headers={"Cache-Control": "public, max-age=600"})


async def _h_logo(request):
    path = _logo_path()
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "rb") as f:
            data = f.read()
        return web.Response(body=data, content_type="image/jpeg",
                            headers={"Cache-Control": "public, max-age=86400"})
    _ensure_logo()   # خيط خلفي غير معطِّل؛ الخلفية المتدرجة تظهر ريثما يجهز
    return web.Response(status=404)


async def _h_pavatar(request):
    viewer, cid = _auth_query(request)
    if not viewer or cid in (None, False):
        return web.Response(status=403 if viewer else 401)
    uid = int(request.match_info["uid"])
    peer = int(request.match_info["peer"])
    label = request.rel_url.query.get("name", str(peer))
    path = await download_peer_avatar(uid, peer)
    if path and os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "rb") as f:
            data = f.read()
        return web.Response(body=data, content_type="image/jpeg",
                            headers={"Cache-Control": "private, max-age=3600"})
    return web.Response(body=_letter_avatar_svg(label), content_type="image/svg+xml",
                        headers={"Cache-Control": "public, max-age=600"})


async def _h_stream(request):
    # EventSource لا يرسل ترويسات؛ نقبل initData في الاستعلام (i=...)
    viewer = _viewer_from_init(request.rel_url.query.get("i", ""))
    if not viewer:
        return web.Response(status=401, text="unauthorized")
    try:
        cid = int(request.match_info["cid"])
    except (KeyError, ValueError):
        return web.Response(status=400)
    if not is_authorized_for(viewer["id"], cid):
        return web.Response(status=403)
    uid = int(request.match_info["uid"])
    resp = web.StreamResponse(headers={
        "Content-Type": "text/event-stream", "Cache-Control": "no-cache",
        "Connection": "keep-alive", "X-Accel-Buffering": "no"})
    await resp.prepare(request)
    q = asyncio.Queue()
    _subs[uid].add(q)
    try:
        await resp.write(b": connected\n\n")
        while True:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=25)
                await resp.write(("data: " + json.dumps(ev, ensure_ascii=False) + "\n\n").encode("utf-8"))
            except asyncio.TimeoutError:
                await resp.write(b": ping\n\n")
    except (asyncio.CancelledError, ConnectionResetError):
        pass
    except Exception:
        pass
    finally:
        _subs[uid].discard(q)
    return resp


async def _h_avatar(request):
    kind = request.match_info.get("kind")
    ident = request.match_info.get("ident")
    label = request.rel_url.query.get("name", ident or "?")
    if kind not in ("channel", "user"):
        return web.Response(status=404)
    try:
        ident_int = int(ident)
    except (TypeError, ValueError):
        ident_int = ident
    path = None
    try:
        path = await _download_photo(kind, ident_int)
    except Exception:
        path = None
    if path and os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            with open(path, "rb") as f:
                data = f.read()
            return web.Response(body=data, content_type="image/jpeg",
                                headers={"Cache-Control": "public, max-age=3600"})
        except Exception:
            pass
    return web.Response(body=_letter_avatar_svg(label), content_type="image/svg+xml",
                        headers={"Cache-Control": "public, max-age=600"})


def _build_app():
    app = web.Application()
    app.router.add_get("/", _h_index)
    app.router.add_get("/health", _h_health)
    app.router.add_post("/api/channels", _h_channels)
    app.router.add_post("/api/channel/{cid}/admins", _h_admins)
    app.router.add_post("/api/channel/{cid}/admin/{uid}/dialogs", _h_dialogs)
    app.router.add_post("/api/channel/{cid}/admin/{uid}/dialog/{peer}/messages", _h_messages)
    app.router.add_get("/api/channel/{cid}/admin/{uid}/stream", _h_stream)
    app.router.add_get("/api/channel/{cid}/admin/{uid}/media/{peer}/{mid}", _h_media)
    app.router.add_get("/api/channel/{cid}/admin/{uid}/vmedia/{peer}/{mid}", _h_vmedia)
    app.router.add_post("/api/channel/{cid}/admin/{uid}/dialog/{peer}/send", _h_send)
    app.router.add_get("/api/channel/{cid}/admin/{uid}/pavatar/{peer}", _h_pavatar)
    app.router.add_get("/uavatar/{uid}", _h_uavatar)
    app.router.add_get("/logo.jpg", _h_logo)
    app.router.add_get("/avatar/{kind}/{ident}", _h_avatar)
    return app


async def start(host="0.0.0.0", port=8080):
    if not AIOHTTP_OK:
        logger.warning("aiohttp غير مثبت - Mini App معطّل.")
        return None
    if _cfg["db_path"]:
        try:
            init_miniapp_db(_cfg["db_path"])
        except Exception as e:
            logger.warning("init_miniapp_db: %s", e)
    try:
        _ensure_logo()
    except Exception:
        pass
    runner = web.AppRunner(_build_app())
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info("Mini App يعمل على http://%s:%s", host, port)
    return runner


async def stop(runner):
    await stop_archiver()
    if runner is not None:
        try:
            await runner.cleanup()
        except Exception:
            pass


# =========================================================================
#  الواجهة (احمر/اسود - eye of hell)
# =========================================================================

INDEX_HTML = r"""<!doctype html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>eye of hell</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
  :root{
    --bg:#080607; --bg2:#141012; --card:#1c1113; --card2:#241416;
    --line:#33191c; --line2:#4a1f24;
    --red:#ff2233; --red2:#c8121f; --red-dim:#7a0d16; --red-soft:#2a0c10;
    --fg:#f5ecec; --muted:#a8999a; --green:#33d17a;
    --shadow:0 10px 30px rgba(0,0,0,.55);
    --ring:0 0 0 2px var(--red-dim);
  }
  *{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
  /* يجب ان تسبق قواعد display الاخرى: العنصر ذو hidden يبقى مخفيا فعلا
     (والا فان #acct{display:flex} كان يتجاوز hidden ويغطّي كل الموقع). */
  [hidden]{display:none!important}
  html,body{margin:0}
  body{
    background:
      radial-gradient(1200px 520px at 50% -160px, rgba(255,34,51,.16), transparent 60%),
      radial-gradient(700px 380px at 100% 0, rgba(255,34,51,.07), transparent 55%),
      var(--bg);
    background-attachment:fixed;
    color:var(--fg); min-height:100vh;
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Tahoma,Arial,sans-serif;
    font-size:15px; line-height:1.5; letter-spacing:.1px;
  }
  ::selection{background:var(--red);color:#fff}
  .wrap{max-width:600px;margin:0 auto;
    padding:calc(env(safe-area-inset-top,0) + 8px) 14px calc(env(safe-area-inset-bottom,0) + 48px)}
  /* الرأس */
  .top{position:sticky;top:0;z-index:20;display:flex;align-items:center;gap:13px;
    padding:12px 6px;margin:0 -6px 12px;
    background:linear-gradient(180deg, rgba(8,6,7,.92) 60%, rgba(8,6,7,0));
    backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px)}
  .logo{width:46px;height:46px;border-radius:50%;flex:0 0 auto;position:relative;
    background:url('/logo.jpg') center/cover no-repeat,
      radial-gradient(circle at 50% 42%,#fff 0 20%,#0a0a0a 23% 100%);
    box-shadow:0 0 0 2px var(--red),0 0 22px rgba(255,34,51,.55)}
  .logo::after{content:"";position:absolute;inset:-5px;border-radius:50%;
    border:1px solid rgba(255,34,51,.35);animation:pulse 2.4s ease-out infinite}
  @keyframes pulse{0%{transform:scale(.9);opacity:.9}100%{transform:scale(1.5);opacity:0}}
  .brand{font-weight:800;font-size:22px;line-height:1.1;letter-spacing:.3px}
  .brand b{color:var(--red)}
  .sub{color:var(--muted);font-size:12px;margin-top:3px}
  /* عناوين الاقسام */
  .section-title{color:var(--muted);font-size:13px;font-weight:600;margin:10px 4px 12px;
    display:flex;justify-content:space-between;align-items:center;gap:8px}
  .pill{background:linear-gradient(180deg,var(--red2),var(--red-dim));color:#fff;
    padding:2px 11px;border-radius:999px;font-size:12px;font-weight:800;
    box-shadow:0 2px 8px rgba(255,34,51,.25)}
  /* البطاقات */
  #view{animation:fade .25s ease}
  @keyframes fade{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
  .card{display:flex;align-items:center;gap:13px;
    background:linear-gradient(180deg,var(--card2),var(--card));
    border:1px solid var(--line);border-radius:18px;padding:12px 14px;margin-bottom:11px;
    cursor:pointer;transition:transform .12s ease,border-color .15s,box-shadow .2s;
    box-shadow:0 1px 0 rgba(255,255,255,.02) inset}
  .card:active{transform:scale(.985)}
  @media (hover:hover){.card:hover{border-color:var(--line2);
    box-shadow:var(--shadow),0 0 0 1px rgba(255,34,51,.15);transform:translateY(-1px)}}
  .av{width:54px;height:54px;border-radius:50%;object-fit:cover;background:var(--bg2);
    box-shadow:var(--ring);flex:0 0 auto}
  .meta{flex:1;min-width:0}
  .name{font-weight:700;font-size:15.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
    display:flex;align-items:center;gap:7px}
  .hint{color:var(--muted);font-size:12.5px;margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .chev{color:var(--red);font-size:22px;opacity:.55;flex:0 0 auto;transition:transform .15s}
  @media (hover:hover){.card:hover .chev{opacity:.9;transform:translateX(-3px)}}
  .dot{width:9px;height:9px;border-radius:50%;display:inline-block;flex:0 0 auto}
  .dot.on{background:var(--green);box-shadow:0 0 8px rgba(51,209,122,.7)}
  .dot.off{background:#5a4a4a}
  .xbadge{color:#fff;background:linear-gradient(180deg,var(--red),var(--red2));border-radius:7px;
    font-size:11px;font-weight:800;padding:2px 8px;display:inline-flex;align-items:center;gap:3px;
    box-shadow:0 2px 8px rgba(255,34,51,.35)}
  /* حالات فارغة */
  .empty{text-align:center;padding:64px 22px;color:var(--muted)}
  .empty .big{font-size:52px;margin-bottom:12px;filter:drop-shadow(0 6px 16px rgba(255,34,51,.3))}
  .empty div{font-size:15px}
  /* رجوع */
  .back{display:inline-flex;align-items:center;gap:6px;color:var(--fg);
    background:var(--card);border:1px solid var(--line);border-radius:12px;
    font-size:14px;padding:8px 14px;margin:2px 0 14px;cursor:pointer;font-family:inherit;
    transition:.15s;font-weight:600}
  @media (hover:hover){.back:hover{border-color:var(--red);color:var(--red)}}
  /* تحميل */
  .spin{border:3px solid var(--line);border-top-color:var(--red);width:34px;height:34px;
    border-radius:50%;margin:56px auto;animation:s 1s linear infinite}
  @keyframes s{to{transform:rotate(360deg)}}
  .err{background:linear-gradient(180deg,var(--red-soft),#1a0a0c);color:#ffd7da;
    padding:14px 16px;border-radius:14px;margin:12px 0;border:1px solid var(--line2)}
  /* المحادثة */
  .msgs{display:flex;flex-direction:column;gap:8px;padding:8px 2px 26px}
  .bub{max-width:82%;padding:9px 13px;border-radius:16px;font-size:14.5px;line-height:1.55;
    word-break:break-word;position:relative;box-shadow:0 2px 10px rgba(0,0,0,.3);animation:pop .18s ease}
  @keyframes pop{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}
  .bub.in{background:linear-gradient(180deg,var(--card2),var(--card));border:1px solid var(--line);
    align-self:flex-start;border-bottom-right-radius:5px}
  .bub.out{background:linear-gradient(180deg,#8f0f1b,#630a12);align-self:flex-end;
    border-bottom-left-radius:5px}
  .bub.del{opacity:.92;border:1px dashed var(--red)}
  .bub .mx{position:absolute;top:-9px;inset-inline-start:-9px;
    background:linear-gradient(180deg,var(--red),var(--red2));color:#fff;
    width:20px;height:20px;border-radius:50%;font-size:12px;font-weight:800;
    display:flex;align-items:center;justify-content:center;box-shadow:0 0 0 2px var(--bg),0 2px 8px rgba(255,34,51,.5)}
  .bub .t{color:rgba(255,255,255,.5);font-size:10.5px;margin-top:5px;text-align:end}
  .bub.in .t{color:var(--muted)}
  .media-tag{color:#ffb3b8;font-style:italic}
  .mmedia{max-width:230px;max-height:280px;border-radius:12px;margin-bottom:5px;display:block;
    border:1px solid var(--line)}
  .mtag{color:#ffd7da;font-size:12px;margin-top:3px;display:flex;align-items:center;gap:4px}
  .live{font-size:11px;color:var(--green);margin-inline-start:8px;font-weight:700}
  /* هوية فاتح الموقع (الجهة المقابلة للاسم) */
  .opener{display:flex;align-items:center;gap:8px;flex:0 0 auto;max-width:44%}
  .opener .oav{width:34px;height:34px;border-radius:50%;object-fit:cover;background:var(--card2);
    box-shadow:var(--ring)}
  .opener .oname{font-size:12.5px;font-weight:700;white-space:nowrap;overflow:hidden;
    text-overflow:ellipsis;max-width:120px}
  /* احصائيات اسفل الموقع */
  .stats{margin:22px 2px 8px;display:grid;grid-template-columns:repeat(2,1fr);gap:11px}
  .stat{background:linear-gradient(180deg,var(--card2),var(--card));border:1px solid var(--line);
    border-radius:16px;padding:14px;text-align:center}
  .stat .n{font-size:26px;font-weight:800;color:var(--red);line-height:1}
  .stat .l{color:var(--muted);font-size:12px;margin-top:6px}
  .stats-title{color:var(--muted);font-size:13px;font-weight:600;margin:26px 4px 4px}
  @media (min-width:820px){.stats{grid-template-columns:repeat(4,1fr)}}
  /* شريط تمرير للابتوب */
  @media (hover:hover){
    *::-webkit-scrollbar{width:10px}
    *::-webkit-scrollbar-thumb{background:var(--line2);border-radius:8px;border:2px solid var(--bg)}
    *::-webkit-scrollbar-track{background:transparent}
  }
  /* ===== واجهة الحساب بملء الشاشة (تُغطّي الرأس واسم البوت واسمك) ===== */
  #acct{position:fixed;inset:0;z-index:200;background:var(--bg);
    display:flex;flex-direction:column;animation:fade .2s ease}
  .acct-top{flex:0 0 auto;display:flex;align-items:center;gap:11px;
    padding:calc(env(safe-area-inset-top,0) + 12px) 14px 12px;
    background:linear-gradient(180deg,#170a0d,#0d0708);
    border-bottom:1px solid var(--line2);box-shadow:0 6px 22px rgba(0,0,0,.55)}
  .acct-back{flex:0 0 auto;background:var(--card);border:1px solid var(--line);
    color:var(--fg);width:40px;height:40px;border-radius:13px;font-size:23px;line-height:1;
    cursor:pointer;display:flex;align-items:center;justify-content:center;
    font-family:inherit;transition:.15s}
  @media (hover:hover){.acct-back:hover{border-color:var(--red);color:var(--red)}}
  .acct-av{width:46px;height:46px;border-radius:50%;object-fit:cover;background:var(--card2);
    box-shadow:var(--ring);flex:0 0 auto}
  .acct-ti{flex:1;min-width:0}
  .acct-name{font-weight:800;font-size:17px;line-height:1.2;white-space:nowrap;
    overflow:hidden;text-overflow:ellipsis}
  .acct-sub{color:var(--muted);font-size:12px;margin-top:3px;white-space:nowrap;
    overflow:hidden;text-overflow:ellipsis}
  .acct-sub.on{color:var(--green)}
  .acct-body{flex:1;min-height:0;overflow-y:auto;overflow-x:hidden;-webkit-overflow-scrolling:touch;
    padding:12px 14px 26px;width:100%;max-width:760px;margin:0 auto}
  .acct-body .msgs{padding-top:4px}
  /* شريط المجلدات: تبويبات علوية قابلة للتبديل كمجلدات تيليجرام */
  .folders{flex:0 0 auto;display:flex;gap:8px;overflow-x:auto;overflow-y:hidden;
    padding:10px 14px 6px;scrollbar-width:none;-webkit-overflow-scrolling:touch}
  .folders::-webkit-scrollbar{display:none}
  .folder{flex:0 0 auto;background:var(--card);border:1px solid var(--line);color:var(--muted);
    border-radius:999px;padding:7px 14px;font-size:13.5px;font-weight:700;white-space:nowrap;
    cursor:pointer;font-family:inherit;transition:.15s;display:flex;align-items:center;gap:6px}
  .folder .cnt{background:var(--bg2);color:var(--fg);border-radius:999px;font-size:11px;
    padding:1px 7px;font-weight:800}
  .folder.act{background:linear-gradient(180deg,var(--red2),var(--red-dim));color:#fff;
    border-color:transparent;box-shadow:0 3px 12px rgba(255,34,51,.3)}
  .folder.act .cnt{background:rgba(0,0,0,.28);color:#fff}
  /* الشريط السفلي (المحادثات | جهات الاتصال) كتيليجرام وملف server */
  .acct-bottom{flex:0 0 auto;display:flex;background:linear-gradient(0deg,#150a0c,#0d0708);
    border-top:1px solid var(--line2);padding-bottom:env(safe-area-inset-bottom,0)}
  .bnav{flex:1;background:none;border:0;color:var(--muted);font-family:inherit;
    padding:9px 6px 11px;cursor:pointer;display:flex;flex-direction:column;align-items:center;gap:3px;
    font-size:11.5px;font-weight:700;transition:.15s}
  .bnav .bi{font-size:20px;line-height:1}
  @media (hover:hover){.bnav:hover{color:var(--fg)}}
  .bnav.act{color:var(--red)}
  video.mmedia{background:#000}
  .mmedia{cursor:pointer}
  /* شريط ارسال رسالة (اسفل المحادثة) */
  .composer{flex:0 0 auto;display:flex;gap:8px;align-items:flex-end;
    padding:8px 10px calc(env(safe-area-inset-bottom,0) + 8px);
    background:linear-gradient(0deg,#150a0c,#0d0708);border-top:1px solid var(--line2)}
  .cinput{flex:1;min-width:0;resize:none;max-height:120px;background:var(--card);
    color:var(--fg);border:1px solid var(--line);border-radius:16px;padding:10px 14px;
    font-family:inherit;font-size:14.5px;line-height:1.4;outline:none}
  .cinput:focus{border-color:var(--red)}
  .csend{flex:0 0 auto;width:42px;height:42px;border-radius:50%;border:0;cursor:pointer;
    background:linear-gradient(180deg,var(--red),var(--red2));color:#fff;font-size:18px;
    display:flex;align-items:center;justify-content:center;transform:scaleX(-1)}
  .csend:disabled{opacity:.5}
  /* عارض الصورة بملء الشاشة */
  .lightbox{position:fixed;inset:0;z-index:400;background:rgba(0,0,0,.92);
    display:flex;align-items:center;justify-content:center;padding:16px}
  .lightbox img{max-width:100%;max-height:100%;border-radius:10px}
  /* اسم المطوّر تحت العلامة */
  .devrow{display:flex;align-items:center;gap:6px;margin-top:3px}
  .devtag{background:linear-gradient(180deg,var(--red2),var(--red-dim));color:#fff;
    font-size:9.5px;font-weight:800;border-radius:6px;padding:1px 6px}
  .devname{color:var(--fg);font-size:12.5px;font-weight:700;white-space:nowrap;
    overflow:hidden;text-overflow:ellipsis}
  /* لابتوب/شاشات واسعة */
  @media (min-width:820px){
    body{font-size:15.5px}
    .wrap{max-width:760px;padding-left:22px;padding-right:22px}
    .card{padding:14px 18px;border-radius:20px}
    .av{width:58px;height:58px}
    .brand{font-size:24px}
    .bub{max-width:70%}
  }
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div class="logo"></div>
    <div style="flex:1;min-width:0">
      <div class="brand">eye of <b>hell</b></div>
      <div class="sub" id="whoami">جارٍ التحميل…</div>
    </div>
    <div class="opener" id="opener" hidden>
      <div class="oname" id="oname"></div>
      <img class="oav" id="oav" alt="">
    </div>
  </div>
  <div id="view"><div class="spin"></div></div>
</div>

<!-- واجهة الحساب بملء الشاشة (تظهر فوق كل شيء عند فتح مشرف) -->
<div id="acct" hidden>
  <div class="acct-top">
    <button class="acct-back" id="acctBack" aria-label="رجوع">‹</button>
    <img class="acct-av" id="acctAv" alt="">
    <div class="acct-ti">
      <div class="acct-name" id="acctName">—</div>
      <div class="acct-sub" id="acctSub"></div>
    </div>
  </div>
  <div class="folders" id="acctFolders" hidden></div>
  <div class="acct-body" id="acctBody"><div class="spin"></div></div>
  <div class="acct-bottom" id="acctBottom" hidden>
    <button class="bnav act" id="navChats" onclick="acctTab('chats')"><span class="bi">💬</span>المحادثات</button>
    <button class="bnav" id="navContacts" onclick="acctTab('contacts')"><span class="bi">👤</span>جهات الاتصال</button>
  </div>
  <div class="composer" id="acctComposer" hidden>
    <textarea class="cinput" id="cinput" rows="1" placeholder="اكتب رسالة…"></textarea>
    <button class="csend" id="csend" onclick="sendMsg()" aria-label="ارسال">➤</button>
  </div>
</div>
<div class="lightbox" id="lightbox" hidden onclick="closeImg()"><img id="lightboxImg" alt=""></div>
<script>
const TG = window.Telegram ? window.Telegram.WebApp : null;
if(TG){try{TG.ready();TG.expand();}catch(e){}}
const INIT = TG ? TG.initData : "";
const view = document.getElementById('view');
const whoami = document.getElementById('whoami');
let ES = null;
const state = {cid:null, uid:null};

async function api(path){
  const r = await fetch(path,{method:'POST',headers:{'X-Telegram-Init-Data':INIT}});
  if(!r.ok){throw new Error(r.status);} return r.json();
}
function esc(s){const d=document.createElement('div');d.textContent=s==null?'':s;return d.innerHTML;}
function closeES(){ if(ES){try{ES.close();}catch(e){} ES=null;} stopPoll(); }
// تحديث مباشر عبر الاستطلاع (يضمن العمل عبر نفق Cloudflare حيث قد لا يصل SSE)
let _pollT=null;
function stopPoll(){ if(_pollT){ clearInterval(_pollT); _pollT=null; } }
function startPoll(mode, peer){
  stopPoll();
  if(mode==='dialog'){
    _pollT=setInterval(()=>{ if(state.inDialog && state.peer==peer) pollDialog(peer); }, 3500);
  } else if(mode==='dialogs'){
    _pollT=setInterval(()=>{ if(!state.inDialog) refreshDialogs(state.uid); }, 9000);
  }
}
async function pollDialog(peer){
  if(state.polling) return;
  state.polling=true;
  try{
    const d=await api('/api/channel/'+state.cid+'/admin/'+state.uid+'/dialog/'+peer+'/messages');
    const box=document.getElementById('msgs');
    if(!box || !state.inDialog || state.peer!=peer) return;
    const atBottom=(acctBody.scrollHeight-acctBody.scrollTop-acctBody.clientHeight)<90;
    let added=false;
    for(const m of d.messages){
      const b=box.querySelector('.bub[data-id="'+m.id+'"]');
      if(!b){
        if(box.querySelector('.empty')) box.innerHTML='';
        box.insertAdjacentHTML('beforeend', msgHtml(m)); added=true;
      } else if(m.deleted && !b.classList.contains('del')){
        b.classList.add('del'); b.insertAdjacentHTML('afterbegin','<span class="mx">✕</span>');
      }
    }
    if(added && atBottom) scrollDown();
  }catch(e){}
  finally{ state.polling=false; }
}
function ttime(ts){ if(!ts)return''; const d=new Date(ts*1000);
  return d.toLocaleString('ar',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'}); }

/* ===== واجهة الحساب بملء الشاشة ===== */
const acct=document.getElementById('acct');
const acctBody=document.getElementById('acctBody');
function setAcctHeader(name, avUrl, sub, live, onBack){
  document.getElementById('acctName').textContent = name || '—';
  const sb=document.getElementById('acctSub');
  sb.textContent = sub || ''; sb.classList.toggle('on', !!live);
  const av=document.getElementById('acctAv');
  if(avUrl){ av.src=avUrl; av.style.visibility='visible'; } else { av.style.visibility='hidden'; }
  document.getElementById('acctBack').onclick = onBack;
  syncTgBack(onBack);
}
function syncTgBack(fn){
  try{ if(TG && TG.BackButton){ if(fn){ TG.BackButton.offClick(); TG.BackButton.onClick(fn); TG.BackButton.show(); } } }catch(e){}
}
function closeAcct(){
  closeES(); acct.hidden=true; document.body.style.overflow='';
  try{ if(TG && TG.BackButton) TG.BackButton.hide(); }catch(e){}
}
function openAcct(){ acct.hidden=false; document.body.style.overflow='hidden'; acctBody.scrollTop=0; }

async function loadChannels(){
  closeES(); state.cid=null; state.uid=null;
  view.innerHTML='<div class="spin"></div>';
  let data; try{ data=await api('/api/channels'); }
  catch(e){ view.innerHTML='<div class="err">تعذّر التحقق من هويتك. افتح التطبيق من داخل تيليجرام.</div>';
    whoami.textContent='غير مُتحقّق'; return; }
  const dev = data.dev_name || 'eye of hell';
  whoami.innerHTML='<span class="devrow"><span class="devtag">المطوّر</span>'+
    '<span class="devname">'+esc(dev)+'</span></span>';
  fillOpener(data.viewer);
  if(!data.channels.length){ view.innerHTML='<div class="empty"><div class="big">🚫</div>'+
    '<div>لا توجد قنوات مضافة لك</div></div>'; return; }
  let h='<div class="section-title">القنوات <span class="pill">'+data.channels.length+'</span></div>';
  for(const c of data.channels){
    h+='<div class="card" onclick="openChannel('+c.id+')">'+
      '<img class="av" src="'+c.avatar+'?name='+encodeURIComponent(c.name)+'">'+
      '<div class="meta"><div class="name">'+esc(c.name)+'</div>'+
      (c.username?'<div class="hint">@'+esc(c.username)+'</div>':'<div class="hint">قناة خاصة</div>')+
      '</div><div class="chev">‹</div></div>';
  }
  h+=statsHtml(data.stats);
  view.innerHTML=h;
}

function fillOpener(viewer){
  try{
    const u = (TG && TG.initDataUnsafe) ? TG.initDataUnsafe.user : null;
    const box=document.getElementById('opener');
    const nm=document.getElementById('oname'); const av=document.getElementById('oav');
    const name = (u && ((u.first_name||'')+' '+(u.last_name||'')).trim()) || (viewer && viewer.name) || '';
    if(!name){ box.hidden=true; return; }
    nm.textContent = name;
    if(u && u.photo_url){ av.src=u.photo_url; av.style.display=''; }
    else {
      const ch=(name.trim()[0]||'?').toUpperCase();
      av.src='data:image/svg+xml;utf8,'+encodeURIComponent(
        '<svg xmlns="http://www.w3.org/2000/svg" width="68" height="68">'+
        '<rect width="68" height="68" rx="34" fill="#c8121f"/>'+
        '<text x="50%" y="50%" dy=".35em" text-anchor="middle" font-family="Arial" '+
        'font-size="34" fill="#0a0a0a" font-weight="700">'+ch+'</text></svg>');
    }
    box.hidden=false;
  }catch(e){}
}

function statItem(n,l){ return '<div class="stat"><div class="n">'+(n==null?0:n)+
  '</div><div class="l">'+l+'</div></div>'; }
function statsHtml(st){          // احصائيات عامة لكل القنوات (الصفحة الرئيسية)
  if(!st) return '';
  return '<div class="stats-title">إحصائيات عامة</div><div class="stats">'+
    statItem(st.channels,'القنوات')+
    statItem(st.violations,'مخالفات مُزالة')+
    statItem(st.edits_deleted,'تعديلات مُسحت')+
    statItem(st.archived,'رسائل مؤرشفة')+
    '</div>';
}
function channelStatsHtml(st){   // احصائيات خاصة بالقناة (داخل القناة عند الضغط)
  if(!st) return '';
  let tiles=statItem(st.admins,'المشرفون')+
    statItem(st.violations,'مخالفات مُزالة')+
    statItem(st.edits_deleted,'تعديلات مُسحت');
  if(st.members!=null) tiles+=statItem(st.members,'الأعضاء');
  return '<div class="stats-title">إحصائيات القناة</div><div class="stats">'+tiles+'</div>';
}

async function openChannel(cid){
  closeES(); state.cid=cid; state.uid=null;
  view.innerHTML='<div class="spin"></div>';
  let data; try{ data=await api('/api/channel/'+cid+'/admins'); }
  catch(e){ view.innerHTML='<button class="back" onclick="loadChannels()">‹ رجوع</button>'+
    '<div class="err">تعذّر جلب المشرفين.</div>'; return; }
  let h='<button class="back" onclick="loadChannels()">‹ رجوع للقنوات</button>';
  h+='<div class="section-title">المشرفون <span class="pill">'+data.count+'</span></div>';
  if(!data.admins.length){ h+='<div class="empty"><div class="big">👤</div><div>لا يوجد مشرفون</div>'+
    '<div class="hint" style="margin-top:8px">أو الحساب المساعد غير مُضاف للقناة.</div></div>'; }
  else for(const a of data.admins){
    const nm=esc(a.name).replace(/'/g,'&#39;');
    h+='<div class="card" onclick="openAdmin('+a.id+",'"+nm+"'"+')">'+
      '<img class="av" src="/uavatar/'+a.id+'?i='+encodeURIComponent(INIT)+
        '&name='+encodeURIComponent(a.name)+'">'+
      '<div class="meta"><div class="name">'+esc(a.name)+
      '<span class="dot '+(a.has_session?'on':'off')+'"></span></div>'+
      '<div class="hint">'+(a.has_session?'اضغط':'بلا جلسة')+'</div>'+
      '</div><div class="chev">‹</div></div>';
  }
  h+=channelStatsHtml(data.stats);
  view.innerHTML=h;
}

// عناصر الواجهة السفلية/العلوية
const acctFolders=document.getElementById('acctFolders');
const acctBottom=document.getElementById('acctBottom');
const acctComposer=document.getElementById('acctComposer');

// فتح حساب المشرف بملء الشاشة — مجلدات علوية (كتيليجرام) + شريط سفلي للمحادثات/جهات الاتصال
async function openAdmin(uid, name){
  closeES(); state.uid=uid;
  state.aname = name || ('#'+uid);
  state.aav = '/uavatar/'+uid+'?i='+encodeURIComponent(INIT)+'&name='+encodeURIComponent(state.aname);
  state.folder='all'; state.tab='chats'; state.inDialog=false;
  state.dialogs=[]; state.contacts=[];
  openAcct();
  acctFolders.hidden=true; acctBottom.hidden=true;
  acctBody.innerHTML='<div class="spin"></div>';
  setAcctHeader(state.aname, state.aav, '', false, closeAcct);
  let data; try{ data=await api('/api/channel/'+state.cid+'/admin/'+uid+'/dialogs'); }
  catch(e){ acctBody.innerHTML='<div class="err">تعذّر جلب المحادثات.</div>'; return; }
  if(data.error==='no_session'){
    acctBody.innerHTML='<div class="empty"><div class="big">🔒</div>'+
      '<div>لا توجد جلسة لهذا المشرف</div></div>'; return; }
  state.dialogs=data.dialogs||[]; state.contacts=data.contacts||[];
  acctBottom.hidden=false;
  paintAcct();
  startStream(state.uid, 'dialogs', null);
  startPoll('dialogs', null);
}

// المجلدات (كمجلدات تيليجرام): الكل/الخاص/المجموعات/القنوات/البوتات
function foldersDef(){ return [
  {k:'all', t:'الكل'}, {k:'user', t:'الخاص'}, {k:'group', t:'المجموعات'},
  {k:'channel', t:'القنوات'}, {k:'bot', t:'البوتات'}]; }
function folderCount(k){
  const ds=state.dialogs||[];
  return k==='all' ? ds.length : ds.filter(d=>d.type===k).length;
}
function renderFolders(){
  let h='';
  for(const f of foldersDef()){
    h+='<button class="folder'+(state.folder===f.k?' act':'')+
       '" onclick="setFolder(\''+f.k+'\')">'+f.t+
       ' <span class="cnt">'+folderCount(f.k)+'</span></button>';
  }
  return h;
}
function currentDialogs(){
  const ds=state.dialogs||[];
  return state.folder==='all' ? ds : ds.filter(d=>d.type===state.folder);
}
function dialogCard(d){
  const nm = esc(d.name).replace(/'/g,'&#39;');
  const badge = d.deleted ? ' <span class="xbadge">✕ محذوفة</span>'
    : (d.del_count ? ' <span class="xbadge">✕ '+d.del_count+'</span>' : '');
  return '<div class="card" onclick="openDialog('+d.peer+",'"+nm+"'"+')">'+
    '<img class="av" src="/api/channel/'+state.cid+'/admin/'+state.uid+'/pavatar/'+d.peer+
      '?i='+encodeURIComponent(INIT)+'&name='+encodeURIComponent(d.name)+'">'+
    '<div class="meta"><div class="name">'+esc(d.name)+badge+'</div>'+
    '<div class="hint">'+esc(d.preview||'…')+'</div></div>'+
    '<div class="chev">‹</div></div>';
}
function contactCard(c){
  const un = c.username ? ("'"+String(c.username).replace(/'/g,'')+"'") : 'null';
  return '<div class="card" onclick="openContactTg('+c.peer+','+un+')">'+
    '<img class="av" src="/api/channel/'+state.cid+'/admin/'+state.uid+'/pavatar/'+c.peer+
      '?i='+encodeURIComponent(INIT)+'&name='+encodeURIComponent(c.name)+'">'+
    '<div class="meta"><div class="name">'+esc(c.name)+'</div>'+
    (c.username?'<div class="hint">@'+esc(c.username)+'</div>':'<div class="hint">جهة اتصال</div>')+
    '</div><div class="chev">↗</div></div>';
}
// فتح حساب جهة الاتصال في تيليجرام مباشرة
function openContactTg(peer, username){
  try{
    if(username){
      const l='https://t.me/'+username;
      if(TG && TG.openTelegramLink){ TG.openTelegramLink(l); return; }
      window.open(l,'_blank'); return;
    }
    const u='tg://user?id='+peer;
    try{ if(TG && TG.openTelegramLink){ TG.openTelegramLink(u); return; } }catch(e){}
    window.location.href=u;
  }catch(e){ try{ window.open('https://t.me/'+(username||''),'_blank'); }catch(_){} }
}
function renderList(){
  if(state.tab==='contacts'){
    const cs=state.contacts||[];
    if(!cs.length) return '<div class="empty"><div class="big">📇</div>'+
      '<div>لا توجد جهات اتصال</div></div>';
    let h=''; for(const c of cs) h+=contactCard(c); return h;
  }
  const arr=currentDialogs();
  if(!arr.length) return '<div class="empty"><div class="big">💬</div>'+
    '<div>لا توجد محادثات في هذا المجلد</div></div>';
  let h=''; for(const d of arr) h+=dialogCard(d); return h;
}
function paintAcct(){
  acctFolders.hidden = (state.tab!=='chats');
  if(state.tab==='chats') acctFolders.innerHTML=renderFolders();
  document.getElementById('navChats').classList.toggle('act', state.tab==='chats');
  document.getElementById('navContacts').classList.toggle('act', state.tab==='contacts');
  acctBody.innerHTML='<div id="dlist">'+renderList()+'</div>';
  acctBody.scrollTop=0;
}
function paintList(){
  if(state.tab==='chats') acctFolders.innerHTML=renderFolders();
  const l=document.getElementById('dlist'); if(l) l.innerHTML=renderList();
}
function setFolder(f){ state.folder=f; paintList(); }
function acctTab(t){ if(state.inDialog) return; state.tab=t; paintAcct(); }
function backToList(){
  closeES(); state.inDialog=false; state.peer=null;
  setAcctHeader(state.aname, state.aav, '', false, closeAcct);
  acctBottom.hidden=false; acctComposer.hidden=true;
  paintAcct();
  startStream(state.uid, 'dialogs', null);
  startPoll('dialogs', null);
}

// فتح محادثة داخل الواجهة الكاملة — تُخفى المجلدات والشريط السفلي، ويظهر شريط الارسال
async function openDialog(peer, name){
  closeES(); state.inDialog=true; state.peer=peer;
  acctFolders.hidden=true; acctBottom.hidden=true;
  const pav='/api/channel/'+state.cid+'/admin/'+state.uid+'/pavatar/'+peer+
            '?i='+encodeURIComponent(INIT)+'&name='+encodeURIComponent(name);
  setAcctHeader(name, pav, '', false, backToList);
  acctBody.innerHTML='<div class="spin"></div>'; acctComposer.hidden=true;
  let data; try{ data=await api('/api/channel/'+state.cid+'/admin/'+state.uid+'/dialog/'+peer+'/messages'); }
  catch(e){ acctBody.innerHTML='<div class="err">تعذّر جلب الرسائل.</div>'; return; }
  acctBody.innerHTML='<div class="msgs" id="msgs">'+renderMsgs(data.messages)+'</div>';
  acctComposer.hidden=false;
  scrollDown();
  startStream(state.uid, 'dialog', peer);
  startPoll('dialog', peer);
}

function renderMsgs(ms){
  if(!ms.length) return '<div class="empty"><div class="hint">لا توجد رسائل محفوظة.</div></div>';
  let h=''; for(const m of ms) h+=msgHtml(m); return h;
}
function msgHtml(m){
  let media='';
  const iq='?i='+encodeURIComponent(INIT);
  if(!m.deleted && (m.media==='video'||m.media==='gif') && m.video_url){
    media='<video class="mmedia" controls playsinline preload="metadata"'+
      (m.media_url?(' poster="'+m.media_url+iq+'"'):'')+
      ' src="'+m.video_url+iq+'"></video>';
  } else if(!m.deleted && m.media==='photo' && m.media_url){
    media='<img class="mmedia" loading="lazy" src="'+m.media_url+iq+
      '" onclick="openImg(this.src)" onerror="this.remove()">';
  } else if(!m.deleted && m.media_url && ['gif','sticker','video'].includes(m.media)){
    media='<img class="mmedia" loading="lazy" src="'+m.media_url+iq+'" onerror="this.remove()">';
    if(m.media==='video') media+='<div class="mtag">▶ فيديو</div>';
  } else if(m.media){
    media='<div class="media-tag">['+esc(m.media)+']</div>';
  }
  const body = m.text ? '<div>'+esc(m.text)+'</div>' : '';
  return '<div class="bub '+(m.out?'out':'in')+(m.deleted?' del':'')+'" data-id="'+m.id+'">'+
    (m.deleted?'<span class="mx">✕</span>':'')+media+body+'<div class="t">'+ttime(m.ts)+'</div></div>';
}
function openImg(src){
  const lb=document.getElementById('lightbox'); document.getElementById('lightboxImg').src=src;
  lb.hidden=false;
}
function closeImg(){ document.getElementById('lightbox').hidden=true; }

async function sendMsg(){
  const inp=document.getElementById('cinput'); const btn=document.getElementById('csend');
  const text=(inp.value||'').trim();
  if(!text || state.peer==null) return;
  btn.disabled=true;
  let r;
  try{
    const res=await fetch('/api/channel/'+state.cid+'/admin/'+state.uid+'/dialog/'+state.peer+'/send',
      {method:'POST',headers:{'X-Telegram-Init-Data':INIT,'Content-Type':'application/json'},
       body:JSON.stringify({text:text})});
    r=await res.json();
  }catch(e){ r={error:'network'}; }
  btn.disabled=false;
  if(r && r.ok){
    inp.value=''; inp.style.height='auto';
    const box=document.getElementById('msgs');
    if(box){
      if(box.querySelector('.empty')) box.innerHTML='';
      if(!box.querySelector('.bub[data-id="'+r.id+'"]'))
        box.insertAdjacentHTML('beforeend',
          msgHtml({id:r.id,out:1,text:text,media:null,media_url:null,video_url:null,
                   ts:r.ts||Math.floor(Date.now()/1000),deleted:false}));
      scrollDown();
    }
  } else {
    const d=(r&&r.detail)||'';
    alert(d && /write|forbidden|permission|CHAT_WRITE/i.test(d)
      ? 'لا تملك صلاحية الكتابة في هذه المحادثة.'
      : ('تعذّر الإرسال'+(d?': '+d:'.')));
  }
}
function scrollDown(){ if(document.getElementById('msgs')) acctBody.scrollTop=acctBody.scrollHeight; }

function startStream(uid, mode, peer){
  closeES();
  ES = new EventSource('/api/channel/'+state.cid+'/admin/'+uid+'/stream?i='+encodeURIComponent(INIT));
  ES.onmessage = (e)=>{
    let ev; try{ ev=JSON.parse(e.data); }catch(_){ return; }
    if(mode==='dialog' && peer!=null){
      if(ev.peer!=peer) return;
      const box=document.getElementById('msgs'); if(!box) return;
      if(ev.type==='new'){
        if(box.querySelector('.bub[data-id="'+ev.message_id+'"]')) return;   // معروضة اصلا
        if(box.querySelector('.empty')) box.innerHTML='';
        const mb='/api/channel/'+state.cid+'/admin/'+state.uid;
        const murl = (ev.media && ['photo','video','gif','sticker'].includes(ev.media))
          ? (mb+'/media/'+ev.peer+'/'+ev.message_id) : null;
        const vurl = (ev.media && ['video','gif'].includes(ev.media))
          ? (mb+'/vmedia/'+ev.peer+'/'+ev.message_id) : null;
        box.insertAdjacentHTML('beforeend', msgHtml(
          {id:ev.message_id,out:ev.out,text:ev.text,media:ev.media,media_url:murl,
           video_url:vurl,ts:ev.ts,deleted:false}));
        scrollDown();
      } else if(ev.type==='deleted'){
        const b=box.querySelector('.bub[data-id="'+ev.message_id+'"]');
        if(b && !b.classList.contains('del')){ b.classList.add('del');
          b.insertAdjacentHTML('afterbegin','<span class="mx">✕</span>'); }
      }
    } else if(mode==='dialogs'){
      if(ev.type==='new'||ev.type==='deleted'||ev.type==='dialog_update') refreshDialogs(uid);
    }
  };
  ES.onerror = ()=>{};
}
let _refreshT=null;
function refreshDialogs(uid){
  clearTimeout(_refreshT);
  _refreshT=setTimeout(async()=>{
    try{ const d=await api('/api/channel/'+state.cid+'/admin/'+uid+'/dialogs');
      state.dialogs=d.dialogs||state.dialogs; state.contacts=d.contacts||state.contacts;
      if(!state.inDialog) paintList();
    }catch(e){}
  }, 600);
}

(function(){
  const inp=document.getElementById('cinput');
  if(!inp) return;
  inp.addEventListener('input',()=>{ inp.style.height='auto';
    inp.style.height=Math.min(inp.scrollHeight,120)+'px'; });
  inp.addEventListener('keydown',(e)=>{
    if(e.key==='Enter' && !e.shiftKey){ e.preventDefault(); sendMsg(); }
  });
})();

loadChannels();
</script>
</body>
</html>
"""
