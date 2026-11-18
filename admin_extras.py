# -*- coding: utf-8 -*-
"""admin_extras.py - جلسات المشرفين (telethon) + كشف الانتحال + تقرير المخالفة
المجهولة + مراقبة الجلسات وآخر ظهور. يُستعمل من bot.py عبر AdminExtras.

حمايات: لا يُطبع في السجل session_string ولا رمز التحقق ولا كلمة المرور؛
ورسالة المستخدم الحساسة تُحذف فور استخدامها؛ ولا يُعدَّل اي اعداد خصوصية
الا بعد تسجيل الدخول بموافقة المشرف."""

import asyncio
import html
import logging
import sqlite3
import time

logger = logging.getLogger("admin_extras")

try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.errors import (
        SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError,
        PhoneNumberInvalidError, FloodWaitError, PasswordHashInvalidError,
        UnauthorizedError,
    )
    from telethon.tl.functions.channels import GetFullChannelRequest
    from telethon.tl.functions.account import (GetPrivacyRequest, SetPrivacyRequest,
                                               GetAuthorizationsRequest)
    from telethon.tl.functions.updates import GetStateRequest
    from telethon.tl.types import (
        InputPrivacyKeyStatusTimestamp,
        InputPrivacyValueAllowUsers, InputPrivacyValueDisallowUsers,
        InputPrivacyValueAllowAll, InputPrivacyValueAllowContacts,
        InputPrivacyValueDisallowAll, InputPrivacyValueDisallowContacts,
        InputPrivacyValueAllowCloseFriends,
        PrivacyValueAllowUsers, PrivacyValueDisallowUsers,
        PrivacyValueAllowAll, PrivacyValueAllowContacts, PrivacyValueDisallowAll,
        PrivacyValueDisallowContacts, PrivacyValueAllowCloseFriends,
        PeerUser,
    )
    TELETHON_OK = True
except ImportError:
    TELETHON_OK = False

try:
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.constants import ParseMode
    PTB_OK = True
except ImportError:
    PTB_OK = False


# =========================================================================
#  اعداد قاعده البيانات
# =========================================================================

_SESSIONS_SCHEMA = """CREATE TABLE IF NOT EXISTS admin_sessions (
    user_id INTEGER, channel_id INTEGER,
    session_string TEXT, phone TEXT,
    added_by INTEGER, added_at INTEGER,
    last_seen_ok INTEGER DEFAULT 0,
    PRIMARY KEY (user_id, channel_id))"""


def init_extras_db(db_path):
    """جلسة مستقلة لكل (مشرف, قناة): تسجيل دخول منفصل لكل قناة كي لا يتعارض
    تسجيل الخروج/التنزيل من قناة مع جلسات المشرف في القنوات الأخرى."""
    with sqlite3.connect(db_path) as c:
        c.execute("PRAGMA journal_mode=WAL")
        cols = [r[1] for r in c.execute("PRAGMA table_info(admin_sessions)").fetchall()]
        if cols and "channel_id" not in cols:
            # ترحيل من المخطط القديم (مفتاح user_id فقط): انسخ جلسة كل مشرف الى
            # كل قناة يديرها (bot_admins) كي تبقى جلساته القائمة عاملة.
            c.execute("ALTER TABLE admin_sessions RENAME TO _admin_sessions_old")
            c.execute(_SESSIONS_SCHEMA)
            old = c.execute("SELECT user_id, session_string, phone, added_by, added_at, "
                            "last_seen_ok FROM _admin_sessions_old").fetchall()
            for uid, ss, phone, ab, at, lso in old:
                try:
                    chans = [r[0] for r in c.execute(
                        "SELECT channel_id FROM bot_admins WHERE user_id=?", (uid,)).fetchall()]
                except sqlite3.OperationalError:
                    chans = []
                for cid in (chans or [0]):
                    c.execute("INSERT OR IGNORE INTO admin_sessions "
                              "(user_id, channel_id, session_string, phone, added_by, "
                              "added_at, last_seen_ok) VALUES (?,?,?,?,?,?,?)",
                              (uid, cid, ss, phone, ab, at, lso))
            c.execute("DROP TABLE _admin_sessions_old")
        else:
            c.execute(_SESSIONS_SCHEMA)
        c.commit()


class _SessionDB:
    def __init__(self, db_path):
        self.path = db_path

    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def save(self, uid, cid, session_string, phone, added_by):
        with self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO admin_sessions "
                "(user_id, channel_id, session_string, phone, added_by, added_at, last_seen_ok) "
                "VALUES (?,?,?,?,?,?,COALESCE("
                "(SELECT last_seen_ok FROM admin_sessions WHERE user_id=? AND channel_id=?),0))",
                (uid, cid, session_string, phone, added_by, int(time.time()), uid, cid))
            c.commit()

    def get(self, uid, cid):
        """(session_string, phone, last_seen_ok) لجلسة قناة محددة او None."""
        with self._conn() as c:
            return c.execute(
                "SELECT session_string, phone, last_seen_ok FROM admin_sessions "
                "WHERE user_id=? AND channel_id=?", (uid, cid)).fetchone()

    def get_any(self, uid):
        """(channel_id, session_string, phone, last_seen_ok) لاي جلسة للحساب (الأحدث)
        او None. تُستعمل للعمليات على مستوى الحساب (أي جلسة ترى نفس بياناته)."""
        with self._conn() as c:
            return c.execute(
                "SELECT channel_id, session_string, phone, last_seen_ok FROM admin_sessions "
                "WHERE user_id=? ORDER BY added_at DESC LIMIT 1", (uid,)).fetchone()

    def has(self, uid, cid=None):
        with self._conn() as c:
            if cid is None:
                return c.execute("SELECT 1 FROM admin_sessions WHERE user_id=? LIMIT 1",
                                 (uid,)).fetchone() is not None
            return c.execute("SELECT 1 FROM admin_sessions WHERE user_id=? AND channel_id=?",
                             (uid, cid)).fetchone() is not None

    def mark_privacy_ok(self, uid, cid=None):
        with self._conn() as c:
            if cid is None:
                c.execute("UPDATE admin_sessions SET last_seen_ok=1 WHERE user_id=?", (uid,))
            else:
                c.execute("UPDATE admin_sessions SET last_seen_ok=1 "
                          "WHERE user_id=? AND channel_id=?", (uid, cid))
            c.commit()

    def delete(self, uid, cid=None):
        with self._conn() as c:
            if cid is None:
                c.execute("DELETE FROM admin_sessions WHERE user_id=?", (uid,))
            else:
                c.execute("DELETE FROM admin_sessions WHERE user_id=? AND channel_id=?",
                          (uid, cid))
            c.commit()

    def all_ids(self):
        """حسابات المشرفين المميّزة (لعمليات على مستوى الحساب/الأرشيف)."""
        with self._conn() as c:
            return [r[0] for r in c.execute(
                "SELECT DISTINCT user_id FROM admin_sessions").fetchall()]

    def all_pairs(self):
        """كل (مشرف, قناة) لها جلسة - لفحص كل جلسة قناة على حدة في المراقب."""
        with self._conn() as c:
            return [(r[0], r[1]) for r in c.execute(
                "SELECT user_id, channel_id FROM admin_sessions").fetchall()]

    def channels_of(self, uid):
        with self._conn() as c:
            return [r[0] for r in c.execute(
                "SELECT channel_id FROM admin_sessions WHERE user_id=?", (uid,)).fetchall()]


# =========================================================================
#  تحويل قواعد الخصوصيه من output الى input
# =========================================================================

async def _output_to_input_rules(client, current_rules, assistant_uid, assistant_entity):
    """يحوّل قواعد الخصوصيه القادمه من GetPrivacyRequest الى صيغتها
    المدخله لـ SetPrivacyRequest مع تضمين الحساب المساعد ضمن المسموحين
    وحذفه من قائمه المحظورين ان وُجد. يحافظ على بقيه القواعد كما هي."""
    if not TELETHON_OK:
        return None
    new_rules = []
    already_added = False
    for r in current_rules:
        if isinstance(r, PrivacyValueAllowAll):
            new_rules.append(InputPrivacyValueAllowAll())
        elif isinstance(r, PrivacyValueAllowContacts):
            new_rules.append(InputPrivacyValueAllowContacts())
        elif isinstance(r, PrivacyValueAllowCloseFriends):
            new_rules.append(InputPrivacyValueAllowCloseFriends())
        elif isinstance(r, PrivacyValueDisallowAll):
            new_rules.append(InputPrivacyValueDisallowAll())
        elif isinstance(r, PrivacyValueDisallowContacts):
            new_rules.append(InputPrivacyValueDisallowContacts())
        elif isinstance(r, PrivacyValueAllowUsers):
            # نبني القائمه ونضمن وجود المساعد فيها (بكيانه المحلول مسبقا)
            entities = [assistant_entity]
            for u in (r.users or []):
                if u == assistant_uid:
                    continue
                try:
                    entities.append(await client.get_input_entity(u))
                except Exception:
                    pass
            new_rules.append(InputPrivacyValueAllowUsers(users=entities))
            already_added = True
        elif isinstance(r, PrivacyValueDisallowUsers):
            # احذف المساعد من قائمه المحظورين ان وُجد، دون لمس البقيه
            entities = []
            for u in (r.users or []):
                if u == assistant_uid:
                    continue
                try:
                    entities.append(await client.get_input_entity(u))
                except Exception:
                    pass
            if entities:
                new_rules.append(InputPrivacyValueDisallowUsers(users=entities))
    if not already_added:
        new_rules.insert(0, InputPrivacyValueAllowUsers(users=[assistant_entity]))
    return new_rules


# =========================================================================
#  اداره الجلسات
# =========================================================================

def _name_link(user):
    """رابط باسم الحساب فقط، بلا اظهار المعرّف الرقمي.
    يعمل حتى لو لم يكن للمستخدم يوزر (tg://user?id=... داخل الرابط)."""
    try:
        uid = user.id
        name = getattr(user, "full_name", None) or getattr(user, "first_name", None) or "مستخدم"
    except Exception:
        uid = user
        name = "مستخدم"
    return '<a href="tg://user?id=%s">%s</a>' % (uid, html.escape(str(name)))


def _pyro_status_text(user):
    """حالة اتصال من كائن مستخدم pyrogram."""
    st = getattr(user, "status", None)
    last = getattr(user, "last_online_date", None)
    name = getattr(st, "name", None) or (str(st) if st is not None else None)
    if name == "ONLINE":
        return "متصل الآن"
    if name == "OFFLINE":
        if last:
            try:
                return "آخر ظهور " + last.strftime("%d/%m %I:%M %p")
            except Exception:
                pass
        return "غير متصل"
    return {
        "RECENTLY":  "ظهور مؤخراً",
        "LAST_WEEK": "خلال الاسبوع",
        "LAST_MONTH":"خلال الشهر",
        "LONG_AGO":  "مخفي",
    }.get(name, "غير معروف")


class AdminExtras:
    """مدير جلسات المشرفين + وظائف الكشف والتقرير."""

    def __init__(self, api_id, api_hash, db_path, assistant_client_provider,
                 assistant_username=None, on_login_promote=None, on_logout_demote=None):
        self.api_id = int(api_id)
        self.api_hash = str(api_hash).strip()
        self.db = _SessionDB(db_path)
        # دوال ربط مع bot.py:
        #  on_login_promote(bot, cid, uid, name) بعد تسجيل دخول مشرف جديد -> ترقيته
        #  on_logout_demote(bot, cid, uid)        عند اخراج الجلسة -> تنزيله من القناة
        self.on_login_promote = on_login_promote
        self.on_logout_demote = on_logout_demote
        # دالة تُرجع (client, err) للحساب المساعد الحالي في bot.py
        self._get_assistant = assistant_client_provider
        # معرّف الحساب المساعد (@eyeofhellAssistant) - حلّه بالـ username
        # اكثر متانه لان حساب المشرف قد لا يكون تعامل معه من قبل.
        self.assistant_username = (assistant_username or "").lstrip("@") or None
        self._clients = {}                # (uid, cid) -> TelegramClient
        self._clients_lock = asyncio.Lock()
        self._pending_login = {}          # requester_uid -> dict(phone, hash, client)

    # ---------- ادوات داخليه ----------

    async def _new_client(self, session_string=None):
        client = TelegramClient(StringSession(session_string), self.api_id, self.api_hash,
                                device_model="eye of hell", app_version="1",
                                system_version="1")
        await client.connect()
        return client

    def _resolve_session(self, uid, cid):
        """(key_cid, session_string) للجلسة المناسبة: جلسة القناة المحددة ان وُجدت،
        والا اي جلسة للحساب (كلها ترى نفس بيانات الحساب). او (None, None)."""
        if cid is not None:
            row = self.db.get(uid, cid)
            if row:
                return cid, row[0]
        anyrow = self.db.get_any(uid)
        if anyrow:
            return anyrow[0], anyrow[1]
        return None, None

    async def get_client(self, uid, cid=None):
        """عميل telethon لجلسة (المشرف, القناة). مع cid=None يستعمل اي جلسة للحساب
        (كافٍ للعمليات على مستوى الحساب: قراءة الخاص/الأرشفة/كشف الانتحال)."""
        if not TELETHON_OK:
            return None
        key_cid, session_string = self._resolve_session(uid, cid)
        if session_string is None:
            return None
        key = (uid, key_cid)
        async with self._clients_lock:
            existing = self._clients.get(key)
            if existing is not None:
                try:
                    if existing.is_connected():
                        return existing
                    await existing.connect()
                    return existing
                except Exception:
                    self._clients.pop(key, None)
            try:
                client = await self._new_client(session_string)
                if not await client.is_user_authorized():
                    await client.disconnect()
                    # لا نحذف الجلسة هنا: مراقب الجلسات وحده يكتشف الاخراج
                    # فيُنزّل المشرف من هذه القناة ويُشعر ثم يحذف.
                    logger.warning("admin uid=%s cid=%s session unauthorized (kept for monitor)"
                                   % (uid, key_cid))
                    return None
            except Exception as e:
                logger.warning("admin uid=%s cid=%s connect failed: %s" % (uid, key_cid, e))
                return None
            self._clients[key] = client
            return client

    async def session_alive(self, uid, cid):
        """هل جلسة (المشرف, القناة) ما زالت صالحة؟
        True = صالحة. False = أُخرجت/أُلغيت او لا جلسة. None = تعذّر التحقق مؤقتا."""
        if not TELETHON_OK:
            return None
        row = self.db.get(uid, cid)
        if not row:
            return False
        key = (uid, cid)
        async with self._clients_lock:
            client = self._clients.get(key)
        try:
            if client is None or not client.is_connected():
                client = await self._new_client(row[0])
                async with self._clients_lock:
                    self._clients[key] = client
            # get_me() يبتلع UnauthorizedError بصمت؛ GetState يرمي فعلا عند الالغاء.
            await client(GetStateRequest())
            return True
        except UnauthorizedError:
            return False
        except Exception as e:
            logger.warning("session_alive uid=%s cid=%s: %s" % (uid, cid, e))
            return None

    async def get_authorizations(self, uid):
        """تفويضات (اجهزة) حساب المشرف عبر account.getAuthorizations (على مستوى
        الحساب، اي جلسة تكفي). يرجع [(hash, device, location, is_current)] او None."""
        if not TELETHON_OK:
            return None
        anyrow = self.db.get_any(uid)
        if not anyrow:
            return None
        key = (uid, anyrow[0])
        session_string = anyrow[1]
        async with self._clients_lock:
            client = self._clients.get(key)
        try:
            if client is None or not client.is_connected():
                client = await self._new_client(session_string)
                async with self._clients_lock:
                    self._clients[key] = client
            res = await client(GetAuthorizationsRequest())
        except UnauthorizedError:
            return None
        except Exception as e:
            logger.warning("get_authorizations uid=%s: %s" % (uid, e))
            return None
        out = []
        for a in getattr(res, "authorizations", []) or []:
            device = " · ".join(x for x in (getattr(a, "device_model", None),
                                            getattr(a, "platform", None),
                                            getattr(a, "app_name", None)) if x)
            location = " · ".join(x for x in (getattr(a, "country", None),
                                              getattr(a, "region", None)) if x)
            out.append((str(getattr(a, "hash", 0)), device or "جهاز",
                        location or None, bool(getattr(a, "current", False))))
        return out

    async def stop_one(self, uid, cid=None):
        """يُغلق عميل جلسة قناة محددة (cid) او كل عملاء الحساب (cid=None)."""
        async with self._clients_lock:
            if cid is None:
                keys = [k for k in list(self._clients) if k[0] == uid]
            else:
                keys = [(uid, cid)]
            clients = [self._clients.pop(k, None) for k in keys]
        for c in clients:
            if c is not None:
                try:
                    await c.disconnect()
                except Exception:
                    pass

    async def stop_all(self):
        async with self._clients_lock:
            clients = list(self._clients.values())
            self._clients.clear()
        for c in clients:
            try:
                await c.disconnect()
            except Exception:
                pass
        # الغِ اي login معلّق
        pending = list(self._pending_login.values())
        self._pending_login.clear()
        for p in pending:
            c = p.get("client")
            if c is not None:
                try:
                    await c.disconnect()
                except Exception:
                    pass

    def has_session(self, uid, cid=None):
        return self.db.has(uid, cid)

    # ---------- تدفق تسجيل الدخول ----------

    def has_pending(self, requester_uid):
        return requester_uid in self._pending_login

    async def cancel_pending(self, requester_uid):
        p = self._pending_login.pop(requester_uid, None)
        if p is not None:
            c = p.get("client")
            if c is not None:
                try:
                    await c.disconnect()
                except Exception:
                    pass

    async def begin_login(self, requester_uid, phone, target_uid=None, target_cid=None):
        """يبدأ ارسال رمز التحقق. target_uid للتأكد ان الحساب هو المشرف المستهدف،
        و target_cid القناة التي تُخزَّن الجلسة الجديدة تحتها (جلسة مستقلة لكل قناة)."""
        if not TELETHON_OK:
            return False, "مكتبة telethon غير مثبتة"
        await self.cancel_pending(requester_uid)
        try:
            client = await self._new_client()
        except Exception as e:
            return False, f"تعذر بدء العميل: {e}"
        try:
            sent = await client.send_code_request(phone)
        except PhoneNumberInvalidError:
            await client.disconnect()
            return False, "رقم الهاتف غير صالح"
        except FloodWaitError as e:
            await client.disconnect()
            return False, f"انتظار {e.seconds}ث بسبب كثرة الطلبات"
        except Exception as e:
            await client.disconnect()
            return False, f"تعذر ارسال الرمز: {e}"
        self._pending_login[requester_uid] = {
            "phone": phone,
            "phone_code_hash": sent.phone_code_hash,
            "client": client,
            "target_uid": target_uid,
            "target_cid": target_cid,
        }
        return True, "تم ارسال رمز التحقق. ارسله هنا."

    async def submit_code(self, requester_uid, code):
        """يرجع (ok, message, needs_2fa) او (ok, me_object, False) عند النجاح."""
        pending = self._pending_login.get(requester_uid)
        if not pending:
            return False, "لا توجد جلسة تسجيل قيد الانتظار.", False
        client = pending["client"]
        try:
            await client.sign_in(pending["phone"], code,
                                 phone_code_hash=pending["phone_code_hash"])
        except SessionPasswordNeededError:
            return False, "التحقق بخطوتين مفعّل - ارسل كلمة المرور.", True
        except PhoneCodeInvalidError:
            return False, "الرمز غير صحيح. حاول مجدداً.", False
        except PhoneCodeExpiredError:
            await self.cancel_pending(requester_uid)
            return False, "انتهت صلاحية الرمز - ابدأ من جديد.", False
        except Exception as e:
            return False, f"فشل الدخول: {e}", False
        return await self._finalize(requester_uid)

    async def submit_password(self, requester_uid, password):
        pending = self._pending_login.get(requester_uid)
        if not pending:
            return False, "لا توجد جلسة تسجيل قيد الانتظار.", False
        client = pending["client"]
        try:
            await client.sign_in(password=password)
        except PasswordHashInvalidError:
            return False, "كلمة المرور غير صحيحة. حاول مجدداً.", True
        except Exception as e:
            return False, f"فشل التحقق بخطوتين: {e}", True
        return await self._finalize(requester_uid)

    async def _finalize(self, requester_uid):
        pending = self._pending_login.get(requester_uid, {})
        client = pending.get("client")
        target_uid = pending.get("target_uid")
        target_cid = pending.get("target_cid")
        if client is None:
            return False, "خطأ داخلي.", False
        try:
            me = await client.get_me()
        except Exception as e:
            return False, f"تعذر قراءة الحساب: {e}", False
        real_uid = me.id
        if target_uid and real_uid != target_uid:
            self._pending_login.pop(requester_uid, None)
            try:
                await client.disconnect()
            except Exception:
                pass
            return (False,
                    f"الحساب الذي سُجّل دخوله ({real_uid}) لا يطابق المشرف المستهدف "
                    f"({target_uid}). اُلغي التسجيل.", False)
        key_cid = target_cid if target_cid is not None else 0
        session_string = StringSession.save(client.session)
        # جلسة مستقلة تحت هذه القناة وحدها (لا تمس جلسات القنوات الأخرى للحساب).
        self.db.save(real_uid, key_cid, session_string, getattr(me, "phone", None), requester_uid)
        key = (real_uid, key_cid)
        async with self._clients_lock:
            old = self._clients.pop(key, None)
            if old is not None and old is not client:
                try:
                    await old.disconnect()
                except Exception:
                    pass
            self._clients[key] = client
        self._pending_login.pop(requester_uid, None)
        return True, me, False

    # ---------- استعلامات ----------

    async def get_default_send_as(self, uid, cid):
        """يرجع dict: {kind: 'self'|'anon'|'other_channel', title, username, peer_id}."""
        client = await self.get_client(uid, cid)
        if client is None:
            return None
        try:
            full = await client(GetFullChannelRequest(cid))
        except Exception as e:
            logger.warning(f"get_default_send_as uid={uid} cid={cid}: {e}")
            return None
        default = getattr(full.full_chat, "default_send_as", None)
        if default is None or isinstance(default, PeerUser):
            return {"kind": "self"}
        # PeerChannel
        peer_id = getattr(default, "channel_id", None)
        title, uname = None, None
        for ch in getattr(full, "chats", []) or []:
            if getattr(ch, "id", None) == peer_id:
                title = getattr(ch, "title", None)
                uname = getattr(ch, "username", None)
                break
        # cid يأتي بصيغه -100xxxxxxxxxx غالبا؛ الـ peer_id مجرّد
        raw_cid = int(str(cid)[4:]) if str(cid).startswith("-100") else abs(cid)
        if peer_id == raw_cid:
            return {"kind": "anon", "peer_id": peer_id, "title": title, "username": uname}
        return {"kind": "other_channel", "peer_id": peer_id, "title": title, "username": uname}

    async def add_privacy_exception_for(self, uid, assistant_uid, assistant_username=None):
        """يضيف الحساب المساعد لاستثناء "اخر ظهور" فقط. لا يوسّع القاعده:
        لو كانت "لا احد" تبقى "لا احد" مع استثناء المساعد وحده."""
        client = await self.get_client(uid)
        if client is None:
            return False, "لا توجد جلسه لهذا المشرف"
        try:
            current = await client(GetPrivacyRequest(key=InputPrivacyKeyStatusTimestamp()))
        except Exception as e:
            return False, f"تعذر قراءه الخصوصيه: {e}"
        assistant_entity = None
        uname = (assistant_username or self.assistant_username)
        targets = ([uname] if uname else []) + [assistant_uid]
        for t in targets:
            try:
                assistant_entity = await client.get_input_entity(t)
                break
            except Exception:
                continue
        if assistant_entity is None:
            return False, "تعذر التعرف على الحساب المساعد"
        try:
            new_rules = await _output_to_input_rules(client, current.rules,
                                                    assistant_uid, assistant_entity)
        except Exception as e:
            return False, f"خطأ في تحويل القواعد: {e}"
        if new_rules is None:
            return False, "لا يمكن تعديل الخصوصيه (telethon مفقود)"
        try:
            await client(SetPrivacyRequest(key=InputPrivacyKeyStatusTimestamp(), rules=new_rules))
        except Exception as e:
            return False, f"تعذر حفظ الخصوصيه: {e}"
        self.db.mark_privacy_ok(uid)
        return True, "تم"

    async def get_status_map(self, cid):
        """{uid: نص حالة الاتصال} لكل مشرفي القناة عبر الحساب المساعد (pyrogram).
        نمر عبر القناة (get_chat_members) فلا يقع PEER_ID_INVALID."""
        out = {}
        if self._get_assistant is None:
            return out
        try:
            client, _err = await self._get_assistant()
        except Exception:
            client = None
        if client is None:
            return out
        weak = []
        try:
            from pyrogram.enums import ChatMembersFilter
            async for m in client.get_chat_members(cid, filter=ChatMembersFilter.ADMINISTRATORS):
                u = getattr(m, "user", None)
                if u is None:
                    continue
                txt = _pyro_status_text(u)
                out[u.id] = txt
                if txt in ("غير معروف", "مخفي"):
                    weak.append(u.id)
        except Exception as e:
            logger.warning("status map cid=%s: %s" % (cid, e))
        # get_chat_members قد لا يملأ الحالة؛ get_users يملؤها بدقة بعد ان صار
        # المساعد يعرف المستخدمين (رآهم في القناة) + مسموحا له باخر ظهورهم.
        if weak:
            try:
                users = await client.get_users(weak)
                for u in (users if isinstance(users, list) else [users]):
                    out[u.id] = _pyro_status_text(u)
            except Exception as e:
                logger.warning("status get_users cid=%s: %s" % (cid, e))
        return out

    # ---------- تقرير المخالفه المجهوله ----------

    async def build_anon_report(self, bot, cid, msg, admins, editor_uid=None, editor_name=None):
        """تقرير: لكل مشرف التوقيع الحالي + اخر ظهور، او "الجلسة غير موجودة".
        بلا اي معرّفات رقمية ظاهرة - الاسم مدموج برابط الحساب فقط."""
        lines = []
        # اسماء المشرفين (لكشف انتحال الاسم عبر قناة)
        admin_names = {}
        for a in admins:
            if not getattr(a.user, "is_bot", False):
                admin_names[(a.user.full_name or "").strip().lower()] = a.user

        sender_chat = getattr(msg, "sender_chat", None) if msg else None
        if sender_chat is not None:
            title = html.escape(sender_chat.title or "")
            uname = getattr(sender_chat, "username", None)
            if uname:
                ch_link = '<a href="https://t.me/%s"><b>%s</b></a>' % (uname, title)
            else:
                ch_link = "<b>%s</b>" % title
            info = "<b>الرسالة باسم قناة</b>\nالقناة: " + ch_link
            # هل اسم القناة يطابق اسم احد المشرفين؟ (انتحال)
            victim = admin_names.get((sender_chat.title or "").strip().lower())
            if victim is not None:
                info += ("\n⚠️ اسم هذه القناة يطابق اسم المشرف: "
                         + _name_link(victim))
            lines.append(info + "\n")

        if editor_uid:
            lines.append("من كتَبَ الرسالة فعلا (سجل الادارة): "
                         + '<a href="tg://user?id=%s">%s</a>' % (editor_uid, html.escape(editor_name or "مستخدم"))
                         + "\n")

        # خريطة حالات الاتصال (استدعاء واحد)
        try:
            status_map = await self.get_status_map(cid)
        except Exception:
            status_map = {}

        lines.append("\n<b>حالة المشرفين:</b>")
        for a in admins:
            if getattr(a.user, "is_bot", False):
                continue
            uid = a.user.id
            link = _name_link(a.user)
            if not self.db.has(uid):
                lines.append("\n• " + link + "\n  الجلسة: <b>غير موجودة</b>")
                continue
            sig = None
            try:
                sig = await self.get_default_send_as(uid, cid)
            except Exception:
                sig = None
            if not sig:
                sig_txt = "غير معروف"
            elif sig["kind"] == "self":
                sig_txt = "يكتب باسم حسابه الشخصي"
            elif sig["kind"] == "anon":
                sig_txt = "يكتب باسم القناة (توقيع مخفي)"
            elif sig["kind"] == "other_channel":
                sig_txt = "يكتب باسم قناة اخرى: <b>%s</b>" % html.escape(sig.get("title") or "قناة")
            else:
                sig_txt = "غير معروف"
            seen = status_map.get(uid, "غير معروف")
            lines.append("\n• " + link + "\n  التوقيع: " + sig_txt + "\n  آخر ظهور: " + seen)
        return "\n".join(lines)

    async def detect_culprit_by_signature(self, cid, sender_chat, admins):
        """اذا كانت الرساله باسم قناة (sender_chat) غير القناة الحاليه،
        ابحث عن مشرف توقيعه الحالي (default_send_as) = تلك القناة نفسها.
        يرجع (uid, name) او None.

        ملاحظه: التوقيع المخفي (باسم القناة نفسها) غامض - قد يكون اي مشرف
        فعّل النشر المجهول - فلا نحدّد احدا فيه ونترك التجريد الشامل.
        """
        if sender_chat is None:
            return None
        target_id = getattr(sender_chat, "id", None)
        if target_id is None:
            return None
        raw_target = int(str(target_id)[4:]) if str(target_id).startswith("-100") else abs(target_id)
        raw_cid = int(str(cid)[4:]) if str(cid).startswith("-100") else abs(cid)
        if raw_target == raw_cid:
            return None   # توقيع مخفي باسم القناة نفسها - غامض
        st = (getattr(sender_chat, "title", None) or "").strip().lower()
        for a in admins:
            if getattr(a.user, "is_bot", False):
                continue
            uid = a.user.id
            if not self.db.has(uid):
                continue
            try:
                sig = await self.get_default_send_as(uid, cid)
            except Exception:
                sig = None
            if not sig or sig.get("kind") != "other_channel":
                continue
            if sig.get("peer_id") == raw_target:
                return uid, a.user.full_name
            t = (sig.get("title") or "").strip().lower()
            if t and t == st:
                return uid, a.user.full_name
        return None

    async def channel_creator_id(self, sender_chat):
        """يحاول ايجاد منشئ (creator) القناة/الكروب المرسِل عبر اي جلسة متاحة.

        يكشف الانتحال حتى لو لم يكن للجاني جلسة في البوت: من انشأ قناة/كروب
        عام باسم مشرف آخر ثم كتب باسمها. يرجع user_id للمنشئ او None."""
        if not TELETHON_OK or sender_chat is None:
            return None
        try:
            from telethon.tl.functions.channels import GetParticipantsRequest
            from telethon.tl.types import (ChannelParticipantsAdmins,
                                           ChannelParticipantCreator)
        except Exception:
            return None
        # اليوزر اضمن للوصول (عام) ثم المعرّف الرقمي
        ref = getattr(sender_chat, "username", None) or getattr(sender_chat, "id", None)
        if not ref:
            return None
        for uid in list(self.db.all_ids()):
            try:
                client = await self.get_client(uid)
            except Exception:
                client = None
            if client is None:
                continue
            try:
                entity = await client.get_entity(ref)
            except Exception:
                continue
            try:
                res = await client(GetParticipantsRequest(
                    entity, ChannelParticipantsAdmins(), 0, 200, 0))
                for p in getattr(res, "participants", []) or []:
                    if isinstance(p, ChannelParticipantCreator):
                        return p.user_id
            except Exception:
                continue
        return None

    async def find_impersonator_by_owned_channel(self, name, admin_uids):
        """يدخل حسابات المشرفين (جلساتهم) ويبحث عمّن *يملك/أنشأ* قناة او كروب
        بنفس الاسم المنتحَل (اسم الضحية). صاحب تلك القناة هو المنتحِل.
        يرجع user_id للمنتحِل او None."""
        if not TELETHON_OK or not name:
            return None
        target = str(name).strip().lower()
        if not target:
            return None
        try:
            from telethon.utils import get_display_name
        except Exception:
            return None
        for uid in admin_uids:
            try:
                client = await self.get_client(uid)
            except Exception:
                client = None
            if client is None:
                continue
            try:
                async for d in client.iter_dialogs(limit=200):
                    ent = d.entity
                    if not (getattr(ent, "broadcast", False) or getattr(ent, "megagroup", False)):
                        continue
                    if not getattr(ent, "creator", False):
                        continue   # لا يملكها هذا المشرف
                    title = (get_display_name(ent) or getattr(ent, "title", "") or "").strip().lower()
                    if title == target:
                        return uid
            except Exception as e:
                logger.warning("find impersonator uid=%s: %s" % (uid, e))
                continue
        return None

    # ---------- المعالجات ----------

    async def handle_callback(self, q, ans, ctx):
        """يعالج ازرار ADMSESS. يرجع True لو استهلك الحدث."""
        d = q.data or ""
        if not d.startswith("ADMSESS:"):
            return False
        parts = d.split(":")
        action = parts[1] if len(parts) > 1 else ""
        cid = _to_int(parts[2]) if len(parts) > 2 else None
        tuid = _to_int(parts[3]) if len(parts) > 3 else None
        uid = q.from_user.id

        async def _who(_tuid):
            try:
                m = await ctx.bot.get_chat_member(cid, _tuid)
                return _name_link(m.user)
            except Exception:
                return _name_link(type("U", (), {"id": _tuid, "full_name": "مشرف"})())

        if action == "OPEN" and cid and tuid:
            has = self.has_session(tuid, cid)
            who = await _who(tuid)
            body = (f"<b>جلسة المشرف {who} في هذه القناة</b>\n\n"
                    f"الحاله: {'<b>موجوده</b>' if has else '<b>غير موجوده</b>'}")
            rows = []
            if has:
                rows.append([InlineKeyboardButton("خروج (حذف الجلسه)",
                            callback_data=f"ADMSESS:LOGOUT:{cid}:{tuid}", style="danger")])
                rows.append([InlineKeyboardButton("اضف المساعد لاستثناء اخر ظهور",
                            callback_data=f"ADMSESS:PRIV:{cid}:{tuid}", style="success")])
            else:
                rows.append([InlineKeyboardButton("تسجيل الدخول",
                            callback_data=f"ADMSESS:LOGIN:{cid}:{tuid}", style="success")])
            rows.append([InlineKeyboardButton("رجوع",
                        callback_data=f"ADM_DETAIL:{cid}:{tuid}")])
            await q.edit_message_text(body, parse_mode=ParseMode.HTML,
                                      reply_markup=InlineKeyboardMarkup(rows))
            await ans()
            return True

        if action == "LOGIN" and cid and tuid:
            ctx.user_data["flow"] = "admsess_phone"
            ctx.user_data["admsess_cid"] = cid
            ctx.user_data["admsess_tuid"] = tuid
            who = await _who(tuid)
            await q.edit_message_text(
                f"<b>تسجيل دخول للمشرف {who}</b>\n\n"
                "ارسل رقم هاتف هذا المشرف مع رمز الدوله (مثال: <code>+9647...</code>).\n\n"
                "⚠️ لا تُدخل ارقام حسابات لا تملك اذنا صريحا باستعمالها.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                    "الغاء", callback_data=f"ADMSESS:CANCEL:{cid}:{tuid}", style="danger")]]))
            await ans()
            return True

        if action == "CANCEL":
            await self.cancel_pending(uid)
            for k in ("flow", "admsess_cid", "admsess_tuid", "admsess_promote"):
                ctx.user_data.pop(k, None)
            body = "تم الالغاء."
            rows = [[InlineKeyboardButton("رجوع",
                    callback_data=f"CH:{cid}" if cid else "BACK_MAIN")]]
            await q.edit_message_text(body,
                                      reply_markup=InlineKeyboardMarkup(rows))
            await ans("تم الالغاء.")
            return True

        if action == "LOGOUT" and cid and tuid:
            # جلسة هذه القناة وحدها تُخرَج؛ جلسات المشرف في القنوات الأخرى تبقى.
            await self.stop_one(tuid, cid)
            self.db.delete(tuid, cid)
            who = await _who(tuid)
            # اخراج الجلسة = تنزيله من القناة (النمط المطلوب)
            if self.on_logout_demote is not None:
                try:
                    await self.on_logout_demote(ctx.bot, cid, tuid)
                except Exception as e:
                    logger.warning("on_logout_demote cid=%s uid=%s: %s" % (cid, tuid, e))
            await ans("تم اخراج الجلسة وتنزيله من القناة.", alert=True)
            body = f"تم اخراج جلسة المشرف {who} وتنزيله من القناة."
            rows = [[InlineKeyboardButton("لوحة القناة",
                    callback_data=f"CH:{cid}", style="primary")]]
            await q.edit_message_text(body, parse_mode=ParseMode.HTML,
                                      reply_markup=InlineKeyboardMarkup(rows))
            return True

        if action == "PRIV" and cid and tuid:
            # يحتاج معرف الحساب المساعد؛ نطلبه من bot.py عبر ctx.user_data
            assistant_uid = ctx.user_data.get("_admsess_assistant_uid")
            if assistant_uid is None:
                # حاول اخذه من الحساب المساعد الحالي
                try:
                    client, _ = await self._get_assistant()
                    if client is not None:
                        me = await client.get_me()
                        assistant_uid = me.id
                except Exception:
                    assistant_uid = None
            if not assistant_uid:
                await ans("الحساب المساعد غير مهيّأ.", alert=True)
                return True
            ok, message = await self.add_privacy_exception_for(
                tuid, assistant_uid, self.assistant_username)
            await ans("تم." if ok else f"فشل: {message}", alert=True)
            return True

        return False

    async def handle_private_message(self, update, ctx):
        """يعالج رسائل تدفق تسجيل الدخول. يرجع True لو استهلك."""
        msg = update.message
        if not msg:
            return False
        flow = ctx.user_data.get("flow", "")
        if not flow.startswith("admsess_"):
            return False
        uid = update.effective_user.id
        text = (msg.text or "").strip()
        tuid = ctx.user_data.get("admsess_tuid")
        session_cid = ctx.user_data.get("admsess_cid")   # القناة التي تُخزَّن الجلسة تحتها
        promote_cid = session_cid if ctx.user_data.get("admsess_promote") else None

        # نحذف رساله المستخدم الحساسه (رقم/رمز/كلمه مرور)
        async def _del_user_msg():
            try:
                await msg.delete()
            except Exception:
                pass

        if flow == "admsess_phone":
            await _del_user_msg()
            if not text.startswith("+") or len(text) < 7:
                await msg.reply_text("ارسل الرقم بصيغه دوليه تبدأ بـ +.")
                return True
            ok, m = await self.begin_login(uid, text, target_uid=tuid, target_cid=session_cid)
            if not ok:
                await msg.reply_text(f"فشل: {m}")
                return True
            ctx.user_data["flow"] = "admsess_code"
            await msg.reply_text(
                "تم ارسال رمز التحقق. الصق الرمز هنا كما وصل، بدون مسافات.")
            return True

        if flow == "admsess_code":
            await _del_user_msg()
            code = text.replace(" ", "")
            ok, res, needs_2fa = await self.submit_code(uid, code)
            if ok:
                await self._after_login_success(msg, ctx, res, promote_cid)
                return True
            if needs_2fa:
                ctx.user_data["flow"] = "admsess_2fa"
                await msg.reply_text(res)
                return True
            await msg.reply_text(res)
            return True

        if flow == "admsess_2fa":
            await _del_user_msg()
            ok, res, needs_2fa = await self.submit_password(uid, text)
            if ok:
                await self._after_login_success(msg, ctx, res, promote_cid)
                return True
            await msg.reply_text(res)
            return True

        return False

    async def _after_login_success(self, msg, ctx, me, promote_cid):
        name = (getattr(me, "first_name", "") or "") + " " + (getattr(me, "last_name", "") or "")
        name = name.strip() or (getattr(me, "username", None) or "مشرف")
        link = '<a href="tg://user?id=%s">%s</a>' % (me.id, html.escape(name))
        for k in ("flow", "admsess_cid", "admsess_tuid", "admsess_promote"):
            ctx.user_data.pop(k, None)
        # فور الدخول: استثناء الحساب المساعد في (اخر ظهور) كي يظهر اخر ظهور المشرف فورا
        await self._auto_last_seen_exception(me.id)
        if promote_cid and self.on_login_promote is not None:
            try:
                await self.on_login_promote(ctx.bot, promote_cid, me.id, name)
                # بلا رسالة «تم تسجيل الدخول وترقيته» - يصل المالك اشعار الترقية.
            except Exception as e:
                await msg.reply_text("تم تسجيل الدخول لكن تعذرت الترقية: " + html.escape(str(e)))
        else:
            await msg.reply_text("تم تسجيل الدخول بنجاح لحساب " + link + ".",
                                 parse_mode=ParseMode.HTML)

    async def _auto_last_seen_exception(self, admin_uid):
        """يضيف الحساب المساعد الى استثناء (اخر ظهور) لدى المشرف فور الدخول،
        فيتمكّن البوت من قراءة اخر ظهوره فورا. لا يوسّع الخصوصيه العامه."""
        if self._get_assistant is None:
            return
        try:
            client, _ = await self._get_assistant()
            if client is None:
                return
            me_asst = await client.get_me()
            ok, message = await self.add_privacy_exception_for(
                admin_uid, me_asst.id, self.assistant_username)
            if not ok:
                logger.warning("auto last-seen uid=%s: %s" % (admin_uid, message))
        except Exception as e:
            logger.warning("auto last-seen exception uid=%s: %s" % (admin_uid, e))


def _to_int(s):
    try:
        return int(s)
    except (TypeError, ValueError):
        return None
