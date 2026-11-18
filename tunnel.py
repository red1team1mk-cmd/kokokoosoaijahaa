# -*- coding: utf-8 -*-
"""
tunnel.py  --  رابط عام للـ Mini App، يعمل حيث تُحظر خدمات اخرى (مثل ngrok).
================================================================
المزوّد الافتراضي: Cloudflare (اوسع انتشارا واصعب حظرا). طريقتان:

  A) نفق Cloudflare السريع (بلا حساب ولا نطاق) — الاسهل:
     - نزّل cloudflared.exe وضعه بجانب هذا الملف.
     - في run.bat:  set TUNNEL_PROVIDER=cloudflared
     - عند التشغيل ينشئ رابطا مثل https://xxxx.trycloudflare.com، والبوت
       يلتقطه تلقائيا ويحدّث زر القائمة، فلا يهم انه يتغيّر بين التشغيلات.

  B) نفق Cloudflare مسمّى (رابط ثابت — يحتاج نطاقك على Cloudflare):
     - في run.bat:  set TUNNEL_PROVIDER=cloudflared
                    set CLOUDFLARED_TOKEN=<توكن النفق>
                    set CLOUDFLARED_HOSTNAME=app.example.com

  C) ngrok (اذا لم يكن محظورا عندك):
     - set TUNNEL_PROVIDER=ngrok  + NGROK_DOMAIN (+ سجّل authtoken مرة).

اذا لم يُضبط TUNNEL_PROVIDER، لا يعمل نفق (الموقع محلي فقط).
"""

import logging
import os
import re
import shutil
import subprocess
import threading

logger = logging.getLogger("tunnel")

_proc = None
_TRYCF_RE = re.compile(r"https://[a-z0-9][a-z0-9\-]*\.trycloudflare\.com")


def _find(name):
    here = os.path.dirname(os.path.abspath(__file__))
    for cand in (os.path.join(here, name), os.path.join(here, name + ".exe")):
        if os.path.exists(cand):
            return cand
    return shutil.which(name) or shutil.which(name + ".exe")


def _extract_trycloudflare_url(lines):
    """يلتقط رابط trycloudflare من اسطر مخرجات cloudflared (قابل للاختبار)."""
    for line in lines:
        m = _TRYCF_RE.search(line or "")
        if m:
            return m.group(0)
    return ""


def public_url():
    """الرابط المتوقّع من الاعداد بلا تشغيل (للنفق المسمّى/ngrok فقط)."""
    provider = (os.getenv("TUNNEL_PROVIDER") or "").strip().lower()
    if provider == "ngrok":
        dom = (os.getenv("NGROK_DOMAIN") or "").strip()
        return f"https://{dom}" if dom else ""
    if provider == "cloudflared":
        host = (os.getenv("CLOUDFLARED_HOSTNAME") or "").strip()
        return f"https://{host}" if host else ""   # فارغ للنفق السريع (يُلتقط لاحقا)
    return ""


_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def _start_cloudflared_quick(port):
    exe = _find("cloudflared")
    if not exe:
        logger.warning("cloudflared.exe غير موجود - ضعه بجانب tunnel.py او في PATH.")
        return ""
    global _proc
    try:
        _proc = subprocess.Popen(
            [exe, "tunnel", "--no-autoupdate", "--url", f"http://localhost:{port}"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            encoding="utf-8", errors="ignore", bufsize=1, creationflags=_NO_WINDOW)
    except Exception as e:
        logger.warning("تعذّر تشغيل cloudflared: %s", e)
        return ""
    holder = {}
    ev = threading.Event()

    def _reader():
        try:
            for line in _proc.stdout:          # يستمر بالتفريغ حتى لا يمتلئ الـ pipe
                if "url" not in holder:
                    m = _TRYCF_RE.search(line)
                    if m:
                        holder["url"] = m.group(0)
                        ev.set()
        except Exception:
            pass

    threading.Thread(target=_reader, daemon=True).start()
    ev.wait(timeout=30)
    url = holder.get("url", "")
    if url:
        logger.info("نفق Cloudflare السريع: %s", url)
    else:
        logger.warning("تعذّر التقاط رابط cloudflare خلال 30ث.")
    return url


def _start_cloudflared_named(token):
    exe = _find("cloudflared")
    if not exe:
        logger.warning("cloudflared.exe غير موجود.")
        return ""
    global _proc
    try:
        _proc = subprocess.Popen(
            [exe, "tunnel", "--no-autoupdate", "run", "--token", token],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, creationflags=_NO_WINDOW)
    except Exception as e:
        logger.warning("تعذّر تشغيل cloudflared المسمّى: %s", e)
        return ""
    return public_url()


def _start_ngrok(port):
    exe = _find("ngrok")
    if not exe:
        logger.warning("ngrok.exe غير موجود.")
        return ""
    domain = (os.getenv("NGROK_DOMAIN") or "").strip()
    token = (os.getenv("NGROK_AUTHTOKEN") or "").strip()
    if not domain:
        logger.warning("NGROK_DOMAIN غير مضبوط.")
        return ""
    cmd = [exe, "http", str(port), "--domain", domain]
    if token:
        cmd += ["--authtoken", token]
    global _proc
    try:
        _proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, creationflags=_NO_WINDOW)
    except Exception as e:
        logger.warning("تعذّر تشغيل ngrok: %s", e)
        return ""
    return f"https://{domain}"


def start(port=8080):
    """يشغّل النفق ويعيد الرابط العام، او "" اذا لم يُضبط/تعذّر."""
    provider = (os.getenv("TUNNEL_PROVIDER") or "").strip().lower()
    if not provider:
        # افتراضيا: لو cloudflared.exe موجود شغّله تلقائيا (يعمل بلا run.bat)
        if _find("cloudflared"):
            provider = "cloudflared"
        else:
            return ""
    if provider == "cloudflared":
        token = (os.getenv("CLOUDFLARED_TOKEN") or "").strip()
        if token:
            return _start_cloudflared_named(token)   # رابط ثابت
        return _start_cloudflared_quick(port)         # رابط سريع يُلتقط
    if provider == "ngrok":
        return _start_ngrok(port)
    logger.warning("TUNNEL_PROVIDER غير معروف: %s", provider)
    return ""


def stop():
    global _proc
    if _proc is not None:
        try:
            _proc.terminate()
        except Exception:
            pass
        _proc = None
