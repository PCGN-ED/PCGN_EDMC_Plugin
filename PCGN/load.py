# PCGN - EDMC plugin (load.py)
# Sends Elite Dangerous journal events to your backend with batching + gzip.
# Toggle DEBUG=True to post every event immediately and print verbose logs.

import time, json, gzip, threading, queue
from typing import Optional

import tkinter as tk
import myNotebook as nb
from config import config
import requests
import webbrowser
from tkinter import font as tkfont

PLUGIN_NAME = "PCGN"

# ---------- Debug / hardcoded settings ----------
DEBUG = False  # set False after you verify events flow

SERVER_BASE = "https://elitedashboard-backend-production.up.railway.app"  # <-- change if needed
POST_PATH   = "/api/ingest/journal"

# In debug: send every event immediately; otherwise use larger batch
BATCH_SIZE  = 1 if DEBUG else 50
FLUSH_SECS  = 1 if DEBUG else 5

# ---------- Config key (only the token is in prefs) ----------
CFG_TOKEN = "PCGN_Token"

# ---------- Runtime ----------
token_var: Optional[tk.StringVar] = None
session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

_lock = threading.Lock()
_batch = []
_last_flush = time.time()
_seq = 0
q: "queue.Queue[list]" = queue.Queue()
_uploader_thread: Optional[threading.Thread] = None
_flusher_thread: Optional[threading.Thread] = None
_running = True

# ---------- Helpers ----------
# EDMC typed getter (avoid deprecation). Provide a fallback if older EDMC.
if not hasattr(config, "get_str"):
    # Older builds expose get(); keep behavior
    config.get_str = config.get  # type: ignore

def _get_token() -> str:
    try:
        return (config.get_str(CFG_TOKEN) or "").strip()
    except Exception:
        return ""

def _start_threads():
    global _uploader_thread, _flusher_thread, _running
    _running = True
    if _uploader_thread is None:
        _uploader_thread = threading.Thread(target=_uploader_loop, name="PCGN-uploader", daemon=True)
        _uploader_thread.start()
    if _flusher_thread is None:
        _flusher_thread = threading.Thread(target=_flusher_loop, name="PCGN-flusher", daemon=True)
        _flusher_thread.start()

def _flusher_loop():
    global _running, _last_flush
    while _running:
        time.sleep(1)
        now = time.time()
        with _lock:
            if _batch and (now - _last_flush) >= FLUSH_SECS:
                if DEBUG:
                    print(f"[{PLUGIN_NAME}] flusher enqueue size={len(_batch)} (idle {int(now - _last_flush)}s)")
                q.put(_batch[:])
                _batch.clear()
                _last_flush = now

def _post_summary(raw_bytes: bytes) -> str:
    try:
        decoded = json.loads(raw_bytes.decode("utf-8"))
        arr = decoded if isinstance(decoded, list) else [decoded]
        types: dict[str, int] = {}
        for it in arr:
            t = it.get("event")
            types[t] = types.get(t, 0) + 1
        return str(types)
    except Exception:
        return "{}"

# ---------- EDMC entrypoints ----------
def plugin_start3(plugin_dir: str) -> str:
    _start_threads()
    tok_len = len(_get_token())
    print(f"[{PLUGIN_NAME}] loaded from {plugin_dir}")
    print(f"[{PLUGIN_NAME}] server={SERVER_BASE}{POST_PATH} batch={BATCH_SIZE} flush={FLUSH_SECS} token_len={tok_len} debug={DEBUG}")
    return PLUGIN_NAME

def plugin_stop():
    global _running
    print(f"[{PLUGIN_NAME}] stopping")
    _running = False
    with _lock:
        if _batch:
            q.put(_batch[:])
            _batch.clear()
    q.put(None)

def plugin_prefs(parent: nb.Notebook, cmdr: str, is_beta: bool) -> Optional[tk.Frame]:
    frame = nb.Frame(parent)
    frame.columnconfigure(1, weight=1)

    global token_var
    token_var = tk.StringVar(value=_get_token())

    nb.Label(frame, text="Commander API Token:").grid(row=0, column=0, padx=10, pady=5, sticky=tk.W)
    tk.Entry(frame, textvariable=token_var, width=48, show="•").grid(row=0, column=1, padx=10, pady=5, sticky=tk.EW)

    # create an underlined font for link appearance
    link_font = tkfont.Font(underline=True)

    def _add_link_sentence(before: str, url_text: str, after: str, url: str, r: int) -> None:
        # Compose text + clickable URL inline using grid
        col = 0
        if before:
            nb.Label(frame, text=before).grid(row=r, column=col, padx=(10, 0), pady=5, sticky=tk.W)
            col += 1
        
        # Use tk.Label instead of nb.Label for custom colors
        link_lbl = tk.Label(frame, text=url_text, fg="#0645AD", cursor="hand2", font=link_font)
        link_lbl.grid(row=r, column=col, padx=0, pady=5, sticky=tk.W)
        link_lbl.bind("<Button-1>", lambda e, u=url: webbrowser.open_new_tab(u))
        col += 1
        
        if after:
            nb.Label(frame, text=after).grid(row=r, column=col, padx=(0, 10), pady=5, sticky=tk.W)

    _add_link_sentence("Please visit ", "https://elite.pcgn.net", " to get your Commander API key.", "https://elite.pcgn.net", 1)
    _add_link_sentence("Visit us in Discord: ", "https://discord.pcgn.net", "", "https://discord.pcgn.net", 2)

    nb.Button(frame, text="Save", command=_save_prefs).grid(row=3, column=1, padx=10, pady=8, sticky=tk.E)
    return frame

def prefs_changed(cmdr: str, is_beta: bool) -> None:
    # Saved via the button; nothing to do here.
    pass

# ---------- Journal hook ----------
def journal_entry(cmdr, is_beta, system, station, entry, state):
    global _seq, _last_flush
    ev = entry.get("event", "")
    if DEBUG:
        print(f"[{PLUGIN_NAME}] seen event: {ev}")

    tok = _get_token()
    if not tok:
        if DEBUG:
            print(f"[{PLUGIN_NAME}] skipped {ev}: no token (token_len=0)")
        return

    # Build idempotency key (prefer file::line)
    file_name = entry.get("_file", "unknown")
    line_num  = entry.get("_line", 0)
    ts = entry.get("timestamp", "")

    if file_name != "unknown" and line_num:
        idk = f"{file_name}::{line_num}"
    else:
        _seq += 1
        idk = f"{ts}::{ev}::{_seq}"

    enriched = dict(entry)
    enriched["idempotency_key"] = idk
    enriched["file"] = file_name
    enriched["line"] = line_num
    # keep underscore variants (backend relies on these sometimes)
    enriched["_file"] = file_name
    enriched["_line"] = line_num

    # Buffer & flush on size/time
    with _lock:
        _batch.append(enriched)
        now = time.time()
        if len(_batch) >= BATCH_SIZE or (now - _last_flush) >= FLUSH_SECS:
            if DEBUG:
                print(f"[{PLUGIN_NAME}] enqueue batch size={len(_batch)} (since flush {int(now - _last_flush)}s)")
            q.put(_batch[:])
            _batch.clear()
            _last_flush = now

# ---------- Uploader ----------
def _uploader_loop():
    backoff = 1.0
    url = SERVER_BASE + POST_PATH
    while True:
        batch = q.get()
        if batch is None:
            break

        tok = _get_token()
        if not tok:
            if DEBUG:
                print(f"[{PLUGIN_NAME}] drop batch: no token")
            continue

        try:
            raw = json.dumps(batch, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            gz  = gzip.compress(raw)
            headers = {
                "Authorization": f"Bearer {tok}",
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
            }

            if DEBUG:
                print(f"[{PLUGIN_NAME}] posting batch -> {_post_summary(raw)}")

            resp = session.post(url, data=gz, headers=headers, timeout=15)
            code = resp.status_code

            if 200 <= code < 300:
                if DEBUG:
                    print(f"[{PLUGIN_NAME}] posted ok ({code})")
                backoff = 1.0
            elif code in (429, 500, 502, 503, 504):
                if DEBUG:
                    print(f"[{PLUGIN_NAME}] retryable {code}, requeueing batch")
                time.sleep(backoff)
                backoff = min(60.0, backoff * 2.0)
                q.put(batch)
            else:
                print(f"[{PLUGIN_NAME}] HTTP {code}: {resp.text[:200]}")
        except Exception as e:
            if DEBUG:
                print(f"[{PLUGIN_NAME}] network error: {e}")
            time.sleep(backoff)
            backoff = min(60.0, backoff * 2.0)
            q.put(batch)

# ---------- Save prefs ----------
def _save_prefs():
    try:
        tok = token_var.get().strip() if token_var else ""
        config.set(CFG_TOKEN, tok)
        print(f"[{PLUGIN_NAME}] token saved (len={len(tok)})")
    except Exception as e:
        print(f"[{PLUGIN_NAME}] save prefs error: {e}")
