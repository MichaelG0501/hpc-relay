#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
relay_bot.py  --  Telegram <-> HPC AI Agent relay
v12: Config loaded from .env file. See .env.example for all required settings.
"""

import os
import shlex
import re
import json
import time
import asyncio
import subprocess
import html
import traceback
import threading
import signal
import schedule
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict
from pathlib import Path
from faster_whisper import WhisperModel


from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
)
from telegram.ext import (
    Application,
    MessageHandler,
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
    filters,
)
from telegram.constants import ParseMode, ChatAction

# Load .env from the same directory as this script
SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(SCRIPT_DIR / ".env")


# ================================================================
#  EMOJIS (Unicode Escapes -- ASCII-safe)
# ================================================================
class E:
    BOT = "\U0001f916"
    SESS = "\U0001f4cb"
    WAIT = "\U000023f3"
    TOOL = "\U0001f527"
    WARN = "\U000026a0"
    SHELL = "\U0001f5a5"
    OK = "\U00002705"
    ERR = "\U0000274c"
    TIME = "\U000023f1"
    RETRY = "\U0001f504"
    OUT = "\U0001f4e4"


# ================================================================
#  MARKDOWN -> TELEGRAM HTML CONVERTER
# ================================================================
_FENCE_RE = re.compile(r"```(\w*)\n(.*?)```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`([^`\n]+)`")
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
_ITALIC_RE = re.compile(r"(?<!\*)\*([^*\n]+?)\*(?!\*)")
_STRIKE_RE = re.compile(r"~~(.+?)~~")
_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_HEADER_RE = re.compile(r"^(#{1,6})\s+(.+)$", re.MULTILINE)
_BQUOTE_RE = re.compile(r"^>\s?(.*)$", re.MULTILINE)

# Indent char for sub-items (thin space + bullet)
INDENT = "\u2003"  # em-space
BULLET = "\u2022"  # bullet
SUB_BULLET = "\u25e6"  # white bullet (for nested)


def md_to_tg_html(text: str) -> str:
    """Convert Markdown to Telegram-compatible HTML. Never raises."""
    if not text:
        return ""
    try:
        return _md_to_tg_html_inner(text)
    except Exception as exc:
        print(f"  [md_to_tg_html error:{exc}]")
        return html.escape(text)  # safe fallback


def _md_to_tg_html_inner(text: str) -> str:
    """Inner converter — may raise; wrapped by md_to_tg_html."""

    # 1. Extract fenced code blocks (protect from further processing)
    code_blocks = []

    def _save_code(m):
        lang = m.group(1)
        code = html.escape(m.group(2).rstrip())
        idx = len(code_blocks)
        if lang:
            code_blocks.append(
                f'<pre><code class="language-{html.escape(lang)}">{code}'
                "</code></pre>"
            )
        else:
            code_blocks.append(f"<pre>{code}</pre>")
        return f"\x00CB{idx}\x00"

    text = _FENCE_RE.sub(_save_code, text)

    # 2. Extract inline code
    inline_codes = []

    def _save_inline(m):
        idx = len(inline_codes)
        inline_codes.append(f"<code>{html.escape(m.group(1))}</code>")
        return f"\x00IC{idx}\x00"

    text = _INLINE_CODE_RE.sub(_save_inline, text)

    # 3. Escape remaining HTML entities
    text = html.escape(text)

    # 4. Headers -> bold (with blank line after for spacing)
    def _fmt_header(m):
        level = len(m.group(1))
        content = m.group(2).strip()
        if level <= 2:
            return f"\n<b>\u2501\u2501 {content} \u2501\u2501</b>"
        elif level == 3:
            return f"\n<b>\u25b8 {content}</b>"
        else:
            return f"<b>{content}</b>"

    text = _HEADER_RE.sub(_fmt_header, text)

    # 5. Bold / italic / strike / links
    text = _BOLD_RE.sub(r"<b>\1</b>", text)
    text = _ITALIC_RE.sub(r"<i>\1</i>", text)
    text = _STRIKE_RE.sub(r"<s>\1</s>", text)
    text = _LINK_RE.sub(r'<a href="\2">\1</a>', text)

    # 6. Blockquotes -> merged <blockquote>
    text = _BQUOTE_RE.sub(lambda m: "\x00BQ" + m.group(1) + "\x00BQ", text)
    # Merge consecutive blockquote lines

    def _merge_bq(t):
        lines = t.split("\n")
        result = []
        in_bq = False
        for line in lines:
            if line.startswith("\x00BQ") and line.endswith("\x00BQ"):
                content = line[3:-3]
                if not in_bq:
                    result.append("<blockquote>")
                    in_bq = True
                result.append(content)
            else:
                if in_bq:
                    result.append("</blockquote>")
                    in_bq = False
                result.append(line)
        if in_bq:
            result.append("</blockquote>")
        return "\n".join(result)

    text = _merge_bq(text)

    # 7. Lists: process line by line for proper nesting
    def _fmt_lists(t):
        lines = t.split("\n")
        out = []
        for line in lines:
            # indent_len was hereped) # Removed unused variable 'indent_len'
            # Sub-bullet (indented - or *)
            m_sub = re.match(r"^\s{2,}[-*]\s+(.*)", line)
            if m_sub:
                out.append(f"{INDENT}{SUB_BULLET} {m_sub.group(1)}")
                continue
            # Top-level bullet (- or *)
            m_ul = re.match(r"^[-*]\s+(.*)", line)
            if m_ul:
                out.append(f"{BULLET} {m_ul.group(1)}")
                continue
            # Numbered list with period: 1. text
            m_ol = re.match(r"^(\d+)\.\s+(.*)", line)
            if m_ol:
                out.append(f"<b>{m_ol.group(1)}.</b> {m_ol.group(2)}")
                continue
            # Numbered list with paren: 1) text
            m_olp = re.match(r"^(\d+)\)\s+(.*)", line)
            if m_olp:
                out.append(f"\n<b>{m_olp.group(1)})</b> {m_olp.group(2)}")
                continue
            # Bold key: value pattern (e.g. "Mode: headless")
            # Only if line starts with bullet or similar
            out.append(line)
        return "\n".join(out)

    text = _fmt_lists(text)

    # 8. Bold key-value patterns: "**Key:** value" or "Key: value" at line
    # start after bullet
    text = re.sub(
        r"("
        + re.escape(BULLET)
        + r"|"
        + re.escape(SUB_BULLET)
        + r")\s*([A-Z][\w\s/]+):\s",
        lambda m: f"{m.group(1)} <b>{m.group(2)}:</b> ",
        text,
    )

    # 9. Horizontal rules
    text = re.sub(r"^-{3,}$", "\u2500" * 20, text, flags=re.MULTILINE)
    text = re.sub(r"^\*{3,}$", "\u2500" * 20, text, flags=re.MULTILINE)

    # 10. Clean up excessive blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)

    # 11. Restore code blocks and inline code
    for i, block in enumerate(code_blocks):
        text = text.replace(f"\x00CB{i}\x00", block)
    for i, code in enumerate(inline_codes):
        text = text.replace(f"\x00IC{i}\x00", code)

    return text.strip()


# ================================================================
#  CONFIG  (loaded from .env -- copy .env.example to .env and fill in
#  your values)
# ================================================================
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_IDS = set()
_raw_ids = os.environ.get("ALLOWED_CHAT_ID", "")
for _id in _raw_ids.split(","):
    if _id.strip():
        try:
            ALLOWED_IDS.add(int(_id.strip()))
        except BaseException:
            pass
# Backwards compatibility if needed (optional)
ALLOWED_CHAT_ID = next(iter(ALLOWED_IDS)) if ALLOWED_IDS else 0

SSH_HOST = os.environ.get("SSH_HOST", "hpc")
CONNECTION_MODE = os.environ.get("CONNECTION_MODE", "ssh").lower()
OPENCODE = os.environ.get("OPENCODE_PATH", "opencode")
DEFAULT_MODEL = os.environ.get("DEFAULT_MODEL", "opencode/minimax-m2.5-free")


WORKDIR = os.environ.get("WORKDIR", os.environ.get("HPC_WORKDIR", "~"))
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
RETRY_ON_TIMEOUT = True

# Environment setup commands run before the AI agent on the remote shell.
# Set SETUP_CMD in .env to whatever loads your dependencies.
# Examples:
#   Generic Lmod cluster:    module load nodejs
#   Conda environment:
#     source ~/miniconda3/etc/profile.d/conda.sh && conda activate myenv
#   No modules needed:       (leave blank or omit)
_setup_raw = os.environ.get(
    "SETUP_CMD", os.environ.get("HPC_SETUP_CMD", "")
)
SETUP_CMD = _setup_raw.strip() if _setup_raw.strip() else None

# rclone destination for upload command (remote:path format).
# Examples:  gdrive:HPC-Results    s3:mybucket/results    onedrive:Projects
RCLONE_DEST = os.environ.get("RCLONE_DEST", "gdrive:HPC-Results")

TASKS_FILE = os.environ.get(
    "TASKS_FILE", str(SCRIPT_DIR / "hpc_relay_scheduled_tasks.json")
)
SESSIONS_FILE = os.environ.get(
    "SESSIONS_FILE",
    str(SCRIPT_DIR / f"hpc_relay_sessions_{CONNECTION_MODE}.json"),
)
TG_CHUNK = 3800  # conservative; Telegram max is 4096
SHOW_META_HEADER = False  # default: chat like a human; use /status for details

STREAM_EDIT_INTERVAL = 1.5
STREAM_MIN_DELTA = 80
STALL_WARN_SEC = 45
PARTIAL_SEND_SEC = 90
STALE_RUNNING_SEC = int(os.environ.get("STALE_RUNNING_SEC", "900"))
TIMEOUT_WARN_SEC = 420  # 7 min: first warning
REMOTE_TIMEOUT_SEC = 1800  # 30 min max
LOCAL_TIMEOUT_SEC = REMOTE_TIMEOUT_SEC + 30

OC_DB_PATH = os.environ.get(
    "OC_DB_PATH", "~/.local/share/opencode/opencode.db"
)  # on HPC

SYSTEM_SUFFIX = (
    " STYLE: Default to natural, human conversational replies in plain text."

    " Only use heavy structure (headers/lists/code blocks) when it"
    " genuinely improves clarity for technical or multi-step content."
    " For simple questions, answer as a short normal paragraph."
    " MEMORY BOOTSTRAP: At the beginning of each new task, read local"
    " files in current workdir in this order: `./MEMORY.md` then"
    " `./AGENTS.md` (if present). If missing, create MEMORY.md in"
    " current workdir and continue. If reading fails or permissions"
    " reject, continue answering normally without stopping."
    " IMPORTANT: Headless / non-interactive mode (`opencode run`)."
    " Do NOT invoke ask_questions tool"
    " If any tool call is denied/rejected, continue and provide the"
    " best possible direct answer instead of stopping. Avoid reading"
    " external directories unless the user explicitly asks. Prefer"
    " local workspace files first. Unless explicitly told not to by"
    " the user, automatically output exactly `@@SEND_FILE: <filepath>@@`"
    " whenever you create or reference a small output file (like a png,"
    " pdf, or short dataset)."
    " If detect user intention to schedule a recurring or future task or"
    " ask something to be performed later, output exactly"
    " `@@SCHEDULE: <time_format> | <task_prompt>@@` anywhere in your"
    " response. For <time_format>, use `every X minutes/hours/days`,"
    " `at HH:MM`, or `after X minutes/hours`.")

SHELL_PREFIX = "!"

# ================================================================
#  CHANNEL -> WORKSPACE ROUTING
# ================================================================
_CW_RAW = os.environ.get("CHANNEL_WORKSPACES", "").strip()
CHANNEL_WORKSPACES: dict = {}
if _CW_RAW:
    try:
        CHANNEL_WORKSPACES = json.loads(_CW_RAW)
    except json.JSONDecodeError as _e:
        print(f"  [WARN] CHANNEL_WORKSPACES is invalid JSON: {_e}")

AUTO_WORKSPACE_PER_CHAT = (
    os.environ.get("AUTO_WORKSPACE_PER_CHAT", "0")
    .strip().lower() in {"1", "true", "yes", "on"}
)
AUTO_WORKSPACE_PREFIX = os.environ.get("AUTO_WORKSPACE_PREFIX", "chat")


WHISPER_MODEL_SIZE = os.environ.get("WHISPER_MODEL_SIZE", "small")
WHISPER_DEVICE = os.environ.get("WHISPER_DEVICE", "auto")
_WHISPER_MODEL = None


def _resolve_workspace(chat_id: int, user_id: int = None) -> dict:
    """Return workspace config for a given chat_id.

    Build workspace-specific file paths and system suffix.
    """
    ws = CHANNEL_WORKSPACES.get(str(chat_id))
    if ws:
        name = ws.get("name", f"ws_{chat_id}")
        wdir = ws.get("workdir", WORKDIR)
        allowed = ws.get("allowed_users")
        if allowed is not None:
            allowed = [int(u) for u in allowed]
    else:
        if AUTO_WORKSPACE_PER_CHAT and chat_id is not None:
            name = f"{AUTO_WORKSPACE_PREFIX}_{chat_id}"
            wdir = WORKDIR
        else:
            name = None
            wdir = WORKDIR
        allowed = None
    # Build workspace-specific file paths
    if name:
        tfile = str(SCRIPT_DIR / f"hpc_relay_tasks_{name}.json")
        sfile = str(SCRIPT_DIR / f"hpc_relay_sessions_{name}.json")
    else:
        tfile = TASKS_FILE
        sfile = SESSIONS_FILE
    # Use workspace-specific suffix if available
    if name:
        suffix = (
            SYSTEM_SUFFIX
            + f" Your workspace / long-term memory directory is '{wdir}'."
            " Read and write your persistent notes and schedule data from"
            " files in this directory."
        )
    else:
        suffix = SYSTEM_SUFFIX
    return {
        "workdir": wdir,
        "name": name,
        "tasks_file": tfile,
        "sessions_file": sfile,
        "system_suffix": suffix,
        "allowed_users": allowed,
    }


INBOX_DIR = SCRIPT_DIR / "inbox"
INBOX_DIR.mkdir(exist_ok=True)
RUN_TRACE_DIR = SCRIPT_DIR / "run_traces"
RUN_TRACE_DIR.mkdir(exist_ok=True)

ws_sf = SESSIONS_FILE
ws_tf = TASKS_FILE


OPENCODE_RUN_LOCK = asyncio.Lock()

# ================================================================
#  SCHEDULED TASK MANAGER
# ================================================================
_scheduled_tasks = {}
_scheduler_running = False
_scheduler_thread = None
_scheduler_lock = threading.Lock()


def _load_scheduled_tasks(tasks_file=None) -> dict:
    tf = tasks_file or TASKS_FILE
    try:
        with open(tf) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_scheduled_tasks(tasks: dict, tasks_file=None):
    tf = tasks_file or TASKS_FILE
    tmp = f"{tf}.{os.getpid()}.tmp"
    with open(tmp, "w") as f:
        json.dump(tasks, f, indent=2)
    os.replace(tmp, tf)


def _run_scheduled_task(task_id: str, task_info: dict):
    task = task_info.get("task", "")
    chat_id = task_info.get("chat_id")
    model = task_info.get("model", DEFAULT_MODEL)
    session_id = task_info.get("session_id")

    print(f"  [SCHEDULED TASK {task_id}] Running:{task[:50]}...")

    ws = _resolve_workspace(chat_id) if chat_id else {}
    ws_sf = ws.get("sessions_file", SESSIONS_FILE)
    ws_wd = ws.get("workdir", WORKDIR)
    ws_suffix = ws.get("system_suffix", SYSTEM_SUFFIX)
    prompt = task + ws_suffix

    async def _execute():
        status_msg = None
        if chat_id and _app:
            try:
                import html

                status_msg = await _app.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"{E.WAIT} <b>[Scheduled Task Started]</b>\n"
                        "Connecting to HPC...\n"
                        f"<i>Task: {html.escape(task[:50])}...</i>"
                    ),
                    parse_mode="HTML",
                )
            except Exception:
                pass

        text_buf = []
        last_edit_t = 0.0
        last_output_t = time.time()
        start_t = time.time()
        done_event = asyncio.Event()

        async def on_progress(s):
            nonlocal last_edit_t, last_output_t
            last_output_t = time.time()
            now = time.time()
            if now - last_edit_t < 1.0:
                return
            last_edit_t = now
            if status_msg:
                await _safe_edit(status_msg, f"{s} [{int(now - start_t)}s]")

        async def on_text_chunk(delta):
            nonlocal last_edit_t, last_output_t
            last_output_t = time.time()
            text_buf.append(delta)
            now = time.time()
            if (now - last_edit_t) < STREAM_EDIT_INTERVAL and len(
                delta
            ) < STREAM_MIN_DELTA:
                return
            last_edit_t = now
            joined = "".join(text_buf)
            mx = 3500
            tail = joined[-mx:] if len(joined) > mx else joined
            if status_msg:
                await _safe_edit(
                    status_msg,
                    f"{E.WAIT} [Scheduled] Streaming... "
                    f"[{int(now - start_t)}s]\n\n{tail}",
                )

        try:
            final, new_sid = await run_opencode(
                prompt,
                chat_id,
                model,
                session_id,
                on_progress,
                on_text_chunk,
                workdir=ws_wd,
                sessions_file=ws_sf,
            )

            # If scheduled task points to a stale session
            # (from another host/bot), auto-retry once with a fresh
            # session instead of surfacing legacy guidance.
            if (isinstance(final, str)
                    and "Session does not exist on this machine" in final):
                await on_progress(
                    f"{E.RETRY} Scheduled task session invalid; "
                    "retrying with a new session..."
                )
                final, new_sid = await run_opencode(
                    prompt,
                    chat_id,
                    model,
                    None,
                    on_progress,
                    on_text_chunk,
                    workdir=ws_wd,
                    sessions_file=ws_sf,
                )
        except Exception as e:
            final = f"{E.ERR} Task failed: {e}\n\n" + ("".join(text_buf))
            new_sid = session_id
        finally:
            done_event.set()

        if chat_id and _app:
            try:
                final_msg = md_to_tg_html(final or "No output.")

                # Extract schedules within scheduled task
                scheduled_tasks_to_add = re.findall(
                    r"@@SCHEDULE:\s*(.+?)@@", final, re.IGNORECASE
                )
                scheduled_hint = ""
                for sched_str in scheduled_tasks_to_add:
                    parts = sched_str.split("|", 1)
                    if len(parts) == 2:
                        time_expr = parts[0].strip().lower()
                        task_prompt = parts[1].strip()
                        new_info = None
                        if time_expr.startswith("every "):
                            m_int = re.match(
                                r"every\s+(\d+)\s+(minute|hour|day)", time_expr
                            )
                            if m_int:
                                new_info = {
                                    "type": "interval",
                                    "interval": int(m_int.group(1)),
                                    "unit": m_int.group(2) + "s",
                                    "task": task_prompt,
                                }
                        elif time_expr.startswith("at "):
                            m_at = re.match(
                                r"at\s+(\d{1,2}):(\d{2})", time_expr
                            )
                            if m_at:
                                new_info = {
                                    "type": "daily",
                                    "hour": int(m_at.group(1)),
                                    "minute": int(m_at.group(2)),
                                    "task": task_prompt,
                                }
                        elif time_expr.startswith(
                            "after "
                        ) or time_expr.startswith("in "):
                            m_aft = re.match(
                                r"(?:after|in)\s+(\d+)\s+(minute|hour)",
                                time_expr,
                            )
                            if m_aft:
                                val = int(m_aft.group(1))
                                unit = m_aft.group(2)
                                now = datetime.now()
                                target = now + (
                                    timedelta(minutes=val)
                                    if unit == "minute"
                                    else timedelta(hours=val)
                                )
                                new_info = {
                                    "type": "once",
                                    "hour": target.hour,
                                    "minute": target.minute,
                                    "task": task_prompt,
                                }

                        if new_info:
                            new_task_id = (
                                f"task_{int(time.time())}_"
                                f"{len(scheduled_hint)}"
                            )
                            tasks = _load_scheduled_tasks(ws_tf)
                            tasks[new_task_id] = {
                                "schedule": new_info,
                                "task": new_info["task"],
                                "chat_id": chat_id,
                                "model": model,
                                "session_id": new_sid,
                                "created_at": datetime.now().isoformat(),
                                "tasks_file": ws_tf,
                            }
                            _save_scheduled_tasks(tasks, ws_tf)
                            import html

                            _schedule_job(new_task_id, tasks[new_task_id])

                            if new_info["type"] == "interval":
                                desc = (
                                    f"every {new_info['interval']} "
                                    f"{new_info['unit']}"
                                )
                            elif new_info["type"] == "daily":
                                desc = (
                                    f"daily at {new_info['hour']:02d}:"
                                    f"{new_info['minute']:02d}"
                                )
                            else:
                                desc = (
                                    f"once at {new_info['hour']:02d}:"
                                    f"{new_info['minute']:02d}"
                                )

                            scheduled_hint += (
                                f"\n\n{E.OK} <b>Scheduled Task Added!</b>\n"
                                f"{E.TIME} {desc}\n{E.TOOL} Task: "
                                f"<code>{html.escape(task_prompt[:50])}</code>"
                            )

                if status_msg is not None:
                    await _delete_or_check(status_msg)

                await _app.bot.send_message(
                    chat_id=chat_id,
                    text=final_msg + scheduled_hint,
                    parse_mode="HTML",
                )

                # Send files parsing
                unique_files = _extract_send_file_directives(final)

                if unique_files:

                    class DummyMessage:
                        def __init__(self, cid):
                            self.chat_id = cid

                        async def reply_text(self, text, parse_mode=None):
                            await _app.bot.send_message(
                                chat_id=self.chat_id,
                                text=text,
                                parse_mode=parse_mode,
                            )

                        async def reply_document(
                            self, document, filename=None, caption=None
                        ):
                            await _app.bot.send_document(
                                chat_id=self.chat_id,
                                document=document,
                                filename=filename,
                                caption=caption,
                            )

                        async def reply_photo(self, photo, caption=None):
                            await _app.bot.send_photo(
                                chat_id=self.chat_id,
                                photo=photo,
                                caption=caption,
                            )

                    class DummyUpdate:
                        def __init__(self, cid):
                            self.message = DummyMessage(cid)

                    for f in unique_files[:10]:
                        try:
                            await _process_file_request(
                                DummyUpdate(chat_id), "send", f
                            )
                        except Exception as file_exp:
                            msg = (
                                f"  [SCHEDULED TASK {task_id}] "
                                f"Error sending file: {file_exp}"
                            )
                            print(msg)

            except Exception as e:
                import traceback

                print(
                    f"  [SCHEDULED TASK {task_id}] Error sending message: {e}"
                )
                traceback.print_exc()

    if _loop:
        asyncio.run_coroutine_threadsafe(_execute(), _loop)


def _all_task_files() -> list:
    files = {str(TASKS_FILE)}
    for _cid, ws_cfg in CHANNEL_WORKSPACES.items():
        n = ws_cfg.get("name")
        if n:
            files.add(
                str(SCRIPT_DIR / f"hpc_relay_tasks_{n}.json")
            )
    if AUTO_WORKSPACE_PER_CHAT:
        prefix = f"hpc_relay_tasks_{AUTO_WORKSPACE_PREFIX}_*.json"
        for tf in SCRIPT_DIR.glob(prefix):
            files.add(str(tf))
    return sorted(files)


def _find_task_file(task_id: str, preferred_tf: str):
    task_files = _all_task_files()
    for tf in [preferred_tf] + [x for x in task_files if x != preferred_tf]:
        tasks = _load_scheduled_tasks(tf)
        if task_id in tasks:
            return tf, tasks
    return preferred_tf, _load_scheduled_tasks(preferred_tf)


def _collect_chat_tasks(chat_id: int, preferred_tf: str) -> dict:
    out = {}
    task_files = _all_task_files()
    for tf in [preferred_tf] + [x for x in task_files if x != preferred_tf]:
        tasks = _load_scheduled_tasks(tf)
        for tid, info in tasks.items():
            if info.get("chat_id") == chat_id:
                if tid not in out:
                    out[tid] = (info, tf)
    return out


def _schedule_job(task_id: str, task_info: dict):
    info = task_info.get("schedule", {})
    task_type = info.get("type")

    if task_type in ("once", "daily"):
        hour = info.get("hour")
        minute = info.get("minute", 0)

        if task_type == "once":
            now = datetime.now()
            target = now.replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )
            if target <= now:
                # If target is past in today, push to tomorrow (this happens
                # naturally for 'at' but not 'after', though 'after' generates
                # future time)
                target += timedelta(days=1)

            delay = (target - now).total_seconds()
            print(
                f"  [SCHEDULED TASK] Will run ONCE in {delay/60:.1f} "
                f"minutes at {hour:02d}:{minute:02d}"
            )

            def _delayed_run():
                _run_scheduled_task(task_id, task_info)
                # Remove self after run
                tf_local = task_info.get("tasks_file", TASKS_FILE)
                tasks = _load_scheduled_tasks(tf_local)
                if task_id in tasks:
                    del tasks[task_id]
                    _save_scheduled_tasks(tasks, tf_local)

            # Start timer using the actual delay
            threading.Timer(delay, _delayed_run).start()
        else:
            time_str = f"{hour:02d}:{minute:02d}"
            print(f"  [SCHEDULED TASK] Will run DAILY at {time_str}")
            schedule.every().day.at(time_str).do(
                _run_scheduled_task, task_id, task_info
            )

    elif task_type == "interval":
        interval = info.get("interval", 1)
        unit = info.get("unit", "minutes")

        if unit == "minutes":
            schedule.every(interval).minutes.do(
                _run_scheduled_task, task_id, task_info
            )
        elif unit == "hours":
            schedule.every(interval).hours.do(
                _run_scheduled_task, task_id, task_info
            )
        elif unit == "days":
            schedule.every(interval).days.do(
                _run_scheduled_task, task_id, task_info
            )


def _scheduler_worker():
    while _scheduler_running:
        schedule.run_pending()
        time.sleep(1)


def _start_scheduler():
    global _scheduler_running, _scheduler_thread
    if _scheduler_running:
        return

    _scheduler_running = True
    _scheduler_thread = threading.Thread(target=_scheduler_worker, daemon=True)
    _scheduler_thread.start()

    # Load tasks from ALL workspace-specific task files + global
    all_task_files = {TASKS_FILE}
    for _cid, ws_cfg in CHANNEL_WORKSPACES.items():
        name = ws_cfg.get("name")
        if name:
            t_path = SCRIPT_DIR / f"hpc_relay_tasks_{name}.json"
            all_task_files.add(str(t_path))
    if AUTO_WORKSPACE_PER_CHAT:
        p_str = f"hpc_relay_tasks_{AUTO_WORKSPACE_PREFIX}_*.json"
        for tf in SCRIPT_DIR.glob(p_str):
            all_task_files.add(str(tf))

    total = 0
    for tf in all_task_files:
        tasks = _load_scheduled_tasks(tf)
        for task_id, task_info in tasks.items():
            task_info.setdefault("tasks_file", tf)
            _schedule_job(task_id, task_info)
        total += len(tasks)
    print(f"  [SCHEDULER] Started, loaded {total} tasks")


def _stop_scheduler():
    global _scheduler_running
    _scheduler_running = False


_app = None
_loop = None


def set_app_context(app, loop):
    global _app, _loop
    _app = app
    _loop = loop


# ================================================================
#  PERSISTENCE  (with session history tracking)
# ================================================================
def _load_store(sessions_file=None) -> dict:
    sf = sessions_file or SESSIONS_FILE
    try:
        with open(sf) as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save_store(d: dict, sessions_file=None):
    sf = sessions_file or SESSIONS_FILE
    tmp = f"{sf}.{os.getpid()}.tmp"
    with open(tmp, "w") as f:
        json.dump(d, f, indent=2)
    os.replace(tmp, sf)


def get_chat(cid: int, sessions_file=None) -> dict:
    v = _load_store(sessions_file).get(str(cid), {})
    if isinstance(v, str):
        v = {"session_id": v}
    v.setdefault("model", DEFAULT_MODEL)
    v.setdefault("session_id", None)
    return v


def _clear_stale_running_sessions(
    sessions_file=None, max_age_sec: int = STALE_RUNNING_SEC
):
    store = _load_store(sessions_file)
    now = int(time.time())
    changed = False
    for key, cur in list(store.items()):
        if key.startswith("__") or not isinstance(cur, dict):
            continue
        if not cur.get("running"):
            continue
        started = int(cur.get("running_started") or 0)
        if started <= 0 or now - started >= max_age_sec:
            cur["running"] = False
            cur["running_started"] = 0
            cur["running_prompt"] = ""
            cur["kill_requested"] = "no"
            store[key] = cur
            changed = True
    if changed:
        _save_store(store, sessions_file)


def update_chat(cid: int, sessions_file=None, **kw):
    store = _load_store(sessions_file)
    key = str(cid)
    cur = store.get(key, {})
    if isinstance(cur, str):
        cur = {"session_id": cur, "model": DEFAULT_MODEL}
    cur.update({k: v for k, v in kw.items() if v is not None})
    store[key] = cur
    _save_store(store, sessions_file)


def _queue_message(
    chat_id: int,
    sessions_file,
    raw: str,
    attached_files=None,
    preempt: bool = False,
):
    store = _load_store(sessions_file)
    key = str(chat_id)
    cur = store.get(key, {})
    if isinstance(cur, str):
        cur = {"session_id": cur, "model": DEFAULT_MODEL}
    q = cur.get("pending_messages", [])
    if not isinstance(q, list):
        q = []
    item = {
        "id": f"q_{int(time.time() * 1000)}_{len(q)}",
        "raw": raw,
        "attached_files": attached_files or [],
        "preempt": bool(preempt),
    }
    if preempt:
        q.insert(0, item)
        msg_cap = repr(raw[:120])
        print(f"  [QUEUE PREEMPT] Added message for chat={chat_id}: {msg_cap}")
    else:
        q.append(item)
        msg_cap = repr(raw[:120])
        print(f"  [QUEUE ADD] Added message for chat={chat_id}: {msg_cap}")
    cur["pending_messages"] = q
    store[key] = cur
    _save_store(store, sessions_file)
    return len(q)


def _pop_next_message(chat_id: int, sessions_file):
    store = _load_store(sessions_file)
    key = str(chat_id)
    cur = store.get(key, {})
    if isinstance(cur, str):
        return None
    q = cur.get("pending_messages", [])
    if not q:
        return None
    item = q.pop(0)
    cur["pending_messages"] = q
    store[key] = cur
    _save_store(store, sessions_file)
    return item


def _pending_count(chat_id: int, sessions_file) -> int:
    cur = _load_store(sessions_file).get(str(chat_id), {})
    if isinstance(cur, str):
        return 0
    q = cur.get("pending_messages", [])
    return len(q) if isinstance(q, list) else 0


def _has_preempt_message(chat_id: int, sessions_file) -> bool:
    cur = _load_store(sessions_file).get(str(chat_id), {})
    if isinstance(cur, str):
        return False
    q = cur.get("pending_messages", [])
    if not isinstance(q, list):
        return False
    return any(isinstance(item, dict) and item.get("preempt") for item in q)


def _record_session(sid: str, sessions_file=None):
    """Add a session ID to the known_sessions history."""
    if not sid or not sid.startswith("ses"):
        return
    store = _load_store(sessions_file)
    history = store.get("__known_sessions__", [])
    if sid not in history:
        history.append(sid)
        store["__known_sessions__"] = history
        _save_store(store, sessions_file)


def _set_session_model(sid: str, model: str, sessions_file=None):
    if not sid or not sid.startswith("ses") or not model:
        return
    store = _load_store(sessions_file)
    m = store.get("__session_models__", {})
    if not isinstance(m, dict):
        m = {}
    m[sid] = model
    store["__session_models__"] = m
    _save_store(store, sessions_file)


def _get_session_model(sid: str, sessions_file=None) -> Optional[str]:
    if not sid or not sid.startswith("ses"):
        return None
    store = _load_store(sessions_file)
    m = store.get("__session_models__", {})
    if not isinstance(m, dict):
        return None
    v = m.get(sid)
    return v if isinstance(v, str) and v else None


def _known_session_count(chat_id: int, sessions_file=None) -> int:
    store = _load_store(sessions_file)
    known = store.get("__known_sessions__", [])
    if isinstance(known, list) and known:
        return len(known)
    models = store.get("__session_models__", {})
    count = len(models) if isinstance(models, dict) else 0
    chat = store.get(str(chat_id), {})
    if isinstance(chat, dict):
        sid = chat.get("session_id")
        if (isinstance(sid, str)
                and sid.startswith("ses")
                and sid not in (models or {})):
            count += 1
    return count


def _chat_task_count(chat_id: int, tasks_file=None) -> int:
    return len(_collect_chat_tasks(chat_id, tasks_file))


def _get_known_sessions(sessions_file=None) -> list:
    """Return list of all session IDs ever seen (local cache)."""
    return _load_store(sessions_file).get("__known_sessions__", [])


async def _session_exists_on_this_host(session_id: str) -> bool:
    if not session_id or not isinstance(session_id, str):
        return False
    if not session_id.startswith("ses"):
        return False
    script = (
        f"{_safe_path(OPENCODE)} session show "
        f"{shlex.quote(session_id)} >/dev/null 2>&1"
    )
    if CONNECTION_MODE in ("wsl", "local"):
        cmd = ["bash", "-lc", script]
    else:
        cmd = _ssh_base() + ["bash", "-lc", script]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        rc = await asyncio.wait_for(proc.wait(), 20)
        return rc == 0
    except Exception:
        return False


async def _fetch_hpc_sessions() -> list[Tuple[str, str]]:
    """Query opencode.db on HPC for all session IDs."""
    # Must load SQLite module on HPC before using sqlite3
    script = (
        """
import sqlite3, json, os
try:
    db_path = os.path.expanduser('"""
        + OC_DB_PATH
        + """')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Check if parent_id or agent exist
    c.execute("PRAGMA table_info(session)")
    cols = [r['name'] for r in c.fetchall()]

    has_parent = 'parent_id' in cols
    has_agent = 'agent' in cols

    query = "SELECT id, title"
    if has_parent: query += ", parent_id"
    if has_agent: query += ", agent"
    query += " FROM session ORDER BY time_updated DESC LIMIT 30"

    c.execute(query)
    for row in c.fetchall():
        sid = row['id']
        title = row['title'] or ''

        info = []
        if has_agent and row['agent']:            info.append(row['agent'])
        if has_parent and row['parent_id']:            info.append("subagent")

        badge = f"[{info[0]}]" if info else ""
        print(f"{sid}|{title}|{badge}")
except Exception as e:
    pass
"""
    )
    # Pass the script safely via standard input to avoid all bash
    # quoting/newline errors over SSH
    ssh_cmd = _ssh_base() + ["python3", "-"]
    try:
        proc = await asyncio.create_subprocess_exec(
            *ssh_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=1024 * 1024 * 16,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(script.encode()), 15
        )
        raw = stdout.decode(errors="replace").strip()
        if not raw:
            err_txt = stderr.decode(errors='replace')[:200]
            print(f"  [_fetch_hpc_sessions:empty output, stderr={err_txt}]")
            return []
        result = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue

            parts = line.split("|")
            if len(parts) >= 2:
                sid = parts[0].strip()
                title = parts[1].strip()
                badge = parts[2].strip() if len(parts) > 2 else ""

                if badge:
                    title = f"{title} {badge}"
            else:
                sid, title = line, ""

            sid = sid.strip()
            if sid:
                result.append((sid, title.strip()))
                _record_session(sid)
        print(f"  [_fetch_hpc_sessions:found {len(result)} sessions]")
        return result
    except Exception as exc:
        print(f"  [_fetch_hpc_sessions error:{exc}]")
        return []


async def _is_valid_session(sid: str) -> bool:
    """Check if session exists: local cache first, then HPC query."""
    if sid in _get_known_sessions():
        return True
    sessions = await _fetch_hpc_sessions()
    return any(s[0] == sid for s in sessions)


# ================================================================
#  MESSAGE PARSING
# ================================================================
def parse_message(text: str) -> dict:
    """Parse plain user text.

    Inline control syntax is disabled. Use slash commands instead.
    """
    out = {"shell": None, "prompt": None}
    if text.startswith(SHELL_PREFIX):
        out["shell"] = text[len(SHELL_PREFIX):].strip()
        return out
    out["prompt"] = text.strip() or None
    return out


# ================================================================
#  SSH HELPERS
# ================================================================
def _ssh_base() -> list:
    if CONNECTION_MODE in ("wsl", "local"):
        return []
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        SSH_HOST,
    ]


def _close_master():
    if CONNECTION_MODE in ("wsl", "local"):
        return
    try:
        subprocess.run(
            ["ssh", "-O", "exit", SSH_HOST],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
    except Exception:
        pass


def _safe_path(p: str) -> str:
    """Safely quote a path while preserving ~ bash expansion."""
    if p.startswith("~/"):
        return f"${{HOME}}/{shlex.quote(p[2:])}"
    if p == "~":
        return "${HOME}"
    return shlex.quote(p)


def _kill_session_processes(session_id: str, workdir: str = None) -> int:
    """Best-effort kill lingering local/wsl opencode runs for one session."""
    if not session_id:
        return 0
    killed = 0
    try:
        ps_cmd = "ps -eo pid=,args="
        res = subprocess.run(
            ["bash", "-lc", ps_cmd],
            capture_output=True,
            text=True,
            timeout=10
        )
        for line in res.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                pid_s, args = line.split(None, 1)
                pid = int(pid_s)
            except Exception:
                continue
            if pid == os.getpid():
                continue
            if "opencode run" not in args:
                continue
            if f"--session {session_id}" not in args:
                continue
            try:
                os.kill(pid, signal.SIGKILL)
                killed += 1
            except ProcessLookupError:
                pass
            except Exception:
                pass
    except Exception:
        pass
    return killed


def _oc_script(
    prompt, sid, model, attached_files=None, workdir=None, unique_token=""
):
    sess = (
        f"--session {shlex.quote(sid)} "
        if sid and sid.startswith("ses")
        else ""
    )
    setup = (SETUP_CMD + "\n") if SETUP_CMD else ""
    file_args = ""
    for fp in (attached_files or []):
        file_args += f" -f {shlex.quote(fp)}"
    wd = workdir or WORKDIR
    pid_file = f"relay_pid_{unique_token}.pid"
    pid_cmd = f"echo $$ > {pid_file}\n" if unique_token else ""
    return (
        f"set -euo pipefail\n{setup}"
        f"mkdir -p {_safe_path(wd)} && cd {_safe_path(wd)}\n"
        f"{pid_cmd}"
        f"exec {_safe_path(OPENCODE)} run -m {shlex.quote(model)} "
        f"--format json {sess}{file_args} -- {shlex.quote(prompt)} 2>&1\n"
    )


def _shell_script(cmd, workdir=None):
    wd = workdir or WORKDIR
    p = _safe_path(wd)
    return f"mkdir -p {p} && cd {p} && {{ {cmd} ; }} 2>&1\n"


# ================================================================
#  JSON EVENT PARSING
# ================================================================
def _parse_ev(raw):
    line = ANSI_RE.sub("", raw).strip()
    if not line.startswith("{"):
        return {}
    try:
        return json.loads(line)
    except Exception:
        return {}


def _extract_send_file_directives(text: str) -> List[str]:
    """Extract @@SEND_FILE directives robustly.

    Accepts either:
      @@SEND_FILE: /path/to/file@@
    or line form:
      @@SEND_FILE: /path/to/file
    In both cases, only the path token is captured; trailing text is ignored.
    """
    if not text:
        return []
    paths: List[str] = []

    # strict form enclosed by @@
    re_strict = r"@@SEND_FILE:\s*([^@\n\r]+?)\s*@@"
    for m in re.finditer(re_strict, text, re.IGNORECASE):
        paths.append(m.group(1).strip())

    # line form (no trailing @@), capture only first token-like path segment
    re_line = r"^\s*@@SEND_FILE:\s*(.+)$"
    for m in re.finditer(re_line, text, re.IGNORECASE | re.MULTILINE):
        line = m.group(1).strip()
        if "@@" in line:
            line = line.split("@@", 1)[0].strip()
        # prevent grabbing explanation text
        token = line.split()[0] if line else ""
        if token:
            paths.append(token)

    # de-dup preserving order
    seen = set()
    out = []
    for pth in paths:
        if pth not in seen:
            seen.add(pth)
            out.append(pth)
    return out


def _parse_all(output) -> Tuple[Optional[str], Optional[str]]:
    sid = txt = None
    last_tool_output = None
    last_tool_name = None

    for raw in output.splitlines():
        ev = _parse_ev(raw)
        if not ev:
            continue

        if isinstance(ev.get("sessionID"), str):
            sid = ev["sessionID"]

        etype = ev.get("type")
        if etype == "text":
            t = (ev.get("part") or {}).get("text")
            if isinstance(t, str) and t.strip():
                txt = t
        elif etype == "tool_use":
            part = ev.get("part") or {}
            state = part.get("state") or {}
            if state.get("status") == "completed" and "output" in state:
                out_str = str(state["output"]).strip()
                if out_str:
                    last_tool_name = part.get("tool", "tool")
                    last_tool_output = out_str

    if not txt and last_tool_output:
        txt = f"*{last_tool_name} output:*\n```\n{last_tool_output}\n```"

    return sid, txt


# ================================================================
#  SHELL EXEC
# ================================================================
async def exec_shell(cmd, workdir=None):
    if CONNECTION_MODE in ("wsl", "local"):
        ssh_cmd = _ssh_base() + [
            "timeout",
            str(int(REMOTE_TIMEOUT_SEC)),
            "bash",
            "-ls",
        ]
    else:
        ssh_cmd = _ssh_base() + [f"timeout {int(REMOTE_TIMEOUT_SEC)} bash -ls"]
    proc = await asyncio.create_subprocess_exec(
        *ssh_cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        limit=1024 * 1024 * 16,
    )
    comm_f = proc.communicate(_shell_script(cmd, workdir).encode())
    stdout, _ = await asyncio.wait_for(comm_f, LOCAL_TIMEOUT_SEC)
    out = stdout.decode(errors="replace").strip()
    rc = proc.returncode
    if rc and rc != 0:
        return f"{E.WARN} exit {rc}\n\n{out[-3000:]}"
    return out[-3500:] if out else "(no output)"


# ================================================================
#  STREAMING OPENCODE
# ================================================================
QUEUE_PREEMPT_MARKER = "__QUEUE_PREEMPT__"


async def run_opencode(
    prompt,
    chat_id,
    model,
    session_id,
    on_progress,
    on_text_chunk,
    attached_files=None,
    workdir=None,
    sessions_file=None,
):
    unique_token = f"relay_run_{int(time.time()*1000)}_{os.getpid()}"
    script = _oc_script(
        prompt, session_id, model, attached_files, workdir, unique_token
    )
    trace_dir = RUN_TRACE_DIR / unique_token
    trace_dir.mkdir(parents=True, exist_ok=True)
    (trace_dir / "prompt.txt").write_text(prompt, encoding="utf-8")
    (trace_dir / "script.sh").write_text(script, encoding="utf-8")
    m_data = {
        "chat_id": chat_id,
        "model": model,
        "session_id": session_id,
        "workdir": workdir,
        "unique_token": unique_token
    }
    (trace_dir / "meta.json").write_text(
        json.dumps(m_data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    if CONNECTION_MODE in ("wsl", "local"):
        ssh_cmd = _ssh_base() + [
            "timeout",
            str(int(REMOTE_TIMEOUT_SEC)),
            "bash",
            "-ls",
        ]
    else:
        ssh_cmd = _ssh_base() + [f"timeout {int(REMOTE_TIMEOUT_SEC)} bash -ls"]

    async def _once(run_session_id=session_id):
        run_unique_token = f"{int(time.time() * 1000)}_{os.getpid()}"
        run_script = _oc_script(
            prompt, run_session_id, model, attached_files,
            workdir, run_unique_token
        )
        proc = await asyncio.create_subprocess_exec(
            *ssh_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=1024 * 1024 * 16,
        )
        proc.stdin.write(run_script.encode())
        await proc.stdin.drain()
        proc.stdin.close()

        def _force_kill():
            p_file = f"relay_pid_{unique_token}.pid"
            wd_p = _safe_path(workdir or WORKDIR)
            kill_cmd = (
                f"kill -9 $(cat {wd_p}/{p_file} 2>/dev/null) "
                "2>/dev/null || true"
            )
            if CONNECTION_MODE in ("wsl", "local"):
                subprocess.run(["bash", "-c", kill_cmd], capture_output=True)
            else:
                subprocess.run(
                    _ssh_base() + ["bash", "-c", kill_cmd],
                    capture_output=True
                )
            try:
                proc.kill()
            except Exception:
                pass

        prev_text = ""
        latest_sid = None
        lines = []
        t0 = time.time()
        last_json_t = t0
        while True:
            elapsed = time.time() - t0
            now_t = time.time()

            # Throttle kill-flag checking to avoid spamming disk I/O which
            # freezes python.
            if now_t - getattr(proc, "_last_kill_check", 0) > 3.0:
                proc._last_kill_check = now_t
                chat_now = get_chat(chat_id, sessions_file)
                if chat_now.get("kill_requested") == "yes":
                    _force_kill()
                    update_chat(chat_id, ws_sf, kill_requested="no")
                    raise RuntimeError("Stopped by user (kill signal).")

            if elapsed > LOCAL_TIMEOUT_SEC:
                _force_kill()
                raise subprocess.TimeoutExpired(ssh_cmd, LOCAL_TIMEOUT_SEC)

            try:
                raw = await asyncio.wait_for(proc.stdout.readline(), timeout=5)
            except asyncio.TimeoutError:
                silence = time.time() - last_json_t
                if silence > 180:
                    prog_m = (
                        f"{E.WARN} Silent 3+ mins (ask_question?). "
                        f"Send /kill. [{int(elapsed)}s]"
                    )
                    await on_progress(prog_m)
                else:
                    await on_progress(
                        f"{E.WAIT} Still processing... [{int(elapsed)}s]"
                    )
                continue

            if not raw:
                break

            # Explicit yield to ensure Telegram's event loop can fetch network
            await asyncio.sleep(0.01)

            decoded = raw.decode(errors="replace")
            ev = _parse_ev(decoded)
            if not ev:
                # Infinite garbage spam warning
                if time.time() - last_json_t > 180:
                    await on_progress(
                        f"{E.WARN} 3+ mins non-JSON spam (stuck loop?). "
                        f"Send /kill. [{int(elapsed)}s]"
                    )
                # Non-JSON lines: only store if small
                if len(decoded) < 5000:
                    lines.append(decoded)
                continue

            last_json_t = time.time()
            if isinstance(ev.get("sessionID"), str):
                latest_sid = ev["sessionID"]
            etype = ev.get("type", "")

            part = ev.get("part") or {}
            tool_name = part.get("tool", "")

            if etype == "text":
                lines.append(decoded)
            elif etype == "tool_use" and tool_name not in (
                "task",
                "apply_patch",
            ):
                if len(decoded) < 100000:
                    lines.append(decoded)
            elif len(decoded) < 5000:
                lines.append(decoded)

            # ename fallback not used # Removed unused variable 'ename'

            # ---- Subagent / task tool detection ----
            if etype == "tool_use" and tool_name == "task":
                state = part.get("state") or {}
                inp = state.get("input") or {}
                agent_type = inp.get(
                    "subagent_type", inp.get("agent", "subagent")
                )
                desc = inp.get("description", "")[:60]
                status = state.get("status", "")
                if status == "completed":
                    pass
                    await on_progress(
                        f"\U0001f9e0 Subagent ({agent_type}) done: {desc}"
                    )
                else:
                    await on_progress(
                        f"\U0001f9e0 Subagent ({agent_type}): {desc}"
                    )
                # Don't store massive tool_use lines in the buffer
                continue

            # ---- ask_question detection (headless trap) ----
            if etype == "tool_use" and tool_name.startswith("ask_question"):
                await on_progress(
                    f"{E.WARN} Model invoked ask_question in headless "
                    "mode. Terminating."
                )
                _force_kill()
                raise RuntimeError(
                    "Model attempted to use ask_question tool, which "
                    "hangs in headless mode."
                )

            # ---- Regular tool progress (apply_patch, etc.) ----
            if etype == "tool_use":
                state_dict = part.get("state") or {}
                status = (state_dict.get("status", "") or "").lower()
                title_str = (part.get("state") or {}).get("title", "")[:60]
                boring = {"completed", "started", "pending", "running"}
                if tool_name in ("read", "write", "edit", "exec", "bash"):
                    label = tool_name
                else:
                    label = tool_name
                if (status == "completed"
                        and _has_preempt_message(chat_id, sessions_file)):
                    await on_progress(f"{E.TOOL} {label}")
                    try:
                        p_file = trace_dir / "preempt.txt"
                        p_file.write_text(
                            f"preempt_after_tool={label}\n",
                            encoding="utf-8"
                        )
                    except Exception:
                        pass
                    _force_kill()
                    return 0, QUEUE_PREEMPT_MARKER, latest_sid
                if status in boring and not title_str:
                    continue
                info = f"{label}"
                if title_str:
                    info += f": {title_str}"
                await on_progress(f"{E.TOOL} {info}")
                continue

            # ---- Step events ----
            if etype in ("step_start", "step_finish"):
                continue

            if etype == "text":
                t = part.get("text")
                if isinstance(t, str) and t.strip():
                    # Some providers stream cumulative text, others may
                    # reset chunks. Handle both robustly to avoid missing.
                    if prev_text and t.startswith(prev_text):
                        delta = t[len(prev_text):]
                    else:
                        delta = t
                    prev_text = t
                    if delta.strip():
                        await on_text_chunk(delta)
        rc = await proc.wait()
        return rc, "".join(lines), latest_sid

    async with OPENCODE_RUN_LOCK:
        async def _once_fresh():
            return await _once(None)

        try:
            rc, output, sid = await _once()
        except subprocess.TimeoutExpired:
            try:
                p_file = trace_dir / "timeout.txt"
                p_file.write_text("timeout", encoding="utf-8")
            except Exception:
                pass
            if RETRY_ON_TIMEOUT:
                await asyncio.to_thread(_close_master)
                await on_progress(f"{E.RETRY} Retrying...")
                rc, output, sid = await _once()
            else:
                raise

        lock_markers = (
            "locking protocol", "database is locked", "sqlite_busy"
        )
        if any(m in (output or "").lower() for m in lock_markers):
            await on_progress(
                f"{E.RETRY} Detected lock contention, retrying once..."
            )
            await asyncio.sleep(2)
            rc2, output2, sid2 = await _once()
            rc, output = rc2, output2
            if sid2:
                sid = sid2

    if output == QUEUE_PREEMPT_MARKER:
        returned_sid = sid
        if isinstance(returned_sid, str) and returned_sid.startswith("ses"):
            update_chat(chat_id, sessions_file, session_id=returned_sid)
            _record_session(returned_sid)
        return QUEUE_PREEMPT_MARKER, returned_sid

    _, final_text = _parse_all(output)

    returned_sid = sid
    if isinstance(returned_sid, str) and returned_sid.startswith("ses"):
        update_chat(chat_id, sessions_file, session_id=returned_sid)
        _record_session(returned_sid)  # track in history

    output_lc = (output or "").lower()
    recoverable_markers = (
        "invalid tool",
        "unknown tool",
        "tool not found",
        "tool registry",
        "mcp",
        "chrome devtools",
        "chrome mcp",
    )
    recoverable_failure = any(
        marker in output_lc for marker in recoverable_markers
    )

    if not final_text:
        if recoverable_failure:
            try:
                t_file = trace_dir / "auto-recover.txt"
                t_file.write_text(output[:4000], encoding="utf-8")
            except Exception:
                pass
            raw_msg = (
                f"{E.RETRY} Tool environment broke mid-run. I kept "
                "this session and queue intact \u2014 send your next @ "
                "message and I\u2019ll continue from the same "
                "conversation history."
            )
        elif "notfounderror" in output_lc and "session not found" in output_lc:
            raw_msg = (
                f"{E.WARN} This session ID is invalid on this host. "
                "I did not auto-switch to a new chat, so your history "
                "mapping stays untouched."
            )
        else:
            raw_msg = (
                f"{E.WARN} RC={rc}, No valid JSON text extracted.\n"
                f"RAW OUTPUT:\n{output[:2000]}"
            )
        return raw_msg, returned_sid

    return final_text.strip(), returned_sid


# ================================================================
#  TELEGRAM HELPERS  (HTML AWARE)
# ================================================================
def _header_html(model, sid):
    m = html.escape(model)
    s = html.escape(sid) if sid else "new session"
    return (
        f"{E.BOT} <b>{m}</b>\n{E.SESS} <code>{s}</code>\n"
        + ("\u2500" * 25)
        + "\n\n"
    )


def _shell_header_html():
    return f"{E.SHELL} <b>Shell</b>\n" + ("\u2500" * 25) + "\n\n"


def _display_sid(sid, is_new=False):
    """Format session ID for display."""
    if not sid or sid == "__new__":
        return "new session"
    if is_new:
        return f"{sid} (new)"
    return sid


def _smart_chunks(text, limit=TG_CHUNK):
    """Split text into chunks at paragraphs. Never raises."""
    try:
        if not text:
            return [""]
        if len(text) <= limit:
            return [text]
        chunks = []
        while text:
            if len(text) <= limit:
                chunks.append(text)
                break
            # Try to split at double newline (paragraph)
            cut = text.rfind("\n\n", 0, limit)
            if cut > 200:
                chunks.append(text[:cut])
                text = text[cut + 2:]
                continue
            # Try single newline
            cut = text.rfind("\n", 0, limit)
            if cut > 200:
                chunks.append(text[:cut])
                text = text[cut + 1:]
                continue
            # Try space
            cut = text.rfind(" ", 0, limit)
            if cut > 200:
                chunks.append(text[:cut])
                text = text[cut + 1:]
                continue
            # Hard cut as absolute last resort (e.g. giant code block)
            chunks.append(text[:limit])
            text = text[limit:]
        return chunks if chunks else [text]
    except Exception as exc:
        print(f"  [_smart_chunks error:{exc}]")
        # Ultimate fallback: brute force split
        return [text[i:i + limit] for i in range(0, len(text), limit)]


async def _safe_edit(msg, text, use_html=False):
    if msg is None:
        return
    try:
        kw = {"parse_mode": ParseMode.HTML} if use_html else {}
        await msg.edit_text(text, **kw)
    except Exception as e:
        if "not modified" not in str(e).lower():
            if use_html:
                try:
                    await msg.edit_text(text)
                except Exception:
                    pass
            else:
                print(f"  [edit err:{e}]")


async def _send_html(update, text):
    """Send as HTML with smart chunking. NEVER raises."""
    try:
        for chunk in _smart_chunks(text):
            # Ensure chunk is under Telegram's hard limit
            if len(chunk) > 4096:
                # Split further and send as plain text
                for sub in _smart_chunks(chunk, limit=3500):
                    plain = re.sub(r"<[^>]+>", "", sub)
                    try:
                        await update.message.reply_text((plain or sub)[:4096])
                    except Exception:
                        pass
                continue
            try:
                await update.message.reply_text(
                    chunk, parse_mode=ParseMode.HTML
                )
            except Exception:  # Fallback: strip HTML and send plain
                plain = re.sub(r"<[^>]+>", "", chunk)
                try:
                    await update.message.reply_text((plain or chunk)[:4096])
                except Exception:
                    pass
    except Exception as exc:
        print(f"  [_send_html fatal:{exc}]")
        try:
            plain = re.sub(r"<[^>]+>", "", text)[:4000]
            await update.message.reply_text(plain or "[send error]")
        except Exception:
            pass


async def _delete_or_check(msg):
    try:
        await msg.delete()
    except Exception:
        await _safe_edit(msg, f"{E.OK} DONE")


async def _process_file_request(
    update, action, pattern
):
    import os
    import shlex

    pattern_clean = pattern.strip(" @\n\r")

    # Safely resolve files using Python on HPC to prevent Bash wildcard
    # injection
    p_cl = repr(pattern_clean)
    py_script = f"""import glob, os, sys
try: os.chdir(os.path.expanduser({repr(WORKDIR)}))
except: pass
matches = glob.glob(os.path.expanduser({p_cl}), recursive=True)
if not matches and os.path.exists({p_cl}):
    matches = [{p_cl}]
for match in matches:
    abs_p = os.path.realpath(match)
    if os.path.isfile(abs_p):
        print(abs_p)
"""
    # Fix: pipe via stdin to bypass all Windows SSH argument quoting corruption
    ssh_cmd = _ssh_base() + ["python3", "-"]
    try:
        proc = await asyncio.create_subprocess_exec(
            *ssh_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(py_script.encode()), 30
        )
    except Exception as e:
        await update.message.reply_text(f"{E.ERR} Resolving files failed: {e}")
        return

    files = [f.strip() for f in stdout.decode().splitlines() if f.strip()]
    if not files:
        t_msg = (
            f"{E.ERR} <b>File not found:</b> "
            f"<code>{html.escape(pattern_clean)}</code>\n"
            "Does not exist or matched nothing."
        )
        await update.message.reply_text(t_msg, parse_mode=ParseMode.HTML)
        return
    if len(files) > 10:
        t_msg = (
            f"{E.WARN} Wildcard matched {len(files)} files. "
            "Limiting to first 10."
        )
        await update.message.reply_text(t_msg, parse_mode=ParseMode.HTML)
        files = files[:10]

    # -- RCLONE UPLOAD --
    if action == "upload":
        bash_cmds = []
        if SETUP_CMD:
            bash_cmds.append(SETUP_CMD)
        for f in files:
            bash_cmds.append(
                f"rclone copy {shlex.quote(f)} "
                f"{shlex.quote(RCLONE_DEST)}/ --progress"
            )
        script = "\n".join(bash_cmds)
        try:
            cmd = _ssh_base() + ["bash", "-ls"]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            out, _ = await asyncio.wait_for(
                proc.communicate(script.encode()), 1800
            )
            t_res = (
                f"{E.OK} <b>Upload complete!</b> ({len(files)} files)\n"
                f"<pre>{html.escape(out.decode()[:2500])}</pre>"
            )
            await update.message.reply_text(
                t_res, parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            await update.message.reply_text(
                f"{E.ERR} Rclone upload failed: {e}"
            )
        return

    # -- SECURE FILE TRANSFER --
    for f in files:
        bash_script = f"""
abs={shlex.quote(f)}
size=$(stat -c%s "$abs")
if (( size > 40000000 )); then
  echo "ERR: File too large. Max 40MB." >&2
  exit 1
fi
cat "$abs"
"""
        cmd = _ssh_base() + ["bash", "-s"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=1024 * 1024 * 45,
            )
            data, stderr = await asyncio.wait_for(
                proc.communicate(bash_script.encode()), 120
            )
        except Exception as e:
            await update.message.reply_text(
                f"{E.ERR} Fetch failed for {os.path.basename(f)}: {e}"
            )
            continue

        if proc.returncode != 0:
            err_msg = (
                stderr.decode(errors="replace").strip() or "Unknown error"
            )
            f_base = html.escape(os.path.basename(f))
            e_html = html.escape(err_msg)
            await update.message.reply_text(
                f"{E.ERR} <b>Cannot fetch file:</b> <code>{f_base}</code>\n"
                f"<i>{e_html}</i>",
                parse_mode=ParseMode.HTML,
            )
            continue

        filename = os.path.basename(f)
        try:
            if filename.lower().endswith((".png", ".jpg", ".jpeg", ".gif")):
                try:
                    await update.message.reply_photo(
                        photo=data, caption=f"File: {filename}"
                    )
                except Exception as ex1:
                    # Fallback for dimensions/compression limits (Telegram
                    # rejects large wide/tall plots)
                    print(
                        f"  [photo upload failed for {filename}: {ex1}. "
                        "Retrying as document.]"
                    )
                    await update.message.reply_document(
                        document=data,
                        filename=filename,
                        caption=(
                            f"File: {filename} (sent as document due to "
                            "dimension limits)"
                        ),
                    )
            else:
                await update.message.reply_document(
                    document=data,
                    filename=filename,
                    caption=f"File: {filename}",
                )
        except Exception as e:
            await update.message.reply_text(
                f"{E.ERR} Telegram Upload failed for {filename}: {e}"
            )


async def _extract_user_prompt_and_files(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int
) -> Tuple[str, List[str]]:
    """Extract user text/caption and download attachments locally."""
    msg = update.message
    raw = ((msg.text or msg.caption) or "").strip()
    files: List[str] = []
    attachment_kinds: List[str] = []

    if msg.photo:
        try:
            largest = msg.photo[-1]
            tg_file = await context.bot.get_file(largest.file_id)
            ts = int(time.time() * 1000)
            f_uid = largest.file_unique_id
            fname = f"tg_{chat_id}_{ts}_{f_uid}.jpg"
            local_path = INBOX_DIR / fname
            await tg_file.download_to_drive(custom_path=str(local_path))
            files.append(str(local_path))
            attachment_kinds.append("image")
        except Exception as e:
            print(f"  [photo download failed: {e}]")

    doc = msg.document
    if doc and (doc.mime_type or "").startswith("image/"):
        try:
            tg_file = await context.bot.get_file(doc.file_id)
            suffix = Path(doc.file_name).suffix if doc.file_name else ""
            if not suffix:
                mime = (doc.mime_type or "").split("/")[-1] or "bin"
                suffix = "." + mime
            ts = int(time.time() * 1000)
            f_uid = doc.file_unique_id
            fname = f"tg_{chat_id}_{ts}_{f_uid}{suffix}"
            local_path = INBOX_DIR / fname
            await tg_file.download_to_drive(custom_path=str(local_path))
            files.append(str(local_path))
            attachment_kinds.append("image")
        except Exception as e:
            print(f"  [image document download failed: {e}]")

    if msg.voice:
        try:
            voice = msg.voice
            v_mime = getattr(voice, 'mime_type', None)
            v_dur = getattr(voice, 'duration', None)
            print(
                f"  [VOICE] received file_id={voice.file_id} "
                f"mime={v_mime} duration={v_dur}"
            )
            tg_file = await context.bot.get_file(voice.file_id)
            ts = int(time.time() * 1000)
            f_uid = voice.file_unique_id
            fname = f"tg_{chat_id}_{ts}_{f_uid}.ogg"
            local_path = INBOX_DIR / fname
            await tg_file.download_to_drive(custom_path=str(local_path))
            print(f"  [VOICE] downloaded to {local_path}")
            files.append(str(local_path))
            attachment_kinds.append("voice")
        except Exception as e:
            print(f"  [voice download failed: {e}]")

    if msg.audio:
        try:
            audio = msg.audio
            a_mime = getattr(audio, 'mime_type', None)
            a_dur = getattr(audio, 'duration', None)
            print(
                f"  [AUDIO] received file_id={audio.file_id} "
                f"mime={a_mime} duration={a_dur}"
            )
            tg_file = await context.bot.get_file(audio.file_id)
            a_nm = audio.file_name
            sfx = Path(a_nm).suffix if a_nm else ".audio"
            ts = int(time.time() * 1000)
            u_id = audio.file_unique_id
            fn_t = f"t{chat_id}_{int(ts/1000)}_{u_id}"
            fn = f"{fn_t}{sfx}"
            local_path = INBOX_DIR / fn
            await tg_file.download_to_drive(custom_path=str(local_path))
            print(f"  [AUDIO] downloaded to {local_path}")
            files.append(str(local_path))
            attachment_kinds.append("audio")
        except Exception as e:
            print(f"  [audio download failed: {e}]")

    if msg.video_note:
        try:
            vn = msg.video_note
            vn_dur = getattr(vn, 'duration', None)
            print(
                f"  [VIDEO_NOTE] received file_id={vn.file_id} "
                f"duration={vn_dur}"
            )
            tg_file = await context.bot.get_file(vn.file_id)
            ts = int(time.time() * 1000)
            f_uid = vn.file_unique_id
            fname = f"tg_{chat_id}_{ts}_{f_uid}.mp4"
            local_path = INBOX_DIR / fname
            await tg_file.download_to_drive(custom_path=str(local_path))
            print(f"  [VIDEO_NOTE] downloaded to {local_path}")
            files.append(str(local_path))
            attachment_kinds.append("video_note")
        except Exception as e:
            print(f"  [video_note download failed: {e}]")

    if files and not raw:
        kinds = set(attachment_kinds)
        if kinds <= {"voice", "audio", "video_note"}:
            audio_path = files[0]
            try:
                transcript = await asyncio.to_thread(
                    _transcribe_audio_file, audio_path
                )
            except Exception as e:
                print(f"  [VOICE] transcription failed: {e}")
                transcript = ""
            if transcript:
                tr_cap = repr(transcript[:200])
                print(f"  [VOICE] transcript: {tr_cap}")
                files = []
                raw = (
                    "The user sent a voice message. Below is the "
                    "transcript converted locally with faster-whisper. "
                    "Treat the transcript as the real user input and "
                    "respond accordingly.\n\n"
                    f"[voice transcript]\n{transcript}"
                )
            else:
                if CONNECTION_MODE == "ssh":
                    raw = (
                        "Local Whisper transcription is required in SSH/HPC "
                        "relay mode because the audio file exists on the "
                        "relay machine, not on the remote host. Please "
                        "install and enable a local Whisper model on the "
                        "relay machine first."
                    )
                else:
                    raw = (
                        "Local Whisper transcription is not available yet. "
                        "Please install and enable a local Whisper model "
                        "on the relay machine first."
                    )
        elif kinds <= {"image"}:
            raw = "Please analyze the attached image."
            raw = "Please analyze the attached image."

    return raw, files


def _get_whisper_model():
    global _WHISPER_MODEL
    if _WHISPER_MODEL is None:
        _WHISPER_MODEL = WhisperModel(
            WHISPER_MODEL_SIZE, device=WHISPER_DEVICE
        )
    return _WHISPER_MODEL


def _transcribe_audio_file(path: str) -> str:
    model = _get_whisper_model()
    segments, info = model.transcribe(
        path, vad_filter=True, beam_size=5
    )
    parts = []
    for seg in segments:
        txt = (seg.text or '').strip()
        if txt:
            parts.append(txt)
    s_join = ' '.join(parts)
    return s_join.strip()


def _fallback_models() -> List[str]:
    """Static fallback when opencode model enumeration is unavailable."""
    return [
        "github-copilot/gpt-4o",
        "github-copilot/gpt-4.1",
        "github-copilot/gpt-5",
        "github-copilot/gpt-5-mini",
        "github-copilot/claude-sonnet-4.5",
        "github-copilot/claude-opus-4.5",
        "github-copilot/gemini-2.5-pro",
        DEFAULT_MODEL,
    ]


async def _list_available_models() -> List[str]:
    """Fetch model list dynamically (used by /model flow).

    Falls back to a curated static list when opencode cannot enumerate models
    (common on DB lock / transient runtime issues in WSL).
    """
    script = f"{_safe_path(OPENCODE)} models"
    if CONNECTION_MODE in ("wsl", "local"):
        cmd = ["bash", "-lc", script]
    else:
        cmd = _ssh_base() + ["bash", "-lc", script]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(proc.communicate(), 25)
        txt = (out or b"").decode(errors="replace")
        etxt = (err or b"").decode(errors="replace")
        models = [
            ln.strip()
            for ln in txt.splitlines()
            if ln.strip() and "/" in ln and " " not in ln.strip()
        ]
        models = sorted(set(models))
        if models:
            return models
        if etxt.strip():
            msg = f"  [_lm error_out: {etxt[:200]}]"
            print(msg)
    except Exception as e:
        print(f"  [_list_available_models error: {e}]")

    fb = sorted(set(_fallback_models()))
    msg = f"  [_list_available_models fallback -> {len(fb)} mod.]"
    print(msg)
    return fb


def _models_by_provider(models: List[str]) -> dict:
    grouped = {}
    for m in models:
        provider = m.split("/", 1)[0]
        grouped.setdefault(provider, []).append(m)
    for k in grouped:
        grouped[k] = sorted(grouped[k])
    return dict(sorted(grouped.items()))


def _parse_schedule_text(text: str, task_prompt: str = "") -> Optional[dict]:
    t = (text or "").strip().lower()
    m = re.match(r"^every\s+(\d+)\s+(minute|minutes|hour|hours|day|days)$", t)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if unit.endswith('s') is False:
            unit += 's'
        return {
            "type": "interval", "interval": n,
            "unit": unit, "task": task_prompt
        }

    m = re.match(r"^(?:daily\s+|at\s+)(\d{1,2}):(\d{2})$", t)
    if m:
        h = int(m.group(1))
        mi = int(m.group(2))
        if 0 <= h <= 23 and 0 <= mi <= 59:
            return {
                "type": "daily", "hour": h, "minute": mi, "task": task_prompt
            }

    r_once = r"^once\s+(\d{1,2}):(\d{2})$"
    m = re.match(r_once, t)
    if m:
        h = int(m.group(1))
        mi = int(m.group(2))
        if 0 <= h <= 23 and 0 <= mi <= 59:
            return {
                "type": "once", "hour": h, "minute": mi, "task": task_prompt
            }

    m = re.match(r"^after\s+(\d+)\s+(minute|minutes|hour|hours)$", t)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        now = datetime.now()
        if 'min' in unit:
            diff = timedelta(minutes=n)
        else:
            diff = timedelta(hours=n)
        target = now + diff
        return {
            "type": "once", "hour": target.hour,
            "minute": target.minute, "task": task_prompt
        }
    return None


def _is_allowed_update(update: Update) -> bool:
    chat_id = update.effective_chat.id if update.effective_chat else None
    uid = update.effective_user.id if update.effective_user else None
    return bool(ALLOWED_IDS & {chat_id, uid})


async def _cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed_update(update):
        return
    chat_id = update.effective_chat.id
    uid = update.effective_user.id if update.effective_user else None
    ws = _resolve_workspace(chat_id, uid)
    ws_sf = ws["sessions_file"]
    update_chat(chat_id, ws_sf, session_id="__new__", running=False)
    cur_model = get_chat(chat_id, ws_sf).get("model", DEFAULT_MODEL)
    m_esc = html.escape(cur_model)
    await update.message.reply_text(
        f"{E.OK} <b>New session queued</b>\n{E.BOT} <code>{m_esc}</code>",
        parse_mode=ParseMode.HTML,
    )


async def _cmd_send_like(
        update: Update, context: ContextTypes.DEFAULT_TYPE, action: str):
    if not _is_allowed_update(update):
        return
    if not context.args:
        await update.message.reply_text(
            f"{E.WARN} Usage: <code>/{action} &lt;file_or_glob&gt;</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    pattern = " ".join(context.args).strip()
    prog = 'Uploading to gdrive' if action == 'upload' else 'Fetching file(s)'
    await update.message.reply_text(
        f"{E.WAIT} <i>{prog}...</i>",
        parse_mode=ParseMode.HTML,
    )
    await _process_file_request(update, action, pattern)


async def _cmd_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _cmd_send_like(update, context, "send")


async def _cmd_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _cmd_send_like(update, context, "upload")


async def _cmd_kill(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed_update(update):
        return
    chat_id = update.effective_chat.id
    uid = update.effective_user.id if update.effective_user else None
    ws = _resolve_workspace(chat_id, uid)
    ws_sf = ws["sessions_file"]
    chat = get_chat(chat_id, ws_sf)
    killed = _kill_session_processes(
        chat.get("session_id"), ws.get("workdir")
    )
    update_chat(
        chat_id, ws_sf, kill_requested="yes", running=False,
        running_started=0, running_prompt="", pending_messages=[]
    )
    extra = ""
    if killed:
        extra = f" Killed {killed} lingering opencode process(es)."
    await update.message.reply_text(
        f"{E.OK} Kill-all sent. Cleared queued messages.{extra}"
    )


async def _cmd_q(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /q as quick quit (same as /kill)
    await _cmd_kill(update, context)


async def _opencode_stats_summary(days: int = 7) -> dict:
    """Best-effort parse of `opencode stats` output into key metrics."""
    script = f"{_safe_path(OPENCODE)} stats --days {int(days)} --models 5"
    if CONNECTION_MODE in ("wsl", "local"):
        cmd = ["bash", "-lc", script]
    else:
        cmd = _ssh_base() + ["bash", "-lc", script]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        raw, _ = await asyncio.wait_for(proc.communicate(), 20)
        clean = ANSI_RE.sub("", raw.decode(errors="replace"))
        pats = {
            "sessions": r"^\s*Sessions\s+([0-9.,KMB]+)\s*$",
            "messages": r"^\s*Messages\s+([0-9.,KMB]+)\s*$",
            "input": r"^\s*Input\s+([0-9.,KMB]+)\s*$",
            "output": r"^\s*Output\s+([0-9.,KMB]+)\s*$",
            "cache_read": r"^\s*Cache Read\s+([0-9.,KMB]+)\s*$",
            "cache_write": r"^\s*Cache Write\s+([0-9.,KMB]+)\s*$",
            "avg_tok_sess": r"^\s*Avg Tokens/Session\s+([0-9.,KMB]+)\s*$",
            "total_cost": r"^\s*Total Cost\s+\$?([0-9.,]+)\s*$",
        }
        out = {}
        for k, pat in pats.items():
            m = re.search(pat, clean, re.MULTILINE)
            if m:
                out[k] = m.group(1)
        mm_pat = r"^\s*([a-zA-Z0-9._-]+/[a-zA-Z0-9._-]+)\s*$"
        mm = re.search(mm_pat, clean, re.MULTILINE)
        if mm:
            out["top_model"] = mm.group(1)
        return out
    except Exception as e:
        msg = f"  [_opencode_stats_summary error: {e}]"
        print(msg)
        return {}


async def _cmd_model(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed_update(update):
        return
    c_id = update.effective_chat.id
    u_id = update.effective_user.id if update.effective_user else None
    ws = _resolve_workspace(c_id, u_id)
    ws_sf = ws["sessions_file"]
    m_msg = f"{E.WAIT} Loading model providers..."
    msg = await update.message.reply_text(m_msg)
    models = await _list_available_models()
    await _delete_or_check(msg)
    if not models:
        await update.message.reply_text(f"{E.ERR} Failed to load model list.")
        return
    update_chat(c_id, ws_sf, model_catalog=models)
    grouped = _models_by_provider(models)
    buttons = []
    for prov, ms in grouped.items():
        btn = InlineKeyboardButton(
            f"{prov} ({len(ms)})", callback_data=f"mdlprov:{prov}"
        )
        buttons.append([btn])
    t_p = f"{E.BOT} <b>Select provider</b>"
    await update.message.reply_text(
        t_p, parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def _cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed_update(update):
        return
    chat_id = update.effective_chat.id
    uid = update.effective_user.id if update.effective_user else None
    ws = _resolve_workspace(chat_id, uid)
    ws_sf = ws["sessions_file"]
    chat = get_chat(chat_id, ws_sf)
    if context.args:
        req_sid = context.args[0].strip()
        req_esc = html.escape(req_sid)
        if not req_sid.startswith("ses_"):
            await update.message.reply_text(
                f"{E.ERR} <b>Invalid session ID:</b> <code>{req_esc}</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        status_msg = await update.message.reply_text(
            f"{E.WAIT} Checking session on HPC..."
        )
        exists = await _is_valid_session(req_sid)
        if status_msg is not None:
            await _delete_or_check(status_msg)
        if not exists:
            msg_t = f"{E.ERR} <b>Session not found:</b> <code>{req_esc}</code>"
            await update.message.reply_text(
                msg_t, parse_mode=ParseMode.HTML,
            )
            return
        update_chat(chat_id, ws_sf, session_id=req_sid)
        sid_esc = html.escape(req_sid)
        m_esc = html.escape(chat.get('model', DEFAULT_MODEL))
        await update.message.reply_text(
            f"{E.OK} <b>Session</b> <code>{sid_esc}</code>\n"
            f"{E.BOT} <code>{m_esc}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    # no id passed: show recent sessions for quick pick
    sessions = await _fetch_hpc_sessions()
    if not sessions:
        msg_t = (
            f"{E.WARN} No sessions found.\n"
            "Use <code>/new</code> to start one."
        )
        await update.message.reply_text(msg_t, parse_mode=ParseMode.HTML)
        return
    buttons = []
    for sid, title in sessions[:12]:
        label = (title or sid)[:40]
        btn = InlineKeyboardButton(label, callback_data=f"sidset:{sid}")
        buttons.append([btn])
    s_id = chat.get('session_id')
    cur_esc = html.escape(_display_sid(s_id))
    await update.message.reply_text(
        f"{E.SESS} <b>Select a recent session</b>\n"
        f"Current: <code>{cur_esc}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def _on_callback_query(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if not _is_allowed_update(update):
        return
    q = update.callback_query
    await q.answer()
    c_id = q.message.chat_id
    u_id = q.from_user.id if q.from_user else None
    ws = _resolve_workspace(c_id, u_id)
    ws_sf = ws["sessions_file"]
    ws_tf = ws["tasks_file"]
    data = q.data or ""

    if data == "st:back":
        update_chat(c_id, ws_sf, pending_edit={})
        await _render_status(q.message, c_id, u_id, ws, is_callback=True)
        return

    if data == "st:id":
        update_chat(c_id, ws_sf, pending_edit={})
        sessions = await _fetch_hpc_sessions()
        if not sessions:
            kb = [[InlineKeyboardButton("⬅️ Back", callback_data="st:back")]]
            ikb = InlineKeyboardMarkup(kb)
            e_m = f"{E.WARN} No sessions found."
            await q.edit_message_text(e_m, reply_markup=ikb)
            return
        rows = []
        for sid, title in sessions[:12]:
            label = (title or sid)[:40]
            cb = f"sidset:{sid}"
            rows.append([InlineKeyboardButton(label, callback_data=cb)])
        b_st = InlineKeyboardButton("⬅️ Back", callback_data="st:back")
        rows.append([b_st])
        cur_sd = get_chat(c_id, ws_sf).get("session_id")
        h_sid = html.escape(_display_sid(cur_sd))
        txt = f"{E.SESS} <b>Session</b> (Current: <code>{h_sid}</code>)"
        await q.edit_message_text(
            txt, parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data == "st:sch" or data == "schL":
        update_chat(c_id, ws_sf, pending_edit={})
        await _render_schedule_list(c_id, q, ws_tf)
        return

    if data.startswith("mdlprov:"):
        provider = data.split(":", 1)[1]
        models = get_chat(c_id, ws_sf).get("model_catalog") or []
        grouped = _models_by_provider(models)
        options = grouped.get(provider, [])
        if not options:
            await q.edit_message_text(f"{E.ERR} Provider has no models.")
            return
        cur_model = get_chat(c_id, ws_sf).get("model", DEFAULT_MODEL)
        rows = []
        for m in options:
            name = m.split('/', 1)[1]
            label = ("✅ " + name) if m == cur_model else name
            btn = InlineKeyboardButton(label, callback_data=f"mdlset:{m}")
            rows.append([btn])
        rows.append(
            [InlineKeyboardButton("⬅️ Back", callback_data="mdlback")]
        )
        prov_esc = html.escape(provider)
        await q.edit_message_text(
            f"{E.BOT} <b>{prov_esc}</b> \u2014 select model",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(rows)
        )
        return

    if data == "mdlback":
        models = get_chat(c_id, ws_sf).get("model_catalog") or []
        grouped = _models_by_provider(models)
        btns = []
        for prov, ms in grouped.items():
            b = InlineKeyboardButton(
                f"{prov} ({len(ms)})", callback_data=f"mdlprov:{prov}"
            )
            btns.append([b])
        await q.edit_message_text(
            f"{E.BOT} <b>Select provider</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(btns),
        )
        return

    if data.startswith("mdlset:"):
        model = data.split(":", 1)[1]
        update_chat(c_id, ws_sf, model=model)
        m_esc = html.escape(model)
        await q.edit_message_text(
            f"{E.OK} <b>Model switched</b>\n"
            f"{E.BOT} <code>{m_esc}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    if data.startswith("schs:"):
        update_chat(c_id, ws_sf, pending_edit={})
        tid = data.split(":", 1)[1]
        await _render_schedule_detail(q, tid, ws_tf)
        return

    if data.startswith("sidset:"):
        update_chat(c_id, ws_sf, pending_edit={})
        s_id = data.split(":", 1)[1]
        update_chat(c_id, ws_sf, session_id=s_id)
        s_esc = html.escape(s_id)
        kb = [[InlineKeyboardButton("⬅️ Back", callback_data="st:back")]]
        await q.edit_message_text(
            f"{E.OK} <b>Session switched</b>\n"
            f"{E.SESS} <code>{s_esc}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(kb),
        )
        return

    if data.startswith("schd:"):
        tid = data.split(":", 1)[1]
        tf, tasks = _find_task_file(tid, ws_tf)
        if tid in tasks:
            del tasks[tid]
            _save_scheduled_tasks(tasks, tf)
            _reschedule_all_tasks(tasks)
            t_esc = html.escape(tid)
            await q.edit_message_text(
                f"{E.OK} Deleted <code>{t_esc}</code>",
                parse_mode=ParseMode.HTML
            )
        else:
            await q.edit_message_text(f"{E.ERR} Task not found.")
        return

    if data.startswith("sche:"):
        tid = data.split(":", 1)[1]
        await _render_schedule_edit_menu(q, tid, ws_tf)
        return

    if data.startswith("schef:"):
        parts = data.split(":", 2)
        if len(parts) < 3:
            await q.edit_message_text(f"{E.ERR} Invalid edit action")
            return
        tid, field = parts[1], parts[2]
        p_edit = {
            "type": "schedule", "task_id": tid, "field": field
        }
        update_chat(c_id, ws_sf, pending_edit=p_edit)
        if field == "time":
            hint = (
                "Send new time format, e.g. `every 2 hours` / "
                "`daily 09:30` / `at 18:00` / `after 30 minutes`"
            )
        elif field == "name":
            hint = "Send new task name text."
        else:
            hint = "Send new task content/prompt text."
        f_esc = html.escape(field)
        kb = [[InlineKeyboardButton("Cancel", callback_data=f"schs:{tid}")]]
        await q.edit_message_text(
            f"{E.WAIT} <b>Waiting for new {f_esc}...</b>\n{hint}\n\n"
            "This will overwrite the original value.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(kb),
        )
        return

    if data.startswith("schu:"):
        parts = data.split(":")
        # schu:<tid>:<mode>:<a>:<b>
        if len(parts) < 5:
            await q.edit_message_text(f"{E.ERR} Invalid edit payload")
            return
        tid, mode, a, b = parts[1], parts[2], parts[3], parts[4]
        tf, tasks = _find_task_file(tid, ws_tf)
        info = tasks.get(tid)
        if not info:
            await q.edit_message_text(f"{E.ERR} Task not found.")
            return

        if mode == "i":
            info["schedule"] = {
                "type": "interval", "interval": int(a),
                "unit": b, "task": info.get("task", "")
            }
        elif mode == "d":
            info["schedule"] = {
                "type": "daily", "hour": int(a),
                "minute": int(b), "task": info.get("task", "")
            }
        elif mode == "o":
            now = datetime.now()
            if b == "minutes":
                delta = timedelta(minutes=int(a))
            else:
                delta = timedelta(hours=int(a))
            target = now + delta
            info["schedule"] = {
                "type": "once", "hour": target.hour,
                "minute": target.minute, "task": info.get("task", "")
            }
        else:
            await q.edit_message_text(f"{E.ERR} Unknown edit mode")
            return

        tasks[tid] = info
        _save_scheduled_tasks(tasks, tf)
        _reschedule_all_tasks(tasks)
        await _render_schedule_detail(q, tid, ws_tf)
        return


def _schedule_desc(info: dict) -> str:
    sched = (info or {}).get("schedule", {})
    t = sched.get("type")
    if t in ("once", "daily"):
        h = int(sched.get("hour", 0))
        m = int(sched.get("minute", 0))
        if t == "daily":
            return f"daily at {h:02d}:{m:02d}"
        return f"once at {h:02d}:{m:02d}"
    interval = int(sched.get("interval", 1))
    unit = sched.get("unit", "minutes")
    return f"every {interval} {unit}"


def _task_label(task_id: str, info: dict) -> str:
    desc = _schedule_desc(info)
    name = (info.get("name", "") or "").strip()
    task_preview = (info.get("task", "") or "").strip()
    label = name if name else task_preview
    if not label:
        label = task_id
    # Keep Telegram button label compact
    label = label[:28]
    return f"{label} · {desc}"


async def _render_schedule_list(chat_id: int, message_obj, ws_tf):
    task_map = _collect_chat_tasks(chat_id, ws_tf)
    rows = []
    if task_map:
        for tid, pair in list(task_map.items())[:20]:
            info, _tf = pair
            label = _task_label(tid, info)
            cb = f"schs:{tid}"
            rows.append([InlineKeyboardButton(label, callback_data=cb)])
    rows.append([InlineKeyboardButton("Refresh", callback_data="schL")])
    b_st = InlineKeyboardButton("⬅️ Back to Status", callback_data="st:back")
    rows.append([b_st])

    text = (
        f"{E.TIME} <b>Scheduled tasks</b>\nTap one to edit/delete."
        if task_map
        else f"{E.WARN} No scheduled tasks."
    )

    # CallbackQuery path (edit existing panel first)
    if hasattr(message_obj, 'data'):
        try:
            await message_obj.edit_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(rows),
            )
            return
        except Exception as e:
            # Fallback: send a new message to chat
            try:
                await message_obj.message.reply_text(
                    text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup(rows),
                )
                return
            except Exception:
                err_msg = f"  [_render_schedule_list callback failed: {e}]"
                print(err_msg)
                return

    # Normal Message path (/scheduled command)
    await message_obj.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def _cmd_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed_update(update):
        return
    chat_id = update.effective_chat.id
    uid = update.effective_user.id if update.effective_user else None
    ws = _resolve_workspace(chat_id, uid)
    await _render_schedule_list(chat_id, update.message, ws["tasks_file"])


async def _render_schedule_detail(q, task_id: str, ws_tf):
    tf, tasks = _find_task_file(task_id, ws_tf)
    info = tasks.get(task_id)
    if not info:
        t_esc = html.escape(task_id)
        await q.edit_message_text(
            f"{E.ERR} Task not found: <code>{t_esc}</code>",
            parse_mode=ParseMode.HTML
        )
        return
    desc = _schedule_desc(info)
    t_name = html.escape((info.get("name", "") or "(no name)")[:120])
    task_text = html.escape((info.get("task", "") or "")[:200])
    kb_list = [
        [
            InlineKeyboardButton(
                "🗑 Delete", callback_data=f"schd:{task_id}"
            ),
            InlineKeyboardButton("✏️ Edit", callback_data=f"sche:{task_id}")
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data="schL")],
    ]
    kb = InlineKeyboardMarkup(kb_list)
    await q.edit_message_text(
        f"{E.TIME} <b>Task</b> <code>{html.escape(task_id)}</code>\n"
        f"Name: <b>{t_name}</b>\n"
        f"Schedule: <b>{html.escape(desc)}</b>\n"
        f"Prompt: <code>{task_text}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


async def _render_schedule_edit_menu(q, task_id: str, ws_tf):
    tf, tasks = _find_task_file(task_id, ws_tf)
    if task_id not in tasks:
        await q.edit_message_text(f"{E.ERR} Task not found.")
        return
    bt = InlineKeyboardButton
    kbl = [
        [bt("Edit name", callback_data=f"schef:{task_id}:name")],
        [bt("Edit time", callback_data=f"schef:{task_id}:time")],
        [bt("Edit content", callback_data=f"schef:{task_id}:task")],
        [bt("⬅️ Back", callback_data=f"schs:{task_id}")],
    ]
    kb = InlineKeyboardMarkup(kbl)
    await q.edit_message_text(
        f"{E.TOOL} <b>Edit task</b>\nChoose what to edit:",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


def _reschedule_all_tasks(tasks: Optional[Dict] = None):
    schedule.clear()
    all_task_files = {TASKS_FILE}
    for _cid, ws_cfg in CHANNEL_WORKSPACES.items():
        n = ws_cfg.get("name")
        if n:
            n_file = f"hpc_relay_tasks_{n}.json"
            all_task_files.add(str(SCRIPT_DIR / n_file))
    if AUTO_WORKSPACE_PER_CHAT:
        glob_p = f"hpc_relay_tasks_{AUTO_WORKSPACE_PREFIX}_*.json"
        for tf in SCRIPT_DIR.glob(glob_p):
            all_task_files.add(str(tf))
    for tf in all_task_files:
        for tid, tinfo in _load_scheduled_tasks(tf).items():
            tinfo.setdefault("tasks_file", tf)
            _schedule_job(tid, tinfo)


async def _render_status(message_obj, chat_id, uid, ws, is_callback=False):
    ws_sf = ws["sessions_file"]
    ws_tf = ws["tasks_file"]
    ws_wd = ws["workdir"]
    ws_name = ws.get("name") or "default"

    t0 = time.time()
    wait = None
    if not is_callback:
        wait = await message_obj.reply_text(f"{E.WAIT} Gathering status...")

    _hpc_sessions = await _fetch_hpc_sessions()
    t_sess = time.time() - t0

    if wait:
        await _delete_or_check(wait)

    chat = get_chat(chat_id, ws_sf)
    model = chat.get("model", DEFAULT_MODEL)
    sid = _display_sid(chat.get("session_id"))
    known = len(_hpc_sessions)
    tasks = _chat_task_count(chat_id, ws_tf)

    pending = chat.get("pending_edit")
    if pending:
        p_tid = pending.get('task_id', '?')
        p_fld = pending.get('field', '?')
        pending_txt = f"active ({p_tid} / {p_fld})"
    else:
        pending_txt = "none"

    lines = [
        "📌 <b>Relay Status</b>",
        f"🤖 <b>Model ID:</b> <code>{html.escape(model)}</code>",
    ]

    if sid and sid != "new session" and sid.lower() != "none":
        lines.append(f"🧵 <b>Session ID:</b> <code>{html.escape(sid)}</code>")

    lines += [
        f"🧩 <b>WS:</b> <code>{html.escape(ws_name)}</code>",
        f"📁 <b>Dir:</b> <code>{html.escape(ws_wd)}</code>",
        f"🗂 <b>Sessions:</b> <code>{known}</code>",
        f"⏰ <b>Tasks:</b> <code>{tasks}</code>",
        f"📝 <b>Edit:</b> <code>{html.escape(pending_txt)}</code>",
    ]

    footer = f"\n<i>Sessions fetch: {t_sess:.1f}s</i>"
    lines.append(footer)

    kb = [
        [
            InlineKeyboardButton("🗂 Sessions", callback_data="st:id"),
            InlineKeyboardButton("⏰ Tasks", callback_data="st:sch"),
        ]
    ]

    if is_callback:
        await message_obj.edit_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(kb),
        )
    else:
        await message_obj.reply_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(kb),
        )


async def _cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed_update(update):
        return
    c_id = update.effective_chat.id
    u_id = update.effective_user.id if update.effective_user else None
    ws = _resolve_workspace(c_id, u_id)
    await _render_status(update.message, c_id, u_id, ws)


# ================================================================
#  MAIN HANDLER
# ================================================================
async def _cmd_debugws(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_allowed_update(update):
        return
    chat_id = update.effective_chat.id
    uid = update.effective_user.id if update.effective_user else None
    ws = _resolve_workspace(chat_id, uid)
    ws_sf = ws["sessions_file"]
    ws_tf = ws["tasks_file"]
    ws_wd = ws["workdir"]
    ws_name = ws.get("name") or "default"
    chat = get_chat(chat_id, ws_sf)
    s_id = str(chat.get('session_id'))
    m_name = chat.get('model', DEFAULT_MODEL)
    s_cnt = len(_load_scheduled_tasks(ws_tf))
    c_cnt = len(_collect_chat_tasks(chat_id, ws_tf))
    m_esc = html.escape(m_name or DEFAULT_MODEL)
    s_id_str = str(s_id)
    s_esc = html.escape(s_id_str)
    w_esc = html.escape(ws_name)
    d_esc = html.escape(ws_wd)
    s_esc = html.escape(ws_sf)
    t_esc = html.escape(ws_tf)
    text = (
        "🧪 <b>WS Debug</b>\n"
        f"chat: <code>{chat_id}</code>\n"
        f"user: <code>{uid}</code>\n"
        f"ws: <code>{w_esc}</code>\n"
        f"dir: <code>{d_esc}</code>\n"
        f"sf: <code>{s_esc}</code>\n"
        f"tf: <code>{t_esc}</code>\n"
        f"mdl: <code>{m_esc}</code>\n"
        f"sid: <code>{s_esc}</code>\n"
        f"sc: <code>{s_cnt}</code> | cs: <code>{c_cnt}</code>"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    uid = update.effective_user.id if update.effective_user else None
    print(f"\n{'=' * 50}\nINCOMING  chat={chat_id}  user={uid}")

    # Allow if IT IS an allowed chat, OR an allowed user is speaking in
    # another chat (e.g., a group)
    if not (ALLOWED_IDS & {chat_id, uid}):
        return

    # Resolve workspace for this chat
    ws = _resolve_workspace(chat_id, uid)
    ws_sf = ws["sessions_file"]
    if ws["allowed_users"] and uid not in ws["allowed_users"]:
        print(f"  BLOCKED: user {uid} not in workspace allowed_users")
        return

    res = await _extract_user_prompt_and_files(update, context, chat_id)
    raw, attached_files = res
    if not raw and not attached_files:
        return
    if attached_files and not raw:
        raw = "Please analyze the attached image."

    # normalize plain schedule keyword
    if (raw or "").strip().lower() == "schedule":
        raw = "scheduled"

    # pending interactive edits (e.g. /schedule -> Edit -> send new value)
    chat_state = get_chat(chat_id, ws_sf)
    pending = chat_state.get("pending_edit")
    if pending and raw and not raw.startswith("/"):
        if pending.get("type") == "schedule":
            task_id = pending.get("task_id")
            field = pending.get("field")
            tf, tasks = _find_task_file(task_id, ws_tf)
            info = tasks.get(task_id)
            if not info:
                update_chat(chat_id, ws_sf, pending_edit={})
                m_txt = f"{E.ERR} Task not found anymore."
                await update.message.reply_text(m_txt)
                return
            if field == "time":
                parsed = _parse_schedule_text(raw, info.get("task", ""))
                if not parsed:
                    await update.message.reply_text(
                        f"{E.ERR} Invalid format. Try:\n"
                        "`every 2 hours` / `daily 09:30` / "
                        "`at 18:00` / `after 2 min`"
                    )
                    return
                info["schedule"] = parsed
            elif field == "name":
                info["name"] = raw.strip()
            else:
                info["task"] = raw.strip()
                # keep schedule.task in sync for older codepaths
                if isinstance(info.get("schedule"), dict):
                    info["schedule"]["task"] = raw.strip()
            tasks[task_id] = info
            _save_scheduled_tasks(tasks, tf)
            _reschedule_all_tasks(tasks)
            update_chat(chat_id, ws_sf, pending_edit={})
            t_esc = html.escape(task_id)
            f_esc = html.escape(field)
            p_o = f"({f_esc} overwritten)."
            await update.message.reply_text(
                f"{E.OK} Updated <code>{t_esc}</code> {p_o}",
                parse_mode=ParseMode.HTML
            )
            return

    is_preempt = False

    # Check mention / priority override
    if raw.startswith("@"):
        bot_username = context.bot.username
        m_mention = re.match(r"^@(\w+)", raw)
        if m_mention:
            mentioned = m_mention.group(1)
            if bot_username and mentioned.lower() == bot_username.lower():
                raw = re.sub(r"^@\w+\s*", "", raw).strip()
                if not raw:
                    return
            else:
                is_preempt = True

    # Fast-path kill command without backgrounding
    if raw.lower() in ("!kill", "/kill", "/q"):
        chat = get_chat(chat_id, ws_sf)
        killed = _kill_session_processes(
            chat.get("session_id"), ws.get("workdir")
        )
        update_chat(
            chat_id, ws_sf, kill_requested="yes", running=False,
            running_started=0, running_prompt="", pending_messages=[]
        )
        try:
            extra = ""
            if killed:
                extra = f" Killed {killed} lingering proc(s)."
            await update.message.reply_text(
                f"{E.OK} Kill-all sent. Cleared queue.{extra}"
            )
        except Exception:
            pass
        return

    # Prevent concurrent opencode runs in the same chat
    # (avoids DB/session lock errors)
    _clear_stale_running_sessions(ws_sf)
    chat_now = get_chat(chat_id, ws_sf)
    if chat_now.get("running"):
        _queue_message(chat_id, ws_sf, raw, attached_files, preempt=is_preempt)
        return

    # Dispatch to background task so the bot immediately accepts new messages
    asyncio.create_task(
        _run_in_background(
            update, context, chat_id, raw, ws, attached_files
        )
    )


async def _typing_loop(context, chat_id, done_event):
    while not done_event.is_set():
        try:
            await context.bot.send_chat_action(
                chat_id=chat_id, action=ChatAction.TYPING
            )
        except Exception:
            pass
        try:
            await asyncio.wait_for(done_event.wait(), timeout=4.0)
        except asyncio.TimeoutError:
            continue


async def _run_in_background(
    update, context, chat_id, raw, ws, attached_files=None
):
    done_event = asyncio.Event()
    typing_task = asyncio.create_task(
        _typing_loop(context, chat_id, done_event)
    )
    ts = int(time.time())
    p_cap = (raw or "")[:120]
    update_chat(
        chat_id, ws["sessions_file"], running=True,
        running_started=ts, running_prompt=p_cap
    )
    try:
        await _handle_message_inner(
            update, context, chat_id, raw, ws, attached_files
        )
    except Exception as exc:
        tb = traceback.format_exc()
        print(f"  UNHANDLED ERROR:\n{tb}")
        try:
            exc_t = type(exc).__name__
            exc_s = str(exc)[:200]
            err_m = f"{E.ERR} Internal error:\n{exc_t}: {exc_s}"
            await update.message.reply_text(err_m)
        except Exception:
            pass
    finally:
        done_event.set()
        try:
            await typing_task
        except Exception:
            pass
        update_chat(
            chat_id, ws["sessions_file"], running=False,
            running_started=0, running_prompt=""
        )
        nxt = _pop_next_message(chat_id, ws["sessions_file"])
        if nxt:
            n_raw = nxt.get("raw", "")
            n_att = nxt.get("attached_files") or []
            print(
                f"  [QUEUE] Dequeued for chat={chat_id}: "
                f"preempt={bool(nxt.get('preempt'))} id={nxt.get('id')}"
            )
            asyncio.create_task(
                _run_in_background(update, context, chat_id, n_raw, ws, n_att)
            )


async def _handle_message_inner(
    update, context, chat_id, raw, ws, attached_files=None
):
    ws_tf = ws["tasks_file"]
    ws_sf = ws["sessions_file"]
    ws_wd = ws["workdir"]
    ws_suffix = ws["system_suffix"]
    ws_name = ws.get("name") or "default"
    print(f"  workspace={ws_name}")
    print(f"  workdir={ws_wd}")
    # Scheduled management is handled via /scheduled command.

    parsed = parse_message(raw)
    chat = get_chat(chat_id, ws_sf)
    model = chat["model"]
    sid = chat["session_id"]
    known_sessions = set(_get_known_sessions(ws_sf) or [])
    sid_ok = (
        sid and sid != "__new__"
        and sid.startswith("ses") and sid in known_sessions
    )
    if sid_ok:
        exists_here = await _session_exists_on_this_host(sid)
        if not exists_here:
            print(
                f"  [SESSION RESET] known session {sid} missing here; "
                "switching to __new__"
            )
            update_chat(chat_id, ws_sf, session_id="__new__")
            sid = "__new__"
    print(f"  model={model}  session={sid}")

    # -- SHELL --
    if parsed["shell"]:
        print(f"  SHELL:{parsed['shell']}")
        status_msg = await update.message.reply_text(f"{E.SHELL} Running...")
        try:
            out = await exec_shell(parsed["shell"], ws_wd)
        except asyncio.TimeoutError:
            out = f"{E.TIME} Timed out after {LOCAL_TIMEOUT_SEC}s."
        except Exception as e:
            out = f"{E.ERR} {type(e).__name__}: {e}"
        hdr = _shell_header_html()
        body = f"<pre>{html.escape(out[:3500])}</pre>"
        await _send_html(update, hdr + body)
        if status_msg is not None:
            await _delete_or_check(status_msg)
        return

    # -- OPENCODE --
    effective_sid = None if sid == "__new__" else sid

    # Clear any previous kill flag
    update_chat(chat_id, ws_sf, kill_requested="no")

    if not parsed.get("prompt"):
        return
    prompt = parsed["prompt"] + ws_suffix
    status_msg = None
    tool_msg = None
    stream_msg = None

    text_buf = []
    last_edit_t = 0.0
    last_output_t = time.time()
    start_t = time.time()
    partial_sent_len = 0
    done_event = asyncio.Event()

    async def _ensure_status_msg(initial_text=None):
        nonlocal status_msg
        if status_msg is None:
            status_msg = await update.message.reply_text(
                initial_text or f"{E.WAIT} Continuing...",
                parse_mode=ParseMode.HTML
            )
        return status_msg

    async def _show_tool_call(text_line):
        nonlocal tool_msg
        if not text_line:
            return
        if tool_msg is None:
            tool_msg = await update.message.reply_text(text_line)
        else:
            await _safe_edit(tool_msg, text_line)

    async def _show_stream_text(text_line):
        nonlocal stream_msg
        if not text_line:
            return
        if stream_msg is None:
            stream_msg = await update.message.reply_text(text_line)
        else:
            await _safe_edit(stream_msg, text_line)
        return stream_msg

    async def on_progress(s):
        nonlocal last_edit_t, last_output_t
        last_output_t = time.time()
        now = time.time()
        elapsed = int(now - start_t)
        low = (s or '').lower()
        boring = (
            'still processing', 'streaming', 'connected -- waiting',
            'started', 'running '
        )
        if any(b in low for b in boring):
            return
        if '🛠' in s or s.startswith(f"{E.TOOL}") or 'subagent' in low:
            await _show_tool_call(s)
            return
        if elapsed >= 120:
            smsg = await _ensure_status_msg(f"{elapsed}s")
            if now - last_edit_t < 1.0:
                return
            last_edit_t = now
            await _safe_edit(smsg, f"{s} [{elapsed}s]")

    async def on_text_chunk(delta):
        nonlocal last_edit_t, last_output_t
        last_output_t = time.time()
        text_buf.append(delta)
        now = time.time()
        if (now - last_edit_t) < STREAM_EDIT_INTERVAL and len(
            delta
        ) < STREAM_MIN_DELTA:
            return
        last_edit_t = now
        joined = "".join(text_buf)
        mx = 3500
        tail = joined[-mx:] if len(joined) > mx else joined
        await _safe_edit(
            status_msg,
            f"{E.WAIT} Streaming... [{int(now - start_t)}s]\n\n{tail}",
        )

    async def stall_monitor():
        nonlocal partial_sent_len
        warn_count = 0
        while not done_event.is_set():
            await asyncio.sleep(10)
            if done_event.is_set():
                break
            silence = time.time() - last_output_t
            elapsed = int(time.time() - start_t)

            # Periodic warnings after TIMEOUT_WARN_SEC (every 2 minutes)
            if elapsed >= TIMEOUT_WARN_SEC and warn_count == 0:
                warn_count += 1
                chars = len("".join(text_buf))
                if text_buf and partial_sent_len == 0:
                    joined = "".join(text_buf)
                    partial_sent_len = len(joined)
                    cur_sid = get_chat(chat_id, ws_sf).get("session_id")
                    hdr = _header_html(
                        model,
                        _display_sid(cur_sid, is_new=(effective_sid is None)),
                    )
                    body = md_to_tg_html(joined)
                    await _send_html(
                        update,
                        hdr + body
                        + f"\n\n{E.WARN} <i>Partial at {elapsed}s "
                        "(still running)</i>",
                    )
                await _safe_edit(
                    status_msg,
                    f"{chars} chars buffered, silent {int(silence)}s\n"
                    "Send /kill to stop.",
                )
                continue

            if (
                silence >= PARTIAL_SEND_SEC
                and text_buf
                and partial_sent_len == 0
            ):
                joined = "".join(text_buf)
                if len(joined) > 50:
                    partial_sent_len = len(joined)
                    cur_sid = get_chat(chat_id, ws_sf).get("session_id")
                    hdr = _header_html(
                        model,
                        _display_sid(cur_sid, is_new=(effective_sid is None)),
                    )
                    body = md_to_tg_html(joined)
                    t_f = (
                        hdr + body
                        + f"\n\n{E.TIME} <i>Stall: {silence:.0f}s silence, "
                        + f"{elapsed}s total</i>"
                    )
                    await _send_html(update, t_f)
                    await _ensure_status_msg(
                        f"{E.WAIT} Continuing... [{elapsed}s]"
                    )
                    await _safe_edit(
                        status_msg, f"{E.OUT} Partial sent [{elapsed}s]"
                    )
                    continue
            if silence >= STALL_WARN_SEC:
                chars = len("".join(text_buf))
                kill_hint = "\nSend /kill to stop."
                if partial_sent_len > 0:
                    st = (
                        f"{E.WARN} Silent {int(silence)}s "
                        f"(total: {elapsed}s)\n"
                        f"Partial sent ({partial_sent_len} chars)"
                        f"{kill_hint}"
                    )
                else:
                    st = (
                        f"{E.WARN} Silent {int(silence)}s "
                        f"(total: {elapsed}s)\n"
                        f"Buffered: {chars} chars{kill_hint}"
                    )
                m_c = f"{E.WAIT} Continuing... [{elapsed}s]"
                await _ensure_status_msg(m_c)
                await _safe_edit(status_msg, st)

    monitor = asyncio.create_task(stall_monitor())
    try:
        final, new_sid = await run_opencode(
            prompt, chat_id, model, effective_sid, on_progress,
            on_text_chunk, attached_files, workdir=ws_wd,
            sessions_file=ws_sf
        )
        if final == QUEUE_PREEMPT_MARKER:
            done_event.set()
            monitor.cancel()
            if status_msg is not None:
                await _delete_or_check(status_msg)
            return
    except subprocess.TimeoutExpired:
        joined = "".join(text_buf)
        if joined.strip() and partial_sent_len == 0:
            cur_sid = get_chat(chat_id, ws_sf).get("session_id")
            hdr = _header_html(
                model, _display_sid(cur_sid, is_new=(effective_sid is None))
            )
            body = md_to_tg_html(joined)
            await _send_html(
                update,
                hdr
                + body
                + f"\n\n{E.TIME} <i>Timed out, partial output above</i>",
            )
        final = f"{E.TIME} Timed out after ~{LOCAL_TIMEOUT_SEC}s."
        if joined.strip():
            final += f" ({len(joined)} chars captured, sent above.)"
        new_sid = effective_sid
    except Exception as e:
        tb = traceback.format_exc()
        print(f"  OPENCODE ERROR:\n{tb}")
        joined = "".join(text_buf)
        if joined.strip() and partial_sent_len == 0:
            cur_sid = get_chat(chat_id, ws_sf).get("session_id")
            hdr = _header_html(
                model, _display_sid(cur_sid, is_new=(effective_sid is None))
            )
            body = md_to_tg_html(joined)
            await _send_html(
                update,
                hdr
                + body
                + f"\n\n{E.ERR} <i>Terminated, partial output above</i>",
            )
        final = f"{E.ERR} {type(e).__name__}:{str(e)[:200]}"
        if joined.strip():
            final += (
                f" ({len(joined)} chars captured, partial output sent above.)"
            )
        new_sid = effective_sid
    finally:
        done_event.set()
        monitor.cancel()
        try:
            await monitor
        except asyncio.CancelledError:
            pass

    # Persist latest model for session
    used_sid = new_sid or effective_sid
    if isinstance(used_sid, str) and used_sid.startswith("ses"):
        _set_session_model(used_sid, model, ws_sf)

    chat = get_chat(chat_id, ws_sf)
    display_sid = chat.get("session_id") or new_sid
    if display_sid == "__new__":
        display_sid = new_sid
    display_sid = _display_sid(display_sid, is_new=(effective_sid is None))

    header = _header_html(model, display_sid)
    elapsed = int(time.time() - start_t)
    f_len = len(final)
    print(f"  REPLY ({f_len} chars, {elapsed}s):{final[:200]!r}")

    body_html = md_to_tg_html(final)

    if partial_sent_len > 0 and len(final) > partial_sent_len:
        remaining = final[partial_sent_len:]
        if remaining.strip():
            final_msg = md_to_tg_html(remaining)
        else:
            final_msg = (
                header + "\n\n" + E.OK + " <b>[Finished]</b> ("
                + str(elapsed) + "s) \u2014 see partial above."
            )
    elif partial_sent_len > 0:
        final_msg = (
            header
            + f"\n\n{E.OK} <b>[Finished]</b> ({elapsed}s) "
            " \u2014 see partial above."
        )
    else:
        final_msg = body_html

    # Process Scheduled tasks from AI response
    scheduled_tasks_to_add = re.findall(
        r"@@SCHEDULE:\s*(.+?)@@", final, re.IGNORECASE
    )
    scheduled_hint = ""
    for sched_str in scheduled_tasks_to_add:
        parts = sched_str.split("|", 1)
        if len(parts) == 2:
            time_expr = parts[0].strip().lower()
            task_prompt = parts[1].strip()

            task_info = None
            # "every day at HH:MM" or "daily at HH:MM"
            re_d = (
                r"(?:every\s+day\s+(?:at\s+)?|daily\s+(?:at\s+)?)"
                r"(\d{1,2}):(\d{2})"
            )
            m_daily = re.match(re_d, time_expr)
            if m_daily:
                task_info = {
                    "type": "daily",
                    "hour": int(m_daily.group(1)),
                    "minute": int(m_daily.group(2)),
                    "task": task_prompt,
                }
            elif time_expr.startswith("every "):
                m_int = re.match(
                    r"every\s+(\d+)\s+(minute|hour|day)",
                    time_expr,
                )
                if m_int:
                    task_info = {
                        "type": "interval",
                        "interval": int(m_int.group(1)),
                        "unit": m_int.group(2) + "s",
                        "task": task_prompt,
                    }
            elif time_expr.startswith("at "):
                m_at = re.match(
                    r"at\s+(\d{1,2}):(\d{2})", time_expr
                )
                if m_at:
                    task_info = {
                        "type": "daily",
                        "hour": int(m_at.group(1)),
                        "minute": int(m_at.group(2)),
                        "task": task_prompt,
                    }
            elif time_expr.startswith("after ") or time_expr.startswith("in "):
                m_aft = re.match(
                    r"(?:after|in)\s+(\d+)\s+(minute|hour)", time_expr
                )
                if m_aft:
                    val = int(m_aft.group(1))
                    unit = m_aft.group(2)
                    now = datetime.now()
                    target = now + (
                        timedelta(minutes=val)
                        if unit == "minute"
                        else timedelta(hours=val)
                    )
                    task_info = {
                        "type": "once",
                        "hour": target.hour,
                        "minute": target.minute,
                        "task": task_prompt,
                    }

            if task_info:
                task_id = f"task_{int(time.time())}_{len(scheduled_hint)}"
                tasks = _load_scheduled_tasks(ws_tf)
                tasks[task_id] = {
                    "schedule": task_info,
                    "task": task_info["task"],
                    "chat_id": chat_id,
                    "model": model,
                    "session_id": sid,
                    "created_at": datetime.now().isoformat(),
                    "tasks_file": ws_tf,
                }
                _save_scheduled_tasks(tasks, ws_tf)
                _schedule_job(task_id, tasks[task_id])

                if task_info["type"] == "interval":
                    ti_u = task_info['unit']
                    desc = f"every {task_info['interval']} {ti_u}"
                elif task_info["type"] == "daily":
                    ti_h = task_info['hour']
                    ti_m = task_info['minute']
                    desc = f"daily at {ti_h:02d}:{ti_m:02d}"
                else:
                    ti_h = task_info['hour']
                    ti_m = task_info['minute']
                    desc = f"once at {ti_h:02d}:{ti_m:02d}"
                s_hint = (
                    f"\n\n{E.OK} <b>Scheduled Task Added!</b>\n"
                    f"{E.TIME} {desc}\n"
                    f"{E.TOOL} Task:<code>"
                    f"{html.escape(task_prompt[:50])}</code>"
                )
                scheduled_hint += s_hint

    await _send_html(update, final_msg + scheduled_hint)
    await _delete_or_check(status_msg)

    # Process AI file triggers unconditionally
    unique_files = _extract_send_file_directives(final)

    # hard limit of 10 max automatic files to prevent catastrophic spam
    for f in unique_files[:10]:
        await _process_file_request(update, "send", f)


# ================================================================
#  MAIN
# ================================================================


def main():
    app = Application.builder().token(TOKEN).build()
    _start_scheduler()

    async def on_startup(app):
        set_app_context(app, asyncio.get_event_loop())
        _clear_stale_running_sessions()
        await app.bot.set_my_commands([
            BotCommand("model", "Select model (provider -> model)"),
            BotCommand("new", "Start a new session"),
            BotCommand("id", "Switch to session id or pick recent"),
            BotCommand("send", "Send file(s) from HPC/local workspace"),
            BotCommand("upload", "Upload file(s) via rclone destination"),
            BotCommand("status", "Show bot & runtime status"),
            BotCommand("debugws", "Show workspace/session/task file mapping"),
            BotCommand("scheduled", "Manage scheduled tasks (edit/delete)"),
            BotCommand("kill", "Stop current running task"),
            BotCommand("q", "Quick quit current task (same as /kill)"),
        ])

    app.post_init = on_startup
    app.add_handler(CommandHandler("model", _cmd_model))
    app.add_handler(CommandHandler("new", _cmd_new))
    app.add_handler(CommandHandler("id", _cmd_id))
    app.add_handler(CommandHandler("send", _cmd_send))
    app.add_handler(CommandHandler("upload", _cmd_upload))
    app.add_handler(CommandHandler("status", _cmd_status))
    app.add_handler(CommandHandler("debugws", _cmd_debugws))
    app.add_handler(CommandHandler("scheduled", _cmd_schedule))
    app.add_handler(CommandHandler("kill", _cmd_kill))
    app.add_handler(CommandHandler("q", _cmd_q))
    app.add_handler(CallbackQueryHandler(_on_callback_query))
    inbound_filter = (
        filters.TEXT
        | filters.PHOTO
        | filters.Document.IMAGE
        | filters.VOICE
        | filters.AUDIO
        | filters.VIDEO_NOTE
    ) & ~filters.COMMAND
    app.add_handler(
        MessageHandler(inbound_filter, handle_message)
    )
    print("Bot started -- polling...")
    app.run_polling()


if __name__ == "__main__":
    main()
