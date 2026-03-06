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
from typing import Optional, Tuple
from pathlib import Path

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, MessageHandler, ContextTypes, filters
from telegram.constants import ParseMode

# Load .env from the same directory as this script
load_dotenv(Path(__file__).resolve().parent / ".env")


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
        print(f"  [md_to_tg_html error: {exc}]")
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
                f'<pre><code class="language-{html.escape(lang)}">{code}</code></pre>'
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
#  CONFIG  (loaded from .env -- copy .env.example to .env and fill in your values)
# ================================================================
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_CHAT_ID = int(os.environ["ALLOWED_CHAT_ID"])

SSH_HOST = os.environ.get("SSH_HOST", "hpc")
OPENCODE = os.environ.get("OPENCODE_PATH", "opencode")
DEFAULT_MODEL = os.environ.get("DEFAULT_MODEL", "github-copilot/gpt-4o")

MODEL_ALIASES = {
    "pickle": "opencode/big-pickle",
    "nano": "opencode/gpt-5-nano",
    "m25": "opencode/minimax-m2.5-free",
    "trinity": "opencode/trinity-large-preview-free",
    "haiku": "github-copilot/claude-haiku-4.5",
    "opus45": "github-copilot/claude-opus-4.5",
    "opus46": "github-copilot/claude-opus-4.6",
    "opus41": "github-copilot/claude-opus-41",
    "sonnet4": "github-copilot/claude-sonnet-4",
    "sonnet45": "github-copilot/claude-sonnet-4.5",
    "sonnet46": "github-copilot/claude-sonnet-4.6",
    "g25": "github-copilot/gemini-2.5-pro",
    "g3f": "github-copilot/gemini-3-flash-preview",
    "g3p": "github-copilot/gemini-3-pro-preview",
    "g31": "github-copilot/gemini-3.1-pro-preview",
    "g41": "github-copilot/gpt-4.1",
    "g4o": "github-copilot/gpt-4o",
    "g5": "github-copilot/gpt-5",
    "g5m": "github-copilot/gpt-5-mini",
    "g51": "github-copilot/gpt-5.1",
    "c51": "github-copilot/gpt-5.1-codex",
    "c51x": "github-copilot/gpt-5.1-codex-max",
    "c51m": "github-copilot/gpt-5.1-codex-mini",
    "g52": "github-copilot/gpt-5.2",
    "c52": "github-copilot/gpt-5.2-codex",
    "grok": "github-copilot/grok-code-fast-1",
}

# All known full model names (values from aliases + common direct names)
KNOWN_MODELS = set(MODEL_ALIASES.values()) | {DEFAULT_MODEL}

WORKDIR = os.environ.get("HPC_WORKDIR", "~")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
RETRY_ON_TIMEOUT = True

# HPC environment setup commands run before the AI agent on the remote shell.
# Set HPC_SETUP_CMD in .env to whatever loads your dependencies.
# Examples:
#   Imperial College London: module purge && module load tools/dev && module load nodejs/20.13.1-GCCcore-13.3.0
#   Generic Lmod cluster:    module load nodejs
#   Conda environment:       source ~/miniconda3/etc/profile.d/conda.sh && conda activate myenv
#   No modules needed:       (leave blank or omit)
_setup_raw = os.environ.get("HPC_SETUP_CMD", "")
HPC_SETUP_CMD = _setup_raw.strip() if _setup_raw.strip() else None

# rclone destination for the @@upload:@@ directive (remote:path format).
# Examples:  gdrive:HPC-Results    s3:mybucket/results    onedrive:Projects
RCLONE_DEST = os.environ.get("RCLONE_DEST", "gdrive:HPC-Results")

SESSIONS_FILE = os.environ.get(
    "SESSIONS_FILE", os.path.expanduser("~/.hpc_relay_sessions.json")
)
TG_CHUNK = 3800  # conservative; Telegram max is 4096

STREAM_EDIT_INTERVAL = 1.5
STREAM_MIN_DELTA = 80
STALL_WARN_SEC = 45
PARTIAL_SEND_SEC = 90
TIMEOUT_WARN_SEC = 420  # 7 min: first warning
REMOTE_TIMEOUT_SEC = 1800  # 30 min max
LOCAL_TIMEOUT_SEC = REMOTE_TIMEOUT_SEC + 30

OC_DB_PATH = os.environ.get(
    "OC_DB_PATH", "~/.local/share/opencode/opencode.db"
)  # on HPC

SYSTEM_SUFFIX = (
    " FORMAT: All your output / reply should strictly be in proper Markdown format."
    " Use ## or ### for section headers."
    " Use **bold** for key terms and labels."
    " Use `backticks` for code/commands."
    " Use - for bullet lists and indent sub-items with 2 spaces."
    " Use numbered lists (1. 2. 3.) for sequential steps."
    " Use > for important callouts."
    " Use ```lang for code blocks."
    " Keep reply concise and highly structured."
    " IMPORTANT: Headless / non-interactive mode (`opencode run`)."
    " Do NOT invoke ask_questions tool"
    " Unless explicitly told not to by the user, automatically output exactly `@@SEND_FILE: <filepath>@@` whenever you create or reference a small output file (like a png, pdf, or short dataset)."
    "If need to ask follow-up questions, generate in Markdown formatted text"
    " Only return text/code responses."
)

MODEL_RE = re.compile(r"@@model:\s*(.+?)@@", re.IGNORECASE)
SESSION_RE = re.compile(r"@@session:\s*(.+?)@@", re.IGNORECASE)
SHELL_PREFIX = "!"


# ================================================================
#  PERSISTENCE  (with session history tracking)
# ================================================================
def _load_store() -> dict:
    try:
        with open(SESSIONS_FILE) as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save_store(d: dict):
    tmp = SESSIONS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(d, f, indent=2)
    os.replace(tmp, SESSIONS_FILE)


def get_chat(cid: int) -> dict:
    v = _load_store().get(str(cid), {})
    if isinstance(v, str):
        v = {"session_id": v}
    v.setdefault("model", DEFAULT_MODEL)
    v.setdefault("session_id", None)
    return v


def update_chat(cid: int, **kw):
    store = _load_store()
    key = str(cid)
    cur = store.get(key, {})
    if isinstance(cur, str):
        cur = {"session_id": cur, "model": DEFAULT_MODEL}
    cur.update({k: v for k, v in kw.items() if v is not None})
    store[key] = cur
    _save_store(store)


def _record_session(sid: str):
    """Add a session ID to the known_sessions history."""
    if not sid or not sid.startswith("ses"):
        return
    store = _load_store()
    history = store.get("__known_sessions__", [])
    if sid not in history:
        history.append(sid)
        store["__known_sessions__"] = history
        _save_store(store)


def _get_known_sessions() -> list:
    """Return list of all session IDs ever seen (local cache)."""
    return _load_store().get("__known_sessions__", [])


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
        if has_agent and row['agent']:
            info.append(row['agent'])
        if has_parent and row['parent_id']:
            info.append("subagent")

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
            print(
                f"  [_fetch_hpc_sessions: empty output, stderr={stderr.decode(errors='replace')[:200]}]"
            )
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
        print(f"  [_fetch_hpc_sessions: found {len(result)} sessions]")
        return result
    except Exception as exc:
        print(f"  [_fetch_hpc_sessions error: {exc}]")
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
    out = {"model": None, "session": None, "shell": None, "prompt": None}
    if text.startswith(SHELL_PREFIX):
        out["shell"] = text[len(SHELL_PREFIX):].strip()
        return out
    m = MODEL_RE.search(text)
    if m:
        req = m.group(1).strip().lower()
        out["model"] = MODEL_ALIASES.get(req, req)
        text = MODEL_RE.sub("", text)
    s = SESSION_RE.search(text)
    if s:
        val = s.group(1).strip()
        out["session"] = "new" if val.lower() == "new" else val
        text = SESSION_RE.sub("", text)
    out["prompt"] = text.strip() or None
    return out


# ================================================================
#  SSH HELPERS
# ================================================================
def _ssh_base() -> list:
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
    try:
        subprocess.run(
            ["ssh", "-O", "exit", SSH_HOST],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
    except Exception:
        pass


def _oc_script(prompt, sid, model):
    sess = (
        f"--session {shlex.quote(sid)} "
        if sid and sid.startswith("ses")
        else ""
    )
    setup = (HPC_SETUP_CMD + "\n") if HPC_SETUP_CMD else ""
    return (
        f"set -euo pipefail\n{setup}"
        f"cd {shlex.quote(WORKDIR)}\n"
        f"{shlex.quote(OPENCODE)} run -m {shlex.quote(model)} "
        f"--format json {sess}{shlex.quote(prompt)} 2>&1\n"
    )


def _shell_script(cmd):
    return f"cd {shlex.quote(WORKDIR)} && {{ {cmd} ; }} 2>&1\n"


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


def _parse_all(output) -> Tuple[Optional[str], Optional[str]]:
    sid = txt = None
    for raw in output.splitlines():
        ev = _parse_ev(raw)
        if not ev:
            continue
        if isinstance(ev.get("sessionID"), str):
            sid = ev["sessionID"]
        if ev.get("type") == "text":
            t = (ev.get("part") or {}).get("text")
            if isinstance(t, str) and t.strip():
                txt = t
    return sid, txt


# ================================================================
#  SHELL EXEC
# ================================================================
async def exec_shell(cmd):
    ssh_cmd = _ssh_base() + [f"timeout {int(REMOTE_TIMEOUT_SEC)} bash -ls"]
    proc = await asyncio.create_subprocess_exec(
        *ssh_cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        limit=1024 * 1024 * 16,
    )
    stdout, _ = await asyncio.wait_for(
        proc.communicate(_shell_script(cmd).encode()), LOCAL_TIMEOUT_SEC
    )
    out = stdout.decode(errors="replace").strip()
    rc = proc.returncode
    if rc and rc != 0:
        return f"{E.WARN} exit {rc}\n\n{out[-3000:]}"
    return out[-3500:] if out else "(no output)"


# ================================================================
#  STREAMING OPENCODE
# ================================================================
async def run_opencode(
    prompt, chat_id, model, session_id, on_progress, on_text_chunk
):
    script = _oc_script(prompt, session_id, model)
    ssh_cmd = _ssh_base() + [f"timeout {int(REMOTE_TIMEOUT_SEC)} bash -ls"]

    async def _once():
        proc = await asyncio.create_subprocess_exec(
            *ssh_cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            limit=1024 * 1024 * 16,
        )
        proc.stdin.write(script.encode())
        await proc.stdin.drain()
        proc.stdin.close()
        prev_text_len = 0
        latest_sid = None
        lines = []
        t0 = time.time()
        last_json_t = t0
        await on_progress(f"{E.WAIT} Connected -- waiting for response...")
        while True:
            elapsed = time.time() - t0
            now_t = time.time()

            # Throttle kill-flag checking to avoid spamming disk I/O which
            # freezes python.
            if now_t - getattr(proc, "_last_kill_check", 0) > 3.0:
                proc._last_kill_check = now_t
                chat_now = get_chat(chat_id)
                if chat_now.get("kill_requested") == "yes":
                    proc.kill()
                    update_chat(chat_id, kill_requested="no")
                    raise RuntimeError("Stopped by user (kill signal).")

            if elapsed > LOCAL_TIMEOUT_SEC:
                proc.kill()
                raise subprocess.TimeoutExpired(ssh_cmd, LOCAL_TIMEOUT_SEC)

            try:
                raw = await asyncio.wait_for(proc.stdout.readline(), timeout=5)
            except asyncio.TimeoutError:
                silence = time.time() - last_json_t
                if silence > 180:
                    await on_progress(
                        f"{E.WARN} Silent 3+ mins (ask_question pending?). Send @@kill@@. [{int(elapsed)}s]"
                    )
                else:
                    await on_progress(
                        f"{E.WAIT} Still processing... [{int(elapsed)}s]"
                    )
                continue

            if not raw:
                break

            # Explicit yield to ensure Telegram's event loop can fetch network
            # payloads like @@kill@@!
            await asyncio.sleep(0.01)

            decoded = raw.decode(errors="replace")
            ev = _parse_ev(decoded)
            if not ev:
                # Infinite garbage spam warning
                if time.time() - last_json_t > 180:
                    await on_progress(
                        f"{E.WARN} 3+ mins non-JSON spam (stuck loop?). Send @@kill@@. [{int(elapsed)}s]"
                    )
                # Non-JSON lines: only store if small
                if len(decoded) < 5000:
                    lines.append(decoded)
                continue

            last_json_t = time.time()
            if isinstance(ev.get("sessionID"), str):
                latest_sid = ev["sessionID"]
            etype = ev.get("type", "")
            # Only store text events and small events in lines buffer
            # tool_use events (apply_patch, task) can be 100KB+ and cause
            # ValueError
            if etype == "text":
                lines.append(decoded)
            elif len(decoded) < 5000:
                lines.append(decoded)
            # Extract tool name from the actual opencode JSON structure
            # Real format: {"type":"tool_use", "part":{"tool":"task",
            # "state":{"input":{...}}}}
            part = ev.get("part") or {}
            tool_name = part.get("tool", "")  # the actual tool name
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
                    f"{E.WARN} Model invoked ask_question in headless mode. Terminating."
                )
                proc.kill()
                raise RuntimeError(
                    "Model attempted to use ask_question tool, which hangs in headless mode."
                )

            # ---- Regular tool progress (apply_patch, etc.) ----
            if etype == "tool_use":
                status = (part.get("state") or {}).get("status", "")
                title_str = (part.get("state") or {}).get("title", "")[:60]
                info = f"{tool_name}"
                if status:
                    info += f" ({status})"
                if title_str:
                    info += f": {title_str}"
                await on_progress(f"{E.TOOL} {info}")
                # Don't store massive tool_use lines (diffs can be 100KB+)
                continue

            # ---- Step events ----
            if etype in ("step_start", "step_finish"):
                reason = part.get("reason", "")
                if reason:
                    await on_progress(f"{E.TOOL} step: {reason}")
                continue

            if etype == "text":
                t = part.get("text")
                if isinstance(t, str) and t.strip():
                    delta = t[prev_text_len:]
                    prev_text_len = len(t)
                    if delta.strip():
                        await on_text_chunk(delta)
        rc = await proc.wait()
        return rc, "".join(lines), latest_sid

    try:
        rc, output, sid = await _once()
    except subprocess.TimeoutExpired:
        if RETRY_ON_TIMEOUT:
            await asyncio.to_thread(_close_master)
            await on_progress(f"{E.RETRY} Retrying...")
            rc, output, sid = await _once()
        else:
            raise

    _, final_text = _parse_all(output)

    returned_sid = sid
    if isinstance(returned_sid, str) and returned_sid.startswith("ses"):
        update_chat(chat_id, session_id=returned_sid)
        _record_session(returned_sid)  # track in history

    if rc != 0 and not final_text:
        return f"{E.ERR} exit {rc}:\n{output[:1200]}", returned_sid
    return (final_text or "[no output]").strip(), returned_sid


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
    """Split text into chunks at paragraph / newline boundaries. Never raises."""
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
        print(f"  [_smart_chunks error: {exc}]")
        # Ultimate fallback: brute force split
        return [text[i:i + limit] for i in range(0, len(text), limit)]


async def _safe_edit(msg, text, use_html=False):
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
                print(f"  [edit err: {e}]")


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
        print(f"  [_send_html fatal: {exc}]")
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


async def _process_file_request(update, action, pattern):
    import os
    import shlex

    pattern_clean = pattern.strip(" @\n\r")

    # Safely resolve files using Python on HPC to prevent Bash wildcard
    # injection
    py_script = f"""import glob, os, sys
try: os.chdir({repr(WORKDIR)})
except: pass
matches = glob.glob({repr(pattern_clean)}, recursive=True)
if not matches and os.path.exists({repr(pattern_clean)}):
    matches = [{repr(pattern_clean)}]
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
        await update.message.reply_text(
            f"{E.ERR} <b>File not found:</b> <code>{html.escape(pattern_clean)}</code>\nDoes not exist or matched nothing.",
            parse_mode=ParseMode.HTML,
        )
        return
    if len(files) > 10:
        await update.message.reply_text(
            f"{E.WARN} Wildcard matched {len(files)} files. Limiting to first 10.",
            parse_mode=ParseMode.HTML,
        )
        files = files[:10]

    # -- RCLONE UPLOAD --
    if action == "upload":
        bash_cmds = []
        if HPC_SETUP_CMD:
            bash_cmds.append(HPC_SETUP_CMD)
        for f in files:
            bash_cmds.append(
                f"rclone copy {shlex.quote(f)} {shlex.quote(RCLONE_DEST)}/ --progress"
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
            await update.message.reply_text(
                f"{E.OK} <b>Upload complete!</b> ({len(files)} files)\n<pre>{html.escape(out.decode()[:2500])}</pre>",
                parse_mode=ParseMode.HTML,
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
if (( size > 40000000 )); then echo "ERR: File too large (Size is $size bytes. Max allowed is 40MB). Please use @@upload: instead." >&2; exit 1; fi
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
            await update.message.reply_text(
                f"{E.ERR} <b>Cannot fetch file:</b> <code>{html.escape(os.path.basename(f))}</code>\n<i>{html.escape(err_msg)}</i>",
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
                        f"  [photo upload failed for {filename}: {ex1}. Retrying as document.]"
                    )
                    await update.message.reply_document(
                        document=data,
                        filename=filename,
                        caption=f"File: {filename} (sent as document due to dimension limits)",
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


# ================================================================
#  MAIN HANDLER
# ================================================================
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    uid = update.effective_user.id if update.effective_user else None
    print(f"\n{'=' * 50}\nINCOMING  chat={chat_id}  user={uid}")
    if chat_id != ALLOWED_CHAT_ID:
        return

    raw = (update.message.text or "").strip()
    if not raw:
        return

    # Fast-path kill command without backgrounding
    if raw.lower() == "!kill" or "@@kill@@" in raw.lower():
        update_chat(chat_id, kill_requested="yes")
        try:
            await update.message.reply_text(
                f"{E.OK} Kill signal sent. Running processes will terminate."
            )
        except Exception:
            pass
        return

    # Dispatch to background task so the bot immediately accepts new messages
    # (like @@kill@@)
    asyncio.create_task(_run_in_background(update, context, chat_id, raw))


async def _run_in_background(update, context, chat_id, raw):
    try:
        await _handle_message_inner(update, context, chat_id, raw)
    except Exception as exc:
        tb = traceback.format_exc()
        print(f"  UNHANDLED ERROR:\n{tb}")
        try:
            await update.message.reply_text(
                f"{E.ERR} Internal error (see server log):\n{type(exc).__name__}: {str(exc)[:200]}"
            )
        except Exception:
            pass


async def _handle_message_inner(update, context, chat_id, raw):
    # -- MANUAL FILE FETCH --
    m_send = re.match(r"(?i)^@@(send|upload):\s*(.+?)(?:@@)?$", raw.strip())
    if m_send:
        action = m_send.group(1).lower()
        await update.message.reply_text(
            f"{E.WAIT} <i>{'Uploading to gdrive' if action == 'upload' else 'Fetching file(s)'}...</i>",
            parse_mode=ParseMode.HTML,
        )
        await _process_file_request(update, action, m_send.group(2))
        return

    parsed = parse_message(raw)
    chat = get_chat(chat_id)
    model = parsed["model"] or chat["model"]
    sid = chat["session_id"]
    print(f"  model={model}  session={sid}")

    # -- MODEL-ONLY --
    if (
        parsed["model"]
        and not parsed["prompt"]
        and not parsed["session"]
        and not parsed["shell"]
    ):
        requested = parsed["model"]
        if requested not in KNOWN_MODELS and not requested.startswith(
            ("opencode/", "github-copilot/")
        ):
            lines = []
            for alias in sorted(MODEL_ALIASES.keys()):
                full = MODEL_ALIASES[alias]
                lines.append(f"  <code>{alias:8s}</code> {html.escape(full)}")
            table = "\n".join(lines)
            await update.message.reply_text(
                f"{E.ERR} <b>Unknown model:</b> <code>{html.escape(requested)}</code>\n\n"
                f"<b>Available models (alias \u2192 full name):</b>\n{table}\n\n"
                f"Or use full name: <code>github-copilot/model-name</code>",
                parse_mode=ParseMode.HTML,
            )
            return
        update_chat(chat_id, model=requested)
        await update.message.reply_text(
            f"{E.OK} <b>Model</b> <code>{html.escape(requested)}</code>\n"
            f"{E.SESS} <code>{_display_sid(sid)}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    # -- SESSION-ONLY --
    if parsed["session"] and not parsed["prompt"] and not parsed["shell"]:
        if parsed["session"] == "new":
            update_chat(chat_id, session_id="__new__")
            txt = f"{E.OK} <b>New session queued</b>\n{E.BOT} <code>{html.escape(model)}</code>"
        else:
            req_sid = parsed["session"]
            if not req_sid.startswith("ses_"):
                hpc_sessions = await _fetch_hpc_sessions()
                if hpc_sessions:
                    known_str = "\n".join(
                        f"  <code>{s[0][:25]}</code> {html.escape((s[1] or 'Untitled')[:30])}"
                        for s in hpc_sessions[:10]
                    )
                else:
                    known_str = "  (none found)"
                await update.message.reply_text(
                    f"{E.ERR} <b>Invalid session ID:</b> <code>{html.escape(req_sid)}</code>\n"
                    f"Session IDs must start with <code>ses_</code>\n\n"
                    f"<b>Current session:</b> <code>{sid or 'None'}</code>\n\n"
                    f"<b>Recent sessions on HPC:</b>\n{known_str}\n\n"
                    f"Use <code>@@session: new@@</code> for a new session.",
                    parse_mode=ParseMode.HTML,
                )
                return
            status_msg = await update.message.reply_text(
                f"{E.WAIT} Checking session on HPC..."
            )
            exists = await _is_valid_session(req_sid)
            await _delete_or_check(status_msg)
            if not exists:
                hpc_sessions = await _fetch_hpc_sessions()
                if hpc_sessions:
                    known_str = "\n".join(
                        f"  <code>{s[0][:25]}</code> {html.escape((s[1] or 'Untitled')[:30])}"
                        for s in hpc_sessions[:10]
                    )
                else:
                    known_str = "  (none found)"
                await update.message.reply_text(
                    f"{E.ERR} <b>Session not found:</b> <code>{html.escape(req_sid)}</code>\n\n"
                    f"<b>Current session:</b> <code>{sid or 'None'}</code>\n\n"
                    f"<b>Recent sessions on HPC:</b>\n{known_str}\n\n"
                    f"Use <code>@@session: new@@</code> for a new session.",
                    parse_mode=ParseMode.HTML,
                )
                return
            update_chat(chat_id, session_id=req_sid)
            txt = f"{E.OK} <b>Session</b> <code>{html.escape(req_sid)}</code>\n{E.BOT} <code>{html.escape(model)}</code>"
        await update.message.reply_text(txt, parse_mode=ParseMode.HTML)
        return

    # -- SHELL --
    if parsed["shell"]:
        print(f"  SHELL: {parsed['shell']}")
        status_msg = await update.message.reply_text(f"{E.SHELL} Running...")
        try:
            out = await exec_shell(parsed["shell"])
        except asyncio.TimeoutError:
            out = f"{E.TIME} Timed out after {LOCAL_TIMEOUT_SEC}s."
        except Exception as e:
            out = f"{E.ERR} {type(e).__name__}: {e}"
        hdr = _shell_header_html()
        body = f"<pre>{html.escape(out[:3500])}</pre>"
        await _send_html(update, hdr + body)
        await _delete_or_check(status_msg)
        return

    # -- OPENCODE --
    if parsed["model"]:
        update_chat(chat_id, model=model)
    effective_sid = sid
    if parsed["session"]:
        if parsed["session"] == "new":
            effective_sid = None
            update_chat(chat_id, session_id="__new__")
        else:
            effective_sid = parsed["session"]
            update_chat(chat_id, session_id=effective_sid)
    if effective_sid == "__new__":
        effective_sid = None

    # Clear any previous kill flag
    update_chat(chat_id, kill_requested="no")

    prompt = parsed["prompt"] + SYSTEM_SUFFIX
    status_msg = await update.message.reply_text(
        f"{E.WAIT} Connecting to HPC..."
    )

    text_buf = []
    last_edit_t = 0.0
    last_output_t = time.time()
    start_t = time.time()
    partial_sent_len = 0
    done_event = asyncio.Event()

    async def on_progress(s):
        nonlocal last_edit_t, last_output_t
        last_output_t = time.time()
        now = time.time()
        if now - last_edit_t < 1.0:
            return
        last_edit_t = now
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
                    cur_sid = get_chat(chat_id).get("session_id")
                    hdr = _header_html(
                        model,
                        _display_sid(cur_sid, is_new=(effective_sid is None)),
                    )
                    body = md_to_tg_html(joined)
                    await _send_html(
                        update,
                        hdr
                        + body
                        + f"\n\n{E.WARN} <i>Partial at {elapsed}s (still running)</i>",
                    )
                await _safe_edit(
                    status_msg,
                    f"{E.WARN} Running {elapsed}s | buf {chars} chars | silent {int(silence)}s\n"
                    f"Send !kill to stop.",
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
                    cur_sid = get_chat(chat_id).get("session_id")
                    hdr = _header_html(
                        model,
                        _display_sid(cur_sid, is_new=(effective_sid is None)),
                    )
                    body = md_to_tg_html(joined)
                    await _send_html(
                        update,
                        hdr
                        + body
                        + f"\n\n{E.TIME} <i>Stall: {int(silence)}s silence, {elapsed}s total</i>",
                    )
                    await _safe_edit(
                        status_msg, f"{E.OUT} Partial sent [{elapsed}s]"
                    )
                    continue
            if silence >= STALL_WARN_SEC:
                chars = len("".join(text_buf))
                kill_hint = "\nSend !kill to stop."
                if partial_sent_len > 0:
                    st = f"{E.WARN} Silent {int(silence)}s (total: {elapsed}s)\nPartial sent ({partial_sent_len} chars){kill_hint}"
                else:
                    st = f"{E.WARN} Silent {int(silence)}s (total: {elapsed}s)\nBuffered: {chars} chars{kill_hint}"
                await _safe_edit(status_msg, st)

    monitor = asyncio.create_task(stall_monitor())
    try:
        final, new_sid = await run_opencode(
            prompt, chat_id, model, effective_sid, on_progress, on_text_chunk
        )
    except subprocess.TimeoutExpired:
        joined = "".join(text_buf)
        if joined.strip() and partial_sent_len == 0:
            cur_sid = get_chat(chat_id).get("session_id")
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
            cur_sid = get_chat(chat_id).get("session_id")
            hdr = _header_html(
                model, _display_sid(cur_sid, is_new=(effective_sid is None))
            )
            body = md_to_tg_html(joined)
            await _send_html(
                update,
                hdr
                + body
                + f"\n\n{E.ERR} <i>Process terminated, partial output above</i>",
            )
        final = f"{E.ERR} {type(e).__name__}: {str(e)[:200]}"
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

    chat = get_chat(chat_id)
    display_sid = chat.get("session_id") or new_sid
    if display_sid == "__new__":
        display_sid = new_sid
    display_sid = _display_sid(display_sid, is_new=(effective_sid is None))

    header = _header_html(model, display_sid)
    elapsed = int(time.time() - start_t)
    print(f"  REPLY ({len(final)} chars, {elapsed}s): {final[:200]!r}")

    body_html = md_to_tg_html(final)

    if partial_sent_len > 0 and len(final) > partial_sent_len:
        remaining = final[partial_sent_len:]
        if remaining.strip():
            final_msg = (
                header
                + f"<i>...continued ({elapsed}s)</i>\n\n"
                + md_to_tg_html(remaining)
            )
        else:
            final_msg = (
                header
                + f"{E.OK} <b>Complete</b> ({elapsed}s) \u2014 see partial above."
            )
    elif partial_sent_len > 0:
        final_msg = (
            header
            + f"{E.OK} <b>Complete</b> ({elapsed}s) \u2014 see partial above."
        )
    else:
        final_msg = header + body_html

    await _send_html(update, final_msg)
    await _delete_or_check(status_msg)

    # Process AI file triggers unconditionally
    files_to_send = re.findall(r"@@SEND_FILE:\s*(.+?)@@", final, re.IGNORECASE)
    # Deduplicate while preserving order
    seen_files = set()
    unique_files = [
        f.strip(" @")
        for f in files_to_send
        if not (f in seen_files or seen_files.add(f))
    ]

    # hard limit of 10 max automatic files to prevent catastrophic spam
    for f in unique_files[:10]:
        await _process_file_request(update, "send", f)


# ================================================================
#  MAIN
# ================================================================


def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )
    print("Bot started -- polling...")
    app.run_polling()


if __name__ == "__main__":
    main()
