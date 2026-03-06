#!/usr/bin/env python3
"""
OpenCode Chat History Viewer Generator
=======================================
Extracts chat history from ~/.local/share/opencode/opencode.db
and generates an interactive HTML page.

Usage:
    python3 ~/opencode_chat_viewer/Auto_generate_chat_viewer.py

    Then open:
    ~/opencode_chat_viewer/index.html

    Or serve locally:
    ssh -Y login-ai
    cd ~/opencode_chat_viewer && python3 -m http.server 8765 --bind 127.0.0.1
"""

import sqlite3
import json
import html
import os
import shutil
from datetime import datetime
# removed

# ── Config ──────────────────────────────────────────────
DB_SOURCE = os.path.expanduser("~/.local/share/opencode/opencode.db")
OUTPUT_DIR = os.path.expanduser("~/opencode_chat_viewer")
DB_COPY = "/tmp/opencode_viewer_temp.db"
MAX_SESSIONS = 50  # How many recent sessions to include
# ────────────────────────────────────────────────────────


def ms_to_time(ms):
    """Convert millisecond timestamp to human-readable time."""
    try:
        dt = datetime.fromtimestamp(ms / 1000.0)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ms)


def ms_to_short_time(ms):
    """Convert millisecond timestamp to HH:MM:SS."""
    try:
        dt = datetime.fromtimestamp(ms / 1000.0)
        return dt.strftime("%H:%M:%S")
    except Exception:
        return str(ms)


def extract_sessions(cursor):
    """Get all sessions ordered by recency."""
    cursor.execute(f"""
        SELECT s.id, s.title, s.time_created, s.time_updated,
               COUNT(DISTINCT m.id) as msg_count
        FROM session s
        LEFT JOIN message m ON m.session_id = s.id
        GROUP BY s.id
        ORDER BY s.time_updated DESC
        LIMIT {MAX_SESSIONS}
    """)
    return cursor.fetchall()


def extract_conversation(cursor, session_id):
    """Extract full conversation for a session."""
    cursor.execute(
        "SELECT id, time_created, data FROM message WHERE session_id=? ORDER BY time_created ASC",
        (session_id,),
    )
    messages = cursor.fetchall()

    cursor.execute(
        "SELECT id, message_id, time_created, data FROM part WHERE session_id=? ORDER BY time_created ASC",
        (session_id,),
    )
    parts = cursor.fetchall()

    conversation = []
    for m in messages:
        msg_data = json.loads(m[2]) if m[2] else {}
        msg_id = m[0]
        role = msg_data.get("role", "unknown")
        agent = msg_data.get("agent", msg_data.get("mode", ""))
        model_info = msg_data.get("model", {})
        model = msg_data.get("modelID", "")
        if not model and isinstance(model_info, dict):
            model = model_info.get("modelID", "")
        provider = msg_data.get("providerID", "")
        if not provider and isinstance(model_info, dict):
            provider = model_info.get("providerID", "")

        tokens = msg_data.get("tokens", {})
        cost = msg_data.get("cost", 0)

        msg_parts = [p for p in parts if p[1] == msg_id]
        text_content = []
        tool_calls = []

        for p in msg_parts:
            pdata = json.loads(p[3]) if p[3] else {}
            ptype = pdata.get("type", "")

            if ptype == "text" and pdata.get("text"):
                text_content.append(pdata["text"])
            elif ptype == "tool-invocation":
                ti = pdata.get("toolInvocation", {})
                tool_calls.append(
                    {
                        "type": "call",
                        "tool": ti.get("toolName", "unknown"),
                        "args": json.dumps(ti.get("args", {}), indent=2)[:500],
                        "state": ti.get("state", ""),
                    }
                )
            elif ptype == "tool-result":
                ti = pdata.get("toolInvocation", {})
                result = ti.get("result", "")
                if isinstance(result, list):
                    result = "\n".join(
                        [
                            (
                                str(r.get("text", r))
                                if isinstance(r, dict)
                                else str(r)
                            )
                            for r in result
                        ]
                    )
                tool_calls.append(
                    {
                        "type": "result",
                        "tool": ti.get("toolName", "unknown"),
                        "result": str(result)[:800],
                    }
                )
            elif ptype == "tool":
                # Alternative tool format used by some agents
                state = pdata.get("state", {})
                inp = state.get("input", {})
                output = state.get("output", "")
                tool_name = pdata.get("tool", "unknown")
                tool_calls.append(
                    {
                        "type": "call",
                        "tool": tool_name,
                        "args": json.dumps(inp, indent=2)[:500],
                        "state": state.get("status", ""),
                    }
                )
                if output:
                    tool_calls.append(
                        {
                            "type": "result",
                            "tool": tool_name,
                            "result": str(output)[:800],
                        }
                    )

        conversation.append(
            {
                "role": role,
                "agent": agent,
                "model": model,
                "provider": provider,
                "text": "\n".join(text_content),
                "tools": tool_calls,
                "tokens": tokens,
                "cost": cost,
                "time": m[1],
            }
        )

    return conversation


def generate_html(sessions, conversations):
    """Generate complete HTML visualization."""

    session_options = []
    for s in sessions:
        sid, title, created, updated, msg_count = s
        time_str = ms_to_time(updated)
        session_options.append(
            f'<option value="{sid}">{html.escape(title or "Untitled")} ({msg_count} msgs, {time_str})</option>'
        )

    def sanitize_for_js(text):
        """Escape characters that break JS template literals."""
        if not text:
            return text
        return (
            text.replace("\\", "\\\\")
            .replace("`", "\\`")
            .replace("${", "\\${")
        )

    conv_data = {}
    for sid, conv in conversations.items():
        conv_data[sid] = []
        for msg in conv:
            # Sanitize all text fields to prevent JS template literal injection
            sanitized_tools = []
            for t in msg["tools"]:
                st = dict(t)
                if "args" in st:
                    st["args"] = sanitize_for_js(st["args"])
                if "result" in st:
                    st["result"] = sanitize_for_js(st["result"])
                sanitized_tools.append(st)

            conv_data[sid].append(
                {
                    "role": msg["role"],
                    "agent": msg["agent"],
                    "model": msg["model"],
                    "provider": msg["provider"],
                    "text": sanitize_for_js(msg["text"]),
                    "tools": sanitized_tools,
                    "tokens": msg.get("tokens", {}),
                    "cost": msg.get("cost", 0),
                    "time": ms_to_time(msg["time"]),
                }
            )

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OpenCode Chat History Viewer</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {{
    --bg-primary: #0a0a0f;
    --bg-secondary: #12121a;
    --bg-tertiary: #1a1a2e;
    --bg-card: #16162a;
    --border-color: #2a2a4a;
    --text-primary: #e8e8f0;
    --text-secondary: #a0a0c0;
    --text-muted: #6a6a8a;
    --accent-user: #6366f1;
    --accent-user-bg: rgba(99, 102, 241, 0.08);
    --accent-user-border: rgba(99, 102, 241, 0.25);
    --accent-assistant: #10b981;
    --accent-assistant-bg: rgba(16, 185, 129, 0.06);
    --accent-assistant-border: rgba(16, 185, 129, 0.2);
    --accent-delegate: #f59e0b;
    --accent-delegate-bg: rgba(245, 158, 11, 0.06);
    --accent-delegate-border: rgba(245, 158, 11, 0.2);
    --accent-tool: #8b5cf6;
    --accent-tool-bg: rgba(139, 92, 246, 0.08);
    --accent-tool-border: rgba(139, 92, 246, 0.2);
    --glow-user: 0 0 20px rgba(99, 102, 241, 0.15);
    --glow-assistant: 0 0 20px rgba(16, 185, 129, 0.1);
    --glow-delegate: 0 0 20px rgba(245, 158, 11, 0.1);
    --radius: 16px;
    --radius-sm: 10px;
}}

* {{ margin: 0; padding: 0; box-sizing: border-box; }}

body {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    background: var(--bg-primary);
    color: var(--text-primary);
    min-height: 100vh;
    overflow-x: hidden;
}}

body::before {{
    content: '';
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background:
        radial-gradient(ellipse 800px 600px at 20% 20%, rgba(99, 102, 241, 0.05), transparent),
        radial-gradient(ellipse 600px 800px at 80% 80%, rgba(16, 185, 129, 0.04), transparent),
        radial-gradient(ellipse 400px 400px at 50% 50%, rgba(139, 92, 246, 0.03), transparent);
    pointer-events: none;
    z-index: 0;
}}

.app {{
    position: relative;
    z-index: 1;
    max-width: 1100px;
    margin: 0 auto;
    padding: 24px 20px 60px;
}}

.header {{
    text-align: center;
    margin-bottom: 32px;
    padding: 40px 20px;
    background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-tertiary) 100%);
    border: 1px solid var(--border-color);
    border-radius: var(--radius);
    position: relative;
    overflow: hidden;
}}

.header::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--accent-user), var(--accent-assistant), var(--accent-delegate));
}}

.header h1 {{
    font-size: 2rem;
    font-weight: 800;
    background: linear-gradient(135deg, #e8e8f0 0%, #a0a0c0 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 8px;
    letter-spacing: -0.02em;
}}

.header p {{
    color: var(--text-muted);
    font-size: 0.9rem;
}}

.header .gen-time {{
    color: var(--text-muted);
    font-size: 0.75rem;
    margin-top: 8px;
    font-family: 'JetBrains Mono', monospace;
}}

.controls {{
    display: flex;
    gap: 12px;
    margin-bottom: 20px;
    flex-wrap: wrap;
    align-items: center;
}}

.controls label {{
    font-weight: 600;
    font-size: 0.85rem;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    white-space: nowrap;
}}

.controls select {{
    flex: 1;
    min-width: 250px;
    background: var(--bg-secondary);
    color: var(--text-primary);
    border: 1px solid var(--border-color);
    border-radius: var(--radius-sm);
    padding: 12px 40px 12px 16px;
    font-family: 'Inter', sans-serif;
    font-size: 0.9rem;
    cursor: pointer;
    outline: none;
    transition: border-color 0.2s, box-shadow 0.2s;
    appearance: none;
    background-image: url("data:image/svg+xml,%3Csvg width='12' height='8' viewBox='0 0 12 8' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M1 1L6 6L11 1' stroke='%236a6a8a' stroke-width='2' stroke-linecap='round'/%3E%3C/svg%3E");
    background-repeat: no-repeat;
    background-position: right 14px center;
}}

.controls select:hover {{ border-color: var(--accent-user); }}
.controls select:focus {{ border-color: var(--accent-user); box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.15); }}

.search-box {{
    flex: 0 0 280px;
    background: var(--bg-secondary);
    color: var(--text-primary);
    border: 1px solid var(--border-color);
    border-radius: var(--radius-sm);
    padding: 12px 16px;
    font-family: 'Inter', sans-serif;
    font-size: 0.9rem;
    outline: none;
    transition: border-color 0.2s;
}}

.search-box:focus {{ border-color: var(--accent-user); }}
.search-box::placeholder {{ color: var(--text-muted); }}

.stats-bar {{
    display: flex;
    gap: 10px;
    margin-bottom: 20px;
    flex-wrap: wrap;
}}

.stat-chip {{
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 14px;
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 100px;
    font-size: 0.78rem;
    color: var(--text-secondary);
    transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}}

.stat-chip[onclick]:hover {{
    border-color: var(--accent-user) !important;
    background: var(--accent-user-bg);
    transform: translateY(-1px);
    cursor: pointer;
}}

.stat-chip .dot {{
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
}}

.stat-chip .count {{
    font-weight: 700;
    color: var(--text-primary);
}}

.legend {{
    display: flex;
    gap: 20px;
    margin-bottom: 22px;
    padding: 12px 20px;
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: var(--radius-sm);
    flex-wrap: wrap;
}}

.legend-item {{
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 0.8rem;
    color: var(--text-secondary);
}}

.legend-dot {{
    width: 12px;
    height: 12px;
    border-radius: 4px;
}}

.chat-container {{
    display: flex;
    flex-direction: column;
    gap: 14px;
}}

.message {{
    border-radius: var(--radius);
    padding: 18px 22px;
    position: relative;
    animation: fadeIn 0.25s ease-out;
    transition: transform 0.15s ease, opacity 0.15s ease;
}}

.message.hidden {{
    display: none;
}}

.message:hover {{
    transform: translateY(-1px);
}}

@keyframes fadeIn {{
    from {{ opacity: 0; transform: translateY(8px); }}
    to {{ opacity: 1; transform: translateY(0); }}
}}

.message.user {{
    background: var(--accent-user-bg);
    border: 1px solid var(--accent-user-border);
    border-left: 3px solid var(--accent-user);
    box-shadow: var(--glow-user);
}}

.message.assistant {{
    background: var(--accent-assistant-bg);
    border: 1px solid var(--accent-assistant-border);
    border-left: 3px solid var(--accent-assistant);
    box-shadow: var(--glow-assistant);
}}

.message.delegate {{
    background: var(--accent-delegate-bg);
    border: 1px solid var(--accent-delegate-border);
    border-left: 3px solid var(--accent-delegate);
    box-shadow: var(--glow-delegate);
    margin-left: 36px;
}}

.msg-header {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 10px;
    flex-wrap: wrap;
}}

.role-badge {{
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 3px 11px;
    border-radius: 100px;
    font-size: 0.73rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}}

.role-badge.user {{ background: rgba(99, 102, 241, 0.2); color: #818cf8; }}
.role-badge.assistant {{ background: rgba(16, 185, 129, 0.2); color: #34d399; }}
.role-badge.delegate {{ background: rgba(245, 158, 11, 0.2); color: #fbbf24; }}

.agent-name {{
    font-size: 0.8rem;
    color: var(--text-muted);
    font-weight: 500;
}}

.model-tag {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    padding: 2px 8px;
    background: rgba(255,255,255, 0.05);
    border: 1px solid rgba(255,255,255, 0.08);
    border-radius: 4px;
    color: var(--text-muted);
}}

.msg-time {{
    margin-left: auto;
    font-size: 0.7rem;
    color: var(--text-muted);
    font-family: 'JetBrains Mono', monospace;
}}

.msg-body {{
    font-size: 0.88rem;
    line-height: 1.7;
    color: var(--text-primary);
    white-space: pre-wrap;
    word-wrap: break-word;
}}

.msg-body:empty {{ display: none; }}

.tool-section {{ margin-top: 12px; }}

.tool-toggle {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 5px 13px;
    background: var(--accent-tool-bg);
    border: 1px solid var(--accent-tool-border);
    border-radius: 8px;
    color: #a78bfa;
    font-size: 0.76rem;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s;
    user-select: none;
}}

.tool-toggle:hover {{
    background: rgba(139, 92, 246, 0.15);
    border-color: rgba(139, 92, 246, 0.35);
}}

.tool-toggle .arrow {{
    transition: transform 0.2s;
    font-size: 0.6rem;
}}

.tool-toggle.expanded .arrow {{
    transform: rotate(90deg);
}}

.tool-details {{
    display: none;
    margin-top: 10px;
    padding: 12px;
    background: rgba(0,0,0,0.25);
    border: 1px solid rgba(139, 92, 246, 0.12);
    border-radius: var(--radius-sm);
    overflow-x: auto;
}}

.tool-details.show {{ display: block; }}

.tool-item {{
    margin-bottom: 8px;
    padding-bottom: 8px;
    border-bottom: 1px solid rgba(255,255,255,0.04);
}}

.tool-item:last-child {{
    margin-bottom: 0;
    padding-bottom: 0;
    border-bottom: none;
}}

.tool-name {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.76rem;
    font-weight: 600;
    color: #a78bfa;
    margin-bottom: 3px;
}}

.tool-content {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.7rem;
    color: var(--text-muted);
    white-space: pre-wrap;
    word-wrap: break-word;
    max-height: 200px;
    overflow-y: auto;
}}

.token-info {{
    margin-top: 8px;
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
}}

.token-chip {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.66rem;
    padding: 2px 8px;
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 4px;
    color: var(--text-muted);
}}

.empty-state {{
    text-align: center;
    padding: 80px 20px;
    color: var(--text-muted);
}}

.empty-state .icon {{
    font-size: 3rem;
    margin-bottom: 16px;
    opacity: 0.5;
}}

.msg-num {{
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.65rem;
    color: var(--text-muted);
    opacity: 0.6;
    margin-right: 4px;
}}

::-webkit-scrollbar {{ width: 6px; height: 6px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{ background: var(--border-color); border-radius: 3px; }}
::-webkit-scrollbar-thumb:hover {{ background: var(--text-muted); }}

@media (max-width: 768px) {{
    .app {{ padding: 12px 10px 40px; }}
    .header h1 {{ font-size: 1.4rem; }}
    .message.delegate {{ margin-left: 16px; }}
    .controls {{ flex-direction: column; }}
    .search-box {{ flex: 1 1 100%; }}
}}
</style>
</head>
<body>
<div class="app">
    <div class="header">
        <h1>🔮 OpenCode Chat History</h1>
        <p>Interactive viewer — {len(sessions)} sessions, {sum(len(v) for v in conversations.values())} total messages</p>
        <div class="gen-time">Generated: {now}</div>
    </div>

    <div class="controls">
        <label for="session-select">Session</label>
        <select id="session-select" onchange="loadSession(this.value)">
            {''.join(session_options)}
        </select>
        <input type="text" class="search-box" id="search-box" placeholder="🔍 Filter messages..." oninput="filterMessages(this.value)">
    </div>

    <div class="legend">
        <div class="legend-item">
            <div class="legend-dot" style="background: var(--accent-user);"></div>
            <span>You (User)</span>
        </div>
        <div class="legend-item">
            <div class="legend-dot" style="background: var(--accent-assistant);"></div>
            <span>Agent Response</span>
        </div>
        <div class="legend-item">
            <div class="legend-dot" style="background: var(--accent-delegate);"></div>
            <span>Delegate / Sub-agent</span>
        </div>
        <div class="legend-item">
            <div class="legend-dot" style="background: var(--accent-tool);"></div>
            <span>Tool Calls</span>
        </div>
    </div>

    <div id="stats-bar" class="stats-bar"></div>

    <div id="chat-container" class="chat-container">
        <div class="empty-state">
            <div class="icon">💬</div>
            <p>Select a session above to view the conversation</p>
        </div>
    </div>
</div>

<script>
const conversations = {json.dumps(conv_data, ensure_ascii=False)};

const DELEGATE_KEYWORDS = [
    'sisyphus', 'explore', 'librarian', 'oracle', 'junior',
    'sub', 'delegate', 'worker', 'search'
];

function isDelegate(agent) {{
    if (!agent) return false;
    const lower = agent.toLowerCase();
    return DELEGATE_KEYWORDS.some(d => lower.includes(d)) || lower.includes('@');
}}

function escapeHtml(text) {{
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}}

let currentMessages = [];

function loadSession(sessionId) {{
    const conv = conversations[sessionId];
    const container = document.getElementById('chat-container');
    const statsBar = document.getElementById('stats-bar');
    document.getElementById('search-box').value = '';

    if (!conv || conv.length === 0) {{
        container.innerHTML = '<div class="empty-state"><div class="icon">📭</div><p>No messages in this session</p></div>';
        statsBar.innerHTML = '';
        currentMessages = [];
        return;
    }}

    let userCount = 0, assistantCount = 0, delegateCount = 0, toolCount = 0;
    const agents = new Set();
    const models = new Set();

    conv.forEach(msg => {{
        if (msg.role === 'user') userCount++;
        else if (isDelegate(msg.agent)) delegateCount++;
        else assistantCount++;
        if (msg.agent) agents.add(msg.agent);
        if (msg.model) models.add(msg.model);
        if (msg.tools) toolCount += msg.tools.length;
    }});

    statsBar.innerHTML = `
        <div class="stat-chip" title="Click to copy Session ID" onclick="navigator.clipboard.writeText('${{sessionId}}'); this.style.borderColor='var(--accent-assistant)'; setTimeout(()=>{{this.style.borderColor=''}}, 1000)">
            <span style="opacity: 0.7">🆔</span>
            <span class="count" style="font-family: 'JetBrains Mono', monospace; font-size: 0.7rem">${{sessionId.slice(0,8)}}...</span>
        </div>
        <div class="stat-chip"><div class="dot" style="background: var(--accent-user);"></div><span class="count">${{userCount}}</span> user</div>
        <div class="stat-chip"><div class="dot" style="background: var(--accent-assistant);"></div><span class="count">${{assistantCount}}</span> agent</div>
        ${{delegateCount > 0 ? `<div class="stat-chip"><div class="dot" style="background: var(--accent-delegate);"></div><span class="count">${{delegateCount}}</span> sub</div>` : ''}}
        <div class="stat-chip"><div class="dot" style="background: var(--accent-tool);"></div><span class="count">${{toolCount}}</span> tools</div>
        <div class="stat-chip">🤖 ${{agents.size}} reps</div>
        <div class="stat-chip">🧠 ${{[...models].join(', ')}}</div>
    `;

    let html = '';
    let toolToggleId = 0;
    let visibleIdx = 0;

    conv.forEach((msg, idx) => {{
        const isUser = msg.role === 'user';
        const isDel = !isUser && isDelegate(msg.agent);
        const roleClass = isUser ? 'user' : (isDel ? 'delegate' : 'assistant');
        const roleIcon = isUser ? '👤' : (isDel ? '🔧' : '🤖');
        const roleLabel = isUser ? 'You' : (isDel ? 'Delegate' : 'Agent');

        if (!msg.text && (!msg.tools || msg.tools.length === 0)) return;
        visibleIdx++;

        const toolsHtml = (msg.tools && msg.tools.length > 0) ? (() => {{
            const toggleId = `tool-${{toolToggleId++}}`;
            const items = msg.tools.map(t => {{
                if (t.type === 'call') {{
                    return `<div class="tool-item">
                        <div class="tool-name">▶ ${{escapeHtml(t.tool)}}</div>
                        <div class="tool-content">${{escapeHtml(t.args || '')}}</div>
                    </div>`;
                }} else {{
                    return `<div class="tool-item">
                        <div class="tool-name">◀ ${{escapeHtml(t.tool || 'result')}}</div>
                        <div class="tool-content">${{escapeHtml(t.result || '')}}</div>
                    </div>`;
                }}
            }}).join('');
            return `<div class="tool-section">
                <div class="tool-toggle" onclick="toggleTools('${{toggleId}}', this)">
                    <span class="arrow">▶</span>
                    🔧 ${{msg.tools.length}} tool interaction(s)
                </div>
                <div class="tool-details" id="${{toggleId}}">${{items}}</div>
            </div>`;
        }})() : '';

        const tokenHtml = (msg.tokens && (msg.tokens.input || msg.tokens.output)) ? `
            <div class="token-info">
                ${{msg.tokens.input ? `<span class="token-chip">↗ ${{msg.tokens.input.toLocaleString()}} in</span>` : ''}}
                ${{msg.tokens.output ? `<span class="token-chip">↙ ${{msg.tokens.output.toLocaleString()}} out</span>` : ''}}
                ${{msg.tokens.total ? `<span class="token-chip">Σ ${{msg.tokens.total.toLocaleString()}} total</span>` : ''}}
            </div>
        ` : '';

        html += `
        <div class="message ${{roleClass}}" data-text="${{escapeHtml((msg.text + ' ' + msg.agent + ' ' + msg.model).toLowerCase())}}" style="animation-delay: ${{visibleIdx * 0.02}}s">
            <div class="msg-header">
                <span class="msg-num">#${{visibleIdx}}</span>
                <span class="role-badge ${{roleClass}}">
                    <span>${{roleIcon}}</span>
                    ${{roleLabel}}
                </span>
                ${{msg.agent ? `<span class="agent-name">${{escapeHtml(msg.agent)}}</span>` : ''}}
                ${{msg.model ? `<span class="model-tag">${{escapeHtml(msg.model)}}</span>` : ''}}
                <span class="msg-time">${{escapeHtml(msg.time)}}</span>
            </div>
            ${{msg.text ? `<div class="msg-body">${{escapeHtml(msg.text)}}</div>` : ''}}
            ${{toolsHtml}}
            ${{tokenHtml}}
        </div>`;
    }});

    container.innerHTML = html || '<div class="empty-state"><div class="icon">📭</div><p>No visible messages</p></div>';
    currentMessages = container.querySelectorAll('.message');
}}

function filterMessages(query) {{
    const q = query.toLowerCase().trim();
    currentMessages.forEach(el => {{
        if (!q || el.dataset.text.includes(q)) {{
            el.classList.remove('hidden');
        }} else {{
            el.classList.add('hidden');
        }}
    }});
}}

function toggleTools(id, btn) {{
    const el = document.getElementById(id);
    el.classList.toggle('show');
    btn.classList.toggle('expanded');
}}

window.addEventListener('DOMContentLoaded', () => {{
    const select = document.getElementById('session-select');
    if (select.value) loadSession(select.value);
}});
</script>
</body>
</html>"""


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(DB_SOURCE):
        print(f"ERROR: OpenCode database not found at {DB_SOURCE}")
        print("Is OpenCode installed? Check ~/.local/share/opencode/")
        return

    # Copy DB to avoid locking issues
    print(f"Copying database from {DB_SOURCE}...")
    shutil.copy2(DB_SOURCE, DB_COPY)

    conn = sqlite3.connect(DB_COPY)
    cursor = conn.cursor()

    print("Extracting sessions...")
    sessions = extract_sessions(cursor)
    print(f"  Found {len(sessions)} sessions")

    print("Extracting conversations...")
    conversations = {}
    for s in sessions:
        sid = s[0]
        conversations[sid] = extract_conversation(cursor, sid)

    conn.close()

    # Clean up temp db
    try:
        os.remove(DB_COPY)
    except OSError:
        pass

    print("Generating HTML...")
    html_content = generate_html(sessions, conversations)

    output_path = os.path.join(OUTPUT_DIR, "index.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    total_msgs = sum(len(v) for v in conversations.values())
    print(f"\n{'='*50}")
    print(f"✅ Generated: {output_path}")
    print(f"   Sessions:  {len(sessions)}")
    print(f"   Messages:  {total_msgs}")
    print(f"{'='*50}")
    print("\nTo view, open in browser or run:")
    print("  cd ~/opencode_chat_viewer && python3 -m http.server 8765")
    print("  Then open: http://localhost:8765")


if __name__ == "__main__":
    main()
