#!/usr/bin/env python3
"""
Kimi Web Proxy - Local OpenAI-compatible API bridge for Kimi Web (kimi.ai)
Accepts standard OpenAI chat completion requests (including streaming, reasoning, and tool calls)
and proxies them to your logged-in Kimi browser session via a Tampermonkey userscript.

Supports:
  - Streaming SSE completions (choices[0].delta.content)
  - Real-time Thinking / Reasoning streaming (choices[0].delta.reasoning_content)
  - Tool calling via XML emulation (<tool_call>)
  - Models: kimi-chat (default; respects active UI toggles), kimi-thinking (explicit reasoning)
  - Session reset commands (/reset, new chat, clear)
  - Connect-RPC (application/connect+json) stream decoding
  - Both WebSocket and HTTP long-polling (CSP-safe)
"""

import argparse
import asyncio
import contextlib
import json
import logging
import re
import sys
import time
import uuid
from typing import Any, Optional

from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="\033[90m%(asctime)s\033[0m %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("kimi-proxy")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 1340
JOB_TIMEOUT = 3600  # seconds
HISTORY_TAIL = 20
MAX_PROMPT_CHARS = 48000
ARG_CHUNK_SIZE = 128

MODELS = [
    {
        "id": "kimi-chat",
        "object": "model",
        "created": 1700000000,
        "owned_by": "moonshot",
        "name": "Kimi Chat (via Web Proxy)",
    },
    {
        "id": "kimi",
        "object": "model",
        "created": 1700000000,
        "owned_by": "moonshot",
        "name": "Kimi Chat",
    },
    {
        "id": "kimi-thinking",
        "object": "model",
        "created": 1700000000,
        "owned_by": "moonshot",
        "name": "Kimi Chat (Thinking / Reasoning)",
    },
]

RESET_COMMANDS = {"/reset", "reset", "clear", "new chat", "/new", "/deletecurrentchat"}

# Global State
active_ws: Optional[web.WebSocketResponse] = None
pending_jobs: dict[str, asyncio.Future] = {}
job_queues: dict[str, asyncio.Queue] = {}
_job_semaphore: Optional[asyncio.Semaphore] = None

# HTTP long-polling queue (CSP bypass)
poll_queue: asyncio.Queue = asyncio.Queue()
last_poll_time: float = 0.0
_poll_connected: bool = False


def is_bridge_connected() -> bool:
    global _poll_connected
    if active_ws is not None and not active_ws.closed:
        return True
    if time.time() - last_poll_time < 30.0:
        return True
    if _poll_connected:
        _poll_connected = False
        print(
            "\033[93m[Bridge]\033[0m Kimi browser tab disconnected (HTTP poll timeout)."
        )
    return False


def mark_poll_active(remote: str = "127.0.0.1") -> None:
    global last_poll_time, _poll_connected
    last_poll_time = time.time()
    if not _poll_connected and (not active_ws or active_ws.closed):
        _poll_connected = True
        print(
            f"\033[92m[Bridge]\033[0m Kimi browser tab connected! (HTTP Long-Poll / CSP-safe, Remote: {remote})"
        )


TOOL_PROTOCOL_TEMPLATE = """You are an AI assistant exposed through an OpenAI-compatible API with tool support.

You have access to the following tools:
{tool_list}

TOOL INSTRUCTIONS:
1. When you need to take an action (e.g. create a file, write/edit code, execute commands, list or read files), output a tool call using this EXACT XML format:
<tool_call>
<name>TOOL_NAME</name>
<arguments>{{"argument_name": "argument value"}}</arguments>
</tool_call>

2. <arguments> must be a valid JSON object matching the tool parameters.
3. Use ONLY tool names from the available tools list above.
4. You do NOT need direct filesystem or terminal access yourself. The user's external IDE agent (OpenCode / Cline) will intercept your <tool_call> XML block and execute it locally on the user's machine. Therefore, NEVER apologize, NEVER refuse, and NEVER say that workspace or file-writing tools are not available. Simply emit the <tool_call> block.
5. When you DO NOT need to call a tool (e.g. conversational answers, greetings, explanations, reviews), respond DIRECTLY in normal markdown text without any <tool_call> tags.
"""

CLINE_XML_PROTOCOL = """# Tool Use Formatting

You are an AI assistant integrated into Cline (a VS Code coding agent).

You MUST respond using ONLY the following XML tool format. Do NOT write plain prose answers.

## attempt_completion
When you are ready to give your final answer or complete the task, use:

<attempt_completion>
<result>
Your final answer or task summary goes here.
</result>
</attempt_completion>

## ask_followup_question
When you need more information from the user, use:

<ask_followup_question>
<question>Your question goes here.</question>
</ask_followup_question>

## Rules
1. Every response MUST be exactly one tool call block.
2. Do not wrap the XML in markdown code fences.
3. Do not include any text outside the XML block.
4. For simple factual questions, respond with attempt_completion containing the answer.
"""

CLINE_XML_MARKERS = (
    "<attempt_completion>",
    "<ask_followup_question>",
    "# Tool Use",
    "TOOL USE",
)


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    global active_ws
    ws = web.WebSocketResponse(heartbeat=30, max_msg_size=16 * 1024 * 1024)
    await ws.prepare(request)

    active_ws = ws
    print(
        f"\033[92m[Bridge]\033[0m Kimi browser tab connected! (WebSocket, Remote: {request.remote})"
    )

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    print(
                        f"\033[91m[Bridge]\033[0m Bad JSON from browser: {msg.data[:200]!r}"
                    )
                    continue

                req_id = data.get("id")
                msg_type = data.get("type")

                if msg_type == "chunk":
                    q = job_queues.get(req_id)
                    if q:
                        q.put_nowait(data)
                    continue

                if msg_type == "final":
                    q = job_queues.get(req_id)
                    if q:
                        q.put_nowait(data)

                future = pending_jobs.get(req_id) if req_id else None
                if future and not future.done():
                    future.set_result(data)

            elif msg.type in (web.WSMsgType.CLOSED, web.WSMsgType.ERROR):
                break
    finally:
        if active_ws == ws:
            active_ws = None
        print("\033[93m[Bridge]\033[0m Kimi browser tab disconnected (WebSocket).")

    return ws


async def handle_poll(request: web.Request) -> web.Response:
    mark_poll_active(request.remote or "127.0.0.1")
    try:
        payload = await asyncio.wait_for(poll_queue.get(), timeout=25.0)
        return web.json_response(payload, headers=CORS_HEADERS)
    except asyncio.TimeoutError:
        return web.json_response({"action": "noop"}, headers=CORS_HEADERS)


async def handle_chunk(request: web.Request) -> web.Response:
    mark_poll_active(request.remote or "127.0.0.1")
    try:
        data = await request.json()
    except Exception:
        return web.json_response(
            {"error": "invalid json"}, status=400, headers=CORS_HEADERS
        )

    req_id = data.get("id")
    q = job_queues.get(req_id)
    if q:
        q.put_nowait({"type": "chunk", **data})

    return web.json_response({"status": "ok"}, headers=CORS_HEADERS)


async def handle_reply(request: web.Request) -> web.Response:
    mark_poll_active(request.remote or "127.0.0.1")
    try:
        data = await request.json()
    except Exception:
        return web.json_response(
            {"error": "invalid json"}, status=400, headers=CORS_HEADERS
        )

    req_id = data.get("id")
    future = pending_jobs.get(req_id)
    if future and not future.done():
        future.set_result(data)

    q = job_queues.get(req_id)
    if q:
        q.put_nowait({"type": "final", **data})

    return web.json_response({"status": "ok"}, headers=CORS_HEADERS)


def _message_text(message: dict) -> str:
    parts = []
    content = message.get("content", "")
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict):
                if block.get("type") == "text":
                    parts.append(block.get("text", ""))
                elif "text" in block:
                    parts.append(str(block["text"]))
    elif content:
        parts.append(str(content))
    tool_calls = message.get("tool_calls")
    if isinstance(tool_calls, list):
        for tc in tool_calls:
            fn = tc.get("function", tc) if isinstance(tc, dict) else {}
            name = fn.get("name", "")
            args = fn.get("arguments", "")
            if isinstance(args, dict):
                args = json.dumps(args, ensure_ascii=False)
            if name:
                parts.append(
                    f"<tool_call>\n<name>{name}</name>\n<arguments>{args}</arguments>\n</tool_call>"
                )

    return "\n".join(p for p in parts if p)


def _truncate_middle(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    keep = limit // 2
    return text[:keep] + "\n\n[... truncated ...]\n\n" + text[-keep:]


def _build_tool_list(tools: list[dict]) -> str:
    lines = []
    for tool in tools:
        fn = tool.get("function", tool) if isinstance(tool, dict) else {}
        name = fn.get("name", "unknown")
        desc = (fn.get("description") or "").strip().replace("\n", " ")
        params = fn.get("parameters") or {}
        lines.append(f"- {name}: {desc}")
        lines.append(f"  parameters: {json.dumps(params, ensure_ascii=False)}")
    return "\n".join(lines)


def _detect_cline_xml(messages: list[dict]) -> bool:
    for message in messages:
        if message.get("role") == "system":
            text = _message_text(message)
            if any(marker in text for marker in CLINE_XML_MARKERS):
                return True
    return False


def build_prompt(
    messages: list[dict],
    tools: Optional[list[dict]],
    tool_choice: Any = None,
) -> tuple[str, str]:
    use_tools = bool(tools) and tool_choice != "none"
    cline_xml = (not use_tools) and _detect_cline_xml(messages)

    if use_tools:
        mode = "native_tools"
    elif cline_xml:
        mode = "cline_xml"
    else:
        mode = "text"

    system_parts: list[str] = []
    conversation: list[str] = []

    for message in messages:
        role = (message.get("role") or "user").lower()
        text = _message_text(message)

        if role == "system":
            system_parts.append(text)
        elif role == "user":
            conversation.append(f"User:\n{text}")
        elif role == "assistant":
            if text:
                conversation.append(f"Assistant:\n{text}")
        elif role == "tool":
            conversation.append(f"Tool result:\n{text}")

    if len(conversation) > HISTORY_TAIL:
        conversation = conversation[-HISTORY_TAIL:]

    sections: list[str] = []

    if mode == "native_tools":
        sections.append(
            TOOL_PROTOCOL_TEMPLATE.format(tool_list=_build_tool_list(tools or []))
        )
    elif mode == "cline_xml":
        sections.append(CLINE_XML_PROTOCOL)

    if system_parts:
        sections.append("# System Context\n" + "\n\n".join(system_parts))

    if conversation:
        sections.append("# Conversation\n" + "\n\n".join(conversation))

    if mode == "native_tools":
        sections.append(
            "# Immediate Action Instruction\n"
            "If the user is asking you to create, write, or modify a file (e.g. `simple_game.html`) or run a command, "
            "do NOT say you cannot write files or that tools are unavailable. "
            "The client editor (OpenCode) will execute the action on the user's computer. "
            "Output your action immediately using this exact XML format:\n"
            "<tool_call>\n"
            "<name>TOOL_NAME</name>\n"
            '<arguments>{"param": "value"}</arguments>\n'
            "</tool_call>"
        )

    prompt = "\n\n".join(sections).strip()
    prompt = _truncate_middle(prompt, MAX_PROMPT_CHARS)
    return prompt, mode


_TOOL_CALL_BLOCK_RE = re.compile(
    r"<tool_call>(.*?)(?:</tool_call>|(?=<tool_call>)|$)",
    re.DOTALL | re.IGNORECASE,
)
_TAG_NAME_RE = re.compile(r"<name>(.*?)</name>", re.DOTALL | re.IGNORECASE)
_TAG_ARGS_RE = re.compile(
    r"<arguments>(.*?)(?:</arguments>|$)",
    re.DOTALL | re.IGNORECASE,
)
_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)
_FENCE_RE = re.compile(r"```[a-zA-Z0-9_-]*\s*|\s*```")


def _strip_fences(text: str) -> str:
    return _FENCE_RE.sub("", text).strip()


def _ensure_code_fenced(text: str) -> str:
    trimmed = text.strip()
    if "```" in trimmed or not trimmed:
        return text

    code_indicators = (
        (
            "python",
            (
                "with open(",
                "import ",
                "from ",
                "def ",
                "class ",
                "print(",
                "if __name__",
            ),
        ),
        (
            "powershell",
            (
                "Get-",
                "Set-",
                "New-Item",
                "Remove-Item",
                "Write-Output",
                "Test-Path",
            ),
        ),
        (
            "bash",
            (
                "#!/bin/bash",
                "#!/bin/sh",
                "sudo ",
                "curl ",
                "npm ",
                "git ",
                "pip ",
                "docker ",
            ),
        ),
        (
            "javascript",
            (
                "const ",
                "let ",
                "var ",
                "function(",
                "function ",
                "console.log(",
                "export ",
            ),
        ),
    )

    first_line = trimmed.split("\n", 1)[0].strip()
    for lang, markers in code_indicators:
        if any(first_line.startswith(m) for m in markers):
            return f"```{lang}\n{trimmed}\n```"

    lines = trimmed.split("\n")
    if len(lines) >= 2:
        if any(
            l.strip().startswith(
                ("import ", "from ", "def ", "class ", "with open(", "with ")
            )
            for l in lines[:3]
        ):
            return f"```python\n{trimmed}\n```"

    return text


def _loads_lenient(raw: str) -> Optional[Any]:
    if raw is None:
        return None
    candidate = raw.strip()
    if not candidate:
        return None

    def _escape_newlines_in_strings(text: str) -> str:
        result: list[str] = []
        in_string = False
        i = 0
        while i < len(text):
            c = text[i]
            if c == "\\" and in_string:
                result.append(c)
                i += 1
                if i < len(text):
                    result.append(text[i])
                    i += 1
                continue
            if c == '"':
                in_string = not in_string
                result.append(c)
            elif in_string and c == "\n":
                result.append("\\n")
            elif in_string and c == "\r":
                if i + 1 < len(text) and text[i + 1] == "\n":
                    i += 1
                result.append("\\n")
            else:
                result.append(c)
            i += 1
        return "".join(result)

    no_trail = re.sub(r",\s*([}\]])", r"\1", candidate)
    escaped = _escape_newlines_in_strings(candidate)
    escaped_no_trail = re.sub(r",\s*([}\]])", r"\1", escaped)

    for attempt in (candidate, no_trail, escaped, escaped_no_trail):
        try:
            return json.loads(attempt)
        except (json.JSONDecodeError, ValueError):
            continue
    return None


def _find_json_in(text: str) -> Optional[dict]:
    for match in _JSON_OBJECT_RE.finditer(text):
        parsed = _loads_lenient(match.group(0))
        if isinstance(parsed, dict):
            return parsed
    return None


def parse_tool_calls(raw: str, tool_names: list[str]) -> list[dict]:
    if not raw:
        return []

    calls: list[dict] = []

    for block in _TOOL_CALL_BLOCK_RE.findall(raw):
        block = block.strip()
        if not block:
            continue
        name_match = _TAG_NAME_RE.search(block)
        args_match = _TAG_ARGS_RE.search(block)

        if not name_match:
            continue

        raw_name = name_match.group(1).strip()
        raw_args = args_match.group(1).strip() if args_match else "{}"

        matched_name = raw_name
        for candidate in tool_names:
            if candidate.lower() == raw_name.lower():
                matched_name = candidate
                break

        parsed_args = _loads_lenient(raw_args)
        if parsed_args is None:
            parsed_args = _find_json_in(raw_args)
        if parsed_args is None:
            parsed_args = _loads_lenient(_strip_fences(raw_args))
        if parsed_args is None:
            parsed_args = _find_json_in(_strip_fences(raw_args))

        if not isinstance(parsed_args, dict):
            parsed_args = {}

        calls.append(
            {
                "id": f"call_{uuid.uuid4().hex[:8]}",
                "type": "function",
                "function": {
                    "name": matched_name,
                    "arguments": json.dumps(parsed_args, ensure_ascii=False),
                },
            }
        )

    if not calls and tool_names:
        fallback = _extract_unfenced_tool_call(raw, tool_names)
        if fallback:
            calls.append(fallback)

    return calls


def _extract_unfenced_tool_call(raw: str, tool_names: list[str]) -> Optional[dict]:
    name_match = _TAG_NAME_RE.search(raw)
    args_match = _TAG_ARGS_RE.search(raw)
    if name_match and args_match:
        name = name_match.group(1).strip()
        args_str = args_match.group(1).strip()
        parsed = _loads_lenient(args_str) or _find_json_in(args_str)
        if isinstance(parsed, dict):
            return {
                "id": f"call_{uuid.uuid4().hex[:8]}",
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": json.dumps(parsed, ensure_ascii=False),
                },
            }

    parsed = _find_json_in(raw)
    if isinstance(parsed, dict):
        action = parsed.get("action") or parsed.get("tool") or parsed.get("name")
        params = (
            parsed.get("parameters") or parsed.get("arguments") or parsed.get("args")
        )
        if action and isinstance(action, str) and action in tool_names:
            args_obj = params if isinstance(params, dict) else {}
            return {
                "id": f"call_{uuid.uuid4().hex[:8]}",
                "type": "function",
                "function": {
                    "name": action,
                    "arguments": json.dumps(args_obj, ensure_ascii=False),
                },
            }

    return None


def strip_tool_call_xml(text: str) -> str:
    cleaned = _TOOL_CALL_BLOCK_RE.sub("", text)
    cleaned = re.sub(r"</?tool_call>", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


async def dispatch_browser_job(
    job_id: str,
    prompt: str,
    thinking: Optional[bool] = None,
    model: Optional[str] = None,
    feature: Optional[dict] = None,
) -> bool:
    payload: dict[str, Any] = {
        "action": "prompt",
        "id": job_id,
        "prompt": prompt,
    }
    if thinking is not None:
        payload["thinking"] = thinking
    if model:
        payload["model"] = model
    if feature:
        payload["feature"] = feature

    if active_ws and not active_ws.closed:
        await active_ws.send_str(json.dumps(payload))
        return True
    else:
        await poll_queue.put(payload)
        return True


async def run_browser_job(
    prompt: str,
    thinking: Optional[bool] = None,
    model: Optional[str] = None,
    feature: Optional[dict] = None,
) -> dict:
    if not is_bridge_connected():
        return {
            "error": "No Kimi browser tab is connected. Open https://www.kimi.ai/ and make sure the Tampermonkey bridge script is active."
        }

    job_id = uuid.uuid4().hex
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    pending_jobs[job_id] = future

    try:
        await dispatch_browser_job(job_id, prompt, thinking, model, feature)
        result = await asyncio.wait_for(future, timeout=JOB_TIMEOUT)
        return result
    except asyncio.TimeoutError:
        return {"error": f"Kimi request timed out after {JOB_TIMEOUT}s."}
    except Exception as exc:
        return {"error": f"Failed to communicate with browser tab: {exc}"}
    finally:
        pending_jobs.pop(job_id, None)


async def send_bridge_action(action: str) -> dict:
    if not is_bridge_connected():
        return {"error": "No Kimi browser tab connected."}

    job_id = uuid.uuid4().hex
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    pending_jobs[job_id] = future

    payload = {"action": action, "id": job_id}
    try:
        if active_ws and not active_ws.closed:
            await active_ws.send_str(json.dumps(payload))
        else:
            await poll_queue.put(payload)

        result = await asyncio.wait_for(future, timeout=15)
        return result
    except asyncio.TimeoutError:
        return {"error": f"Action '{action}' timed out after 15s."}
    except Exception as exc:
        return {"error": f"Action '{action}' failed: {exc}"}
    finally:
        pending_jobs.pop(job_id, None)


CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
}


def error_response(
    message: str, status: int = 400, err_type: str = "invalid_request_error"
) -> web.Response:
    return web.json_response(
        {
            "error": {
                "message": message,
                "type": err_type,
                "param": None,
                "code": None,
            }
        },
        status=status,
        headers=CORS_HEADERS,
    )


async def handle_models(request: web.Request) -> web.Response:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)
    return web.json_response({"object": "list", "data": MODELS}, headers=CORS_HEADERS)


async def handle_health(request: web.Request) -> web.Response:
    conn_type = "none"
    if active_ws and not active_ws.closed:
        conn_type = "websocket"
    elif is_bridge_connected():
        conn_type = "http_poll"

    return web.json_response(
        {
            "status": "ok",
            "browser_connected": is_bridge_connected(),
            "connection_type": conn_type,
            "pending_jobs": len(pending_jobs),
        },
        headers=CORS_HEADERS,
    )


def _check_reset_command(messages: list[dict]) -> Optional[str]:
    for msg in reversed(messages):
        if (msg.get("role") or "").lower() == "user":
            text = _message_text(msg).strip().lower()
            if text in RESET_COMMANDS:
                return text
            break
    return None


def chunk(
    completion_id: str,
    created: int,
    model: str,
    delta: dict,
    finish_reason: Optional[str] = None,
    usage: Optional[dict] = None,
) -> str:
    choice: dict[str, Any] = {
        "index": 0,
        "delta": delta,
        "finish_reason": finish_reason,
    }
    obj: dict[str, Any] = {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [choice],
    }
    if usage:
        obj["usage"] = usage
    return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"


async def sse_write(response: web.StreamResponse, payload: str) -> None:
    await response.write(payload.encode("utf-8"))


@contextlib.asynccontextmanager
async def job_lock():
    global _job_semaphore
    if _job_semaphore is None:
        _job_semaphore = asyncio.Semaphore(1)
    async with _job_semaphore:
        yield


async def handle_chat_completions(request: web.Request) -> web.StreamResponse:
    if request.method == "OPTIONS":
        return web.Response(status=204, headers=CORS_HEADERS)

    try:
        body = await request.json()
    except (json.JSONDecodeError, ValueError):
        return error_response("Request body must be valid JSON.", 400)

    messages = body.get("messages")
    if not messages or not isinstance(messages, list):
        return error_response("'messages' is required and must be a list.", 400)

    reset_cmd = _check_reset_command(messages)
    if reset_cmd:
        print(
            f"\033[94m[Command]\033[0m Detected reset command: {reset_cmd!r}. Resetting Kimi chat session..."
        )
        bridge_res = await send_bridge_action("delete_chat")
        if "error" in bridge_res:
            print(f"\033[91m[Command]\033[0m Reset failed: {bridge_res['error']}")
            msg_content = f"Failed to reset Kimi chat: {bridge_res['error']}"
        else:
            print(f"\033[92m[Command]\033[0m Kimi chat successfully reset.")
            msg_content = "Kimi chat session has been reset and a new chat started."

        completion_id = f"chatcmpl-{uuid.uuid4().hex}"
        created = int(time.time())
        model = body.get("model", "kimi-chat")
        stream = body.get("stream", False)

        if stream:
            response = web.StreamResponse(
                status=200,
                headers={
                    "Content-Type": "text/event-stream; charset=utf-8",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    **CORS_HEADERS,
                },
            )
            await response.prepare(request)
            await sse_write(
                response,
                chunk(
                    completion_id,
                    created,
                    model,
                    {"role": "assistant", "content": msg_content},
                ),
            )
            await sse_write(response, chunk(completion_id, created, model, {}, "stop"))
            await response.write(b"data: [DONE]\n\n")
            await response.write_eof()
            return response
        else:
            return web.json_response(
                {
                    "id": completion_id,
                    "object": "chat.completion",
                    "created": created,
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": msg_content},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 1,
                        "completion_tokens": 1,
                        "total_tokens": 2,
                    },
                },
                headers=CORS_HEADERS,
            )

    tools = body.get("tools")
    tool_choice = body.get("tool_choice")
    stream = body.get("stream", False)
    stream_options = body.get("stream_options") or {}
    include_usage = stream and stream_options.get("include_usage", False)

    model = body.get("model", "kimi-chat")
    prompt, mode = build_prompt(messages, tools, tool_choice)

    # If model explicitly specifies thinking, set it; otherwise leave None so Kimi Web UI toggle is respected
    thinking: Optional[bool] = None
    if "thinking" in model.lower() or "reasoning" in model.lower():
        thinking = True

    feature: Optional[dict] = None
    if "search" in model.lower():
        feature = {"chat_type": "search"}

    tool_names = [
        t["function"]["name"]
        for t in (tools or [])
        if "function" in t and "name" in t["function"]
    ]

    print(
        f"\033[96m[Request]\033[0m model={model} mode={mode} stream={stream} "
        f"messages={len(messages)} tools={len(tool_names)} prompt_chars={len(prompt)}"
    )

    if stream:
        return await handle_live_stream_completions(
            request, prompt, model, thinking, feature, mode, tool_names, include_usage
        )

    # Non-streaming
    completion_id = f"chatcmpl-{uuid.uuid4().hex}"
    created = int(time.time())

    async with job_lock():
        result = await run_browser_job(prompt, thinking, model, feature)

    if "error" in result:
        print(f"\033[91m[Error]\033[0m {result['error']}")
        return error_response(result["error"], 502)

    raw_text = result.get("text", "")
    reasoning_content = result.get("reasoning", "")

    estimated_prompt_tokens = result.get("input_tokens", 0) or max(1, len(prompt) // 4)
    estimated_completion_tokens = result.get("output_tokens", 0) or max(
        1, len(raw_text) // 4
    )
    usage = {
        "prompt_tokens": estimated_prompt_tokens,
        "completion_tokens": estimated_completion_tokens,
        "total_tokens": estimated_prompt_tokens + estimated_completion_tokens,
    }

    if mode == "native_tools":
        tool_calls = parse_tool_calls(raw_text, tool_names)
        clean_text = strip_tool_call_xml(raw_text)
    else:
        tool_calls = []
        clean_text = _ensure_code_fenced(raw_text)

    message: dict[str, Any] = {"role": "assistant"}
    if clean_text:
        message["content"] = clean_text
    else:
        message["content"] = None

    if reasoning_content:
        message["reasoning_content"] = reasoning_content

    if tool_calls:
        message["tool_calls"] = tool_calls
        finish_reason = "tool_calls"
    else:
        finish_reason = "stop"

    return web.json_response(
        {
            "id": completion_id,
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": message,
                    "finish_reason": finish_reason,
                }
            ],
            "usage": usage,
        },
        headers=CORS_HEADERS,
    )


async def handle_live_stream_completions(
    request: web.Request,
    prompt: str,
    model: str,
    thinking: Optional[bool],
    feature: Optional[dict],
    mode: str,
    tool_names: list[str],
    include_usage: bool,
) -> web.StreamResponse:
    if not is_bridge_connected():
        return error_response(
            "No Kimi browser tab connected. Make sure https://www.kimi.ai/ is open with userscript running.",
            502,
        )

    completion_id = f"chatcmpl-{uuid.uuid4().hex}"
    created = int(time.time())
    job_id = uuid.uuid4().hex

    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            **CORS_HEADERS,
        },
    )
    await response.prepare(request)

    # Initial chunk
    await sse_write(
        response,
        chunk(completion_id, created, model, {"role": "assistant", "content": ""}),
    )

    q: asyncio.Queue = asyncio.Queue()
    job_queues[job_id] = q

    full_text = ""
    full_reasoning = ""
    last_sent_text_len = 0
    last_sent_reasoning_len = 0
    is_tool_mode = mode == "native_tools"

    try:
        # Serialized execution: lock held for stream duration to prevent collisions in browser
        async with job_lock():
            await dispatch_browser_job(job_id, prompt, thinking, model, feature)

            while True:
                try:
                    item = await asyncio.wait_for(q.get(), timeout=JOB_TIMEOUT)
                except asyncio.TimeoutError:
                    break

                if item.get("type") == "chunk":
                    t_delta = item.get("textDelta") or ""
                    r_delta = item.get("reasoningDelta") or ""
                    full_text += t_delta
                    full_reasoning += r_delta

                    if r_delta:
                        await sse_write(
                            response,
                            chunk(
                                completion_id,
                                created,
                                model,
                                {"reasoning_content": r_delta},
                            ),
                        )

                    if not is_tool_mode:
                        if t_delta:
                            await sse_write(
                                response,
                                chunk(
                                    completion_id, created, model, {"content": t_delta}
                                ),
                            )
                    else:
                        stripped_start = full_text.lstrip()
                        if (
                            stripped_start
                            and not stripped_start.startswith("<tool_call")
                            and not "<tool_call>" in full_text
                        ):
                            unsent = full_text[last_sent_text_len:]
                            if unsent:
                                last_sent_text_len = len(full_text)
                                await sse_write(
                                    response,
                                    chunk(
                                        completion_id,
                                        created,
                                        model,
                                        {"content": unsent},
                                    ),
                                )

                elif item.get("type") == "final":
                    result = item
                    raw_text = result.get("text", full_text)
                    reasoning_content = result.get("reasoning", full_reasoning)
                    tokens_meta = {
                        "input_tokens": max(1, len(prompt) // 4),
                        "output_tokens": max(1, len(raw_text) // 4),
                        "total_tokens": len(prompt) // 4 + len(raw_text) // 4,
                    }

                    print(
                        f"\033[92m[Success]\033[0m Streamed: {len(raw_text)} chars, reasoning: {len(reasoning_content)} chars "
                        f"(tokens: ~{tokens_meta['total_tokens']})"
                    )

                    if is_tool_mode:
                        tool_calls = parse_tool_calls(raw_text, tool_names)
                        if tool_calls:
                            for idx, tc in enumerate(tool_calls):
                                await sse_write(
                                    response,
                                    chunk(
                                        completion_id,
                                        created,
                                        model,
                                        {
                                            "tool_calls": [
                                                {
                                                    "index": idx,
                                                    "id": tc["id"],
                                                    "type": "function",
                                                    "function": {
                                                        "name": tc["function"]["name"],
                                                        "arguments": "",
                                                    },
                                                }
                                            ]
                                        },
                                    ),
                                )
                                raw_args = tc["function"]["arguments"]
                                for i in range(0, len(raw_args), ARG_CHUNK_SIZE):
                                    await sse_write(
                                        response,
                                        chunk(
                                            completion_id,
                                            created,
                                            model,
                                            {
                                                "tool_calls": [
                                                    {
                                                        "index": idx,
                                                        "function": {
                                                            "arguments": raw_args[
                                                                i : i + ARG_CHUNK_SIZE
                                                            ]
                                                        },
                                                    }
                                                ]
                                            },
                                        ),
                                    )
                            await sse_write(
                                response,
                                chunk(completion_id, created, model, {}, "tool_calls"),
                            )
                        else:
                            unsent = raw_text[last_sent_text_len:]
                            if unsent:
                                await sse_write(
                                    response,
                                    chunk(
                                        completion_id,
                                        created,
                                        model,
                                        {"content": unsent},
                                    ),
                                )
                            await sse_write(
                                response,
                                chunk(completion_id, created, model, {}, "stop"),
                            )
                    else:
                        unsent = raw_text[last_sent_text_len:]
                        if unsent:
                            await sse_write(
                                response,
                                chunk(
                                    completion_id, created, model, {"content": unsent}
                                ),
                            )
                        await sse_write(
                            response, chunk(completion_id, created, model, {}, "stop")
                        )

                    if include_usage:
                        await sse_write(
                            response,
                            chunk(
                                completion_id,
                                created,
                                model,
                                {},
                                finish_reason=None,
                                usage=tokens_meta,
                            ),
                        )

                    await response.write(b"data: [DONE]\n\n")
                    await response.write_eof()
                    break

    except Exception as exc:
        print(f"\033[91m[Error]\033[0m Streaming error: {exc}")
    finally:
        job_queues.pop(job_id, None)

    return response


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/poll", handle_poll)
    app.router.add_post("/chunk", handle_chunk)
    app.router.add_post("/reply", handle_reply)
    app.router.add_post("/v1/chat/completions", handle_chat_completions)
    app.router.add_get("/v1/models", handle_models)
    app.router.add_get("/health", handle_health)
    return app


def main():
    parser = argparse.ArgumentParser(description="Kimi Web Proxy")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host to bind to")
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="Port to listen on"
    )
    args = parser.parse_args()

    print(
        f"\033[96m"
        f"==================================================\n"
        f"  Kimi Web Proxy - OpenAI API Bridge\n"
        f"  Port: {args.port} | Host: {args.host}\n"
        f"  Default Model: kimi-chat\n"
        f"  Status: Waiting for Kimi browser tab to connect...\n"
        f"==================================================\033[0m"
    )
    app = make_app()
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
