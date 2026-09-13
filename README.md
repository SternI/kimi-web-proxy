# Kimi Web Proxy

OpenAI-compatible local API proxy for Kimi Web (`kimi.ai` & `kimi.moonshot.cn`).

Allows you to use Kimi (**Kimi Chat** with native thinking/reasoning and search toggles) with OpenCode or any OpenAI-compatible client.

---

## Disclaimer

This project is for educational and research purposes only. It is not affiliated with or endorsed by Moonshot AI or Kimi.

---

## Features

- **OpenAI API Compatibility**: Exposes `http://127.0.0.1:1340/v1/chat/completions` and `/v1/models`.
- **Connect-RPC Streaming**: Native handling of Kimi's `application/connect+json` binary framing protocol, unpacking 5-byte envelope frames directly into live text & reasoning tokens.
- **Thinking / Reasoning**: Streams thinking / reasoning process into `reasoning_content` delta chunks in real-time.
- **UI Setting Respect**: Uses `kimi-chat`, which automatically respects whatever you toggle in the Kimi Web UI (Thinking mode, Web Search).
- **Tool Calling**: Translates tool schemas and parses `<tool_call>` outputs into OpenAI function call structures for agent tools (`write`, `edit`, `bash`, `read`).
- **Chat Management**: Send `/clear`, `/reset`, or `/new` in chat to start a clean conversation session.
- **Lightweight**: Pure Python (`aiohttp`) + Tampermonkey script with no heavy automation frameworks.

---

## Quick Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Install Userscript

1. Install [Tampermonkey](https://www.tampermonkey.net/) or Violentmonkey in your browser.
2. Create a new userscript and paste the contents of [`kimi-bridge.user.js`](./kimi-bridge.user.js).
3. Open [kimi.ai](https://www.kimi.ai/) (or [kimi.moonshot.cn](https://kimi.moonshot.cn/)) and log in.
4. You will see a badge at the bottom-right: **Kimi Bridge: Connected (Ready)** once the proxy is running.

### 3. Start the Proxy

```bash
python kimi-proxy.py
```

Options:
- `--host 127.0.0.1`: Listening host (default: `127.0.0.1`).
- `--port 1340`: Listening port (default: `1340`).

---

## OpenCode Configration

Add this provider to your OpenCode config (`opencode.jsonc`):

```json
{
  "provider": {
    "kimi-proxy": {
      "api": "openai",
      "name": "Kimi Web Proxy",
      "options": {
        "baseURL": "http://127.0.0.1:1340/v1",
        "apiKey": "nah",
        "timeout": 300000,
        "chunkTimeout": 300000
      },
      "models": {
        "kimi-chat": {
          "id": "kimi-chat",
          "name": "Kimi Chat (Web Proxy)",
          "tool_call": true,
          "reasoning": true,
          "temperature": true
        }
      }
    }
  }
}
```

---

## Other Clients (Cline, Cursor, etc.)

- **Base URL**: `http://127.0.0.1:1340/v1`
- **API Key**: `nah`
- **Models**:
  - `kimi-chat`: Recommended default (respects active toggles in Web UI)
  - `kimi-thinking`: Forces thinking / reasoning mode via API

---

## Python Example

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:1340/v1",
    api_key="nah",
)

response = client.chat.completions.create(
    model="kimi-chat",
    messages=[{"role": "user", "content": "Hello Kimi"}],
    stream=True,
)

for chunk in response:
    reasoning = getattr(chunk.choices[0].delta, "reasoning_content", None)
    if reasoning:
        print(reasoning, end="", flush=True)
    content = chunk.choices[0].delta.content or ""
    print(content, end="", flush=True)
```

---

## Notes & Chat Management

- Keep the browser tab open while using the proxy.
- If the badge shows disconnected, click it to reconnect immediately.
- To reset manually, send `/clear`, `/reset`, or `/new` directly from your client prompt (or run `window.deleteCurrentChat()` in the browser console).

---

## License

MIT
