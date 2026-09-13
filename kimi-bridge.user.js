// ==UserScript==
// @name         Kimi Web Bridge
// @namespace    https://github.com/your-username/kimi-web-proxy
// @version      1.0.0
// @description  Automates kimi.ai bridge for local OpenAI-compatible proxy
// @match        https://www.kimi.ai/*
// @match        https://kimi.moonshot.cn/*
// @grant        GM_xmlhttpRequest
// @grant        unsafeWindow
// @connect      127.0.0.1
// @run-at       document-start
// ==/UserScript==

(function () {
  "use strict";

  const win = typeof unsafeWindow !== "undefined" ? unsafeWindow : window;
  const doc = win.document || document;

  if (win.__KIMI_BRIDGE_INITIALIZED__) {
    console.log("[Kimi Bridge] Already running.");
    return;
  }
  win.__KIMI_BRIDGE_INITIALIZED__ = true;

  const PROXY_PORT = 1340;
  const WS_URL = `ws://127.0.0.1:${PROXY_PORT}/ws`;
  const POLL_URL = `http://127.0.0.1:${PROXY_PORT}/poll`;
  const CHUNK_URL = `http://127.0.0.1:${PROXY_PORT}/chunk`;
  const REPLY_URL = `http://127.0.0.1:${PROXY_PORT}/reply`;

  const RECONNECT_INTERVAL_MS = 3_000;
  const POLL_INTERVAL_MS = 250;

  let ws = null;
  let isConnected = false;
  let usingHttpPoll = false;
  let isPollLoopRunning = false;
  let _pendingCapture = null;

  const log = (msg, color = "#38bdf8") => {
    const time = new Date().toLocaleTimeString();
    console.log(`%c[Kimi Bridge ${time}] ${msg}`, `color:${color};font-weight:bold;`);
  };

  const badge = doc.createElement("div");
  badge.id = "kimi-bridge-badge";
  badge.title = "Click to reconnect to proxy";
  Object.assign(badge.style, {
    position: "fixed",
    bottom: "16px",
    right: "16px",
    zIndex: "999999",
    padding: "6px 14px",
    borderRadius: "20px",
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
    fontSize: "12px",
    fontWeight: "600",
    color: "#fff",
    backgroundColor: "rgba(15, 23, 42, 0.85)",
    backdropFilter: "blur(6px)",
    border: "1px solid rgba(255, 255, 255, 0.12)",
    boxShadow: "0 4px 12px rgba(0, 0, 0, 0.25)",
    display: "flex",
    alignItems: "center",
    gap: "8px",
    cursor: "pointer",
    userSelect: "none",
    transition: "all 0.2s ease",
  });

  const dot = doc.createElement("span");
  Object.assign(dot.style, {
    width: "8px",
    height: "8px",
    borderRadius: "50%",
    backgroundColor: "#ef4444",
    transition: "background-color 0.2s ease",
  });

  const text = doc.createElement("span");
  text.textContent = "Bridge: Disconnected";

  badge.appendChild(dot);
  badge.appendChild(text);

  function updateBadge(state, message) {
    if (!badge.parentElement && doc.body) {
      doc.body.appendChild(badge);
    }
    if (state === "connected") {
      dot.style.backgroundColor = "#10b981";
      text.textContent = message || "Bridge: Connected (Ready)";
    } else if (state === "busy") {
      dot.style.backgroundColor = "#f59e0b";
      text.textContent = message || "Bridge: Working...";
    } else {
      dot.style.backgroundColor = "#ef4444";
      text.textContent = message || "Bridge: Disconnected";
    }
  }

  if (doc.body) {
    doc.body.appendChild(badge);
  } else {
    win.addEventListener("DOMContentLoaded", () => doc.body.appendChild(badge));
  }

  function gmRequest(method, url, data) {
    return new Promise((resolve, reject) => {
      if (typeof GM_xmlhttpRequest !== "function") {
        return fetch(url, {
          method,
          headers: { "Content-Type": "application/json" },
          body: data ? JSON.stringify(data) : undefined,
        })
          .then((r) => r.json())
          .then(resolve)
          .catch(reject);
      }
      GM_xmlhttpRequest({
        method,
        url,
        data: data ? JSON.stringify(data) : undefined,
        headers: { "Content-Type": "application/json" },
        timeout: 35000,
        onload: (res) => {
          try {
            resolve(JSON.parse(res.responseText));
          } catch (_) {
            resolve({ raw: res.responseText });
          }
        },
        onerror: (err) => reject(err),
        ontimeout: () => reject(new Error("GM request timeout")),
      });
    });
  }

  function sendChunkToProxy(jobId, textDelta, reasoningDelta) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        id: jobId,
        type: "chunk",
        textDelta: textDelta || "",
        reasoningDelta: reasoningDelta || "",
      }));
      return;
    }
    gmRequest("POST", CHUNK_URL, {
      id: jobId,
      type: "chunk",
      textDelta: textDelta || "",
      reasoningDelta: reasoningDelta || "",
    }).catch(() => { });
  }

  function sendFinalToProxy(jobId, payload) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        id: jobId,
        type: "final",
        ...payload,
      }));
      return;
    }
    gmRequest("POST", REPLY_URL, {
      id: jobId,
      type: "final",
      ...payload,
    }).catch((err) => {
      log(`Failed to post final reply: ${err.message}`, "#ef4444");
    });
  }

  function concatUint8Arrays(a, b) {
    const c = new Uint8Array(a.length + b.length);
    c.set(a, 0);
    c.set(b, a.length);
    return c;
  }

  function decodeConnectFrame(data) {
    let bytes;
    if (data instanceof Uint8Array) {
      bytes = data;
    } else if (data instanceof ArrayBuffer) {
      bytes = new Uint8Array(data);
    } else if (typeof data === "string") {
      bytes = new Uint8Array(data.length);
      for (let i = 0; i < data.length; i++) {
        bytes[i] = data.charCodeAt(i) & 0xff;
      }
    } else {
      return null;
    }

    if (bytes.length < 5) return null;
    const flag = bytes[0];
    const length =
      ((bytes[1] << 24) >>> 0) +
      ((bytes[2] << 16) >>> 0) +
      ((bytes[3] << 8) >>> 0) +
      bytes[4];

    if (bytes.length < 5 + length) return null;

    const payloadBytes = bytes.slice(5, 5 + length);
    const jsonStr = new TextDecoder("utf-8").decode(payloadBytes);
    return { flag, length, obj: JSON.parse(jsonStr) };
  }

  function encodeConnectFrame(jsonObj) {
    const jsonStr = JSON.stringify(jsonObj);
    const textBytes = new TextEncoder().encode(jsonStr);
    const len = textBytes.length;
    const frame = new Uint8Array(5 + len);
    frame[0] = 0; // flag = 0
    frame[1] = (len >>> 24) & 0xff;
    frame[2] = (len >>> 16) & 0xff;
    frame[3] = (len >>> 8) & 0xff;
    frame[4] = len & 0xff;
    frame.set(textBytes, 5);
    return frame;
  }

  function extractKimiDeltas(obj) {
    let textDelta = "";
    let reasoningDelta = "";
    let isDone = false;

    if (!obj || typeof obj !== "object") {
      return { textDelta, reasoningDelta, isDone };
    }

    if (obj.done) {
      isDone = true;
    }

    const block = obj.block || (obj.event && obj.event.value);
    if (block && typeof block === "object") {
      const content = block.content;
      if (content && typeof content === "object") {
        if (content.case === "text" && content.value) {
          textDelta = content.value.content || "";
        } else if (content.case === "think" && content.value) {
          reasoningDelta = content.value.content || "";
        }

        if (!textDelta && content.text) {
          textDelta = typeof content.text === "string" ? content.text : (content.text.content || "");
        }
        if (!reasoningDelta && content.think) {
          reasoningDelta = typeof content.think === "string" ? content.think : (content.think.content || "");
        }
      }

      if (!textDelta && block.text) {
        textDelta = typeof block.text === "string" ? block.text : (block.text.content || "");
      }
      if (!reasoningDelta && block.think) {
        reasoningDelta = typeof block.think === "string" ? block.think : (block.think.content || "");
      }
    }

    return { textDelta, reasoningDelta, isDone };
  }

  async function processKimiConnectStream(stream, jobId) {
    log(`Reading Connect-RPC stream for job ${jobId}...`, "#38bdf8");
    const reader = stream.getReader();
    const decoder = new TextDecoder("utf-8");

    let buffer = new Uint8Array(0);
    let fullText = "";
    let fullReasoning = "";

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value && value.length > 0) {
          buffer = concatUint8Arrays(buffer, value);

          while (buffer.length >= 5) {
            const flag = buffer[0];
            const length =
              ((buffer[1] << 24) >>> 0) +
              ((buffer[2] << 16) >>> 0) +
              ((buffer[3] << 8) >>> 0) +
              buffer[4];

            if (buffer.length < 5 + length) {
              break;
            }

            const framePayload = buffer.slice(5, 5 + length);
            buffer = buffer.slice(5 + length);

            const isEndTrailer = (flag & 0x02) !== 0;
            if (isEndTrailer) {
              log("Reached Connect-RPC end trailer frame.", "#38bdf8");
              break;
            }

            try {
              const jsonStr = decoder.decode(framePayload);
              const obj = JSON.parse(jsonStr);
              const { textDelta, reasoningDelta, isDone } = extractKimiDeltas(obj);

              if (textDelta) {
                fullText += textDelta;
              }
              if (reasoningDelta) {
                fullReasoning += reasoningDelta;
              }

              if (textDelta || reasoningDelta) {
                sendChunkToProxy(jobId, textDelta, reasoningDelta);
              }

              if (isDone) {
                log("Received 'done' event from Kimi.", "#38bdf8");
                break;
              }
            } catch (_) { }
          }
        }
      }
    } catch (err) {
      log(`Stream read error: ${err.message}`, "#ef4444");
    } finally {
      log(
        `Stream finished. Total text: ${fullText.length} chars, reasoning: ${fullReasoning.length} chars`,
        "#10b981"
      );
      updateBadge("connected", "Bridge: Connected (Ready)");
      sendFinalToProxy(jobId, {
        text: fullText,
        reasoning: fullReasoning,
        done: true,
      });
      _pendingCapture = null;
    }
  }

  const origFetch = win.fetch;
  win.fetch = async function (resource, init) {
    const url = typeof resource === "string" ? resource : (resource && resource.url ? resource.url : "");

    if (url.includes("ChatService/Chat") || url.includes("kimi.gateway.chat.v1.ChatService/Chat")) {
      log("Intercepted Kimi ChatService/Chat request!", "#a855f7");

      if (_pendingCapture && !_pendingCapture.handled) {
        const capture = _pendingCapture;
        capture.handled = true;
        capture._intercepted = true;

        if (init && init.body) {
          try {
            const decoded = decodeConnectFrame(init.body);
            if (decoded && decoded.obj) {
              const reqObj = decoded.obj;

              if (reqObj.message && Array.isArray(reqObj.message.blocks) && reqObj.message.blocks.length > 0) {
                const block = reqObj.message.blocks[0];
                if (block.text) {
                  block.text.content = capture.prompt;
                } else if (block.content && block.content.value) {
                  block.content.value.content = capture.prompt;
                }
              }

              if (!reqObj.options) reqObj.options = {};
              if (capture.thinking === true) {
                reqObj.options.thinking = true;
                reqObj.options.reasoning_effort = "REASONING_EFFORT_HIGH";
              } else if (capture.thinking === false) {
                reqObj.options.thinking = false;
                reqObj.options.reasoning_effort = "REASONING_EFFORT_NONE";
              }

              if (capture.feature && capture.feature.chat_type === "search") {
                if (!Array.isArray(reqObj.tools)) reqObj.tools = [];
                if (!reqObj.tools.some((t) => t.type === "TOOL_TYPE_SEARCH")) {
                  reqObj.tools.push({ type: "TOOL_TYPE_SEARCH", search: { force: true } });
                }
              }

              const modifiedFrame = encodeConnectFrame(reqObj);
              init.body = modifiedFrame;
              log(
                `Injected prompt (${capture.prompt.length} chars) into Connect-RPC payload!`,
                "#10b981"
              );
            }
          } catch (err) {
            log(`Note: payload injection skipped: ${err.message}`, "#facc15");
          }
        }

        const response = await origFetch.apply(this, arguments);

        if (response.body && typeof response.body.tee === "function") {
          const [streamForPage, streamForBridge] = response.body.tee();
          processKimiConnectStream(streamForBridge, capture.id).catch((err) => {
            log(`Process stream error: ${err.message}`, "#ef4444");
          });

          return new win.Response(streamForPage, {
            status: response.status,
            statusText: response.statusText,
            headers: response.headers,
          });
        }
        return response;
      }
    }

    return origFetch.apply(this, arguments);
  };

  async function waitForIdle(maxWaitMs = 8000) {
    const start = Date.now();
    while (Date.now() - start < maxWaitMs) {
      const stopBtn = doc.querySelector(
        ".send-button-container.stop, [name='stop'], button[aria-label*='停止'], button[aria-label*='Stop']"
      );
      if (!stopBtn) return true;
      updateBadge("busy", "Bridge: Waiting for previous turn to finish...");
      await new Promise((r) => setTimeout(r, 120));
    }
    return true;
  }

  async function findChatInput(maxWaitMs = 8000) {
    const start = Date.now();
    const selectors = [
      ".chat-input-editor[data-lexical-editor='true']",
      "div.chat-input-editor",
      "div.editor[contenteditable='true']",
      "div[contenteditable='true'][role='textbox']",
      "div[contenteditable='true']",
      ".chat-input textarea",
      "textarea",
      "[placeholder*='Kimi']",
      "[placeholder*='想问']",
      "[placeholder*='输入']",
      "[placeholder*='Ask']",
    ];

    while (Date.now() - start < maxWaitMs) {
      for (const sel of selectors) {
        const el = doc.querySelector(sel);
        if (el && el.offsetParent !== null) {
          return el;
        }
      }
      await new Promise((r) => setTimeout(r, 200));
    }
    return null;
  }

  function trySetVueInputValue(inputEl, value) {
    let cur = inputEl;
    while (cur) {
      if (cur.__vueParentComponent__) {
        const comp = cur.__vueParentComponent__;
        if (comp.setupState) {
          if ("inputValue" in comp.setupState) {
            const iv = comp.setupState.inputValue;
            if (iv && typeof iv === "object" && "value" in iv) {
              iv.value = value;
            } else {
              comp.setupState.inputValue = value;
            }
            return true;
          }
        }
        if (comp.parent && comp.parent.setupState && "inputValue" in comp.parent.setupState) {
          const iv = comp.parent.setupState.inputValue;
          if (iv && typeof iv === "object" && "value" in iv) {
            iv.value = value;
          } else {
            comp.parent.setupState.inputValue = value;
          }
          return true;
        }
      }
      cur = cur.parentElement;
    }
    return false;
  }

  function tryTriggerVueSend(sendBtnEl, inputEl) {
    if (sendBtnEl && sendBtnEl.__vueParentComponent__) {
      const comp = sendBtnEl.__vueParentComponent__;
      if (comp.setupState && typeof comp.setupState.handleClick === "function") {
        comp.setupState.handleClick();
        return true;
      }
    }
    if (inputEl) {
      let cur = inputEl;
      while (cur) {
        const comp = cur.__vueParentComponent__;
        if (comp && comp.setupState && typeof comp.setupState.sendInputMessage === "function") {
          comp.setupState.sendInputMessage();
          return true;
        }
        cur = cur.parentElement;
      }
    }
    return false;
  }

  function setKimiInputValue(element, value) {
    element.focus();

    trySetVueInputValue(element, value);

    if (element.isContentEditable || element.hasAttribute("data-lexical-editor")) {
      try {
        const selection = win.getSelection();
        const range = doc.createRange();
        range.selectNodeContents(element);
        selection.removeAllRanges();
        selection.addRange(range);
        doc.execCommand("selectAll", false, null);
        doc.execCommand("insertText", false, value);
      } catch (_) { }

      try {
        element.dispatchEvent(
          new InputEvent("beforeinput", {
            bubbles: true,
            cancelable: true,
            inputType: "insertText",
            data: value,
          })
        );
        element.dispatchEvent(
          new InputEvent("input", {
            bubbles: true,
            cancelable: true,
            inputType: "insertText",
            data: value,
          })
        );
      } catch (_) { }

      if (!element.textContent.trim()) {
        element.innerHTML = "";
        const p = doc.createElement("p");
        p.setAttribute("dir", "ltr");
        const span = doc.createElement("span");
        span.setAttribute("data-lexical-text", "true");
        span.textContent = value;
        p.appendChild(span);
        element.appendChild(p);
      }

      element.dispatchEvent(new Event("input", { bubbles: true }));
      element.dispatchEvent(new Event("change", { bubbles: true }));
      return true;
    } else {
      const tracker = element._valueTracker;
      if (tracker) tracker.setValue("");
      const proto = Object.getPrototypeOf(element);
      const desc = Object.getOwnPropertyDescriptor(proto, "value");
      if (desc && desc.set) desc.set.call(element, value);
      else element.value = value;
      element.dispatchEvent(new Event("input", { bubbles: true }));
      element.dispatchEvent(new Event("change", { bubbles: true }));
      return true;
    }
  }

  async function submitPrompt(prompt, modelId, feature) {
    log(`Submitting prompt (${prompt.length} chars, model: ${modelId || "kimi-chat"})...`, "#facc15");
    updateBadge("busy", "Bridge: Generating stream...");

    await waitForIdle(8000);

    const input = await findChatInput(8000);
    if (!input) {
      throw new Error("Kimi chat input not found. Make sure https://www.kimi.ai/ is open.");
    }

    const triggerText = prompt.length > 200 ? prompt.slice(0, 80) : prompt;
    setKimiInputValue(input, triggerText);

    const MouseEventCls = win.MouseEvent || MouseEvent;
    const KeyboardEventCls = win.KeyboardEvent || KeyboardEvent;
    const startTime = Date.now();
    let clicked = false;

    while (Date.now() - startTime < 12000) {
      if (_pendingCapture && _pendingCapture._intercepted) {
        log("Generation successfully started (stream intercepted)!", "#10b981");
        return true;
      }

      const stopBtn = doc.querySelector(
        ".send-button-container.stop, [name='stop'], button[aria-label*='停止'], button[aria-label*='Stop']"
      );
      if (stopBtn) {
        log("Generation successfully started (stop button active)!", "#10b981");
        return true;
      }

      const curInput = await findChatInput(500);
      if (curInput) {
        const textInBox = curInput.textContent || "";
        if (!textInBox.trim() && clicked) {
          await new Promise((r) => setTimeout(r, 400));
          return true;
        }
        if (!textInBox.trim()) {
          setKimiInputValue(curInput, triggerText);
        }
      }

      const sendBtn = doc.querySelector(
        ".send-button-container, [name='Send'], [name='Send_b'], button[aria-label*='发送'], button[aria-label*='Send']"
      );

      if (sendBtn) {
        const isDisabled = sendBtn.classList.contains("disabled");

        if (!isDisabled) {
          sendBtn.focus();
          sendBtn.dispatchEvent(new MouseEventCls("mousedown", { bubbles: true, cancelable: true }));
          sendBtn.dispatchEvent(new MouseEventCls("mouseup", { bubbles: true, cancelable: true }));
          sendBtn.click();
          clicked = true;

          await new Promise((r) => setTimeout(r, 350));
          if (_pendingCapture && _pendingCapture._intercepted) return true;
        } else {
          tryTriggerVueSend(sendBtn, curInput);
        }
      }

      if (curInput) {
        curInput.dispatchEvent(new KeyboardEventCls("keydown", {
          key: "Enter",
          code: "Enter",
          keyCode: 13,
          which: 13,
          bubbles: true,
          cancelable: true,
        }));
        curInput.dispatchEvent(new KeyboardEventCls("keyup", {
          key: "Enter",
          code: "Enter",
          keyCode: 13,
          which: 13,
          bubbles: true,
          cancelable: true,
        }));
      }

      await new Promise((r) => setTimeout(r, 300));
    }

    return true;
  }

  win.deleteCurrentChat = async () => {
    log("Resetting Kimi chat session...", "#facc15");
    const newChatBtn = doc.querySelector(
      "a[href='/'], a[href='/chat'], .new-chat-text, [data-testid='new-chat'], [name='new-chat']"
    );
    if (newChatBtn) {
      newChatBtn.click();
      log("New chat started via button!", "#10b981");
      return;
    }
    win.location.href = "/";
  };

  async function handleIncomingJob(data) {
    if (data.action === "noop") return;

    if (data.action === "delete_chat") {
      log("Reset chat command received from client", "#facc15");
      updateBadge("busy", "Bridge: Resetting chat...");
      try {
        await win.deleteCurrentChat();
        updateBadge("connected", "Bridge: Connected (Ready)");
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ id: data.id, success: true }));
        } else if (usingHttpPoll) {
          sendFinalToProxy(data.id, { success: true });
        }
      } catch (err) {
        updateBadge("connected", "Bridge: Connected (Ready)");
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ id: data.id, error: err.message }));
        } else if (usingHttpPoll) {
          sendFinalToProxy(data.id, { error: err.message });
        }
      }
      return;
    }

    if (data.action === "prompt") {
      _pendingCapture = {
        id: data.id,
        prompt: data.prompt,
        thinking: data.thinking,
        model: data.model,
        feature: data.feature,
        handled: false,
        _intercepted: false,
      };

      try {
        await submitPrompt(data.prompt, data.model, data.feature);
      } catch (err) {
        log(`Failed to submit prompt: ${err.message}`, "#ef4444");
        updateBadge("connected", "Bridge: Connected (Ready)");
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ id: data.id, error: err.message }));
        } else if (usingHttpPoll) {
          sendFinalToProxy(data.id, { error: err.message });
        }
        _pendingCapture = null;
      }
    }
  }

  async function startHttpPollLoop() {
    if (isPollLoopRunning) return;
    isPollLoopRunning = true;
    usingHttpPoll = true;
    log("Starting HTTP Long-Poll loop (CSP-Safe)...", "#38bdf8");
    updateBadge("connected", "Bridge: Connected (HTTP Poll)");

    while (usingHttpPoll) {
      try {
        const data = await gmRequest("GET", POLL_URL);
        if (data && data.action) {
          await handleIncomingJob(data);
        }
      } catch (err) {
        updateBadge("disconnected", "Bridge: Disconnected (Retrying...)");
        await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS * 4));
      }
      await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
    }
    isPollLoopRunning = false;
  }

  function connectWebSocket() {
    log(`Connecting to proxy at ${WS_URL}...`, "#38bdf8");
    try {
      ws = new WebSocket(WS_URL);
    } catch (e) {
      log(`WebSocket error: ${e.message}. Falling back to HTTP polling.`, "#facc15");
      startHttpPollLoop();
      return;
    }

    ws.onopen = () => {
      isConnected = true;
      usingHttpPoll = false;
      log("WebSocket connected to proxy!", "#10b981");
      updateBadge("connected", "Bridge: Connected (Ready)");
    };

    ws.onmessage = async (event) => {
      try {
        const data = JSON.parse(event.data);
        await handleIncomingJob(data);
      } catch (err) {
        log(`Error processing message: ${err.message}`, "#ef4444");
      }
    };

    ws.onclose = () => {
      if (isConnected) {
        log("WebSocket closed.", "#ef4444");
      }
      isConnected = false;
      updateBadge("disconnected");
      startHttpPollLoop();
      setTimeout(connectWebSocket, RECONNECT_INTERVAL_MS);
    };

    ws.onerror = () => {
      log("WebSocket error, falling back to HTTP Long-Poll...", "#facc15");
      startHttpPollLoop();
    };
  }

  win.__kimiBridge = {
    get wsState() {
      if (!ws) return "NONE";
      return ["CONNECTING", "OPEN", "CLOSING", "CLOSED"][ws.readyState] || "UNKNOWN";
    },
    get isBusy() {
      const stopBtn = doc.querySelector(
        ".send-button-container.stop, [name='stop'], button[aria-label*='停止'], button[aria-label*='Stop']"
      );
      return Boolean(stopBtn);
    },
    reconnect: () => connectWebSocket(),
    reset: () => win.deleteCurrentChat(),
  };

  badge.addEventListener("click", () => {
    log("Manual reconnect triggered.", "#facc15");
    if (ws) {
      try { ws.close(); } catch (_) { }
    }
    connectWebSocket();
  });

  connectWebSocket();
  log("Kimi Web Bridge initialized.", "#10b981");
})();
