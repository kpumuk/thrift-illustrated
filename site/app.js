const DATA_ROOT = "../data/captures";

const state = {
  manifest: null,
  combo: null,
  dataset: null,
  messageIndex: 0,
  navWarning: null
};

const statusEl = document.querySelector("#status");
const comboPickerEl = document.querySelector("#combo-picker");
const messageNavEl = document.querySelector("#message-nav");
const summaryEl = document.querySelector("#message-summary");
const rawHexEl = document.querySelector("#raw-hex");
const fieldTreeEl = document.querySelector("#field-tree");
const parseErrorsEl = document.querySelector("#parse-errors");
const highlightsEl = document.querySelector("#highlights");

bootstrap().catch((error) => {
  statusEl.textContent = `Fatal: ${error.message}`;
  statusEl.style.color = "#9f3418";
  console.error(error);
});

async function bootstrap() {
  state.manifest = await fetchJson(`${DATA_ROOT}/manifest.json`);
  ensureManifest(state.manifest);

  const nav = resolveNavigation(state.manifest);
  state.combo = nav.combo;
  state.messageIndex = nav.msg;
  state.navWarning = nav.warning;

  await loadCombo(state.combo);
  render();

  window.addEventListener("hashchange", () => {
    const next = resolveNavigation(state.manifest);
    state.combo = next.combo;
    state.messageIndex = next.msg;
    state.navWarning = next.warning;
    loadCombo(state.combo).then(render).catch(handleRuntimeError);
  });
}

async function loadCombo(comboId) {
  const entry = state.manifest.combos.find((combo) => combo.id === comboId);
  if (!entry) {
    throw new Error(`Unknown combo: ${comboId}`);
  }

  state.dataset = await fetchJson(`${DATA_ROOT}/${entry.file}`);
  if (!state.dataset.messages || state.dataset.messages.length === 0) {
    throw new Error(`Dataset has no messages: ${comboId}`);
  }

  if (state.messageIndex >= state.dataset.messages.length) {
    state.messageIndex = 0;
    state.navWarning = "E_NAV_HASH_INVALID: invalid msg";
  } else {
    state.messageIndex = clamp(state.messageIndex, 0, state.dataset.messages.length - 1);
  }
  writeHashIfNeeded(state.combo, state.messageIndex);
}

function render() {
  statusEl.textContent = state.navWarning || "Ready";
  statusEl.style.color = state.navWarning ? "#9f3418" : "#575f6d";

  renderComboPicker();
  renderMessageNav();
  renderMessageDetails();
}

function renderComboPicker() {
  comboPickerEl.innerHTML = "";

  for (const combo of state.manifest.combos) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = `${combo.protocol} + ${combo.transport}`;
    if (combo.id === state.combo) btn.classList.add("is-active");
    btn.addEventListener("click", () => updateSelection(combo.id, 0));
    comboPickerEl.append(btn);
  }
}

function renderMessageNav() {
  messageNavEl.innerHTML = "";

  state.dataset.messages.forEach((message, idx) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "message-item";
    if (idx === state.messageIndex) btn.classList.add("is-active");
    btn.textContent = `${message.index}. ${message.method} (${message.message_type})`;
    btn.addEventListener("click", () => updateSelection(state.combo, idx));
    messageNavEl.append(btn);
  });
}

function renderMessageDetails() {
  const message = state.dataset.messages[state.messageIndex];
  const transport = message.transport || {};
  const envelope = message.envelope || {};

  summaryEl.innerHTML = "";
  addSummary("Actor", message.actor);
  addSummary("Direction", message.direction);
  addSummary("Method", message.method);
  addSummary("Type", message.message_type);
  addSummary("SeqID", String(message.seqid));
  addSummary("Raw Size", String(message.raw_size));
  addSummary("Protocol", state.dataset.combo.protocol);
  addSummary("Transport", `${transport.type}${transport.frame_length != null ? ` (${transport.frame_length})` : ""}`);
  addSummary("Envelope Span", spanToString(envelope.span));
  addSummary("Payload Span", spanToString(message.payload?.span));

  rawHexEl.textContent = message.raw_hex;
  renderFieldTree(message.payload?.fields || []);
  renderList(parseErrorsEl, message.parse_errors || [], (item) => `${item.code}: ${item.message}`, "warn");
  renderList(highlightsEl, message.highlights || [], (item) => `${item.kind} ${item.label} ${item.start}-${item.end}`);
}

function addSummary(label, value) {
  const dt = document.createElement("dt");
  dt.textContent = label;
  const dd = document.createElement("dd");
  dd.textContent = value ?? "-";
  summaryEl.append(dt, dd);
}

function renderFieldTree(fields) {
  fieldTreeEl.innerHTML = "";

  if (fields.length === 0) {
    const empty = document.createElement("div");
    empty.className = "field-node";
    empty.textContent = "No payload fields";
    fieldTreeEl.append(empty);
    return;
  }

  const fragment = document.createDocumentFragment();
  for (const field of fields) {
    fragment.append(renderFieldNode(field, 0));
  }
  fieldTreeEl.append(fragment);
}

function renderFieldNode(field, depth) {
  const wrapper = document.createElement("div");
  wrapper.className = "field-node";
  wrapper.style.marginLeft = `${depth * 10}px`;

  const line = document.createElement("div");
  const name = document.createElement("strong");
  name.textContent = String(field.name ?? "");
  const meta = document.createElement("span");
  meta.className = "meta";
  meta.textContent = ` ${field.ttype} id=${field.id} span=${spanToString(field.span)}`;
  line.append(name, meta);
  wrapper.append(line);

  if (field.value !== null && field.value !== undefined && String(field.value) !== "") {
    const value = document.createElement("div");
    value.className = "meta";
    value.textContent = `value: ${String(field.value)}`;
    wrapper.append(value);
  }

  for (const child of field.children || []) {
    wrapper.append(renderFieldNode(child, depth + 1));
  }

  return wrapper;
}

function renderList(container, items, formatter, itemClass = "") {
  container.innerHTML = "";

  if (items.length === 0) {
    const li = document.createElement("li");
    li.textContent = "None";
    container.append(li);
    return;
  }

  for (const item of items) {
    const li = document.createElement("li");
    li.textContent = formatter(item);
    if (itemClass) li.classList.add(itemClass);
    container.append(li);
  }
}

function updateSelection(combo, msg) {
  state.combo = combo;
  state.messageIndex = msg;
  state.navWarning = null;
  writeHashIfNeeded(combo, msg);
  loadCombo(combo).then(render).catch(handleRuntimeError);
}

function resolveNavigation(manifest) {
  const defaults = {
    combo: manifest.combos[0].id,
    msg: 0,
    warning: null
  };

  const raw = window.location.hash.startsWith("#") ? window.location.hash.slice(1) : "";
  if (raw.length === 0) return defaults;
  if (raw.length > 128) {
    return { ...defaults, warning: "E_NAV_HASH_INVALID: hash length exceeds 128" };
  }

  const firstValues = {};
  for (const piece of raw.split("&")) {
    if (!piece) continue;
    const [rawKey, rawValue = ""] = piece.split("=", 2);

    let key;
    let value;
    try {
      key = decodeURIComponent(rawKey);
      value = decodeURIComponent(rawValue);
    } catch {
      return { ...defaults, warning: "E_NAV_HASH_INVALID: malformed encoding" };
    }

    if (!(key in firstValues)) {
      firstValues[key] = value;
    }
  }

  const combo = firstValues.combo;
  const msgRaw = firstValues.msg;
  const comboInManifest = (value) => manifest.combos.some((entry) => entry.id === value);
  if (combo !== undefined && !comboInManifest(combo)) {
    return { ...defaults, warning: "E_NAV_HASH_INVALID: invalid combo" };
  }

  const resolvedCombo = combo === undefined ? defaults.combo : combo;

  if (msgRaw === undefined) {
    return {
      combo: resolvedCombo,
      msg: 0,
      warning: null
    };
  }

  if (!/^\d+$/.test(msgRaw)) {
    return { combo: resolvedCombo, msg: 0, warning: "E_NAV_HASH_INVALID: invalid msg" };
  }

  const msg = Number(msgRaw);
  if (!Number.isSafeInteger(msg) || msg < 0) {
    return { combo: resolvedCombo, msg: 0, warning: "E_NAV_HASH_INVALID: invalid msg" };
  }

  return {
    combo: resolvedCombo,
    msg,
    warning: null
  };
}

function writeHashIfNeeded(combo, msg) {
  const next = `#combo=${encodeURIComponent(combo)}&msg=${encodeURIComponent(String(msg))}`;
  if (window.location.hash !== next) {
    history.replaceState(null, "", next);
  }
}

function ensureManifest(manifest) {
  if (manifest.schema_version !== "1.0.0") {
    throw new Error(`Unsupported schema_version: ${manifest.schema_version}`);
  }
  if (!Array.isArray(manifest.combos) || manifest.combos.length === 0) {
    throw new Error("Manifest has no combos");
  }
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-cache" });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${url}`);
  }

  return response.json();
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function spanToString(span) {
  if (!Array.isArray(span) || span.length !== 2) return "-";
  return `[${span[0]}, ${span[1]})`;
}

function handleRuntimeError(error) {
  statusEl.textContent = `Runtime error: ${error.message}`;
  statusEl.style.color = "#9f3418";
  console.error(error);
}
