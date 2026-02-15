const DATA_ROOT = "../data/captures"
const MANIFEST_URL = `${DATA_ROOT}/manifest.json`

const SUPPORTED_SCHEMA_VERSION = "1.0.0"
const MANIFEST_MAX_BYTES = 256 * 1024
const DATASET_MAX_BYTES = 2 * 1024 * 1024
const REQUEST_TIMEOUT_MS = 5000
const TRANSFER_WALL_MS = 8000

const COMBO_IDS = [
  "binary-buffered",
  "binary-framed",
  "compact-buffered",
  "compact-framed"
]

const MESSAGE_TYPES = new Set(["call", "reply", "exception", "oneway"])
const ACTORS = new Set(["client", "server"])
const DIRECTIONS = new Set(["client->server", "server->client"])
const PROTOCOLS = new Set(["binary", "compact"])
const TRANSPORTS = new Set(["buffered", "framed"])

const DATASET_ERROR_CODES = new Set([
  "E_TUTORIAL_FLOW_MISMATCH",
  "E_MESSAGE_COUNT_MISMATCH"
])

const PARSE_ERROR_CODES = new Set([
  "E_FRAME_TRUNCATED",
  "E_FRAME_LENGTH_MISMATCH",
  "E_PROTOCOL_DECODE",
  "E_STRUCT_UNEXPECTED_STOP",
  "E_FIELD_TYPE_UNKNOWN",
  "E_SEQID_MISMATCH",
  "E_FIELD_NODE_LIMIT",
  "E_RECURSION_LIMIT",
  "E_STRING_TOO_LARGE",
  "E_HIGHLIGHT_LIMIT"
])

const state = {
  manifest: null,
  combo: null,
  dataset: null,
  messageIndex: 0,
  navWarning: null,
  blockingError: null,
  datasetCache: new Map()
}

const statusEl = document.querySelector("#status")
const comboPickerEl = document.querySelector("#combo-picker")
const messageNavEl = document.querySelector("#message-nav")
const summaryEl = document.querySelector("#message-summary")
const rawHexEl = document.querySelector("#raw-hex")
const fieldTreeEl = document.querySelector("#field-tree")
const parseErrorsEl = document.querySelector("#parse-errors")
const highlightsEl = document.querySelector("#highlights")

void bootstrap()

async function bootstrap() {
  try {
    state.manifest = await loadManifest()
    await applyNavigationFromHash()
    render()

    window.addEventListener("hashchange", () => {
      void applyNavigationFromHash().then(render).catch(handleRuntimeError)
    })
  } catch (error) {
    handleRuntimeError(error)
  }
}

async function applyNavigationFromHash() {
  const nav = resolveNavigation(state.manifest)
  state.combo = nav.combo
  state.messageIndex = nav.msg
  state.navWarning = nav.warning
  await loadCombo(state.combo)
}

async function loadCombo(comboId) {
  const entry = state.manifest.combos.find((combo) => combo.id === comboId)
  if (!entry) {
    throw runtimeFailure({
      code: "E_DATASET_INVALID",
      className: "blocking",
      reason: "schema_invalid",
      message: `Unknown combo id: ${comboId}`,
      combo: comboId,
      msg: state.messageIndex,
      httpStatus: null
    })
  }

  const cacheKey = `${entry.id}:${entry.bytes}:${entry.sha256}`
  let dataset

  if (state.datasetCache.has(cacheKey)) {
    dataset = state.datasetCache.get(cacheKey)
  } else {
    dataset = await loadDataset(entry)
    state.datasetCache.set(cacheKey, dataset)
  }

  state.dataset = dataset

  if (state.messageIndex >= state.dataset.messages.length) {
    state.messageIndex = 0
    if (!state.navWarning) {
      state.navWarning = runtimeFailure({
        code: "E_NAV_HASH_INVALID",
        className: "non-blocking",
        reason: "msg_invalid",
        message: "Message index is out of range; using message 0.",
        combo: state.combo,
        msg: 0,
        httpStatus: null
      }).toObject()
    }
  } else {
    state.messageIndex = clamp(state.messageIndex, 0, state.dataset.messages.length - 1)
  }

  state.blockingError = null
  writeHashIfNeeded(state.combo, state.messageIndex)
}

function render() {
  if (state.blockingError) {
    renderBlockingState()
    return
  }

  statusEl.textContent = state.navWarning
    ? `${state.navWarning.code}: ${state.navWarning.message}`
    : "Ready"
  statusEl.style.color = state.navWarning ? "#9f3418" : "#575f6d"

  renderComboPicker()
  renderMessageNav()
  renderMessageDetails()
}

function renderBlockingState() {
  const error = state.blockingError
  statusEl.textContent = `${error.code}: ${error.message}`
  statusEl.style.color = "#9f3418"

  renderComboPicker()
  messageNavEl.innerHTML = ""

  summaryEl.innerHTML = ""
  addSummary("State", "Blocking error")
  addSummary("Code", error.code)
  addSummary("Reason", error.context.reason)
  addSummary("Hint", "Fix the source issue and refresh the page.")

  rawHexEl.textContent = ""
  fieldTreeEl.innerHTML = ""
  const blocked = document.createElement("div")
  blocked.className = "field-node"
  blocked.textContent = "Detail view is unavailable while a blocking runtime check fails."
  fieldTreeEl.append(blocked)

  renderList(parseErrorsEl, [{ text: `${error.code}: ${error.message}` }], (item) => item.text, "warn")
  renderList(highlightsEl, [], () => "")
}

function renderComboPicker() {
  comboPickerEl.innerHTML = ""

  if (!state.manifest || !Array.isArray(state.manifest.combos)) {
    return
  }

  for (const combo of state.manifest.combos) {
    const btn = document.createElement("button")
    btn.type = "button"
    btn.textContent = `${combo.protocol} + ${combo.transport}`
    if (combo.id === state.combo) btn.classList.add("is-active")
    btn.addEventListener("click", () => updateSelection(combo.id, 0))
    comboPickerEl.append(btn)
  }
}

function renderMessageNav() {
  messageNavEl.innerHTML = ""

  if (!state.dataset || !Array.isArray(state.dataset.messages)) {
    return
  }

  state.dataset.messages.forEach((message, idx) => {
    const btn = document.createElement("button")
    btn.type = "button"
    btn.className = "message-item"
    if (idx === state.messageIndex) btn.classList.add("is-active")
    btn.textContent = `${message.index}. ${message.method} (${message.message_type})`
    btn.addEventListener("click", () => updateSelection(state.combo, idx))
    messageNavEl.append(btn)
  })
}

function renderMessageDetails() {
  if (!state.dataset || !state.dataset.messages || state.dataset.messages.length === 0) {
    return
  }

  const message = state.dataset.messages[state.messageIndex]
  const transport = message.transport || {}
  const envelope = message.envelope || {}

  summaryEl.innerHTML = ""
  addSummary("Actor", message.actor)
  addSummary("Direction", message.direction)
  addSummary("Method", message.method)
  addSummary("Type", message.message_type)
  addSummary("SeqID", String(message.seqid))
  addSummary("Raw Size", String(message.raw_size))
  addSummary("Protocol", state.dataset.combo.protocol)
  addSummary("Transport", `${transport.type}${transport.frame_length != null ? ` (${transport.frame_length})` : ""}`)
  addSummary("Envelope Span", spanToString(envelope.span))
  addSummary("Payload Span", spanToString(message.payload?.span))

  rawHexEl.textContent = message.raw_hex
  renderFieldTree(message.payload?.fields || [])
  renderList(parseErrorsEl, message.parse_errors || [], (item) => `${item.code}: ${item.message}`, "warn")
  renderList(highlightsEl, message.highlights || [], (item) => `${item.kind} ${item.label} ${item.start}-${item.end}`)
}

function addSummary(label, value) {
  const dt = document.createElement("dt")
  dt.textContent = label
  const dd = document.createElement("dd")
  dd.textContent = value ?? "-"
  summaryEl.append(dt, dd)
}

function renderFieldTree(fields) {
  fieldTreeEl.innerHTML = ""

  if (fields.length === 0) {
    const empty = document.createElement("div")
    empty.className = "field-node"
    empty.textContent = "No payload fields"
    fieldTreeEl.append(empty)
    return
  }

  const fragment = document.createDocumentFragment()
  for (const field of fields) {
    fragment.append(renderFieldNode(field, 0))
  }
  fieldTreeEl.append(fragment)
}

function renderFieldNode(field, depth) {
  const wrapper = document.createElement("div")
  wrapper.className = "field-node"
  wrapper.style.marginLeft = `${depth * 10}px`

  const line = document.createElement("div")
  const name = document.createElement("strong")
  name.textContent = String(field.name ?? "")
  const meta = document.createElement("span")
  meta.className = "meta"
  meta.textContent = ` ${field.ttype} id=${field.id} span=${spanToString(field.span)}`
  line.append(name, meta)
  wrapper.append(line)

  if (field.value !== null && field.value !== undefined && String(field.value) !== "") {
    const value = document.createElement("div")
    value.className = "meta"
    value.textContent = `value: ${String(field.value)}`
    wrapper.append(value)
  }

  for (const child of field.children || []) {
    wrapper.append(renderFieldNode(child, depth + 1))
  }

  return wrapper
}

function renderList(container, items, formatter, itemClass = "") {
  container.innerHTML = ""

  if (items.length === 0) {
    const li = document.createElement("li")
    li.textContent = "None"
    container.append(li)
    return
  }

  for (const item of items) {
    const li = document.createElement("li")
    li.textContent = formatter(item)
    if (itemClass) li.classList.add(itemClass)
    container.append(li)
  }
}

function updateSelection(combo, msg) {
  state.combo = combo
  state.messageIndex = msg
  state.navWarning = null
  writeHashIfNeeded(combo, msg)
  void loadCombo(combo).then(render).catch(handleRuntimeError)
}

function resolveNavigation(manifest) {
  const defaults = {
    combo: manifest.combos[0].id,
    msg: 0,
    warning: null
  }

  const raw = window.location.hash.startsWith("#") ? window.location.hash.slice(1) : ""
  if (raw.length === 0) return defaults

  if (raw.length > 128) {
    return {
      ...defaults,
      warning: runtimeFailure({
        code: "E_NAV_HASH_INVALID",
        className: "non-blocking",
        reason: "hash_too_long",
        message: "Hash is longer than 128 characters; using defaults.",
        combo: defaults.combo,
        msg: defaults.msg,
        httpStatus: null
      }).toObject()
    }
  }

  const firstValues = {}
  for (const piece of raw.split("&")) {
    if (!piece) continue

    const [rawKey, rawValue = ""] = piece.split("=", 2)

    let key
    let value
    try {
      key = decodeURIComponent(rawKey)
      value = decodeURIComponent(rawValue)
    } catch {
      return {
        ...defaults,
        warning: runtimeFailure({
          code: "E_NAV_HASH_INVALID",
          className: "non-blocking",
          reason: "hash_decode_failed",
          message: "Hash contains invalid percent-encoding; using defaults.",
          combo: defaults.combo,
          msg: defaults.msg,
          httpStatus: null
        }).toObject()
      }
    }

    if (!(key in firstValues)) {
      firstValues[key] = value
    }
  }

  const combo = firstValues.combo
  const msgRaw = firstValues.msg

  const resolvedCombo = combo === undefined ? defaults.combo : combo
  if (combo !== undefined && !COMBO_IDS.includes(combo)) {
    return {
      ...defaults,
      warning: runtimeFailure({
        code: "E_NAV_HASH_INVALID",
        className: "non-blocking",
        reason: "combo_invalid",
        message: "Hash combo value is invalid; using defaults.",
        combo: defaults.combo,
        msg: defaults.msg,
        httpStatus: null
      }).toObject()
    }
  }

  if (msgRaw === undefined) {
    return {
      combo: resolvedCombo,
      msg: 0,
      warning: null
    }
  }

  if (!/^\d+$/.test(msgRaw)) {
    return {
      combo: resolvedCombo,
      msg: 0,
      warning: runtimeFailure({
        code: "E_NAV_HASH_INVALID",
        className: "non-blocking",
        reason: "msg_invalid",
        message: "Hash message index is invalid; using message 0.",
        combo: resolvedCombo,
        msg: 0,
        httpStatus: null
      }).toObject()
    }
  }

  const msg = Number(msgRaw)
  if (!Number.isSafeInteger(msg) || msg < 0) {
    return {
      combo: resolvedCombo,
      msg: 0,
      warning: runtimeFailure({
        code: "E_NAV_HASH_INVALID",
        className: "non-blocking",
        reason: "msg_invalid",
        message: "Hash message index is invalid; using message 0.",
        combo: resolvedCombo,
        msg: 0,
        httpStatus: null
      }).toObject()
    }
  }

  return {
    combo: resolvedCombo,
    msg,
    warning: null
  }
}

function writeHashIfNeeded(combo, msg) {
  const next = `#combo=${encodeURIComponent(combo)}&msg=${encodeURIComponent(String(msg))}`
  if (window.location.hash !== next) {
    history.replaceState(null, "", next)
  }
}

async function loadManifest() {
  const payload = await fetchJsonResource({
    url: MANIFEST_URL,
    maxBytes: MANIFEST_MAX_BYTES,
    fetchCode: "E_MANIFEST_FETCH_FAILED",
    invalidCode: "E_MANIFEST_INVALID",
    tooLargeCode: "E_MANIFEST_TOO_LARGE",
    tooLargeReason: "manifest_too_large",
    combo: null,
    msg: null
  })

  const manifest = payload.json

  validateManifestMinimal(manifest)
  enforceSchemaVersionGate({
    value: manifest.schema_version,
    invalidCode: "E_MANIFEST_INVALID",
    combo: null,
    msg: null
  })
  validateManifestFull(manifest)

  return manifest
}

async function loadDataset(entry) {
  const payload = await fetchJsonResource({
    url: `${DATA_ROOT}/${entry.file}`,
    maxBytes: DATASET_MAX_BYTES,
    fetchCode: "E_DATASET_FETCH_FAILED",
    invalidCode: "E_DATASET_INVALID",
    tooLargeCode: "E_DATASET_TOO_LARGE",
    tooLargeReason: "dataset_too_large",
    combo: entry.id,
    msg: state.messageIndex
  })

  const dataset = payload.json

  validateDatasetMinimal(dataset, entry)
  enforceSchemaVersionGate({
    value: dataset.schema_version,
    invalidCode: "E_DATASET_INVALID",
    combo: entry.id,
    msg: state.messageIndex
  })

  if (payload.bytes.byteLength !== entry.bytes) {
    throw runtimeFailure({
      code: "E_DATASET_INVALID",
      className: "blocking",
      reason: "bytes_mismatch",
      message: `Dataset bytes mismatch: expected ${entry.bytes}, got ${payload.bytes.byteLength}.`,
      combo: entry.id,
      msg: state.messageIndex,
      httpStatus: payload.httpStatus
    })
  }

  await verifyDatasetHash(payload.bytes, entry)
  validateDatasetFull(dataset, entry)

  return dataset
}

async function fetchJsonResource({
  url,
  maxBytes,
  fetchCode,
  invalidCode,
  tooLargeCode,
  tooLargeReason,
  combo,
  msg
}) {
  const response = await fetchWithRevalidation({ url, fetchCode, combo, msg })

  const contentType = response.headers.get("content-type") || ""
  if (!contentType.toLowerCase().startsWith("application/json")) {
    throw runtimeFailure({
      code: invalidCode,
      className: "blocking",
      reason: "schema_invalid",
      message: `Unexpected content type: ${contentType || "(missing)"}.`,
      combo,
      msg,
      httpStatus: response.status
    })
  }

  const bytes = await readResponseBytes({
    response,
    maxBytes,
    tooLargeCode,
    tooLargeReason,
    fetchCode,
    combo,
    msg
  })

  let decoded
  try {
    decoded = new TextDecoder("utf-8", { fatal: true }).decode(bytes)
  } catch {
    throw runtimeFailure({
      code: invalidCode,
      className: "blocking",
      reason: "json_parse_failed",
      message: "Response body is not valid UTF-8 JSON.",
      combo,
      msg,
      httpStatus: response.status
    })
  }

  let json
  try {
    json = JSON.parse(decoded)
  } catch {
    throw runtimeFailure({
      code: invalidCode,
      className: "blocking",
      reason: "json_parse_failed",
      message: "Response body is not valid JSON.",
      combo,
      msg,
      httpStatus: response.status
    })
  }

  return {
    json,
    bytes,
    httpStatus: response.status
  }
}

async function fetchWithRevalidation({ url, fetchCode, combo, msg }) {
  const first = await fetchOnce({
    url,
    cacheMode: "no-cache",
    fetchCode,
    combo,
    msg
  })

  if (first.status !== 304) {
    return first
  }

  const second = await fetchOnce({
    url,
    cacheMode: "reload",
    fetchCode,
    combo,
    msg
  })

  if (second.status === 304) {
    throw runtimeFailure({
      code: fetchCode,
      className: "blocking",
      reason: "fetch_failed",
      message: "Resource revalidation returned 304 without a usable body.",
      combo,
      msg,
      httpStatus: 304
    })
  }

  return second
}

async function fetchOnce({ url, cacheMode, fetchCode, combo, msg }) {
  const controller = new AbortController()
  const timeoutId = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS)

  let response
  try {
    response = await fetch(url, {
      cache: cacheMode,
      redirect: "follow",
      signal: controller.signal
    })
  } catch {
    throw runtimeFailure({
      code: fetchCode,
      className: "blocking",
      reason: "fetch_failed",
      message: `Failed to fetch ${url}.`,
      combo,
      msg,
      httpStatus: null
    })
  } finally {
    window.clearTimeout(timeoutId)
  }

  if (response.redirected) {
    const finalOrigin = new URL(response.url, window.location.href).origin
    if (finalOrigin !== window.location.origin) {
      throw runtimeFailure({
        code: fetchCode,
        className: "blocking",
        reason: "fetch_failed",
        message: "Cross-origin redirect is not allowed for artifact loading.",
        combo,
        msg,
        httpStatus: response.status
      })
    }
  }

  if (response.status === 304) {
    return response
  }

  if (!response.ok) {
    throw runtimeFailure({
      code: fetchCode,
      className: "blocking",
      reason: "fetch_failed",
      message: `HTTP ${response.status} for ${url}.`,
      combo,
      msg,
      httpStatus: response.status
    })
  }

  return response
}

async function readResponseBytes({
  response,
  maxBytes,
  tooLargeCode,
  tooLargeReason,
  fetchCode,
  combo,
  msg
}) {
  const contentLength = parseContentLength(response.headers.get("content-length"))
  if (contentLength !== null && contentLength > maxBytes) {
    throw runtimeFailure({
      code: tooLargeCode,
      className: "blocking",
      reason: tooLargeReason,
      message: `Resource exceeds size limit of ${maxBytes} bytes.`,
      combo,
      msg,
      httpStatus: response.status
    })
  }

  const startedAt = performance.now()

  if (response.body && typeof response.body.getReader === "function") {
    const reader = response.body.getReader()
    const chunks = []
    let total = 0

    while (true) {
      if (performance.now() - startedAt > TRANSFER_WALL_MS) {
        await reader.cancel()
        throw runtimeFailure({
          code: fetchCode,
          className: "blocking",
          reason: "fetch_failed",
          message: "Transfer exceeded wall-clock limit.",
          combo,
          msg,
          httpStatus: response.status
        })
      }

      const readResult = await withTimeout(reader.read(), REQUEST_TIMEOUT_MS)
      if (readResult === null) {
        await reader.cancel()
        throw runtimeFailure({
          code: fetchCode,
          className: "blocking",
          reason: "fetch_failed",
          message: "Transfer timed out while reading response bytes.",
          combo,
          msg,
          httpStatus: response.status
        })
      }

      if (readResult.done) break

      const chunk = readResult.value
      total += chunk.byteLength
      if (total > maxBytes) {
        await reader.cancel()
        throw runtimeFailure({
          code: tooLargeCode,
          className: "blocking",
          reason: tooLargeReason,
          message: `Resource exceeds size limit of ${maxBytes} bytes.`,
          combo,
          msg,
          httpStatus: response.status
        })
      }

      chunks.push(chunk)
    }

    const bytes = concatChunks(chunks, total)
    if (bytes.byteLength > maxBytes) {
      throw runtimeFailure({
        code: tooLargeCode,
        className: "blocking",
        reason: tooLargeReason,
        message: `Resource exceeds size limit of ${maxBytes} bytes.`,
        combo,
        msg,
        httpStatus: response.status
      })
    }

    return bytes
  }

  const fallback = await withTimeout(response.arrayBuffer(), REQUEST_TIMEOUT_MS)
  if (fallback === null || performance.now() - startedAt > TRANSFER_WALL_MS) {
    throw runtimeFailure({
      code: fetchCode,
      className: "blocking",
      reason: "fetch_failed",
      message: "Transfer timed out while reading response bytes.",
      combo,
      msg,
      httpStatus: response.status
    })
  }

  const bytes = new Uint8Array(fallback)
  if (bytes.byteLength > maxBytes) {
    throw runtimeFailure({
      code: tooLargeCode,
      className: "blocking",
      reason: tooLargeReason,
      message: `Resource exceeds size limit of ${maxBytes} bytes.`,
      combo,
      msg,
      httpStatus: response.status
    })
  }

  return bytes
}

async function verifyDatasetHash(bytes, entry) {
  if (!globalThis.crypto || !globalThis.crypto.subtle) {
    throw runtimeFailure({
      code: "E_WEBCRYPTO_UNAVAILABLE",
      className: "blocking",
      reason: "webcrypto_unavailable",
      message: "WebCrypto API is unavailable in this browser.",
      combo: entry.id,
      msg: state.messageIndex,
      httpStatus: null
    })
  }

  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes)
  const actual = toHex(new Uint8Array(digest))
  if (actual !== entry.sha256) {
    throw runtimeFailure({
      code: "E_DATASET_HASH_MISMATCH",
      className: "blocking",
      reason: "hash_mismatch",
      message: "Dataset SHA-256 does not match manifest hash.",
      combo: entry.id,
      msg: state.messageIndex,
      httpStatus: null
    })
  }
}

function validateManifestMinimal(manifest) {
  if (!isObject(manifest)) {
    throw runtimeFailure({
      code: "E_MANIFEST_INVALID",
      className: "blocking",
      reason: "schema_invalid",
      message: "Manifest must be a JSON object.",
      combo: null,
      msg: null,
      httpStatus: null
    })
  }

  if (!("schema_version" in manifest) || !("release_id" in manifest) || !("generated_at_utc" in manifest)) {
    throw runtimeFailure({
      code: "E_MANIFEST_INVALID",
      className: "blocking",
      reason: "schema_invalid",
      message: "Manifest is missing required top-level fields.",
      combo: null,
      msg: null,
      httpStatus: null
    })
  }

  if (!Array.isArray(manifest.combos) || manifest.combos.length === 0) {
    throw runtimeFailure({
      code: "E_MANIFEST_INVALID",
      className: "blocking",
      reason: "schema_invalid",
      message: "Manifest combos must be a non-empty array.",
      combo: null,
      msg: null,
      httpStatus: null
    })
  }

  for (const combo of manifest.combos) {
    if (!isObject(combo) || !("file" in combo) || !("sha256" in combo) || !("bytes" in combo)) {
      throw runtimeFailure({
        code: "E_MANIFEST_INVALID",
        className: "blocking",
        reason: "schema_invalid",
        message: "Manifest combo entries must include file, sha256, and bytes.",
        combo: null,
        msg: null,
        httpStatus: null
      })
    }
  }
}

function validateManifestFull(manifest) {
  if (manifest.schema_version !== SUPPORTED_SCHEMA_VERSION) {
    return
  }

  if (typeof manifest.release_id !== "string" || !/^[a-f0-9]{40}$/.test(manifest.release_id)) {
    throw invalidManifest("release_id must match 40-char lowercase hex git SHA.")
  }

  if (typeof manifest.generated_at_utc !== "string" || !isValidUtcDate(manifest.generated_at_utc)) {
    throw invalidManifest("generated_at_utc must be a valid RFC3339 UTC timestamp.")
  }

  if (!isObject(manifest.generated_with)) {
    throw invalidManifest("generated_with must be an object.")
  }

  const generatedWith = manifest.generated_with
  const generatedFields = ["ruby_version", "bundler_version", "thrift_ref", "platform"]
  for (const field of generatedFields) {
    if (typeof generatedWith[field] !== "string" || generatedWith[field].length === 0) {
      throw invalidManifest(`generated_with.${field} must be a non-empty string.`)
    }
  }

  if (!/^(git:[a-f0-9]{40}|gem:[0-9A-Za-z._+-]+)$/.test(generatedWith.thrift_ref)) {
    throw invalidManifest("generated_with.thrift_ref is invalid.")
  }

  if (manifest.combos.length < 1 || manifest.combos.length > 4) {
    throw invalidManifest("combos length must be between 1 and 4.")
  }

  const seenIds = new Set()
  const seenFiles = new Set()

  for (const combo of manifest.combos) {
    if (!isObject(combo)) {
      throw invalidManifest("combos entries must be objects.")
    }

    if (!COMBO_IDS.includes(combo.id)) {
      throw invalidManifest("combos[].id is invalid.")
    }

    if (seenIds.has(combo.id)) {
      throw invalidManifest("combos[].id must be unique.")
    }
    seenIds.add(combo.id)

    if (!PROTOCOLS.has(combo.protocol) || !TRANSPORTS.has(combo.transport)) {
      throw invalidManifest("combos[].protocol or combos[].transport is invalid.")
    }

    const expectedFile = `${combo.id}.json`
    if (combo.file !== expectedFile) {
      throw invalidManifest("combos[].file must match <id>.json.")
    }

    if (seenFiles.has(combo.file)) {
      throw invalidManifest("combos[].file must be unique.")
    }
    seenFiles.add(combo.file)

    if (typeof combo.sha256 !== "string" || !/^[a-f0-9]{64}$/.test(combo.sha256)) {
      throw invalidManifest("combos[].sha256 must be 64-char lowercase hex.")
    }

    if (!Number.isInteger(combo.bytes) || combo.bytes < 1 || combo.bytes > DATASET_MAX_BYTES) {
      throw invalidManifest("combos[].bytes is out of allowed range.")
    }

    if (!Number.isInteger(combo.message_count) || combo.message_count < 1 || combo.message_count > 200) {
      throw invalidManifest("combos[].message_count is out of allowed range.")
    }
  }
}

function validateDatasetMinimal(dataset, entry) {
  if (!isObject(dataset)) {
    throw invalidDataset(entry.id, "Dataset must be a JSON object.")
  }

  const required = ["schema_version", "combo", "metadata", "dataset_errors", "messages"]
  for (const field of required) {
    if (!(field in dataset)) {
      throw invalidDataset(entry.id, `Dataset missing required field: ${field}.`)
    }
  }

  if (!isObject(dataset.combo) || !isObject(dataset.metadata)) {
    throw invalidDataset(entry.id, "Dataset combo and metadata must be objects.")
  }

  if (!Array.isArray(dataset.dataset_errors) || !Array.isArray(dataset.messages)) {
    throw invalidDataset(entry.id, "dataset_errors and messages must be arrays.")
  }
}

function validateDatasetFull(dataset, entry) {
  if (!COMBO_IDS.includes(dataset.combo.id)) {
    throw invalidDataset(entry.id, "dataset.combo.id is invalid.")
  }

  if (!PROTOCOLS.has(dataset.combo.protocol) || !TRANSPORTS.has(dataset.combo.transport)) {
    throw invalidDataset(entry.id, "dataset combo protocol/transport is invalid.")
  }

  if (dataset.combo.id !== entry.id) {
    throw invalidDataset(entry.id, "dataset.combo.id does not match manifest entry.")
  }

  if (dataset.combo.protocol !== entry.protocol || dataset.combo.transport !== entry.transport) {
    throw invalidDataset(entry.id, "dataset combo protocol/transport does not match manifest entry.")
  }

  const metadata = dataset.metadata

  if (!Number.isInteger(metadata.message_count) || metadata.message_count < 1 || metadata.message_count > 200) {
    throw invalidDataset(entry.id, "metadata.message_count is out of bounds.")
  }

  if (!Number.isInteger(metadata.total_field_nodes) || metadata.total_field_nodes < 0 || metadata.total_field_nodes > 50000) {
    throw invalidDataset(entry.id, "metadata.total_field_nodes is out of bounds.")
  }

  if (!Number.isInteger(metadata.max_highlights_per_message) || metadata.max_highlights_per_message < 0 || metadata.max_highlights_per_message > 1000) {
    throw invalidDataset(entry.id, "metadata.max_highlights_per_message is out of bounds.")
  }

  if (!Number.isInteger(metadata.max_string_value_bytes) || metadata.max_string_value_bytes < 0 || metadata.max_string_value_bytes > 65536) {
    throw invalidDataset(entry.id, "metadata.max_string_value_bytes is out of bounds.")
  }

  if (dataset.messages.length !== metadata.message_count) {
    throw invalidDataset(entry.id, "messages length must match metadata.message_count.")
  }

  if (entry.message_count !== metadata.message_count) {
    throw invalidDataset(entry.id, "manifest message_count does not match dataset metadata.message_count.")
  }

  if (dataset.dataset_errors.length > 50) {
    throw invalidDataset(entry.id, "dataset_errors exceeds maximum length.")
  }

  for (const item of dataset.dataset_errors) {
    if (!isObject(item) || !DATASET_ERROR_CODES.has(item.code)) {
      throw invalidDataset(entry.id, "dataset_errors contains an invalid error code.")
    }
  }

  let expectedIndex = 0
  for (const message of dataset.messages) {
    if (!isObject(message)) {
      throw invalidDataset(entry.id, "messages entries must be objects.")
    }

    if (!Number.isInteger(message.index) || message.index !== expectedIndex) {
      throw invalidDataset(entry.id, "messages indexes must be contiguous from 0.")
    }
    expectedIndex += 1

    if (!ACTORS.has(message.actor) || !DIRECTIONS.has(message.direction)) {
      throw invalidDataset(entry.id, "message actor/direction is invalid.")
    }

    if (!MESSAGE_TYPES.has(message.message_type)) {
      throw invalidDataset(entry.id, "message_type is invalid.")
    }

    if (typeof message.raw_hex !== "string" || !/^[0-9a-f]{2}( [0-9a-f]{2})*$/.test(message.raw_hex)) {
      throw invalidDataset(entry.id, "raw_hex format is invalid.")
    }

    if (!Number.isInteger(message.raw_size) || message.raw_size < 1) {
      throw invalidDataset(entry.id, "raw_size must be a positive integer.")
    }

    if (rawHexByteCount(message.raw_hex) !== message.raw_size) {
      throw invalidDataset(entry.id, "raw_hex byte count does not match raw_size.")
    }

    if (!Array.isArray(message.parse_errors) || message.parse_errors.length > 100) {
      throw invalidDataset(entry.id, "parse_errors length exceeds maximum.")
    }

    for (const parseError of message.parse_errors) {
      if (!isObject(parseError) || !PARSE_ERROR_CODES.has(parseError.code)) {
        throw invalidDataset(entry.id, "parse_errors contains an invalid code.")
      }
    }

    if (!Array.isArray(message.highlights) || message.highlights.length > 1000) {
      throw invalidDataset(entry.id, "highlights length exceeds maximum.")
    }
  }
}

function enforceSchemaVersionGate({ value, invalidCode, combo, msg }) {
  if (typeof value !== "string") {
    throw runtimeFailure({
      code: invalidCode,
      className: "blocking",
      reason: "schema_invalid",
      message: "schema_version is missing or not a string.",
      combo,
      msg,
      httpStatus: null
    })
  }

  if (value !== SUPPORTED_SCHEMA_VERSION) {
    throw runtimeFailure({
      code: "E_SCHEMA_VERSION_UNSUPPORTED",
      className: "blocking",
      reason: "schema_version_unsupported",
      message: `Unsupported schema_version: ${value}.`,
      combo,
      msg,
      httpStatus: null
    })
  }
}

function handleRuntimeError(error) {
  const runtime = normalizeRuntimeError(error)
  state.blockingError = runtime

  if (runtime.class === "blocking") {
    console.error(runtime)
  } else {
    state.navWarning = runtime
    console.warn(runtime)
  }

  render()
}

function normalizeRuntimeError(error) {
  if (error instanceof RuntimeFailure) {
    return error.toObject()
  }

  return runtimeFailure({
    code: state.manifest ? "E_DATASET_INVALID" : "E_MANIFEST_INVALID",
    className: "blocking",
    reason: "schema_invalid",
    message: error instanceof Error ? error.message : "Unexpected runtime failure.",
    combo: state.combo ?? null,
    msg: Number.isInteger(state.messageIndex) ? state.messageIndex : null,
    httpStatus: null
  }).toObject()
}

function invalidManifest(message) {
  return runtimeFailure({
    code: "E_MANIFEST_INVALID",
    className: "blocking",
    reason: "schema_invalid",
    message,
    combo: null,
    msg: null,
    httpStatus: null
  })
}

function invalidDataset(combo, message) {
  return runtimeFailure({
    code: "E_DATASET_INVALID",
    className: "blocking",
    reason: "schema_invalid",
    message,
    combo,
    msg: state.messageIndex,
    httpStatus: null
  })
}

class RuntimeFailure extends Error {
  constructor({ code, className, reason, message, combo, msg, httpStatus }) {
    super(message)
    this.name = "RuntimeFailure"
    this.code = code
    this.className = className
    this.reason = reason
    this.combo = combo ?? null
    this.msg = Number.isInteger(msg) ? msg : null
    this.httpStatus = Number.isInteger(httpStatus) ? httpStatus : null
  }

  toObject() {
    return {
      code: this.code,
      class: this.className,
      message: this.message,
      context: {
        combo: this.combo,
        msg: this.msg,
        reason: this.reason,
        http_status: this.httpStatus
      },
      ts: new Date().toISOString()
    }
  }
}

function runtimeFailure({ code, className, reason, message, combo, msg, httpStatus }) {
  return new RuntimeFailure({
    code,
    className,
    reason,
    message,
    combo,
    msg,
    httpStatus
  })
}

function concatChunks(chunks, totalBytes) {
  const out = new Uint8Array(totalBytes)
  let offset = 0
  for (const chunk of chunks) {
    out.set(chunk, offset)
    offset += chunk.byteLength
  }
  return out
}

async function withTimeout(promise, timeoutMs) {
  let timeoutId = null
  const timeout = new Promise((resolve) => {
    timeoutId = window.setTimeout(() => resolve(null), timeoutMs)
  })

  const result = await Promise.race([promise, timeout])
  if (timeoutId !== null) {
    window.clearTimeout(timeoutId)
  }
  return result
}

function parseContentLength(headerValue) {
  if (typeof headerValue !== "string" || headerValue.length === 0) {
    return null
  }

  const parsed = Number.parseInt(headerValue, 10)
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null
}

function rawHexByteCount(rawHex) {
  return rawHex.split(" ").length
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value)
}

function isValidUtcDate(value) {
  return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/.test(value) && !Number.isNaN(Date.parse(value))
}

function toHex(bytes) {
  let output = ""
  for (const byte of bytes) {
    output += byte.toString(16).padStart(2, "0")
  }
  return output
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max)
}

function spanToString(span) {
  if (!Array.isArray(span) || span.length !== 2) return "-"
  return `[${span[0]}, ${span[1]})`
}
