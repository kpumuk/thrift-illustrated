const DATA_ROOT = "./data/captures"
const MANIFEST_URL = `${DATA_ROOT}/manifest.json`

const SUPPORTED_SCHEMA_VERSION = "1.0.0"
const MANIFEST_MAX_BYTES = 256 * 1024
const DATASET_MAX_BYTES = 2 * 1024 * 1024
const REQUEST_TIMEOUT_MS = 5000
const TRANSFER_WALL_MS = 8000
const DEFAULT_HEXDUMP_ROW_BYTES = 8
const HEXDUMP_ROW_BYTES_CSS_VAR = "--hexdump-row-bytes"
const MIN_HEXDUMP_ROW_BYTES = 1
const MAX_HEXDUMP_ROW_BYTES = 32

const COMBO_IDS = [
  "binary-buffered",
  "binary-framed",
  "compact-buffered",
  "compact-framed",
  "json-buffered",
  "json-framed",
  "header-header"
]

const MESSAGE_TYPES = new Set(["call", "reply", "exception", "oneway"])
const ACTORS = new Set(["client", "server"])
const DIRECTIONS = new Set(["client->server", "server->client"])
const PROTOCOLS = new Set(["binary", "compact", "json", "header"])
const TRANSPORTS = new Set(["buffered", "framed", "header"])

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
  datasetCache: new Map(),
  lastErrorKey: null,
  lastErrorCycle: -1,
  errorCycle: 0,
  interaction: null,
  renderCycle: 0
}

const runtimeMessageEl = document.querySelector("#runtime-message")
const comboPickerEl = document.querySelector("#combo-picker")
const messageNavEl = document.querySelector("#message-nav")
const summaryCardEl = document.querySelector("#summary-card")
const summaryEl = document.querySelector("#message-summary")
const rawHexEl = document.querySelector("#raw-hex")
const byteExplainerEl = document.querySelector("#byte-explainer")
const fieldTreeEl = document.querySelector("#field-tree")
const parseErrorsEl = document.querySelector("#parse-errors")
const highlightsEl = document.querySelector("#highlights")
let responsiveRenderFrame = 0

initObservability()
void bootstrap()

async function bootstrap() {
  try {
    beginErrorCycle()
    const manifestStartedAt = performance.now()
    state.manifest = await loadManifest()
    recordMetric("manifest_load_ms", roundMetric(performance.now() - manifestStartedAt))
    await applyNavigationFromHash()
    render()

    window.addEventListener("hashchange", () => {
      beginErrorCycle()
      void applyNavigationFromHash().then(render).catch(handleRuntimeError)
    })
    window.addEventListener("resize", scheduleResponsiveRerender)
    rawHexEl.addEventListener("pointermove", handleHexPointerMove)
    rawHexEl.addEventListener("pointerleave", clearInteraction)
  } catch (error) {
    handleRuntimeError(error)
  }
}

function scheduleResponsiveRerender() {
  if (!state.dataset || state.blockingError) return
  if (responsiveRenderFrame !== 0) return
  responsiveRenderFrame = window.requestAnimationFrame(() => {
    responsiveRenderFrame = 0
    if (!state.dataset || state.blockingError || !state.interaction) return
    const nextRowBytes = resolveHexdumpRowBytes()
    if (nextRowBytes !== state.interaction.hexdumpRowBytes) {
      renderMessageDetails()
    }
  })
}

async function applyNavigationFromHash() {
  const nav = resolveNavigation(state.manifest)
  state.combo = nav.combo
  state.messageIndex = nav.msg
  state.navWarning = null
  if (nav.warning) {
    setNavWarning(nav.warning)
  }
  await loadCombo(state.combo)
}

async function loadCombo(comboId) {
  const startedAt = performance.now()
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
    setNavWarning(runtimeFailure({
      code: "E_NAV_HASH_INVALID",
      className: "non-blocking",
      reason: "msg_invalid",
      message: "Message index is out of range; using message 0.",
      combo: state.combo,
      msg: 0,
      httpStatus: null
    }).toObject())
  } else {
    state.messageIndex = clamp(state.messageIndex, 0, state.dataset.messages.length - 1)
  }

  state.blockingError = null
  writeHashIfNeeded(state.combo, state.messageIndex)
  recordMetric("dataset_load_ms", roundMetric(performance.now() - startedAt))
}

function render() {
  const renderStartedAt = performance.now()
  markRenderPending()

  if (state.blockingError) {
    renderBlockingState()
    markRenderComplete()
    recordMetric("render_ms", roundMetric(performance.now() - renderStartedAt))
    return
  }

  if (state.navWarning) {
    showRuntimeMessage(`${state.navWarning.code}: ${state.navWarning.message}`, "warning")
  } else {
    clearRuntimeMessage()
  }
  if (!state.navWarning) {
    state.lastErrorKey = null
  }

  renderComboPicker()
  renderMessageNav()
  renderMessageDetails()
  markRenderComplete()
  recordMetric("render_ms", roundMetric(performance.now() - renderStartedAt))
}

function markRenderPending() {
  if (!summaryCardEl) return
  summaryCardEl.dataset.renderComplete = "false"
}

function markRenderComplete() {
  if (!summaryCardEl) return
  state.renderCycle += 1
  summaryCardEl.dataset.renderCycle = String(state.renderCycle)
  summaryCardEl.dataset.renderComplete = "true"
}

function renderBlockingState() {
  state.interaction = null
  const error = state.blockingError
  showRuntimeMessage(`${error.code}: ${error.message}`, "error")

  renderComboPicker()
  messageNavEl.innerHTML = ""

  summaryEl.innerHTML = ""
  addSummary("State", "Blocking error")
  addSummary("Code", error.code)
  addSummary("Reason", error.context.reason)
  addSummary("Hint", "Fix the source issue and refresh the page.")

  rawHexEl.textContent = ""
  byteExplainerEl.textContent = "Byte-group details are unavailable while runtime checks are failing."
  fieldTreeEl.innerHTML = ""
  const blocked = document.createElement("div")
  blocked.className = "field-node"
  blocked.textContent = "Detail view is unavailable while a blocking runtime check fails."
  fieldTreeEl.append(blocked)

  renderList(parseErrorsEl, [{ text: `${error.code}: ${error.message}` }], (item) => item.text, "warn")
  renderList(highlightsEl, [], () => "")
}

function showRuntimeMessage(message, level) {
  runtimeMessageEl.hidden = false
  runtimeMessageEl.textContent = message
  runtimeMessageEl.dataset.level = level
}

function clearRuntimeMessage() {
  runtimeMessageEl.hidden = true
  runtimeMessageEl.textContent = ""
  delete runtimeMessageEl.dataset.level
}

function renderComboPicker() {
  comboPickerEl.innerHTML = ""

  if (!state.manifest || !Array.isArray(state.manifest.combos)) {
    return
  }

  const selectedCombo = state.manifest.combos.find((combo) => combo.id === state.combo) || state.manifest.combos[0]
  const selectedProtocol = selectedCombo.protocol
  const selectedTransport = selectedCombo.transport

  comboPickerEl.append(
    buildComboRow({
      label: "Transport",
      options: ["buffered", "framed", "header"],
      selected: selectedTransport,
      onPick: (transport) => {
        const nextCombo = resolveComboId({
          protocol: selectedProtocol,
          transport,
          fallbackId: selectedCombo.id,
          prefer: "transport"
        })
        updateSelection(nextCombo, state.messageIndex)
      }
    }),
    buildComboRow({
      label: "Protocol",
      options: ["binary", "compact", "json", "header"],
      selected: selectedProtocol,
      formatOptionLabel: protocolLabel,
      onPick: (protocol) => {
        const nextCombo = resolveComboId({
          protocol,
          transport: selectedTransport,
          fallbackId: selectedCombo.id,
          prefer: "protocol"
        })
        updateSelection(nextCombo, state.messageIndex)
      }
    })
  )
}

function buildComboRow({ label, options, selected, onPick, formatOptionLabel = (option) => option }) {
  const row = document.createElement("div")
  row.className = "combo-row"

  const rowLabel = document.createElement("span")
  rowLabel.className = "combo-row-label"
  rowLabel.textContent = `${label}:`
  row.append(rowLabel)

  const optionWrap = document.createElement("div")
  optionWrap.className = "combo-row-options"

  for (const option of options) {
    const button = document.createElement("button")
    button.type = "button"
    button.className = "combo-option"
    button.textContent = formatOptionLabel(option)
    if (option === selected) {
      button.classList.add("is-active")
    }
    button.addEventListener("click", () => onPick(option))
    optionWrap.append(button)
  }

  row.append(optionWrap)
  return row
}

function resolveComboId({ protocol, transport, fallbackId, prefer }) {
  const exact = state.manifest.combos.find((combo) => combo.protocol === protocol && combo.transport === transport)
  if (exact) return exact.id

  if (prefer === "transport") {
    const transportOnly = state.manifest.combos.find((combo) => combo.transport === transport)
    if (transportOnly) return transportOnly.id
  }

  const protocolOnly = state.manifest.combos.find((combo) => combo.protocol === protocol)
  if (protocolOnly) return protocolOnly.id

  const transportOnly = state.manifest.combos.find((combo) => combo.transport === transport)
  if (transportOnly) return transportOnly.id

  return fallbackId || state.manifest.combos[0].id
}

function protocolLabel(protocol) {
  if (protocol === "json") return "JSON"
  if (protocol === "header") return "Header"
  return protocol
}

function renderMessageNav() {
  messageNavEl.innerHTML = ""

  if (!state.dataset || !Array.isArray(state.dataset.messages)) {
    return
  }

  state.dataset.messages.forEach((message, idx) => {
    const leftToRight = message.direction === "client->server"

    const row = document.createElement("button")
    row.type = "button"
    row.className = "timeline-row"
    if (idx === state.messageIndex) row.classList.add("is-active")
    row.dataset.direction = message.direction
    row.dataset.messageType = message.message_type
    row.addEventListener("click", () => updateSelection(state.combo, idx))
    row.setAttribute(
      "aria-label",
      `Message ${message.index}, ${message.method}, ${message.message_type}, ${message.direction}`
    )

    const leftCell = document.createElement("span")
    leftCell.className = "timeline-lane-cell"
    const callIndex = document.createElement("span")
    callIndex.className = "timeline-index"
    callIndex.textContent = String(message.index)
    const leftNode = document.createElement("span")
    leftNode.className = "timeline-node"
    leftNode.classList.add(leftToRight ? "is-source" : "is-target")
    leftCell.append(callIndex, leftNode)

    const center = document.createElement("span")
    center.className = "timeline-mid"

    const label = document.createElement("span")
    label.className = "timeline-label"
    const title = document.createElement("span")
    title.className = "timeline-title"
    title.textContent = `${message.method} (${message.message_type})`
    label.append(title)

    const arrow = document.createElement("span")
    arrow.className = "timeline-arrow"
    if (!leftToRight) {
      arrow.classList.add("is-right-to-left")
    }
    if (message.message_type === "reply" || message.message_type === "exception") {
      arrow.classList.add("is-return")
    }
    const seq = document.createElement("span")
    seq.className = "timeline-seq"
    seq.textContent = `seq ${message.seqid}`
    center.append(label, arrow, seq)

    const rightCell = document.createElement("span")
    rightCell.className = "timeline-lane-cell"
    const rightNode = document.createElement("span")
    rightNode.className = "timeline-node"
    rightNode.classList.add(leftToRight ? "is-target" : "is-source")
    rightCell.append(rightNode)

    row.append(leftCell, center, rightCell)
    messageNavEl.append(row)
  })
}

function renderMessageDetails() {
  if (!state.dataset || !state.dataset.messages || state.dataset.messages.length === 0) {
    return
  }

  const message = state.dataset.messages[state.messageIndex]
  const transport = message.transport || {}
  const envelope = message.envelope || {}
  const messageProtocol = message.protocol || state.dataset.combo.protocol

  summaryEl.innerHTML = ""
  addSummary("Actor", message.actor)
  addSummary("Direction", message.direction)
  addSummary("Method", message.method)
  addSummary("Type", message.message_type)
  addSummary("SeqID", String(message.seqid))
  addSummary("Raw Size", String(message.raw_size))
  addSummary("Protocol", protocolLabel(messageProtocol))
  addSummary("Transport", `${transport.type}${transport.frame_length != null ? ` (${transport.frame_length})` : ""}`)
  addSummary("Envelope Span", spanToString(envelope.span))
  addSummary("Payload Span", spanToString(message.payload?.span))

  state.interaction = createInteractionModel(message, messageProtocol)
  renderRawBytes(message, state.interaction)
  renderFieldTree(message.payload?.fields || [], state.interaction)
  renderByteExplanation(state.interaction)
  applyInteractionClasses()
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

function renderFieldTree(fields, interaction) {
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
    fragment.append(renderFieldNode(field, 0, interaction))
  }
  fieldTreeEl.append(fragment)
}

function renderFieldNode(field, depth, interaction) {
  const wrapper = document.createElement("div")
  wrapper.className = "field-node"
  wrapper.style.marginLeft = `${depth * 10}px`
  const fieldKey = interaction.fieldToKey.get(field) || null
  if (fieldKey) {
    wrapper.dataset.fieldKey = fieldKey
    wrapper.tabIndex = 0
    wrapper.setAttribute("role", "button")
    wrapper.setAttribute("aria-label", `Select field ${String(field.name ?? "")}`)
    interaction.fieldElements.set(fieldKey, wrapper)
    wrapper.addEventListener("mouseenter", () => activateFieldInteraction(fieldKey))
    wrapper.addEventListener("mouseleave", clearInteraction)
    wrapper.addEventListener("focusin", () => activateFieldInteraction(fieldKey))
    wrapper.addEventListener("focusout", clearInteraction)
    wrapper.addEventListener("click", (event) => {
      event.stopPropagation()
      selectFieldGroup(fieldKey)
    })
    wrapper.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault()
        event.stopPropagation()
        selectFieldGroup(fieldKey)
      }
    })
  }

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
    wrapper.append(renderFieldNode(child, depth + 1, interaction))
  }

  return wrapper
}

function renderRawBytes(message, interaction) {
  rawHexEl.innerHTML = ""
  interaction.byteElements.clear()
  const byteTexts = message.raw_hex.split(" ")
  const byteValues = byteTexts.map((pair) => Number.parseInt(pair, 16))
  const rowBytes = resolveHexdumpRowBytes()
  interaction.hexdumpRowBytes = rowBytes
  const dump = document.createElement("div")
  dump.className = "hexdump"

  for (let rowStart = 0; rowStart < byteTexts.length; rowStart += rowBytes) {
    const row = document.createElement("div")
    row.className = "hexdump-row"

    const offset = document.createElement("span")
    offset.className = "hexdump-offset"
    offset.textContent = rowStart.toString(16).padStart(4, "0")

    const hexColumn = document.createElement("div")
    hexColumn.className = "hexdump-hex"

    const asciiColumn = document.createElement("div")
    asciiColumn.className = "hexdump-ascii"

    for (let column = 0; column < rowBytes; column += 1) {
      const index = rowStart + column
      if (index >= byteTexts.length) {
        hexColumn.append(createHexdumpPlaceholder("hex"))
        asciiColumn.append(createHexdumpPlaceholder("ascii"))
        continue
      }

      const hexToken = createByteToken({
        interaction,
        index,
        text: byteTexts[index],
        variant: "hex"
      })
      const asciiToken = createByteToken({
        interaction,
        index,
        text: printableAscii(byteValues[index]),
        variant: "ascii"
      })

      hexColumn.append(hexToken)
      asciiColumn.append(asciiToken)
    }

    row.append(offset, hexColumn, asciiColumn)
    dump.append(row)
  }

  rawHexEl.append(dump)
}

function resolveHexdumpRowBytes() {
  if (!rawHexEl || typeof window.getComputedStyle !== "function") {
    return DEFAULT_HEXDUMP_ROW_BYTES
  }
  const cssValue = window.getComputedStyle(rawHexEl).getPropertyValue(HEXDUMP_ROW_BYTES_CSS_VAR).trim()
  const parsed = Number.parseInt(cssValue, 10)
  if (!Number.isInteger(parsed) || parsed < MIN_HEXDUMP_ROW_BYTES || parsed > MAX_HEXDUMP_ROW_BYTES) {
    return DEFAULT_HEXDUMP_ROW_BYTES
  }
  return parsed
}

function createByteToken({ interaction, index, text, variant }) {
  const token = document.createElement("span")
  token.className = `byte-token byte-token-${variant}`
  token.textContent = text
  token.dataset.byteIndex = String(index)
  token.tabIndex = 0
  token.addEventListener("mouseenter", () => activateByteInteraction(index))
  token.addEventListener("focus", () => activateByteInteraction(index))
  token.addEventListener("blur", clearInteraction)
  token.addEventListener("click", () => selectByteGroup(index))
  token.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault()
      selectByteGroup(index)
    }
  })
  registerByteElement(interaction, index, token)
  return token
}

function handleHexPointerMove(event) {
  if (!state.interaction || !state.dataset || state.blockingError) return

  const byteToken = event.target instanceof Element
    ? event.target.closest(".byte-token[data-byte-index]")
    : null
  if (byteToken) {
    const directIndex = Number.parseInt(byteToken.dataset.byteIndex || "", 10)
    if (Number.isInteger(directIndex) && directIndex >= 0) {
      activateByteInteraction(directIndex)
      return
    }
  }

  const inferredIndex = nearestByteIndexForPoint(event.clientX, event.clientY)
  if (Number.isInteger(inferredIndex)) {
    activateByteInteraction(inferredIndex)
  }
}

function nearestByteIndexForPoint(clientX, clientY) {
  if (!state.interaction) return null

  let bestIndex = null
  let bestDistance = Number.POSITIVE_INFINITY

  for (const [byteIndex, elements] of state.interaction.byteElements.entries()) {
    for (const element of elements) {
      const rect = element.getBoundingClientRect()
      const centerX = (rect.left + rect.right) / 2
      const centerY = (rect.top + rect.bottom) / 2
      const dx = clientX - centerX
      const dy = clientY - centerY
      const distance = (dx * dx) + (dy * dy)

      if (distance < bestDistance) {
        bestDistance = distance
        bestIndex = byteIndex
      }
    }
  }

  return bestIndex
}

function createHexdumpPlaceholder(variant) {
  const token = document.createElement("span")
  token.className = `byte-token byte-token-${variant} byte-token-empty`
  token.textContent = variant === "hex" ? "00" : "."
  token.setAttribute("aria-hidden", "true")
  return token
}

function registerByteElement(interaction, index, element) {
  if (!interaction.byteElements.has(index)) {
    interaction.byteElements.set(index, [])
  }
  interaction.byteElements.get(index).push(element)
}

function printableAscii(byteValue) {
  if (byteValue >= 0x20 && byteValue <= 0x7e) {
    return String.fromCharCode(byteValue)
  }
  return "."
}

function createInteractionModel(message, protocol) {
  const fields = flattenFields(message.payload?.fields || [])
  const rawBytes = message.raw_hex.split(" ").map((pair) => Number.parseInt(pair, 16))
  const payloadSpan = clampSpan(message.payload?.span, message.raw_size)
  const envelopeSpan = clampSpan(message.envelope?.span, message.raw_size)
  const frameHeaderSpan = clampSpan(message.transport?.frame_header_span, message.raw_size)
  const headerSpan = clampSpan(message.transport?.header_span, message.raw_size)
  const headerProtocol = message.transport?.header_protocol || null
  const interaction = {
    message,
    protocol,
    fieldToKey: new WeakMap(),
    fieldElements: new Map(),
    byteElements: new Map(),
    bytesByField: new Map(),
    fieldByByte: new Array(message.raw_size).fill(null),
    activeFieldKey: null,
    activeByteIndex: null,
    activeGroupId: null,
    activeSubfieldId: null,
    selectedSubfieldId: null,
    selectedGroupId: null,
    hexdumpRowBytes: null,
    groupByByte: new Array(message.raw_size).fill(null),
    groups: new Map(),
    subfields: new Map(),
    subfieldToGroup: new Map()
  }

  for (const field of fields) {
    interaction.fieldToKey.set(field.node, field.key)
    interaction.bytesByField.set(field.key, [])
  }

  const previousFieldIdByKey = new Map()
  const lastFieldIdByParent = new Map()
  const fieldsInWireOrder = [...fields].sort((left, right) => {
    if (left.span[0] !== right.span[0]) return left.span[0] - right.span[0]
    if (left.depth !== right.depth) return left.depth - right.depth
    return left.key.localeCompare(right.key)
  })
  for (const field of fieldsInWireOrder) {
    const parentKey = field.parentKey
    previousFieldIdByKey.set(field.key, lastFieldIdByParent.get(parentKey) ?? null)
    if (Number.isInteger(field.node.id)) {
      lastFieldIdByParent.set(parentKey, field.node.id)
    }
  }

  const ordered = [...fields].sort((left, right) => {
    if (left.depth !== right.depth) return right.depth - left.depth
    const leftLen = left.span[1] - left.span[0]
    const rightLen = right.span[1] - right.span[0]
    if (leftLen !== rightLen) return leftLen - rightLen
    return left.key.localeCompare(right.key)
  })

  for (const field of ordered) {
    const start = clamp(field.span[0], 0, message.raw_size)
    const end = clamp(field.span[1], 0, message.raw_size)
    for (let index = start; index < end; index += 1) {
      if (interaction.fieldByByte[index] === null) {
        interaction.fieldByByte[index] = field.key
      }
    }
  }

  for (let index = 0; index < interaction.fieldByByte.length; index += 1) {
    const fieldKey = interaction.fieldByByte[index]
    if (fieldKey !== null) {
      interaction.bytesByField.get(fieldKey).push(index)
    }
  }

  for (const field of fields) {
    const start = clamp(field.span[0], 0, message.raw_size)
    const end = clamp(field.span[1], 0, message.raw_size)
    if (start >= end) continue

    const groupId = `field:${field.key}`
    const subfields = buildFieldSubfields({
      protocol,
      rawBytes,
      field,
      groupId,
      start,
      end,
      previousFieldId: previousFieldIdByKey.get(field.key) ?? null
    })

    interaction.groups.set(groupId, {
      id: groupId,
      type: "field",
      label: `Field ${field.node.name} (${field.node.ttype}, id=${field.node.id})`,
      description: buildFieldGroupDescription({
        protocol,
        field,
        start,
        end,
        previousFieldId: previousFieldIdByKey.get(field.key) ?? null
      }),
      start,
      end,
      subfields
    })
    for (const subfield of subfields) {
      interaction.subfields.set(subfield.id, subfield)
      interaction.subfieldToGroup.set(subfield.id, groupId)
    }
    for (let index = start; index < end; index += 1) {
      interaction.groupByByte[index] = groupId
    }
  }

  const envelopeSubfields = buildEnvelopeSubfields({
    protocol,
    rawBytes,
    envelopeSpan,
    envelopeName: message.envelope?.name || ""
  })

  assignGroupIfUnassigned(interaction, {
    id: "envelope",
    type: "envelope",
    label: "Protocol envelope",
    description: "Encodes message metadata (method name, message type, and sequence id); expand subfields below for byte-level breakdown.",
    start: envelopeSpan[0],
    end: envelopeSpan[1],
    subfields: envelopeSubfields
  })

  assignGroupIfUnassigned(interaction, {
    id: "frame-header",
    type: "frame-header",
    label: (message.transport?.type === "header") ? "Header transport length prefix" : "Framed transport header",
    description: (message.transport?.type === "header")
      ? "Encodes the 4-byte big-endian header-transport frame length prefix."
      : "Encodes the 4-byte big-endian framed-transport payload length prefix.",
    start: frameHeaderSpan[0],
    end: frameHeaderSpan[1]
  })

  if (message.transport?.type === "header") {
    assignGroupIfUnassigned(interaction, {
      id: "header-transport",
      type: "header-transport",
      label: "Header transport metadata",
      description: "Header transport metadata between the length prefix and protocol payload; subfields break down fixed header fields and metadata varints.",
      start: headerSpan[0],
      end: headerSpan[1],
      subfields: buildHeaderTransportSubfields({
        rawBytes,
        headerSpan,
        headerProtocol
      })
    })
  }

  assignPayloadStructuralGroups(interaction, payloadSpan)
  assignFallbackGroups(interaction)

  if (payloadSpan[0] < payloadSpan[1]) {
    interaction.selectedGroupId = interaction.groupByByte[payloadSpan[0]]
    interaction.selectedSubfieldId = subfieldIdForByte(interaction, interaction.selectedGroupId, payloadSpan[0])
  } else {
    interaction.selectedGroupId = interaction.groupByByte[0]
    interaction.selectedSubfieldId = subfieldIdForByte(interaction, interaction.selectedGroupId, 0)
  }

  return interaction
}

function flattenFields(fields, parentKey = "f", depth = 0) {
  const flat = []
  fields.forEach((field, idx) => {
    const key = `${parentKey}.${idx}`
    flat.push({
      key,
      parentKey,
      node: field,
      span: Array.isArray(field.span) ? field.span : [0, 0],
      depth
    })
    const children = Array.isArray(field.children) ? field.children : []
    flat.push(...flattenFields(children, key, depth + 1))
  })
  return flat
}

function groupIdForFieldKey(fieldKey) {
  return `field:${fieldKey}`
}

function fieldKeyFromGroupId(groupId) {
  if (typeof groupId !== "string") return null
  return groupId.startsWith("field:") ? groupId.slice("field:".length) : null
}

function subfieldIdForByte(interaction, groupId, byteIndex) {
  if (!interaction || !Number.isInteger(byteIndex) || byteIndex < 0) return null
  if (typeof groupId !== "string") return null

  const group = interaction.groups.get(groupId)
  if (!group || !Array.isArray(group.subfields)) return null

  let bestId = null
  let bestSpan = Number.POSITIVE_INFINITY
  for (const subfield of group.subfields) {
    if (byteIndex < subfield.start || byteIndex >= subfield.end) continue
    const span = subfield.end - subfield.start
    if (span < bestSpan) {
      bestSpan = span
      bestId = subfield.id
    }
  }
  return bestId
}

function activateFieldInteraction(fieldKey) {
  if (!state.interaction) return
  const groupId = groupIdForFieldKey(fieldKey)
  state.interaction.activeFieldKey = fieldKey
  state.interaction.activeGroupId = state.interaction.groups.has(groupId) ? groupId : null
  state.interaction.activeByteIndex = null
  state.interaction.activeSubfieldId = null
  applyInteractionClasses()
}

function activateByteInteraction(byteIndex) {
  if (!state.interaction) return
  const groupId = state.interaction.groupByByte[byteIndex] || null
  const fieldKey = state.interaction.fieldByByte[byteIndex] || null
  const subfieldId = subfieldIdForByte(state.interaction, groupId, byteIndex)
  state.interaction.activeGroupId = groupId
  state.interaction.activeFieldKey = fieldKey
  state.interaction.activeByteIndex = byteIndex
  state.interaction.activeSubfieldId = subfieldId
  applyInteractionClasses()
}

function clearInteraction() {
  if (!state.interaction) return
  state.interaction.activeGroupId = null
  state.interaction.activeFieldKey = null
  state.interaction.activeByteIndex = null
  state.interaction.activeSubfieldId = null
  applyInteractionClasses()
}

function selectByteGroup(byteIndex) {
  if (!state.interaction) return
  const selectedGroupId = state.interaction.groupByByte[byteIndex] || null
  const selectedSubfieldId = subfieldIdForByte(state.interaction, selectedGroupId, byteIndex)
  state.interaction.selectedGroupId = selectedGroupId
  state.interaction.selectedSubfieldId = selectedSubfieldId
  state.interaction.activeGroupId = selectedGroupId
  state.interaction.activeFieldKey = fieldKeyFromGroupId(selectedGroupId) || null
  state.interaction.activeByteIndex = byteIndex
  state.interaction.activeSubfieldId = selectedSubfieldId
  renderByteExplanation(state.interaction)
  applyInteractionClasses()
}

function selectFieldGroup(fieldKey) {
  if (!state.interaction) return
  const selectedGroupId = groupIdForFieldKey(fieldKey)
  if (!state.interaction.groups.has(selectedGroupId)) return
  const fieldBytes = state.interaction.bytesByField.get(fieldKey) || []
  const focusByte = fieldBytes.length > 0 ? fieldBytes[0] : null
  state.interaction.selectedGroupId = selectedGroupId
  state.interaction.selectedSubfieldId = Number.isInteger(focusByte)
    ? subfieldIdForByte(state.interaction, selectedGroupId, focusByte)
    : null
  state.interaction.activeGroupId = selectedGroupId
  state.interaction.activeFieldKey = fieldKey
  state.interaction.activeByteIndex = focusByte
  state.interaction.activeSubfieldId = state.interaction.selectedSubfieldId
  renderByteExplanation(state.interaction)
  applyInteractionClasses()
}

function activateSubfield(subfieldId) {
  if (!state.interaction) return
  state.interaction.activeSubfieldId = subfieldId
  applyInteractionClasses()
}

function clearSubfield() {
  if (!state.interaction) return
  state.interaction.activeSubfieldId = null
  applyInteractionClasses()
}

function toggleSubfieldSelection(subfieldId) {
  if (!state.interaction) return
  if (!state.interaction.subfields.has(subfieldId)) return

  if (state.interaction.selectedSubfieldId === subfieldId) {
    state.interaction.selectedSubfieldId = null
    state.interaction.activeSubfieldId = null
    applyInteractionClasses()
    return
  }

  const groupId = state.interaction.subfieldToGroup.get(subfieldId) || state.interaction.selectedGroupId
  state.interaction.selectedGroupId = groupId || state.interaction.selectedGroupId
  state.interaction.selectedSubfieldId = subfieldId
  state.interaction.activeSubfieldId = subfieldId
  state.interaction.activeGroupId = groupId || state.interaction.activeGroupId
  state.interaction.activeFieldKey = fieldKeyFromGroupId(state.interaction.selectedGroupId) || null
  renderByteExplanation(state.interaction)
  applyInteractionClasses()
}

function renderByteExplanation(interaction) {
  if (!interaction || !interaction.selectedGroupId) {
    byteExplainerEl.textContent = "Click a byte to inspect its semantic group."
    return
  }

  const group = interaction.groups.get(interaction.selectedGroupId)
  if (!group) {
    byteExplainerEl.textContent = "No semantic group found for the selected byte."
    return
  }

  byteExplainerEl.innerHTML = ""
  const title = document.createElement("strong")
  title.textContent = group.label

  const details = document.createElement("span")
  details.textContent = ` (${group.start}-${group.end - 1}, ${group.end - group.start} bytes) `

  const body = document.createElement("span")
  body.textContent = group.description

  byteExplainerEl.append(title, details, body)

  if (Array.isArray(group.subfields) && group.subfields.length > 0) {
    const breakdownTitle = document.createElement("div")
    breakdownTitle.className = "byte-subfields-title"
    if (group.type === "envelope") {
      breakdownTitle.textContent = "Envelope subfields (hover to preview bytes, click to lock):"
    } else if (group.type === "field") {
      breakdownTitle.textContent = "Field subfields (hover to preview bytes, click to lock):"
    } else {
      breakdownTitle.textContent = "Subfields (hover to preview bytes, click to lock):"
    }
    byteExplainerEl.append(breakdownTitle)

    const list = document.createElement("ul")
    list.className = "byte-subfields"

    for (const subfield of group.subfields) {
      const item = document.createElement("li")
      const button = document.createElement("button")
      button.type = "button"
      button.className = "byte-subfield-button"
      button.dataset.subfieldId = subfield.id
      button.textContent = `${subfield.label} [${subfield.start}-${subfield.end - 1}]`
      button.addEventListener("mouseenter", () => activateSubfield(subfield.id))
      button.addEventListener("mouseleave", clearSubfield)
      button.addEventListener("focus", () => activateSubfield(subfield.id))
      button.addEventListener("blur", clearSubfield)
      button.addEventListener("click", () => toggleSubfieldSelection(subfield.id))

      const description = document.createElement("div")
      description.className = "byte-subfield-description"
      description.textContent = subfield.description

      item.append(button, description)
      list.append(item)
    }

    byteExplainerEl.append(list)
  }
}

function applyInteractionClasses() {
  if (!state.interaction) return

  const activeFieldKey = state.interaction.activeFieldKey
  const highlightedBytes = new Set(activeFieldKey ? state.interaction.bytesByField.get(activeFieldKey) || [] : [])
  const hoverGroup = state.interaction.activeGroupId
    ? state.interaction.groups.get(state.interaction.activeGroupId)
    : null
  const hoverGroupBytes = hoverGroup
    ? new Set(rangeIndices(hoverGroup.start, hoverGroup.end))
    : new Set()
  const selectedGroup = state.interaction.selectedGroupId
    ? state.interaction.groups.get(state.interaction.selectedGroupId)
    : null
  const selectedFieldKey = selectedGroup ? fieldKeyFromGroupId(selectedGroup.id) : null
  const selectedBytes = selectedGroup
    ? new Set(rangeIndices(selectedGroup.start, selectedGroup.end))
    : new Set()
  const selectedSubfield = state.interaction.selectedSubfieldId
    ? state.interaction.subfields.get(state.interaction.selectedSubfieldId)
    : null
  const activeSubfield = state.interaction.activeSubfieldId
    ? state.interaction.subfields.get(state.interaction.activeSubfieldId)
    : null
  const suppressSelectedSubfield =
    Boolean(activeSubfield) &&
    Boolean(selectedSubfield) &&
    activeSubfield.id !== selectedSubfield.id
  const selectedSubfieldBytes = (!suppressSelectedSubfield && selectedSubfield)
    ? new Set(rangeIndices(selectedSubfield.start, selectedSubfield.end))
    : new Set()
  const activeSubfieldBytes = activeSubfield
    ? new Set(rangeIndices(activeSubfield.start, activeSubfield.end))
    : new Set()

  for (const [fieldKey, element] of state.interaction.fieldElements.entries()) {
    element.classList.toggle("is-hover-active", fieldKey === activeFieldKey)
    element.classList.toggle("is-group-selected", fieldKey === selectedFieldKey)
  }

  for (const [byteIndex, elements] of state.interaction.byteElements.entries()) {
    for (const element of elements) {
      element.classList.toggle("is-hover-active", highlightedBytes.has(byteIndex))
      element.classList.toggle("is-byte-focus", byteIndex === state.interaction.activeByteIndex)
      element.classList.toggle("is-group-hover", hoverGroupBytes.has(byteIndex))
      element.classList.toggle("is-group-selected", selectedBytes.has(byteIndex))
      element.classList.toggle("is-subfield-selected", selectedSubfieldBytes.has(byteIndex))
      element.classList.toggle("is-subfield-active", activeSubfieldBytes.has(byteIndex))
    }
  }

  const subfieldButtons = byteExplainerEl.querySelectorAll(".byte-subfield-button")
  for (const button of subfieldButtons) {
    const id = button.dataset.subfieldId || ""
    button.classList.toggle("is-active", id === state.interaction.activeSubfieldId)
    button.classList.toggle("is-selected", id === state.interaction.selectedSubfieldId)
  }
}

function assignGroupIfUnassigned(interaction, group) {
  if (group.start >= group.end) return
  interaction.groups.set(group.id, group)
  if (Array.isArray(group.subfields)) {
    for (const subfield of group.subfields) {
      interaction.subfields.set(subfield.id, subfield)
      interaction.subfieldToGroup.set(subfield.id, group.id)
    }
  }
  for (let index = group.start; index < group.end; index += 1) {
    if (interaction.groupByByte[index] === null) {
      interaction.groupByByte[index] = group.id
    }
  }
}

function assignPayloadStructuralGroups(interaction, payloadSpan) {
  let segmentStart = null

  for (let index = payloadSpan[0]; index < payloadSpan[1]; index += 1) {
    const assigned = interaction.groupByByte[index] !== null
    if (!assigned && segmentStart === null) {
      segmentStart = index
    }

    if ((assigned || index === payloadSpan[1] - 1) && segmentStart !== null) {
      const segmentEnd = assigned ? index : index + 1
      const groupId = `payload-struct:${segmentStart}-${segmentEnd}`
      assignGroupIfUnassigned(interaction, {
        id: groupId,
        type: "payload-structural",
        label: "Struct structural bytes",
        description: "Payload bytes outside parsed field spans. For valid struct payloads this is typically the terminating STOP marker (0x00).",
        start: segmentStart,
        end: segmentEnd
      })
      segmentStart = null
    }
  }
}

function assignFallbackGroups(interaction) {
  let segmentStart = null
  for (let index = 0; index < interaction.groupByByte.length; index += 1) {
    const assigned = interaction.groupByByte[index] !== null
    if (!assigned && segmentStart === null) {
      segmentStart = index
    }

    if ((assigned || index === interaction.groupByByte.length - 1) && segmentStart !== null) {
      const segmentEnd = assigned ? index : index + 1
      const groupId = `wire:${segmentStart}-${segmentEnd}`
      assignGroupIfUnassigned(interaction, {
        id: groupId,
        type: "wire",
        label: "Wire bytes",
        description: "Unclassified wire-level bytes outside parsed envelope, frame-header, and payload-field groups.",
        start: segmentStart,
        end: segmentEnd
      })
      segmentStart = null
    }
  }
}

function buildFieldGroupDescription({ protocol, field, previousFieldId }) {
  const ttype = String(field.node.ttype ?? "value")
  const fieldId = Number.isInteger(field.node.id) ? field.node.id : "?"

  if (protocol === "binary") {
    return `Binary field layout: 1-byte type tag, 2-byte big-endian field id (${fieldId}), then ${ttype} value bytes.`
  }

  if (protocol === "compact") {
    const previousText = Number.isInteger(previousFieldId) ? String(previousFieldId) : "0 at struct start"
    return `Compact field layout: header byte carries type and id delta (from ${previousText}); when delta is 0, the field id follows as an i16 zigzag-varint, then ${ttype} value bytes.`
  }

  return "Encodes this field on the wire, including field header bytes and value bytes."
}

function buildFieldSubfields({ protocol, rawBytes, field, groupId, start, end, previousFieldId }) {
  if (start >= end) return []

  const subfields = []
  const pushSubfield = (id, label, description, subStart, subEnd) => {
    if (subStart >= subEnd) return
    subfields.push({ id, label, description, start: subStart, end: subEnd })
  }

  if (protocol === "binary") {
    const typeByte = byteAt(rawBytes, start)
    pushSubfield(
      `${groupId}.type_tag`,
      "Field type tag",
      typeByte === null
        ? `1-byte binary type tag for ${field.node.ttype}.`
        : `1-byte binary type tag: ${hexByte(typeByte)} (${binaryTypeName(typeByte)}).`,
      start,
      Math.min(start + 1, end)
    )

    const idStart = start + 1
    const idEnd = Math.min(start + 3, end)
    if (idStart < idEnd) {
      const decodedId = idEnd - idStart === 2 ? readInt16BE(rawBytes, idStart) : null
      const idText = decodedId === null ? "unknown" : String(decodedId)
      pushSubfield(
        `${groupId}.field_id`,
        "Field id",
        `2-byte big-endian field id bytes. Decoded id: ${idText}.`,
        idStart,
        idEnd
      )
    }

    const valueStart = Math.min(start + 3, end)
    pushSubfield(
      `${groupId}.value`,
      "Field value bytes",
      binaryValueHint(String(field.node.ttype ?? "value")),
      valueStart,
      end
    )
  } else if (protocol === "compact") {
    const headerByte = byteAt(rawBytes, start)
    const headerEnd = Math.min(start + 1, end)
    const typeNibble = headerByte === null ? null : headerByte & 0x0f
    const fieldIdDelta = headerByte === null ? null : (headerByte >> 4) & 0x0f

    pushSubfield(
      `${groupId}.header`,
      "Field header byte",
      headerByte === null
        ? "Compact field header byte (type + field-id delta)."
        : `Compact field header ${hexByte(headerByte)}: type=${compactTypeName(typeNibble)} (${typeNibble}), id delta=${fieldIdDelta}.`,
      start,
      headerEnd
    )

    let cursor = headerEnd
    if (fieldIdDelta === 0) {
      const idVarint = decodeCompactVarint(rawBytes, cursor, end)
      if (idVarint.length > 0) {
        const decodedFieldId = decodeCompactZigZag(idVarint.value)
        pushSubfield(
          `${groupId}.field_id_varint`,
          "Field id varint",
          `Explicit compact field id bytes (i16 zigzag-varint). Decoded id: ${decodedFieldId}.`,
          cursor,
          cursor + idVarint.length
        )
        cursor += idVarint.length
      }
    } else if (fieldIdDelta !== null && fieldIdDelta > 0) {
      const derivedId = Number.isInteger(previousFieldId) ? previousFieldId + fieldIdDelta : field.node.id
      pushSubfield(
        `${groupId}.field_id_delta`,
        "Field id delta (in header)",
        `Field id is encoded in header high nibble via delta ${fieldIdDelta}; decoded id: ${derivedId}.`,
        start,
        headerEnd
      )
    }

    if (typeNibble === 0x01 || typeNibble === 0x02) {
      pushSubfield(
        `${groupId}.bool_value`,
        "Boolean value (in header)",
        `Boolean ${typeNibble === 0x01 ? "true" : "false"} is encoded directly in the header type nibble.`,
        start,
        headerEnd
      )
      pushSubfield(
        `${groupId}.value_remainder`,
        "Additional value bytes",
        compactValueHint(String(field.node.ttype ?? "value")),
        cursor,
        end
      )
    } else {
      pushSubfield(
        `${groupId}.value`,
        "Field value bytes",
        compactValueHint(String(field.node.ttype ?? "value")),
        cursor,
        end
      )
    }
  } else {
    pushSubfield(
      `${groupId}.bytes`,
      "Field bytes",
      "Field bytes for this protocol encoding.",
      start,
      end
    )
  }

  fillSubfieldGaps(subfields, [start, end], `${groupId}.unknown`, "Unclassified field bytes")
  return subfields.sort((left, right) => left.start - right.start || left.end - right.end)
}

function fillSubfieldGaps(subfields, span, idPrefix, label) {
  const [start, end] = span
  const covered = new Array(Math.max(0, end - start)).fill(false)
  for (const subfield of subfields) {
    for (let index = subfield.start; index < subfield.end; index += 1) {
      if (index >= start && index < end) {
        covered[index - start] = true
      }
    }
  }

  let gapStart = null
  for (let offset = 0; offset < covered.length; offset += 1) {
    const isCovered = covered[offset]
    if (!isCovered && gapStart === null) {
      gapStart = start + offset
    }

    if ((isCovered || offset === covered.length - 1) && gapStart !== null) {
      const gapEnd = isCovered ? start + offset : start + offset + 1
      subfields.push({
        id: `${idPrefix}.${gapStart}-${gapEnd}`,
        label,
        description: "Field bytes not yet classified into a finer-grained subfield.",
        start: gapStart,
        end: gapEnd
      })
      gapStart = null
    }
  }
}

function buildHeaderTransportSubfields({ rawBytes, headerSpan, headerProtocol }) {
  if (headerSpan[0] >= headerSpan[1]) return []

  const subfields = []
  const pushSubfield = (id, label, description, start, end) => {
    if (start >= end) return
    subfields.push({ id, label, description, start, end })
  }

  const start = headerSpan[0]
  const end = headerSpan[1]
  const magic = (start + 2 <= end) ? readInt16BE(rawBytes, start) : null
  const flags = (start + 4 <= end) ? readInt16BE(rawBytes, start + 2) : null
  const seqid = (start + 8 <= end) ? readInt32BE(rawBytes, start + 4) : null
  const headerWords = (start + 10 <= end) ? readInt16BE(rawBytes, start + 8) : null
  const metadataStart = Math.min(start + 10, end)

  pushSubfield(
    "header.transport.magic",
    "Header magic",
    magic === null
      ? "2-byte header-transport magic marker (expected 0x0fff)."
      : `2-byte header-transport magic marker: ${hexWord(magic & 0xffff)}.`,
    start,
    Math.min(start + 2, end)
  )
  pushSubfield(
    "header.transport.flags",
    "Flags",
    flags === null
      ? "2-byte header-transport flags field."
      : `2-byte header-transport flags field: ${hexWord(flags & 0xffff)}.`,
    start + 2,
    Math.min(start + 4, end)
  )
  pushSubfield(
    "header.transport.seqid",
    "Header sequence id",
    seqid === null
      ? "4-byte unsigned sequence id in the transport header."
      : `4-byte unsigned sequence id in the transport header: ${seqid >>> 0}.`,
    start + 4,
    Math.min(start + 8, end)
  )
  pushSubfield(
    "header.transport.header_words",
    "Header words",
    headerWords === null
      ? "2-byte count of metadata 32-bit words."
      : `2-byte count of metadata 32-bit words: ${headerWords}.`,
    start + 8,
    Math.min(start + 10, end)
  )

  const protocolVarint = decodeCompactVarint(rawBytes, metadataStart, end)
  if (protocolVarint.length > 0) {
    const protocolId = protocolVarint.value
    const protocolName = headerProtocol || headerSubprotocolName(protocolId)
    pushSubfield(
      "header.transport.protocol_id",
      "Payload protocol id varint",
      `Varint protocol id in header metadata. Decoded value: ${protocolId}${protocolName ? ` (${protocolName})` : ""}.`,
      metadataStart,
      metadataStart + protocolVarint.length
    )
  }

  let cursor = metadataStart + protocolVarint.length
  const transformCountVarint = decodeCompactVarint(rawBytes, cursor, end)
  if (transformCountVarint.length > 0) {
    pushSubfield(
      "header.transport.transform_count",
      "Transform count varint",
      `Varint count of payload transforms declared in header metadata: ${transformCountVarint.value}.`,
      cursor,
      cursor + transformCountVarint.length
    )
    cursor += transformCountVarint.length
  }

  const transformsToRead = clamp(transformCountVarint.value || 0, 0, 16)
  for (let index = 0; index < transformsToRead && cursor < end; index += 1) {
    const transformVarint = decodeCompactVarint(rawBytes, cursor, end)
    if (transformVarint.length <= 0) break
    pushSubfield(
      `header.transport.transform.${index}`,
      `Transform id ${index}`,
      `Varint transport transform identifier ${transformVarint.value}.`,
      cursor,
      cursor + transformVarint.length
    )
    cursor += transformVarint.length
  }

  let infoIndex = 0
  while (cursor < end) {
    const infoTypeStart = cursor
    const infoTypeVarint = decodeCompactVarint(rawBytes, cursor, end)
    if (infoTypeVarint.length <= 0) break

    const infoType = infoTypeVarint.value
    cursor += infoTypeVarint.length
    pushSubfield(
      `header.transport.info_type.${infoIndex}`,
      `Info type ${infoIndex}`,
      `Varint info section type identifier: ${infoType}.`,
      infoTypeStart,
      cursor
    )

    if (infoType === 0) {
      // Type 0 marks padding start to the end of metadata words.
      pushHeaderPaddingSubfields({
        subfields,
        rawBytes,
        start: cursor,
        end
      })
      cursor = end
      break
    }

    if (infoType !== 1) {
      pushSubfield(
        `header.transport.info_payload_unknown.${infoIndex}`,
        `Info payload ${infoIndex}`,
        "Unknown info section type payload bytes.",
        cursor,
        end
      )
      cursor = end
      break
    }

    const kvCountStart = cursor
    const kvCountVarint = decodeCompactVarint(rawBytes, cursor, end)
    if (kvCountVarint.length <= 0) break
    cursor += kvCountVarint.length
    pushSubfield(
      `header.transport.kv_count.${infoIndex}`,
      `KV pair count ${infoIndex}`,
      `Varint count of key/value headers in this section: ${kvCountVarint.value}.`,
      kvCountStart,
      cursor
    )

    const kvPairs = clamp(kvCountVarint.value, 0, 64)
    for (let kvIndex = 0; kvIndex < kvPairs && cursor < end; kvIndex += 1) {
      const keyLengthStart = cursor
      const keyLengthVarint = decodeCompactVarint(rawBytes, cursor, end)
      if (keyLengthVarint.length <= 0) break
      cursor += keyLengthVarint.length
      pushSubfield(
        `header.transport.kv_key_len.${infoIndex}.${kvIndex}`,
        `Key length ${kvIndex}`,
        `Varint byte length of header key ${kvIndex}: ${keyLengthVarint.value}.`,
        keyLengthStart,
        cursor
      )

      const keyLength = clamp(keyLengthVarint.value, 0, Math.max(0, end - cursor))
      const keyStart = cursor
      const keyEnd = cursor + keyLength
      const keyPreview = decodeHeaderString(rawBytes, keyStart, keyEnd)
      pushSubfield(
        `header.transport.kv_key.${infoIndex}.${kvIndex}`,
        `Key bytes ${kvIndex}`,
        `UTF-8 key bytes for header ${kvIndex}: "${keyPreview}".`,
        keyStart,
        keyEnd
      )
      cursor = keyEnd

      const valueLengthStart = cursor
      const valueLengthVarint = decodeCompactVarint(rawBytes, cursor, end)
      if (valueLengthVarint.length <= 0) break
      cursor += valueLengthVarint.length
      pushSubfield(
        `header.transport.kv_value_len.${infoIndex}.${kvIndex}`,
        `Value length ${kvIndex}`,
        `Varint byte length of header value ${kvIndex}: ${valueLengthVarint.value}.`,
        valueLengthStart,
        cursor
      )

      const valueLength = clamp(valueLengthVarint.value, 0, Math.max(0, end - cursor))
      const valueStart = cursor
      const valueEnd = cursor + valueLength
      const valuePreview = decodeHeaderString(rawBytes, valueStart, valueEnd)
      pushSubfield(
        `header.transport.kv_value.${infoIndex}.${kvIndex}`,
        `Value bytes ${kvIndex}`,
        `UTF-8 value bytes for header ${kvIndex}: "${valuePreview}".`,
        valueStart,
        valueEnd
      )
      cursor = valueEnd
    }

    infoIndex += 1
  }

  if (cursor < end) {
    pushHeaderPaddingSubfields({
      subfields,
      rawBytes,
      start: cursor,
      end
    })
  }

  fillSubfieldGaps(subfields, headerSpan, "header.transport.unknown", "Header metadata bytes")
  return subfields.sort((left, right) => left.start - right.start || left.end - right.end)
}

function headerSubprotocolName(protocolId) {
  if (protocolId === 0) return "binary"
  if (protocolId === 2) return "compact"
  return null
}

function pushHeaderPaddingSubfields({ subfields, rawBytes, start, end }) {
  if (start >= end) return

  let cursor = start
  while (cursor < end) {
    const byte = byteAt(rawBytes, cursor)
    const paddingStart = cursor
    const isZero = byte === 0
    cursor += 1
    while (cursor < end) {
      const next = byteAt(rawBytes, cursor)
      if ((next === 0) !== isZero) break
      cursor += 1
    }
    subfields.push({
      id: `header.transport.padding.${paddingStart}-${cursor}`,
      label: isZero ? "Padding bytes" : "Padding/unused bytes",
      description: isZero
        ? "Zero padding used to align header metadata to a 4-byte boundary."
        : "Non-zero trailing metadata/padding bytes.",
      start: paddingStart,
      end: cursor
    })
  }
}

function decodeHeaderString(rawBytes, start, end) {
  if (start >= end) return ""

  const bytes = Uint8Array.from(rawBytes.slice(start, end))
  try {
    const decoded = new TextDecoder("utf-8", { fatal: true }).decode(bytes)
    return shortenHeaderString(decoded)
  } catch {
    let ascii = ""
    for (const value of bytes) {
      ascii += (value >= 0x20 && value <= 0x7e) ? String.fromCharCode(value) : "."
    }
    return shortenHeaderString(ascii)
  }
}

function shortenHeaderString(value) {
  if (value.length <= 48) return value
  return `${value.slice(0, 45)}...`
}

function buildEnvelopeSubfields({ protocol, rawBytes, envelopeSpan, envelopeName }) {
  if (envelopeSpan[0] >= envelopeSpan[1]) {
    return []
  }

  const subfields = []
  const pushSubfield = (id, label, description, start, end) => {
    if (start >= end) return
    subfields.push({ id, label, description, start, end })
  }

  if (protocol === "binary") {
    let cursor = envelopeSpan[0]
    const end = envelopeSpan[1]
    const headerStart = cursor
    const headerEnd = Math.min(cursor + 4, end)
    const headerByte0 = byteAt(rawBytes, headerStart)
    const headerByte1 = byteAt(rawBytes, headerStart + 1)
    const headerByte2 = byteAt(rawBytes, headerStart + 2)
    const headerByte3 = byteAt(rawBytes, headerStart + 3)

    pushSubfield(
      "envelope.binary.header_byte0",
      "Header byte 0 (version marker high)",
      headerByte0 === null
        ? "High byte of the strict binary VERSION_1 marker (expected 0x80)."
        : `High byte of the strict binary VERSION_1 marker: ${hexByte(headerByte0)} (expected 0x80).`,
      headerStart,
      Math.min(headerStart + 1, headerEnd)
    )
    pushSubfield(
      "envelope.binary.header_byte1",
      "Header byte 1 (version marker low)",
      headerByte1 === null
        ? "Second byte of the strict binary VERSION_1 marker (expected 0x01)."
        : `Second byte of the strict binary VERSION_1 marker: ${hexByte(headerByte1)} (expected 0x01).`,
      headerStart + 1,
      Math.min(headerStart + 2, headerEnd)
    )
    pushSubfield(
      "envelope.binary.header_byte2",
      "Header byte 2 (version/type high byte)",
      headerByte2 === null
        ? "Third byte of the strict VERSION_1 header word (normally 0x00)."
        : `Third byte of the strict VERSION_1 header word: ${hexByte(headerByte2)} (normally 0x00).`,
      headerStart + 2,
      Math.min(headerStart + 3, headerEnd)
    )
    pushSubfield(
      "envelope.binary.header_byte3",
      "Header byte 3 (message type)",
      headerByte3 === null
        ? "Low byte of the strict header word; carries message type (call/reply/exception/oneway)."
        : `Low byte of the strict header word: ${hexByte(headerByte3)} (${binaryMessageTypeLabel(headerByte3)}).`,
      headerStart + 3,
      Math.min(headerStart + 4, headerEnd)
    )
    cursor += 4

    const nameLengthStart = cursor
    const nameLengthEnd = Math.min(cursor + 4, end)
    let methodNameLength = utf8ByteLength(envelopeName)
    if (nameLengthEnd - nameLengthStart === 4) {
      methodNameLength = readInt32BE(rawBytes, nameLengthStart)
    }
    pushSubfield(
      "envelope.binary.name_length",
      "Method name length",
      "4-byte big-endian length of the method name string.",
      nameLengthStart,
      nameLengthEnd
    )
    cursor += 4

    const safeNameLength = clamp(methodNameLength, 0, Math.max(0, end - cursor))
    pushSubfield(
      "envelope.binary.name_bytes",
      "Method name bytes",
      "UTF-8 bytes for the method name referenced by the envelope.",
      cursor,
      cursor + safeNameLength
    )
    cursor += safeNameLength

    pushSubfield(
      "envelope.binary.seqid",
      "Sequence id",
      "4-byte signed sequence id used to match requests and responses.",
      cursor,
      Math.min(cursor + 4, end)
    )
  } else if (protocol === "compact") {
    let cursor = envelopeSpan[0]
    const end = envelopeSpan[1]

    pushSubfield(
      "envelope.compact.protocol_id",
      "Protocol id",
      "Compact protocol identifier byte (0x82).",
      cursor,
      Math.min(cursor + 1, end)
    )
    cursor += 1

    pushSubfield(
      "envelope.compact.version_type",
      "Version + message type",
      "Compact envelope byte combining protocol version (low bits) and message type (high bits).",
      cursor,
      Math.min(cursor + 1, end)
    )
    cursor += 1

    const seqidVarint = decodeCompactVarint(rawBytes, cursor, end)
    pushSubfield(
      "envelope.compact.seqid_varint",
      "Sequence id varint",
      "Unsigned varint-encoded sequence id used to correlate requests and responses.",
      cursor,
      cursor + seqidVarint.length
    )
    cursor += seqidVarint.length

    const nameLenVarint = decodeCompactVarint(rawBytes, cursor, end)
    pushSubfield(
      "envelope.compact.name_length_varint",
      "Method name length varint",
      "Unsigned varint byte length of the method name.",
      cursor,
      cursor + nameLenVarint.length
    )
    cursor += nameLenVarint.length

    const safeNameLength = clamp(nameLenVarint.value, 0, Math.max(0, end - cursor))
    pushSubfield(
      "envelope.compact.name_bytes",
      "Method name bytes",
      "UTF-8 bytes for the method name referenced by the envelope.",
      cursor,
      cursor + safeNameLength
    )
  } else {
    pushSubfield(
      "envelope.generic",
      "Envelope bytes",
      "Protocol envelope bytes for this message.",
      envelopeSpan[0],
      envelopeSpan[1]
    )
  }

  fillEnvelopeGaps(subfields, envelopeSpan)
  return subfields.sort((left, right) => left.start - right.start || left.end - right.end)
}

function fillEnvelopeGaps(subfields, envelopeSpan) {
  const covered = new Array(envelopeSpan[1] - envelopeSpan[0]).fill(false)
  for (const subfield of subfields) {
    for (let index = subfield.start; index < subfield.end; index += 1) {
      if (index >= envelopeSpan[0] && index < envelopeSpan[1]) {
        covered[index - envelopeSpan[0]] = true
      }
    }
  }

  let gapStart = null
  for (let offset = 0; offset < covered.length; offset += 1) {
    const isCovered = covered[offset]
    if (!isCovered && gapStart === null) {
      gapStart = envelopeSpan[0] + offset
    }

    if ((isCovered || offset === covered.length - 1) && gapStart !== null) {
      const gapEnd = isCovered ? envelopeSpan[0] + offset : envelopeSpan[0] + offset + 1
      subfields.push({
        id: `envelope.unknown.${gapStart}-${gapEnd}`,
        label: "Envelope continuation bytes",
        description: "Additional envelope bytes that keep the envelope fully covered for educational inspection.",
        start: gapStart,
        end: gapEnd
      })
      gapStart = null
    }
  }
}

function decodeCompactVarint(bytes, start, limit) {
  let value = 0
  let shift = 0
  let cursor = start
  const maxBytes = 5

  while (cursor < limit && cursor - start < maxBytes) {
    const byte = bytes[cursor]
    value |= (byte & 0x7f) << shift
    cursor += 1
    if ((byte & 0x80) === 0) {
      return { length: cursor - start, value }
    }
    shift += 7
  }

  return { length: Math.max(0, cursor - start), value: 0 }
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
  beginErrorCycle()
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

  if (manifest.combos.length < 1 || manifest.combos.length > COMBO_IDS.length) {
    throw invalidManifest(`combos length must be between 1 and ${COMBO_IDS.length}.`)
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

  if (runtime.class === "blocking") {
    state.blockingError = runtime
    emitRuntimeError(runtime)
  } else {
    setNavWarning(runtime)
  }

  render()
}

function setNavWarning(runtime) {
  state.navWarning = runtime
  emitRuntimeError(runtime)
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

function beginErrorCycle() {
  state.errorCycle += 1
}

function initObservability() {
  window.__lastError = null
  window.__errors = []
  window.__metrics = {
    manifest_load_ms: 0,
    dataset_load_ms: 0,
    render_ms: 0,
    ui_error_total: {}
  }
}

function emitRuntimeError(runtime) {
  const key = `${runtime.code}|${runtime.context.reason}|${runtime.context.combo}|${runtime.context.msg}`
  if (state.lastErrorKey === key && state.lastErrorCycle === state.errorCycle) {
    return
  }

  state.lastErrorKey = key
  state.lastErrorCycle = state.errorCycle
  window.__lastError = runtime

  if (!Array.isArray(window.__errors)) {
    window.__errors = []
  }
  window.__errors.push(runtime)
  if (window.__errors.length > 100) {
    window.__errors.splice(0, window.__errors.length - 100)
  }

  if (!window.__metrics || typeof window.__metrics !== "object") {
    initObservability()
  }
  if (!window.__metrics.ui_error_total || typeof window.__metrics.ui_error_total !== "object") {
    window.__metrics.ui_error_total = {}
  }
  if (!(runtime.code in window.__metrics.ui_error_total)) {
    window.__metrics.ui_error_total[runtime.code] = 0
  }
  window.__metrics.ui_error_total[runtime.code] += 1

  if (runtime.class === "blocking") {
    console.error(runtime)
  } else {
    console.warn(runtime)
  }
}

function recordMetric(name, value) {
  if (!window.__metrics || typeof window.__metrics !== "object") {
    initObservability()
  }
  window.__metrics[name] = value
  console.info({ metric: name, value })
}

function roundMetric(value) {
  return Number(value.toFixed(2))
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

function byteAt(bytes, index) {
  if (index < 0 || index >= bytes.length) return null
  return bytes[index]
}

function hexByte(value) {
  return `0x${value.toString(16).padStart(2, "0")}`
}

function hexWord(value) {
  return `0x${value.toString(16).padStart(4, "0")}`
}

function binaryMessageTypeLabel(value) {
  switch (value) {
    case 1:
      return "call"
    case 2:
      return "reply"
    case 3:
      return "exception"
    case 4:
      return "oneway"
    default:
      return "unknown type"
  }
}

function binaryTypeName(typeTag) {
  const map = {
    0: "stop",
    1: "void",
    2: "bool",
    3: "byte",
    4: "double",
    6: "i16",
    8: "i32",
    10: "i64",
    11: "string/binary",
    12: "struct",
    13: "map",
    14: "set",
    15: "list",
    16: "uuid"
  }
  return map[typeTag] || "unknown"
}

function compactTypeName(typeNibble) {
  const map = {
    0: "stop",
    1: "bool(true)",
    2: "bool(false)",
    3: "byte",
    4: "i16",
    5: "i32",
    6: "i64",
    7: "double",
    8: "binary/string",
    9: "list",
    10: "set",
    11: "map",
    12: "struct"
  }
  return map[typeNibble] || "unknown"
}

function binaryValueHint(ttype) {
  switch (ttype) {
    case "bool":
    case "byte":
      return `Binary ${ttype} value (1 byte).`
    case "i16":
      return "Binary i16 value (2-byte big-endian signed integer)."
    case "i32":
      return "Binary i32 value (4-byte big-endian signed integer)."
    case "i64":
      return "Binary i64 value (8-byte big-endian signed integer)."
    case "double":
      return "Binary double value (8-byte IEEE-754 big-endian)."
    case "string":
      return "Binary string value (4-byte length prefix followed by UTF-8 bytes)."
    case "struct":
    case "map":
    case "set":
    case "list":
      return `Binary ${ttype} value bytes (nested container encoding).`
    default:
      return `Binary ${ttype} value bytes.`
  }
}

function compactValueHint(ttype) {
  switch (ttype) {
    case "i16":
    case "i32":
    case "i64":
      return `Compact ${ttype} value bytes (zigzag varint).`
    case "bool":
      return "Compact boolean struct fields encode true/false directly in the field-header type nibble."
    case "string":
      return "Compact string value bytes (length varint followed by UTF-8 bytes)."
    case "double":
      return "Compact double value bytes (fixed 8-byte IEEE-754 little-endian payload)."
    case "struct":
    case "map":
    case "set":
    case "list":
      return `Compact ${ttype} value bytes (nested container encoding).`
    default:
      return `Compact ${ttype} value bytes.`
  }
}

function decodeCompactZigZag(value) {
  return (value >>> 1) ^ -(value & 1)
}

function readInt16BE(bytes, start) {
  if (start + 1 >= bytes.length) return 0
  const value = ((bytes[start] << 8) | bytes[start + 1]) & 0xffff
  return value & 0x8000 ? value - 0x10000 : value
}

function readInt32BE(bytes, start) {
  if (start + 3 >= bytes.length) return 0
  const value = (
    (bytes[start] << 24) |
    (bytes[start + 1] << 16) |
    (bytes[start + 2] << 8) |
    bytes[start + 3]
  )
  return value | 0
}

function utf8ByteLength(value) {
  return new TextEncoder().encode(value).length
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max)
}

function clampSpan(span, upperBound) {
  if (!Array.isArray(span) || span.length !== 2) return [0, 0]
  const start = clamp(Number(span[0]) || 0, 0, upperBound)
  const end = clamp(Number(span[1]) || 0, 0, upperBound)
  if (end <= start) return [0, 0]
  return [start, end]
}

function rangeIndices(start, end) {
  const indices = []
  for (let value = start; value < end; value += 1) {
    indices.push(value)
  }
  return indices
}

function spanToString(span) {
  if (!Array.isArray(span) || span.length !== 2) return "-"
  return `[${span[0]}, ${span[1]})`
}
