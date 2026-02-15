#!/usr/bin/env bun

import { spawnSync } from "node:child_process"
import { createServer } from "node:http"
import { promises as fs, constants as fsConstants } from "node:fs"
import path from "node:path"
import process from "node:process"
import { performance } from "node:perf_hooks"

const DEFAULT_OUTPUT = path.join("tmp", "benchmarks", "latest.json")
const DEFAULT_WARMUP_RUNS = 2
const DEFAULT_MEASURED_RUNS = 10
const NAVIGATION_TIMEOUT_MS = 5000

const CAPTURE_P95_LIMIT_MS = 10_000
const CAPTURE_MAX_LIMIT_MS = 15_000
const COMBO_SWITCH_MEDIAN_LIMIT_MS = 100
const COMBO_SWITCH_P95_LIMIT_MS = 200
const P95_TOLERANCE_FACTOR = 1.10
const ASSET_BUDGET_LIMIT_BYTES = 200 * 1024
const COMBO_ARTIFACT_LIMIT_BYTES = 2 * 1024 * 1024
const HEAP_LIMIT_BYTES = 50 * 1024 * 1024

const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".ico": "image/x-icon",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".md": "text/plain; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml"
}

async function main() {
  const options = parseArgs(process.argv.slice(2))
  const repoRoot = process.cwd()
  const outputPath = path.resolve(repoRoot, options.outputPath)
  const manifestPath = path.join(repoRoot, "data", "captures", "manifest.json")
  const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8"))
  const comboIds = Array.isArray(manifest.combos) ? manifest.combos.map((combo) => combo.id) : []

  if (comboIds.length < 2) {
    throw new Error("Benchmark requires at least two combos in data/captures/manifest.json.")
  }

  const chromePath = await resolveChromeBinary(options.chromeBin)
  let baseUrl = options.baseUrl
  let server = null

  if (!baseUrl) {
    const started = await startStaticServer(repoRoot)
    server = started.server
    baseUrl = started.baseUrl
  }

  try {
    const captureDistribution = await benchmarkCapture({
      warmupRuns: options.warmupRuns,
      measuredRuns: options.measuredRuns,
      repoRoot
    })

    const assetBudgetBytes = await measureAssetBudget({ repoRoot })
    const comboArtifactBytes = await measureComboArtifactBudget({
      repoRoot,
      comboEntries: manifest.combos
    })

    const uiBenchmark = await benchmarkComboSwitch({
      baseUrl,
      chromePath,
      warmupRuns: options.warmupRuns,
      measuredRuns: options.measuredRuns,
      comboIds
    })

    const report = {
      schema_version: "1.0.0",
      generated_at_utc: new Date().toISOString(),
      environment: {
        platform: `${process.platform}-${process.arch}`,
        runner: process.env.CI ? "ci" : "local",
        browser: {
          name: "chromium",
          major: uiBenchmark.browserMajor
        }
      },
      metrics: {
        capture_all_combos_ms: captureDistribution,
        combo_switch_cached_ms: uiBenchmark.distribution,
        asset_budget_bytes: assetBudgetBytes,
        combo_artifact_bytes: comboArtifactBytes,
        heap_after_combo_load: uiBenchmark.heap
      }
    }

    await fs.mkdir(path.dirname(outputPath), { recursive: true })
    await fs.writeFile(outputPath, `${JSON.stringify(report, null, 2)}\n`, "utf8")
    printSummary(report, outputPath)

    const failures = evaluateSla(report)
    if (failures.length > 0) {
      for (const failure of failures) {
        console.error(`SLA_FAIL: ${failure}`)
      }
      process.exitCode = 2
    }
  } finally {
    if (server) {
      await stopServer(server)
    }
  }
}

function parseArgs(argv) {
  const options = {
    baseUrl: null,
    chromeBin: process.env.CHROME_BIN || null,
    outputPath: DEFAULT_OUTPUT,
    warmupRuns: DEFAULT_WARMUP_RUNS,
    measuredRuns: DEFAULT_MEASURED_RUNS
  }

  for (let idx = 0; idx < argv.length; idx += 1) {
    const current = argv[idx]
    if (current === "--help" || current === "-h") {
      printUsage()
      process.exit(0)
    }
    if (current === "--base-url") {
      options.baseUrl = requireArgValue(argv, ++idx, current)
      continue
    }
    if (current === "--chrome-bin") {
      options.chromeBin = requireArgValue(argv, ++idx, current)
      continue
    }
    if (current === "--output") {
      options.outputPath = requireArgValue(argv, ++idx, current)
      continue
    }
    if (current === "--warmup-runs") {
      options.warmupRuns = parsePositiveInt(requireArgValue(argv, ++idx, current), current)
      continue
    }
    if (current === "--runs") {
      options.measuredRuns = parsePositiveInt(requireArgValue(argv, ++idx, current), current)
      continue
    }
    throw new Error(`Unknown argument: ${current}`)
  }

  return options
}

function printUsage() {
  console.log("Usage: bun run scripts/benchmark_ui.mjs [--output PATH] [--base-url URL] [--chrome-bin PATH] [--warmup-runs N] [--runs N]")
}

function requireArgValue(argv, index, flag) {
  const value = argv[index]
  if (!value) {
    throw new Error(`Missing value for ${flag}`)
  }
  return value
}

function parsePositiveInt(value, flag) {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${flag} must be a non-negative integer`)
  }
  return parsed
}

async function benchmarkCapture({ warmupRuns, measuredRuns, repoRoot }) {
  const samples = []
  const totalRuns = warmupRuns + measuredRuns

  for (let index = 0; index < totalRuns; index += 1) {
    const outputDir = path.join(repoRoot, "tmp", "benchmark-captures", `run-${index}`)
    await fs.rm(outputDir, { recursive: true, force: true })
    await fs.mkdir(outputDir, { recursive: true })

    const startedAt = performance.now()
    const result = executeCaptureRun({ outputDir, repoRoot })
    const elapsedMs = performance.now() - startedAt

    if (result.status !== 0) {
      throw new Error(
        `Capture benchmark run failed (status ${result.status}).\nSTDOUT:\n${result.stdout}\nSTDERR:\n${result.stderr}`
      )
    }

    if (index >= warmupRuns) {
      samples.push(elapsedMs)
    }
  }

  return summarizeDistribution(samples, warmupRuns)
}

function executeCaptureRun({ outputDir, repoRoot }) {
  const env = { ...process.env, TZ: "UTC", LC_ALL: "C" }
  const miseResult = spawnSync(
    "mise",
    ["exec", "ruby@4.0.1", "--", "bundle", "exec", "ruby", "scripts/capture_all.rb", "--output", outputDir],
    {
      cwd: repoRoot,
      encoding: "utf8",
      env
    }
  )

  if (!miseResult.error) {
    return miseResult
  }

  if (miseResult.error.code !== "ENOENT") {
    return miseResult
  }

  return spawnSync(
    "bundle",
    ["exec", "ruby", "scripts/capture_all.rb", "--output", outputDir],
    {
      cwd: repoRoot,
      encoding: "utf8",
      env
    }
  )
}

async function measureAssetBudget({ repoRoot }) {
  const bytes = await directoryBytes(path.join(repoRoot, "site"))
  return {
    observed: bytes,
    limit: ASSET_BUDGET_LIMIT_BYTES,
    unit: "bytes"
  }
}

async function measureComboArtifactBudget({ repoRoot, comboEntries }) {
  let observed = 0
  for (const combo of comboEntries) {
    const filePath = path.join(repoRoot, "data", "captures", combo.file)
    const stats = await fs.stat(filePath)
    observed = Math.max(observed, stats.size)
  }

  return {
    observed,
    limit: COMBO_ARTIFACT_LIMIT_BYTES,
    unit: "bytes"
  }
}

async function benchmarkComboSwitch({ baseUrl, chromePath, warmupRuns, measuredRuns, comboIds }) {
  const { chromium } = await loadPlaywright()
  const browser = await chromium.launch({
    executablePath: chromePath,
    headless: true,
    args: ["--enable-precise-memory-info"]
  })

  try {
    const context = await browser.newContext()
    const page = await context.newPage()
    const firstCombo = comboIds[0]
    await page.goto(`${baseUrl}/site/#combo=${encodeURIComponent(firstCombo)}&msg=0`, {
      waitUntil: "domcontentloaded",
      timeout: NAVIGATION_TIMEOUT_MS
    })
    await page.waitForSelector('#summary-card[data-render-complete="true"]', {
      timeout: NAVIGATION_TIMEOUT_MS
    })

    for (const comboId of comboIds) {
      await switchCombo(page, comboId)
    }

    const samples = []
    let pointer = comboIds.length - 1
    const totalRuns = warmupRuns + measuredRuns
    for (let index = 0; index < totalRuns; index += 1) {
      pointer = (pointer + 1) % comboIds.length
      const comboId = comboIds[pointer]
      const elapsedMs = await switchCombo(page, comboId)
      if (index >= warmupRuns) {
        samples.push(elapsedMs)
      }
    }

    const heap = await page.evaluate(() => {
      if (!performance.memory || typeof performance.memory.usedJSHeapSize !== "number") {
        return { status: "unavailable", used_js_heap_size: null }
      }
      return { status: "available", used_js_heap_size: Math.round(performance.memory.usedJSHeapSize) }
    })

    return {
      browserMajor: extractBrowserMajor(browser.version()),
      distribution: summarizeDistribution(samples, warmupRuns),
      heap: {
        status: heap.status,
        used_js_heap_size: heap.used_js_heap_size,
        limit: HEAP_LIMIT_BYTES
      }
    }
  } finally {
    await browser.close()
  }
}

async function switchCombo(page, comboId) {
  const currentCombo = await page.evaluate(() => new URLSearchParams(window.location.hash.slice(1)).get("combo"))
  if (currentCombo === comboId) {
    return 0
  }

  const previousCycle = await page.$eval("#summary-card", (element) => Number.parseInt(element.dataset.renderCycle || "0", 10))
  const startedAt = performance.now()
  await page.evaluate((nextCombo) => {
    const params = new URLSearchParams(window.location.hash.slice(1))
    params.set("combo", nextCombo)
    params.set("msg", "0")
    window.location.hash = `#${params.toString()}`
  }, comboId)
  await page.waitForFunction(
    (lastCycle) => {
      const element = document.querySelector("#summary-card")
      if (!element) return false
      const currentCycle = Number.parseInt(element.dataset.renderCycle || "0", 10)
      return element.dataset.renderComplete === "true" && currentCycle > lastCycle
    },
    previousCycle,
    { timeout: NAVIGATION_TIMEOUT_MS }
  )
  const blockingMessage = await page.evaluate(() => {
    const runtimeMessage = document.querySelector("#runtime-message")
    if (!runtimeMessage || runtimeMessage.hidden || runtimeMessage.dataset.level !== "error") {
      return null
    }
    return runtimeMessage.textContent || null
  })
  if (blockingMessage) {
    throw new Error(`UI benchmark encountered blocking runtime error after switching to ${comboId}: ${blockingMessage}`)
  }
  return performance.now() - startedAt
}

function summarizeDistribution(values, warmupRuns) {
  if (values.length === 0) {
    return {
      runs: 0,
      warmup_runs: warmupRuns,
      median_ms: 0,
      p95_ms: 0,
      max_ms: 0
    }
  }

  const sorted = [...values].sort((left, right) => left - right)
  return {
    runs: values.length,
    warmup_runs: warmupRuns,
    median_ms: roundMetric(percentile(sorted, 0.5)),
    p95_ms: roundMetric(percentile(sorted, 0.95)),
    max_ms: roundMetric(sorted[sorted.length - 1])
  }
}

function percentile(sortedValues, quantile) {
  const index = Math.max(0, Math.ceil(quantile * sortedValues.length) - 1)
  return sortedValues[index]
}

function roundMetric(value) {
  return Math.round(value * 1000) / 1000
}

async function directoryBytes(rootPath) {
  const queue = [rootPath]
  let total = 0
  while (queue.length > 0) {
    const current = queue.pop()
    const entries = await fs.readdir(current, { withFileTypes: true })
    for (const entry of entries) {
      const entryPath = path.join(current, entry.name)
      if (entry.isDirectory()) {
        queue.push(entryPath)
        continue
      }
      if (entry.isFile()) {
        const stat = await fs.stat(entryPath)
        total += stat.size
      }
    }
  }
  return total
}

function evaluateSla(report) {
  const failures = []
  const capture = report.metrics.capture_all_combos_ms
  const comboSwitch = report.metrics.combo_switch_cached_ms
  const assets = report.metrics.asset_budget_bytes
  const artifact = report.metrics.combo_artifact_bytes
  const heap = report.metrics.heap_after_combo_load

  if (capture.p95_ms > CAPTURE_P95_LIMIT_MS * P95_TOLERANCE_FACTOR) {
    failures.push(
      `capture_all_combos_ms p95 ${capture.p95_ms}ms exceeded allowed ceiling ${CAPTURE_P95_LIMIT_MS * P95_TOLERANCE_FACTOR}ms`
    )
  }
  if (capture.max_ms > CAPTURE_MAX_LIMIT_MS) {
    failures.push(`capture_all_combos_ms max ${capture.max_ms}ms exceeded ${CAPTURE_MAX_LIMIT_MS}ms`)
  }
  if (comboSwitch.median_ms > COMBO_SWITCH_MEDIAN_LIMIT_MS) {
    failures.push(`combo_switch_cached_ms median ${comboSwitch.median_ms}ms exceeded ${COMBO_SWITCH_MEDIAN_LIMIT_MS}ms`)
  }
  if (comboSwitch.p95_ms > COMBO_SWITCH_P95_LIMIT_MS * P95_TOLERANCE_FACTOR) {
    failures.push(
      `combo_switch_cached_ms p95 ${comboSwitch.p95_ms}ms exceeded allowed ceiling ${COMBO_SWITCH_P95_LIMIT_MS * P95_TOLERANCE_FACTOR}ms`
    )
  }
  if (assets.observed > assets.limit) {
    failures.push(`asset_budget_bytes observed ${assets.observed} exceeded ${assets.limit}`)
  }
  if (artifact.observed > artifact.limit) {
    failures.push(`combo_artifact_bytes observed ${artifact.observed} exceeded ${artifact.limit}`)
  }
  if (heap.status === "available" && heap.used_js_heap_size > heap.limit) {
    failures.push(`heap_after_combo_load observed ${heap.used_js_heap_size} exceeded ${heap.limit}`)
  }

  return failures
}

function printSummary(report, outputPath) {
  const capture = report.metrics.capture_all_combos_ms
  const comboSwitch = report.metrics.combo_switch_cached_ms
  console.log(`Benchmark report written to ${outputPath}`)
  console.log(
    `capture_all_combos_ms: median=${capture.median_ms} p95=${capture.p95_ms} max=${capture.max_ms} runs=${capture.runs}`
  )
  console.log(
    `combo_switch_cached_ms: median=${comboSwitch.median_ms} p95=${comboSwitch.p95_ms} max=${comboSwitch.max_ms} runs=${comboSwitch.runs}`
  )
  console.log(
    `asset_budget_bytes: ${report.metrics.asset_budget_bytes.observed}/${report.metrics.asset_budget_bytes.limit}`
  )
  console.log(
    `combo_artifact_bytes: ${report.metrics.combo_artifact_bytes.observed}/${report.metrics.combo_artifact_bytes.limit}`
  )
  if (report.metrics.heap_after_combo_load.status === "available") {
    console.log(
      `heap_after_combo_load: ${report.metrics.heap_after_combo_load.used_js_heap_size}/${report.metrics.heap_after_combo_load.limit}`
    )
  } else {
    console.log("heap_after_combo_load: unavailable")
  }
}

async function loadPlaywright() {
  try {
    return await import("playwright-core")
  } catch {
    throw new Error("Missing dependency playwright-core. Run `bun install` before running benchmark_ui.mjs.")
  }
}

async function resolveChromeBinary(explicitPath) {
  if (explicitPath) {
    await assertExecutable(explicitPath)
    return explicitPath
  }

  const candidates = [
    "chromium-browser",
    "chromium",
    "google-chrome",
    "google-chrome-stable",
    "chrome",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium"
  ]

  for (const candidate of candidates) {
    const resolved = await commandPath(candidate)
    if (resolved) {
      return resolved
    }
  }

  throw new Error("Chrome/Chromium binary not found. Set CHROME_BIN to a valid executable path.")
}

async function assertExecutable(filePath) {
  try {
    await fs.access(filePath, fsConstants.X_OK)
  } catch {
    throw new Error(`CHROME_BIN is not executable: ${filePath}`)
  }
}

async function commandPath(command) {
  if (command.startsWith("/")) {
    try {
      await fs.access(command, fsConstants.X_OK)
      return command
    } catch {
      return null
    }
  }
  const result = spawnSync("which", [command], { encoding: "utf8" })
  if (result.status !== 0) {
    return null
  }
  const resolved = result.stdout.trim()
  return resolved.length > 0 ? resolved : null
}

function extractBrowserMajor(versionString) {
  const match = versionString.match(/(\d+)\./)
  if (!match) {
    return 0
  }
  return Number.parseInt(match[1], 10)
}

async function startStaticServer(rootDir) {
  const absoluteRoot = path.resolve(rootDir)
  const server = createServer(async (request, response) => {
    if (!request.url) {
      response.writeHead(400)
      response.end("Bad Request")
      return
    }

    try {
      const parsed = new URL(request.url, "http://localhost")
      const filePath = await resolveRequestPath(absoluteRoot, parsed.pathname)
      const bytes = await fs.readFile(filePath)
      const ext = path.extname(filePath).toLowerCase()
      response.writeHead(200, { "Content-Type": MIME_TYPES[ext] || "application/octet-stream" })
      response.end(bytes)
    } catch (error) {
      if (error.code === "E_NOT_FOUND") {
        response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" })
        response.end("Not Found")
        return
      }
      response.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" })
      response.end(`Internal Server Error: ${error.message}`)
    }
  })

  await new Promise((resolve) => {
    server.listen(0, "127.0.0.1", resolve)
  })

  const address = server.address()
  const port = typeof address === "object" && address ? address.port : 0
  return {
    server,
    baseUrl: `http://127.0.0.1:${port}`
  }
}

async function stopServer(server) {
  await new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error)
        return
      }
      resolve()
    })
  })
}

async function resolveRequestPath(rootDir, pathname) {
  let decodedPath
  try {
    decodedPath = decodeURIComponent(pathname)
  } catch {
    const error = new Error("Invalid path encoding")
    error.code = "E_NOT_FOUND"
    throw error
  }

  let relativePath = decodedPath
  if (relativePath.endsWith("/")) {
    relativePath = `${relativePath}index.html`
  }

  const candidate = path.resolve(rootDir, `.${relativePath}`)
  if (!candidate.startsWith(rootDir)) {
    const error = new Error("Path traversal")
    error.code = "E_NOT_FOUND"
    throw error
  }

  try {
    const stats = await fs.stat(candidate)
    if (stats.isDirectory()) {
      const withIndex = path.join(candidate, "index.html")
      await fs.access(withIndex)
      return withIndex
    }
    if (stats.isFile()) {
      return candidate
    }
  } catch {
    // handled below
  }

  const error = new Error("Not found")
  error.code = "E_NOT_FOUND"
  throw error
}

main().catch((error) => {
  console.error(`benchmark_ui failed: ${error.message}`)
  process.exit(1)
})
