#!/usr/bin/env bun

import { spawnSync } from "node:child_process"
import { constants as fsConstants, promises as fs } from "node:fs"
import process from "node:process"

const DEFAULT_TIMEOUT_MS = 5000

async function main() {
  const options = parseArgs(process.argv.slice(2))
  if (!options.url) {
    throw new Error("--url is required")
  }

  const { chromium } = await loadPlaywright()
  const chromePath = await resolveChromeBinary(options.chromeBin || process.env.CHROME_BIN || null)
  const browser = await chromium.launch({ executablePath: chromePath, headless: true })

  try {
    const page = await browser.newPage()
    await page.goto(options.url, { waitUntil: "domcontentloaded", timeout: options.timeoutMs })
    await page.waitForSelector('#summary-card[data-render-complete="true"]', { timeout: options.timeoutMs })

    const details = await page.evaluate(() => {
      const runtimeMessage = document.querySelector("#runtime-message")
      const blockingText = runtimeMessage && !runtimeMessage.hidden && runtimeMessage.dataset.level === "error"
        ? runtimeMessage.textContent
        : null
      const error = window.__lastError || null

      return {
        blocking_text: blockingText,
        last_error: error,
        render_complete: Boolean(document.querySelector('#summary-card[data-render-complete="true"]'))
      }
    })

    if (!details.render_complete) {
      throw new Error("render marker was not set to data-render-complete=true")
    }
    if (details.blocking_text) {
      throw new Error(`blocking runtime message: ${details.blocking_text}`)
    }
    if (details.last_error && details.last_error.class === "blocking") {
      throw new Error(`blocking runtime error object: ${details.last_error.code}`)
    }

    console.log(JSON.stringify({
      ok: true,
      checked_url: options.url,
      details
    }))
  } finally {
    await browser.close()
  }
}

function parseArgs(argv) {
  const options = {
    url: null,
    timeoutMs: DEFAULT_TIMEOUT_MS,
    chromeBin: null
  }

  for (let idx = 0; idx < argv.length; idx += 1) {
    const current = argv[idx]
    if (current === "--url") {
      options.url = requireValue(argv, ++idx, current)
      continue
    }
    if (current === "--timeout-ms") {
      options.timeoutMs = parsePositiveInt(requireValue(argv, ++idx, current), current)
      continue
    }
    if (current === "--chrome-bin") {
      options.chromeBin = requireValue(argv, ++idx, current)
      continue
    }
    if (current === "--help" || current === "-h") {
      printUsage()
      process.exit(0)
    }
    throw new Error(`Unknown argument: ${current}`)
  }

  return options
}

function printUsage() {
  console.log("Usage: bun run scripts/health_check_browser.mjs --url <absolute-url> [--timeout-ms 5000] [--chrome-bin <path>]")
}

function requireValue(argv, index, flag) {
  const value = argv[index]
  if (!value) throw new Error(`Missing value for ${flag}`)
  return value
}

function parsePositiveInt(value, flag) {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${flag} must be a positive integer`)
  }
  return parsed
}

async function loadPlaywright() {
  try {
    return await import("playwright-core")
  } catch {
    throw new Error("Missing dependency playwright-core. Run `bun install` before running browser health checks.")
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
    if (resolved) return resolved
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
  if (result.status !== 0) return null
  const resolved = result.stdout.trim()
  return resolved.length > 0 ? resolved : null
}

main().catch((error) => {
  console.error(`health_check_browser failed: ${error.message}`)
  process.exit(1)
})
