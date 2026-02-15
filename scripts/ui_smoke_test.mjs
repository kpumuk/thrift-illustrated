#!/usr/bin/env bun

import { spawnSync } from "node:child_process"
import { createServer } from "node:http"
import { constants as fsConstants, promises as fs } from "node:fs"
import path from "node:path"
import process from "node:process"

const NAV_TIMEOUT_MS = 10_000
const MANIFEST_MAX_BYTES = 256 * 1024
const DATASET_MAX_BYTES = 2 * 1024 * 1024

const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".ico": "image/x-icon",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml"
}

async function main() {
  const repoRoot = process.cwd()
  const siteRoot = path.join(repoRoot, "site")
  const manifestPath = path.join(siteRoot, "data", "captures", "manifest.json")
  const manifest = JSON.parse(await fs.readFile(manifestPath, "utf8"))
  if (!Array.isArray(manifest.combos) || manifest.combos.length === 0) {
    throw new Error("Manifest does not contain combos for UI smoke tests.")
  }
  const defaultCombo = manifest.combos[0].id
  const chromePath = await resolveChromeBinary(process.env.CHROME_BIN || null)
  const { chromium } = await loadPlaywright()
  const browser = await chromium.launch({ executablePath: chromePath, headless: true })

  const scenarios = [
    {
      name: "happy-path",
      hash: `#combo=${encodeURIComponent(defaultCombo)}&msg=0`,
      mode: "normal",
      expectedCode: null
    },
    {
      name: "invalid-hash-recovery",
      hash: "#combo=definitely-invalid&msg=999999",
      mode: "normal",
      expectedCode: "E_NAV_HASH_INVALID",
      expectedClass: "non-blocking",
      postCheck: async (page) => {
        const locationHash = await page.evaluate(() => window.location.hash)
        if (locationHash.includes("definitely-invalid")) {
          throw new Error("Hash recovery did not replace invalid combo value.")
        }
        if (!locationHash.includes("msg=0")) {
          throw new Error("Hash recovery did not reset message index to 0.")
        }
      }
    },
    {
      name: "manifest-fetch-failure",
      hash: "",
      mode: "manifest-fetch-failure",
      expectedCode: "E_MANIFEST_FETCH_FAILED",
      expectedClass: "blocking"
    },
    {
      name: "manifest-too-large",
      hash: "",
      mode: "manifest-too-large",
      expectedCode: "E_MANIFEST_TOO_LARGE",
      expectedClass: "blocking"
    },
    {
      name: "dataset-too-large",
      hash: "",
      mode: "dataset-too-large",
      expectedCode: "E_DATASET_TOO_LARGE",
      expectedClass: "blocking"
    },
    {
      name: "manifest-schema-version-unsupported",
      hash: "",
      mode: "manifest-schema-version-unsupported",
      expectedCode: "E_SCHEMA_VERSION_UNSUPPORTED",
      expectedClass: "blocking"
    },
    {
      name: "dataset-hash-mismatch",
      hash: "",
      mode: "dataset-hash-mismatch",
      expectedCode: "E_DATASET_HASH_MISMATCH",
      expectedClass: "blocking"
    }
  ]

  const failures = []
  try {
    for (const scenario of scenarios) {
      try {
        await runScenario({ browser, siteRoot, scenario })
        console.log(`PASS ${scenario.name}`)
      } catch (error) {
        failures.push({ scenario: scenario.name, message: error.message })
        console.error(`FAIL ${scenario.name}: ${error.message}`)
      }
    }
  } finally {
    await browser.close()
  }

  if (failures.length > 0) {
    process.exitCode = 1
  }
}

async function runScenario({ browser, siteRoot, scenario }) {
  const serverState = await startScenarioServer({ siteRoot, mode: scenario.mode })
  const context = await browser.newContext()
  const page = await context.newPage()
  const url = `${serverState.baseUrl}/${scenario.hash || ""}`

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: NAV_TIMEOUT_MS })
    if (scenario.expectedCode) {
      await page.waitForFunction(
        (expectedCode) => window.__lastError && window.__lastError.code === expectedCode,
        scenario.expectedCode,
        { timeout: NAV_TIMEOUT_MS }
      )

      const errorObject = await page.evaluate(() => window.__lastError)
      if (!errorObject) {
        throw new Error("Expected runtime error object was not emitted.")
      }
      if (scenario.expectedClass && errorObject.class !== scenario.expectedClass) {
        throw new Error("Runtime error class mismatch.")
      }
      if (typeof scenario.postCheck === "function") {
        await scenario.postCheck(page)
      }
      return
    }

    await page.waitForSelector('#summary-card[data-render-complete="true"]', { timeout: NAV_TIMEOUT_MS })
    const blockingMessage = await page.evaluate(() => {
      const runtimeMessage = document.querySelector("#runtime-message")
      if (!runtimeMessage || runtimeMessage.hidden || runtimeMessage.dataset.level !== "error") {
        return null
      }
      return runtimeMessage.textContent || null
    })
    if (blockingMessage) {
      throw new Error(`Unexpected blocking runtime error: ${blockingMessage}`)
    }
  } finally {
    await context.close()
    await stopServer(serverState.server)
  }
}

async function startScenarioServer({ siteRoot, mode }) {
  const absoluteRoot = path.resolve(siteRoot)
  const server = createServer(async (request, response) => {
    if (!request.url) {
      response.writeHead(400, { "Content-Type": "text/plain; charset=utf-8" })
      response.end("Bad Request")
      return
    }

    try {
      const parsed = new URL(request.url, "http://localhost")
      const pathname = parsed.pathname

      if (mode === "manifest-fetch-failure" && pathname === "/data/captures/manifest.json") {
        response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" })
        response.end("missing manifest")
        return
      }

      if (mode === "manifest-too-large" && pathname === "/data/captures/manifest.json") {
        response.writeHead(200, { "Content-Type": "application/json; charset=utf-8" })
        response.end(Buffer.alloc(MANIFEST_MAX_BYTES + 512, 0x20))
        return
      }

      if (mode === "dataset-too-large" && isDatasetJsonPath(pathname)) {
        response.writeHead(200, { "Content-Type": "application/json; charset=utf-8" })
        response.end(Buffer.alloc(DATASET_MAX_BYTES + 512, 0x20))
        return
      }

      const filePath = await resolveRequestPath(absoluteRoot, pathname)
      let body = await fs.readFile(filePath)

      if (mode === "manifest-schema-version-unsupported" && pathname === "/data/captures/manifest.json") {
        body = rewriteSchemaVersion(body, "9.0.0")
      }

      if (mode === "dataset-hash-mismatch" && isDatasetJsonPath(pathname)) {
        body = tamperDatasetBytes(body)
      }

      const extension = path.extname(filePath).toLowerCase()
      response.writeHead(200, { "Content-Type": MIME_TYPES[extension] || "application/octet-stream" })
      response.end(body)
    } catch (error) {
      if (error && error.code === "E_NOT_FOUND") {
        response.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" })
        response.end("Not Found")
        return
      }
      response.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" })
      response.end("Internal Server Error")
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

function isDatasetJsonPath(pathname) {
  return pathname.startsWith("/data/captures/") &&
    pathname.endsWith(".json") &&
    pathname !== "/data/captures/manifest.json"
}

function rewriteSchemaVersion(bytes, version) {
  const parsed = JSON.parse(bytes.toString("utf8"))
  parsed.schema_version = version
  return Buffer.from(JSON.stringify(parsed), "utf8")
}

function tamperDatasetBytes(bytes) {
  const source = bytes.toString("utf8")
  const marker = '"raw_hex": "'
  const markerOffset = source.indexOf(marker)
  if (markerOffset >= 0) {
    const valueOffset = markerOffset + marker.length
    const current = source[valueOffset]
    const replacement = current === "0" ? "1" : "0"
    const updated = `${source.slice(0, valueOffset)}${replacement}${source.slice(valueOffset + 1)}`
    return Buffer.from(updated, "utf8")
  }

  const genericOffset = source.search(/[A-Za-z]/)
  if (genericOffset < 0) {
    return bytes
  }
  const current = source[genericOffset]
  const replacement = current === "a" ? "b" : "a"
  const updated = `${source.slice(0, genericOffset)}${replacement}${source.slice(genericOffset + 1)}`
  return Buffer.from(updated, "utf8")
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
    // fall through
  }

  const error = new Error("Not found")
  error.code = "E_NOT_FOUND"
  throw error
}

async function loadPlaywright() {
  try {
    return await import("playwright-core")
  } catch {
    throw new Error("Missing dependency playwright-core. Run `bun install` before running ui_smoke_test.mjs.")
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

main().catch((error) => {
  console.error(`ui_smoke_test failed: ${error.message}`)
  process.exit(1)
})
