// src/worker.js
require("dotenv").config();

const { Worker } = require("bullmq");
const { chromium } = require("playwright");
const fs = require("fs-extra");
const crypto = require("crypto");
const path = require("path");

const Ajv = require("ajv");
const addFormats = require("ajv-formats");

const { checkProtocol } = require("./utils/checkProtocol");

// Queue imports
// Accept both legacy and new exports to minimize breakage
const queueMod = require("./queue");
const connection = queueMod.connection;
const queueName =
  queueMod.queueName ||
  queueMod.queueNameLegacy ||
  queueMod.queue ||
  queueMod.queueName; // keep tolerant
const inquiryQueueName = queueMod.inquiryQueueName || process.env.INQUIRY_QUEUE_NAME || "resolver-inquiry";
const inquiryQueue = queueMod.inquiryQueue || null;

const { upsertNode, insertCapsule, pool } = require("./db");
const { fp } = require("./normalize");
const { inferCapsule } = require("./inferencer");
const { classifyNodeType } = require("./utils/classifyNodeType");

// ✅ JSON-LD extractor (must export { extractJsonLd })
const { extractJsonLd } = require("./extractor/jsonld");

// ------------------------------
// Config
// ------------------------------
const CONCURRENCY = parseInt(process.env.CONCURRENCY || 4, 10);
const UA = process.env.USER_AGENT || "AgentNet-Capsulizer/1.0 (+https://agentnet.ai)";
const PER_HOST_DELAY = parseInt(process.env.PER_HOST_DELAY_MS || 500, 10);
const MAX_DEPTH = parseInt(process.env.MAX_DEPTH || 10, 10);
const MAX_PAGES_PER_SITE = parseInt(process.env.MAX_PAGES_PER_SITE || 10, 10);

const LOG_PATH = "./crawler.log";
const SNAPSHOT_DIR = "./snapshots";
const RUNS_DIR = "./runs";

// Optional flags (safe defaults)
const ENABLE_LLM = (process.env.ENABLE_LLM ?? "true").toLowerCase() === "true";
const WRITE_SNAPSHOTS = (process.env.WRITE_SNAPSHOTS ?? "true").toLowerCase() === "true";

// Demo-friendly flag: default to single-page mode
const SINGLE_PAGE = (process.env.SINGLE_PAGE ?? "true").toLowerCase() === "true";

// Deterministic mode (reproducible fingerprints)
const CG_DETERMINISTIC = (process.env.CG_DETERMINISTIC ?? "false").toLowerCase() === "true";

// CG version marker
const CG_VERSION = process.env.CG_VERSION || "cg-0.5-determinism-ajv-output";

// Envelope schema validation gate
const VALIDATE_ENVELOPE = (process.env.VALIDATE_ENVELOPE ?? "true").toLowerCase() === "true";

// Determinism: disable LLM
const EFFECTIVE_ENABLE_LLM = CG_DETERMINISTIC ? false : ENABLE_LLM;

// Inquiry enqueue toggle (default true if inquiryQueue is available)
const ENABLE_INQUIRY_ENQUEUE =
  (process.env.ENABLE_INQUIRY_ENQUEUE ?? "true").toLowerCase() === "true" && Boolean(inquiryQueue);

// Inquiry job naming
const INQUIRY_JOB_NAME = process.env.INQUIRY_JOB_NAME || "inquire";

// ------------------------------
// Enhancement #1: Boot logging + sanity checks
// ------------------------------
(function bootLog() {
  const safe = (v) => (v == null ? null : String(v));
  console.log("[WORKER] Boot config:");
  console.log("  pid:", process.pid);
  console.log("  cwd:", process.cwd());
  console.log("  queueName:", queueName);
  console.log("  inquiryQueueName:", inquiryQueueName);
  console.log("  ENABLE_INQUIRY_ENQUEUE:", ENABLE_INQUIRY_ENQUEUE);
  console.log("  Redis:", {
    host: safe(process.env.REDIS_HOST || "127.0.0.1"),
    port: safe(process.env.REDIS_PORT || "6379"),
    hasPassword: !!process.env.REDIS_PASSWORD,
  });
  console.log("  MySQL:", {
    host: safe(process.env.DB_HOST),
    port: safe(process.env.DB_PORT),
    name: safe(process.env.DB_NAME),
    user: safe(process.env.DB_USER),
    passSet: !!(process.env.DB_PASS || process.env.DB_PASSWORD),
  });
})();

process.on("unhandledRejection", (err) => {
  console.error("[WORKER] Unhandled rejection:", err);
});
process.on("uncaughtException", (err) => {
  console.error("[WORKER] Uncaught exception:", err);
});

// ------------------------------
// AJV schema validator (SYNC init - CommonJS safe)
// ------------------------------
let validateEnvelope = null;
let envelopeSchemaLoaded = false;

if (VALIDATE_ENVELOPE) {
  try {
    const schemaPath = path.resolve(__dirname, "../schemas/cg-envelope.schema.json");
    const exists = fs.existsSync(schemaPath);

    if (!exists) {
      console.warn(`⚠️  VALIDATE_ENVELOPE=true but schema file not found at ${schemaPath}. Validation disabled.`);
    } else {
      const raw = fs.readFileSync(schemaPath, "utf8");
      if (!raw || !raw.trim()) {
        console.warn(`⚠️  VALIDATE_ENVELOPE=true but schema file is empty at ${schemaPath}. Validation disabled.`);
      } else {
        const schemaJson = JSON.parse(raw);

        const ajv = new Ajv({
          allErrors: true,
          strict: false,
          allowUnionTypes: true,
        });

        try {
          addFormats(ajv);
        } catch (fmtErr) {
          console.warn(`⚠️  ajv-formats not available (formats ignored): ${fmtErr.message}`);
        }

        validateEnvelope = ajv.compile(schemaJson);
        envelopeSchemaLoaded = true;
        console.log(`✅ Envelope schema loaded: ${schemaPath}`);
      }
    }
  } catch (e) {
    console.warn(`⚠️  Failed to initialize AJV validator. Validation disabled. ${e.message}`);
    validateEnvelope = null;
  }
}

// ------------------------------
// Polite throttle per host
// ------------------------------
const lastHit = new Map();
async function hostThrottle(url) {
  const host = new URL(url).host;
  const now = Date.now();
  const last = lastHit.get(host) || 0;
  const wait = Math.max(0, PER_HOST_DELAY - (now - last));
  if (wait) await new Promise((r) => setTimeout(r, wait));
  lastHit.set(host, Date.now());
}

// ------------------------------
// Structured logging
// ------------------------------
async function appendLog(entry) {
  const line = `[${new Date().toISOString()}] ${JSON.stringify(entry)}\n`;
  await fs.appendFile(LOG_PATH, line);
}

// ------------------------------
// URL normalization
// ------------------------------
function normalizeUrl(raw) {
  const u = new URL(raw);
  u.hash = "";

  const dropPrefixes = ["utm_", "gclid", "fbclid", "msclkid", "a_ajs_"];
  for (const key of [...u.searchParams.keys()]) {
    if (dropPrefixes.some((p) => key === p || key.startsWith(p)) || key.startsWith("a_ajs_")) {
      u.searchParams.delete(key);
    }
  }
  return u.toString();
}

// ------------------------------
// Snapshot naming (avoid ENAMETOOLONG)
// ------------------------------
function snapshotName(url) {
  const u = new URL(url);
  const base = `${u.host}${u.pathname}`.replace(/[^a-zA-Z0-9/_-]/g, "_");
  const hash = crypto.createHash("sha256").update(url).digest("hex").slice(0, 16);
  const safeBase = base.replace(/\//g, "_").slice(0, 120);
  return `${safeBase}__${hash}.html`;
}

// ------------------------------
// Run ID
// ------------------------------
function makeRunId() {
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const nonce = crypto.randomBytes(16).toString("hex").slice(0, 12);
  return `run_${ts}__${nonce}`;
}

// ------------------------------
// Enhancement #2: Safe BullMQ jobId helpers
//   - BullMQ rejects ":" in custom jobId (as you observed)
// ------------------------------
function safeJobId(raw) {
  // allow only: letters, numbers, underscore, dash, dot
  // (keep it boring to avoid BullMQ/Redis edge cases)
  return String(raw)
    .replace(/[:/\\\s]/g, "_")
    .replace(/[^a-zA-Z0-9._-]/g, "_")
    .slice(0, 180);
}

function sha1Hex(str) {
  return crypto.createHash("sha1").update(str).digest("hex");
}

// ------------------------------
// Deterministic helpers
// ------------------------------
function stableSortJsonLd(value) {
  if (Array.isArray(value)) {
    return value
      .map(stableSortJsonLd)
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  }
  if (value && typeof value === "object") {
    return Object.keys(value)
      .sort()
      .reduce((acc, k) => {
        acc[k] = stableSortJsonLd(value[k]);
        return acc;
      }, {});
  }
  return value;
}

function selectPrimaryIndexDeterministic(blocks) {
  if (!Array.isArray(blocks) || blocks.length === 0) return { index: null, type: null };

  const scored = blocks.map((b, i) => ({
    i,
    s: JSON.stringify(b?.json ?? {}),
    t: b?.json ? b.json["@type"] : null,
  }));

  scored.sort((a, b) => a.s.localeCompare(b.s));
  const pick = scored[0];

  const type = Array.isArray(pick.t) ? pick.t[0] : pick.t;
  const cleanedType = typeof type === "string" ? type.replace(/^schema:/, "") : type;

  return { index: pick.i, type: cleanedType || null };
}

function stableFingerprintView(envelope) {
  const e = JSON.parse(JSON.stringify(envelope || {}));

  delete e["agentnet:captureDate"];
  delete e["agentnet:cgRunId"];
  delete e["agentnet:cgManifestPath"];

  const prov = e?.["agentnet:asserted"]?.provenance;
  if (prov && typeof prov === "object") {
    delete prov.capturedAt;
  }

  const asserted = e?.["agentnet:asserted"]?.json;
  if (Array.isArray(asserted)) {
    const keyed = asserted.map((obj) => stableSortJsonLd(obj));
    keyed.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    e["agentnet:asserted"].json = keyed;
  } else if (asserted && typeof asserted === "object") {
    e["agentnet:asserted"].json = stableSortJsonLd(asserted);
  }

  if (e["agentnet:content"] && typeof e["agentnet:content"] === "object") {
    e["agentnet:content"] = stableSortJsonLd(e["agentnet:content"]);
  }

  return e;
}

// ------------------------------
// Required tiny price guardrail
// ------------------------------
function guardTinyPrice(content, report) {
  const p = content?.["agentnet:price"];
  if (p == null) return;

  const n = typeof p === "string" ? Number(p) : Number(p);
  if (!Number.isFinite(n)) return;

  if (n > 0 && n < 5) {
    delete content["agentnet:price"];
    report.priceGuardrail = {
      dropped: true,
      reason: "tiny_price",
      threshold: 5,
      observed: n,
    };
  }
}

// ------------------------------
// Enhancement #3: Node reachability updates (best-effort)
//   - Updates nodes.* if those columns exist
//   - If schema lacks columns, we disable further attempts to avoid spam
// ------------------------------
let NODE_STATUS_UPDATES_DISABLED = false;

async function tryUpdateNodeStatus(nodeId, status) {
  if (!nodeId) return;
  if (NODE_STATUS_UPDATES_DISABLED) return;

  // status: { reachable, httpStatus, error, lastObservedAt, latencyMs, pageUrl }
  const reachable = status?.reachable ? 1 : 0;
  const httpStatus = status?.httpStatus ?? null;
  const err = status?.error ? String(status.error).slice(0, 240) : null;
  const observedAt = status?.lastObservedAt ? new Date(status.lastObservedAt) : new Date();
  const latencyMs = Number.isFinite(status?.latencyMs) ? status.latencyMs : null;

  // We intentionally do NOT require all columns; this will succeed only if columns exist.
  // If your schema.sql does not include these columns yet, we'll see ER_BAD_FIELD_ERROR once and then stop trying.
  const sql = `
    UPDATE nodes
    SET
      last_observed_at = ?,
      last_reachable = ?,
      last_http_status = ?,
      last_error = ?,
      last_latency_ms = ?,
      last_reachable_at = IF(?, ?, last_reachable_at),
      last_unreachable_at = IF(?, ?, last_unreachable_at)
    WHERE id = ?
  `;

  try {
    await pool.query(sql, [
      observedAt,
      reachable,
      httpStatus,
      err,
      latencyMs,
      reachable === 1,
      observedAt,
      reachable === 0,
      observedAt,
      nodeId,
    ]);
  } catch (e) {
    // If schema doesn't have these fields, stop attempting to update.
    const msg = String(e?.message || e);
    if (msg.includes("Unknown column") || msg.includes("ER_BAD_FIELD_ERROR")) {
      NODE_STATUS_UPDATES_DISABLED = true;
      console.warn("[WORKER] ⚠️ Node status columns not present on nodes table. Disabling node status updates.", msg);
      return;
    }
    console.warn("[WORKER] ⚠️ Failed to update node status (non-fatal):", msg);
  }
}

// ------------------------------
// Enhancement #4: Navigation w/ HTTP->HTTPS fallback + status classification
// ------------------------------
function classifyNavError(errMsg) {
  const m = String(errMsg || "").toLowerCase();

  if (m.includes("timeout") || m.includes("timed out")) return "timeout";
  if (m.includes("net::err_cert") || m.includes("certificate") || m.includes("ssl")) return "tls_error";
  if (m.includes("net::err_connection") || m.includes("couldn't connect") || m.includes("refused")) return "unreachable";
  if (m.includes("net::err_name_not_resolved") || m.includes("dns")) return "unreachable";

  return "unreachable";
}

async function gotoWithProtocolFallback(page, inputUrl) {
  const attempts = [];

  async function attempt(url) {
    const t0 = Date.now();
    try {
      const resp = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });
      const finalUrl = page.url();
      const status = resp?.status?.() ?? null;

      return {
        ok: true,
        attemptedUrl: url,
        finalUrl,
        finalScheme: (() => {
          try {
            return new URL(finalUrl).protocol.replace(":", "");
          } catch {
            return null;
          }
        })(),
        httpStatus: status,
        redirected: finalUrl && finalUrl !== url,
        latencyMs: Date.now() - t0,
        error: null,
      };
    } catch (err) {
      return {
        ok: false,
        attemptedUrl: url,
        finalUrl: null,
        finalScheme: null,
        httpStatus: null,
        redirected: false,
        latencyMs: Date.now() - t0,
        error: err?.message || String(err),
      };
    }
  }

  // 1) try as-is
  const first = await attempt(inputUrl);
  attempts.push(first);
  if (first.ok) return { result: first, attempts };

  // 2) try http→https if starts with http://
  if (/^http:\/\//i.test(inputUrl)) {
    const httpsUrl = inputUrl.replace(/^http:\/\//i, "https://");
    const second = await attempt(httpsUrl);
    attempts.push(second);
    if (second.ok) return { result: second, attempts };
  }

  return { result: null, attempts };
}

// ------------------------------
// Build envelope
// ------------------------------
function buildEnvelope({
  url,
  harvestedAt,
  cgRunId,
  manifestPath,
  assertedJsonLd,
  assertedProvenance,
  jsonldRawScriptCount,
  jsonldParseErrors,
  enrichedContent,
  inferredMeta,
  structuredMarkup,
  assertedPrimaryIndex,
  assertedPrimaryType,
  // site status signal on each capsule
  siteStatus,
}) {
  return {
    "@context": "https://agentnet.ai/context",
    "@type": "agentnet:Capsule",

    "agentnet:cgVersion": CG_VERSION,
    "agentnet:cgRunId": cgRunId,
    ...(manifestPath ? { "agentnet:cgManifestPath": manifestPath } : {}),

    "agentnet:source": url,
    "agentnet:captureDate": harvestedAt,

    "agentnet:asserted": assertedJsonLd ? { json: assertedJsonLd, provenance: assertedProvenance || null } : null,

    "agentnet:content": enrichedContent || {},

    ...(inferredMeta && Object.keys(inferredMeta).length ? { "agentnet:inferred": inferredMeta } : {}),

    "agentnet:report": {
      structuredMarkup,
      jsonldRawScriptCount: jsonldRawScriptCount || 0,
      jsonldParseErrors: jsonldParseErrors || 0,
      singlePageMode: SINGLE_PAGE,

      assertedPrimaryIndex: assertedPrimaryIndex ?? null,
      assertedPrimaryType: assertedPrimaryType ?? null,

      deterministic: CG_DETERMINISTIC,
      llmEnabled: EFFECTIVE_ENABLE_LLM,

      ...(siteStatus ? { siteStatus } : {}),
    },
  };
}

// ------------------------------
// Deterministic "status capsule" content helper
// ------------------------------
function buildStatusContent({ effectiveOrigin, attemptedUrl, finalUrl, observedAt, crawlStatus }) {
  return {
    "@context": "https://agentnet.ai/context",
    "@type": "agentnet:SiteStatus",
    "agentnet:site": effectiveOrigin,
    "agentnet:attemptedUrl": attemptedUrl || null,
    "agentnet:finalUrl": finalUrl || null,
    "agentnet:observedAt": observedAt,
    "agentnet:status": crawlStatus,
  };
}

// ------------------------------
// Crawl
// ------------------------------
async function crawlSite({ baseUrl, ctx, nodeId, cgRunId, manifestPath }) {
  const origin = new URL(baseUrl).origin;

  const visited = new Set();
  const q = [{ url: normalizeUrl(baseUrl), depth: 0 }];

  const siteStats = {
    site: origin,
    pages: 0,
    capsules: 0,
    inferred: 0,
    errors: 0,
    schemaErrors: 0,
    rejected: 0,
    inserted: 0,
    start: new Date().toISOString(),
    singlePageMode: SINGLE_PAGE,
    deterministic: CG_DETERMINISTIC,

    // Aggregate site status for this run (latest)
    siteStatus: {
      lastObserved: null,
      reachable: null,
      httpStatus: null,
      error: null,
      latencyMs: null,
      crawlStatus: null,
      finalUrl: null,
      finalScheme: null,
    },
  };

  const allCapsulesForClassifier = [];
  const capsuleReceipts = [];

  if (WRITE_SNAPSHOTS) await fs.ensureDir(SNAPSHOT_DIR);

  const pageLimit = SINGLE_PAGE ? 1 : MAX_PAGES_PER_SITE;

  while (q.length && visited.size < pageLimit) {
    const item = q.shift();
    if (!item) break;

    const url = normalizeUrl(item.url);
    const depth = item.depth;

    if (visited.has(url) || depth > MAX_DEPTH) continue;
    visited.add(url);

    try {
      checkProtocol(url);
    } catch (e) {
      console.warn(`⚠️ Protocol check warning for ${url}: ${e.message}`);
    }

    await hostThrottle(url);

    const page = await ctx.newPage();

    try {
      const nav = await gotoWithProtocolFallback(page, url);
      const harvestedAt = new Date().toISOString();

      // ------------------------------
      // If navigation totally failed -> INSERT STATUS CAPSULE and continue
      // ------------------------------
      if (!nav.result) {
        const lastAttempt = nav.attempts[nav.attempts.length - 1] || {};
        const crawlStatus = classifyNavError(lastAttempt.error);

        const failSiteStatus = {
          crawlStatus, // ok/redirected/unreachable/tls_error/timeout
          reachable: false,
          httpStatus: null,
          attemptedUrl: url,
          finalUrl: null,
          finalScheme: null,
          error: lastAttempt.error || "Navigation failed",
          latencyMs: lastAttempt.latencyMs ?? null,
          attempts: nav.attempts.map((a) => ({
            attemptedUrl: a.attemptedUrl,
            ok: a.ok,
            httpStatus: a.httpStatus ?? null,
            latencyMs: a.latencyMs ?? null,
            error: a.error ?? null,
          })),
          observedAt: harvestedAt,
        };

        // Update aggregate + DB node status (best-effort)
        siteStats.siteStatus = {
          lastObserved: harvestedAt,
          reachable: false,
          httpStatus: null,
          error: failSiteStatus.error,
          latencyMs: failSiteStatus.latencyMs,
          crawlStatus,
          finalUrl: null,
          finalScheme: null,
        };

        await tryUpdateNodeStatus(nodeId, {
          reachable: false,
          httpStatus: null,
          error: failSiteStatus.error,
          lastObservedAt: harvestedAt,
          latencyMs: failSiteStatus.latencyMs,
          pageUrl: url,
        });

        // Insert status capsule (so resolver can return provenance even on failure)
        const statusContent = buildStatusContent({
          effectiveOrigin: origin,
          attemptedUrl: url,
          finalUrl: null,
          observedAt: harvestedAt,
          crawlStatus,
        });

        const envelope = buildEnvelope({
          url, // source remains attempted url for failure capsule
          harvestedAt,
          cgRunId,
          manifestPath,
          assertedJsonLd: null,
          assertedProvenance: null,
          jsonldRawScriptCount: 0,
          jsonldParseErrors: 0,
          enrichedContent: statusContent,
          inferredMeta: {},
          structuredMarkup: "none",
          assertedPrimaryIndex: null,
          assertedPrimaryType: null,
          siteStatus: failSiteStatus,
        });

        // Deterministic fingerprint still includes siteStatus, so this is a real provenance event
        const fingerprint = fp(CG_DETERMINISTIC ? stableFingerprintView(envelope) : envelope);

        // Status capsule should *not* be ok
        const status = "needs_review";

        const ins = await insertCapsule(nodeId, envelope, fingerprint, harvestedAt, status);

        capsuleReceipts.push({
          pageUrl: url,
          capsuleId: ins?.capsuleId ?? null,
          fingerprint,
          status,
          assertedPrimaryType: null,
          structuredMarkup: "none",
          httpStatus: null,
          latencyMs: failSiteStatus.latencyMs,
          crawlStatus,
          finalUrl: null,
          finalScheme: null,
        });

        siteStats.capsules += 1;
        siteStats.pages += 1;
        siteStats.rejected += 1;
        siteStats.errors += 1;

        console.warn(`⚠️ Status capsule inserted for unreachable site ${url} crawlStatus=${crawlStatus}`);
        continue; // next page/site
      }

      // ------------------------------
      // Navigation succeeded
      // ------------------------------
      const navRes = nav.result;
      const finalUrl = navRes.finalUrl || page.url();
      const finalScheme = navRes.finalScheme;
      const httpStatus = navRes.httpStatus;
      const crawlStatus = navRes.redirected ? "redirected" : "ok";

      await page.waitForTimeout(800);

      const html = await page.content();
      const text = await page.evaluate(() => document.body?.innerText || "");

      // Use finalUrl for capsule identity (important if http->https or other redirects)
      const effectiveUrl = normalizeUrl(finalUrl);

      const pageSiteStatus = {
        crawlStatus,
        reachable: true,
        httpStatus,
        attemptedUrl: navRes.attemptedUrl,
        finalUrl,
        finalScheme,
        error: null,
        latencyMs: navRes.latencyMs ?? null,
        attempts: nav.attempts.map((a) => ({
          attemptedUrl: a.attemptedUrl,
          ok: a.ok,
          httpStatus: a.httpStatus ?? null,
          latencyMs: a.latencyMs ?? null,
          error: a.error ?? null,
        })),
        observedAt: harvestedAt,
      };

      // Update aggregate + DB node status (best-effort)
      siteStats.siteStatus = {
        lastObserved: harvestedAt,
        reachable: true,
        httpStatus,
        error: null,
        latencyMs: navRes.latencyMs ?? null,
        crawlStatus,
        finalUrl,
        finalScheme,
      };

      await tryUpdateNodeStatus(nodeId, {
        reachable: true,
        httpStatus,
        error: null,
        lastObservedAt: harvestedAt,
        latencyMs: navRes.latencyMs ?? null,
        pageUrl: effectiveUrl,
      });

      // Extract asserted JSON-LD (use effectiveUrl)
      const jsonld = extractJsonLd(html, effectiveUrl, { capturedAt: harvestedAt });
      const rawCount = Number(jsonld?.rawCount || 0);
      const blocksRaw = Array.isArray(jsonld?.blocks) ? jsonld.blocks : [];
      const parseErrors = Array.isArray(jsonld?.parseErrors) ? jsonld.parseErrors : [];
      const found = Boolean(jsonld?.found);

      if (rawCount > 0 && !found) {
        console.warn(`⚠️ JSON-LD scripts present but unparsable on ${effectiveUrl}:`, parseErrors);
      }

      // Build single asserted-array
      let assertedJson = null;
      let assertedProvenance = null;
      let assertedPrimaryIndex = null;
      let assertedPrimaryType = null;

      if (found && blocksRaw.length > 0) {
        const blocks = blocksRaw.map((b) => {
          if (b && typeof b === "object" && "json" in b) return b;
          return { json: b, provenance: { evidenceType: "jsonld-script", url: effectiveUrl, capturedAt: harvestedAt } };
        });

        const cleanedBlocks = blocks.map((b) => ({
          json: CG_DETERMINISTIC ? stableSortJsonLd(b.json) : b.json,
          provenance: b.provenance || null,
        }));

        assertedJson = cleanedBlocks.map((b) => b.json);
        assertedProvenance = {
          evidenceType: "jsonld-script",
          url: effectiveUrl,
          capturedAt: harvestedAt,
        };

        const primary = selectPrimaryIndexDeterministic(cleanedBlocks);
        assertedPrimaryIndex = primary.index;
        assertedPrimaryType = primary.type;
      }

      const primaryAssertedObject =
        Array.isArray(assertedJson) && assertedJson.length > 0
          ? assertedJson[assertedPrimaryIndex ?? 0] || assertedJson[0]
          : {};

      // Inference
      let enrichedContent;
      let inferredMeta;

      try {
        const out = await inferCapsule({
          url: effectiveUrl,
          html,
          text,
          extractedCapsule:
            primaryAssertedObject && typeof primaryAssertedObject === "object" ? primaryAssertedObject : {},
          options: { enableLLM: EFFECTIVE_ENABLE_LLM },
        });
        enrichedContent = out.capsule;
        inferredMeta = out.inferred;
      } catch (infErr) {
        console.warn(`⚠️ Inference failed on ${effectiveUrl}: ${infErr.message}`);
        siteStats.errors += 1;

        enrichedContent = {
          "@context": "https://agentnet.ai/context",
          "@type": "agentnet:Thing",
          "agentnet:name": (html.match(/<title[^>]*>([^<]+)<\/title>/i) || [])[1] || "Unknown",
          "agentnet:inferred": {
            "agentnet:name": { confidence: 0.4, source: "heuristic", method: "title-fallback" },
          },
        };
        inferredMeta = enrichedContent["agentnet:inferred"] || {};
      }

      // Envelope (use effectiveUrl for agentnet:source)
      const envelope = buildEnvelope({
        url: effectiveUrl,
        harvestedAt,
        cgRunId,
        manifestPath,
        assertedJsonLd: assertedJson,
        assertedProvenance,
        jsonldRawScriptCount: rawCount,
        jsonldParseErrors: parseErrors.length,
        enrichedContent,
        inferredMeta,
        structuredMarkup: found ? "jsonld" : "none",
        assertedPrimaryIndex,
        assertedPrimaryType,
        siteStatus: pageSiteStatus,
      });

      // price guardrail
      guardTinyPrice(envelope["agentnet:content"], envelope["agentnet:report"]);

      // Deterministic fingerprint
      const fingerprint = fp(CG_DETERMINISTIC ? stableFingerprintView(envelope) : envelope);

      // Schema validation gate
      let status = "ok";
      if (validateEnvelope) {
        const valid = validateEnvelope(envelope);
        if (!valid) {
          status = "needs_review";
          envelope["agentnet:report"].schemaErrors = validateEnvelope.errors || [];
          siteStats.schemaErrors += (validateEnvelope.errors || []).length;
        }
      }

      // Insert capsule
      const ins = await insertCapsule(nodeId, envelope, fingerprint, harvestedAt, status);

      capsuleReceipts.push({
        pageUrl: effectiveUrl,
        capsuleId: ins?.capsuleId ?? null,
        fingerprint,
        status,
        assertedPrimaryType: assertedPrimaryType ?? null,
        structuredMarkup: found ? "jsonld" : "none",
        httpStatus,
        latencyMs: navRes.latencyMs ?? null,
        crawlStatus,
        finalUrl,
        finalScheme,
      });

      siteStats.capsules += 1;
      siteStats.pages += 1;

      if (status === "ok") siteStats.inserted += 1;
      else siteStats.rejected += 1;

      if (inferredMeta && Object.keys(inferredMeta).length) siteStats.inferred += 1;

      allCapsulesForClassifier.push({ "agentnet:content": envelope["agentnet:content"] });

      if (WRITE_SNAPSHOTS) {
        const name = snapshotName(effectiveUrl);
        await fs.writeFile(`${SNAPSHOT_DIR}/${name}`, html);
      }

      console.log(
        `✅ 1 capsule processed for ${effectiveUrl} (HTTP: ${httpStatus ?? "?"}, ${navRes.latencyMs ?? "?"}ms, JSON-LD scripts: ${rawCount}, parsed objects: ${blocksRaw.length})`
      );

      // Discover same-origin links (disabled in SINGLE_PAGE mode)
      if (!SINGLE_PAGE && depth < MAX_DEPTH) {
        const links = await page.$$eval("a[href]", (as) => as.map((a) => a.href).filter(Boolean));
        for (const link of links) {
          let normLink;
          try {
            normLink = normalizeUrl(link);
          } catch {
            continue;
          }
          try {
            if (new URL(normLink).origin !== origin) continue;
          } catch {
            continue;
          }
          if (!visited.has(normLink)) q.push({ url: normLink, depth: depth + 1 });
        }
      }
    } catch (e) {
      const harvestedAt = new Date().toISOString();
      const errMsg = String(e?.message || e);
      const crawlStatus = classifyNavError(errMsg);

      const failSiteStatus = {
        crawlStatus,
        reachable: false,
        httpStatus: null,
        attemptedUrl: url,
        finalUrl: null,
        finalScheme: null,
        error: errMsg,
        latencyMs: null,
        attempts: [],
        observedAt: harvestedAt,
      };

      siteStats.siteStatus = {
        lastObserved: harvestedAt,
        reachable: false,
        httpStatus: null,
        error: errMsg,
        latencyMs: null,
        crawlStatus,
        finalUrl: null,
        finalScheme: null,
      };

      await tryUpdateNodeStatus(nodeId, {
        reachable: false,
        httpStatus: null,
        error: errMsg,
        lastObservedAt: harvestedAt,
        latencyMs: null,
        pageUrl: url,
      });

      // Insert a status capsule even on unexpected errors
      try {
        const statusContent = buildStatusContent({
          effectiveOrigin: origin,
          attemptedUrl: url,
          finalUrl: null,
          observedAt: harvestedAt,
          crawlStatus,
        });

        const envelope = buildEnvelope({
          url,
          harvestedAt,
          cgRunId,
          manifestPath,
          assertedJsonLd: null,
          assertedProvenance: null,
          jsonldRawScriptCount: 0,
          jsonldParseErrors: 0,
          enrichedContent: statusContent,
          inferredMeta: {},
          structuredMarkup: "none",
          assertedPrimaryIndex: null,
          assertedPrimaryType: null,
          siteStatus: failSiteStatus,
        });

        const fingerprint = fp(CG_DETERMINISTIC ? stableFingerprintView(envelope) : envelope);
        const status = "needs_review";
        const ins = await insertCapsule(nodeId, envelope, fingerprint, harvestedAt, status);

        capsuleReceipts.push({
          pageUrl: url,
          capsuleId: ins?.capsuleId ?? null,
          fingerprint,
          status,
          assertedPrimaryType: null,
          structuredMarkup: "none",
          httpStatus: null,
          latencyMs: null,
          crawlStatus,
          finalUrl: null,
          finalScheme: null,
        });

        siteStats.capsules += 1;
        siteStats.pages += 1;
        siteStats.rejected += 1;
      } catch (inner) {
        console.warn(`[WORKER] ⚠️ Failed to insert status capsule for error on ${url}: ${inner?.message || inner}`);
      }

      console.error(`❌ Error crawling ${url}: ${errMsg}`);
      siteStats.errors += 1;
    } finally {
      await page.close();
    }
  }

  // Classify node type
  try {
    const category = classifyNodeType(allCapsulesForClassifier);
    await pool.query(`UPDATE nodes SET node_category=? WHERE id=?`, [category, nodeId]);
    console.log(`🏷️  Node ${origin} classified as '${category}'`);
    siteStats.nodeCategory = category;
  } catch (err) {
    console.warn(`⚠️ Node classification failed for ${origin}: ${err.message}`);
    siteStats.nodeCategory = null;
  }

  siteStats.end = new Date().toISOString();
  await appendLog(siteStats);
  console.log(`🌐 Crawl complete for ${origin}: ${visited.size} pages processed.`);

  return { ...siteStats, capsuleReceipts };
}

// ------------------------------
// Run manifest writer (Audit Receipt)
// ------------------------------
async function writeRunManifest({ runId, startedAt, finishedAt, seed, settings, node, summary, capsules, errors }) {
  await fs.ensureDir(RUNS_DIR);
  const manifestPath = `${RUNS_DIR}/${runId}.json`;

  const manifest = {
    runId,
    startedAt,
    finishedAt,
    cgVersion: CG_VERSION,
    queueName,
    seed,
    settings,
    node,
    summary,
    capsules,
    errors: errors || [],
    manifestPath,
    nodeCategory: node?.nodeCategory || null,

    // include the last known site status in the run manifest
    siteStatus: summary?.siteStatus || null,

    // ✅ CG Output Contract
    "agentnet:cgOutput": {
      capsulesInserted: summary?.inserted ?? 0,
      capsulesRejected: summary?.rejected ?? 0,
      schemaErrorsCount: summary?.schemaErrors ?? 0,
    },
  };

  await fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2), "utf8");
  return manifestPath;
}

// ------------------------------
// BullMQ Worker
// ------------------------------
const worker = new Worker(
  queueName,
  async (job) => {
    const { url, owner_slug } = job.data;

    const runId = makeRunId();
    const startedAt = new Date().toISOString();

    const browser = await chromium.launch({ headless: true });
    const ctx = await browser.newContext({ userAgent: UA });

    const settings = {
      SINGLE_PAGE,
      MAX_DEPTH,
      MAX_PAGES_PER_SITE,
      PER_HOST_DELAY,
      ENABLE_LLM,
      EFFECTIVE_ENABLE_LLM,
      CG_DETERMINISTIC,
      VALIDATE_ENVELOPE: Boolean(validateEnvelope),
      SCHEMA_LOADED: envelopeSchemaLoaded,
      WRITE_SNAPSHOTS,
      USER_AGENT: UA,
      CONCURRENCY,
      ENABLE_INQUIRY_ENQUEUE,
      inquiryQueueName,
      inquiryJobName: INQUIRY_JOB_NAME,
    };

    const seed = { owner_slug, url };
    const capsuleReceipts = [];
    const errors = [];

    try {
      const nodeId = await upsertNode(owner_slug, url);

      // Capsules will write this into the envelope immediately
      const manifestPath = `${RUNS_DIR}/${runId}.json`;

      const stats = await crawlSite({
        baseUrl: url,
        ctx,
        nodeId,
        cgRunId: runId,
        manifestPath,
      });

      // ------------------------------
      // Enqueue resolver inquiry job (consumed by ant-inquiry-generator)
      // ------------------------------
      if (ENABLE_INQUIRY_ENQUEUE) {
        try {
          const receipts = stats.capsuleReceipts || [];
          const payload = {
            runId,
            owner_slug,
            nodeId,
            nodeOrigin: new URL(url).origin,
            capsules: receipts.map((c) => ({
              capsuleId: c.capsuleId,
              fingerprint: c.fingerprint,
              status: c.status,
            })),
          };

          // Dedupe controls:
          // - default: NO dedupe (unique per run)
          // - if ENABLE_INQUIRY_DEDUPE=true: dedupe by nodeId + capsules-fingerprint-hash
          const wantDedupe = (process.env.ENABLE_INQUIRY_DEDUPE ?? "false").toLowerCase() === "true";

          const capsuleHash = sha1Hex(JSON.stringify(payload.capsules || []));
          const rawJobId = wantDedupe ? `inquiry_${nodeId}_${capsuleHash}` : `inquiry_${runId}`;

          const jobId = safeJobId(rawJobId);

          console.log(`[WORKER] 📩 enqueue inquiry -> queue=${inquiryQueueName} jobId=${jobId}`, {
            runId,
            nodeId,
            capsules: payload.capsules.length,
            wantDedupe,
          });

          await inquiryQueue.add(INQUIRY_JOB_NAME, payload, {
            jobId,
            removeOnComplete: true,
            removeOnFail: false,
          });

          console.log(`[WORKER] 📨 Inquiry job enqueued ok jobId=${jobId}`);
        } catch (e) {
          console.warn(`[WORKER] ⚠️ Failed to enqueue inquiry job for ${url}: ${e.message}`);
        }
      } else {
        console.log("[WORKER] ℹ️ Inquiry enqueue disabled (inquiryQueue not available or ENABLE_INQUIRY_ENQUEUE=false).");
      }

      await browser.close();

      // Manifest-friendly summary receipt
      capsuleReceipts.push({
        url,
        finishedAt: stats.end || new Date().toISOString(),
        pages: stats.pages,
        capsules: stats.capsules,
        inserted: stats.inserted,
        rejected: stats.rejected,
        schemaErrors: stats.schemaErrors,
      });

      const finishedAt = new Date().toISOString();

      const summary = {
        pages: stats.pages,
        capsules: stats.capsules,
        inferred: stats.inferred,
        errors: stats.errors,
        inserted: stats.inserted,
        rejected: stats.rejected,
        schemaErrors: stats.schemaErrors,

        // propagate site status into manifest
        siteStatus: stats.siteStatus || null,
      };

      const node = { nodeId, nodeCategory: stats.nodeCategory || null };

      const written = await writeRunManifest({
        runId,
        startedAt,
        finishedAt,
        seed,
        settings,
        node,
        summary,
        capsules: capsuleReceipts,
        errors,
      });

      console.log(`[WORKER] 🏁 ${stats.pages} pages / ${stats.capsules} capsules (${stats.inferred} inferred) for ${url}`);
      console.log(`[WORKER] 🧾 Run manifest written: ${written}`);

      return { ok: true, runId, manifestPath: written };
    } catch (e) {
      try {
        await browser.close();
      } catch {
        // ignore
      }
      console.error(`[WORKER] 💥 Fatal error on ${url}: ${e.message}`);

      errors.push({ site: url, error: e.message, time: new Date().toISOString() });
      await appendLog({ site: url, error: e.message, time: new Date().toISOString() });

      throw e;
    }
  },
  { connection, concurrency: CONCURRENCY }
);

// ------------------------------
// Worker lifecycle logging
// ------------------------------
worker.on("ready", () => console.log(`[WORKER] ✅ BullMQ worker ready. queue=${queueName} concurrency=${CONCURRENCY}`));
worker.on("active", (job) => console.log(`[WORKER] ▶️  active job id=${job.id} name=${job.name}`));
worker.on("completed", (job, res) => console.log(`[WORKER] ✅ completed job id=${job.id} name=${job.name}`, res || ""));
worker.on("failed", (job, err) =>
  console.warn(`[WORKER] ❌ failed job id=${job?.id} name=${job?.name} err=${err?.message}`)
);
worker.on("error", (err) => console.error("[WORKER] ❌ worker error:", err));
