// src/tools/capsulizeRepo.js
// Repo document capsulization — processes .md and .docx files into doc + section capsules.
require("../bootstrap/env");

const fs = require("fs-extra");
const path = require("path");
const crypto = require("crypto");
const mammoth = require("mammoth");
const cheerio = require("cheerio");

const { ensureNode, pool, buildSearchText } = require("../db");

// ------------------------------
// CLI args
// ------------------------------
const args = process.argv.slice(2);

function getArg(name) {
  const idx = args.indexOf(`--${name}`);
  if (idx === -1 || idx + 1 >= args.length) return null;
  return args[idx + 1];
}

function getAllArgs(name) {
  const values = [];
  for (let i = 0; i < args.length; i++) {
    if (args[i] === `--${name}` && i + 1 < args.length) {
      values.push(args[++i]);
    }
  }
  return values;
}

function splitCsvArgs(values) {
  const out = [];
  for (const raw of values) {
    const parts = String(raw || "")
      .split(",")
      .map((p) => p.trim())
      .filter(Boolean);
    out.push(...parts);
  }
  return out;
}

const repoPath = getArg("repoPath");
const defaultIncludePatterns = ["**/*.md", "**/*.mdx", "**/*.markdown", "**/*.docx"];
const includePatternsInput = splitCsvArgs(getAllArgs("include"));
const excludePatternsInput = splitCsvArgs(getAllArgs("exclude"));
const includeMode = includePatternsInput.length ? "override-defaults" : "defaults";
const includePatterns = includePatternsInput.length ? includePatternsInput : defaultIncludePatterns;

function parseLimit(raw) {
  if (raw === null || raw === undefined || String(raw).trim() === "") return null;
  const parsed = Number.parseInt(String(raw).trim(), 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("--limit must be a positive integer when provided");
  }
  return parsed;
}

const limit = parseLimit(getArg("limit"));
function resolveOwnerIdInput() {
  const candidates = [
    { source: "cli:ownerId", value: getArg("ownerId") },
    { source: "cli:owner_id", value: getArg("owner_id") },
    { source: "env:OWNER_ID", value: process.env.OWNER_ID },
    { source: "env:DEMO_OWNER_ID", value: process.env.DEMO_OWNER_ID },
    { source: "env:DEFAULT_OWNER_ID", value: process.env.DEFAULT_OWNER_ID },
    { source: "env:ANT_WORKER_OWNER_ID", value: process.env.ANT_WORKER_OWNER_ID },
  ];
  for (const candidate of candidates) {
    const trimmed = String(candidate.value || "").trim();
    if (trimmed) return { raw: trimmed, source: candidate.source };
  }
  return { raw: "", source: "none" };
}

if (!repoPath) {
  console.error(
    "Usage: node src/tools/capsulizeRepo.js --repoPath <path> [--include <glob>] [--exclude <glob|dir>] [--limit <n>]"
  );
  process.exit(1);
}

const resolvedRepo = path.resolve(repoPath);

function parseOwnerId(raw) {
  const parsed = Number(String(raw || "").trim());
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Missing ownerId. Provide --ownerId or set OWNER_ID in .env");
  }
  return parsed;
}

const ownerIdInput = resolveOwnerIdInput();
const ownerId = parseOwnerId(ownerIdInput.raw);

// ------------------------------
// Constants
// ------------------------------
const DEFAULT_EXCLUDE_DIRS = [
  "node_modules",
  ".git",
  "dist",
  "build",
  "coverage",
  ".next",
  ".cache",
  "tmp",
  "temp",
  "runs",
];
const IGNORE_DIRS = new Set([...DEFAULT_EXCLUDE_DIRS, "keys"]);
const CG_VERSION = process.env.CG_VERSION || "cg-0.5-determinism-ajv-output";
const RUNS_DIR = "./runs";
const PIPELINE_VERSION = "repo-docs-v1";
const SECTION_TEXT_LIMIT = 4000;
let loggedNodeFallback = false;

// ------------------------------
// Recursive walk
// ------------------------------
function isDocPath(filePath) {
  const lower = filePath.toLowerCase();
  return (
    lower.endsWith(".md") ||
    lower.endsWith(".mdx") ||
    lower.endsWith(".markdown") ||
    lower.endsWith(".docx")
  );
}

function normalizePattern(pattern) {
  return String(pattern || "")
    .trim()
    .replace(/\\/g, "/")
    .replace(/^\.\/+/, "")
    .replace(/^\/+/, "")
    .replace(/\/+$/, "");
}

const defaultExcludePatterns = DEFAULT_EXCLUDE_DIRS.map((d) => `${d}/**`);
const excludePatterns = [...defaultExcludePatterns, ...excludePatternsInput.map(normalizePattern)].filter(Boolean);

function isExcludedPath(relPosix, isDirectory) {
  if (!relPosix || relPosix === ".") return false;
  const relWithSlash = isDirectory ? `${relPosix}/` : relPosix;
  return excludePatterns.some((pattern) => {
    const p = normalizePattern(pattern);
    if (!p) return false;
    if (!p.includes("*") && !p.includes("?")) {
      if (isDirectory) {
        return relPosix === p || relPosix.startsWith(`${p}/`);
      }
      return relPosix === p || relPosix.startsWith(`${p}/`);
    }
    const direct = matchInclude(relWithSlash, p);
    const deep = isDirectory ? matchInclude(`${relPosix}/__child__`, p) : false;
    return direct || deep;
  });
}

async function walkDir(dir, state) {
  const results = [];
  const entries = await fs.readdir(dir, { withFileTypes: true });

  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    const relPath = path.relative(resolvedRepo, full);
    const relPosix = relPath.split(path.sep).join("/");

    if (entry.isDirectory()) {
      if (IGNORE_DIRS.has(entry.name) || isExcludedPath(relPosix, true)) {
        state.excludedCount += 1;
        if (state.skipExamples.length < 10) {
          state.skipExamples.push({ path: relPosix, reason: "excluded by directory filter" });
        }
        continue;
      }
      results.push(...(await walkDir(full, state)));
    } else if (entry.isFile()) {
      state.discoveredFiles += 1;
      if (!isDocPath(full)) continue;
      if (isExcludedPath(relPosix, false)) {
        state.excludedCount += 1;
        if (state.skipExamples.length < 10) {
          state.skipExamples.push({ path: relPosix, reason: "excluded by pattern" });
        }
        continue;
      }
      state.discoveredDocs += 1;
      if (!includePatterns.some((p) => matchInclude(relPosix, p))) {
        state.skippedByInclude += 1;
        if (state.skipExamples.length < 10) {
          state.skipExamples.push({ path: relPosix, reason: "did not match include filters" });
        }
        continue;
      }
      state.matchedByExt[fileType(full)] += 1;
      state.matchedDocs += 1;
      if (limit !== null && state.matchedDocs > limit) {
        state.skippedByLimit += 1;
        if (state.skipExamples.length < 10) {
          state.skipExamples.push({ path: relPosix, reason: `skipped by --limit (${limit})` });
        }
        continue;
      }
      state.processedCandidates += 1;
      results.push(full);
    }
  }
  return results;
}

// ------------------------------
// Text helpers
// ------------------------------
function slugify(text) {
  return text
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-{2,}/g, "-")
    .replace(/^-|-$/g, "");
}

function stripMarkdown(text) {
  return text
    .replace(/^#{1,6}\s+/gm, "")
    .replace(/!\[.*?\]\(.*?\)/g, "")
    .replace(/\[([^\]]*)\]\(.*?\)/g, "$1")
    .replace(/[*_`~]/g, "")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

// ------------------------------
// Markdown extraction (doc-level)
// ------------------------------
function extractFromMarkdown(content) {
  const titleMatch = content.match(/^# (.+)$/m);
  const title = titleMatch ? titleMatch[1].trim() : null;

  const headings = [];
  const headingRe = /^#{2,3} (.+)$/gm;
  let m;
  while ((m = headingRe.exec(content)) !== null) {
    headings.push(m[1].trim());
  }

  const excerpt = stripMarkdown(content).slice(0, 800);
  return { title, headings, excerpt };
}

// ------------------------------
// Section parsing — split by ## headings
// ------------------------------
function parseSections(content) {
  const sections = [];
  const re = /^## (.+)$/gm;
  const matches = [];
  let m;
  while ((m = re.exec(content)) !== null) {
    matches.push({ title: m[1].trim(), start: m.index });
  }

  for (let i = 0; i < matches.length; i++) {
    const start = matches[i].start;
    const end = i + 1 < matches.length ? matches[i + 1].start : content.length;
    const rawBody = content.slice(start, end);
    const sectionText = stripMarkdown(rawBody).slice(0, SECTION_TEXT_LIMIT);

    sections.push({
      sectionTitle: matches[i].title,
      sectionSlug: slugify(matches[i].title),
      sectionText,
      sectionIndex: i,
    });
  }

  return sections;
}

// ------------------------------
// DOCX extraction (via mammoth → HTML → cheerio)
// ------------------------------
async function extractFromDocx(filePath) {
  const result = await mammoth.convertToHtml({ path: filePath });
  const html = result.value;
  const $ = cheerio.load(html);

  const title = $("h1").first().text().trim() || null;

  const headings = [];
  $("h2, h3").each((_, el) => {
    const text = $(el).text().trim();
    if (text) headings.push(text);
  });

  const excerpt = $("body").text().trim().slice(0, 800);

  return { title, headings, excerpt, html };
}

function parseSectionsFromHtml(html) {
  const $ = cheerio.load(html);
  const sections = [];
  let currentTitle = null;
  let currentParts = [];
  let sectionIndex = 0;

  $("body").children().each((_, el) => {
    const tag = $(el).prop("tagName")?.toLowerCase();
    if (tag === "h2") {
      if (currentTitle !== null) {
        sections.push({
          sectionTitle: currentTitle,
          sectionSlug: slugify(currentTitle),
          sectionText: currentParts.join("\n").trim().slice(0, SECTION_TEXT_LIMIT),
          sectionIndex: sectionIndex++,
        });
      }
      currentTitle = $(el).text().trim();
      currentParts = [];
    } else if (currentTitle !== null) {
      const text = $(el).text().trim();
      if (text) currentParts.push(text);
    }
  });

  if (currentTitle !== null) {
    sections.push({
      sectionTitle: currentTitle,
      sectionSlug: slugify(currentTitle),
      sectionText: currentParts.join("\n").trim().slice(0, SECTION_TEXT_LIMIT),
      sectionIndex: sectionIndex,
    });
  }

  return sections;
}

// ------------------------------
// File type helper
// ------------------------------
function fileType(filePath) {
  return filePath.toLowerCase().endsWith(".docx") ? "docx" : "md";
}

// ------------------------------
// Build capsule envelope (doc-level)
// ------------------------------
function buildRepoEnvelope({ relativePath, nodeId, title, headings, excerpt, harvestedAt, cgRunId }) {
  return {
    "@context": "https://agentnet.ai/context",
    "@type": "agentnet:Capsule",

    "agentnet:cgVersion": CG_VERSION,
    "agentnet:cgRunId": cgRunId,

    "agentnet:source": `agentnet-doc:${relativePath}`,
    "agentnet:registrarNodeId": nodeId,
    "agentnet:captureDate": harvestedAt,

    "agentnet:content": {
      "@context": "https://agentnet.ai/context",
      "@type": "agentnet:Document",
      "agentnet:name": title || relativePath,
      "agentnet:description": excerpt || "",
      "agentnet:headings": headings,
      "agentnet:relativePath": relativePath,
    },

    "agentnet:report": {
      structuredMarkup: "markdown",
      mode: "repo",
    },
  };
}

// ------------------------------
// SHA-256 helper for payload hashing
// ------------------------------
function sha256(str) {
  return crypto.createHash("sha256").update(str).digest("hex");
}

function deterministicUuidFromString(input) {
  const hex = sha256(String(input || "")).slice(0, 32);
  const p1 = hex.slice(0, 8);
  const p2 = hex.slice(8, 12);
  const p3 = `4${hex.slice(13, 16)}`;
  const variantNibble = ((parseInt(hex[16], 16) & 0x3) | 0x8).toString(16);
  const p4 = `${variantNibble}${hex.slice(17, 20)}`;
  const p5 = hex.slice(20, 32);
  return `${p1}-${p2}-${p3}-${p4}-${p5}`;
}

async function ensureNodeWithFallback(nodeUri, meta) {
  try {
    return await ensureNode(nodeUri, meta);
  } catch (err) {
    const msg = String(err?.message || "");
    const registrarIssue =
      msg.includes("Registrar POST") ||
      msg.includes("issueNode") ||
      msg.includes("Failed to issue node");
    if (!registrarIssue) throw err;

    const fallbackNodeId = deterministicUuidFromString(nodeUri);
    await pool.query(
      `
      INSERT INTO nodes (id, node_uri, source_url, domain, owner_slug, owner_id)
      VALUES (?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        source_url = VALUES(source_url),
        domain = VALUES(domain),
        owner_slug = VALUES(owner_slug),
        owner_id = IF(nodes.owner_id IS NULL OR nodes.owner_id = 0, VALUES(owner_id), nodes.owner_id),
        updated_at = NOW()
      `,
      [
        fallbackNodeId,
        nodeUri,
        meta.source_url || null,
        meta.domain || null,
        meta.owner_slug || null,
        ownerId,
      ]
    );
    if (!loggedNodeFallback) {
      console.warn("[capsulizeRepo] WARN: registrar node issuance unavailable; using deterministic local node IDs.");
      loggedNodeFallback = true;
    }
    return fallbackNodeId;
  }
}

// ------------------------------
// Insert doc capsule (stable URI, idempotent upsert)
// ------------------------------
async function insertDocCapsule({
  capsuleUri,
  ownerId,
  nodeId,
  nodeUri,
  payload,
  capsuleJson,
  producedAt,
  harvestedAt,
}) {
  const producedAtSql = producedAt.replace("T", " ").replace("Z", "").split(".")[0];
  const harvestedAtSql = harvestedAt.replace("T", " ").replace("Z", "").split(".")[0];
  const payloadStr = JSON.stringify(payload);
  const payloadHash = sha256(payloadStr);
  const searchText = buildSearchText({
    capsuleUri,
    nodeUri,
    payload,
    capsuleJson,
  });

  await pool.query(
    `
    INSERT INTO capsules (
      capsule_uri, owner_id, node_id, type, payload, capsule_json,
      method, pipeline_version, status,
      produced_at, harvested_at,
      is_authoritative, \`rank\`, payload_hash_sha256, search_text
    )
    VALUES (?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      owner_id = IF(capsules.owner_id IS NULL OR capsules.owner_id = 0, VALUES(owner_id), capsules.owner_id),
      node_id = VALUES(node_id),
      type = VALUES(type),
      payload = VALUES(payload),
      capsule_json = VALUES(capsule_json),
      method = VALUES(method),
      pipeline_version = VALUES(pipeline_version),
      status = VALUES(status),
      produced_at = VALUES(produced_at),
      harvested_at = VALUES(harvested_at),
      is_authoritative = VALUES(is_authoritative),
      \`rank\` = VALUES(\`rank\`),
      payload_hash_sha256 = VALUES(payload_hash_sha256),
      search_text = IF(capsules.search_text IS NULL OR capsules.search_text = '', VALUES(search_text), capsules.search_text),
      updated_at = NOW()
    `,
    [
      capsuleUri,
      ownerId,
      nodeId,
      "repo-doc",
      payloadStr,
      JSON.stringify(capsuleJson),
      "repo",
      PIPELINE_VERSION,
      "active",
      producedAtSql,
      harvestedAtSql,
      1,
      10,
      payloadHash,
      searchText,
    ]
  );

  const [repair] = await pool.query(
    `UPDATE capsules
     SET owner_id = IF(owner_id IS NULL OR owner_id = 0, ?, owner_id),
         search_text = IF(search_text IS NULL OR search_text = '', ?, search_text)
     WHERE capsule_uri = ?
       AND ((owner_id IS NULL OR owner_id = 0) OR (search_text IS NULL OR search_text = ''))`,
    [ownerId, searchText, capsuleUri]
  );
  if (repair?.affectedRows > 0) {
    console.log(`[capsulizeRepo] repaired capsules.owner_id capsule_uri=${capsuleUri} owner_id=${ownerId}`);
  }

  return capsuleUri;
}

// ------------------------------
// Insert section capsule directly (uses columns not covered by generic insertCapsule)
// ------------------------------
async function insertSectionCapsule({
  capsuleUri,
  ownerId,
  nodeId,
  nodeUri,
  payload,
  capsuleJson,
  producedAt,
  harvestedAt,
}) {
  const producedAtSql = producedAt.replace("T", " ").replace("Z", "").split(".")[0];
  const harvestedAtSql = harvestedAt.replace("T", " ").replace("Z", "").split(".")[0];
  const payloadStr = JSON.stringify(payload);
  const payloadHash = sha256(payloadStr);
  const searchText = buildSearchText({
    capsuleUri,
    nodeUri,
    payload,
    capsuleJson,
  });

  await pool.query(
    `
    INSERT INTO capsules (
      capsule_uri, owner_id, node_id, type, payload, capsule_json,
      method, pipeline_version, status,
      produced_at, harvested_at,
      is_authoritative, \`rank\`, payload_hash_sha256, search_text
    )
    VALUES (?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      owner_id = IF(capsules.owner_id IS NULL OR capsules.owner_id = 0, VALUES(owner_id), capsules.owner_id),
      node_id = VALUES(node_id),
      type = VALUES(type),
      payload = VALUES(payload),
      capsule_json = VALUES(capsule_json),
      method = VALUES(method),
      pipeline_version = VALUES(pipeline_version),
      status = VALUES(status),
      produced_at = VALUES(produced_at),
      harvested_at = VALUES(harvested_at),
      is_authoritative = VALUES(is_authoritative),
      \`rank\` = VALUES(\`rank\`),
      payload_hash_sha256 = VALUES(payload_hash_sha256),
      search_text = IF(capsules.search_text IS NULL OR capsules.search_text = '', VALUES(search_text), capsules.search_text),
      updated_at = NOW()
    `,
    [
      capsuleUri,
      ownerId,
      nodeId,
      "repo-doc-section",
      payloadStr,
      JSON.stringify(capsuleJson),
      "repo",
      PIPELINE_VERSION,
      "active",
      producedAtSql,
      harvestedAtSql,
      1,
      10,
      payloadHash,
      searchText,
    ]
  );

  const [repair] = await pool.query(
    `UPDATE capsules
     SET owner_id = IF(owner_id IS NULL OR owner_id = 0, ?, owner_id),
         search_text = IF(search_text IS NULL OR search_text = '', ?, search_text)
     WHERE capsule_uri = ?
       AND ((owner_id IS NULL OR owner_id = 0) OR (search_text IS NULL OR search_text = ''))`,
    [ownerId, searchText, capsuleUri]
  );
  if (repair?.affectedRows > 0) {
    console.log(`[capsulizeRepo] repaired capsules.owner_id capsule_uri=${capsuleUri} owner_id=${ownerId}`);
  }

  return capsuleUri;
}

// ------------------------------
// Include-pattern matching on repo-relative POSIX paths (no deps)
// ------------------------------
function globToRegex(glob) {
  const normalized = String(glob || "").split(path.sep).join("/");
  let out = "^";
  for (let i = 0; i < normalized.length; i++) {
    const ch = normalized[i];
    const next = normalized[i + 1];
    if (ch === "*" && next === "*") {
      out += ".*";
      i += 1;
      continue;
    }
    if (ch === "*") {
      out += "[^/]*";
      continue;
    }
    if (ch === "?") {
      out += "[^/]";
      continue;
    }
    if (/[|\\{}()[\]^$+?.]/.test(ch)) {
      out += `\\${ch}`;
      continue;
    }
    out += ch;
  }
  out += "$";
  return new RegExp(out, "i");
}

function matchInclude(relPosix, pattern) {
  const regex = globToRegex(pattern);
  return regex.test(relPosix);
}

// ------------------------------
// Main
// ------------------------------
async function main() {
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const nonce = crypto.randomBytes(6).toString("hex");
  const cgRunId = `repo_run_${ts}__${nonce}`;
  const startedAt = new Date().toISOString();

  console.log(`[REPO] Scanning ${resolvedRepo}`);
  console.log(`[REPO] runId=${cgRunId}`);
  console.log(
    `[capsulizeRepo] ownerId=${ownerId} (source=${ownerIdInput.source}), repoPath=${resolvedRepo}, includeMode=${includeMode}, includeCount=${includePatterns.length}, excludeCount=${excludePatterns.length}, limit=${limit === null ? "unlimited" : limit}`
  );
  if (includePatternsInput.length) {
    console.log(`[REPO] include override enabled via --include: ${JSON.stringify(includePatterns)}`);
  } else {
    console.log(`[REPO] include defaults: ${JSON.stringify(includePatterns)}`);
  }

  const scanState = {
    discoveredFiles: 0,
    discoveredDocs: 0,
    excludedCount: 0,
    skippedByInclude: 0,
    skippedByLimit: 0,
    matchedDocs: 0,
    processedCandidates: 0,
    matchedByExt: { md: 0, docx: 0 },
    skipExamples: [],
  };
  const allFiles = await walkDir(resolvedRepo, scanState);

  if (scanState.matchedDocs > 2000) {
    console.warn(
      `[REPO] WARN: matched=${scanState.matchedDocs} docs. Large run; continuing (no implicit cap).`
    );
  }
  console.log(
    `[REPO] summary discoveredTotal=${scanState.discoveredDocs} excluded=${scanState.excludedCount} matched=${scanState.matchedDocs} matchedByExtension=${JSON.stringify(
      scanState.matchedByExt
    )} processed=${allFiles.length}`
  );
  const skippedTotal = scanState.excludedCount + scanState.skippedByInclude + scanState.skippedByLimit;
  console.log(
    `[REPO] skipped total=${skippedTotal} (excluded=${scanState.excludedCount}, includeFilter=${scanState.skippedByInclude}, limit=${scanState.skippedByLimit})`
  );
  if (scanState.skipExamples.length) {
    console.log("[REPO] skipped examples (first 10):");
    for (const sample of scanState.skipExamples) {
      console.log(`  - ${sample.path}: ${sample.reason}`);
    }
  }

  const docReceipts = [];
  const sectionReceipts = [];
  const errors = [];
  let docsInserted = 0;
  let sectionsInserted = 0;

  for (const filePath of allFiles) {
    const relativePath = path.relative(resolvedRepo, filePath).replace(/\\/g, "/");
    const docNodeUri = `agentnet-doc:${relativePath}`;

    try {
      const docNodeId = await ensureNodeWithFallback(docNodeUri, {
        source_url: relativePath,
        domain: "repo",
        owner_id: ownerId,
      });
      const harvestedAt = new Date().toISOString();
      const ft = fileType(filePath);
      const stat = await fs.stat(filePath);
      const fileModifiedAt = stat.mtime.toISOString();

      // --- Extract content based on file type ---
      let title, headings, excerpt, sections;

      if (ft === "docx") {
        const docx = await extractFromDocx(filePath);
        title = docx.title;
        headings = docx.headings;
        excerpt = docx.excerpt;
        sections = parseSectionsFromHtml(docx.html);
      } else {
        const content = await fs.readFile(filePath, "utf8");
        const md = extractFromMarkdown(content);
        title = md.title;
        headings = md.headings;
        excerpt = md.excerpt;
        sections = parseSections(content);
      }

      // --- Doc-level capsule ---
      const docCapsuleUri = `agentnet-capsule:doc:${relativePath}`;

      const docPayload = {
        docNodeUri,
        docRelativePath: relativePath,
        title: title || relativePath,
        headings,
        excerpt,
        fileType: ft,
        fileModifiedAt,
        capturedAt: harvestedAt,
      };

      const docCapsuleJson = buildRepoEnvelope({
        relativePath,
        nodeId: docNodeId,
        title,
        headings,
        excerpt,
        harvestedAt,
        cgRunId,
      });

      await insertDocCapsule({
        capsuleUri: docCapsuleUri,
        ownerId,
        nodeId: docNodeId,
        nodeUri: docNodeUri,
        payload: docPayload,
        capsuleJson: docCapsuleJson,
        producedAt: harvestedAt,
        harvestedAt,
      });

      console.log(`✅ capsulized ${relativePath} nodeId=${docNodeId} capsuleUri=${docCapsuleUri}`);
      docReceipts.push({ relativePath, nodeId: docNodeId, capsuleUri: docCapsuleUri });
      docsInserted++;

      // --- Section-level capsules ---
      for (const sec of sections) {
        const sectionNodeUri = `agentnet-doc-section:${relativePath}#${sec.sectionSlug}`;

        try {
          const sectionNodeId = await ensureNodeWithFallback(sectionNodeUri, {
            source_url: relativePath,
            domain: "repo",
            owner_id: ownerId,
          });
          const capsuleUri = `agentnet-capsule:section:${relativePath}#${sec.sectionSlug}`;

          const payload = {
            parentDocNodeId: docNodeId,
            parentDocNodeUri: docNodeUri,
            docRelativePath: relativePath,
            sectionTitle: sec.sectionTitle,
            sectionSlug: sec.sectionSlug,
            sectionIndex: sec.sectionIndex,
            sectionText: sec.sectionText,
            fileType: ft,
            fileModifiedAt,
            capturedAt: harvestedAt,
          };

          const capsuleJson = {
            "@context": "https://agentnet.ai/context",
            "@type": "agentnet:DocumentSection",
            "agentnet:cgVersion": CG_VERSION,
            "agentnet:cgRunId": cgRunId,
            "agentnet:source": sectionNodeUri,
            "agentnet:registrarNodeId": sectionNodeId,
            "agentnet:captureDate": harvestedAt,
            "agentnet:content": {
              "agentnet:name": sec.sectionTitle,
              "agentnet:description": sec.sectionText,
              "agentnet:relativePath": relativePath,
              "agentnet:sectionSlug": sec.sectionSlug,
              "agentnet:sectionIndex": sec.sectionIndex,
            },
          };

          await insertSectionCapsule({
            capsuleUri,
            ownerId,
            nodeId: sectionNodeId,
            nodeUri: sectionNodeUri,
            payload,
            capsuleJson,
            producedAt: harvestedAt,
            harvestedAt,
          });

          console.log(`  ✅ section capsulized ${relativePath}#${sec.sectionSlug} nodeId=${sectionNodeId}`);
          sectionReceipts.push({ relativePath, sectionSlug: sec.sectionSlug, capsuleUri, nodeId: sectionNodeId });
          sectionsInserted++;
        } catch (secErr) {
          console.error(`  ❌ section ${relativePath}#${sec.sectionSlug}: ${secErr.message}`);
          errors.push({ relativePath, sectionSlug: sec.sectionSlug, error: secErr.message });
        }
      }
    } catch (err) {
      console.error(`❌ ${relativePath}: ${err.message}`);
      errors.push({ relativePath, error: err.message });
    }
  }

  // Write run manifest
  const finishedAt = new Date().toISOString();
  await fs.ensureDir(RUNS_DIR);
  const manifestPath = `${RUNS_DIR}/${cgRunId}.json`;

  const manifest = {
    runId: cgRunId,
    startedAt,
    finishedAt,
    cgVersion: CG_VERSION,
    mode: "repo",
    repoPath: resolvedRepo,
    summary: {
      filesDiscoveredTotal: scanState.discoveredDocs,
      filesExcluded: scanState.excludedCount,
      filesMatched: scanState.matchedDocs,
      filesProcessed: allFiles.length,
      matchedByExtension: scanState.matchedByExt,
      skippedByInclude: scanState.skippedByInclude,
      skippedByLimit: scanState.skippedByLimit,
      docsInserted,
      sectionsInserted,
      errors: errors.length,
    },
    docs: docReceipts,
    sections: sectionReceipts,
    errors,
  };

  await fs.writeFile(manifestPath, JSON.stringify(manifest, null, 2), "utf8");
  console.log(`[REPO] docs=${docsInserted} sections=${sectionsInserted} errors=${errors.length}`);
  console.log(`[REPO] Run manifest: ${manifestPath}`);

  await pool.end();
}

main().catch((err) => {
  console.error("[REPO] Fatal:", err.message);
  process.exit(1);
});
