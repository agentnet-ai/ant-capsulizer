// src/db.js
// ------------------------------------------
// MySQL database connection and operations
// Safe for repeated ANT-Capsulizer runs
// ------------------------------------------
require("./bootstrap/env");
const mysql = require("mysql2/promise");
const { issueNode } = require("./clients/registrarClient");

const passwordRaw = process.env.DB_PASSWORD || process.env.DB_PASS || "";
const password = String(passwordRaw).trim();
console.log("[db:init]", {
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT),
  user: process.env.DB_USER,
  database: process.env.DB_NAME,
  passSet: Boolean(password),
  passLen: password.length,
});

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT),
  user: process.env.DB_USER,
  password,
  database: process.env.DB_NAME,
  connectionLimit: 10,
});

function parseRequiredOwnerId(raw, context) {
  const parsed = Number(String(raw ?? "").trim());
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`[db] owner_id required for ${context}`);
  }
  return parsed;
}

async function repairNodeOwnerIdIfMissing(nodeId, ownerId, context) {
  const normalizedOwnerId = parseRequiredOwnerId(ownerId, context);
  const [res] = await pool.query(
    `UPDATE nodes SET owner_id = ? WHERE id = ? AND (owner_id IS NULL OR owner_id = 0)`,
    [normalizedOwnerId, nodeId]
  );
  if (res?.affectedRows > 0) {
    console.log(`[db] repaired nodes.owner_id nodeId=${nodeId} owner_id=${normalizedOwnerId}`);
  }
}

// -----------------------------------------------------
// Upsert or fetch existing URL node (owner context + domain)
// nodes.id is a CHAR(36) UUID issued by ant-registrar
// -----------------------------------------------------
async function upsertNode(owner_slug, source_url, owner_id) {
  if (!source_url) throw new Error("upsertNode: source_url is required");
  if (owner_id === undefined || owner_id === null || String(owner_id).trim() === "") {
    throw new Error("Missing owner_id for URL node issuance");
  }

  let node_uri;
  let domain = null;

  try {
    const u = new URL(source_url);
    node_uri = u.origin;
    domain = u.host;
  } catch (e) {
    throw new Error(`upsertNode: invalid URL source_url=${source_url}`);
  }

  // Check if node already exists by node_uri
  const [rows] = await pool.query(
    `SELECT id FROM nodes WHERE node_uri = ? LIMIT 1`,
    [node_uri]
  );

  if (rows && rows.length) {
    // Refresh metadata on existing row
    await pool.query(
      `UPDATE nodes SET source_url = ?, domain = ?, owner_slug = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
      [source_url, domain, owner_slug || null, rows[0].id]
    );
    await repairNodeOwnerIdIfMissing(rows[0].id, owner_id, "upsertNode(existing)");
    return rows[0].id;
  }

  // Node doesn't exist — issue a UUID from the Registrar
  const issuePayload = {
    owner_id: Number(owner_id),
  };
  if (owner_slug) issuePayload.owner_slug = owner_slug;
  console.log(`[upsertNode] issuing URL node with owner_id=${issuePayload.owner_id}, url=${source_url}`);
  const result = await issueNode(issuePayload);
  const nodeId = result.nodeId;
  if (!nodeId) {
    throw new Error(
      `upsertNode: Registrar issueNode() did not return a nodeId. Response: ${JSON.stringify(result)}`
    );
  }

  try {
    await pool.query(
      `INSERT INTO nodes (id, node_uri, source_url, domain, owner_slug, owner_id) VALUES (?, ?, ?, ?, ?, ?)`,
      [nodeId, node_uri, source_url, domain, owner_slug || null, issuePayload.owner_id]
    );
  } catch (err) {
    const msg = String(err?.message || "");
    if (err?.code === "ER_DUP_ENTRY" || msg.includes("Duplicate entry")) {
      const [existingRows] = await pool.query(
        `SELECT id FROM nodes WHERE node_uri = ? LIMIT 1`,
        [node_uri]
      );
      if (existingRows && existingRows.length) {
        await repairNodeOwnerIdIfMissing(
          existingRows[0].id,
          owner_id,
          "upsertNode(duplicate)"
        );
        console.log(
          `[upsertNode] duplicate node_uri, using existing nodeId=${existingRows[0].id} url=${source_url}`
        );
        return existingRows[0].id;
      }
    }
    throw err;
  }

  // Deterministic return: always read by node_uri.
  const [finalRows] = await pool.query(
    `SELECT id FROM nodes WHERE node_uri = ? LIMIT 1`,
    [node_uri]
  );
  if (!finalRows || !finalRows.length) {
    throw new Error(`upsertNode: insert succeeded but id not found for node_uri=${node_uri}`);
  }
  return finalRows[0].id;
}


// -----------------------------------------------------
// Generic node ensure — accepts node_uri directly
// (for non-URL URIs like agentnet-doc:path/to/file.md)
// -----------------------------------------------------
async function ensureNode(node_uri, meta = {}) {
  if (!node_uri) throw new Error("ensureNode: node_uri is required");
  const ownerId = parseRequiredOwnerId(meta.owner_id, "ensureNode");

  const [rows] = await pool.query(
    `SELECT id FROM nodes WHERE node_uri = ? LIMIT 1`,
    [node_uri]
  );

  if (rows && rows.length) {
    await repairNodeOwnerIdIfMissing(rows[0].id, ownerId, "ensureNode(existing)");
    return rows[0].id;
  }

  const issuePayload = { owner_id: ownerId };
  if (meta.owner_slug) issuePayload.owner_slug = meta.owner_slug;
  const result = await issueNode(issuePayload);
  const nodeId = result.nodeId;
  if (!nodeId) {
    throw new Error(
      `ensureNode: Registrar issueNode() did not return a nodeId. Response: ${JSON.stringify(result)}`
    );
  }

  await pool.query(
    `INSERT INTO nodes (id, node_uri, source_url, domain, owner_slug, owner_id) VALUES (?, ?, ?, ?, ?, ?)`,
    [nodeId, node_uri, meta.source_url || null, meta.domain || null, meta.owner_slug || null, ownerId]
  );

  return nodeId;
}

// -----------------------------------------------------
// Insert or overwrite capsule (unique by capsule_uri)
// -----------------------------------------------------
async function insertCapsule(
  node_id,
  capsule_json,
  fingerprint,
  harvested_at,
  status = "ok",
  opts = {}
) {
  if (!node_id) throw new Error("insertCapsule: node_id is required");
  if (!fingerprint) throw new Error("insertCapsule: fingerprint is required");
  if (!harvested_at) throw new Error("insertCapsule: harvested_at is required");

  // Convert ISO timestamp to MySQL DATETIME (no Z, no ms)
  const producedAt = harvested_at
    .replace("T", " ")
    .replace("Z", "")
    .split(".")[0];

  // Fetch node_uri so capsule_uri can be deterministic and unique
  const [nodeRows] = await pool.query(
    `SELECT node_uri, owner_id FROM nodes WHERE id = ? LIMIT 1`,
    [node_id]
  );
  if (!nodeRows || !nodeRows.length) {
    throw new Error(`insertCapsule: node not found id=${node_id}`);
  }
  const node_uri = nodeRows[0].node_uri;
  const ownerId = parseRequiredOwnerId(
    opts.owner_id != null ? opts.owner_id : nodeRows[0].owner_id,
    "insertCapsule"
  );

  // Deterministic capsule_uri (unique key in your table)
  // Example: https://example.com#capsule/<fingerprint>
  const capsule_uri = `${node_uri}#capsule/${fingerprint}`;

  // Try to infer a type; fall back to an explicit status type
  const inferredType =
    (capsule_json && (capsule_json["@type"] || capsule_json.type)) ||
    opts.type ||
    "ant:StatusCapsule";

  const method = opts.method || "crawl";
  const pipeline_version =
    opts.pipeline_version || process.env.PIPELINE_VERSION || null;

  // Ensure status is actually in the JSON too (useful for consumers)
  const capsuleOut =
    capsule_json && typeof capsule_json === "object"
      ? { ...capsule_json, status }
      : { status, ...opts.meta };

  await pool.query(
    `
    INSERT INTO capsules (
      capsule_uri,
      owner_id,
      node_id,
      fingerprint,
      type,
      method,
      produced_at,
      pipeline_version,
      capsule_json
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON))
    ON DUPLICATE KEY UPDATE
      owner_id = IF(capsules.owner_id IS NULL OR capsules.owner_id = 0, VALUES(owner_id), capsules.owner_id),
      fingerprint = VALUES(fingerprint),
      type = VALUES(type),
      method = VALUES(method),
      produced_at = VALUES(produced_at),
      pipeline_version = VALUES(pipeline_version),
      capsule_json = VALUES(capsule_json),
      updated_at = NOW()
    `,
    [
      capsule_uri,
      ownerId,
      node_id,
      fingerprint,
      inferredType,
      method,
      producedAt,
      pipeline_version,
      JSON.stringify(capsuleOut),
    ]
  );

  const [repair] = await pool.query(
    `UPDATE capsules SET owner_id = ? WHERE capsule_uri = ? AND (owner_id IS NULL OR owner_id = 0)`,
    [ownerId, capsule_uri]
  );
  if (repair?.affectedRows > 0) {
    console.log(`[db] repaired capsules.owner_id capsule_uri=${capsule_uri} owner_id=${ownerId}`);
  }

  return capsule_uri;
}


// -----------------------------------------------------
// Optional helper: run arbitrary read query
// -----------------------------------------------------
async function query(sql, params = []) {
  const [rows] = await pool.query(sql, params);
  return rows;
}

// -----------------------------------------------------
// Exports
// -----------------------------------------------------
module.exports = {
  pool,
  upsertNode,
  ensureNode,
  insertCapsule,
  query,
};
