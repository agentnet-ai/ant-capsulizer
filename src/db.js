// src/db.js
// ------------------------------------------
// MySQL database connection and operations
// Safe for repeated ANT-Capsulizer runs
// ------------------------------------------
const mysql = require("mysql2/promise");

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT),
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  connectionLimit: 10,
});

// -----------------------------------------------------
// Upsert or fetch existing node (owner_slug + domain)
// -----------------------------------------------------
async function upsertNode(owner_slug, source_url) {
  if (!source_url) throw new Error("upsertNode: source_url is required");

  // Canonical identity for the node (stable)
  // This is what your nodes.node_uri UNIQUE key should represent.
  let node_uri;
  let domain = null;

  try {
    const u = new URL(source_url);
    node_uri = u.origin;
    domain = u.host;
  } catch (e) {
    throw new Error(`upsertNode: invalid URL source_url=${source_url}`);
  }

  // Insert if new; otherwise update metadata on existing row.
  // We use node_uri as the unique key.
  const [result] = await pool.query(
    `
    INSERT INTO nodes (node_uri, source_url, domain, owner_slug)
    VALUES (?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      source_url = VALUES(source_url),
      domain = VALUES(domain),
      owner_slug = VALUES(owner_slug),
      updated_at = CURRENT_TIMESTAMP
    `,
    [node_uri, source_url, domain, owner_slug || null]
  );

  // If inserted, result.insertId is the new id.
  if (result.insertId) return result.insertId;

  // If updated (duplicate), fetch the existing row's id by node_uri.
  const [rows] = await pool.query(`SELECT id FROM nodes WHERE node_uri = ? LIMIT 1`, [node_uri]);
  if (!rows || !rows.length) throw new Error(`upsertNode: upsert succeeded but id not found for node_uri=${node_uri}`);
  return rows[0].id;
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
    `SELECT node_uri FROM nodes WHERE id = ? LIMIT 1`,
    [node_id]
  );
  if (!nodeRows || !nodeRows.length) {
    throw new Error(`insertCapsule: node not found id=${node_id}`);
  }
  const node_uri = nodeRows[0].node_uri;

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
      node_id,
      fingerprint,
      type,
      method,
      produced_at,
      pipeline_version,
      capsule_json
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON))
    ON DUPLICATE KEY UPDATE
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
      node_id,
      fingerprint,
      inferredType,
      method,
      producedAt,
      pipeline_version,
      JSON.stringify(capsuleOut),
    ]
  );

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
  insertCapsule,
  query,
};
