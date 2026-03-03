require("./bootstrap/env");

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { Queue } = require('bullmq');
const IORedis = require('ioredis');

const ANT_WORKER_OWNER_SLUG = process.env.ANT_WORKER_OWNER_SLUG || "ant-worker";
const OWNER_ID_DISCOVERY_HINT =
  "Set ANT_WORKER_OWNER_ID (discover via registrar GET /v1/owners/ant-worker)";

function ownerSlugForPublish() {
  return ANT_WORKER_OWNER_SLUG;
}

function parseRequiredOwnerId(raw, sourceName = "ANT_WORKER_OWNER_ID") {
  if (raw === undefined || raw === null || String(raw).trim() === "") {
    throw new Error(`[SEED] ${sourceName} is required. ${OWNER_ID_DISCOVERY_HINT}`);
  }
  const parsed = Number(String(raw).trim());
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(
      `[SEED] ${sourceName} must be a positive integer. ${OWNER_ID_DISCOVERY_HINT}`
    );
  }
  return parsed;
}

const ANT_WORKER_OWNER_ID = parseRequiredOwnerId(process.env.ANT_WORKER_OWNER_ID);

function ownerIdForPublish(ownerId) {
  return parseRequiredOwnerId(ownerId, "job.owner_id");
}

function assertOwnerIdInvariant(ownerId) {
  if (!Number.isInteger(ownerId) || ownerId <= 0) {
    throw new Error(`[SEED] owner_id invariant failed. Expected positive integer, got "${ownerId}".`);
  }
}

function buildSeedJobPayload(url, { ownerId, ownerSlug = ANT_WORKER_OWNER_SLUG } = {}) {
  const payload = {
    owner_id: ownerIdForPublish(ownerId),
    owner_slug: ownerSlugForPublish(ownerSlug),
    url,
  };
  assertSeedJobPayload(payload);
  return payload;
}

function assertSeedJobPayload(payload) {
  if (!Object.prototype.hasOwnProperty.call(payload, "owner_slug")) {
    throw new Error("[SEED] Job payload must include owner_slug");
  }
  if (!Object.prototype.hasOwnProperty.call(payload, "url")) {
    throw new Error("[SEED] Job payload must include url");
  }
  if (!Object.prototype.hasOwnProperty.call(payload, "owner_id")) {
    throw new Error("[SEED] Job payload must include owner_id");
  }
  assertOwnerIdInvariant(payload.owner_id);
}

function assertOwnerSlugInvariant() {
  const sampleUrls = [
    "https://example.com/products",
    "https://subdomain.other-example.org/about",
  ];

  for (const sampleUrl of sampleUrls) {
    const resolved = ownerSlugForPublish(sampleUrl);
    if (resolved !== ANT_WORKER_OWNER_SLUG) {
      throw new Error(
        `[SEED] owner_slug invariant failed for ${sampleUrl}. Expected "${ANT_WORKER_OWNER_SLUG}", got "${resolved}".`
      );
    }
  }
}

function parseUrlArgs(argv = []) {
  const urls = [];
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--url")) continue;

    let rawValue = "";
    if (token === "--url") {
      rawValue = argv[i + 1] || "";
      i += 1;
    } else if (token.startsWith("--url=")) {
      rawValue = token.slice("--url=".length);
    } else {
      continue;
    }

    for (const part of String(rawValue).split(",")) {
      const trimmed = part.trim();
      if (trimmed) urls.push(trimmed);
    }
  }
  return urls;
}

async function main() {
  assertOwnerSlugInvariant();

  // identical connection config as worker
  const connection = new IORedis({
    host: process.env.REDIS_HOST,
    port: Number(process.env.REDIS_PORT),
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
  });

  const queueName = 'capsuleQueue';
  const q = new Queue(queueName, { connection });
  const qConn = q.opts?.connection || connection;
  const qPrefix = q.opts?.prefix || "bull";
  const qHost = qConn?.options?.host || process.env.REDIS_HOST;
  const qPort = qConn?.options?.port || Number(process.env.REDIS_PORT);
  const qDb = qConn?.options?.db ?? 0;
  console.log(`[seed:init] name=${queueName}, prefix=${qPrefix}, redis=${qHost}:${qPort}, db=${qDb}`);

  const cliUrls = parseUrlArgs(process.argv.slice(2));
  let urlsToSeed = [];
  if (cliUrls.length > 0) {
    urlsToSeed = cliUrls;
    console.log(`📥 Seeding ${urlsToSeed.length} job(s) from --url ...`);
  } else {
    // Path to the seed CSV file
    const seedPath = path.resolve(__dirname, '../seeds/instabuild-seed.csv');
    const csvData = fs.readFileSync(seedPath, 'utf-8');
    const records = parse(csvData, { columns: false, skip_empty_lines: true });
    urlsToSeed = records
      .map((row) => row[0]?.trim())
      .filter((url) => Boolean(url));
    console.log(`📥 Seeding ${urlsToSeed.length} jobs from ${seedPath} ...`);
  }

  for (const url of urlsToSeed) {
    const payload = buildSeedJobPayload(url, { ownerId: ANT_WORKER_OWNER_ID });
    await q.add('capsule', payload);
    console.log(`➕ Enqueued ${url}`);
  }

  console.log('✅ Seeding complete.');
  await connection.quit();
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err.message || err);
    process.exit(1);
  });
}

module.exports = {
  parseRequiredOwnerId,
  buildSeedJobPayload,
  parseUrlArgs,
};
