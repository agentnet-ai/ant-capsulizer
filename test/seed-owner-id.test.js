const test = require("node:test");
const assert = require("node:assert/strict");

process.env.ANT_WORKER_OWNER_ID = process.env.ANT_WORKER_OWNER_ID || "42";
const { parseRequiredOwnerId, buildSeedJobPayload } = require("../src/seed");

test("parseRequiredOwnerId throws when owner id missing", () => {
  assert.throws(
    () => parseRequiredOwnerId(undefined),
    /Set ANT_WORKER_OWNER_ID \(discover via registrar GET \/v1\/owners\/ant-worker\)/,
  );
});

test("buildSeedJobPayload always includes valid owner_id", () => {
  const payload = buildSeedJobPayload("https://example.com/docs", {
    ownerId: 42,
    ownerSlug: "ant-worker",
  });

  assert.equal(payload.owner_id, 42);
  assert.equal(payload.owner_slug, "ant-worker");
  assert.equal(payload.url, "https://example.com/docs");
});
