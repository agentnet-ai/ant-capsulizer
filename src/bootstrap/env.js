const path = require("path");
const dotenv = require("dotenv");

const ENV_PATH = path.resolve(__dirname, "../../.env");
const REQUIRED_DB_VARS = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"];

const result = dotenv.config({ path: ENV_PATH, override: false, quiet: true  });
const loadedCount = result && result.parsed ? Object.keys(result.parsed).length : 0;

// Keep backward compatibility with older DB_PASS usage.
const dbPassTrimmed = String(process.env.DB_PASS || "").trim();
const dbPasswordTrimmed = String(process.env.DB_PASSWORD || "").trim();
if (!dbPasswordTrimmed && dbPassTrimmed) {
  process.env.DB_PASSWORD = dbPassTrimmed;
}
if (!dbPassTrimmed && dbPasswordTrimmed) {
  process.env.DB_PASS = dbPasswordTrimmed;
}

const missing = REQUIRED_DB_VARS.filter((key) => !String(process.env[key] || "").trim());
console.log(`[env] loaded ${ENV_PATH} (${loadedCount} keys), DB_PASSWORD=${process.env.DB_PASSWORD ? "set" : "missing"}`);

if (missing.length) {
  throw new Error(
    `[env] Missing required env vars: ${missing.join(", ")}. Expected .env at ${ENV_PATH}`
  );
}

module.exports = {
  ENV_PATH,
};
