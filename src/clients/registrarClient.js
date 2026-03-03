// src/clients/registrarClient.js
const REGISTRAR_BASE_URL = process.env.REGISTRAR_BASE_URL || "http://localhost:4002";
const TIMEOUT_MS = 8000;

async function registrarFetch(urlPath, options = {}) {
  const url = `${REGISTRAR_BASE_URL}${urlPath}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    const body = await res.text();
    if (!res.ok) {
      throw new Error(
        `Registrar ${options.method || "GET"} ${url} returned HTTP ${res.status}: ${body.slice(0, 500)}`
      );
    }
    return JSON.parse(body);
  } catch (err) {
    if (err.name === "AbortError") {
      throw new Error(`Registrar request timed out after ${TIMEOUT_MS}ms: ${url}`);
    }
    const msg = String(err?.cause?.code || err?.message || "");
    if (msg.includes("ECONNREFUSED") || msg.includes("fetch failed")) {
      throw new Error(
        `Start ant-registrar at ${REGISTRAR_BASE_URL}`
      );
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

async function issueNode(payload = {}) {
  return registrarFetch("/nodes", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

async function getNode(nodeId) {
  return registrarFetch(`/nodes/${encodeURIComponent(nodeId)}`);
}

module.exports = { issueNode, getNode };
