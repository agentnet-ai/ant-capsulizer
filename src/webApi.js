const http = require("http");

const WEB_API_PORT = Number(process.env.CAPSULIZER_API_PORT) || 5176;
const WEB_API_TIMEOUT_MS = Number(process.env.CAPSULIZER_API_TIMEOUT_MS) || 6000;

function startWebApi() {
  const server = http.createServer(async (req, res) => {
    try {
      if (req.method === "GET" && req.url === "/health") {
        return json(res, 200, { ok: true, service: "ant-capsulizer-web-api" });
      }

      if (req.method === "POST" && req.url === "/v1/web/crawl") {
        const body = await readJson(req);
        const q = String(body?.q || "").trim();
        const limit = Math.min(Math.max(Number(body?.limit) || 5, 1), 10);

        if (!q) return json(res, 400, { ok: false, error: "q is required" });

        const sources = await collectWebSources(q, limit);
        return json(res, 200, { ok: true, sources });
      }

      return json(res, 404, { ok: false, error: "not found" });
    } catch (err) {
      return json(res, 500, { ok: false, error: err?.message || "internal" });
    }
  });

  server.listen(WEB_API_PORT, () => {
    console.log(`[WEB-API] listening on http://localhost:${WEB_API_PORT}`);
    console.log("[WEB-API] endpoints: GET /health, POST /v1/web/crawl");
  });
}

async function collectWebSources(query, limit) {
  const directUrls = extractUrls(query).slice(0, limit);
  if (directUrls.length) {
    return crawlUrls(directUrls, limit);
  }

  // Lightweight deterministic web lookup (no API key)
  const lookupUrl = `https://api.duckduckgo.com/?q=${encodeURIComponent(query)}&format=json&no_html=1&skip_disambig=1`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), WEB_API_TIMEOUT_MS);
  try {
    const r = await fetch(lookupUrl, { signal: controller.signal });
    clearTimeout(timer);
    if (!r.ok) return [];
    const data = await r.json();

    const out = [];
    if (data.AbstractURL) {
      out.push({
        url: data.AbstractURL,
        title: data.Heading || data.AbstractURL,
        snippet: data.AbstractText || "",
      });
    }

    const topics = Array.isArray(data.RelatedTopics) ? data.RelatedTopics : [];
    for (const t of topics) {
      if (out.length >= limit) break;
      if (t.FirstURL) {
        out.push({
          url: t.FirstURL,
          title: t.Text?.split(" - ")[0] || t.FirstURL,
          snippet: t.Text || "",
        });
      } else if (Array.isArray(t.Topics)) {
        for (const nested of t.Topics) {
          if (out.length >= limit) break;
          if (nested.FirstURL) {
            out.push({
              url: nested.FirstURL,
              title: nested.Text?.split(" - ")[0] || nested.FirstURL,
              snippet: nested.Text || "",
            });
          }
        }
      }
    }
    return out.slice(0, limit);
  } catch {
    return [];
  } finally {
    clearTimeout(timer);
  }
}

async function crawlUrls(urls, limit) {
  const out = [];
  for (const url of urls) {
    if (out.length >= limit) break;
    const source = await fetchUrlSnippet(url);
    if (source) out.push(source);
  }
  return out;
}

async function fetchUrlSnippet(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), WEB_API_TIMEOUT_MS);
  try {
    const r = await fetch(url, { signal: controller.signal });
    clearTimeout(timer);
    if (!r.ok) return null;
    const html = await r.text();
    const title = (html.match(/<title[^>]*>([^<]+)<\/title>/i) || [])[1] || url;
    const bodyText = stripHtml(html).replace(/\s+/g, " ").trim();
    return {
      url,
      title: title.trim(),
      snippet: bodyText.slice(0, 280),
    };
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

function stripHtml(html) {
  return String(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ");
}

function extractUrls(text) {
  const matches = String(text).match(/https?:\/\/[^\s]+/g) || [];
  return Array.from(new Set(matches.map((u) => u.replace(/[),.;]+$/, ""))));
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (chunk) => {
      raw += chunk;
      if (raw.length > 1e6) req.destroy();
    });
    req.on("end", () => {
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

function json(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(payload),
  });
  res.end(payload);
}

module.exports = { startWebApi };
