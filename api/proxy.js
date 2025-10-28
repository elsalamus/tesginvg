// api/proxy.js
import crypto from "crypto";
import { put, list, del } from "@vercel/blob";

const SECRET_KEY =
  "z9b8x7c6v5n4m3l2k1j9h8g7f6d5s4a3p0o9i8u7y6t5r4e2w1q_!@";
const BLOB_BASE =
  "https://73vhpohjcehuphyg.public.blob.vercel-storage.com";
const TTL = 300; // 5 minutes

// ephemeral memory cache & in-flight locks
const memCache = new Map();
const inflight = new Map();

function decrypt(b64, key) {
  let s = b64.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4) s += "=";
  const enc = Buffer.from(s, "base64").toString("binary");
  let out = "";
  for (let i = 0; i < enc.length; i++) {
    out += String.fromCharCode(
      enc.charCodeAt(i) ^ key.charCodeAt(i % key.length)
    );
  }
  return out;
}

// cleanup endpoint
async function cleanup(limit = 10) {
  try {
    const { blobs } = await list();
    const now = Date.now();
    const stale = blobs
      .filter(b => now - new Date(b.uploadedAt).getTime() > TTL * 1000)
      .slice(0, limit);
    for (const b of stale) await del(b.url);
    return { deleted: stale.length, total: blobs.length };
  } catch (e) {
    return { error: e.message };
  }
}

export default async function handler(req, res) {
  if (req.method === "OPTIONS") {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "*");
    return res.status(204).end();
  }
  res.setHeader("Access-Control-Allow-Origin", "*");

  // Manual cleanup endpoint
  if (req.url.split("?")[0] === "/cleanup") {
    const result = await cleanup();
    return res.status(200).json(result);
  }

  const enc = req.url.startsWith("/") ? req.url.slice(1) : req.url;
  if (!enc) return res.status(400).send("Powered by V.CDN");

  let dest;
  try {
    dest = decrypt(enc.split("?")[0], SECRET_KEY);
    new URL(dest);
  } catch {
    return res.status(400).send("Invalid encrypted URL.");
  }

  const key = crypto.createHash("sha1").update(dest).digest("hex");
  const blobUrl = `${BLOB_BASE}/${key}`;

  // ---- Memory cache quick path ----
  const mem = memCache.get(dest);
  if (mem && mem.expires > Date.now()) {
    res.setHeader("X-Cache-Source", "MEMORY");
    res.setHeader("Content-Type", mem.type);
    res.setHeader(
      "Cache-Control",
      `public, max-age=${TTL}, s-maxage=${TTL}, stale-while-revalidate=60`
    );
    return res.status(200).send(mem.buf);
  }

  // ---- In-flight deduplication ----
  if (inflight.has(dest)) {
    const data = await inflight.get(dest);
    res.setHeader("X-Cache-Source", "WAIT");
    res.setHeader("Content-Type", data.type);
    res.setHeader(
      "Cache-Control",
      `public, max-age=${TTL}, s-maxage=${TTL}, stale-while-revalidate=60`
    );
    return res.status(200).send(data.buf);
  }

  const promise = (async () => {
    try {
      // Try Blob
      const blobResp = await fetch(blobUrl);
      if (blobResp.ok) {
        const lastMod = new Date(blobResp.headers.get("last-modified"));
        const age = Math.round((Date.now() - lastMod.getTime()) / 1000);
        if (age < TTL) {
          const buf = Buffer.from(await blobResp.arrayBuffer());
          const type =
            blobResp.headers.get("content-type") || "application/octet-stream";
          memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
          res.setHeader("X-Cache-Source", "BLOB");
          res.setHeader("X-Blob-Age", age.toString());
          return { buf, type };
        }
      }

      // Fetch origin
      const origin = await fetch(dest, {
        headers: { "User-Agent": "Mozilla/5.0 (VercelBlobProxy/Stable)" },
      });
      if (!origin.ok)
        throw new Error(`Origin fetch failed: ${origin.status}`);
      const buf = Buffer.from(await origin.arrayBuffer());
      const type =
        origin.headers.get("content-type") || "application/octet-stream";

      // Upload to Blob
      await put(key, buf, {
        access: "public",
        contentType: type,
        cacheControlMaxAge: TTL,
        addRandomSuffix: false,
        allowOverwrite: true,
      });

      memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
      res.setHeader("X-Cache-Source", "ORIGIN");
      res.setHeader("X-Blob-Uploaded", "true");
      return { buf, type };
    } finally {
      inflight.delete(dest);
    }
  })();

  inflight.set(dest, promise);

  try {
    const data = await promise;
    if (!res.getHeader("X-Cache-Source")) res.setHeader("X-Cache-Source", "UNKNOWN");
    res.setHeader("Content-Type", data.type);
    res.setHeader(
      "Cache-Control",
      `public, max-age=${TTL}, s-maxage=${TTL}, stale-while-revalidate=60`
    );
    return res.status(200).send(data.buf);
  } catch (err) {
    console.error("[Proxy error]", err);
    res.status(502).send("Error processing request.");
  }
}
