// api/proxy.js
import crypto from "crypto";
import { put, head, del, list } from "@vercel/blob";

const SECRET_KEY = "z9b8x7c6v5n4m3l2k1j9h8g7f6d5s4a3p0o9i8u7y6t5r4e2w1q_!@";
const BLOB_BASE = "https://73vhpohjcehuphyg.public.blob.vercel-storage.com";
const TTL = 300; // 5 min

// local memory cache for warm instances
const memCache = new Map();

// XOR decrypt
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

// waits up to `timeout` ms for blob to exist
async function waitForBlob(blobUrl, timeout = 10000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const r = await fetch(blobUrl, { method: "HEAD" });
    if (r.ok) return true;
    await new Promise(r => setTimeout(r, 400));
  }
  return false;
}

export default async function handler(req, res) {
  if (req.method === "OPTIONS") {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "*");
    return res.status(204).end();
  }
  res.setHeader("Access-Control-Allow-Origin", "*");

  // Manual cleanup trigger
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
  const lockKey = `${key}.lock`;

  // 1️⃣ Memory quick path
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

  try {
    // 2️⃣ Try to read Blob
    const blobResp = await fetch(blobUrl);
    if (blobResp.ok) {
      const lastMod = new Date(blobResp.headers.get("last-modified"));
      const age = (Date.now() - lastMod.getTime()) / 1000;
      if (age < TTL) {
        const buf = Buffer.from(await blobResp.arrayBuffer());
        const type =
          blobResp.headers.get("content-type") || "application/octet-stream";
        memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
        res.setHeader("X-Cache-Source", "BLOB");
        res.setHeader("X-Blob-Age", Math.round(age).toString());
        res.setHeader("Content-Type", type);
        res.setHeader(
          "Cache-Control",
          `public, max-age=${TTL}, s-maxage=${TTL}, stale-while-revalidate=60`
        );
        return res.status(200).send(buf);
      }
    }

    // 3️⃣ Acquire distributed lock
    const headLock = await fetch(`${BLOB_BASE}/${lockKey}`, { method: "HEAD" });
    if (headLock.ok) {
      // another instance is already fetching/uploading — wait
      const ok = await waitForBlob(blobUrl, 15000);
      if (ok) {
        const resp = await fetch(blobUrl);
        const buf = Buffer.from(await resp.arrayBuffer());
        const type =
          resp.headers.get("content-type") || "application/octet-stream";
        memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
        res.setHeader("X-Cache-Source", "WAIT-BLOB");
        res.setHeader("Content-Type", type);
        res.setHeader(
          "Cache-Control",
          `public, max-age=${TTL}, s-maxage=${TTL}, stale-while-revalidate=60`
        );
        return res.status(200).send(buf);
      }
      res.setHeader("X-Cache-Source", "WAIT-TIMEOUT");
    } else {
      // create lock marker (~tiny blob)
      await put(lockKey, Buffer.from("lock"), {
        access: "public",
        contentType: "text/plain",
        cacheControlMaxAge: 15, // expires soon
        addRandomSuffix: false,
        allowOverwrite: false
      });
      res.setHeader("X-Cache-Lock", "ACQUIRED");
      // 4️⃣ Fetch from origin
      const origin = await fetch(dest, {
        headers: { "User-Agent": "Mozilla/5.0 (VercelBlobProxy/Locking)" },
      });
      if (!origin.ok)
        throw new Error(`Origin fetch failed: ${origin.status}`);
      const buf = Buffer.from(await origin.arrayBuffer());
      const type =
        origin.headers.get("content-type") || "application/octet-stream";

      // upload to Blob
      await put(key, buf, {
        access: "public",
        contentType: type,
        cacheControlMaxAge: TTL,
        addRandomSuffix: false,
        allowOverwrite: true,
      });

      // cleanup lock blob
      try { await del(`${BLOB_BASE}/${lockKey}`); } catch (_) {}

      memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
      res.setHeader("X-Cache-Source", "ORIGIN");
      res.setHeader("X-Blob-Uploaded", "true");
      res.setHeader("Content-Type", type);
      res.setHeader(
        "Cache-Control",
        `public, max-age=${TTL}, s-maxage=${TTL}, stale-while-revalidate=60`
      );
      return res.status(200).send(buf);
    }
  } catch (e) {
    console.error("[Proxy error]", e);
    return res.status(502).send("Error processing request.");
  }
}
