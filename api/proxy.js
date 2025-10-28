// api/proxy.js
import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import { createClient } from "redis";
import { put, list, del } from "@vercel/blob";

// --- Config ---
const SECRET_KEY = "z9b8x7c6v5n4m3l2k1j9h8g7f6d5s4a3p0o9i8u7y6t5r4e2w1q_!@";
const BLOB_BASE = "https://73vhpohjcehuphyg.public.blob.vercel-storage.com";
const REDIS_URL =
  "redis://default:m21C2O6O7pbghmkp0JcOOS2by1an0941@redis-14551.c83.us-east-1-2.ec2.redns.redis-cloud.com:14551";
const TTL = 600; // 10 min retention
const TMP_DIR = "/tmp";
const memCache = new Map();

let redisPromise;
function getRedis() {
  if (!redisPromise)
    redisPromise = (async () => {
      const r = createClient({ url: REDIS_URL });
      r.on("error", (e) => console.error("Redis:", e));
      await r.connect();
      return r;
    })();
  return redisPromise;
}

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

async function cleanup(limit = 20) {
  try {
    const { blobs } = await list();
    const now = Date.now();
    const stale = blobs
      .filter((b) => now - new Date(b.uploadedAt).getTime() > TTL * 1000)
      .slice(0, limit);
    for (const b of stale) await del(b.url);
    return { deleted: stale.length, total: blobs.length };
  } catch (e) {
    return { error: e.message };
  }
}

// --- Main handler ---
export default async function handler(req, res) {
  // CORS
  if (req.method === "OPTIONS") {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "*");
    return res.status(204).end();
  }
  res.setHeader("Access-Control-Allow-Origin", "*");

  // Cleanup trigger
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
  const tmpPath = path.join(TMP_DIR, key);
  const metaKey = `meta:${key}`;
  const lockKey = `lock:${key}`;
  const redis = await getRedis();

  // --- 1️⃣ Memory Cache ---
  const mem = memCache.get(dest);
  if (mem && mem.expires > Date.now()) {
    res.setHeader("X-Cache-Source", "MEMORY");
    res.setHeader("Content-Type", mem.type);
    res.setHeader(
      "Cache-Control",
      "public, max-age=600, s-maxage=600, stale-while-revalidate=120"
    );
    return res.status(200).send(mem.buf);
  }

  // --- 2️⃣ /tmp cache ---
  try {
    const stat = await fs.stat(tmpPath);
    if (Date.now() - stat.mtimeMs < TTL * 1000) {
      const buf = await fs.readFile(tmpPath);
      res.setHeader("X-Cache-Source", "TMP");
      res.setHeader("Content-Type", "application/octet-stream");
      res.setHeader(
        "Cache-Control",
        "public, max-age=600, s-maxage=600, stale-while-revalidate=120"
      );
      return res.status(200).send(buf);
    }
  } catch {}

  // --- 3️⃣ Redis metadata check (fast existence check, no HEADs) ---
  const metaRaw = await redis.get(metaKey);
  if (metaRaw) {
    const meta = JSON.parse(metaRaw);
    if (Date.now() - meta.uploadedAt < TTL * 1000) {
      const blobResp = await fetch(blobUrl, { cache: "force-cache" });
      if (blobResp.ok) {
        const buf = Buffer.from(await blobResp.arrayBuffer());
        const type =
          blobResp.headers.get("content-type") || "application/octet-stream";
        await fs.writeFile(tmpPath, buf);
        memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
        res.setHeader("X-Cache-Source", "BLOB");
        res.setHeader("Content-Type", type);
        res.setHeader(
          "Cache-Control",
          "public, max-age=600, s-maxage=600, stale-while-revalidate=120"
        );
        return res.status(200).send(buf);
      }
    }
  }

  // --- 4️⃣ Distributed Redis Lock (3s) ---
  const locked = await redis.set(lockKey, "1", { NX: true, EX: 3 });
  if (!locked) {
    // Wait for metadata to appear, no blob polling
    let waited = 0;
    while (waited < 5000) {
      const meta = await redis.get(metaKey);
      if (meta) {
        const blobResp = await fetch(blobUrl, { cache: "force-cache" });
        if (blobResp.ok) {
          const buf = Buffer.from(await blobResp.arrayBuffer());
          const type =
            blobResp.headers.get("content-type") || "application/octet-stream";
          await fs.writeFile(tmpPath, buf);
          memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
          res.setHeader("X-Cache-Source", "WAIT-BLOB");
          res.setHeader("Content-Type", type);
          res.setHeader(
            "Cache-Control",
            "public, max-age=600, s-maxage=600, stale-while-revalidate=120"
          );
          return res.status(200).send(buf);
        }
      }
      await new Promise((r) => setTimeout(r, 250));
      waited += 250;
    }
    res.setHeader("X-Cache-Source", "WAIT-TIMEOUT");
    return res.status(504).send("Timeout waiting for cache");
  }

  // --- 5️⃣ Origin fetch (leader only) ---
  res.setHeader("X-Cache-Lock", "ACQUIRED");
  const origin = await fetch(dest, {
    headers: {
      "User-Agent": "VercelBlobProxy/UltraFast",
      Connection: "keep-alive",
      "Accept-Encoding": "identity",
    },
  });
  if (!origin.ok) throw new Error(`Origin fetch failed: ${origin.status}`);
  const buf = Buffer.from(await origin.arrayBuffer());
  const type =
    origin.headers.get("content-type") || "application/octet-stream";

  // --- Save locally ---
  await fs.writeFile(tmpPath, buf);
  memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });

  // --- Upload asynchronously (don’t block response) ---
  res.setHeader("X-Cache-Source", "ORIGIN");
  res.setHeader("Content-Type", type);
  res.setHeader(
    "Cache-Control",
    "public, max-age=600, s-maxage=600, stale-while-revalidate=120"
  );
  res.write(buf);
  res.end();

  // --- Async background upload ---
  (async () => {
    try {
      await put(key, buf, {
        access: "public",
        contentType: type,
        cacheControlMaxAge: TTL,
        addRandomSuffix: false,
        allowOverwrite: true,
      });
      await redis.set(
        metaKey,
        JSON.stringify({ uploadedAt: Date.now() }),
        { EX: TTL }
      );
    } catch (err) {
      console.warn("Blob upload failed:", err.message);
    } finally {
      await redis.del(lockKey);
    }
  })();
}
