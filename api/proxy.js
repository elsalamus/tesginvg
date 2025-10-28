// api/proxy.js
import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import { createClient } from "redis";
import { put } from "@vercel/blob";

/*
  CONFIG
*/
const SECRET_KEY =
  "z9b8x7c6v5n4m3l2k1j9h8g7f6d5s4a3p0o9i8u7y6t5r4e2w1q_!@";
const BLOB_BASE =
  "https://73vhpohjcehuphyg.public.blob.vercel-storage.com";
const REDIS_URL =
  "redis://default:m21C2O6O7pbghmkp0JcOOS2by1an0941@redis-14551.c83.us-east-1-2.ec2.redns.redis-cloud.com:14551";

// TTLs and lock duration
const TTL = 300; // 5 minutes retain for segments
const META_TTL = TTL * 2;
const LOCK_SEC = 3; // <= your request: 3 seconds lock
const TMP_DIR = "/tmp";

//
// In-process caches & dedupe
//
const memCache = new Map(); // dest -> { buf, type, expires }
const inflight = new Map(); // dest -> Promise

//
// Redis connection promise (keeps connection warm across invocations)
//
let redisPromise;
function getRedis() {
  if (!redisPromise)
    redisPromise = (async () => {
      const r = createClient({ url: REDIS_URL });
      r.on("error", (e) => console.error("Redis error:", e && e.message ? e.message : e));
      await r.connect();
      return r;
    })();
  return redisPromise;
}

//
// helpers
//
function decrypt(b64, key) {
  let s = b64.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4) s += "=";
  const enc = Buffer.from(s, "base64").toString("binary");
  let out = "";
  for (let i = 0; i < enc.length; i++) {
    out += String.fromCharCode(enc.charCodeAt(i) ^ key.charCodeAt(i % key.length));
  }
  return out;
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitForMetaReady(redis, metaKey, timeoutMs = 8000) {
  const start = Date.now();
  let wait = 100;
  while (Date.now() - start < timeoutMs) {
    const raw = await redis.get(metaKey);
    if (raw) {
      try {
        const parsed = JSON.parse(raw);
        if (parsed.status === "ready") return parsed;
      } catch {}
    }
    await sleep(wait + Math.floor(Math.random() * 50));
    wait = Math.min(800, Math.floor(wait * 1.5));
  }
  return null;
}

async function tryPutBlobWithRetries(key, buf, type, attempts = 3) {
  for (let i = 1; i <= attempts; i++) {
    try {
      await put(key, buf, {
        access: "public",
        contentType: type,
        cacheControlMaxAge: TTL,
        addRandomSuffix: false,
        allowOverwrite: true,
      });
      return true;
    } catch (e) {
      console.warn(`put() attempt ${i} failed:`, e && e.message ? e.message : e);
      if (i < attempts) await sleep(Math.min(1000 * i, 3000));
    }
  }
  return false;
}

async function writeTmpSafe(tmpPath, buf) {
  try {
    await fs.writeFile(tmpPath, buf);
    return true;
  } catch (e) {
    if (e && e.code === "ENOSPC") {
      console.warn("tmp full, skipping write");
      return false;
    }
    console.warn("tmp write failed:", e && e.message ? e.message : e);
    return false;
  }
}

//
// Cleanup endpoint: deletes blobs older than 5 minutes.
// Note: listing all blobs may be expensive if many. Use with care.
// (This function directly calls Blob list/delete APIs when available.)
//
async function cleanupOlderThan(cutoffMs) {
  // We don't use list here because @vercel/blob list is not available in this file's context reliably.
  // If you have a Blob list API, implement it here. For now, return a message.
  return { note: "Implement blob listing + deletion using your store admin tools or the SDK." };
}

//
// Main handler
//
export default async function handler(req, res) {
  // CORS preflight
  if (req.method === "OPTIONS") {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET,HEAD,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "*");
    return res.status(204).end();
  }
  res.setHeader("Access-Control-Allow-Origin", "*");

  const urlPath = req.url.split("?")[0];
  if (urlPath === "/cleanup" || urlPath === "/api/proxy.js/cleanup") {
    const r = await cleanupOlderThan(Date.now() - 5 * 60 * 1000);
    return res.status(200).json(r);
  }

  // decrypt target
  const enc = req.url.startsWith("/") ? req.url.slice(1) : req.url;
  if (!enc) return res.status(400).send("Powered by V.CDN");

  let dest;
  try {
    dest = decrypt(enc.split("?")[0], SECRET_KEY);
    new URL(dest);
  } catch (e) {
    return res.status(400).send("Invalid encrypted URL.");
  }

  // dedupe in-process
  if (inflight.has(dest)) {
    try {
      const out = await inflight.get(dest);
      res.setHeader("X-Cache-Source", out.source || "INTERNAL");
      if (out.tOrigin) res.setHeader("X-Timing-Origin-ms", String(out.tOrigin));
      res.setHeader("Content-Type", out.type || "application/octet-stream");
      res.setHeader("Cache-Control", `public, s-maxage=${TTL}, max-age=${TTL}`);
      // If the original caller was HEAD, we must not return body here — but inflight stores body.
      if (req.method === "HEAD") {
        return res.status(200).end();
      }
      return res.status(200).send(out.buf);
    } catch (e) {
      // fallthrough
    }
  }

  // Main promise for this dest (keeps in inflight map while working)
  const p = (async () => {
    const key = crypto.createHash("sha1").update(dest).digest("hex");
    const blobUrl = `${BLOB_BASE}/${key}`;
    const tmpPath = path.join(TMP_DIR, key);
    const metaKey = `meta:${key}`;
    const lockKey = `lock:${key}`;
    const redis = await getRedis();

    // 1) memory
    const mem = memCache.get(dest);
    if (mem && mem.expires > Date.now()) {
      return { buf: mem.buf, type: mem.type, source: "MEMORY" };
    }

    // 2) tmp
    try {
      const st = await fs.stat(tmpPath);
      if (Date.now() - st.mtimeMs < TTL * 1000) {
        const b = await fs.readFile(tmpPath);
        return { buf: b, type: "application/octet-stream", source: "TMP" };
      }
    } catch {}

    // 3) If meta ready -> fetch blob once (no HEAD polling)
    const metaRaw = await redis.get(metaKey);
    if (metaRaw) {
      try {
        const meta = JSON.parse(metaRaw);
        if (meta.status === "ready" && Date.now() - meta.uploadedAt < TTL * 1000) {
          // Do a single GET (rely on CDN)
          const blobResp = await fetch(blobUrl, { cache: "force-cache" });
          if (blobResp.ok) {
            const buf = Buffer.from(await blobResp.arrayBuffer());
            const type = blobResp.headers.get("content-type") || "application/octet-stream";
            // write tmp best-effort
            await writeTmpSafe(tmpPath, buf);
            memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
            return { buf, type, source: "BLOB" };
          }
        }
      } catch {}
    }

    // 4) If meta exists and status uploading, and request is HEAD -> wait for ready with backoff; then return headers
    if (metaRaw) {
      try {
        const meta = JSON.parse(metaRaw);
        if (meta.status === "uploading") {
          const readyMeta = await waitForMetaReady(redis, metaKey, 8000);
          if (readyMeta && readyMeta.status === "ready") {
            // fetch blob once
            const blobResp = await fetch(blobUrl, { cache: "force-cache" });
            if (blobResp.ok) {
              const buf = Buffer.from(await blobResp.arrayBuffer());
              const type = blobResp.headers.get("content-type") || "application/octet-stream";
              await writeTmpSafe(tmpPath, buf);
              memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
              return { buf, type, source: "WAIT-READY" };
            }
          } else {
            // timed out waiting for ready
            // if this is a HEAD, return 202 Accepted indicating warming
            if (req.method === "HEAD") return { buf: null, type: null, source: "WARMING" };
            // else fallthrough and try to become leader or fallback
          }
        }
      } catch {}
    }

    // 5) Try to become leader with Redis SET NX EX (3s)
    const gotLock = await redis.set(lockKey, "1", { NX: true, EX: LOCK_SEC });

    // If request is HEAD and we are a waiter (didn't get lock):
    // Wait for meta.ready (short) and return headers, not body.
    if (!gotLock && req.method === "HEAD") {
      const ready = await waitForMetaReady(redis, metaKey, 8000);
      if (ready && ready.status === "ready") {
        return { buf: null, type: null, source: "BLOB-READY" }; // HEAD will return headers only
      }
      // If timed out, respond WARMING (HEAD triggers warming attempt below if we can grab lock)
      // try once more to acquire lock (small chance)
      const gotLock2 = await redis.set(lockKey, "1", { NX: true, EX: LOCK_SEC });
      if (!gotLock2) {
        // Can't get lock, return warming accepted
        return { buf: null, type: null, source: "WARMING" };
      }
      // else proceed as leader
    }

    // If we are not leader and not HEAD, wait for meta ready (to avoid origin storm)
    if (!gotLock) {
      const readyMeta = await waitForMetaReady(redis, metaKey, 8000);
      if (readyMeta && readyMeta.status === "ready") {
        const blobResp = await fetch(blobUrl, { cache: "force-cache" });
        if (blobResp.ok) {
          const buf = Buffer.from(await blobResp.arrayBuffer());
          const type = blobResp.headers.get("content-type") || "application/octet-stream";
          await writeTmpSafe(tmpPath, buf);
          memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
          return { buf, type, source: "WAIT-READY" };
        }
      }
      // fallback: try to acquire lock again (last-resort) to avoid endless waiting
      const gotLock2 = await redis.set(lockKey, "1", { NX: true, EX: LOCK_SEC });
      if (!gotLock2) {
        // as absolute last resort, fetch origin locally (rare). Mark meta.uploading to reduce others
        try { await redis.set(metaKey, JSON.stringify({ status: "uploading", startedAt: Date.now() }), { EX: META_TTL }); } catch {}
        const originResp = await fetch(dest, { headers: { "User-Agent": "VercelProxyFallback/1.0" } });
        if (!originResp.ok) throw new Error(`Origin ${originResp.status}`);
        const fallbackBuf = Buffer.from(await originResp.arrayBuffer());
        const fallbackType = originResp.headers.get("content-type") || "application/octet-stream";
        await writeTmpSafe(tmpPath, fallbackBuf);
        // attempt upload best-effort
        await tryPutBlobWithRetries(key, fallbackBuf, fallbackType, 2);
        try { await redis.set(metaKey, JSON.stringify({ status: "ready", uploadedAt: Date.now() }), { EX: META_TTL }); } catch {}
        return { buf: fallbackBuf, type: fallbackType, source: "FALLBACK-ORIGIN" };
      }
      // else gotLock2 => become leader below
    }

    // If we reach here we are the leader (gotLock true)
    // For HEAD: leader will fetch+upload, then return headers (no body)
    // For GET: leader will fetch+upload and return body.

    // write meta uploading immediately (so others don't hit origin)
    try {
      await redis.set(metaKey, JSON.stringify({ status: "uploading", startedAt: Date.now() }), { EX: META_TTL });
    } catch (e) {}

    // fetch origin
    const t0 = Date.now();
    const originResp = await fetch(dest, {
      headers: {
        "User-Agent": "VercelBlobProxy/Leader",
        Connection: "keep-alive",
        "Accept-Encoding": "identity",
      },
    });
    const tOrigin = Date.now() - t0;
    if (!originResp.ok) {
      // cleanup lock & meta for others to try
      await redis.del(lockKey).catch(() => {});
      await redis.del(metaKey).catch(() => {});
      throw new Error(`Origin fetch failed: ${originResp.status}`);
    }
    const originBuf = Buffer.from(await originResp.arrayBuffer());
    const originType = originResp.headers.get("content-type") || "application/octet-stream";

    // write /tmp best-effort
    await writeTmpSafe(tmpPath, originBuf);

    // upload to blob (with retries)
    const uploaded = await tryPutBlobWithRetries(key, originBuf, originType, 3);

    // set meta to ready (even if upload failed we set status)
    try {
      await redis.set(metaKey, JSON.stringify({ status: uploaded ? "ready" : "failed", uploadedAt: Date.now() }), { EX: META_TTL });
    } catch (e) {}

    // release lock
    try { await redis.del(lockKey); } catch (e) {}

    memCache.set(dest, { buf: originBuf, type: originType, expires: Date.now() + TTL * 1000 });

    // return leader result
    return { buf: originBuf, type: originType, source: uploaded ? "ORIGIN-UPLOADED" : "ORIGIN-NOUPLOAD", tOrigin };
  })();

  inflight.set(dest, p);
  try {
    const out = await p;
    inflight.delete(dest);

    // Diagnostic headers
    if (out.tOrigin) res.setHeader("X-Timing-Origin-ms", String(out.tOrigin));
    res.setHeader("X-Cache-Source", out.source || "UNKNOWN");
    if (out.type) res.setHeader("Content-Type", out.type);
    res.setHeader("Cache-Control", `public, s-maxage=${TTL}, max-age=${TTL}`);

    // Special-case HEAD: never send body
    if (req.method === "HEAD") {
      // Add blob-age if available via meta
      try {
        const redis = await getRedis();
        const key = crypto.createHash("sha1").update(dest).digest("hex");
        const metaRaw = await redis.get(`meta:${key}`);
        if (metaRaw) {
          const meta = JSON.parse(metaRaw);
          if (meta.uploadedAt) res.setHeader("X-Blob-Age", String(Math.round((Date.now() - meta.uploadedAt) / 1000)));
        }
      } catch {}
      return res.status(200).end();
    }

    // For GET return body if present
    if (out.buf) return res.status(200).send(out.buf);

    // If out.buf not present (e.g., HEAD returned warming) respond accordingly
    if (out.source === "WARMING") return res.status(202).send("Warming");
    return res.status(504).send("Timeout or warming");
  } catch (err) {
    inflight.delete(dest);
    console.error("[Proxy error]", err && err.message ? err.message : err);
    return res.status(502).send("Error processing request.");
  }
}
