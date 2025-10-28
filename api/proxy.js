// api/proxy.js
import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import { createClient } from "redis";
import { put, list, del } from "@vercel/blob";

/**
 * CONFIG — edit these values if needed
 */
const SECRET_KEY = "z9b8x7c6v5n4m3l2k1j9h8g7f6d5s4a3p0o9i8u7y6t5r4e2w1q_!@";
const BLOB_BASE = "https://73vhpohjcehuphyg.public.blob.vercel-storage.com";
const REDIS_URL =
  "redis://default:m21C2O6O7pbghmkp0JcOOS2by1an0941@redis-14551.c83.us-east-1-2.ec2.redns.redis-cloud.com:14551";
const TTL = 600; // blob / meta TTL in seconds (10 min)
const LOCK_SEC = 3; // global leader lock
const TMP_DIR = "/tmp";

/**
 * In-process maps (dedupe multiple concurrent requests in the same instance)
 */
const inflight = new Map(); // dest -> Promise
const memCache = new Map(); // dest -> { buf, type, expires }

/**
 * Redis connection (single connecting promise)
 */
let redisPromise;
function getRedis() {
  if (!redisPromise)
    redisPromise = (async () => {
      const r = createClient({ url: REDIS_URL });
      r.on("error", (e) => console.error("Redis client error:", e));
      await r.connect();
      return r;
    })();
  return redisPromise;
}

/**
 * Helper: Base64 XOR decrypt (keeps your original algorithm)
 */
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

/**
 * Helper: wait for notification using a dedicated Redis client + BRPOP
 * - returns true if notified, false if timed out
 */
async function waitForNotify(key, timeoutMs = 5000) {
  const subKey = `notify:${key}`;
  const redis = await getRedis();
  // use a duplicate client for blocking BRPOP to avoid interfering with main client
  const dup = redis.duplicate();
  try {
    await dup.connect();
    // brPop expects seconds; it returns null on timeout
    const seconds = Math.max(1, Math.ceil(timeoutMs / 1000));
    const res = await dup.brPop(subKey, seconds);
    await dup.quit();
    return !!res;
  } catch (e) {
    try { await dup.quit(); } catch (_) {}
    console.warn("waitForNotify error:", e.message);
    return false;
  }
}

/**
 * Cleanup: delete all blobs older than cutoff (5 minutes)
 */
async function cleanupAllOlderThan(cutoffMs) {
  try {
    const { blobs } = await list();
    let deleted = 0;
    for (const b of blobs) {
      if (new Date(b.uploadedAt).getTime() < cutoffMs) {
        await del(b.url);
        deleted++;
      }
    }
    return { deleted };
  } catch (e) {
    return { error: e.message };
  }
}

/**
 * Main handler
 */
export default async function handler(req, res) {
  // Basic CORS + preflight
  if (req.method === "OPTIONS") {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET,HEAD,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "*");
    return res.status(204).end();
  }
  res.setHeader("Access-Control-Allow-Origin", "*");

  // If user called /cleanup or scheduled cron path
  const urlPath = req.url.split("?")[0];
  if (urlPath === "/cleanup" || urlPath === "/api/proxy.js/cleanup") {
    const cutoff = Date.now() - 5 * 60 * 1000; // older than 5 minutes
    const result = await cleanupAllOlderThan(cutoff);
    return res.status(200).json({ ok: true, ...result });
  }

  // Encrypted path handling
  const enc = req.url.startsWith("/") ? req.url.slice(1) : req.url;
  if (!enc) return res.status(400).send("Powered by V.CDN");

  let dest;
  try {
    dest = decrypt(enc.split("?")[0], SECRET_KEY);
    new URL(dest); // validate
  } catch (e) {
    return res.status(400).send("Invalid encrypted URL.");
  }

  // dedupe inside same instance
  if (inflight.has(dest)) {
    // Wait for existing promise
    try {
      const out = await inflight.get(dest);
      res.setHeader("X-Cache-Source", out.source || "INTERNAL");
      res.setHeader("Content-Type", out.type || "application/octet-stream");
      res.setHeader("Cache-Control", `public, s-maxage=${TTL}, max-age=${TTL}`);
      return res.status(200).send(out.buf);
    } catch (e) {
      // fallthrough to new attempt
    }
  }

  const promise = (async () => {
    const key = crypto.createHash("sha1").update(dest).digest("hex");
    const blobUrl = `${BLOB_BASE}/${key}`;
    const tmpPath = path.join(TMP_DIR, key);
    const metaKey = `meta:${key}`;
    const fetchingKey = `fetching:${key}`;
    const notifyKey = `notify:${key}`;
    const redis = await getRedis();

    // 1) memory fast path
    const mem = memCache.get(dest);
    if (mem && mem.expires > Date.now()) {
      return { buf: mem.buf, type: mem.type, source: "MEMORY" };
    }

    // 2) /tmp fast path
    try {
      const stat = await fs.stat(tmpPath);
      if (Date.now() - stat.mtimeMs < TTL * 1000) {
        const buf = await fs.readFile(tmpPath);
        return { buf, type: "application/octet-stream", source: "TMP" };
      }
    } catch (e) {
      // ignore
    }

    // 3) Check redis meta (ready)
    const metaRaw = await redis.get(metaKey);
    if (metaRaw) {
      try {
        const meta = JSON.parse(metaRaw);
        if (meta.status === "ready" && Date.now() - meta.uploadedAt < TTL * 1000) {
          // blob should exist; fetch once
          const blobResp = await fetch(blobUrl, { cache: "force-cache" });
          if (blobResp.ok) {
            const buf = Buffer.from(await blobResp.arrayBuffer());
            const type = blobResp.headers.get("content-type") || "application/octet-stream";
            try { await fs.writeFile(tmpPath, buf); } catch (e) { if (e.code !== "ENOSPC") console.warn("tmp write failed:", e.message); }
            memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
            return { buf, type, source: "BLOB" };
          }
          // if blob missing, continue to leader path
        }
        // if meta.status === 'fetching', fall through to fetchingKey handling
      } catch (e) {
        // broken meta; ignore
      }
    }

    // 4) If someone is already fetching, wait for notification (no HEAD)
    const fetching = await redis.get(fetchingKey);
    if (fetching) {
      // Wait on notify list (blocking BRPOP) for up to 5s
      const notified = await waitForNotify(key, 5000);
      if (notified) {
        // Now blob should exist
        const blobResp = await fetch(blobUrl, { cache: "force-cache" });
        if (blobResp.ok) {
          const buf = Buffer.from(await blobResp.arrayBuffer());
          const type = blobResp.headers.get("content-type") || "application/octet-stream";
          try { await fs.writeFile(tmpPath, buf); } catch (e) {}
          memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
          return { buf, type, source: "WAIT-NOTIFY" };
        }
      }
      // notified timed out or blob missing → fall through to try leader
    }

    // 5) Try to become leader: atomic SET NX
    const instanceId = crypto.randomBytes(8).toString("hex");
    const setRes = await redis.set(fetchingKey, instanceId, { NX: true, EX: LOCK_SEC });
    if (!setRes) {
      // couldn't get lock (rare due to races) → wait for notify, then try blob
      const notified = await waitForNotify(key, 5000);
      if (notified) {
        const blobResp = await fetch(blobUrl, { cache: "force-cache" });
        if (blobResp.ok) {
          const buf = Buffer.from(await blobResp.arrayBuffer());
          const type = blobResp.headers.get("content-type") || "application/octet-stream";
          try { await fs.writeFile(tmpPath, buf); } catch (e) {}
          memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });
          return { buf, type, source: "WAIT-FALLBACK" };
        }
      }
      // if still nothing, attempt to proceed as last resort to avoid infinite waits
    }

    // Leader path: set meta = fetching immediately so other regions won't hit origin
    try {
      await redis.set(metaKey, JSON.stringify({ status: "fetching", startedAt: Date.now() }), { EX: TTL * 2 });
    } catch (e) {
      // ignore
    }

    // 6) Fetch origin (leader)
    let originBuf;
    let originType = "application/octet-stream";
    let originTime = 0;
    try {
      const t0 = Date.now();
      const originResp = await fetch(dest, {
        headers: {
          "User-Agent": "VercelBlobProxy/GlobalLeader",
          Connection: "keep-alive",
          "Accept-Encoding": "identity",
        },
      });
      originTime = Date.now() - t0;
      if (!originResp.ok) throw new Error(`Origin ${originResp.status}`);
      originBuf = Buffer.from(await originResp.arrayBuffer());
      originType = originResp.headers.get("content-type") || originType;
    } catch (e) {
      // leader failed to fetch origin -> clear fetchingKey & meta so others can try
      try { await redis.del(fetchingKey); } catch (_) {}
      try { await redis.del(metaKey); } catch (_) {}
      throw e;
    }

    // 7) Save to /tmp (best-effort)
    try { await fs.writeFile(tmpPath, originBuf); } catch (e) { if (e.code !== "ENOSPC") console.warn("tmp write:", e.message); }

    // 8) Upload to blob with retries (leader)
    let uploaded = false;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        await put(key, originBuf, {
          access: "public",
          contentType: originType,
          cacheControlMaxAge: TTL,
          addRandomSuffix: false,
          allowOverwrite: true,
        });
        uploaded = true;
        break;
      } catch (e) {
        console.warn(`Blob PUT attempt ${attempt} failed:`, e.message);
        // small backoff
        await new Promise((r) => setTimeout(r, Math.min(1000 * attempt, 3000)));
      }
    }

    // 9) Set meta = ready (even if upload failed we indicate attempt) with a TTL longer than blob TTL
    const now = Date.now();
    try {
      await redis.set(metaKey, JSON.stringify({ status: uploaded ? "ready" : "failed", uploadedAt: now }), { EX: TTL * 2 });
    } catch (e) {}

    // 10) Notify waiters by LPUSH to notify list (non-blocking)
    try {
      await redis.lPush(notifyKey, "1");
      // trim to small size
      await redis.lTrim(notifyKey, 0, 10);
    } catch (e) {
      console.warn("notify push failed:", e.message);
    }

    // 11) release fetching key
    try { await redis.del(fetchingKey); } catch (_) {}

    // 12) set memCache and return leader response
    memCache.set(dest, { buf: originBuf, type: originType, expires: Date.now() + TTL * 1000 });
    return { buf: originBuf, type: originType, source: uploaded ? "ORIGIN-UPLOADED" : "ORIGIN-NOUPLOAD", tOrigin: originTime };
  })();

  inflight.set(dest, promise);
  try {
    const out = await promise;
    inflight.delete(dest);
    // set some response headers for diagnostics
    if (out.tOrigin) res.setHeader("X-Timing-Origin-ms", String(out.tOrigin));
    res.setHeader("X-Cache-Source", out.source || "UNKNOWN");
    res.setHeader("X-Cache-Type", out.type || "application/octet-stream");
    res.setHeader("Cache-Control", `public, s-maxage=${TTL}, max-age=${TTL}`);
    res.setHeader("Content-Type", out.type || "application/octet-stream");
    return res.status(200).send(out.buf);
  } catch (err) {
    inflight.delete(dest);
    console.error("[Proxy error]", err && err.message ? err.message : err);
    return res.status(502).send("Error processing request.");
  }
}
