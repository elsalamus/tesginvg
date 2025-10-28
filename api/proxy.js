// api/proxy.js
import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import { createClient } from "redis";
import { put } from "@vercel/blob";

/*
  CONFIG - tune these values to your environment.
  - LOCK_SEC: how long the leader lock lasts (seconds). If your origin+upload can take >3s,
    set this larger. Default here is 10s to avoid races. Set to 3 if you're sure uploads <3s.
*/
const SECRET_KEY = "z9b8x7c6v5n4m3l2k1j9h8g7f6d5s4a3p0o9i8u7y6t5r4e2w1q_!@";
const BLOB_BASE = "https://73vhpohjcehuphyg.public.blob.vercel-storage.com";
const REDIS_URL =
  "redis://default:m21C2O6O7pbghmkp0JcOOS2by1an0941@redis-14551.c83.us-east-1-2.ec2.redns.redis-cloud.com:14551";

// TTLs
const TTL = 600; // seconds for cache retention (s-maxage). Change if needed.
const META_TTL = TTL * 2; // Redis meta TTL (keep longer than blob TTL to be safe)
const LOCK_SEC = 10; // leader lock (seconds). Change to 3 if you want, but riskier.
const TMP_DIR = "/tmp";

// In-process caches (fast)
const memCache = new Map(); // dest -> { buf, type, expires }
const inflight = new Map(); // dest -> Promise

// Redis single connecting promise
let redisPromise;
function getRedis() {
  if (!redisPromise)
    redisPromise = (async () => {
      const r = createClient({ url: REDIS_URL });
      r.on("error", (e) => console.error("Redis client error:", e.message || e));
      await r.connect();
      return r;
    })();
  return redisPromise;
}

// decrypt function (URL-safe base64 + XOR) - same as you used
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

// small sleep helper
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// exponential backoff with jitter
async function waitForMetaReady(redis, metaKey, timeoutMs = 8000) {
  const start = Date.now();
  let wait = 150;
  while (Date.now() - start < timeoutMs) {
    const metaRaw = await redis.get(metaKey);
    if (metaRaw) {
      try {
        const meta = JSON.parse(metaRaw);
        if (meta.status === "ready") return meta;
        // if status === 'failed' maybe allow fallback; continue waiting
      } catch (e) {}
    }
    // jittered sleep
    await sleep(wait + Math.floor(Math.random() * 60));
    wait = Math.min(1000, Math.floor(wait * 1.5));
  }
  return null;
}

/*
  Main handler
*/
export default async function handler(req, res) {
  // CORS preflight
  if (req.method === "OPTIONS") {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET,HEAD,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "*");
    return res.status(204).end();
  }
  res.setHeader("Access-Control-Allow-Origin", "*");

  // support /cleanup route to purge blobs older than 5 minutes (cron or manual)
  const urlPath = req.url.split("?")[0];
  if (urlPath === "/cleanup" || urlPath === "/api/proxy.js/cleanup") {
    try {
      // Use Vercel Blob UI / API for listing + deletion is not exposed here
      // but if you have list/delete utilities you can implement; keep minimal
      // For safety here, return OK — actual purge is expected to be performed via separate script/cron
      return res.status(200).json({ ok: true, note: "Call blob cleanup via dashboard or separate job." });
    } catch (e) {
      return res.status(500).json({ error: e.message });
    }
  }

  // normal encrypted path handling
  const enc = req.url.startsWith("/") ? req.url.slice(1) : req.url;
  if (!enc) return res.status(400).send("Powered by V.CDN");

  let dest;
  try {
    dest = decrypt(enc.split("?")[0], SECRET_KEY);
    new URL(dest);
  } catch (e) {
    return res.status(400).send("Invalid encrypted URL.");
  }

  // dedupe multiple same-instance calls
  if (inflight.has(dest)) {
    try {
      const out = await inflight.get(dest);
      res.setHeader("X-Cache-Source", out.source || "INTERNAL");
      res.setHeader("Content-Type", out.type || "application/octet-stream");
      res.setHeader("Cache-Control", `public, s-maxage=${TTL}, max-age=${TTL}`);
      return res.status(200).send(out.buf);
    } catch (e) {
      // fall through to attempt
    }
  }

  const promise = (async () => {
    const key = crypto.createHash("sha1").update(dest).digest("hex");
    const blobUrl = `${BLOB_BASE}/${key}`;
    const tmpPath = path.join(TMP_DIR, key);
    const metaKey = `meta:${key}`;
    const lockKey = `lock:${key}`;
    const redis = await getRedis();

    // 1) memory fast path
    const mem = memCache.get(dest);
    if (mem && mem.expires > Date.now()) {
      return { buf: mem.buf, type: mem.type, source: "MEMORY" };
    }

    // 2) /tmp fast path
    try {
      const st = await fs.stat(tmpPath);
      if (Date.now() - st.mtimeMs < TTL * 1000) {
        const b = await fs.readFile(tmpPath);
        return { buf: b, type: "application/octet-stream", source: "TMP" };
      }
    } catch (e) {
      // ignore
    }

    // 3) quick meta check: if meta.ready -> fetch blob once
    const metaRaw = await redis.get(metaKey);
    if (metaRaw) {
      try {
        const meta = JSON.parse(metaRaw);
        if (meta.status === "ready" && Date.now() - meta.uploadedAt < TTL * 1000) {
          // fetch blob (single GET) - relies on CDN or cache
          const blobResp = await fetch(blobUrl, { cache: "force-cache" });
          if (blobResp.ok) {
            const b = Buffer.from(await blobResp.arrayBuffer());
            const t = blobResp.headers.get("content-type") || "application/octet-stream";
            try { await fs.writeFile(tmpPath, b); } catch (e) { if (e.code !== "ENOSPC") console.warn("tmp write failed:", e.message); }
            memCache.set(dest, { buf: b, type: t, expires: Date.now() + TTL * 1000 });
            return { buf: b, type: t, source: "BLOB" };
          }
        }
      } catch (e) {
        // broken meta, continue on
      }
    }

    // 4) If meta exists and status == uploading, wait for ready (no Blob HEAD)
    if (metaRaw) {
      try {
        const meta = JSON.parse(metaRaw);
        if (meta.status === "uploading") {
          const readyMeta = await waitForMetaReady(redis, metaKey, 8000);
          if (readyMeta && readyMeta.status === "ready") {
            // fetch blob once
            const blobResp = await fetch(blobUrl, { cache: "force-cache" });
            if (blobResp.ok) {
              const b = Buffer.from(await blobResp.arrayBuffer());
              const t = blobResp.headers.get("content-type") || "application/octet-stream";
              try { await fs.writeFile(tmpPath, b); } catch (e) {}
              memCache.set(dest, { buf: b, type: t, expires: Date.now() + TTL * 1000 });
              return { buf: b, type: t, source: "WAIT-READY" };
            }
          }
          // if timed out, fall through to attempt leader
        }
      } catch (e) {}
    }

    // 5) Try to acquire leader lock (SET NX EX)
    const locked = await redis.set(lockKey, "1", { NX: true, EX: LOCK_SEC });
    if (!locked) {
      // couldn't get lock - but to avoid hammering origin, wait for meta.ready with backoff
      const meta = await waitForMetaReady(redis, metaKey, 8000);
      if (meta && meta.status === "ready") {
        const blobResp = await fetch(blobUrl, { cache: "force-cache" });
        if (blobResp.ok) {
          const b = Buffer.from(await blobResp.arrayBuffer());
          const t = blobResp.headers.get("content-type") || "application/octet-stream";
          try { await fs.writeFile(tmpPath, b); } catch (e) {}
          memCache.set(dest, { buf: b, type: t, expires: Date.now() + TTL * 1000 });
          return { buf: b, type: t, source: "WAIT-LOCK" };
        }
      }
      // if still nothing, attempt to proceed as fallback (to avoid endless wait)
      // try to acquire lock again (short)
      const locked2 = await redis.set(lockKey, "1", { NX: true, EX: LOCK_SEC });
      if (!locked2) {
        // final fallback: fetch origin locally (rare)
        // BUT we set meta.uploading first to reduce others hitting origin
        try {
          await redis.set(metaKey, JSON.stringify({ status: "uploading", startedAt: Date.now() }), { EX: META_TTL });
        } catch (e) {}
        const originResp = await fetch(dest, { headers: { "User-Agent": "VercelProxyFallback/1.0" } });
        if (!originResp.ok) throw new Error(`Origin fetch failed: ${originResp.status}`);
        const fallbackBuf = Buffer.from(await originResp.arrayBuffer());
        const fallbackType = originResp.headers.get("content-type") || "application/octet-stream";
        try { await fs.writeFile(tmpPath, fallbackBuf); } catch (e) {}
        // try upload best-effort
        try {
          await put(key, fallbackBuf, { access: "public", contentType: fallbackType, cacheControlMaxAge: TTL, addRandomSuffix: false, allowOverwrite: true });
          await redis.set(metaKey, JSON.stringify({ status: "ready", uploadedAt: Date.now() }), { EX: META_TTL });
        } catch (e) {
          console.warn("fallback upload failed:", e.message);
          await redis.del(lockKey).catch(()=>{});
        }
        memCache.set(dest, { buf: fallbackBuf, type: fallbackType, expires: Date.now() + TTL * 1000 });
        return { buf: fallbackBuf, type: fallbackType, source: "FALLBACK-ORIGIN" };
      }
      // if locked2 succeeded, continue as leader below
    }

    // 6) Leader path: mark meta uploading immediately
    try {
      await redis.set(metaKey, JSON.stringify({ status: "uploading", startedAt: Date.now() }), { EX: META_TTL });
    } catch (e) {
      // continue even if set fails
    }

    // fetch origin
    const t0 = Date.now();
    const originResp = await fetch(dest, { headers: { "User-Agent": "VercelProxyLeader/1.0", Connection: "keep-alive", "Accept-Encoding": "identity" } });
    const tOrigin = Date.now() - t0;
    if (!originResp.ok) {
      // cleanup lock & meta so others can attempt
      await redis.del(lockKey).catch(()=>{});
      await redis.del(metaKey).catch(()=>{});
      throw new Error(`Origin fetch failed: ${originResp.status}`);
    }
    const originBuf = Buffer.from(await originResp.arrayBuffer());
    const originType = originResp.headers.get("content-type") || "application/octet-stream";

    // write /tmp best-effort
    try { await fs.writeFile(tmpPath, originBuf); } catch (e) { if (e.code !== "ENOSPC") console.warn("tmp write:", e.message); }

    // upload to blob with limited retries
    let uploaded = false;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        await put(key, originBuf, { access: "public", contentType: originType, cacheControlMaxAge: TTL, addRandomSuffix: false, allowOverwrite: true });
        uploaded = true;
        break;
      } catch (e) {
        console.warn(`Blob PUT attempt ${attempt} failed:`, e.message || e);
        await sleep(Math.min(1000 * attempt, 3000));
      }
    }

    // set meta ready or failed
    try {
      await redis.set(metaKey, JSON.stringify({ status: uploaded ? "ready" : "failed", uploadedAt: Date.now() }), { EX: META_TTL });
    } catch (e) {}

    // release lock
    try { await redis.del(lockKey); } catch (e) {}

    memCache.set(dest, { buf: originBuf, type: originType, expires: Date.now() + TTL * 1000 });
    return { buf: originBuf, type: originType, source: uploaded ? "ORIGIN-UPLOADED" : "ORIGIN-NOUPLOAD", tOrigin };
  })();

  inflight.set(dest, promise);
  try {
    const out = await promise;
    inflight.delete(dest);
    if (out.tOrigin) res.setHeader("X-Timing-Origin-ms", String(out.tOrigin));
    res.setHeader("X-Cache-Source", out.source || "UNKNOWN");
    res.setHeader("Content-Type", out.type || "application/octet-stream");
    res.setHeader("Cache-Control", `public, s-maxage=${TTL}, max-age=${TTL}`);
    return res.status(200).send(out.buf);
  } catch (err) {
    inflight.delete(dest);
    console.error("[Proxy error]", err && err.message ? err.message : err);
    return res.status(502).send("Error processing request.");
  }
}
