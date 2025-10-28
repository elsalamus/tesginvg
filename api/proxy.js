// api/proxy.js
import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import { createClient } from "redis";
import { put, list, del } from "@vercel/blob";

/* ------------------------- CONFIG ------------------------- */
const SECRET_KEY = process.env.SECRET_KEY || 'z9b8x7c6v5n4m3l2k1j9h8g7f6d5s4a3p0o9i8u7y6t5r4e2w1q_!@';
const BLOB_BASE = process.env.BLOB_BASE || 'https://73vhpohjcehuphyg.public.blob.vercel-storage.com';
const REDIS_URL = process.env.REDIS_URL || 'redis://default:m21C2O6O7pbghmkp0JcOOS2by1an0941@redis-14551.c83.us-east-1-2.ec2.redns.redis-cloud.com:14551';

// TTLs & lock duration (tune as needed)
const TTL = 600;            // 10 minutes for edge s-maxage
const META_TTL = TTL * 2;   // meta kept longer to avoid races
const LOCK_SEC = 10;        // leader lock (seconds) — set 3 only if uploads <3s reliably
const TMP_DIR = '/tmp';
/* --------------------------------------------------------- */

/* In-process caches/dedupe */
const inflight = new Map(); // dest -> Promise<result>
const memCache = new Map(); // dest -> { buf, type, expires }

/* Redis connection (single connecting promise) */
let redisPromise;
function getRedis() {
  if (!redisPromise) {
    redisPromise = (async () => {
      const r = createClient({ url: REDIS_URL });
      r.on('error', (e) => console.error('Redis client error:', e && e.message ? e.message : e));
      await r.connect();
      return r;
    })();
  }
  return redisPromise;
}

/* helpers */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function decrypt(b64, key) {
  let s = b64.replace(/-/g, '+').replace(/_/g, '/');
  while (s.length % 4) s += '=';
  const enc = Buffer.from(s, 'base64').toString('binary');
  let out = '';
  for (let i = 0; i < enc.length; i++) {
    out += String.fromCharCode(enc.charCodeAt(i) ^ key.charCodeAt(i % key.length));
  }
  return out;
}

async function writeTmpSafe(tmpPath, buf) {
  try {
    await fs.writeFile(tmpPath, buf);
    return true;
  } catch (e) {
    if (e && e.code === 'ENOSPC') {
      console.warn('tmp full, skipping write');
      return false;
    }
    console.warn('tmp write failed:', e && e.message ? e.message : e);
    return false;
  }
}

async function tryPutBlobWithRetries(key, buf, type, attempts = 3) {
  for (let i = 1; i <= attempts; i++) {
    try {
      await put(key, buf, {
        access: 'public',
        contentType: type,
        cacheControlMaxAge: TTL,
        addRandomSuffix: false,
        allowOverwrite: true,
      });
      return { success: true };
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      console.warn(`Blob put attempt ${i} failed:`, msg);
      if (i < attempts) await sleep(Math.min(1000 * i, 3000));
      if (i === attempts) return { success: false, error: msg };
    }
  }
  return { success: false, error: 'unknown' };
}

/* wait for meta.status === 'ready' with backoff */
async function waitForMetaReady(redis, metaKey, timeoutMs = 8000) {
  const start = Date.now();
  let wait = 120;
  while (Date.now() - start < timeoutMs) {
    try {
      const raw = await redis.get(metaKey);
      if (raw) {
        try {
          const meta = JSON.parse(raw);
          if (meta.status === 'ready') return meta;
          // if failed, return it so caller can decide
          if (meta.status === 'failed') return meta;
        } catch {}
      }
    } catch (e) {
      console.warn('redis.get metaKey error:', e && e.message ? e.message : e);
    }
    await sleep(wait + Math.floor(Math.random() * 60));
    wait = Math.min(1000, Math.floor(wait * 1.5));
  }
  return null;
}

/* cleanup all blobs older than 5 minutes (single call) */
async function cleanupAllOlderThan(cutoffMs) {
  try {
    const { blobs } = await list();
    let deleted = 0;
    for (const b of blobs) {
      if (new Date(b.uploadedAt).getTime() < cutoffMs) {
        try {
          await del(b.url);
          deleted++;
        } catch (e) {
          console.warn('del blob failed for', b.url, e && e.message ? e.message : e);
        }
      }
    }
    return { deleted };
  } catch (e) {
    return { error: e.message || String(e) };
  }
}

/* ------------------------- Handler ------------------------- */
export default async function handler(req, res) {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET,HEAD,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', '*');
    return res.status(204).end();
  }
  res.setHeader('Access-Control-Allow-Origin', '*');

  // Cleanup route (cron or manual)
  const urlPath = req.url.split('?')[0];
  if (urlPath === '/cleanup' || urlPath === '/api/proxy.js/cleanup') {
    const cutoff = Date.now() - 5 * 60 * 1000; // 5 minutes
    const result = await cleanupAllOlderThan(cutoff);
    return res.status(200).json({ ok: true, ...result });
  }

  // decrypt path -> dest
  const enc = req.url.startsWith('/') ? req.url.slice(1) : req.url;
  if (!enc) return res.status(400).send('Powered by V.CDN');

  let dest;
  try {
    dest = decrypt(enc.split('?')[0], SECRET_KEY);
    new URL(dest); // validate
  } catch (e) {
    return res.status(400).send('Invalid encrypted URL.');
  }

  // dedupe in-process
  if (inflight.has(dest)) {
    try {
      const previous = await inflight.get(dest);
      // set diagnostic headers
      if (previous.tOrigin) res.setHeader('X-Timing-Origin-ms', String(previous.tOrigin));
      if (previous.metaUploadedAt) res.setHeader('X-Blob-Age', String(Math.round((Date.now() - previous.metaUploadedAt) / 1000)));
      res.setHeader('X-Cache-Source', previous.source || 'INTERNAL');
      if (previous.type) res.setHeader('Content-Type', previous.type);
      res.setHeader('Cache-Control', `public, s-maxage=${TTL}, max-age=${TTL}`);

      if (req.method === 'HEAD') return res.status(200).end();
      return res.status(200).send(previous.buf);
    } catch (_) {
      // fallthrough
    }
  }

  const promise = (async () => {
    const key = crypto.createHash('sha1').update(dest).digest('hex');
    const blobUrl = `${BLOB_BASE}/${key}`;
    const tmpPath = path.join(TMP_DIR, key);
    const metaKey = `meta:${key}`;
    const lockKey = `lock:${key}`;
    const redis = await getRedis();

    // 1) memory quick path
    const mem = memCache.get(dest);
    if (mem && mem.expires > Date.now()) {
      return { buf: mem.buf, type: mem.type, source: 'MEMORY' };
    }

    // 2) tmp quick path
    try {
      const st = await fs.stat(tmpPath);
      if (Date.now() - st.mtimeMs < TTL * 1000) {
        const b = await fs.readFile(tmpPath);
        return { buf: b, type: 'application/octet-stream', source: 'TMP' };
      }
    } catch (e) { /* ignore */ }

    // 3) meta check: if ready -> single blob GET (no HEAD polling)
    try {
      const metaRaw = await redis.get(metaKey);
      if (metaRaw) {
        try {
          const meta = JSON.parse(metaRaw);
          if (meta.status === 'ready' && Date.now() - meta.uploadedAt < TTL * 1000) {
            const blobResp = await fetch(blobUrl, { cache: 'force-cache' });
            if (blobResp.ok) {
              const b = Buffer.from(await blobResp.arrayBuffer());
              const t = blobResp.headers.get('content-type') || 'application/octet-stream';
              await writeTmpSafe(tmpPath, b);
              memCache.set(dest, { buf: b, type: t, expires: Date.now() + TTL * 1000 });
              return { buf: b, type: t, source: 'BLOB', metaUploadedAt: meta.uploadedAt };
            }
            // fallthrough if blob GET fails
          }
          // if meta.status === 'uploading' we'll handle below
        } catch (e) { /* ignore */ }
      }
    } catch (e) {
      console.warn('redis.get(metaKey) error:', e && e.message ? e.message : e);
    }

    // 4) If meta exists and is uploading
    try {
      const metaRaw2 = await redis.get(metaKey);
      if (metaRaw2) {
        const meta2 = JSON.parse(metaRaw2);
        if (meta2.status === 'uploading') {
          // HEAD: wait short for ready and return headers only
          if (req.method === 'HEAD') {
            const ready = await waitForMetaReady(redis, metaKey, 8000);
            if (ready && ready.status === 'ready') {
              return { buf: null, type: null, source: 'BLOB-READY', metaUploadedAt: ready.uploadedAt };
            }
            return { buf: null, type: null, source: 'WARMING' };
          }

          // GET: wait for ready to avoid origin hits
          const readyMeta = await waitForMetaReady(redis, metaKey, 8000);
          if (readyMeta && readyMeta.status === 'ready') {
            const blobResp = await fetch(blobUrl, { cache: 'force-cache' });
            if (blobResp.ok) {
              const b = Buffer.from(await blobResp.arrayBuffer());
              const t = blobResp.headers.get('content-type') || 'application/octet-stream';
              await writeTmpSafe(tmpPath, b);
              memCache.set(dest, { buf: b, type: t, expires: Date.now() + TTL * 1000 });
              return { buf: b, type: t, source: 'WAIT-READY', metaUploadedAt: readyMeta.uploadedAt };
            }
          }
          // else fallthrough to try to get lock and become leader
        }
      }
    } catch (e) {
      // ignore
    }

    // 5) attempt to acquire lock atomically
    let locked = null;
    try {
      locked = await redis.set(lockKey, '1', { NX: true, EX: LOCK_SEC });
    } catch (e) {
      console.warn('redis.set(lockKey) error:', e && e.message ? e.message : e);
      locked = null;
    }

    // If we didn't get lock and request is HEAD => try to wait + return headers
    if (!locked && req.method === 'HEAD') {
      const ready = await waitForMetaReady(redis, metaKey, 8000);
      if (ready && ready.status === 'ready') {
        return { buf: null, type: null, source: 'BLOB-READY', metaUploadedAt: ready.uploadedAt };
      }
      // attempt to acquire lock once more (race); if still can't, return 202 warming
      const locked2 = await redis.set(lockKey, '1', { NX: true, EX: LOCK_SEC }).catch(()=>null);
      if (!locked2) return { buf: null, type: null, source: 'WARMING' };
      locked = locked2;
    }

    // If we didn't get lock and request is GET => wait for ready; if not ready, try final lock before fallback
    if (!locked && req.method === 'GET') {
      const readyMeta = await waitForMetaReady(redis, metaKey, 8000);
      if (readyMeta && readyMeta.status === 'ready') {
        const blobResp = await fetch(blobUrl, { cache: 'force-cache' });
        if (blobResp.ok) {
          const b = Buffer.from(await blobResp.arrayBuffer());
          const t = blobResp.headers.get('content-type') || 'application/octet-stream';
          await writeTmpSafe(tmpPath, b);
          memCache.set(dest, { buf: b, type: t, expires: Date.now() + TTL * 1000 });
          return { buf: b, type: t, source: 'WAIT-LOCK', metaUploadedAt: readyMeta.uploadedAt };
        }
      }
      // final attempt to acquire lock
      const locked2 = await redis.set(lockKey, '1', { NX: true, EX: LOCK_SEC }).catch(()=>null);
      if (!locked2) {
        // fallback origin; but mark meta.uploading to reduce others
        try { await redis.set(metaKey, JSON.stringify({ status: 'uploading', startedAt: Date.now() }), { EX: META_TTL }); } catch {}
        const originResp = await fetch(dest, { headers: { 'User-Agent': 'VercelProxyFallback/1.0' } });
        if (!originResp.ok) throw new Error(`Origin failed: ${originResp.status}`);
        const fallbackBuf = Buffer.from(await originResp.arrayBuffer());
        const fallbackType = originResp.headers.get('content-type') || 'application/octet-stream';
        await writeTmpSafe(tmpPath, fallbackBuf);
        const up = await tryPutBlobWithRetries(key, fallbackBuf, fallbackType, 2);
        if (up.success) {
          try { await redis.set(metaKey, JSON.stringify({ status: 'ready', uploadedAt: Date.now() }), { EX: META_TTL }); } catch {}
        } else {
          try { await redis.set(metaKey, JSON.stringify({ status: 'failed', attemptedAt: Date.now(), error: up.error }), { EX: 60 }); } catch {}
          await redis.del(lockKey).catch(()=>{});
        }
        memCache.set(dest, { buf: fallbackBuf, type: fallbackType, expires: Date.now() + TTL * 1000 });
        return { buf: fallbackBuf, type: fallbackType, source: 'FALLBACK-ORIGIN' };
      }
      locked = locked2;
    }

    // If we reach here we are leader (locked truthy)
    // Immediately mark meta uploading so other regions won't call origin
    try {
      await redis.set(metaKey, JSON.stringify({ status: 'uploading', startedAt: Date.now() }), { EX: META_TTL });
    } catch (e) {
      console.warn('meta set uploading failed:', e && e.message ? e.message : e);
    }

    // Leader fetches origin
    const t0 = Date.now();
    const originResp = await fetch(dest, {
      headers: { 'User-Agent': 'VercelBlobProxy/Leader', Connection: 'keep-alive', 'Accept-Encoding': 'identity' },
    });
    const tOrigin = Date.now() - t0;
    if (!originResp.ok) {
      await redis.del(lockKey).catch(()=>{});
      await redis.del(metaKey).catch(()=>{});
      throw new Error(`Origin fetch failed: ${originResp.status}`);
    }
    const originBuf = Buffer.from(await originResp.arrayBuffer());
    const originType = originResp.headers.get('content-type') || 'application/octet-stream';

    // best-effort write /tmp
    await writeTmpSafe(tmpPath, originBuf).catch(()=>{});

    // **CRITICAL**: upload to blob and await it — only mark meta ready after success
    const up = await tryPutBlobWithRetries(key, originBuf, originType, 3);
    if (up.success) {
      try {
        await redis.set(metaKey, JSON.stringify({ status: 'ready', uploadedAt: Date.now() }), { EX: META_TTL });
      } catch (e) { console.warn('meta set ready failed:', e && e.message ? e.message : e); }
    } else {
      // upload failed — set failed meta and release lock
      try {
        await redis.set(metaKey, JSON.stringify({ status: 'failed', attemptedAt: Date.now(), error: up.error }), { EX: 60 });
      } catch (e) {}
      await redis.del(lockKey).catch(()=>{});
      // return origin to caller but mark upload failure in header
      memCache.set(dest, { buf: originBuf, type: originType, expires: Date.now() + TTL * 1000 });
      return { buf: originBuf, type: originType, source: 'ORIGIN-NOUPLOAD', tOrigin, metaUploadedAt: null, uploadStatus: 'failed', uploadError: up.error };
    }

    // success: release lock
    await redis.del(lockKey).catch(()=>{});

    memCache.set(dest, { buf: originBuf, type: originType, expires: Date.now() + TTL * 1000 });
    return { buf: originBuf, type: originType, source: 'ORIGIN-UPLOADED', tOrigin, metaUploadedAt: Date.now(), uploadStatus: 'success' };
  })();

  inflight.set(dest, promise);

  try {
    const out = await promise;
    inflight.delete(dest);

    // Add diagnostic headers
    if (out.tOrigin) res.setHeader('X-Timing-Origin-ms', String(out.tOrigin));
    if (out.metaUploadedAt) res.setHeader('X-Blob-Age', String(Math.round((Date.now() - out.metaUploadedAt) / 1000)));
    res.setHeader('X-Cache-Source', out.source || 'UNKNOWN');
    if (out.uploadStatus) res.setHeader('X-Blob-Upload-Status', out.uploadStatus);
    if (out.uploadError) res.setHeader('X-Blob-Upload-Error', String(out.uploadError).slice(0, 200));
    if (out.type) res.setHeader('Content-Type', out.type);
    res.setHeader('Cache-Control', `public, s-maxage=${TTL}, max-age=${TTL}`);

    // HEAD must not return body
    if (req.method === 'HEAD') return res.status(200).end();

    // Return body for GET
    if (out.buf) return res.status(200).send(out.buf);

    // Warming responses etc.
    if (out.source === 'WARMING') return res.status(202).send('Warming');
    return res.status(504).send('Timeout/warming');
  } catch (err) {
    inflight.delete(dest);
    console.error('[Proxy error]', err && err.message ? err.message : err);
    return res.status(502).send('Error processing request.');
  }
}
