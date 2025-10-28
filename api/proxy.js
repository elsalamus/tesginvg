// api/proxy.js
import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import { createClient } from "redis";
import { put } from "@vercel/blob";

/* ------------- CONFIG ------------- */
const SECRET_KEY = process.env.SECRET_KEY || 'z9b8x7c6v5n4m3l2k1j9h8g7f6d5s4a3p0o9i8u7y6t5r4e2w1q_!@';
const BLOB_BASE = process.env.BLOB_BASE || 'https://73vhpohjcehuphyg.public.blob.vercel-storage.com';
const REDIS_URL = process.env.REDIS_URL || 'redis://default:m21C2O6O7pbghmkp0JcOOS2by1an0941@redis-14551.c83.us-east-1-2.ec2.redns.redis-cloud.com:14551';

// TTLs and locking
const TTL = 600;           // edge s-maxage seconds (10 min)
const META_TTL = TTL * 2;  // meta TTL in redis
const LOCK_SEC = 5;        // upload lock duration (seconds) - change if needed
const TMP_DIR = '/tmp';
/* ---------------------------------- */

/* in-process maps to dedupe within same instance */
const inflight = new Map();   // dest -> Promise<result>
const memCache = new Map();   // dest -> { buf, type, expires }

/* single Redis connecting promise */
let redisPromise;
function getRedis() {
  if (!redisPromise) {
    redisPromise = (async () => {
      const r = createClient({ url: REDIS_URL });
      r.on('error', (e) => console.error('Redis error:', e && e.message ? e.message : e));
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
      console.warn('tmp full; skipping /tmp write');
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
    } catch (err) {
      const msg = err && err.message ? err.message : String(err);
      console.warn(`blob put attempt ${i} failed:`, msg);
      if (i < attempts) await sleep(Math.min(1000 * i, 3000));
      if (i === attempts) return { success: false, error: msg };
    }
  }
  return { success: false, error: 'unknown' };
}

/* Wait until meta.status === 'ready' (backoff) */
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
          if (meta.status === 'failed') return meta;
        } catch {}
      }
    } catch (e) {
      console.warn('redis.get(metaKey) error:', e && e.message ? e.message : e);
    }
    await sleep(wait + Math.floor(Math.random() * 60));
    wait = Math.min(1000, Math.floor(wait * 1.5));
  }
  return null;
}

/* ------------------ MAIN HANDLER ------------------ */
export default async function handler(req, res) {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET,HEAD,OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', '*');
    return res.status(204).end();
  }
  res.setHeader('Access-Control-Allow-Origin', '*');

  // decrypt the target path
  const enc = req.url.startsWith('/') ? req.url.slice(1) : req.url;
  if (!enc) return res.status(400).send('Powered by V.CDN');

  let dest;
  try {
    dest = decrypt(enc.split('?')[0], SECRET_KEY);
    new URL(dest);
  } catch (e) {
    return res.status(400).send('Invalid encrypted URL.');
  }

  // dedupe inside same instance
  if (inflight.has(dest)) {
    try {
      const out = await inflight.get(dest);
      if (out.tOrigin) res.setHeader('X-Timing-Origin-ms', String(out.tOrigin));
      if (out.metaUploadedAt) res.setHeader('X-Blob-Age', String(Math.round((Date.now() - out.metaUploadedAt) / 1000)));
      res.setHeader('X-Cache-Source', out.source || 'INTERNAL');
      if (out.uploadTriggered) res.setHeader('X-Upload-Triggered', String(out.uploadTriggered));
      if (out.uploadStatus) res.setHeader('X-Blob-Upload-Status', out.uploadStatus);
      if (out.uploadError) res.setHeader('X-Blob-Upload-Error', String(out.uploadError).slice(0,200));
      if (out.type) res.setHeader('Content-Type', out.type);
      res.setHeader('Cache-Control', `public, s-maxage=${TTL}, max-age=${TTL}`);

      if (req.method === 'HEAD') return res.status(200).end();
      return res.status(200).send(out.buf);
    } catch (e) {
      // fall through to new attempt
    }
  }

  const promise = (async () => {
    const key = crypto.createHash('sha1').update(dest).digest('hex');
    const blobUrl = `${BLOB_BASE}/${key}`;
    const tmpPath = path.join(TMP_DIR, key);
    const metaKey = `meta:${key}`;   // JSON { status: 'seen'|'uploading'|'ready'|'failed', uploadedAt: number }
    const hitsKey = `hits:${key}`;   // counter of hits
    const lockKey = `lock:${key}`;   // upload lock
    const redis = await getRedis();

    // 1) memory quick path
    const mem = memCache.get(dest);
    if (mem && mem.expires > Date.now()) {
      return { buf: mem.buf, type: mem.type, source: 'MEMORY', uploadTriggered: false };
    }

    // 2) tmp quick path
    try {
      const st = await fs.stat(tmpPath);
      if (Date.now() - st.mtimeMs < TTL * 1000) {
        const b = await fs.readFile(tmpPath);
        return { buf: b, type: 'application/octet-stream', source: 'TMP', uploadTriggered: false };
      }
    } catch (e) { /* ignore */ }

    // 3) if meta ready -> fetch blob once and return
    try {
      const metaRaw = await redis.get(metaKey);
      if (metaRaw) {
        try {
          const meta = JSON.parse(metaRaw);
          if (meta.status === 'ready' && Date.now() - meta.uploadedAt < TTL * 1000) {
            // single GET from blob (rely on CDN/edge)
            const blobResp = await fetch(blobUrl, { cache: 'force-cache' });
            if (blobResp.ok) {
              const b = Buffer.from(await blobResp.arrayBuffer());
              const t = blobResp.headers.get('content-type') || 'application/octet-stream';
              await writeTmpSafe(tmpPath, b);
              memCache.set(dest, { buf: b, type: t, expires: Date.now() + TTL * 1000 });
              return { buf: b, type: t, source: 'BLOB', metaUploadedAt: meta.uploadedAt, uploadTriggered: false };
            }
            // if blob GET failed, we will proceed to attempt leader etc.
          }
        } catch (e) { /* ignore broken meta */ }
      }
    } catch (e) {
      console.warn('redis.get(metaKey) error:', e && e.message ? e.message : e);
    }

    // 4) NOT uploaded yet: increment hits counter atomically
    let hits = 0;
    try {
      hits = await redis.incr(hitsKey);
      // set an expiry on hitsKey if newly created
      if (hits === 1) {
        await redis.expire(hitsKey, META_TTL).catch(()=>{});
      }
    } catch (e) {
      console.warn('redis.incr(hitsKey) error:', e && e.message ? e.message : e);
      // if redis fails, fallback: behave like hits==1 (do not upload)
      hits = 1;
    }

    // store hits header later
    let uploadTriggered = false;

    // 5) FIRST HIT behavior (hits === 1) -> fetch origin, write /tmp, set meta 'seen' and return, DO NOT upload
    if (hits === 1) {
      // mark meta seen (so others know at least one hit happened)
      try {
        await redis.set(metaKey, JSON.stringify({ status: 'seen', seenAt: Date.now() }), { EX: META_TTL });
      } catch (e){ console.warn('meta seen set failed:', e && e.message ? e.message : e); }

      // fetch origin and return content (no upload)
      const t0 = Date.now();
      const originResp = await fetch(dest, { headers: { 'User-Agent': 'VercelProxy/FirstHit' } });
      const tOrigin = Date.now() - t0;
      if (!originResp.ok) throw new Error(`Origin fetch failed: ${originResp.status}`);
      const buf = Buffer.from(await originResp.arrayBuffer());
      const type = originResp.headers.get('content-type') || 'application/octet-stream';

      // write /tmp and memCache best-effort
      await writeTmpSafe(tmpPath, buf).catch(()=>{});
      memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });

      return { buf, type, source: 'ORIGIN-FIRST', tOrigin, uploadTriggered: false };
    }

    // 6) SECOND HIT or more (hits >= 2): ensure we trigger upload exactly once
    // Try to acquire upload lock. If we get it, we mark meta 'uploading' and start background upload.
    // If someone else already set meta uploading or got lock, we will wait for meta.ready.
    let meta = null;
    try {
      const raw = await redis.get(metaKey);
      if (raw) {
        try { meta = JSON.parse(raw); } catch {}
      }
    } catch (e) { console.warn('redis.get(metaKey) error:', e && e.message ? e.message : e); }

    // If meta is already ready, just GET blob
    if (meta && meta.status === 'ready') {
      try {
        const blobResp = await fetch(blobUrl, { cache: 'force-cache' });
        if (blobResp.ok) {
          const b = Buffer.from(await blobResp.arrayBuffer());
          const t = blobResp.headers.get('content-type') || 'application/octet-stream';
          await writeTmpSafe(tmpPath, b);
          memCache.set(dest, { buf: b, type: t, expires: Date.now() + TTL * 1000 });
          return { buf: b, type: t, source: 'BLOB-READY', metaUploadedAt: meta.uploadedAt, uploadTriggered: false };
        }
      } catch (e) { console.warn('blob GET after ready failed:', e && e.message ? e.message : e); }
    }

    // If meta.status === 'uploading', don't re-trigger upload. WAIT for ready (short)
    if (meta && meta.status === 'uploading') {
      // Wait for ready up to a short time, then GET blob
      const waitedMeta = await waitForMetaReady(redis, metaKey, 8000);
      if (waitedMeta && waitedMeta.status === 'ready') {
        const blobResp = await fetch(blobUrl, { cache: 'force-cache' });
        if (blobResp.ok) {
          const b = Buffer.from(await blobResp.arrayBuffer());
          const t = blobResp.headers.get('content-type') || 'application/octet-stream';
          await writeTmpSafe(tmpPath, b);
          memCache.set(dest, { buf: b, type: t, expires: Date.now() + TTL * 1000 });
          return { buf: b, type: t, source: 'BLOB-WAITED', metaUploadedAt: waitedMeta.uploadedAt, uploadTriggered: false };
        }
      }
      // if not ready, fallback to origin (rare) — but we try to avoid origin storms by not uploading here
    }

    // Try to acquire lock to become uploader
    let lockGot = null;
    try {
      lockGot = await redis.set(lockKey, '1', { NX: true, EX: LOCK_SEC }); // OK or null
    } catch (e) {
      console.warn('redis.set(lockKey) error:', e && e.message ? e.message : e);
      lockGot = null;
    }

    // Fetch origin anyway (uploader or not) so we can respond quickly to this request
    const t0 = Date.now();
    const originResp = await fetch(dest, { headers: { 'User-Agent': 'VercelProxy/SecondHit' } });
    const tOrigin = Date.now() - t0;
    if (!originResp.ok) throw new Error(`Origin fetch failed: ${originResp.status}`);
    const buf = Buffer.from(await originResp.arrayBuffer());
    const type = originResp.headers.get('content-type') || 'application/octet-stream';

    // write /tmp and mem cache
    await writeTmpSafe(tmpPath, buf).catch(()=>{});
    memCache.set(dest, { buf, type, expires: Date.now() + TTL * 1000 });

    // If we got lock, we are uploader: mark meta uploading and start async upload (only one uploader)
    if (lockGot) {
      uploadTriggered = true;
      try {
        await redis.set(metaKey, JSON.stringify({ status: 'uploading', startedAt: Date.now() }), { EX: META_TTL });
      } catch (e) { console.warn('set meta uploading failed:', e && e.message ? e.message : e); }

      // Async upload task — do not block response
      (async () => {
        try {
          const up = await tryPutBlobWithRetries(key, buf, type, 3);
          if (up.success) {
            await redis.set(metaKey, JSON.stringify({ status: 'ready', uploadedAt: Date.now() }), { EX: META_TTL }).catch(()=>{});
            // optionally clear hits counter (or keep it)
            // await redis.del(hitsKey).catch(()=>{});
          } else {
            await redis.set(metaKey, JSON.stringify({ status: 'failed', attemptedAt: Date.now(), error: up.error }), { EX: 60 }).catch(()=>{});
          }
        } catch (e) {
          try { await redis.set(metaKey, JSON.stringify({ status: 'failed', attemptedAt: Date.now(), error: (e && e.message) || String(e) }), { EX: 60 }); } catch (_){}
          console.warn('async upload task error:', e && e.message ? e.message : e);
        } finally {
          // release lock
          await redis.del(lockKey).catch(()=>{});
        }
      })();
    } else {
      // we did not get lock — another instance will upload. mark uploadTriggered false.
      uploadTriggered = false;
    }

    // return origin response immediately
    return { buf, type, source: 'ORIGIN-SECOND', tOrigin, uploadTriggered };
  })();

  inflight.set(dest, promise);
  try {
    const out = await promise;
    inflight.delete(dest);

    // diagnostic headers
    if (out.tOrigin) res.setHeader('X-Timing-Origin-ms', String(out.tOrigin));
    if (out.metaUploadedAt) res.setHeader('X-Blob-Age', String(Math.round((Date.now() - out.metaUploadedAt)/1000)));
    res.setHeader('X-Cache-Source', out.source || 'UNKNOWN');
    res.setHeader('X-Upload-Triggered', String(out.uploadTriggered || false));
    if (out.uploadStatus) res.setHeader('X-Blob-Upload-Status', out.uploadStatus);
    if (out.uploadError) res.setHeader('X-Blob-Upload-Error', String(out.uploadError || '').slice(0,200));
    if (out.type) res.setHeader('Content-Type', out.type);
    // Also include current hits count (best-effort): read hitsKey
    try {
      const redis = await getRedis();
      const h = await redis.get(`hits:${crypto.createHash('sha1').update(dest).digest('hex')}`);
      if (h) res.setHeader('X-Hits', h);
    } catch (_) {}

    res.setHeader('Cache-Control', `public, s-maxage=${TTL}, max-age=${TTL}`);

    // HEAD must not return body
    if (req.method === 'HEAD') return res.status(200).end();

    // GET returns body
    if (out.buf) return res.status(200).send(out.buf);

    return res.status(504).send('Timeout/warming');
  } catch (err) {
    inflight.delete(dest);
    console.error('[proxy error]', err && err.message ? err.message : err);
    return res.status(502).send('Error processing request.');
  }
}
