// api/proxy.js
import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import { createClient } from "redis";
import { put } from "@vercel/blob";

/* ------------------------- CONFIG ------------------------- */
const SECRET_KEY = 'z9b8x7c6v5n4m3l2k1j9h8g7f6d5s4a3p0o9i8u7y6t5r4e2w1q_!@';
const BLOB_BASE = 'https://73vhpohjcehuphyg.public.blob.vercel-storage.com';
const REDIS_URL = process.env.REDIS_URL || 'redis://default:m21C2O6O7pbghmkp0JcOOS2by1an0941@redis-14551.c83.us-east-1-2.ec2.redns.redis-cloud.com:14551';

// TTLs & lock
const TTL = 300;            // segment retention / edge s-maxage (seconds)
const META_TTL = TTL * 2;   // keep meta a bit longer
const LOCK_SEC = 3;         // distributed leader lock duration (seconds) — as requested
const TMP_DIR = '/tmp';
/* ---------------------------------------------------------- */

/* in-process caches/dedupe */
const inflight = new Map(); // dest -> Promise<{buf,type,source,tOrigin}>
const memCache = new Map(); // dest -> {buf,type,expires}

/* Redis client (single connecting promise) */
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

/* small helpers */
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
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
      console.warn('tmp full, skipping tmp write');
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
      return true;
    } catch (e) {
      console.warn(`put() attempt ${i} failed:`, e && e.message ? e.message : e);
      if (i < attempts) await sleep(Math.min(1000 * i, 3000));
    }
  }
  return false;
}

/* Wait for meta.status === 'ready' with exponential backoff and jitter */
async function waitForMetaReady(redis, metaKey, timeoutMs = 8000) {
  const start = Date.now();
  let wait = 120;
  while (Date.now() - start < timeoutMs) {
    try {
      const raw = await redis.get(metaKey);
      if (raw) {
        try {
          const parsed = JSON.parse(raw);
          if (parsed.status === 'ready') return parsed;
        } catch {}
      }
    } catch (e) {
      console.warn('redis.get(metaKey) failed:', e && e.message ? e.message : e);
    }
    await sleep(wait + Math.floor(Math.random() * 60));
    wait = Math.min(900, Math.floor(wait * 1.5));
  }
  return null;
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

  // Cleanup route (optional) — user can implement blob listing/deletion separately if desired
  const urlPath = req.url.split('?')[0];
  if (urlPath === '/cleanup' || urlPath === '/api/proxy.js/cleanup') {
    // best to implement blob list/delete via admin tooling. respond OK here.
    return res.status(200).json({ ok: true, note: 'Run blob cleanup via SDK/admin tools or implement listing.' });
  }

  // decrypt target
  const enc = req.url.startsWith('/') ? req.url.slice(1) : req.url;
  if (!enc) return res.status(400).send('Powered by V.CDN');

  let dest;
  try {
    dest = decrypt(enc.split('?')[0], SECRET_KEY);
    new URL(dest);
  } catch (e) {
    return res.status(400).send('Invalid encrypted URL.');
  }

  // if already in-flight on this instance, reuse result (prevents duplicate origin calls inside same lambda)
  if (inflight.has(dest)) {
    try {
      const out = await inflight.get(dest);
      // set headers, but if this is HEAD, do not send body
      res.setHeader('X-Cache-Source', out.source || 'INTERNAL');
      if (out.tOrigin) res.setHeader('X-Timing-Origin-ms', String(out.tOrigin));
      if (out.metaUploadedAt) res.setHeader('X-Blob-Age', String(Math.round((Date.now() - out.metaUploadedAt) / 1000)));
      if (out.type) res.setHeader('Content-Type', out.type);
      res.setHeader('Cache-Control', `public, s-maxage=${TTL}, max-age=${TTL}`);
      if (req.method === 'HEAD') return res.status(200).end();
      return res.status(200).send(out.buf);
    } catch (e) {
      // fall through to attempt new flow
    }
  }

  // create the single promise for this dest and put in inflight
  const promise = (async () => {
    const key = crypto.createHash('sha1').update(dest).digest('hex');
    const blobUrl = `${BLOB_BASE}/${key}`;
    const tmpPath = path.join(TMP_DIR, key);
    const metaKey = `meta:${key}`;
    const lockKey = `lock:${key}`;
    const redis = await getRedis();

    // 1) memory cache
    const mem = memCache.get(dest);
    if (mem && mem.expires > Date.now()) {
      return { buf: mem.buf, type: mem.type, source: 'MEMORY' };
    }

    // 2) /tmp local cache
    try {
      const st = await fs.stat(tmpPath);
      if (Date.now() - st.mtimeMs < TTL * 1000) {
        const b = await fs.readFile(tmpPath);
        return { buf: b, type: 'application/octet-stream', source: 'TMP' };
      }
    } catch (e) { /* ignore not found */ }

    // 3) meta check: ready => fetch blob once
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
            // if blob fetch failed, allow leader to try below
          }
          // if status === 'uploading', we'll handle below
        } catch (e) { /* broken meta -> continue */ }
      }
    } catch (e) {
      console.warn('redis.get(metaKey) error:', e && e.message ? e.message : e);
    }

    // 4) If meta exists and status === 'uploading', then:
    // - If request is HEAD: wait a short time for ready and return headers (no body) when ready.
    // - If request is GET: wait for meta ready and then fetch blob.
    try {
      const metaRaw2 = await redis.get(metaKey);
      if (metaRaw2) {
        try {
          const meta2 = JSON.parse(metaRaw2);
          if (meta2.status === 'uploading') {
            // HEAD special path: wait for ready (short), then return headers (no body)
            if (req.method === 'HEAD') {
              const ready = await waitForMetaReady(redis, metaKey, 8000);
              if (ready && ready.status === 'ready') {
                return { buf: null, type: null, source: 'BLOB-READY', metaUploadedAt: ready.uploadedAt };
              }
              // timed out waiting -> return WARMING (202)
              return { buf: null, type: null, source: 'WARMING' };
            }

            // GET path: wait for ready (so we don't go to origin)
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
            // if timed out -> continue to try to become leader (rare)
          }
        } catch (e) {}
      }
    } catch (e) {
      console.warn('redis.get(metaKey) error (2):', e && e.message ? e.message : e);
    }

    // 5) Try to acquire leader lock (SET NX EX)
    let locked;
    try {
      locked = await redis.set(lockKey, '1', { NX: true, EX: LOCK_SEC });
    } catch (e) {
      console.warn('redis.set(lockKey) error:', e && e.message ? e.message : e);
      locked = null;
    }

    // If we did NOT get lock AND request is HEAD:
    // we want HEAD to cause warming but not to return body — so wait for ready short time and return headers.
    if (!locked && req.method === 'HEAD') {
      const ready = await waitForMetaReady(redis, metaKey, 8000);
      if (ready && ready.status === 'ready') return { buf: null, type: null, source: 'BLOB-READY', metaUploadedAt: ready.uploadedAt };
      // try once more to become leader (small chance), otherwise return accepted warming
      const locked2 = await redis.set(lockKey, '1', { NX: true, EX: LOCK_SEC }).catch(()=>null);
      if (!locked2) return { buf: null, type: null, source: 'WARMING' };
      locked = locked2;
    }

    // If we did not get lock and request is GET: wait for ready to avoid origin storms
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
      // final fallback: try to acquire lock again briefly
      const locked2 = await redis.set(lockKey, '1', { NX: true, EX: LOCK_SEC }).catch(()=>null);
      if (!locked2) {
        // As absolute last resort fallback to fetching origin locally (rare)
        try {
          await redis.set(metaKey, JSON.stringify({ status: 'uploading', startedAt: Date.now() }), { EX: META_TTL }).catch(()=>{});
        } catch {}
        const originResp = await fetch(dest, { headers: { 'User-Agent': 'VercelProxyFallback/1.0' } });
        if (!originResp.ok) throw new Error(`Origin fetch failed: ${originResp.status}`);
        const fb = Buffer.from(await originResp.arrayBuffer());
        const ft = originResp.headers.get('content-type') || 'application/octet-stream';
        await writeTmpSafe(tmpPath, fb);
        await tryPutBlobWithRetries(key, fb, ft, 2);
        await redis.set(metaKey, JSON.stringify({ status: 'ready', uploadedAt: Date.now() }), { EX: META_TTL }).catch(()=>{});
        memCache.set(dest, { buf: fb, type: ft, expires: Date.now() + TTL * 1000 });
        return { buf: fb, type: ft, source: 'FALLBACK-ORIGIN' };
      }
      locked = locked2;
    }

    // If we reach here, we are the leader (locked truthy)
    // For HEAD: leader will fetch+upload and then return headers (no body)
    // For GET: leader will fetch+upload and return body

    // mark meta uploading immediately (prevents other regions from going to origin)
    try {
      await redis.set(metaKey, JSON.stringify({ status: 'uploading', startedAt: Date.now() }), { EX: META_TTL });
    } catch (e) { console.warn('meta set uploading failed:', e && e.message ? e.message : e); }

    // fetch origin
    const startOrigin = Date.now();
    const originResp = await fetch(dest, {
      headers: { 'User-Agent': 'VercelBlobProxy/Leader', Connection: 'keep-alive', 'Accept-Encoding': 'identity' },
    });
    const tOrigin = Date.now() - startOrigin;
    if (!originResp.ok) {
      // cleanup lock & meta so others can attempt
      await redis.del(lockKey).catch(()=>{});
      await redis.del(metaKey).catch(()=>{});
      throw new Error(`Origin fetch failed: ${originResp.status}`);
    }
    const originBuf = Buffer.from(await originResp.arrayBuffer());
    const originType = originResp.headers.get('content-type') || 'application/octet-stream';

    // write /tmp best-effort
    await writeTmpSafe(tmpPath, originBuf).catch(()=>{});

    // upload to Blob (retries)
    const uploaded = await tryPutBlobWithRetries(key, originBuf, originType, 3);

    // set meta ready (with uploadedAt) even if upload partially failed; we set status accordingly
    try {
      await redis.set(metaKey, JSON.stringify({ status: uploaded ? 'ready' : 'failed', uploadedAt: Date.now() }), { EX: META_TTL });
    } catch (e) { console.warn('meta set ready failed:', e && e.message ? e.message : e); }

    // release leader lock
    try { await redis.del(lockKey); } catch (e) {}

    // populate memCache
    memCache.set(dest, { buf: originBuf, type: originType, expires: Date.now() + TTL * 1000 });

    return { buf: originBuf, type: originType, source: uploaded ? 'ORIGIN-UPLOADED' : 'ORIGIN-NOUPLOAD', tOrigin, metaUploadedAt: Date.now() };
  })();

  inflight.set(dest, promise);

  try {
    const out = await promise;
    inflight.delete(dest);

    // set diagnostic headers
    if (out.tOrigin) res.setHeader('X-Timing-Origin-ms', String(out.tOrigin));
    if (out.metaUploadedAt) res.setHeader('X-Blob-Age', String(Math.round((Date.now() - out.metaUploadedAt) / 1000)));
    res.setHeader('X-Cache-Source', out.source || 'UNKNOWN');
    if (out.type) res.setHeader('Content-Type', out.type);
    res.setHeader('Cache-Control', `public, s-maxage=${TTL}, max-age=${TTL}`);

    // HEAD must never return body — always respond with headers only
    if (req.method === 'HEAD') return res.status(200).end();

    // Otherwise send body if present
    if (out.buf) return res.status(200).send(out.buf);

    // If out has no buf (e.g., HEAD warming returned earlier), respond accordingly
    if (out.source === 'WARMING') return res.status(202).send('Warming');
    return res.status(504).send('Timeout/warming');
  } catch (err) {
    inflight.delete(dest);
    console.error('[Proxy error]', err && err.message ? err.message : err);
    return res.status(502).send('Error processing request.');
  }
}
