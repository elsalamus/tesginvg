import crypto from 'crypto';
import { put, del } from '@vercel/blob';

const SECRET_KEY = 'z9b8x7c6v5n4m3l2k1j9h8g7f6d5s4a3p0o9i8u7y6t5r4e2w1q_!@';

// your blob store base URL
const BLOB_BASE = 'https://73vhpohjcehuphyg.public.blob.vercel-storage.com';

// 5-minute TTL (in seconds)
const TTL = 300;

// XOR decrypt
function decrypt(base64Input, key) {
  let base64 = base64Input.replace(/-/g, '+').replace(/_/g, '/');
  while (base64.length % 4) base64 += '=';
  const encrypted = Buffer.from(base64, 'base64').toString('binary');
  let out = '';
  for (let i = 0; i < encrypted.length; i++) {
    out += String.fromCharCode(
      encrypted.charCodeAt(i) ^ key.charCodeAt(i % key.length)
    );
  }
  return out;
}

export default async function handler(req, res) {
  // --- CORS ---
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', '*');
    return res.status(204).end();
  }
  res.setHeader('Access-Control-Allow-Origin', '*');

  // --- Decrypt ---
  const encryptedPath = req.url.startsWith('/') ? req.url.slice(1) : req.url;
  if (!encryptedPath) return res.status(400).send('Powered by V.CDN');
  let dest;
  try {
    dest = decrypt(encryptedPath.split('?')[0], SECRET_KEY);
    new URL(dest);
  } catch {
    return res.status(400).send('Invalid encrypted URL.');
  }

  // --- Hash for blob key ---
  const blobKey = crypto.createHash('sha1').update(dest).digest('hex');
  const blobUrl = `${BLOB_BASE}/${blobKey}`;

  try {
    // 1️⃣ Try from Blob first
    const head = await fetch(blobUrl, { method: 'HEAD' });

    if (head.ok) {
      const lastMod = new Date(head.headers.get('last-modified'));
      const ageSec = (Date.now() - lastMod.getTime()) / 1000;
      if (ageSec < TTL) {
        // ✅ Blob still valid
        const resp = await fetch(blobUrl);
        const buf = Buffer.from(await resp.arrayBuffer());
        const type = resp.headers.get('content-type');
        if (type) res.setHeader('Content-Type', type);
        res.setHeader(
          'Cache-Control',
          `public, max-age=${TTL}, s-maxage=${TTL}, stale-while-revalidate=60`
        );
        return res.status(200).send(buf);
      } else {
        // 🗑️ Blob expired — delete
        await del(blobUrl);
      }
    }

    // 2️⃣ Fetch from origin
    const origin = await fetch(dest, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; VercelBlobProxy/1.0)' },
    });
    if (!origin.ok) return res.status(origin.status).send('Origin fetch failed.');
    const buf = Buffer.from(await origin.arrayBuffer());
    const type = origin.headers.get('content-type') || 'application/octet-stream';

    // 3️⃣ Upload to Blob
    await put(blobKey, buf, {
      access: 'public',
      contentType: type,
      cacheControlMaxAge: TTL,
      addRandomSuffix: false,
      allowOverwrite: true,
    });

    // 4️⃣ Send response
    res.setHeader('Content-Type', type);
    res.setHeader(
      'Cache-Control',
      `public, max-age=${TTL}, s-maxage=${TTL}, stale-while-revalidate=60`
    );
    return res.status(200).send(buf);
  } catch (err) {
    console.error('[Proxy Error]', err);
    res.status(502).send('Error processing request.');
  }
}
