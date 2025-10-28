// api/proxy.js
const SECRET_KEY = 'z9b8x7c6v5n4m3l2k1j9h8g7f6d5s4a3p0o9i8u7y6t5r4e2w1q_!@';

function decrypt(base64Input, key) {
  let base64 = base64Input.replace(/-/g, '+').replace(/_/g, '/');
  while (base64.length % 4) base64 += '=';
  const encrypted = Buffer.from(base64, 'base64').toString('binary');
  let out = '';
  for (let i = 0; i < encrypted.length; i++) {
    out += String.fromCharCode(encrypted.charCodeAt(i) ^ key.charCodeAt(i % key.length));
  }
  return out;
}

export default async function handler(req, res) {
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', '*');
    return res.status(204).end();
  }

  res.setHeader('Access-Control-Allow-Origin', '*');

  const path = req.url.startsWith('/') ? req.url.slice(1) : req.url;
  if (!path) return res.status(400).send('Powered by V.CDN');

  let dest;
  try {
    dest = decrypt(path.split('?')[0], SECRET_KEY);
    new URL(dest);
  } catch {
    return res.status(400).send('Invalid encrypted URL.');
  }

  try {
    const upstream = await fetch(dest, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; VercelCDNProxy/1.0)' }
    });

    const buf = Buffer.from(await upstream.arrayBuffer());
    res.setHeader('Cache-Control', 'public, max-age=60, s-maxage=120, stale-while-revalidate=86400');
    const type = upstream.headers.get('content-type');
    if (type) res.setHeader('Content-Type', type);
    res.status(upstream.status).send(buf);
  } catch (e) {
    console.error(e);
    res.status(502).send('Error fetching destination.');
  }
}
