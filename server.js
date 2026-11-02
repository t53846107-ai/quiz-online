// سرور کوئیز آنلاین - بدون هیچ وابستگی خارجی (فقط Node.js نسخه ۲۲.۵ به بالا)
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { DatabaseSync } = require('node:sqlite');

const PORT = process.env.PORT || 3000;
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'data.db');
const PUBLIC_DIR = path.join(__dirname, 'public');

// ---------- دیتابیس ----------
const db = new DatabaseSync(DB_PATH);
db.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    name_key TEXT NOT NULL UNIQUE,
    salt TEXT NOT NULL,
    hash TEXT NOT NULL,
    best_score INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS sessions (
    token_hash TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_users_score ON users(best_score DESC);
`);

// ---------- بانک سوال ----------
const questionBank = {
  7: [
    { q: "حاصل عبارت: -8 + 15 چند است؟", opts: ["7", "-7", "23", "-23"], ans: "7", subject: "ریاضی" },
    { q: "در آرایه ادبی جناس، دو کلمه چه ویژگی دارند؟", opts: ["در تلفظ یکسان و در معنی متفاوت", "در معنی یکسان", "هم‌معنی دقیق", "متضاد هم"], ans: "در تلفظ یکسان و در معنی متفاوت", subject: "فارسی" },
    { q: "معنی کلمه «صدیق» در عربی چیست؟", opts: ["دوست", "دشمن", "معلم", "دانش‌آموز"], ans: "دوست", subject: "عربی" },
    { q: "کدام کلید میانبر برای کپی کردن استفاده می‌شود؟", opts: ["Ctrl + C", "Ctrl + V", "Ctrl + X", "Ctrl + Z"], ans: "Ctrl + C", subject: "کامپیوتر" },
    { q: "معنی کلمه «Notebook» چیست؟", opts: ["دفترچه یادداشت", "کتاب", "مداد", "تخته"], ans: "دفترچه یادداشت", subject: "انگلیسی" }
  ],
  8: [
    { q: "حاصل ضرب دو عدد منفی همواره چه علامتی دارد؟", opts: ["مثبت", "منفی", "صفر", "نامشخص"], ans: "مثبت", subject: "ریاضی" },
    { q: "عبارت «الهی دانایی ده که از راه نلغزیم» دارای چه آرایه‌ای است؟", opts: ["سجع", "جناس تام", "تضاد", "تشبیه"], ans: "سجع", subject: "فارسی" },
    { q: "ترجمه «هؤلاء طلاب» چیست؟", opts: ["این‌ها دانش‌آموزند", "آن دانش‌آموز است", "او معلم است", "این یک کتاب است"], ans: "این‌ها دانش‌آموزند", subject: "عربی" },
    { q: "کوچک‌ترین واحد حافظه در کامپیوتر چیست؟", opts: ["بیت", "بایت", "مگابایت", "گیگابایت"], ans: "بیت", subject: "کامپیوتر" },
    { q: "معنی کلمه «Dictionary» چیست؟", opts: ["فرهنگ لغت", "مدرسه", "کلاس", "دفتر"], ans: "فرهنگ لغت", subject: "انگلیسی" }
  ],
  9: [
    { q: "جذر عدد ۱۶۹ کدام است؟", opts: ["13", "14", "12", "15"], ans: "13", subject: "ریاضی" },
    { q: "معنی واژه «میاه» در عربی چیست؟", opts: ["آب‌ها", "درختان", "کوه‌ها", "چشمه‌ها"], ans: "آب‌ها", subject: "عربی" },
    { q: "کدام گزینه یک سیستم‌عامل است؟", opts: ["ویندوز", "ورد", "اکسل", "فتوشاپ"], ans: "ویندوز", subject: "کامپیوتر" },
    { q: "معنی کلمه «Environment» چیست؟", opts: ["محیط زیست", "آزمایشگاه", "صنعت", "جغرافیا"], ans: "محیط زیست", subject: "انگلیسی" }
  ]
};

// ---------- ابزارهای کمکی ----------
const QUESTION_SECONDS = 10;
const GRACE_MS = 2500;
const POINTS = 10;
const ADMIN_PASSWORD = 'sihdirdashageiq2w9i432s';

function normalizeName(s) {
  return s.normalize('NFKC').replace(/ي/g, 'ی').replace(/ك/g, 'ک').replace(/[\u200c\u200d\u200e\u200f\s]+/g, '').toLowerCase();
}

function hashPassword(password, salt) {
  return new Promise((resolve, reject) => crypto.scrypt(password, salt, 64, (e, k) => (e ? reject(e) : resolve(k.toString('hex')))));
}

const sha256 = (s) => crypto.createHash('sha256').update(s).digest('hex');
const hits = new Map();

function rateLimited(key, max, windowMs) {
  const now = Date.now();
  const arr = (hits.get(key) || []).filter((t) => now - t < windowMs);
  arr.push(now);
  hits.set(key, arr);
  return arr.length > max;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (!v.some((t) => now - t < 3600e3)) hits.delete(k); }, 600e3).unref();

const games = new Map();

function getIP(req) { return (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || 'x'; }

function send(res, status, obj) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type, Authorization', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS' });
  res.end(JSON.stringify(obj));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => { data += c; if (data.length > 10_000) { reject(new Error('big')); req.destroy(); } });
    req.on('end', () => { try { resolve(data ? JSON.parse(data) : {}); } catch { reject(new Error('json')); } });
    req.on('error', reject);
  });
}

function authUser(req) {
  const h = req.headers['authorization'] || '';
  const token = h.startsWith('Bearer ') ? h.slice(7) : '';
  if (!token) return null;
  return db.prepare('SELECT u.id, u.name, u.best_score FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token_hash = ?').get(sha256(token)) || null;
}

function newSession(userId) {
  const token = crypto.randomBytes(32).toString('hex');
  db.prepare('INSERT INTO sessions (token_hash, user_id, created_at) VALUES (?, ?, ?)').run(sha256(token), userId, Date.now());
  return token;
}

function pickQuestion(grade) {
  const pool = questionBank[grade];
  const q = pool[crypto.randomInt(pool.length)];
  const opts = [...q.opts];
  for (let i = opts.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [opts[i], opts[j]] = [opts[j], opts[i]]; }
  return { q, opts };
}

// ---------- مسیرها ----------
const routes = {
  'POST /api/register': async (req, res, body) => {
    const ip = getIP(req);
    if (rateLimited('reg:' + ip, 10, 3600e3)) return send(res, 429, { error: 'تعداد تلاش زیاد است.' });
    const name = String(body.name || '').trim().replace(/\s+/g, ' ');
    const password = String(body.password || '');
    if (name.length < 2 || name.length > 20) return send(res, 400, { error: 'نام باید بین ۲ تا ۲۰ حرف باشد.' });
    if (password.length < 6 || password.length > 100) return send(res, 400, { error: 'رمز عبور باید حداقل ۶ حرف باشد.' });
    const key = normalizeName(name);
    if (!key) return send(res, 400, { error: 'نام نامعتبر است.' });
    const salt = crypto.randomBytes(16).toString('hex');
    const hash = await hashPassword(password, salt);
    try {
      const r = db.prepare('INSERT INTO users (name, name_key, salt, hash, created_at) VALUES (?, ?, ?, ?, ?)').run(name, key, salt, hash, Date.now());
      const token = newSession(Number(r.lastInsertRowid));
      return send(res, 200, { token, name, bestScore: 0 });
    } catch (e) {
      if (String(e.message).includes('UNIQUE')) return send(res, 409, { error: 'این نام قبلاً گرفته شده.' });
      throw e;
    }
  },

  'POST /api/login': async (req, res, body) => {
    const ip = getIP(req);
    if (rateLimited('login:' + ip, 20, 15 * 60e3)) return send(res, 429, { error: 'تلاش‌های زیاد.' });
    const name = String(body.name || '').trim();
    const password = String(body.password || '');
    const key = normalizeName(name);
    
    // --- بررسی ورود ادمین ---
    if (name.toLowerCase() === 'admin' && password === ADMIN_PASSWORD) {
      const adminToken = 'ADMIN_' + crypto.randomBytes(16).toString('hex');
      db.prepare('INSERT INTO sessions (token_hash, user_id, created_at) VALUES (?, ?, ?)').run(sha256(adminToken), 0, Date.now());
      return send(res, 200, { token: adminToken, name: 'Admin', bestScore: 0, isAdmin: true });
    }

    const u = db.prepare('SELECT * FROM users WHERE name_key = ?').get(key);
    const salt = u ? u.salt : '00'.repeat(16);
    const hash = await hashPassword(password, salt);
    const ok = u && crypto.timingSafeEqual(Buffer.from(hash, 'hex'), Buffer.from(u.hash, 'hex'));
    if (!ok) return send(res, 401, { error: 'نام یا رمز عبور اشتباه است.' });
    const token = newSession(u.id);
    return send(res, 200, { token, name: u.name, bestScore: u.best_score });
  },

  'GET /api/me': async (req, res) => {
    const h = req.headers['authorization'] || '';
    if (h.startsWith('Bearer ADMIN_')) return send(res, 200, { name: 'Admin', bestScore: 0, isAdmin: true });
    const u = authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    return send(res, 200, { name: u.name, bestScore: u.best_score });
  },

  'POST /api/logout': async (req, res) => {
    const h = req.headers['authorization'] || '';
    const token = h.startsWith('Bearer ') ? h.slice(7) : '';
    if (token) db.prepare('DELETE FROM sessions WHERE token_hash = ?').run(sha256(token));
    return send(res, 200, { ok: true });
  },

  'POST /api/game/start': async (req, res, body) => {
    const u = authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    const grade = Number(body.grade);
    if (!questionBank[grade]) return send(res, 400, { error: 'پایه نامعتبر است.' });
    const { q, opts } = pickQuestion(grade);
    games.set(u.id, { grade, score: 0, current: { q, issuedAt: Date.now() } });
    return send(res, 200, { score: 0, question: { text: q.q, subject: q.subject, opts }, seconds: QUESTION_SECONDS });
  },

  'POST /api/game/answer': async (req, res, body) => {
    const u = authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    const g = games.get(u.id);
    if (!g || !g.current) return send(res, 400, { error: 'بازی فعالی وجود ندارد.' });
    const { q, issuedAt } = g.current;
    g.current = null;
    const late = Date.now() - issuedAt > QUESTION_SECONDS * 1000 + GRACE_MS;
    const correct = !late && body.answer === q.ans;
    if (!correct) {
      const finalScore = g.score;
      games.delete(u.id);
      return send(res, 200, { correct: false, timeout: late, correctAnswer: q.ans, gameOver: true, score: finalScore, bestScore: u.best_score });
    }
    g.score += POINTS;
    let best = u.best_score;
    if (g.score > best) {
      best = g.score;
      db.prepare('UPDATE users SET best_score = ? WHERE id = ?').run(best, u.id);
    }
    const next = pickQuestion(g.grade);
    g.current = { q: next.q, issuedAt: Date.now() };
    return send(res, 200, { correct: true, correctAnswer: q.ans, gameOver: false, score: g.score, bestScore: best, question: { text: next.q.q, subject: next.q.subject, opts: next.opts }, seconds: QUESTION_SECONDS });
  },

  'POST /api/game/timeout': async (req, res) => {
    const u = authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    const g = games.get(u.id);
    if (!g || !g.current) return send(res, 400, { error: 'بازی فعالی وجود ندارد.' });
    if (Date.now() - g.current.issuedAt < (QUESTION_SECONDS - 1) * 1000) return send(res, 400, { error: 'هنوز زمان تمام نشده.' });
    const ans = g.current.q.ans, score = g.score;
    games.delete(u.id);
    return send(res, 200, { correctAnswer: ans, gameOver: true, score, bestScore: u.best_score });
  },

  'GET /api/league': async (req, res) => {
    const me = authUser(req);
    const top = db.prepare('SELECT name, best_score FROM users WHERE best_score > 0 ORDER BY best_score DESC, created_at ASC LIMIT 100').all();
    let myRank = null;
    if (me && me.best_score > 0) {
      myRank = 1 + db.prepare('SELECT COUNT(*) AS c FROM users WHERE best_score > ?').get(me.best_score).c;
    }
    return send(res, 200, { top: top.map((r, i) => ({ rank: i + 1, name: r.name, score: r.best_score })), me: me ? { name: me.name, score: me.best_score, rank: myRank } : null });
  },

  // --- مسیرهای مخصوص ادمین ---
  'GET /api/admin/users': async (req, res) => {
    const h = req.headers['authorization'] || '';
    if (!h.startsWith('Bearer ADMIN_')) return send(res, 403, { error: 'دسترسی غیرمجاز' });
    const users = db.prepare('SELECT id, name, best_score FROM users ORDER BY best_score DESC').all();
    return send(res, 200, { users });
  },

  'POST /api/admin/update': async (req, res, body) => {
    const h = req.headers['authorization'] || '';
    if (!h.startsWith('Bearer ADMIN_')) return send(res, 403, { error: 'دسترسی غیرمجاز' });
    const { id, newScore, deleteUser } = body;
    
    if (deleteUser) {
      db.prepare('DELETE FROM users WHERE id = ?').run(id);
      db.prepare('DELETE FROM sessions WHERE user_id = ?').run(id);
      return send(res, 200, { ok: true, message: 'کاربر حذف شد' });
    } else if (newScore !== undefined) {
      db.prepare('UPDATE users SET best_score = ? WHERE id = ?').run(Number(newScore), id);
      return send(res, 200, { ok: true, message: 'امتیاز تغییر کرد' });
    }
    return send(res, 400, { error: 'دستور نامشخص' });
  }
};

// ---------- فایل‌های استاتیک ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml', '.json': 'application/json' };
function serveStatic(req, res) {
  let p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
  if (p === '/') p = '/index.html';
  const file = path.normalize(path.join(PUBLIC_DIR, p));
  if (!file.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(data);
  });
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === 'OPTIONS') return send(res, 204, {});
    const pathname = new URL(req.url, 'http://x').pathname;
    const handler = routes[`${req.method} ${pathname}`];
    if (handler) {
      const body = req.method === 'POST' ? await readBody(req) : {};
      return await handler(req, res, body);
    }
    if (pathname.startsWith('/api/')) return send(res, 404, { error: 'not found' });
    return serveStatic(req, res);
  } catch (e) {
    console.error(e);
    send(res, 500, { error: 'خطای سرور' });
  }
});

server.listen(PORT, () => console.log(`Quiz server running on http://localhost:${PORT}`));
