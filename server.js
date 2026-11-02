// سرور کوئیز آنلاین - بدون هیچ وابستگی خارجی (فقط Node.js نسخه ۲۲.۵ به بالا)
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { DatabaseSync } = require('node:sqlite');

const PORT = process.env.PORT || 3000;
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'data.db');
const PUBLIC_DIR = path.join(__dirname, 'public');

// ---------- ذخیره‌سازی: اگر کلیدهای Upstash داده شده باشد از Redis، وگرنه SQLite ----------
// خواندن متغیرهای محیطی با تحمل فاصله‌ی اضافه و نقل‌قول (مثلاً وقتی فایل .env چسبانده شده)
function cleanName(k) { return String(k).trim().replace(/^["']+|["']+$/g, '').trim(); }
function cleanValue(v) { return String(v == null ? '' : v).trim().replace(/^["']+|["']+$/g, '').trim(); }
function readEnv(name) {
  const k = Object.keys(process.env).find((x) => cleanName(x) === name);
  return k ? cleanValue(process.env[k]) : '';
}
const REDIS_URL = readEnv('UPSTASH_REDIS_REST_URL');
const REDIS_TOKEN = readEnv('UPSTASH_REDIS_REST_TOKEN');
// خط تشخیصی: فقط «پیدا شد یا نه» و طول مقدار را می‌نویسد، هرگز خود مقدار را
console.log('Redis env check: url=' + (REDIS_URL ? 'found(len ' + REDIS_URL.length + ', https=' + REDIS_URL.startsWith('https://') + ')' : 'MISSING')
  + ' token=' + (REDIS_TOKEN ? 'found(len ' + REDIS_TOKEN.length + ')' : 'MISSING'));
console.log('Env names that mention UPSTASH/REDIS:', JSON.stringify(Object.keys(process.env).filter((x) => /upstash|redis/i.test(x))));
const SESSION_TTL = 60 * 60 * 24 * 90; // ۹۰ روز

function scoreFor(best, created) { return best + (1 - created / 1e14); } // امتیاز مساوی: قدیمی‌تر بالاتر
function memberFor(key, name) { return key + '|' + name; }

function redisStore() {
  async function cmd(...args) {
    const r = await fetch(REDIS_URL.replace(/\/+$/, ''), { method: 'POST', headers: { Authorization: 'Bearer ' + REDIS_TOKEN }, body: JSON.stringify(args) });
    const j = await r.json();
    if (j.error) throw new Error('redis: ' + j.error);
    return j.result;
  }
  async function pipe(cmds) {
    const r = await fetch(REDIS_URL.replace(/\/+$/, '') + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + REDIS_TOKEN }, body: JSON.stringify(cmds) });
    const j = await r.json();
    return j.map((x) => { if (x.error) throw new Error('redis: ' + x.error); return x.result; });
  }
  return {
    kind: 'redis',
    async createUser(key, name, salt, hash) {
      const created = Date.now();
      const ok = await cmd('SET', 'user:' + key, JSON.stringify({ name, salt, hash, created }), 'NX');
      return ok === 'OK';
    },
    async getUser(key) {
      const [raw, best] = await pipe([['GET', 'user:' + key], ['HGET', 'bests', key]]);
      if (!raw) return null;
      const u = JSON.parse(raw);
      return { key, name: u.name, salt: u.salt, hash: u.hash, created: u.created, best: Number(best || 0) };
    },
    async createSession(tokenHash, userKey) { await cmd('SET', 'sess:' + tokenHash, userKey, 'EX', SESSION_TTL); },
    async getSession(tokenHash) { return await cmd('GET', 'sess:' + tokenHash); },
    async deleteSession(tokenHash) { await cmd('DEL', 'sess:' + tokenHash); },
    async setBest(user, best) {
      await pipe([['HSET', 'bests', user.key, String(best)], ['ZADD', 'lb', String(scoreFor(best, user.created)), memberFor(user.key, user.name)]]);
    },
    async leaderboard(limit) {
      const flat = (await cmd('ZREVRANGE', 'lb', '0', String(limit - 1), 'WITHSCORES')) || [];
      const out = [];
      for (let i = 0; i < flat.length; i += 2) {
        const m = flat[i]; out.push({ name: m.slice(m.indexOf('|') + 1), score: Math.floor(Number(flat[i + 1])) });
      }
      return out;
    },
    async rankOf(user) {
      const r = await cmd('ZREVRANK', 'lb', memberFor(user.key, user.name));
      return r === null || r === undefined ? null : r + 1;
    },
    async listUsers() {
      let cursor = '0'; const keys = [];
      do {
        const r = await cmd('SCAN', cursor, 'MATCH', 'user:*', 'COUNT', '200');
        cursor = String(r[0]); keys.push(...r[1]);
      } while (cursor !== '0' && keys.length < 5000);
      if (!keys.length) return [];
      const [vals, flat] = await pipe([['MGET', ...keys], ['HGETALL', 'bests']]);
      const bests = {};
      for (let i = 0; i < (flat || []).length; i += 2) bests[flat[i]] = Number(flat[i + 1]);
      return keys.map((k, i) => {
        const key = k.slice(5); let name = key;
        try { name = JSON.parse(vals[i]).name; } catch (e) {}
        return { key, name, best: bests[key] || 0 };
      }).sort((a, b) => b.best - a.best);
    },
    async deleteUser(key) {
      const raw = await cmd('GET', 'user:' + key);
      let name = key; if (raw) { try { name = JSON.parse(raw).name; } catch (e) {} }
      await pipe([['DEL', 'user:' + key], ['HDEL', 'bests', key], ['ZREM', 'lb', memberFor(key, name)]]);
    },
    async resetScores() { await pipe([['DEL', 'bests'], ['DEL', 'lb']]); }
  };
}

function sqliteStore() {
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
  const toUser = (r) => r && { key: r.name_key, name: r.name, salt: r.salt, hash: r.hash, created: r.created_at, best: r.best_score };
  return {
    kind: 'sqlite',
    async createUser(key, name, salt, hash) {
      try { db.prepare('INSERT INTO users (name, name_key, salt, hash, created_at) VALUES (?, ?, ?, ?, ?)').run(name, key, salt, hash, Date.now()); return true; }
      catch (e) { if (String(e.message).includes('UNIQUE')) return false; throw e; }
    },
    async getUser(key) { return toUser(db.prepare('SELECT * FROM users WHERE name_key = ?').get(key)); },
    async createSession(tokenHash, userKey) {
      const u = db.prepare('SELECT id FROM users WHERE name_key = ?').get(userKey);
      db.prepare('INSERT INTO sessions (token_hash, user_id, created_at) VALUES (?, ?, ?)').run(tokenHash, u.id, Date.now());
    },
    async getSession(tokenHash) {
      const r = db.prepare('SELECT u.name_key AS k FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token_hash = ?').get(tokenHash);
      return r ? r.k : null;
    },
    async deleteSession(tokenHash) { db.prepare('DELETE FROM sessions WHERE token_hash = ?').run(tokenHash); },
    async setBest(user, best) { db.prepare('UPDATE users SET best_score = ? WHERE name_key = ?').run(best, user.key); },
    async leaderboard(limit) {
      return db.prepare('SELECT name, best_score FROM users WHERE best_score > 0 ORDER BY best_score DESC, created_at ASC LIMIT ?').all(limit)
        .map((r) => ({ name: r.name, score: r.best_score }));
    },
    async rankOf(user) {
      if (!user.best) return null;
      return 1 + db.prepare('SELECT COUNT(*) AS c FROM users WHERE best_score > ?').get(user.best).c;
    },
    async listUsers() {
      return db.prepare('SELECT name, name_key, best_score FROM users ORDER BY best_score DESC, created_at ASC').all()
        .map((r) => ({ key: r.name_key, name: r.name, best: r.best_score }));
    },
    async deleteUser(key) {
      const u = db.prepare('SELECT id FROM users WHERE name_key = ?').get(key);
      if (!u) return;
      db.prepare('DELETE FROM sessions WHERE user_id = ?').run(u.id);
      db.prepare('DELETE FROM users WHERE id = ?').run(u.id);
    },
    async resetScores() { db.prepare('UPDATE users SET best_score = 0').run(); }
  };
}

const store = REDIS_URL && REDIS_TOKEN ? redisStore() : sqliteStore();
console.log('Storage:', store.kind);

// کش کوتاه‌مدت در حافظه تا تعداد درخواست به دیتابیس کم شود
const userCache = new Map();      // key -> { user, at }
const sessionCache = new Map();   // tokenHash -> userKey
async function getUserCached(key) {
  const c = userCache.get(key);
  if (c && Date.now() - c.at < 10 * 60e3) return c.user;
  const u = await store.getUser(key);
  if (u) userCache.set(key, { user: u, at: Date.now() });
  return u;
}
let lbCache = { at: 0, data: null };

// ---------- بانک سوال (فقط روی سرور؛ جواب‌ها هیچ‌وقت به کلاینت نمی‌رسن) ----------
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
const GRACE_MS = 2500;            // تحمل تأخیر شبکه
const POINTS = 10;

function normalizeName(s) {
  return s.normalize('NFKC')
    .replace(/ي/g, 'ی').replace(/ك/g, 'ک')
    .replace(/[\u200c\u200d\u200e\u200f\s]+/g, '')   // نیم‌فاصله و فاصله
    .toLowerCase();
}

function hashPassword(password, salt) {
  return new Promise((resolve, reject) =>
    crypto.scrypt(password, salt, 64, (e, k) => (e ? reject(e) : resolve(k.toString('hex')))));
}

const sha256 = (s) => crypto.createHash('sha256').update(s).digest('hex');

// محدودیت تعداد درخواست (ساده، در حافظه)
const hits = new Map();
function rateLimited(key, max, windowMs) {
  const now = Date.now();
  const arr = (hits.get(key) || []).filter((t) => now - t < windowMs);
  arr.push(now);
  hits.set(key, arr);
  return arr.length > max;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (!v.some((t) => now - t < 3600e3)) hits.delete(k); }, 600e3).unref();

// بازی‌های در جریان (در حافظه)
const games = new Map(); // userKey -> { grade, score, current:{q, issuedAt}, over }

function getIP(req) {
  return (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || 'x';
}

function send(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Admin-Key',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
  });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => { data += c; if (data.length > 10_000) { reject(new Error('big')); req.destroy(); } });
    req.on('end', () => { try { resolve(data ? JSON.parse(data) : {}); } catch { reject(new Error('json')); } });
    req.on('error', reject);
  });
}

// ---------- حساب مدیر (ورود از داخل خود بازی) ----------
// رمز مدیر فقط از متغیر محیطی ADMIN_PASSWORD خوانده می‌شود و هرگز داخل کد یا گیت‌هاب نیست.
function adminPassword() { const p = readEnv('ADMIN_PASSWORD'); return p.length >= 8 ? p : ''; }
function adminUserKey() { return normalizeName(readEnv('ADMIN_USER') || 'admin'); }
function adminDisplayName() { return readEnv('ADMIN_USER') || 'admin'; }
function isReservedName(key) { return key === adminUserKey() || key === 'admin'; }
function sha(s) { return crypto.createHash('sha256').update(String(s)).digest(); }
function adminPasswordMatches(given) {
  const p = adminPassword();
  return !!p && crypto.timingSafeEqual(sha(given), sha(p));
}
function signAdmin(exp) { return crypto.createHmac('sha256', sha('admin-session:' + adminPassword())).update('adm.' + exp).digest('hex'); }
function makeAdminToken() { const exp = Date.now() + 30 * 864e5; return 'adm.' + exp + '.' + signAdmin(exp); }
function verifyAdminToken(t) {
  if (!adminPassword()) return false;
  const p = String(t).split('.');
  if (p.length !== 3 || p[0] !== 'adm') return false;
  const exp = Number(p[1]);
  if (!(exp > Date.now())) return false;
  const a = Buffer.from(signAdmin(exp), 'hex'); const b = Buffer.from(p[2], 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
async function ensureAdminUser() {
  const key = adminUserKey();
  let u = await getUserCached(key);
  if (!u) {
    const salt = crypto.randomBytes(16).toString('hex');
    const hash = await hashPassword(crypto.randomBytes(24).toString('hex'), salt); // رمز تصادفی؛ ورود فقط با ADMIN_PASSWORD ممکن است
    await store.createUser(key, adminDisplayName(), salt, hash);
    userCache.delete(key);
    u = await getUserCached(key);
  }
  return u;
}

async function authUser(req) {
  const h = req.headers['authorization'] || '';
  const token = h.startsWith('Bearer ') ? h.slice(7) : '';
  if (!token) return null;
  if (token.startsWith('adm.')) {            // نشست مدیر (با امضا، نه در دیتابیس)
    if (!verifyAdminToken(token)) return null;
    req._admin = true;
    return await ensureAdminUser();
  }
  const th = sha256(token);
  let key = sessionCache.get(th);
  if (!key) {
    key = await store.getSession(th);
    if (!key) return null;
    sessionCache.set(th, key);
  }
  return await getUserCached(key);
}

async function newSession(user) {
  const token = crypto.randomBytes(32).toString('hex');
  const th = sha256(token);
  await store.createSession(th, user.key);
  sessionCache.set(th, user.key);
  userCache.set(user.key, { user, at: Date.now() });
  return token;
}

function pickQuestion(grade) {
  const pool = questionBank[grade];
  const q = pool[crypto.randomInt(pool.length)];
  const opts = [...q.opts];
  for (let i = opts.length - 1; i > 0; i--) { const j = crypto.randomInt(i + 1); [opts[i], opts[j]] = [opts[j], opts[i]]; }
  return { q, opts };
}

// ---------- بخش مدیریت (فقط با ADMIN_KEY که خودت روی سرور می‌گذاری) ----------
function adminOk(req) {
  const k = readEnv('ADMIN_KEY');
  if (!k || k.length < 8) return false;          // اگر کلید نگذاشته باشی یا کوتاه باشد، مدیریت خاموش است
  const given = String(req.headers['x-admin-key'] || '');
  const a = crypto.createHash('sha256').update(given).digest();
  const b = crypto.createHash('sha256').update(k).digest();
  return crypto.timingSafeEqual(a, b);
}
async function adminGuard(req, res) {
  if (rateLimited('admin:' + getIP(req), 60, 15 * 60e3)) { send(res, 429, { error: 'تلاش زیاد. بعداً امتحان کنید.' }); return false; }
  if (req.headers['x-admin-key'] !== undefined) {                  // صفحه‌ی /admin با کلید
    if (adminOk(req)) return true;
    send(res, 401, { error: 'کلید مدیریت اشتباه است یا ADMIN_KEY روی سرور تنظیم نشده.' }); return false;
  }
  const u = await authUser(req);                                    // ورود از داخل بازی با حساب مدیر
  if (!u) { send(res, 401, { error: 'unauthorized' }); return false; }
  if (!req._admin) { send(res, 403, { error: 'دسترسی مدیر ندارید.' }); return false; }
  return true;
}
const ADMIN_HTML = `<!doctype html><html lang="fa" dir="rtl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>مدیریت</title>
<style>body{font-family:sans-serif;background:#10231a;color:#eee;margin:0;padding:16px}h1{font-size:20px}input,button{font-size:16px;padding:10px;border-radius:8px;border:1px solid #555;margin:4px 0}
input{width:100%;box-sizing:border-box;background:#0b1812;color:#fff}button{background:#7a5a3a;color:#fff;cursor:pointer}button.danger{background:#a33}
.row{display:flex;justify-content:space-between;align-items:center;gap:8px;padding:8px 0;border-bottom:1px solid #2a4a3a}.row span{word-break:break-all}#msg{color:#ffd966;min-height:22px}</style></head><body>
<h1>مدیریت لیگ</h1><div id="login"><input id="k" type="password" placeholder="کلید مدیریت (ADMIN_KEY)"><button onclick="load()">ورود و نمایش</button></div>
<div id="msg"></div><div id="tools" style="display:none"><button onclick="load()">بازخوانی</button> <button class="danger" onclick="resetAll()">صفر کردن امتیاز همه (حساب‌ها می‌مانند)</button><div id="list"></div></div>
<script>
let KEY='';
async function call(p,b){const r=await fetch(p,{method:'POST',headers:{'Content-Type':'application/json','X-Admin-Key':KEY},body:JSON.stringify(b||{})});const j=await r.json().catch(()=>({}));if(!r.ok)throw new Error(j.error||'خطا');return j}
function msg(t){document.getElementById('msg').textContent=t||''}
async function load(){const v=document.getElementById('k').value;if(v)KEY=v;msg('در حال بارگذاری...');try{const d=await call('/api/admin/users');render(d.users);msg(d.users.length+' کاربر');document.getElementById('tools').style.display='block'}catch(e){msg(e.message)}}
function render(users){const l=document.getElementById('list');l.textContent='';users.forEach(u=>{const row=document.createElement('div');row.className='row';const s=document.createElement('span');s.textContent=u.name+' — '+u.best+' امتیاز';const b=document.createElement('button');b.className='danger';b.textContent='حذف';b.onclick=()=>del(u);row.appendChild(s);row.appendChild(b);l.appendChild(row)})}
async function del(u){if(!confirm('حساب «'+u.name+'» برای همیشه حذف شود؟'))return;try{await call('/api/admin/delete',{key:u.key});await load()}catch(e){msg(e.message)}}
async function resetAll(){if(!confirm('امتیاز همه صفر شود؟ حساب‌ها حذف نمی‌شوند.'))return;try{await call('/api/admin/reset-scores');await load()}catch(e){msg(e.message)}}
</script></body></html>`;

// ---------- مسیرها ----------
const routes = {
  'POST /api/register': async (req, res, body) => {
    const ip = getIP(req);
    if (rateLimited('reg:' + ip, 10, 3600e3)) return send(res, 429, { error: 'تعداد تلاش زیاد است. کمی بعد دوباره امتحان کنید.' });

    const name = String(body.name || '').trim().replace(/\s+/g, ' ');
    const password = String(body.password || '');
    if (name.length < 2 || name.length > 20) return send(res, 400, { error: 'نام باید بین ۲ تا ۲۰ حرف باشد.' });
    if (!/^[\p{L}\p{N} _.\-\u200c]+$/u.test(name)) return send(res, 400, { error: 'نام فقط می‌تواند شامل حرف، عدد، فاصله و - _ . باشد.' });
    if (password.length < 6 || password.length > 100) return send(res, 400, { error: 'رمز عبور باید حداقل ۶ حرف باشد.' });

    const key = normalizeName(name);
    if (!key) return send(res, 400, { error: 'نام نامعتبر است.' });
    if (isReservedName(key)) return send(res, 409, { error: 'این نام رزرو شده. یک نام دیگر انتخاب کنید.' });

    const salt = crypto.randomBytes(16).toString('hex');
    const hash = await hashPassword(password, salt);
    const ok = await store.createUser(key, name, salt, hash);
    if (!ok) return send(res, 409, { error: 'این نام قبلاً گرفته شده. یک نام دیگر انتخاب کنید.' });
    const user = await store.getUser(key);
    const token = await newSession(user);
    return send(res, 200, { token, name: user.name, bestScore: 0 });
  },

  'POST /api/login': async (req, res, body) => {
    const ip = getIP(req);
    if (rateLimited('login:' + ip, 20, 15 * 60e3)) return send(res, 429, { error: 'تلاش‌های زیاد. ۱۵ دقیقه دیگر دوباره امتحان کنید.' });

    const name = String(body.name || '').trim();
    const password = String(body.password || '');
    const key = normalizeName(name);
    if (rateLimited('loginuser:' + key, 10, 15 * 60e3)) return send(res, 429, { error: 'تلاش‌های زیاد برای این حساب. بعداً امتحان کنید.' });

    if (key && key === adminUserKey()) {
      if (!adminPasswordMatches(password)) return send(res, 401, { error: 'نام یا رمز عبور اشتباه است.' });
      const au = await ensureAdminUser();
      return send(res, 200, { token: makeAdminToken(), name: au.name, bestScore: au.best, isAdmin: true });
    }
    const u = key ? await store.getUser(key) : null;
    const salt = u ? u.salt : '00'.repeat(16);
    const hash = await hashPassword(password, salt);
    const ok = u && crypto.timingSafeEqual(Buffer.from(hash, 'hex'), Buffer.from(u.hash, 'hex'));
    if (!ok) return send(res, 401, { error: 'نام یا رمز عبور اشتباه است.' });
    const shared = await getUserCached(key);
    const token = await newSession(shared || u);
    return send(res, 200, { token, name: u.name, bestScore: (shared || u).best });
  },

  'GET /api/me': async (req, res) => {
    const u = await authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    return send(res, 200, { name: u.name, bestScore: u.best, isAdmin: !!req._admin });
  },

  'POST /api/logout': async (req, res) => {
    const h = req.headers['authorization'] || '';
    const token = h.startsWith('Bearer ') ? h.slice(7) : '';
    if (token) {
      const th = sha256(token);
      const u = await authUser(req);
      if (u) games.delete(u.key);
      sessionCache.delete(th);
      await store.deleteSession(th);
    }
    return send(res, 200, { ok: true });
  },

  'POST /api/game/start': async (req, res, body) => {
    const u = await authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    const grade = Number(body.grade);
    if (!questionBank[grade]) return send(res, 400, { error: 'پایه نامعتبر است.' });
    const { q, opts } = pickQuestion(grade);
    games.set(u.key, { grade, score: 0, current: { q, issuedAt: Date.now() } });
    return send(res, 200, { score: 0, question: { text: q.q, subject: q.subject, opts }, seconds: QUESTION_SECONDS });
  },

  'POST /api/game/answer': async (req, res, body) => {
    const u = await authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    const g = games.get(u.key);
    if (!g || !g.current) return send(res, 400, { error: 'بازی فعالی وجود ندارد.' });

    const { q, issuedAt } = g.current;
    g.current = null; // هر سوال فقط یک بار قابل پاسخ است
    const late = Date.now() - issuedAt > QUESTION_SECONDS * 1000 + GRACE_MS;
    const correct = !late && body.answer === q.ans;

    if (!correct) {
      const finalScore = g.score;
      games.delete(u.key);
      return send(res, 200, { correct: false, timeout: late, correctAnswer: q.ans, gameOver: true, score: finalScore, bestScore: u.best });
    }

    g.score += POINTS;
    if (g.score > u.best) {
      u.best = g.score;
      lbCache.at = 0;
      await store.setBest(u, u.best);
    }
    const next = pickQuestion(g.grade);
    g.current = { q: next.q, issuedAt: Date.now() };
    return send(res, 200, {
      correct: true, correctAnswer: q.ans, gameOver: false, score: g.score, bestScore: u.best,
      question: { text: next.q.q, subject: next.q.subject, opts: next.opts }, seconds: QUESTION_SECONDS
    });
  },

  'POST /api/game/timeout': async (req, res) => {
    const u = await authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    const g = games.get(u.key);
    if (!g || !g.current) return send(res, 400, { error: 'بازی فعالی وجود ندارد.' });
    if (Date.now() - g.current.issuedAt < (QUESTION_SECONDS - 1) * 1000) return send(res, 400, { error: 'هنوز زمان تمام نشده.' });
    const ans = g.current.q.ans, score = g.score;
    games.delete(u.key);
    return send(res, 200, { correctAnswer: ans, gameOver: true, score, bestScore: u.best });
  },

  'GET /admin': async (req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(ADMIN_HTML);
  },

  'POST /api/admin/users': async (req, res) => {
    if (!(await adminGuard(req, res))) return;
    return send(res, 200, { users: await store.listUsers() });
  },

  'POST /api/admin/delete': async (req, res, body) => {
    if (!(await adminGuard(req, res))) return;
    const key = String(body.key || '');
    if (!key) return send(res, 400, { error: 'کلید کاربر خالی است.' });
    if (key === adminUserKey()) return send(res, 400, { error: 'حساب مدیر حذف نمی‌شود.' });
    await store.deleteUser(key);
    userCache.delete(key); games.delete(key); lbCache.at = 0;
    return send(res, 200, { ok: true });
  },

  'POST /api/admin/reset-scores': async (req, res) => {
    if (!(await adminGuard(req, res))) return;
    await store.resetScores();
    userCache.clear(); games.clear(); lbCache.at = 0;
    return send(res, 200, { ok: true });
  },

  'GET /api/league': async (req, res) => {
    const me = await authUser(req);
    if (!lbCache.data || Date.now() - lbCache.at > 15e3) {
      lbCache = { at: Date.now(), data: await store.leaderboard(100) };
    }
    let myRank = null;
    if (me && me.best > 0) myRank = await store.rankOf(me);
    return send(res, 200, {
      top: lbCache.data.map((r, i) => ({ rank: i + 1, name: r.name, score: r.score })),
      me: me ? { name: me.name, score: me.best, rank: myRank } : null
    });
  }
};

// ---------- فایل‌های استاتیک ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml', '.json': 'application/json' };
function serveStatic(req, res) {
  let p = decodeURIComponent(new URL(req.url, 'http://x').pathname);
  if (p === '/') p = '/index.html';
  let file = path.normalize(path.join(PUBLIC_DIR, p));
  if (!file.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end(); }
  // اگر پوشه public ساخته نشده بود، index.html کنار server.js هم قبول است (فقط همین یک فایل)
  if (p === '/index.html' && !fs.existsSync(file)) file = path.join(__dirname, 'index.html');
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
