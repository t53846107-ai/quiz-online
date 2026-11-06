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
      const [raw, best, wins] = await pipe([['GET', 'user:' + key], ['HGET', 'bests', key], ['HGET', 'wins', key]]);
      if (!raw) return null;
      const u = JSON.parse(raw);
      return { key, name: u.name, salt: u.salt, hash: u.hash, created: u.created, best: Number(best || 0), wins: Number(wins || 0) };
    },
    async createSession(tokenHash, userKey) { await cmd('SET', 'sess:' + tokenHash, userKey, 'EX', SESSION_TTL); },
    async getSession(tokenHash) { return await cmd('GET', 'sess:' + tokenHash); },
    async deleteSession(tokenHash) { await cmd('DEL', 'sess:' + tokenHash); },
    async setBest(user, best) {
      if (best <= 0) { await pipe([['HSET', 'bests', user.key, '0'], ['ZREM', 'lb', memberFor(user.key, user.name)]]); return; }
      await pipe([['HSET', 'bests', user.key, String(best)], ['ZADD', 'lb', String(scoreFor(best, user.created)), memberFor(user.key, user.name)]]);
    },
    async addWin(user) {
      const n = Number(await cmd('HINCRBY', 'wins', user.key, '1'));
      await cmd('ZADD', 'lbw', String(scoreFor(n, user.created)), memberFor(user.key, user.name));
      return n;
    },
    async leaderboardWins(limit) {
      const flat = (await cmd('ZREVRANGE', 'lbw', '0', String(limit - 1), 'WITHSCORES')) || [];
      const out = [];
      for (let i = 0; i < flat.length; i += 2) {
        const m = flat[i]; out.push({ name: m.slice(m.indexOf('|') + 1), score: Math.floor(Number(flat[i + 1])) });
      }
      return out;
    },
    async rankOfWins(user) {
      const r = await cmd('ZREVRANK', 'lbw', memberFor(user.key, user.name));
      return r === null || r === undefined ? null : r + 1;
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
      await pipe([['DEL', 'user:' + key], ['HDEL', 'bests', key], ['ZREM', 'lb', memberFor(key, name)], ['HDEL', 'wins', key], ['ZREM', 'lbw', memberFor(key, name)]]);
    },
    async resetScores() { await pipe([['DEL', 'bests'], ['DEL', 'lb'], ['DEL', 'wins'], ['DEL', 'lbw']]); }
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
  try { db.exec('ALTER TABLE users ADD COLUMN wins INTEGER NOT NULL DEFAULT 0'); } catch (e) { /* ستون از قبل هست */ }
  const toUser = (r) => r && { key: r.name_key, name: r.name, salt: r.salt, hash: r.hash, created: r.created_at, best: r.best_score, wins: r.wins || 0 };
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
    async addWin(user) {
      db.prepare('UPDATE users SET wins = wins + 1 WHERE name_key = ?').run(user.key);
      return db.prepare('SELECT wins FROM users WHERE name_key = ?').get(user.key).wins;
    },
    async leaderboardWins(limit) {
      return db.prepare('SELECT name, wins FROM users WHERE wins > 0 ORDER BY wins DESC, created_at ASC LIMIT ?').all(limit)
        .map((r) => ({ name: r.name, score: r.wins }));
    },
    async rankOfWins(user) {
      if (!user.wins) return null;
      return 1 + db.prepare('SELECT COUNT(*) AS c FROM users WHERE wins > ?').get(user.wins).c;
    },
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
    async resetScores() { db.prepare('UPDATE users SET best_score = 0, wins = 0').run(); }
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
let wlbCache = { at: 0, data: null };   // جدول «بیشترین برد»

// ---------- بانک سوال (فقط روی سرور؛ جواب‌ها هیچ‌وقت به کلاینت نمی‌رسن) ----------
const questionBank = {
  7: [
    { q: "حاصل عبارت: -8 + 15 چند است؟", opts: ["7", "-7", "23", "-23"], ans: "7", subject: "ریاضی" },
    { q: "در آرایه ادبی جناس، دو کلمه چه ویژگی دارند؟", opts: ["در تلفظ یکسان و در معنی متفاوت", "در معنی یکسان", "هم‌معنی دقیق", "متضاد هم"], ans: "در تلفظ یکسان و در معنی متفاوت", subject: "فارسی" },
    { q: "معنی کلمه «صدیق» در عربی چیست؟", opts: ["دوست", "دشمن", "معلم", "دانش‌آموز"], ans: "دوست", subject: "عربی" },
    { q: "کدام کلید میانبر برای کپی کردن استفاده می‌شود؟", opts: ["Ctrl + C", "Ctrl + V", "Ctrl + X", "Ctrl + Z"], ans: "Ctrl + C", subject: "کامپیوتر" },
    { q: "معنی کلمه «Notebook» چیست؟", opts: ["دفترچه یادداشت", "کتاب", "مداد", "تخته"], ans: "دفترچه یادداشت", subject: "انگلیسی" },
    { q: "حاصل 3 × (-4) چند است؟", opts: ["-12", "12", "-7", "7"], ans: "-12", subject: "ریاضی" },
    { q: "کوچک‌ترین عدد اول کدام است؟", opts: ["2", "1", "3", "0"], ans: "2", subject: "ریاضی" },
    { q: "بزرگ‌ترین مقسوم‌علیه مشترک دو عدد 12 و 18 کدام است؟", opts: ["6", "3", "12", "36"], ans: "6", subject: "ریاضی" },
    { q: "مجموع زاویه‌های داخلی هر مثلث چند درجه است؟", opts: ["180 درجه", "90 درجه", "360 درجه", "270 درجه"], ans: "180 درجه", subject: "ریاضی" },
    { q: "حاصل (-5) - (-9) چند است؟", opts: ["4", "-14", "14", "-4"], ans: "4", subject: "ریاضی" },
    { q: "متضاد کلمه «بلند» کدام است؟", opts: ["کوتاه", "رفیع", "دراز", "عالی"], ans: "کوتاه", subject: "فارسی" },
    { q: "مفرد کلمه «علما» کدام است؟", opts: ["عالِم", "علم", "معلم", "عالمان"], ans: "عالِم", subject: "فارسی" },
    { q: "کتاب «گلستان» اثر کدام شاعر و نویسنده است؟", opts: ["سعدی", "حافظ", "فردوسی", "مولوی"], ans: "سعدی", subject: "فارسی" },
    { q: "کتاب «شاهنامه» را چه کسی سروده است؟", opts: ["فردوسی", "سعدی", "نظامی", "خیام"], ans: "فردوسی", subject: "فارسی" },
    { q: "در جمله «علی کتاب را خواند» نهاد (فاعل) کدام است؟", opts: ["علی", "کتاب", "را", "خواند"], ans: "علی", subject: "فارسی" },
    { q: "معنی کلمه «مدرسة» چیست؟", opts: ["مدرسه", "بیمارستان", "کتابخانه", "بازار"], ans: "مدرسه", subject: "عربی" },
    { q: "معنی کلمه «ماء» چیست؟", opts: ["آب", "شیر", "نان", "چای"], ans: "آب", subject: "عربی" },
    { q: "معنی کلمه «بیت» چیست؟", opts: ["خانه", "مسجد", "باغ", "کوچه"], ans: "خانه", subject: "عربی" },
    { q: "معنی کلمه «شمس» چیست؟", opts: ["خورشید", "ماه", "ستاره", "آسمان"], ans: "خورشید", subject: "عربی" },
    { q: "معنی کلمه «ولد» چیست؟", opts: ["پسر", "دختر", "مادر", "پدر"], ans: "پسر", subject: "عربی" },
    { q: "کلید میانبر برای چسباندن (Paste) کدام است؟", opts: ["Ctrl + V", "Ctrl + C", "Ctrl + X", "Ctrl + Z"], ans: "Ctrl + V", subject: "کامپیوتر" },
    { q: "کدام یک وسیله ورودی است؟", opts: ["صفحه‌کلید", "چاپگر", "بلندگو", "مانیتور"], ans: "صفحه‌کلید", subject: "کامپیوتر" },
    { q: "پسوند فایل‌های برنامه Word کدام است؟", opts: [".docx", ".jpg", ".mp3", ".exe"], ans: ".docx", subject: "کامپیوتر" },
    { q: "هر بایت از چند بیت تشکیل شده است؟", opts: ["8", "4", "16", "10"], ans: "8", subject: "کامپیوتر" },
    { q: "به کدام قطعه «مغز کامپیوتر» می‌گویند؟", opts: ["پردازنده (CPU)", "ماوس", "مانیتور", "چاپگر"], ans: "پردازنده (CPU)", subject: "کامپیوتر" },
    { q: "معنی کلمه «Teacher» چیست؟", opts: ["معلم", "پزشک", "راننده", "نانوا"], ans: "معلم", subject: "انگلیسی" },
    { q: "معنی کلمه «Monday» چیست؟", opts: ["دوشنبه", "یکشنبه", "سه‌شنبه", "جمعه"], ans: "دوشنبه", subject: "انگلیسی" },
    { q: "معنی کلمه «Window» چیست؟", opts: ["پنجره", "در", "دیوار", "سقف"], ans: "پنجره", subject: "انگلیسی" },
    { q: "معنی «Thank you» چیست؟", opts: ["متشکرم", "ببخشید", "سلام", "خداحافظ"], ans: "متشکرم", subject: "انگلیسی" },
    { q: "معنی کلمه «Red» چیست؟", opts: ["قرمز", "آبی", "سبز", "زرد"], ans: "قرمز", subject: "انگلیسی" }
  ],
  8: [
    { q: "حاصل ضرب دو عدد منفی همواره چه علامتی دارد؟", opts: ["مثبت", "منفی", "صفر", "نامشخص"], ans: "مثبت", subject: "ریاضی" },
    { q: "عبارت «الهی دانایی ده که از راه نلغزیم» دارای چه آرایه‌ای است؟", opts: ["سجع", "جناس تام", "تضاد", "تشبیه"], ans: "سجع", subject: "فارسی" },
    { q: "ترجمه «هؤلاء طلاب» چیست؟", opts: ["این‌ها دانش‌آموزند", "آن دانش‌آموز است", "او معلم است", "این یک کتاب است"], ans: "این‌ها دانش‌آموزند", subject: "عربی" },
    { q: "کوچک‌ترین واحد حافظه در کامپیوتر چیست؟", opts: ["بیت", "بایت", "مگابایت", "گیگابایت"], ans: "بیت", subject: "کامپیوتر" },
    { q: "معنی کلمه «Dictionary» چیست؟", opts: ["فرهنگ لغت", "مدرسه", "کلاس", "دفتر"], ans: "فرهنگ لغت", subject: "انگلیسی" },
    { q: "حاصل 2 به توان 5 چند است؟", opts: ["32", "10", "25", "16"], ans: "32", subject: "ریاضی" },
    { q: "حاصل 1/2 + 1/4 کدام است؟", opts: ["3/4", "2/6", "1/6", "2/4"], ans: "3/4", subject: "ریاضی" },
    { q: "مساحت مستطیلی با طول 8 و عرض 5 چقدر است؟", opts: ["40", "13", "26", "80"], ans: "40", subject: "ریاضی" },
    { q: "محیط مربعی به ضلع 7 سانتی‌متر چقدر است؟", opts: ["28", "49", "14", "21"], ans: "28", subject: "ریاضی" },
    { q: "اگر x + 5 = 12 باشد، مقدار x چند است؟", opts: ["7", "17", "5", "60"], ans: "7", subject: "ریاضی" },
    { q: "منظومه «منطق‌الطیر» اثر کدام شاعر است؟", opts: ["عطار نیشابوری", "سعدی", "مولوی", "نظامی"], ans: "عطار نیشابوری", subject: "فارسی" },
    { q: "«مثنوی معنوی» سروده کدام شاعر است؟", opts: ["مولوی", "سعدی", "حافظ", "فردوسی"], ans: "مولوی", subject: "فارسی" },
    { q: "کدام واژه «مرکب» است؟", opts: ["دانش‌آموز", "درخت", "کوه", "ماه"], ans: "دانش‌آموز", subject: "فارسی" },
    { q: "کدام جمله دارای آرایه «تشبیه» است؟", opts: ["علی مانند شیر شجاع است", "علی به مدرسه رفت", "هوا امروز سرد است", "کتاب روی میز است"], ans: "علی مانند شیر شجاع است", subject: "فارسی" },
    { q: "مصراع «بنی‌آدم اعضای یکدیگرند» از کیست؟", opts: ["سعدی", "حافظ", "فردوسی", "مولوی"], ans: "سعدی", subject: "فارسی" },
    { q: "معنی کلمه «کبیر» چیست؟", opts: ["بزرگ", "کوچک", "زیبا", "زشت"], ans: "بزرگ", subject: "عربی" },
    { q: "معنی فعل «ذَهَبَ» چیست؟", opts: ["رفت", "آمد", "نشست", "خورد"], ans: "رفت", subject: "عربی" },
    { q: "معنی «أَیْنَ» چیست؟", opts: ["کجا", "کی", "چه", "چرا"], ans: "کجا", subject: "عربی" },
    { q: "معنی عبارت «مع السلامة» چیست؟", opts: ["خداحافظ", "سلام", "ممنون", "ببخشید"], ans: "خداحافظ", subject: "عربی" },
    { q: "معنی «ماذا» چیست؟", opts: ["چه چیزی", "چه کسی", "کجا", "کی"], ans: "چه چیزی", subject: "عربی" },
    { q: "پسوند فایل عکس کدام است؟", opts: [".jpg", ".docx", ".exe", ".mp3"], ans: ".jpg", subject: "کامپیوتر" },
    { q: "کدام گزینه یک مرورگر اینترنت است؟", opts: ["Chrome", "Word", "Excel", "Paint"], ans: "Chrome", subject: "کامپیوتر" },
    { q: "هر کیلوبایت چند بایت است؟", opts: ["1024", "1000", "100", "8"], ans: "1024", subject: "کامپیوتر" },
    { q: "کدام حافظه موقت است و با خاموش شدن کامپیوتر پاک می‌شود؟", opts: ["RAM", "هارد دیسک", "فلش مموری", "CD"], ans: "RAM", subject: "کامپیوتر" },
    { q: "فایل با پسوند mp3 چه نوع فایلی است؟", opts: ["صوتی", "تصویری", "متنی", "برنامه اجرایی"], ans: "صوتی", subject: "کامپیوتر" },
    { q: "معنی کلمه «Beautiful» چیست؟", opts: ["زیبا", "زشت", "بزرگ", "سریع"], ans: "زیبا", subject: "انگلیسی" },
    { q: "معنی کلمه «Library» چیست؟", opts: ["کتابخانه", "کتاب‌فروشی", "مدرسه", "بیمارستان"], ans: "کتابخانه", subject: "انگلیسی" },
    { q: "معنی کلمه «Always» چیست؟", opts: ["همیشه", "هرگز", "گاهی", "اغلب"], ans: "همیشه", subject: "انگلیسی" },
    { q: "معنی کلمه «Yesterday» چیست؟", opts: ["دیروز", "امروز", "فردا", "هفته پیش"], ans: "دیروز", subject: "انگلیسی" },
    { q: "معنی کلمه «Healthy» چیست؟", opts: ["سالم", "بیمار", "ضعیف", "خسته"], ans: "سالم", subject: "انگلیسی" }
  ],
  9: [
    { q: "جذر عدد ۱۶۹ کدام است؟", opts: ["13", "14", "12", "15"], ans: "13", subject: "ریاضی" },
    { q: "معنی واژه «میاه» در عربی چیست؟", opts: ["آب‌ها", "درختان", "کوه‌ها", "چشمه‌ها"], ans: "آب‌ها", subject: "عربی" },
    { q: "کدام گزینه یک سیستم‌عامل است؟", opts: ["ویندوز", "ورد", "اکسل", "فتوشاپ"], ans: "ویندوز", subject: "کامپیوتر" },
    { q: "معنی کلمه «Environment» چیست؟", opts: ["محیط زیست", "آزمایشگاه", "صنعت", "جغرافیا"], ans: "محیط زیست", subject: "انگلیسی" },
    { q: "اتحاد مربع دو جمله‌ای (a + b)² کدام است؟", opts: ["a² + 2ab + b²", "a² + b²", "a² - 2ab + b²", "2a + 2b"], ans: "a² + 2ab + b²", subject: "ریاضی" },
    { q: "شیب خط y = 2x + 3 چند است؟", opts: ["2", "3", "5", "1"], ans: "2", subject: "ریاضی" },
    { q: "احتمال «شیر» آمدن در پرتاب یک سکه سالم چقدر است؟", opts: ["1/2", "1/4", "1", "2"], ans: "1/2", subject: "ریاضی" },
    { q: "مجموع زاویه‌های داخلی هر چهارضلعی چند درجه است؟", opts: ["360 درجه", "180 درجه", "270 درجه", "540 درجه"], ans: "360 درجه", subject: "ریاضی" },
    { q: "حاصل (-3)² چند است؟", opts: ["9", "-9", "6", "-6"], ans: "9", subject: "ریاضی" },
    { q: "کتاب «بوستان» اثر کدام شاعر است؟", opts: ["سعدی", "حافظ", "فردوسی", "عطار"], ans: "سعدی", subject: "فارسی" },
    { q: "منظومه «لیلی و مجنون» را کدام شاعر سروده است؟", opts: ["نظامی گنجه‌ای", "مولوی", "سعدی", "فردوسی"], ans: "نظامی گنجه‌ای", subject: "فارسی" },
    { q: "اگر به اشیا و پدیده‌ها ویژگی‌های انسان بدهیم، کدام آرایه ساخته می‌شود؟", opts: ["تشخیص (جان‌بخشی)", "تضاد", "جناس", "سجع"], ans: "تشخیص (جان‌بخشی)", subject: "فارسی" },
    { q: "چه کسی را «پدر شعر نو فارسی» می‌دانند؟", opts: ["نیما یوشیج", "سهراب سپهری", "فروغ فرخزاد", "احمد شاملو"], ans: "نیما یوشیج", subject: "فارسی" },
    { q: "رباعیات مشهور به کدام شاعر تعلق دارد؟", opts: ["خیام", "سعدی", "فردوسی", "عطار"], ans: "خیام", subject: "فارسی" },
    { q: "معنی کلمه «الشَّجَرَة» چیست؟", opts: ["درخت", "گل", "میوه", "برگ"], ans: "درخت", subject: "عربی" },
    { q: "معنی فعل «أَکَلَ» چیست؟", opts: ["خورد", "نوشید", "خوابید", "رفت"], ans: "خورد", subject: "عربی" },
    { q: "جمع کلمه «کتاب» کدام است؟", opts: ["کُتُب", "کاتب", "مکتب", "مکتبة"], ans: "کُتُب", subject: "عربی" },
    { q: "معنی فعل «سَمِعَ» چیست؟", opts: ["شنید", "دید", "نوشت", "گفت"], ans: "شنید", subject: "عربی" },
    { q: "معنی «کَیْفَ» چیست؟", opts: ["چگونه", "کجا", "چرا", "کی"], ans: "چگونه", subject: "عربی" },
    { q: "واژه CPU به چه معناست؟", opts: ["واحد پردازش مرکزی", "حافظه موقت", "صفحه‌کلید", "کارت گرافیک"], ans: "واحد پردازش مرکزی", subject: "کامپیوتر" },
    { q: "کدام گزینه یک زبان برنامه‌نویسی است؟", opts: ["Python", "Windows", "Word", "Chrome"], ans: "Python", subject: "کامپیوتر" },
    { q: "کدام برنامه برای کار با صفحه‌گسترده و جدول‌های محاسباتی است؟", opts: ["Excel", "Word", "Paint", "Chrome"], ans: "Excel", subject: "کامپیوتر" },
    { q: "ویروس کامپیوتری چیست؟", opts: ["برنامه مخرب", "قطعه سخت‌افزاری", "نوعی حافظه", "مرورگر"], ans: "برنامه مخرب", subject: "کامپیوتر" },
    { q: "کلید میانبر Ctrl + Z چه کاری انجام می‌دهد؟", opts: ["برگرداندن آخرین عمل", "کپی کردن", "چاپ کردن", "ذخیره کردن"], ans: "برگرداندن آخرین عمل", subject: "کامپیوتر" },
    { q: "معنی کلمه «Important» چیست؟", opts: ["مهم", "ساده", "قدیمی", "ارزان"], ans: "مهم", subject: "انگلیسی" },
    { q: "معنی کلمه «Weather» چیست؟", opts: ["آب‌وهوا", "فصل", "باران", "ابر"], ans: "آب‌وهوا", subject: "انگلیسی" },
    { q: "معنی کلمه «Knowledge» چیست؟", opts: ["دانش", "قدرت", "ثروت", "سلامت"], ans: "دانش", subject: "انگلیسی" },
    { q: "معنی کلمه «Different» چیست؟", opts: ["متفاوت", "مشابه", "مشکل", "مهربان"], ans: "متفاوت", subject: "انگلیسی" },
    { q: "معنی کلمه «Tomorrow» چیست؟", opts: ["فردا", "دیروز", "امروز", "امشب"], ans: "فردا", subject: "انگلیسی" }
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
function render(users){const l=document.getElementById('list');l.textContent='';users.forEach(u=>{const row=document.createElement('div');row.className='row';const s=document.createElement('span');s.textContent=u.name+' — '+u.best+' امتیاز';const i=document.createElement('input');i.type='number';i.min='0';i.placeholder='امتیاز';i.style.width='90px';const sb=document.createElement('button');sb.textContent='ثبت';sb.onclick=()=>setScore(u,i.value);const b=document.createElement('button');b.className='danger';b.textContent='حذف';b.onclick=()=>del(u);row.appendChild(s);row.appendChild(i);row.appendChild(sb);row.appendChild(b);l.appendChild(row)})}
async function setScore(u,v){if(v===''||isNaN(Number(v))){msg('یک عدد وارد کنید.');return}try{await call('/api/admin/set-score',{key:u.key,score:Number(v)});await load();msg('امتیاز «'+u.name+'» روی '+Number(v)+' تنظیم شد.')}catch(e){msg(e.message)}}
async function del(u){if(!confirm('حساب «'+u.name+'» برای همیشه حذف شود؟'))return;try{await call('/api/admin/delete',{key:u.key});await load()}catch(e){msg(e.message)}}
async function resetAll(){if(!confirm('امتیاز همه صفر شود؟ حساب‌ها حذف نمی‌شوند.'))return;try{await call('/api/admin/reset-scores');await load()}catch(e){msg(e.message)}}
</script></body></html>`;

// ---------- بازی آنلاین دو نفره (۳۰ ثانیه، هر جواب درست +۱۰ و هر جواب غلط -۱۰) ----------
const ONLINE_SECONDS = 30;
const ONLINE_COUNTDOWN_MS = 5000;   // شمارش معکوس قبل از شروع
const ONLINE_GRACE_MS = 2000;       // تحمل تأخیر شبکه بعد از پایان ۳۰ ثانیه
const QUEUE_STALE_MS = 7000;        // اگر بازیکنِ منتظر این‌قدر درخواست نفرستد، رفته حساب می‌شود
const onlineQueue = new Map();      // userKey -> { user, seen }
const onlineMatches = new Map();    // matchId -> match
const userMatch = new Map();        // userKey -> matchId

setInterval(() => {
  const now = Date.now();
  for (const [k, e] of onlineQueue) if (now - e.seen > 30e3) onlineQueue.delete(k);
}, 15e3).unref();

function randomOnlineQuestion(avoid) {
  const grades = Object.keys(questionBank);
  let pick;
  for (let i = 0; i < 5; i++) {
    pick = pickQuestion(grades[crypto.randomInt(grades.length)]);
    if (!avoid || pick.q !== avoid.q) break;
  }
  return pick;
}

// هر دو بازیکن دقیقاً همان دنباله‌ی سوال‌ها را می‌بینند تا عادلانه باشد
function onlineQuestionAt(m, idx) {
  while (m.seq.length <= idx) m.seq.push(randomOnlineQuestion(m.seq[m.seq.length - 1]));
  return m.seq[idx];
}

function publicQuestion(m, idx) {
  const it = onlineQuestionAt(m, idx);
  return { n: idx, text: it.q.q, subject: it.q.subject, opts: it.opts };
}

function createMatch(a, b) {
  const id = crypto.randomBytes(8).toString('hex');
  const now = Date.now();
  const m = {
    id, seq: [], finished: false, result: null,
    startAt: now + ONLINE_COUNTDOWN_MS,
    endAt: now + ONLINE_COUNTDOWN_MS + ONLINE_SECONDS * 1000,
    players: {}
  };
  for (const u of [a, b]) m.players[u.key] = { key: u.key, name: u.name, best: u.best || 0, score: 0, idx: 0 };
  onlineMatches.set(id, m);
  userMatch.set(a.key, id); userMatch.set(b.key, id);
  m.timer = setTimeout(() => { finalizeMatch(m).catch((e) => console.error(e)); }, ONLINE_COUNTDOWN_MS + ONLINE_SECONDS * 1000 + ONLINE_GRACE_MS);
  return m;
}

async function finalizeMatch(m) {
  if (m.finished) return;
  m.finished = true;                  // همین‌جا و قبل از هر await تا دوبار حساب نشود
  clearTimeout(m.timer);
  const [p1, p2] = Object.values(m.players);
  const winnerKey = p1.score === p2.score ? null : (p1.score > p2.score ? p1.key : p2.key);
  m.result = { winnerKey };
  setTimeout(() => {                  // نتیجه تا ۵ دقیقه بعد قابل خواندن است
    onlineMatches.delete(m.id);
    for (const k of Object.keys(m.players)) if (userMatch.get(k) === m.id) userMatch.delete(k);
  }, 5 * 60e3).unref();
  if (winnerKey) {
    const u = await getUserCached(winnerKey);
    if (u) { u.wins = await store.addWin(u); wlbCache.at = 0; }
  }
}

async function matchView(m, key) {
  const me = m.players[key];
  const opp = Object.values(m.players).find((p) => p.key !== key);
  const now = Date.now();
  const view = { status: 'match', me: { name: me.name, best: me.best }, opponent: { name: opp.name, best: opp.best }, score: me.score };
  if (m.finished) {
    view.phase = 'finished';
    const w = m.result.winnerKey;
    const mu = await getUserCached(key);
    view.result = { myScore: me.score, oppScore: opp.score, outcome: w === null ? 'draw' : (w === key ? 'win' : 'lose'), myWins: mu ? (mu.wins || 0) : 0 };
  } else if (now < m.startAt) {
    view.phase = 'countdown'; view.startsInMs = m.startAt - now;
  } else if (now < m.endAt) {
    view.phase = 'playing'; view.endsInMs = m.endAt - now; view.question = publicQuestion(m, me.idx);
  } else {
    view.phase = 'waiting';
  }
  return view;
}

async function myActiveMatch(u) {
  const mid = userMatch.get(u.key);
  const m = mid && onlineMatches.get(mid);
  if (!m) { userMatch.delete(u.key); return null; }
  if (!m.finished && Date.now() >= m.endAt + ONLINE_GRACE_MS) await finalizeMatch(m);
  return m;
}

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
      if (u) { games.delete(u.key); onlineQueue.delete(u.key); }
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

  'POST /api/admin/set-score': async (req, res, body) => {
    if (!(await adminGuard(req, res))) return;
    const key = String(body.key || '');
    const score = Number(body.score);
    if (!key) return send(res, 400, { error: 'کلید کاربر خالی است.' });
    if (!Number.isInteger(score) || score < 0 || score > 1000000) return send(res, 400, { error: 'امتیاز باید عدد صحیح بین ۰ تا ۱٬۰۰۰٬۰۰۰ باشد.' });
    const user = await store.getUser(key);
    if (!user) return send(res, 404, { error: 'کاربر پیدا نشد.' });
    await store.setBest(user, score);
    userCache.delete(key); lbCache.at = 0;
    return send(res, 200, { ok: true });
  },

  'POST /api/admin/reset-scores': async (req, res) => {
    if (!(await adminGuard(req, res))) return;
    await store.resetScores();
    userCache.clear(); games.clear(); lbCache.at = 0; wlbCache.at = 0;
    return send(res, 200, { ok: true });
  },

  'POST /api/online/find': async (req, res) => {
    const u = await authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    if (rateLimited('on:' + u.key, 400, 60e3)) return send(res, 429, { error: 'درخواست‌ها زیاد است. کمی صبر کنید.' });

    const cur = await myActiveMatch(u);
    if (cur && !cur.finished) { onlineQueue.delete(u.key); return send(res, 200, await matchView(cur, u.key)); }
    if (cur && cur.finished) userMatch.delete(u.key);       // بازی قبلی تمام شده؛ بازی تازه شروع می‌شود

    const now = Date.now();
    for (const [k, e] of onlineQueue) {
      if (k !== u.key && now - e.seen < QUEUE_STALE_MS) {
        onlineQueue.delete(k); onlineQueue.delete(u.key);
        const m = createMatch(u, e.user);
        return send(res, 200, await matchView(m, u.key));
      }
    }
    onlineQueue.set(u.key, { user: u, seen: now });
    return send(res, 200, { status: 'searching' });
  },

  'POST /api/online/status': async (req, res) => {
    const u = await authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    if (rateLimited('on:' + u.key, 400, 60e3)) return send(res, 429, { error: 'درخواست‌ها زیاد است. کمی صبر کنید.' });
    const m = await myActiveMatch(u);
    if (m) return send(res, 200, await matchView(m, u.key));
    const e = onlineQueue.get(u.key);
    if (e) { e.seen = Date.now(); return send(res, 200, { status: 'searching' }); }
    return send(res, 200, { status: 'none' });
  },

  'POST /api/online/cancel': async (req, res) => {
    const u = await authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    onlineQueue.delete(u.key);
    return send(res, 200, { ok: true });
  },

  'POST /api/online/answer': async (req, res, body) => {
    const u = await authUser(req);
    if (!u) return send(res, 401, { error: 'unauthorized' });
    if (rateLimited('on:' + u.key, 400, 60e3)) return send(res, 429, { error: 'درخواست‌ها زیاد است. کمی صبر کنید.' });
    const m = await myActiveMatch(u);
    if (!m || m.finished) return send(res, 400, { error: 'بازی فعالی وجود ندارد.' });
    const now = Date.now();
    if (now < m.startAt - 300 || now > m.endAt + ONLINE_GRACE_MS) return send(res, 400, { error: 'زمان بازی تمام شده است.' });

    const me = m.players[u.key];
    const n = Number(body.n);
    if (!Number.isInteger(n) || n !== me.idx) {            // درخواست تکراری یا قدیمی: فقط وضعیت فعلی را برگردان
      return send(res, 200, { stale: true, score: me.score, question: publicQuestion(m, me.idx) });
    }
    const cur = onlineQuestionAt(m, me.idx);
    const correct = body.answer === cur.q.ans;
    me.score += correct ? POINTS : -POINTS;
    me.idx++;
    return send(res, 200, { correct, correctAnswer: cur.q.ans, score: me.score, question: publicQuestion(m, me.idx) });
  },

  'GET /api/league': async (req, res) => {
    const me = await authUser(req);
    if (new URL(req.url, 'http://x').searchParams.get('type') === 'wins') {
      if (!wlbCache.data || Date.now() - wlbCache.at > 15e3) {
        wlbCache = { at: Date.now(), data: await store.leaderboardWins(100) };
      }
      let myRank = null;
      if (me && me.wins > 0) myRank = await store.rankOfWins(me);
      return send(res, 200, {
        type: 'wins',
        top: wlbCache.data.map((r, i) => ({ rank: i + 1, name: r.name, score: r.score })),
        me: me ? { name: me.name, score: me.wins || 0, rank: myRank } : null
      });
    }
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
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp', '.svg': 'image/svg+xml', '.json': 'application/json' };
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
