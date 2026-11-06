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
    { q: "معنی کلمه «Red» چیست؟", opts: ["قرمز", "آبی", "سبز", "زرد"], ans: "قرمز", subject: "انگلیسی" },
    { q: "حاصل عبارت 15 ÷ 3 + 2 چند است؟", opts: ["7", "5", "3", "9"], ans: "7", subject: "ریاضی" },
    { q: "کوچک‌ترین مضرب مشترک دو عدد 4 و 6 کدام است؟", opts: ["12", "24", "10", "2"], ans: "12", subject: "ریاضی" },
    { q: "حاصل 0.5 + 0.25 کدام است؟", opts: ["0.75", "0.70", "0.30", "0.55"], ans: "0.75", subject: "ریاضی" },
    { q: "حاصل 7 × 8 چند است؟", opts: ["56", "54", "48", "63"], ans: "56", subject: "ریاضی" },
    { q: "کدام عدد از بقیه بزرگ‌تر است؟", opts: ["-3", "-7", "-10", "-5"], ans: "-3", subject: "ریاضی" },
    { q: "حاصل (-6) + (-4) چند است؟", opts: ["-10", "10", "-2", "2"], ans: "-10", subject: "ریاضی" },
    { q: "25 درصد عدد 80 چند است؟", opts: ["20", "25", "40", "16"], ans: "20", subject: "ریاضی" },
    { q: "شش‌ضلعی چند ضلع دارد؟", opts: ["6", "5", "8", "7"], ans: "6", subject: "ریاضی" },
    { q: "اندازه زاویه قائمه چند درجه است؟", opts: ["90 درجه", "180 درجه", "45 درجه", "60 درجه"], ans: "90 درجه", subject: "ریاضی" },
    { q: "مساحت مربعی به ضلع 6 سانتی‌متر چقدر است؟", opts: ["36", "24", "12", "18"], ans: "36", subject: "ریاضی" },
    { q: "کسر 3/5 به صورت عدد اعشاری کدام است؟", opts: ["0.6", "0.35", "0.5", "0.3"], ans: "0.6", subject: "ریاضی" },
    { q: "کدام عدد اول است؟", opts: ["13", "15", "21", "27"], ans: "13", subject: "ریاضی" },
    { q: "حاصل 2 × 3² چند است؟", opts: ["18", "36", "12", "15"], ans: "18", subject: "ریاضی" },
    { q: "5 دقیقه چند ثانیه است؟", opts: ["300", "500", "60", "360"], ans: "300", subject: "ریاضی" },
    { q: "میانگین سه عدد 4 و 6 و 8 چند است؟", opts: ["6", "5", "7", "18"], ans: "6", subject: "ریاضی" },
    { q: "کدام عدد بر 3 بخش‌پذیر است؟", opts: ["123", "124", "125", "127"], ans: "123", subject: "ریاضی" },
    { q: "قرینه عدد -9 کدام است؟", opts: ["9", "-9", "0", "1/9"], ans: "9", subject: "ریاضی" },
    { q: "هر زاویه مثلث متساوی‌الاضلاع چند درجه است؟", opts: ["60 درجه", "90 درجه", "45 درجه", "30 درجه"], ans: "60 درجه", subject: "ریاضی" },
    { q: "1000 گرم چند کیلوگرم است؟", opts: ["1", "10", "0.1", "100"], ans: "1", subject: "ریاضی" },
    { q: "محیط مستطیلی با طول 10 و عرض 4 چقدر است؟", opts: ["28", "40", "14", "24"], ans: "28", subject: "ریاضی" },
    { q: "متضاد کلمه «روشن» کدام است؟", opts: ["تاریک", "درخشان", "نورانی", "زرد"], ans: "تاریک", subject: "فارسی" },
    { q: "کدام کلمه با «آسمان» هم‌معنی است؟", opts: ["فلک", "زمین", "ستاره", "ابر"], ans: "فلک", subject: "فارسی" },
    { q: "کدام کلمه «اسم» است؟", opts: ["کتاب", "رفت", "زیبا", "خوب"], ans: "کتاب", subject: "فارسی" },
    { q: "مفرد کلمه «دانش‌آموزان» کدام است؟", opts: ["دانش‌آموز", "دانش", "آموز", "دانشمند"], ans: "دانش‌آموز", subject: "فارسی" },
    { q: "کدام کلمه «فعل» است؟", opts: ["نوشت", "مداد", "سبز", "او"], ans: "نوشت", subject: "فارسی" },
    { q: "حافظ اهل کدام شهر بود؟", opts: ["شیراز", "اصفهان", "تبریز", "مشهد"], ans: "شیراز", subject: "فارسی" },
    { q: "نظامی را به کدام شهر نسبت می‌دهند؟", opts: ["گنجه", "شیراز", "طوس", "نیشابور"], ans: "گنجه", subject: "فارسی" },
    { q: "فردوسی اهل کدام شهر بود؟", opts: ["طوس", "شیراز", "نیشابور", "اصفهان"], ans: "طوس", subject: "فارسی" },
    { q: "متضاد کلمه «خوشحال» کدام است؟", opts: ["غمگین", "شاد", "خندان", "سرحال"], ans: "غمگین", subject: "فارسی" },
    { q: "علامت «؟» چه نام دارد؟", opts: ["علامت سؤال", "نقطه", "ویرگول", "علامت تعجب"], ans: "علامت سؤال", subject: "فارسی" },
    { q: "علامت «!» چه نام دارد؟", opts: ["علامت تعجب", "علامت سؤال", "ویرگول", "نقطه ویرگول"], ans: "علامت تعجب", subject: "فارسی" },
    { q: "معنی کلمه «پاسبان» کدام است؟", opts: ["نگهبان", "معلم", "کشاورز", "راننده"], ans: "نگهبان", subject: "فارسی" },
    { q: "مفهوم ضرب‌المثل «با یک گل بهار نمی‌شود» کدام است؟", opts: ["با یک کار کوچک نمی‌توان به نتیجه کلی رسید", "بهار فصل گل‌هاست", "گل‌ها زود پرپر می‌شوند", "باید باغبان بود"], ans: "با یک کار کوچک نمی‌توان به نتیجه کلی رسید", subject: "فارسی" },
    { q: "کتاب «بهارستان» اثر کدام نویسنده است؟", opts: ["جامی", "سعدی", "حافظ", "فردوسی"], ans: "جامی", subject: "فارسی" },
    { q: "متضاد کلمه «دانا» کدام است؟", opts: ["نادان", "عاقل", "هوشمند", "آگاه"], ans: "نادان", subject: "فارسی" },
    { q: "کدام کلمه با «علم» هم‌خانواده است؟", opts: ["معلم", "مسجد", "کتاب", "مدرسه"], ans: "معلم", subject: "فارسی" },
    { q: "الفبای فارسی چند حرف دارد؟", opts: ["32", "28", "30", "33"], ans: "32", subject: "فارسی" },
    { q: "ماه مهر ماه چندم سال شمسی است؟", opts: ["هفتم", "اول", "ششم", "هشتم"], ans: "هفتم", subject: "فارسی" },
    { q: "مصراع «توانا بود هر که دانا بود» از کدام شاعر است؟", opts: ["فردوسی", "سعدی", "حافظ", "مولوی"], ans: "فردوسی", subject: "فارسی" },
    { q: "کدام کلمه «صفت» است؟", opts: ["بزرگ", "میز", "دویدن", "ایران"], ans: "بزرگ", subject: "فارسی" },
    { q: "معنی کلمه «قَلَم» چیست؟", opts: ["قلم", "دفتر", "کیف", "مدرسه"], ans: "قلم", subject: "عربی" },
    { q: "معنی کلمه «باب» چیست؟", opts: ["در", "پنجره", "دیوار", "سقف"], ans: "در", subject: "عربی" },
    { q: "معنی کلمه «شُبّاک» چیست؟", opts: ["پنجره", "در", "میز", "تخته"], ans: "پنجره", subject: "عربی" },
    { q: "معنی کلمه «سَیّارَة» چیست؟", opts: ["ماشین", "هواپیما", "کشتی", "دوچرخه"], ans: "ماشین", subject: "عربی" },
    { q: "معنی کلمه «طَعام» چیست؟", opts: ["غذا", "آب", "لباس", "کتاب"], ans: "غذا", subject: "عربی" },
    { q: "معنی کلمه «لَیل» چیست؟", opts: ["شب", "روز", "صبح", "ظهر"], ans: "شب", subject: "عربی" },
    { q: "معنی کلمه «نَهار» چیست؟", opts: ["روز", "شب", "صبح", "عصر"], ans: "روز", subject: "عربی" },
    { q: "معنی کلمه «أب» چیست؟", opts: ["پدر", "مادر", "برادر", "خواهر"], ans: "پدر", subject: "عربی" },
    { q: "معنی کلمه «أُمّ» چیست؟", opts: ["مادر", "پدر", "عمو", "دایی"], ans: "مادر", subject: "عربی" },
    { q: "معنی کلمه «أخ» چیست؟", opts: ["برادر", "خواهر", "پسر", "دوست"], ans: "برادر", subject: "عربی" },
    { q: "معنی کلمه «أُخت» چیست؟", opts: ["خواهر", "برادر", "دختر", "عمه"], ans: "خواهر", subject: "عربی" },
    { q: "معنی کلمه «صَغیر» چیست؟", opts: ["کوچک", "بزرگ", "زیبا", "زشت"], ans: "کوچک", subject: "عربی" },
    { q: "معنی کلمه «جَمیل» چیست؟", opts: ["زیبا", "زشت", "بزرگ", "کوچک"], ans: "زیبا", subject: "عربی" },
    { q: "معنی حرف «مَعَ» چیست؟", opts: ["با", "در", "از", "به"], ans: "با", subject: "عربی" },
    { q: "معنی حرف «مِن» چیست؟", opts: ["از", "به", "با", "در"], ans: "از", subject: "عربی" },
    { q: "معنی حرف «فی» چیست؟", opts: ["در", "از", "با", "روی"], ans: "در", subject: "عربی" },
    { q: "معنی «هذا» چیست؟", opts: ["این (مذکر)", "آن", "او", "ما"], ans: "این (مذکر)", subject: "عربی" },
    { q: "معنی «ذلِک» چیست؟", opts: ["آن (مذکر)", "این", "ما", "شما"], ans: "آن (مذکر)", subject: "عربی" },
    { q: "معنی فعل «کَتَبَ» چیست؟", opts: ["نوشت", "خواند", "رفت", "شنید"], ans: "نوشت", subject: "عربی" },
    { q: "معنی فعل «قَرَأَ» چیست؟", opts: ["خواند", "نوشت", "رفت", "خورد"], ans: "خواند", subject: "عربی" },
    { q: "کدام وسیله «خروجی» است؟", opts: ["مانیتور", "ماوس", "صفحه‌کلید", "اسکنر"], ans: "مانیتور", subject: "کامپیوتر" },
    { q: "ماوس چه نوع وسیله‌ای است؟", opts: ["ورودی", "خروجی", "حافظه", "پردازنده"], ans: "ورودی", subject: "کامپیوتر" },
    { q: "هارد دیسک برای چه کاری استفاده می‌شود؟", opts: ["ذخیره‌سازی دائمی اطلاعات", "چاپ کردن", "نمایش تصویر", "پخش صدا"], ans: "ذخیره‌سازی دائمی اطلاعات", subject: "کامپیوتر" },
    { q: "پسوند فایل متنی ساده کدام است؟", opts: [".txt", ".mp4", ".jpg", ".exe"], ans: ".txt", subject: "کامپیوتر" },
    { q: "پسوند فایل ویدیویی کدام است؟", opts: [".mp4", ".txt", ".docx", ".png"], ans: ".mp4", subject: "کامپیوتر" },
    { q: "کدام برنامه برای نقاشی در ویندوز است؟", opts: ["Paint", "Word", "Excel", "Chrome"], ans: "Paint", subject: "کامپیوتر" },
    { q: "کدام برنامه یک ویرایشگر متن ساده در ویندوز است؟", opts: ["Notepad", "Paint", "Excel", "Calculator"], ans: "Notepad", subject: "کامپیوتر" },
    { q: "هر مگابایت چند کیلوبایت است؟", opts: ["1024", "1000", "100", "8"], ans: "1024", subject: "کامپیوتر" },
    { q: "کدام واحد از مگابایت بزرگ‌تر است؟", opts: ["گیگابایت", "کیلوبایت", "بایت", "بیت"], ans: "گیگابایت", subject: "کامپیوتر" },
    { q: "کلید میانبر Ctrl + X چه کاری انجام می‌دهد؟", opts: ["برش (Cut)", "کپی", "چسباندن", "چاپ"], ans: "برش (Cut)", subject: "کامپیوتر" },
    { q: "کلید میانبر Ctrl + A چه کاری انجام می‌دهد؟", opts: ["انتخاب همه", "ذخیره", "چاپ", "بستن"], ans: "انتخاب همه", subject: "کامپیوتر" },
    { q: "کلید میانبر Ctrl + S چه کاری انجام می‌دهد؟", opts: ["ذخیره کردن", "چاپ", "کپی", "برش"], ans: "ذخیره کردن", subject: "کامپیوتر" },
    { q: "کلید میانبر Ctrl + P چه کاری انجام می‌دهد؟", opts: ["چاپ", "ذخیره", "کپی", "انتخاب همه"], ans: "چاپ", subject: "کامپیوتر" },
    { q: "کلید Caps Lock چه کاری انجام می‌دهد؟", opts: ["تغییر حروف انگلیسی بین بزرگ و کوچک", "حذف متن", "خاموش کردن کامپیوتر", "باز کردن پوشه"], ans: "تغییر حروف انگلیسی بین بزرگ و کوچک", subject: "کامپیوتر" },
    { q: "Wi-Fi چه کاربردی دارد؟", opts: ["اتصال بی‌سیم به شبکه و اینترنت", "چاپ اسناد", "ذخیره فایل", "ضبط صدا"], ans: "اتصال بی‌سیم به شبکه و اینترنت", subject: "کامپیوتر" },
    { q: "URL به چه معناست؟", opts: ["نشانی یک صفحه در وب", "نوعی ویروس", "نوعی حافظه", "یک برنامه ویرایش عکس"], ans: "نشانی یک صفحه در وب", subject: "کامپیوتر" },
    { q: "کدام گزینه یک موتور جستجو است؟", opts: ["Google", "Word", "Paint", "Excel"], ans: "Google", subject: "کامپیوتر" },
    { q: "کدام رمز عبور امن‌تر است؟", opts: ["ترکیبی از حروف، عدد و نماد", "تاریخ تولد", "123456", "نام خودتان"], ans: "ترکیبی از حروف، عدد و نماد", subject: "کامپیوتر" },
    { q: "ایمیل (Email) چیست؟", opts: ["نامه الکترونیکی", "نوعی بازی", "نوعی سخت‌افزار", "نوعی حافظه"], ans: "نامه الکترونیکی", subject: "کامپیوتر" },
    { q: "کدام گزینه جزو «سخت‌افزار» است؟", opts: ["مادربورد", "Windows", "Word", "Chrome"], ans: "مادربورد", subject: "کامپیوتر" },
    { q: "معنی کلمه «Apple» چیست؟", opts: ["سیب", "پرتقال", "موز", "انگور"], ans: "سیب", subject: "انگلیسی" },
    { q: "معنی کلمه «Pencil» چیست؟", opts: ["مداد", "خودکار", "دفتر", "پاک‌کن"], ans: "مداد", subject: "انگلیسی" },
    { q: "معنی کلمه «Water» چیست؟", opts: ["آب", "شیر", "چای", "نان"], ans: "آب", subject: "انگلیسی" },
    { q: "معنی کلمه «Cat» چیست؟", opts: ["گربه", "سگ", "پرنده", "ماهی"], ans: "گربه", subject: "انگلیسی" },
    { q: "معنی کلمه «Dog» چیست؟", opts: ["سگ", "گربه", "اسب", "گاو"], ans: "سگ", subject: "انگلیسی" },
    { q: "معنی کلمه «Father» چیست؟", opts: ["پدر", "مادر", "برادر", "خواهر"], ans: "پدر", subject: "انگلیسی" },
    { q: "معنی کلمه «Sister» چیست؟", opts: ["خواهر", "برادر", "مادر", "دختر عمو"], ans: "خواهر", subject: "انگلیسی" },
    { q: "معنی کلمه «Blue» چیست؟", opts: ["آبی", "قرمز", "سبز", "زرد"], ans: "آبی", subject: "انگلیسی" },
    { q: "معنی کلمه «Green» چیست؟", opts: ["سبز", "آبی", "قرمز", "سیاه"], ans: "سبز", subject: "انگلیسی" },
    { q: "متضاد کلمه «Big» کدام است؟", opts: ["Small", "Tall", "Long", "Fast"], ans: "Small", subject: "انگلیسی" },
    { q: "متضاد کلمه «Hot» کدام است؟", opts: ["Cold", "Warm", "Cool", "Dry"], ans: "Cold", subject: "انگلیسی" },
    { q: "معنی کلمه «Happy» چیست؟", opts: ["خوشحال", "غمگین", "خسته", "عصبانی"], ans: "خوشحال", subject: "انگلیسی" },
    { q: "معنی کلمه «School» چیست؟", opts: ["مدرسه", "بیمارستان", "خانه", "بازار"], ans: "مدرسه", subject: "انگلیسی" },
    { q: "معنی کلمه «Friend» چیست؟", opts: ["دوست", "دشمن", "همسایه", "معلم"], ans: "دوست", subject: "انگلیسی" },
    { q: "معنی جمله «How are you?» چیست؟", opts: ["حال شما چطور است؟", "اسم شما چیست؟", "کجا می‌روی؟", "چند سال دارید؟"], ans: "حال شما چطور است؟", subject: "انگلیسی" },
    { q: "معنی جمله «What is your name?» چیست؟", opts: ["اسم شما چیست؟", "حال شما چطور است؟", "شما کجا زندگی می‌کنید؟", "چند سال دارید؟"], ans: "اسم شما چیست؟", subject: "انگلیسی" },
    { q: "معنی «Good morning» چیست؟", opts: ["صبح بخیر", "شب بخیر", "خداحافظ", "ممنون"], ans: "صبح بخیر", subject: "انگلیسی" },
    { q: "شکل جمع کلمه «book» کدام است؟", opts: ["books", "bookes", "bookies", "bookis"], ans: "books", subject: "انگلیسی" },
    { q: "جمله را کامل کنید: I ___ a student.", opts: ["am", "is", "are", "be"], ans: "am", subject: "انگلیسی" },
    { q: "جمله را کامل کنید: She ___ my sister.", opts: ["is", "am", "are", "be"], ans: "is", subject: "انگلیسی" }
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
    { q: "معنی کلمه «Healthy» چیست؟", opts: ["سالم", "بیمار", "ضعیف", "خسته"], ans: "سالم", subject: "انگلیسی" },
    { q: "حاصل (-2)³ چند است؟", opts: ["-8", "8", "-6", "6"], ans: "-8", subject: "ریاضی" },
    { q: "حاصل 3/4 × 2/3 کدام است؟", opts: ["1/2", "5/7", "6/7", "1/3"], ans: "1/2", subject: "ریاضی" },
    { q: "حاصل 2/3 ÷ 4/9 کدام است؟", opts: ["3/2", "8/27", "2/3", "1/2"], ans: "3/2", subject: "ریاضی" },
    { q: "مجموع زاویه‌های داخلی هر پنج‌ضلعی چند درجه است؟", opts: ["540 درجه", "360 درجه", "720 درجه", "900 درجه"], ans: "540 درجه", subject: "ریاضی" },
    { q: "مجموع زاویه‌های داخلی هر شش‌ضلعی چند درجه است؟", opts: ["720 درجه", "540 درجه", "360 درجه", "900 درجه"], ans: "720 درجه", subject: "ریاضی" },
    { q: "مساحت مثلثی با قاعده 10 و ارتفاع 6 چقدر است؟", opts: ["30", "60", "16", "15"], ans: "30", subject: "ریاضی" },
    { q: "مساحت دایره‌ای به شعاع 3 چقدر است؟", opts: ["9π", "6π", "3π", "18π"], ans: "9π", subject: "ریاضی" },
    { q: "محیط دایره‌ای به شعاع 5 چقدر است؟", opts: ["10π", "5π", "25π", "20π"], ans: "10π", subject: "ریاضی" },
    { q: "اگر 2x + 3 = 11 باشد، x چند است؟", opts: ["4", "7", "5", "8"], ans: "4", subject: "ریاضی" },
    { q: "حاصل (a + b)(a - b) کدام است؟", opts: ["a² - b²", "a² + b²", "a² - 2ab + b²", "2a - 2b"], ans: "a² - b²", subject: "ریاضی" },
    { q: "حاصل 10³ چند است؟", opts: ["1000", "100", "30", "10000"], ans: "1000", subject: "ریاضی" },
    { q: "عدد 0.0005 در نماد علمی کدام است؟", opts: ["5 × 10⁻⁴", "5 × 10⁻³", "5 × 10⁴", "0.5 × 10⁻³"], ans: "5 × 10⁻⁴", subject: "ریاضی" },
    { q: "20 درصد عدد 150 چند است؟", opts: ["30", "20", "15", "300"], ans: "30", subject: "ریاضی" },
    { q: "قیمت کالایی 200 تومان است. با 10 درصد تخفیف قیمت جدید چند تومان است؟", opts: ["180", "190", "20", "210"], ans: "180", subject: "ریاضی" },
    { q: "میانه اعداد 3، 7، 5، 9، 1 کدام است؟", opts: ["5", "7", "3", "9"], ans: "5", subject: "ریاضی" },
    { q: "مکمل زاویه 30 درجه چند درجه است؟", opts: ["150 درجه", "60 درجه", "120 درجه", "330 درجه"], ans: "150 درجه", subject: "ریاضی" },
    { q: "متمم زاویه 30 درجه چند درجه است؟", opts: ["60 درجه", "150 درجه", "120 درجه", "90 درجه"], ans: "60 درجه", subject: "ریاضی" },
    { q: "کوچک‌ترین مضرب مشترک دو عدد 6 و 8 کدام است؟", opts: ["24", "48", "12", "14"], ans: "24", subject: "ریاضی" },
    { q: "وقتی دو خط موازی را یک خط مورب قطع کند، زاویه‌های متبادل چه رابطه‌ای دارند؟", opts: ["برابرند", "مکمل‌اند", "متمم‌اند", "هر کدام 90 درجه‌اند"], ans: "برابرند", subject: "ریاضی" },
    { q: "حاصل √144 چند است؟", opts: ["12", "14", "11", "72"], ans: "12", subject: "ریاضی" },
    { q: "«خمسه» اثر کدام شاعر است؟", opts: ["نظامی گنجه‌ای", "سعدی", "مولوی", "فردوسی"], ans: "نظامی گنجه‌ای", subject: "فارسی" },
    { q: "لقب «لسان‌الغیب» برای کدام شاعر است؟", opts: ["حافظ", "سعدی", "مولوی", "فردوسی"], ans: "حافظ", subject: "فارسی" },
    { q: "کدام شاعر به «استاد سخن» مشهور است؟", opts: ["سعدی", "حافظ", "خیام", "نظامی"], ans: "سعدی", subject: "فارسی" },
    { q: "خیام اهل کدام شهر بود؟", opts: ["نیشابور", "شیراز", "طوس", "اصفهان"], ans: "نیشابور", subject: "فارسی" },
    { q: "داستان «رستم و سهراب» از کدام کتاب است؟", opts: ["شاهنامه", "گلستان", "مثنوی", "بوستان"], ans: "شاهنامه", subject: "فارسی" },
    { q: "کدام گزینه تعریف «استعاره» است؟", opts: ["به‌کار بردن کلمه‌ای به جای کلمه دیگر به علاقه شباهت", "آوردن دو کلمه متضاد", "تکرار یک صامت", "اشاره به داستان یا آیه"], ans: "به‌کار بردن کلمه‌ای به جای کلمه دیگر به علاقه شباهت", subject: "فارسی" },
    { q: "عبارت «روز و شب» کدام آرایه را دارد؟", opts: ["تضاد", "تشبیه", "جناس", "سجع"], ans: "تضاد", subject: "فارسی" },
    { q: "تکرار یک صامت یا مصوت در شعر یا نثر چه نام دارد؟", opts: ["واج‌آرایی", "تضاد", "تلمیح", "اغراق"], ans: "واج‌آرایی", subject: "فارسی" },
    { q: "کنایه «سر به هوا بودن» به چه معناست؟", opts: ["بی‌توجه و حواس‌پرت بودن", "به آسمان نگاه کردن", "بلندقد بودن", "تنبل بودن"], ans: "بی‌توجه و حواس‌پرت بودن", subject: "فارسی" },
    { q: "کدام فعل «ماضی» است؟", opts: ["رفت", "می‌رود", "خواهد رفت", "برو"], ans: "رفت", subject: "فارسی" },
    { q: "کدام فعل «مضارع» است؟", opts: ["می‌نویسد", "نوشت", "خواهد نوشت", "نوشته بود"], ans: "می‌نویسد", subject: "فارسی" },
    { q: "کدام فعل «امر» است؟", opts: ["بنویس", "می‌نویسد", "نوشت", "نخواهد نوشت"], ans: "بنویس", subject: "فارسی" },
    { q: "کدام کلمه «حرف اضافه» است؟", opts: ["از", "کتاب", "زیبا", "رفت"], ans: "از", subject: "فارسی" },
    { q: "کدام کلمه «ضمیر شخصی» است؟", opts: ["ما", "کتاب", "بسیار", "اما"], ans: "ما", subject: "فارسی" },
    { q: "در جمله «علی مداد را خرید» مفعول کدام است؟", opts: ["مداد", "علی", "خرید", "را"], ans: "مداد", subject: "فارسی" },
    { q: "کتاب «گلستان» سعدی چند باب دارد؟", opts: ["هشت", "ده", "شش", "هفت"], ans: "هشت", subject: "فارسی" },
    { q: "«تاریخ بیهقی» نوشته کیست؟", opts: ["ابوالفضل بیهقی", "سعدی", "نظامی", "جامی"], ans: "ابوالفضل بیهقی", subject: "فارسی" },
    { q: "کتاب «کلیله و دمنه» را کدام نویسنده به نثر فارسی درآورد؟", opts: ["ابوالمعالی نصرالله منشی", "سعدی", "فردوسی", "مولوی"], ans: "ابوالمعالی نصرالله منشی", subject: "فارسی" },
    { q: "«سیمرغ» از شخصیت‌های مهم کدام اثر است؟", opts: ["منطق‌الطیر", "گلستان", "بوستان", "رباعیات خیام"], ans: "منطق‌الطیر", subject: "فارسی" },
    { q: "«هشت کتاب» مجموعه آثار کدام شاعر است؟", opts: ["سهراب سپهری", "نیما یوشیج", "فروغ فرخزاد", "احمد شاملو"], ans: "سهراب سپهری", subject: "فارسی" },
    { q: "معنی کلمه «مُدَرِّس» چیست؟", opts: ["معلم", "دانش‌آموز", "پزشک", "مهندس"], ans: "معلم", subject: "عربی" },
    { q: "معنی کلمه «طالِب» چیست؟", opts: ["دانش‌آموز", "معلم", "پزشک", "راننده"], ans: "دانش‌آموز", subject: "عربی" },
    { q: "معنی کلمه «مَکْتَبَة» چیست؟", opts: ["کتابخانه", "بیمارستان", "باغ", "مسجد"], ans: "کتابخانه", subject: "عربی" },
    { q: "معنی کلمه «مَطْعَم» چیست؟", opts: ["رستوران", "مدرسه", "کتابخانه", "فرودگاه"], ans: "رستوران", subject: "عربی" },
    { q: "معنی کلمه «مَدینَة» چیست؟", opts: ["شهر", "روستا", "کوچه", "کشور"], ans: "شهر", subject: "عربی" },
    { q: "معنی کلمه «قَرْیَة» چیست؟", opts: ["روستا", "شهر", "کوچه", "بازار"], ans: "روستا", subject: "عربی" },
    { q: "معنی کلمه «بَحْر» چیست؟", opts: ["دریا", "رود", "کوه", "جنگل"], ans: "دریا", subject: "عربی" },
    { q: "معنی کلمه «جَبَل» چیست؟", opts: ["کوه", "دریا", "دشت", "رود"], ans: "کوه", subject: "عربی" },
    { q: "معنی کلمه «نَهْر» چیست؟", opts: ["رود", "دریا", "کوه", "دشت"], ans: "رود", subject: "عربی" },
    { q: "معنی کلمه «سَماء» چیست؟", opts: ["آسمان", "زمین", "ستاره", "ماه"], ans: "آسمان", subject: "عربی" },
    { q: "معنی کلمه «أَرْض» چیست؟", opts: ["زمین", "آسمان", "دریا", "کوه"], ans: "زمین", subject: "عربی" },
    { q: "معنی کلمه «قَمَر» چیست؟", opts: ["ماه", "خورشید", "ستاره", "آسمان"], ans: "ماه", subject: "عربی" },
    { q: "معنی کلمه «نَجْم» چیست؟", opts: ["ستاره", "ماه", "خورشید", "زمین"], ans: "ستاره", subject: "عربی" },
    { q: "معنی کلمه «جَدید» چیست؟", opts: ["نو", "کهنه", "زیبا", "بزرگ"], ans: "نو", subject: "عربی" },
    { q: "معنی کلمه «قَدیم» چیست؟", opts: ["قدیمی", "نو", "زیبا", "کوچک"], ans: "قدیمی", subject: "عربی" },
    { q: "معنی کلمه «سَهْل» چیست؟", opts: ["آسان", "سخت", "دور", "نزدیک"], ans: "آسان", subject: "عربی" },
    { q: "معنی کلمه «صَعْب» چیست؟", opts: ["دشوار", "آسان", "زیبا", "کوچک"], ans: "دشوار", subject: "عربی" },
    { q: "معنی فعل «یَکْتُبُ» چیست؟", opts: ["می‌نویسد", "می‌خواند", "می‌رود", "می‌خورد"], ans: "می‌نویسد", subject: "عربی" },
    { q: "معنی فعل «یَذْهَبُ» چیست؟", opts: ["می‌رود", "می‌آید", "می‌نشیند", "می‌خوابد"], ans: "می‌رود", subject: "عربی" },
    { q: "معنی فعل «یَدْرُسُ» چیست؟", opts: ["درس می‌خواند", "می‌خوابد", "می‌خورد", "می‌رود"], ans: "درس می‌خواند", subject: "عربی" },
    { q: "«شبکه» در کامپیوتر به چه معناست؟", opts: ["اتصال چند دستگاه به یکدیگر برای تبادل اطلاعات", "یک نوع حافظه", "یک برنامه نقاشی", "یک نوع چاپگر"], ans: "اتصال چند دستگاه به یکدیگر برای تبادل اطلاعات", subject: "کامپیوتر" },
    { q: "LAN به چه نوع شبکه‌ای می‌گویند؟", opts: ["شبکه محلی", "شبکه جهانی", "شبکه تلفن همراه", "شبکه ماهواره‌ای"], ans: "شبکه محلی", subject: "کامپیوتر" },
    { q: "نشانی IP چه کاربردی دارد؟", opts: ["شناسایی یکتای دستگاه در شبکه", "ذخیره فایل", "چاپ سند", "ضبط صدا"], ans: "شناسایی یکتای دستگاه در شبکه", subject: "کامپیوتر" },
    { q: "ISP به چه معناست؟", opts: ["ارائه‌دهنده خدمات اینترنت", "نوعی ویروس", "نوعی پردازنده", "نوعی سیستم‌عامل"], ans: "ارائه‌دهنده خدمات اینترنت", subject: "کامپیوتر" },
    { q: "فایروال (Firewall) چه کاری انجام می‌دهد؟", opts: ["ترافیک شبکه را کنترل و از دستگاه محافظت می‌کند", "عکس ویرایش می‌کند", "فایل‌ها را چاپ می‌کند", "سرعت پردازنده را بالا می‌برد"], ans: "ترافیک شبکه را کنترل و از دستگاه محافظت می‌کند", subject: "کامپیوتر" },
    { q: "هدف اصلی نرم‌افزار آنتی‌ویروس چیست؟", opts: ["شناسایی و حذف نرم‌افزارهای مخرب", "ویرایش ویدیو", "ایجاد جدول", "نمایش صفحات وب"], ans: "شناسایی و حذف نرم‌افزارهای مخرب", subject: "کامپیوتر" },
    { q: "بلوتوث (Bluetooth) چه نوع ارتباطی است؟", opts: ["بی‌سیم و کوتاه‌برد", "سیمی و دوربرد", "فقط ماهواره‌ای", "فقط نوری"], ans: "بی‌سیم و کوتاه‌برد", subject: "کامپیوتر" },
    { q: "فایل با پسوند zip چه نوع فایلی است؟", opts: ["فایل فشرده", "فایل صوتی", "فایل تصویری", "فایل متنی ساده"], ans: "فایل فشرده", subject: "کامپیوتر" },
    { q: "ذخیره‌سازی ابری (Cloud) یعنی چه؟", opts: ["ذخیره فایل‌ها روی سرورهای اینترنتی", "ذخیره روی کاغذ", "ذخیره فقط در RAM", "ذخیره روی مانیتور"], ans: "ذخیره فایل‌ها روی سرورهای اینترنتی", subject: "کامپیوتر" },
    { q: "پسوند pdf مربوط به چه نوع فایلی است؟", opts: ["سند قابل حمل", "فایل صوتی", "فایل ویدیویی", "برنامه اجرایی"], ans: "سند قابل حمل", subject: "کامپیوتر" },
    { q: "پیکسل (Pixel) چیست؟", opts: ["کوچک‌ترین جزء تصویر دیجیتال", "نوعی حافظه", "نوعی کابل", "نوعی کیبورد"], ans: "کوچک‌ترین جزء تصویر دیجیتال", subject: "کامپیوتر" },
    { q: "عدد 5 در سیستم دودویی (باینری) کدام است؟", opts: ["101", "110", "111", "100"], ans: "101", subject: "کامپیوتر" },
    { q: "در نرم‌افزار PowerPoint به هر صفحه ارائه چه می‌گویند؟", opts: ["اسلاید", "سلول", "برگه", "پوشه"], ans: "اسلاید", subject: "کامپیوتر" },
    { q: "در Excel به هر خانه جدول چه می‌گویند؟", opts: ["سلول (Cell)", "اسلاید", "پاراگراف", "پوشه"], ans: "سلول (Cell)", subject: "کامپیوتر" },
    { q: "تابع SUM در Excel چه کاری انجام می‌دهد؟", opts: ["جمع کردن مقادیر", "میانگین گرفتن", "شمردن حروف", "حذف سلول"], ans: "جمع کردن مقادیر", subject: "کامپیوتر" },
    { q: "الگوریتم یعنی چه؟", opts: ["دنباله‌ای مرحله‌به‌مرحله برای حل یک مسئله", "نوعی سخت‌افزار", "نوعی ویروس", "نوعی مرورگر"], ans: "دنباله‌ای مرحله‌به‌مرحله برای حل یک مسئله", subject: "کامپیوتر" },
    { q: "نمودار گردشی (فلوچارت) برای چه استفاده می‌شود؟", opts: ["نمایش مراحل یک الگوریتم با شکل", "ویرایش عکس", "ذخیره‌سازی فایل", "چاپ سند"], ans: "نمایش مراحل یک الگوریتم با شکل", subject: "کامپیوتر" },
    { q: "اسکرچ (Scratch) چه نوع ابزاری است؟", opts: ["محیط برنامه‌نویسی بصری", "مرورگر وب", "نرم‌افزار حسابداری", "سیستم‌عامل"], ans: "محیط برنامه‌نویسی بصری", subject: "کامپیوتر" },
    { q: "کلید میانبر Ctrl + F معمولاً برای چه کاری است؟", opts: ["جستجو در متن", "ذخیره", "چاپ", "بستن برنامه"], ans: "جستجو در متن", subject: "کامپیوتر" },
    { q: "سیستم عددی دودویی از چه ارقامی استفاده می‌کند؟", opts: ["فقط 0 و 1", "0 تا 9", "1 تا 8", "0 تا 7"], ans: "فقط 0 و 1", subject: "کامپیوتر" },
    { q: "گذشته فعل «go» کدام است؟", opts: ["went", "goed", "gone", "going"], ans: "went", subject: "انگلیسی" },
    { q: "گذشته فعل «eat» کدام است؟", opts: ["ate", "eated", "eaten", "eating"], ans: "ate", subject: "انگلیسی" },
    { q: "گذشته فعل «write» کدام است؟", opts: ["wrote", "writed", "written", "writing"], ans: "wrote", subject: "انگلیسی" },
    { q: "گذشته فعل «see» کدام است؟", opts: ["saw", "seen", "seed", "sawed"], ans: "saw", subject: "انگلیسی" },
    { q: "شکل جمع کلمه «child» کدام است؟", opts: ["children", "childs", "childes", "childrens"], ans: "children", subject: "انگلیسی" },
    { q: "شکل جمع کلمه «man» کدام است؟", opts: ["men", "mans", "manes", "mens"], ans: "men", subject: "انگلیسی" },
    { q: "معنی کلمه «Quickly» چیست؟", opts: ["به سرعت", "به آرامی", "با دقت", "با صدای بلند"], ans: "به سرعت", subject: "انگلیسی" },
    { q: "معنی کلمه «Expensive» چیست؟", opts: ["گران", "ارزان", "قدیمی", "سریع"], ans: "گران", subject: "انگلیسی" },
    { q: "معنی کلمه «Cheap» چیست؟", opts: ["ارزان", "گران", "قدیمی", "تازه"], ans: "ارزان", subject: "انگلیسی" },
    { q: "معنی کلمه «Dangerous» چیست؟", opts: ["خطرناک", "امن", "مهم", "ساده"], ans: "خطرناک", subject: "انگلیسی" },
    { q: "جمله را کامل کنید: I ___ at home yesterday.", opts: ["was", "were", "am", "is"], ans: "was", subject: "انگلیسی" },
    { q: "معنی کلمه «Sometimes» چیست؟", opts: ["گاهی", "همیشه", "هرگز", "هر روز"], ans: "گاهی", subject: "انگلیسی" },
    { q: "جمله را کامل کنید: She ___ to school every day.", opts: ["goes", "go", "going", "gone"], ans: "goes", subject: "انگلیسی" },
    { q: "جمله را کامل کنید: They ___ playing football now.", opts: ["are", "is", "am", "be"], ans: "are", subject: "انگلیسی" },
    { q: "جمله را کامل کنید: There ___ a book on the table.", opts: ["is", "are", "am", "were"], ans: "is", subject: "انگلیسی" },
    { q: "جمله را کامل کنید: There ___ three chairs in the room.", opts: ["are", "is", "am", "be"], ans: "are", subject: "انگلیسی" },
    { q: "متضاد کلمه «easy» کدام است؟", opts: ["difficult", "simple", "small", "soft"], ans: "difficult", subject: "انگلیسی" },
    { q: "متضاد کلمه «tall» کدام است؟", opts: ["short", "long", "big", "fat"], ans: "short", subject: "انگلیسی" },
    { q: "معنی کلمه «Doctor» چیست؟", opts: ["پزشک", "معلم", "راننده", "نانوا"], ans: "پزشک", subject: "انگلیسی" },
    { q: "معنی کلمه «Hospital» چیست؟", opts: ["بیمارستان", "مدرسه", "فرودگاه", "کتابخانه"], ans: "بیمارستان", subject: "انگلیسی" }
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
    { q: "معنی کلمه «Tomorrow» چیست؟", opts: ["فردا", "دیروز", "امروز", "امشب"], ans: "فردا", subject: "انگلیسی" },
    { q: "ساده‌شده √50 کدام است؟", opts: ["5√2", "2√5", "25√2", "10√2"], ans: "5√2", subject: "ریاضی" },
    { q: "حاصل (x + 3)² کدام است؟", opts: ["x² + 6x + 9", "x² + 9", "x² + 3x + 9", "x² + 6x + 6"], ans: "x² + 6x + 9", subject: "ریاضی" },
    { q: "تجزیه عبارت x² - 9 کدام است؟", opts: ["(x - 3)(x + 3)", "(x - 9)(x + 1)", "(x - 3)²", "(x + 3)²"], ans: "(x - 3)(x + 3)", subject: "ریاضی" },
    { q: "ریشه‌های معادله x² - 5x + 6 = 0 کدام‌اند؟", opts: ["2 و 3", "-2 و -3", "1 و 6", "-1 و -6"], ans: "2 و 3", subject: "ریاضی" },
    { q: "جواب‌های معادله x² = 49 کدام‌اند؟", opts: ["7 و -7", "فقط 7", "فقط -7", "49"], ans: "7 و -7", subject: "ریاضی" },
    { q: "در مثلث قائم‌الزاویه با ضلع‌های 6 و 8، وتر چقدر است؟", opts: ["10", "12", "14", "9"], ans: "10", subject: "ریاضی" },
    { q: "در مثلث قائم‌الزاویه وتر 13 و یک ضلع 5 است. ضلع دیگر چقدر است؟", opts: ["12", "8", "10", "14"], ans: "12", subject: "ریاضی" },
    { q: "مساحت ذوزنقه‌ای با قاعده‌های 6 و 10 و ارتفاع 4 چقدر است؟", opts: ["32", "64", "24", "40"], ans: "32", subject: "ریاضی" },
    { q: "حجم مکعبی به ضلع 3 چقدر است؟", opts: ["27", "9", "18", "81"], ans: "27", subject: "ریاضی" },
    { q: "حجم استوانه‌ای به شعاع 2 و ارتفاع 5 چقدر است؟", opts: ["20π", "10π", "40π", "4π"], ans: "20π", subject: "ریاضی" },
    { q: "شیب خطی که از نقاط (0,0) و (2,4) می‌گذرد چند است؟", opts: ["2", "4", "1/2", "8"], ans: "2", subject: "ریاضی" },
    { q: "عرض از مبدأ خط y = 3x - 1 چند است؟", opts: ["-1", "3", "1", "0"], ans: "-1", subject: "ریاضی" },
    { q: "احتمال آمدن عدد زوج در پرتاب یک تاس سالم چقدر است؟", opts: ["1/2", "1/3", "1/6", "2/3"], ans: "1/2", subject: "ریاضی" },
    { q: "احتمال آمدن عدد 6 در پرتاب یک تاس سالم چقدر است؟", opts: ["1/6", "1/3", "1/2", "1"], ans: "1/6", subject: "ریاضی" },
    { q: "حاصل 2⁻¹ کدام است؟", opts: ["1/2", "-2", "2", "-1/2"], ans: "1/2", subject: "ریاضی" },
    { q: "حاصل 2⁵ ÷ 2³ چند است؟", opts: ["4", "2", "8", "16"], ans: "4", subject: "ریاضی" },
    { q: "عدد 45000 در نماد علمی کدام است؟", opts: ["4.5 × 10⁴", "4.5 × 10³", "45 × 10⁴", "0.45 × 10³"], ans: "4.5 × 10⁴", subject: "ریاضی" },
    { q: "زاویه‌های متقابل به رأس چه رابطه‌ای دارند؟", opts: ["برابرند", "مکمل‌اند", "متمم‌اند", "هر کدام 90 درجه‌اند"], ans: "برابرند", subject: "ریاضی" },
    { q: "زاویه محاطی چه نسبتی با کمان روبه‌رویش دارد؟", opts: ["نصف کمان روبه‌رو", "برابر کمان", "دو برابر کمان", "چهار برابر کمان"], ans: "نصف کمان روبه‌رو", subject: "ریاضی" },
    { q: "جمله بعدی دنباله 2، 5، 8، 11 کدام است؟", opts: ["14", "13", "15", "16"], ans: "14", subject: "ریاضی" },
    { q: "کتاب «گلشن راز» اثر کدام شاعر است؟", opts: ["محمود شبستری", "سعدی", "حافظ", "نظامی"], ans: "محمود شبستری", subject: "فارسی" },
    { q: "رمان «بوف کور» اثر کیست؟", opts: ["صادق هدایت", "جلال آل‌احمد", "سیمین دانشور", "صادق چوبک"], ans: "صادق هدایت", subject: "فارسی" },
    { q: "رمان «سووشون» اثر کیست؟", opts: ["سیمین دانشور", "صادق هدایت", "جلال آل‌احمد", "فروغ فرخزاد"], ans: "سیمین دانشور", subject: "فارسی" },
    { q: "کتاب «غرب‌زدگی» اثر کیست؟", opts: ["جلال آل‌احمد", "صادق هدایت", "سیمین دانشور", "نیما یوشیج"], ans: "جلال آل‌احمد", subject: "فارسی" },
    { q: "مجموعه شعر «تولدی دیگر» اثر کدام شاعر است؟", opts: ["فروغ فرخزاد", "سهراب سپهری", "احمد شاملو", "نیما یوشیج"], ans: "فروغ فرخزاد", subject: "فارسی" },
    { q: "کدام گزینه تعریف «حسن تعلیل» است؟", opts: ["آوردن دلیلی شاعرانه و غیرواقعی برای یک پدیده", "تکرار یک واج", "آوردن دو کلمه متضاد", "اشاره به یک داستان"], ans: "آوردن دلیلی شاعرانه و غیرواقعی برای یک پدیده", subject: "فارسی" },
    { q: "آرایه «ایهام» یعنی چه؟", opts: ["کلمه‌ای که دو معنی دارد و هر دو در نظر است", "تکرار یک صامت", "بزرگ‌نمایی", "شباهت دو چیز"], ans: "کلمه‌ای که دو معنی دارد و هر دو در نظر است", subject: "فارسی" },
    { q: "«مراعات نظیر» یعنی چه؟", opts: ["آوردن کلمات متناسب و هم‌خانواده از نظر معنا", "آوردن دو کلمه متضاد", "جان‌بخشی به اشیا", "تکرار یک واج"], ans: "آوردن کلمات متناسب و هم‌خانواده از نظر معنا", subject: "فارسی" },
    { q: "«اغراق» یعنی چه؟", opts: ["بیش از حد بزرگ یا کوچک نشان دادن چیزی", "شباهت دو چیز", "تکرار یک واج", "اشاره به آیه"], ans: "بیش از حد بزرگ یا کوچک نشان دادن چیزی", subject: "فارسی" },
    { q: "«تلمیح» یعنی چه؟", opts: ["اشاره به داستان، آیه، حدیث یا رویدادی معروف", "بزرگ‌نمایی", "آوردن دو کلمه متضاد", "جان‌بخشی"], ans: "اشاره به داستان، آیه، حدیث یا رویدادی معروف", subject: "فارسی" },
    { q: "در جمله «هوا سرد است» مسند کدام است؟", opts: ["سرد", "هوا", "است", "هوا سرد"], ans: "سرد", subject: "فارسی" },
    { q: "در کلمه «نادان» پیشوند کدام است؟", opts: ["نا", "دان", "ن", "ادان"], ans: "نا", subject: "فارسی" },
    { q: "در کلمه «دانشمند» پسوند کدام است؟", opts: ["مند", "دانش", "ش", "دان"], ans: "مند", subject: "فارسی" },
    { q: "کدام گزینه «مصدر» است؟", opts: ["رفتن", "رفت", "می‌رود", "برو"], ans: "رفتن", subject: "فارسی" },
    { q: "مفهوم مصراع «دو صد گفته چون نیم کردار نیست» چیست؟", opts: ["عمل از حرف مهم‌تر است", "زیاد حرف بزن", "کم‌گویی بد است", "سخن همیشه بهتر از کار است"], ans: "عمل از حرف مهم‌تر است", subject: "فارسی" },
    { q: "«حیدربابایه سلام» از کدام شاعر است؟", opts: ["شهریار", "نیما یوشیج", "سهراب سپهری", "پروین اعتصامی"], ans: "شهریار", subject: "فارسی" },
    { q: "لقب «ملک‌الشعرا» برای کدام شاعر معروف است؟", opts: ["محمدتقی بهار", "شهریار", "نیما یوشیج", "سعدی"], ans: "محمدتقی بهار", subject: "فارسی" },
    { q: "کدام یک از شاعران زیر زن است؟", opts: ["پروین اعتصامی", "نیما یوشیج", "شهریار", "بهار"], ans: "پروین اعتصامی", subject: "فارسی" },
    { q: "«رودکی» به کدام لقب مشهور است؟", opts: ["پدر شعر فارسی", "لسان‌الغیب", "استاد سخن", "حکیم توس"], ans: "پدر شعر فارسی", subject: "فارسی" },
    { q: "کتاب «کیمیای سعادت» اثر کیست؟", opts: ["امام محمد غزالی", "سعدی", "مولوی", "نظامی"], ans: "امام محمد غزالی", subject: "فارسی" },
    { q: "کدام کلمه «فعل» است؟", opts: ["کَتَبَ", "کِتاب", "مَدْرَسَة", "قَلَم"], ans: "کَتَبَ", subject: "عربی" },
    { q: "ترجمه «ذَهَبَ الوَلَدُ إِلَی المَدْرَسَةِ» کدام است؟", opts: ["پسر به مدرسه رفت", "پسر از مدرسه آمد", "پسر در مدرسه نشست", "پسر مدرسه را دید"], ans: "پسر به مدرسه رفت", subject: "عربی" },
    { q: "معنی «لا تَذْهَبْ» چیست؟", opts: ["نرو", "برو", "می‌روی", "رفت"], ans: "نرو", subject: "عربی" },
    { q: "معنی «هَلْ» چیست؟", opts: ["آیا", "کجا", "چرا", "چگونه"], ans: "آیا", subject: "عربی" },
    { q: "معنی «لِماذا» چیست؟", opts: ["چرا", "کجا", "کی", "چگونه"], ans: "چرا", subject: "عربی" },
    { q: "معنی «مَنْ» چیست؟", opts: ["چه کسی", "چه چیزی", "کجا", "کی"], ans: "چه کسی", subject: "عربی" },
    { q: "معنی «مَتی» چیست؟", opts: ["چه زمانی", "کجا", "چه کسی", "چند"], ans: "چه زمانی", subject: "عربی" },
    { q: "معنی «کَمْ» چیست؟", opts: ["چند", "کجا", "چه کسی", "چگونه"], ans: "چند", subject: "عربی" },
    { q: "معنی «عِنْدی کِتابٌ» چیست؟", opts: ["من کتابی دارم", "من کتابی خریدم", "کتاب من گم شد", "کتاب را بخوان"], ans: "من کتابی دارم", subject: "عربی" },
    { q: "معنی ضمیر «أنا» چیست؟", opts: ["من", "تو", "او", "ما"], ans: "من", subject: "عربی" },
    { q: "معنی ضمیر «نَحْنُ» چیست؟", opts: ["ما", "من", "شما", "آنها"], ans: "ما", subject: "عربی" },
    { q: "معنی ضمیر «أنتُمْ» چیست؟", opts: ["شما (مذکر جمع)", "ما", "آنها", "او"], ans: "شما (مذکر جمع)", subject: "عربی" },
    { q: "معنی ضمیر «هُمْ» چیست؟", opts: ["آنها (مذکر)", "ما", "شما", "او"], ans: "آنها (مذکر)", subject: "عربی" },
    { q: "معنی فعل امر «اِقْرَأْ» چیست؟", opts: ["بخوان", "بنویس", "برو", "بنشین"], ans: "بخوان", subject: "عربی" },
    { q: "معنی فعل امر «اُکْتُبْ» چیست؟", opts: ["بنویس", "بخوان", "برو", "بخور"], ans: "بنویس", subject: "عربی" },
    { q: "معنی فعل «أَکْتُبُ» چیست؟", opts: ["می‌نویسم", "می‌نویسی", "می‌نویسد", "نوشت"], ans: "می‌نویسم", subject: "عربی" },
    { q: "جمع مکسر کلمه «وَلَد» کدام است؟", opts: ["أَوْلاد", "وَلَدان", "وَلَدُون", "وَلَدات"], ans: "أَوْلاد", subject: "عربی" },
    { q: "مفرد کلمه «مُسْلِمُونَ» کدام است؟", opts: ["مُسْلِم", "مُسْلِمَة", "مُسْلِمان", "إِسْلام"], ans: "مُسْلِم", subject: "عربی" },
    { q: "مثنای کلمه «کِتاب» کدام است؟", opts: ["کِتابان", "کِتابون", "کُتُب", "کَتَبَ"], ans: "کِتابان", subject: "عربی" },
    { q: "معنی کلمه «الیَوْم» چیست؟", opts: ["امروز", "دیروز", "فردا", "امشب"], ans: "امروز", subject: "عربی" },
    { q: "HTML چیست؟", opts: ["زبان نشانه‌گذاری برای ساخت صفحات وب", "یک سیستم‌عامل", "یک نوع حافظه", "یک نرم‌افزار نقاشی"], ans: "زبان نشانه‌گذاری برای ساخت صفحات وب", subject: "کامپیوتر" },
    { q: "خروجی دستور print(2 + 3) در پایتون چیست؟", opts: ["5", "23", "2 + 3", "خطا"], ans: "5", subject: "کامپیوتر" },
    { q: "متغیر در برنامه‌نویسی چیست؟", opts: ["جایی برای ذخیره یک مقدار با یک نام", "نوعی حافظه فیزیکی", "نوعی کابل", "نوعی ویروس"], ans: "جایی برای ذخیره یک مقدار با یک نام", subject: "کامپیوتر" },
    { q: "حلقه (Loop) در برنامه‌نویسی چه کاری انجام می‌دهد؟", opts: ["چند بار تکرار دستورها", "حذف برنامه", "ذخیره فایل", "چاپ تصویر"], ans: "چند بار تکرار دستورها", subject: "کامپیوتر" },
    { q: "دستور شرطی if چه زمانی دستورهای خود را اجرا می‌کند؟", opts: ["وقتی شرط درست باشد", "همیشه", "هرگز", "فقط وقتی شرط نادرست باشد"], ans: "وقتی شرط درست باشد", subject: "کامپیوتر" },
    { q: "عدد دودویی 1010 در مبنای 10 برابر کدام است؟", opts: ["10", "8", "12", "5"], ans: "10", subject: "کامپیوتر" },
    { q: "فیشینگ (Phishing) چیست؟", opts: ["فریب کاربر برای دزدیدن اطلاعات شخصی", "نوعی بازی", "نوعی حافظه", "نوعی پردازنده"], ans: "فریب کاربر برای دزدیدن اطلاعات شخصی", subject: "کامپیوتر" },
    { q: "رمزنگاری چه هدفی دارد؟", opts: ["تبدیل اطلاعات به شکلی که بدون کلید قابل خواندن نباشد", "افزایش سرعت اینترنت", "ویرایش عکس", "حذف ویروس‌ها"], ans: "تبدیل اطلاعات به شکلی که بدون کلید قابل خواندن نباشد", subject: "کامپیوتر" },
    { q: "پشتیبان‌گیری (Backup) یعنی چه؟", opts: ["تهیه نسخه‌ای دیگر از اطلاعات", "حذف فایل‌ها", "خاموش کردن سیستم", "تغییر نام پوشه"], ans: "تهیه نسخه‌ای دیگر از اطلاعات", subject: "کامپیوتر" },
    { q: "DNS چه کاری انجام می‌دهد؟", opts: ["نام دامنه را به نشانی IP تبدیل می‌کند", "ویروس‌ها را حذف می‌کند", "فایل‌ها را فشرده می‌کند", "صدا را پخش می‌کند"], ans: "نام دامنه را به نشانی IP تبدیل می‌کند", subject: "کامپیوتر" },
    { q: "پیشوند HTTPS در نشانی وب‌سایت نشان‌دهنده چیست؟", opts: ["ارتباط رمزگذاری‌شده و امن‌تر", "سرعت بیشتر", "وب‌سایت رایگان", "وب‌سایت بدون تبلیغات"], ans: "ارتباط رمزگذاری‌شده و امن‌تر", subject: "کامپیوتر" },
    { q: "تابع AVERAGE در Excel چه کاری انجام می‌دهد؟", opts: ["محاسبه میانگین", "جمع کردن", "پیدا کردن بزرگ‌ترین", "شمردن سلول‌ها"], ans: "محاسبه میانگین", subject: "کامپیوتر" },
    { q: "فرمول =A1*B1 در Excel چه کاری انجام می‌دهد؟", opts: ["ضرب مقدار دو سلول", "جمع دو سلول", "تقسیم دو سلول", "مقایسه دو سلول"], ans: "ضرب مقدار دو سلول", subject: "کامپیوتر" },
    { q: "SSD چیست؟", opts: ["نوعی حافظه ذخیره‌سازی سریع‌تر از هارد دیسک معمولی", "نوعی چاپگر", "نوعی مانیتور", "نوعی کیبورد"], ans: "نوعی حافظه ذخیره‌سازی سریع‌تر از هارد دیسک معمولی", subject: "کامپیوتر" },
    { q: "GPU مخفف چیست و چه کاری انجام می‌دهد؟", opts: ["واحد پردازش گرافیکی؛ پردازش تصویر و گرافیک", "واحد ذخیره‌سازی؛ نگهداری فایل", "کارت صدا؛ پخش موسیقی", "کارت شبکه؛ اتصال به اینترنت"], ans: "واحد پردازش گرافیکی؛ پردازش تصویر و گرافیک", subject: "کامپیوتر" },
    { q: "نرم‌افزار «متن‌باز» (Open Source) یعنی چه؟", opts: ["کد منبع آن برای همه قابل مشاهده است", "همیشه پولی است", "فقط روی موبایل کار می‌کند", "کد آن مخفی است"], ans: "کد منبع آن برای همه قابل مشاهده است", subject: "کامپیوتر" },
    { q: "لینوکس چه نوع نرم‌افزاری است؟", opts: ["سیستم‌عامل متن‌باز", "مرورگر اینترنت", "نرم‌افزار ویرایش عکس", "بازی رایانه‌ای"], ans: "سیستم‌عامل متن‌باز", subject: "کامپیوتر" },
    { q: "هوش مصنوعی یعنی چه؟", opts: ["توانایی ماشین برای انجام کارهایی که معمولاً به هوش انسان نیاز دارد", "نوعی کابل شبکه", "نوعی حافظه موقت", "نوعی چاپگر"], ans: "توانایی ماشین برای انجام کارهایی که معمولاً به هوش انسان نیاز دارد", subject: "کامپیوتر" },
    { q: "رعایت «کپی‌رایت» یعنی چه؟", opts: ["احترام به حقوق مالکیت آثار دیگران", "کپی کردن آزادانه هر چیزی", "حذف فایل‌های دیگران", "تغییر نام فایل‌ها"], ans: "احترام به حقوق مالکیت آثار دیگران", subject: "کامپیوتر" },
    { q: "گذشته فعل «buy» کدام است؟", opts: ["bought", "buyed", "boughted", "buyen"], ans: "bought", subject: "انگلیسی" },
    { q: "گذشته فعل «teach» کدام است؟", opts: ["taught", "teached", "tought", "teachen"], ans: "taught", subject: "انگلیسی" },
    { q: "اسم مفعول (Past Participle) فعل «take» کدام است؟", opts: ["taken", "took", "taked", "takes"], ans: "taken", subject: "انگلیسی" },
    { q: "جمله را کامل کنید: If it rains, we ___ at home.", opts: ["will stay", "stayed", "staying", "stays"], ans: "will stay", subject: "انگلیسی" },
    { q: "حالت تفضیلی کلمه «good» کدام است؟", opts: ["better", "gooder", "best", "more good"], ans: "better", subject: "انگلیسی" },
    { q: "حالت عالی کلمه «bad» کدام است؟", opts: ["worst", "baddest", "worse", "most bad"], ans: "worst", subject: "انگلیسی" },
    { q: "حالت تفضیلی کلمه «big» کدام است؟", opts: ["bigger", "more big", "biggest", "bigly"], ans: "bigger", subject: "انگلیسی" },
    { q: "حالت تفضیلی کلمه «beautiful» کدام است؟", opts: ["more beautiful", "beautifuler", "most beautiful", "beautifulest"], ans: "more beautiful", subject: "انگلیسی" },
    { q: "جمله را کامل کنید: I have lived here ___ 2010.", opts: ["since", "for", "from", "in"], ans: "since", subject: "انگلیسی" },
    { q: "جمله را کامل کنید: She has lived here ___ five years.", opts: ["for", "since", "from", "at"], ans: "for", subject: "انگلیسی" },
    { q: "جمله را کامل کنید: He is ___ than me.", opts: ["taller", "tallest", "more tall", "tall"], ans: "taller", subject: "انگلیسی" },
    { q: "کدام کلمه «اسم» است؟", opts: ["happiness", "happy", "quickly", "beautiful"], ans: "happiness", subject: "انگلیسی" },
    { q: "کدام کلمه «صفت» است؟", opts: ["colorful", "quickly", "happiness", "run"], ans: "colorful", subject: "انگلیسی" },
    { q: "معنی کلمه «Achieve» چیست؟", opts: ["به دست آوردن", "از دست دادن", "فراموش کردن", "شکستن"], ans: "به دست آوردن", subject: "انگلیسی" },
    { q: "معنی کلمه «Borrow» چیست؟", opts: ["قرض گرفتن", "قرض دادن", "خریدن", "فروختن"], ans: "قرض گرفتن", subject: "انگلیسی" },
    { q: "معنی کلمه «Improve» چیست؟", opts: ["بهبود دادن", "خراب کردن", "فراموش کردن", "شروع کردن"], ans: "بهبود دادن", subject: "انگلیسی" },
    { q: "معنی کلمه «Necessary» چیست؟", opts: ["ضروری", "غیرممکن", "ارزان", "خطرناک"], ans: "ضروری", subject: "انگلیسی" },
    { q: "معنی کلمه «Opinion» چیست؟", opts: ["عقیده", "سؤال", "قانون", "تصمیم"], ans: "عقیده", subject: "انگلیسی" },
    { q: "معنی کلمه «Hobby» چیست؟", opts: ["سرگرمی", "شغل", "درس", "مسابقه"], ans: "سرگرمی", subject: "انگلیسی" },
    { q: "معنی کلمه «Pollution» چیست؟", opts: ["آلودگی", "انرژی", "ترافیک", "گرمایش"], ans: "آلودگی", subject: "انگلیسی" }
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
    me.score = Math.max(0, me.score + (correct ? POINTS : -POINTS));
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
