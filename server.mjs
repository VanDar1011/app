// Chánh Điện Tịnh Tâm — live server
// Runs on the streaming PC. Serves the overlay + control panel, listens to a TikTok LIVE room,
// keeps the list of seated viewers, generates Vietnamese voice lines with Edge TTS and pushes
// everything to the overlay over WebSocket.
//
//   cd live && npm install && node server.mjs        → http://localhost:3000/live/control.html
//
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { MsEdgeTTS, OUTPUT_FORMAT } from 'msedge-tts';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = __dirname; // All HTML files and assets are located beside server.mjs
const TTS_DIR = path.join(__dirname, 'tts');
fs.mkdirSync(TTS_DIR, { recursive: true });
for (const f of fs.readdirSync(TTS_DIR)) { try { fs.unlinkSync(path.join(TTS_DIR, f)); } catch {} }
const PORT = +process.env.PORT || 8000;

/* ------------------------------------------------------------------ state */
const settings = {
  title: 'Chánh Điện Tịnh Tâm',
  subtitle: 'Nam Mô A Di Đà Phật',
  voice: 'vi-VN-NamMinhNeural',      // or vi-VN-HoaiMyNeural
  pitch: -18,                        // Hz
  rate: -20,                         // %
  music: true, mo: true, moBpm: 52,
  autoChant: true, chantEveryMin: 3,
  seatMode: 'auto',                  // auto | 24 | 36 | 64 | 100
  joinOnEnter: false,                // also seat viewers who only enter the room
  gifts: { readAll: true, minDiamonds: 1 },
};
const viewers = new Map();           // id -> { id, name, skin, avatar, coins, joinedAt, lastAt, lastText, lastTextAt, demo }
const startedAt = Date.now();
let tiktok = null, tiktokState = { status: 'idle', uniqueId: '', error: '' };
const log = [];
function addLog(s){ const line = `[${new Date().toLocaleTimeString('vi-VN')}] ${s}`; log.push(line); if (log.length > 300) log.shift(); broadcast({ t:'log', line }, 'control'); console.log(line); }

const CAPS = [24, 36, 64, 100];
function capacity(){ if (settings.seatMode !== 'auto') return +settings.seatMode; const n = viewers.size; return CAPS.find(c => n <= c) || 100; }

/* skins: sprites that exist in /assets. Walking strips exist for man / woman / granny. */
// A comment changes the outfit only when it IS the keyword (≤ 3 words), so "Nam mô A Di Đà Phật" never reads as "nam".
const SKIN_WORDS = [
  ['master', ['cao tăng','cao tang']], ['monk', ['sư','su','sư thầy','su thay','thầy','thay']], ['nun', ['ni','ni cô','ni co','sư cô','su co']],
  ['child', ['tiểu','tieu','chú tiểu','chu tieu','bé','be']], ['granny', ['bà','ba','áo nâu','ao nau','cụ','cu']],
  ['woman', ['nữ','nu','chị','chi','gái','gai']], ['man', ['nam','anh','trai']],
];
function skinFromText(t){
  const w = String(t).toLowerCase().normalize('NFC').replace(/[^\p{L}\s]/gu, ' ').replace(/\s+/g, ' ').trim();
  if (!w || w.split(' ').length > 3) return null;
  for (const [skin, words] of SKIN_WORDS) if (words.some(k => w === k || w.startsWith(k + ' '))) return skin;
  return null;
}
const DEFAULT_SKINS = ['man', 'woman', 'woman', 'man', 'granny'];

function pub(v){ return { id:v.id, name:v.name, skin:v.skin, avatar:v.avatar||'', coins:v.coins, demo:!!v.demo }; }
function snapshot(){ return { t:'state', settings, viewers:[...viewers.values()].map(pub), cap: capacity(), startedAt, tiktok: tiktokState }; }

function seat(id, name, opts = {}){
  if (!id) return { v:null, isNew:false };
  let v = viewers.get(id);
  if (v){
    v.lastAt = Date.now();
    if (name) v.name = String(name).slice(0, 18);
    if (opts.avatar) v.avatar = opts.avatar;
    return { v, isNew:false };
  }
  if (viewers.size >= 100){                     // full: the longest-quiet non-donor stands up
    const quiet = [...viewers.values()].filter(x => x.coins === 0).sort((a,b) => a.lastAt - b.lastAt)[0];
    if (!quiet) return { v:null, isNew:false };
    viewers.delete(quiet.id); broadcast({ t:'leave', id: quiet.id });
  }
  v = { id, name: (name||id).slice(0, 18), skin: opts.skin || DEFAULT_SKINS[viewers.size % DEFAULT_SKINS.length],
        avatar: opts.avatar || '', coins: 0, joinedAt: Date.now(), lastAt: Date.now(), demo: !!opts.demo };
  viewers.set(id, v);
  broadcast({ t:'join', v: pub(v), cap: capacity() });
  return { v, isNew:true };
}
function leave(id){
  if (!id || !viewers.has(id)) return false;
  viewers.delete(id);
  broadcast({ t:'leave', id, cap: capacity() });
  return true;
}

function webId(id){
  const raw = String(id || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 48);
  if (!raw) return '';
  return raw.startsWith('web_') ? raw : `web_${raw}`;
}

function onViewer(ws, m){
  if (!m || typeof m !== 'object') return;
  switch (m.cmd){
    case 'join': {
      const id = webId(m.id || ws.viewerId);
      if (!id) return;
      ws.viewerId = id;
      const name = String(m.name || 'Phật tử').slice(0, 18);
      const skin = ['man', 'woman', 'granny'].includes(m.skin) ? m.skin : undefined;
      seat(id, name, { skin });
      break;
    }
    case 'leave': {
      const id = webId(m.id || ws.viewerId);
      if (!id) return;
      leave(id);
      break;
    }
    case 'chat': {
      const id = webId(m.id || ws.viewerId);
      if (!id) return;
      const v = seat(id, String(m.name || 'Phật tử').slice(0, 18), {}).v;
      if (!v) return;
      const text = String(m.text || '').slice(0, 120);
      if (!text) return;
      onChat(id, v.name, text, '');
      break;
    }
    case 'gift': {
      const id = webId(m.id || ws.viewerId);
      if (!id) return;
      const v = seat(id, String(m.name || 'Phật tử').slice(0, 18), {}).v;
      if (!v) return;
      const coins = clamp(m.coins, 1, 9999);
      const count = clamp(m.count || 1, 1, 99);
      const giftName = String(m.giftName || 'Cánh sen').slice(0, 30);
      const diamonds = Math.max(1, Math.round(coins / count));
      onGift(id, v.name, giftName, count, diamonds, '');
      break;
    }
    case 'skin': {
      const id = webId(m.id || ws.viewerId);
      if (!id) return;
      const v = viewers.get(id);
      const skin = ['man', 'woman', 'granny'].includes(m.skin) ? m.skin : null;
      if (!v || !skin || skin === v.skin) return;
      v.skin = skin;
      broadcast({ t:'skin', id, skin });
      break;
    }
  }
}

/* ------------------------------------------------------------- events in */
function onChat(id, name, text, avatar){
  text = String(text || '').slice(0, 120);
  const want = skinFromText(text);
  const { v, isNew } = seat(id, name, { avatar, skin: want || undefined });
  if (!v) return;
  if (!isNew && want && want !== v.skin){ v.skin = want; broadcast({ t:'skin', id, skin: want }); }
  v.lastText = text; v.lastTextAt = Date.now();
  broadcast({ t:'chat', id, name: v.name, text });
}

function onGift(id, name, giftName, count, diamonds, avatar){
  giftName = String(giftName || 'quà').slice(0, 30); count = Math.max(1, Math.min(9999, +count || 1));
  const { v } = seat(id, name, { avatar });
  if (!v) return;
  const coins = Math.max(1, (diamonds || 1) * (count || 1));
  v.coins += coins;
  const prayer = (v.lastText && Date.now() - v.lastTextAt < 90_000 && v.lastText.length > 6) ? v.lastText : pickPrayer();
  const tier = coins >= 99 ? 3 : coins >= 10 ? 2 : 1;
  const line = `Nam mô A Di Đà Phật. Phật tử ${spoken(v.name)} thành tâm dâng ${giftName}${count > 1 ? ', ' + count + ' lần' : ''}. ${prayer}.`;
  broadcast({ t:'gift', id, name: v.name, giftName, count, coins, total: v.coins, tier, prayer });
  if (settings.gifts.readAll || coins >= settings.gifts.minDiamonds) say(line, { tag:'gift', id });
}

function pickPrayer(){
  const P = ['Nguyện cầu gia đạo bình an, vạn sự cát tường', 'Nguyện cầu cha mẹ mạnh khỏe, sống lâu', 'Nguyện hồi hướng công đức cho khắp pháp giới chúng sinh',
             'Nguyện cầu thân tâm an lạc, phiền não tiêu trừ', 'Nguyện cầu công việc hanh thông, gia đình hạnh phúc'];
  return P[Math.floor(Math.random() * P.length)];
}
const spoken = s => String(s).replace(/[_@.]+/g, ' ').replace(/[^\p{L}\p{N}\s]/gu, '').trim() || 'ẩn danh';

/* ------------------------------------------------------------------ TTS */
const ttsQueue = []; let ttsBusy = false, ttsSeq = 0;
function esc(s){ return String(s).replace(/[<>&"']/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&apos;'}[c])); }
function say(text, meta = {}){ if (ttsQueue.length >= 15) { addLog('Hàng đợi giọng đọc đầy, bỏ qua 1 câu'); return; } ttsQueue.push({ text: String(text).slice(0, 500), meta }); pumpTTS(); }
async function pumpTTS(){
  if (ttsBusy || !ttsQueue.length) return; ttsBusy = true;
  const { text, meta } = ttsQueue.shift();
  try {
    const file = `tts_${Date.now()}_${++ttsSeq}.mp3`;
    const tts = new MsEdgeTTS();
    await tts.setMetadata(settings.voice, OUTPUT_FORMAT.AUDIO_24KHZ_96KBITRATE_MONO_MP3);
    const { audioStream } = tts.toStream(esc(text), { pitch: `${settings.pitch >= 0 ? '+' : ''}${settings.pitch}Hz`, rate: `${settings.rate >= 0 ? '+' : ''}${settings.rate}%` });
    const chunks = [];
    await new Promise((res, rej) => { audioStream.on('data', c => chunks.push(c)); audioStream.on('close', res); audioStream.on('error', rej); });
    try { tts.close(); } catch {}
    fs.writeFileSync(path.join(TTS_DIR, file), Buffer.concat(chunks));
    broadcast({ t:'tts', url: `/live/tts/${file}`, text, ...meta });
    const old = fs.readdirSync(TTS_DIR).sort(); while (old.length > 40) { try { fs.unlinkSync(path.join(TTS_DIR, old.shift())); } catch {} }
  } catch (e) { addLog('TTS lỗi: ' + (e?.message || e)); }
  ttsBusy = false; setTimeout(pumpTTS, 50);
}

const CHANTS = [
  'Nam mô Bổn Sư Thích Ca Mâu Ni Phật. Nguyện đem công đức này, hướng về khắp tất cả, đệ tử và chúng sinh, đều trọn thành Phật đạo.',
  'Nam mô A Di Đà Phật. Nguyện sanh Tây Phương Tịnh Độ trung, cửu phẩm liên hoa vi phụ mẫu.',
  'Nam mô Đại Bi Quán Thế Âm Bồ Tát. Nguyện cho tất cả chúng sinh thân tâm an lạc, tai qua nạn khỏi.',
  'Quán Tự Tại Bồ Tát hành thâm Bát Nhã Ba La Mật Đa thời, chiếu kiến ngũ uẩn giai không, độ nhất thiết khổ ách.',
];
let chantIdx = 0, chantTimer = null;
function chant(){ const text = CHANTS[chantIdx++ % CHANTS.length]; broadcast({ t:'chant', text }); say(text, { tag:'chant' }); }
function scheduleChant(){ clearInterval(chantTimer); if (settings.autoChant) chantTimer = setInterval(chant, Math.max(1, settings.chantEveryMin) * 60_000); }
scheduleChant();

/* --------------------------------------------------------------- TikTok */
let TikTokLib = null;
async function connectTikTok(uniqueId){
  uniqueId = String(uniqueId || '').trim().replace(/^.*tiktok\.com\/@?/i, '').replace(/^@/, '').replace(/\/.*$/, '');
  if (!uniqueId) return;
  await disconnectTikTok();
  tiktokState = { status: 'connecting', uniqueId, error: '' }; broadcast({ t:'tiktok', ...tiktokState });
  try {
    TikTokLib ||= await import('tiktok-live-connector');
    const { TikTokLiveConnection, WebcastEvent, ControlEvent } = TikTokLib;
    const conn = new TikTokLiveConnection(uniqueId, {
      enableExtendedGiftInfo: false,
      fetchRoomInfoOnConnect: false,
      processInitialData: false,
    });
    const uid = u => u?.displayId || u?.uniqueId || (u?.id ? String(u.id) : '') || (u?.userId ? String(u.userId) : '');
    const av = u => u?.profilePicture?.url?.[0] || u?.profilePicture?.urls?.[0] || u?.profilePicture?.urlList?.[0] || u?.avatarThumb?.urlList?.[0] || u?.avatarThumb?.url?.[0] || '';
    const nm = u => u?.nickname || u?.displayId || u?.uniqueId || 'ẩn danh';

    conn.on(WebcastEvent.CHAT, d => {
      const id = uid(d.user);
      if (id) onChat(id, nm(d.user), d.content ?? d.comment ?? '', av(d.user));
    });
    conn.on(WebcastEvent.GIFT, d => {
      const id = uid(d.user);
      if (!id) return;
      const type = d.gift?.type ?? d.giftDetails?.giftType ?? d.extendedGiftInfo?.type;
      if (type === 1 && !d.repeatEnd) return;                       // streak still running
      const gname = d.gift?.name || d.giftDetails?.giftName || d.extendedGiftInfo?.name || 'quà';
      const dia = d.gift?.diamondCount ?? d.giftDetails?.diamondCount ?? d.extendedGiftInfo?.diamond_count ?? 1;
      onGift(id, nm(d.user), gname, d.repeatCount || 1, dia, av(d.user));
      addLog(`🎁 ${nm(d.user)} tặng ${gname} x${d.repeatCount || 1}`);
    });
    conn.on(WebcastEvent.MEMBER, d => {
      const id = uid(d.user);
      if (id && settings.joinOnEnter) seat(id, nm(d.user), { avatar: av(d.user) });
    });
    conn.on(WebcastEvent.LIKE, d => {
      const id = uid(d.user);
      if (id) broadcast({ t:'like', id, n: d.likeCount || 1 });
    });
    conn.on(ControlEvent.DISCONNECTED, () => { tiktokState = { ...tiktokState, status: 'disconnected' }; broadcast({ t:'tiktok', ...tiktokState }); addLog('TikTok: mất kết nối'); });
    conn.on(ControlEvent.ERROR, e => addLog('TikTok lỗi: ' + (e?.info || e?.message || e)));
    const st = await conn.connect();
    tiktok = conn;
    tiktokState = { status: 'connected', uniqueId, error: '', roomId: st?.roomId || '' };
    addLog(`✅ Đã kết nối live @${uniqueId}`);
  } catch (e) {
    tiktokState = { status: 'error', uniqueId, error: String(e?.message || e) };
    addLog(`❌ Không kết nối được @${uniqueId}: ${tiktokState.error}`);
  }
  broadcast({ t:'tiktok', ...tiktokState });
}
async function disconnectTikTok(){ if (tiktok){ try { await tiktok.disconnect(); } catch {} tiktok = null; } tiktokState = { status:'idle', uniqueId:'', error:'' }; broadcast({ t:'tiktok', ...tiktokState }); }

/* ------------------------------------------------------------- test data */
const DEMO_NAMES = ['Diệu Hạnh','Minh Tâm','Thiện Đức','Huệ An','Tịnh Nhã','Quảng Trí','Chơn Như','Pháp Hỷ','Nguyên Thanh','Tâm Đăng','Diệu Liên','Trí Hải','Phước Lộc','Hạnh Nguyện','Như Ý','Bảo Châu','Thanh Lương','Từ Tâm','An Lạc','Giác Hoa','Tuệ Minh','Liên Hương','Thu Hà','Ngọc Lan','Đức Anh','Hải Đăng','Mai Anh','Phương Thảo','Trung Kiên','Quang Huy','Bảo Ngọc','Diệu Linh'];
function fillDemo(n){
  const have = [...viewers.values()].filter(v => v.demo).length;
  for (let i = have; i < n && viewers.size < 100; i++){
    const name = DEMO_NAMES[i % DEMO_NAMES.length] + (i >= DEMO_NAMES.length ? ' ' + (Math.floor(i / DEMO_NAMES.length) + 1) : '');
    seat('demo_' + i, name, { demo: true, skin: ['man','woman','woman','man','granny','monk','nun','child'][i % 8] });
  }
}
function clearSeats(){ viewers.clear(); broadcast(snapshot()); addLog('Đã làm trống bồ đoàn'); }

/* ------------------------------------------------------------- control */
function onControl(m){
  switch (m.cmd){
    case 'connect': connectTikTok(m.uniqueId); break;
    case 'disconnect': disconnectTikTok(); addLog('Đã ngắt kết nối TikTok'); break;
    case 'chat': { const n = String(m.name || 'Khách').slice(0, 18); onChat('sim_' + n, n, String(m.text || 'Nam mô A Di Đà Phật').slice(0, 120)); break; }
    case 'gift': { const n = String(m.name || 'Khách').slice(0, 18); onGift('sim_' + n, n, String(m.gift || 'Hoa hồng').slice(0, 30), clamp(m.count, 1, 99), clamp(m.diamonds, 1, 50000)); break; }
    case 'fill': fillDemo(clamp(m.n, 1, 100)); addLog(`Đổ người mẫu: ${viewers.size} người`); break;
    case 'clear': clearSeats(); break;
    case 'bell': broadcast({ t:'bell' }); break;
    case 'mo': broadcast({ t:'mo1' }); break;
    case 'chant': chant(); break;
    case 'say': if (m.text) say(String(m.text).slice(0, 400), { tag:'say' }); break;
    case 'settings':
      applySettings(m.settings || {});
      scheduleChant(); broadcast({ t:'settings', settings, cap: capacity() }); break;
    case 'bots': setBots(!!m.on, +m.n || 12, m.pace); break;
  }
}

// Only known keys, right types, sane ranges: the panel is local, but nothing from the socket is trusted blindly.
const SETTING_RULES = {
  title: v => String(v).slice(0, 40), subtitle: v => String(v).slice(0, 60),
  voice: v => ['vi-VN-NamMinhNeural', 'vi-VN-HoaiMyNeural'].includes(v) ? v : settings.voice,
  pitch: v => clamp(v, -50, 50), rate: v => clamp(v, -50, 50), moBpm: v => clamp(v, 20, 120), chantEveryMin: v => clamp(v, 1, 60),
  music: Boolean, mo: Boolean, autoChant: Boolean, joinOnEnter: Boolean,
  seatMode: v => ['auto', '24', '36', '64', '100'].includes(String(v)) ? String(v) : 'auto',
};
const clamp = (v, a, b) => Math.max(a, Math.min(b, Math.round(+v || 0)));
function applySettings(o){ for (const [k, fn] of Object.entries(SETTING_RULES)) if (k in o) settings[k] = fn(o[k]); }

/* ----------------------------------------------------------------- bots */
let botTimer = null;
const BOT_LINES = ['Nam mô A Di Đà Phật', 'Nam mô Bổn Sư Thích Ca Mâu Ni Phật', 'Cầu cho gia đình bình an', 'Con xin thành tâm sám hối', 'nữ', 'nam', 'bà',
  'Nguyện cầu cha mẹ mạnh khỏe', 'Mô Phật, an lạc quá', 'Chúc mọi người một ngày bình an', 'sư', 'Cầu công việc hanh thông'];
function setBots(on, n, pace){
  clearInterval(botTimer); botTimer = null;
  if (on){
    n = clamp(n, 1, 100);
    for (let i = 0; i < n; i++) seat('bot_' + i, DEMO_NAMES[(i + 7) % DEMO_NAMES.length] + ' ·bot', { demo: true });
    const every = { slow: 4000, mid: 1800, fast: 700 }[pace] || 1800;
    botTimer = setInterval(() => {
      const ids = [...viewers.keys()].filter(id => id.startsWith('bot_')); if (!ids.length) return;
      const id = ids[Math.floor(Math.random() * ids.length)], v = viewers.get(id), r = Math.random();
      if (r < .62) onChat(id, v.name, BOT_LINES[Math.floor(Math.random() * BOT_LINES.length)]);
      else if (r < .9) broadcast({ t:'like', id, n: 1 + Math.floor(Math.random() * 4) });
      else if (r < .985) onGift(id, v.name, 'Hoa hồng', 1 + Math.floor(Math.random() * 3), 1);
      else onGift(id, v.name, 'Đèn hoa sen', 1, 99);
    }, every);
    addLog(`🤖 Bật ${n} bot, nhịp ${pace}`);
  } else {
    for (const id of [...viewers.keys()]) if (id.startsWith('bot_')) { viewers.delete(id); broadcast({ t:'leave', id }); }
    addLog('🤖 Tắt bot');
  }
  broadcast({ t:'bots', on }, 'control');
}

/* -------------------------------------------------------------- server */
// Security notes (this file is meant to be shared publicly):
//  • listens on 127.0.0.1 only; nothing is reachable from the LAN or internet unless HOST is changed on purpose
//  • Host header must be localhost/127.0.0.1 (blocks DNS-rebinding pages from talking to the server)
//  • WebSocket only accepts pages served by this server (Origin check), so other sites open in the
//    same browser cannot send control commands
//  • static files: only /assets and /live, no dot-files, no node_modules, no server source, no path escapes
//  • no TikTok password, cookie or session is ever used or stored; only public room events are read
const ALLOW_REMOTE = process.env.ALLOW_REMOTE === '1';
const HOST = process.env.HOST || (ALLOW_REMOTE ? '0.0.0.0' : '127.0.0.1');
const PUBLIC_HOSTS = String(process.env.PUBLIC_HOSTS || '')
  .split(',')
  .map(s => s.trim().toLowerCase())
  .filter(Boolean);
const MIME = { '.html':'text/html; charset=utf-8', '.js':'text/javascript', '.css':'text/css', '.json':'application/json',
  '.png':'image/png', '.jpg':'image/jpeg', '.jpeg':'image/jpeg', '.gif':'image/gif', '.wav':'audio/wav', '.ogg':'audio/ogg', '.woff':'font/woff', '.woff2':'font/woff2', '.webp':'image/webp', '.mp3':'audio/mpeg', '.svg':'image/svg+xml', '.ico':'image/x-icon' };
const ALLOWED_HOSTS = new Set([
  `localhost:${PORT}`, `127.0.0.1:${PORT}`, `[::1]:${PORT}`,
  'localhost', '127.0.0.1', '[::1]',
  ...PUBLIC_HOSTS,
  ...PUBLIC_HOSTS.map(h => h.includes(':') ? h : `${h}:${PORT}`),
]);
const okHost = h => {
  if (ALLOW_REMOTE) return true;
  const host = String(h || '').toLowerCase();
  if (ALLOWED_HOSTS.has(host)) return true;
  if (host.endsWith(':443') && ALLOWED_HOSTS.has(host.slice(0, -4))) return true;
  if (host.endsWith(':80') && ALLOWED_HOSTS.has(host.slice(0, -3))) return true;
  return false;
};
const SEP = path.sep;
function resolveStatic(urlPath){
  // Serve all three HTML entry points from the project root.
  // Keep legacy /live/*.html URLs working for old bookmarks.
  const entryPoints = new Map([
    ['/', 'index.html'],
    ['/index.html', 'index.html'],
    ['/overlay.html', 'overlay.html'],
    ['/control.html', 'control.html'],
    ['/live/', 'index.html'],
    ['/live/index.html', 'index.html'],
    ['/live/overlay.html', 'overlay.html'],
    ['/live/control.html', 'control.html'],
  ]);
  if (entryPoints.has(urlPath)) return path.join(ROOT, entryPoints.get(urlPath));

  // The browser requests /assets/* and /tts/* using absolute paths.
  let rel;
  if (urlPath.startsWith('/assets/')) rel = urlPath.slice(1);
  else if (urlPath.startsWith('/tts/')) rel = urlPath.slice(1);
  else if (urlPath.startsWith('/live/tts/')) rel = urlPath.slice('/live/'.length);
  else return null;

  // Prevent traversal, hidden files, source/config exposure and unsafe paths.
  const segments = rel.split('/');
  if (segments.some(seg => !seg || seg === '.' || seg === '..' || seg.startsWith('.') || seg === 'node_modules' || seg.includes('\\') || seg.includes(':'))) return null;
  const base = segments[0];
  const filename = segments.at(-1);
  const ext = path.extname(filename).toLowerCase();
  const allowed = base === 'assets'
    ? new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.ico', '.mp3', '.wav', '.ogg', '.css', '.js', '.woff', '.woff2'])
    : new Set(['.mp3']);
  if (!allowed.has(ext)) return null;
  const rootDir = path.join(ROOT, base);
  const file = path.resolve(ROOT, ...segments);
  return file.startsWith(rootDir + SEP) ? file : null;
}
const server = http.createServer((req, res) => {
  if (!okHost(req.headers.host)) { res.writeHead(421).end('wrong host'); return; }
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405).end(); return; }
  let p; try { p = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch { res.writeHead(400).end(); return; }
  const file = resolveStatic(p);
  if (!file) { res.writeHead(404).end('not found'); return; }
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404).end('not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file).toLowerCase()] || 'application/octet-stream',
      'Cache-Control': (p.startsWith('/tts/') || p.startsWith('/live/tts/')) ? 'no-store' : 'no-cache', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer' });
    if (req.method === 'HEAD') { res.end(); return; }
    fs.createReadStream(file).pipe(res);
  });
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024,
  verifyClient: ({ origin, req }) => {
    const host = String(req.headers.host || '').toLowerCase();
    const xfHost = String(req.headers['x-forwarded-host'] || '').toLowerCase();
    const realHost = xfHost || host;

    if (!okHost(realHost)) {
      addLog(`WS chặn: host không hợp lệ (${realHost || '-'})`);
      return false;
    }
    if (!origin) return true;                                   // OBS / non-browser clients send no Origin
    try {
      const o = new URL(origin).host.toLowerCase();
      if (okHost(o)) return true;

      // Tolerate default ports stripped by proxies/CDNs (443/80)
      const oh = o.replace(/:443$|:80$/, '');
      const rh = realHost.replace(/:443$|:80$/, '');
      if (oh && rh && oh === rh) return true;

      addLog(`WS chặn: origin không khớp (origin=${o}, host=${realHost || '-'})`);
      return false;
    } catch {
      addLog(`WS chặn: origin không parse được (${origin})`);
      return false;
    }
  } });
function broadcast(msg, only){ const s = JSON.stringify(msg); for (const c of wss.clients) if (c.readyState === 1 && (!only || c.role === only)) c.send(s); }
wss.on('connection', (ws, req) => {
  const role = new URL(req.url, 'http://x').searchParams.get('role');
  ws.role = role === 'control' ? 'control' : role === 'viewer' ? 'viewer' : 'overlay';
  ws.send(JSON.stringify(snapshot()));
  if (ws.role === 'control') ws.send(JSON.stringify({ t:'logs', lines: log }));
  ws.on('message', buf => {
    try {
      const m = JSON.parse(buf);
      if (ws.role === 'control') {
        if (m && m.t === 'ctl') onControl(m);
        return;
      }
      if (ws.role === 'viewer') {
        if (m && m.t === 'viewer') onViewer(ws, m);
      }
    } catch (e) { addLog('Lệnh lỗi: ' + e.message); }
  });
  ws.on('close', () => {
    if (ws.role === 'viewer' && ws.viewerId) leave(ws.viewerId);
  });
});
server.listen(PORT, HOST, () => {
  const mode = ALLOW_REMOTE ? 'LAN' : 'LOCAL';
  addLog(`Server (${mode}) chạy tại http://localhost:${PORT}  ·  Client: http://localhost:${PORT}/  ·  Bảng điều khiển: http://localhost:${PORT}/control.html  ·  Overlay: http://localhost:${PORT}/overlay.html`);
  if (ALLOW_REMOTE) addLog('Đang mở kết nối LAN: máy khác dùng http://IP_MAY_CHAY_SERVER:' + PORT + '/ để vào cùng phiên.');
});