'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { NewMessage } = require('telegram/events');
const { initializeApp, cert } = require('firebase-admin/app');
const { getMessaging } = require('firebase-admin/messaging');
const { getFirestore } = require('firebase-admin/firestore');

// ---------------------------------------------------------
// CONFIG
// ---------------------------------------------------------

const CONFIG_PATH =
  process.env.CONFIG_PATH || path.join(__dirname, 'config.json');
const SERVICE_ACCOUNT_PATH =
  process.env.SERVICE_ACCOUNT_PATH || path.join(__dirname, 'service-account.json');
const TELEGRAM_PATH =
  process.env.TELEGRAM_PATH || path.join(__dirname, 'telegram.json');
const STATE_PATH =
  process.env.STATE_PATH || path.join(__dirname, 'state.json');

const config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));

// The static list of channels this server reads. Users pick from it in the app.
const CHANNELS = (config.channels || []).map((c) => c.toLowerCase());
const CHANNEL_SET = new Set(CHANNELS);

// Messages older than this are never pushed (protects against stale
// alerts after a restart or a long outage).
const MAX_AGE_MS = (config.maxAgeMinutes ?? 3) * 60 * 1000;

// How often the dialog list is checked for new posts.
const POLL_INTERVAL_MS = (config.pollSeconds ?? 5) * 1000;
// Dialogs whose latest message is older than the previous poll minus this
// margin cannot contain anything new, so the pass stops there.
const POLL_MARGIN_MS = 60 * 1000;
const DIALOG_LIMIT = 600;
const SLOW_POLL_MS = 3000; // warn when one pass takes longer than this

const STATS_INTERVAL_MS = 60 * 1000 * 10;
const PING_INTERVAL_MS = 20 * 1000;
const HEALTH_THRESHOLD_MS = 60 * 1000; // alert phones after 1 min of silence
const HEALTH_COOLDOWN_MS = 10 * 60 * 1000; // at most one alert per 10 min
const SELF_RESTART_MS = 3 * 60 * 1000; // exit (Docker restarts us) after 3 min
const PUSH_TTL_MS = 2 * 60 * 1000; // FCM drops undelivered alerts after 2 min
const MAX_WORDS = 50;

const log = (...args) => console.log(new Date().toISOString(), ...args);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const withTimeout = (promise, ms) =>
  Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms)
    ),
  ]);

if (CHANNELS.length === 0) {
  console.error('config.json needs a non-empty "channels" list');
  process.exit(1);
}

initializeApp({
  credential: cert(JSON.parse(fs.readFileSync(SERVICE_ACCOUNT_PATH, 'utf8'))),
});
const messaging = getMessaging();
const db = getFirestore();

const tg = JSON.parse(fs.readFileSync(TELEGRAM_PATH, 'utf8'));
const client = new TelegramClient(new StringSession(tg.session), tg.apiId, tg.apiHash, {
  connectionRetries: Infinity,
  autoReconnect: true,
});
client.setLogLevel('error');

// ---------------------------------------------------------
// STATS (one line per minute, shows the server is alive)
// ---------------------------------------------------------

const stats = {
  polls: 0,
  pollMsTotal: 0,
  pollMsMax: 0,
  pollErrors: 0,
  floodWaits: 0,
  newPosts: 0,
  pushedPosts: 0,
  devicesMatched: 0,
  pushOk: 0,
  pushFailed: 0,
};

function printStats() {
  const avg = stats.polls ? Math.round(stats.pollMsTotal / stats.polls) : 0;
  log(
    `[-STATS-] last min: polls=${stats.polls} (avg ${avg}ms, max ${stats.pollMsMax}ms, ` +
      `errors ${stats.pollErrors}, flood waits ${stats.floodWaits}), ` +
      `received ${stats.newPosts} new message(s), pushed ${stats.pushedPosts}, ` +
      `matched ${stats.devicesMatched} device(s), push ok ${stats.pushOk}, push failed ${stats.pushFailed}, ` +
      `devices registered ${devices.size}`
  );
  for (const key of Object.keys(stats)) stats[key] = 0;
}

// ---------------------------------------------------------
// STATE (highest seen message id per channel, persisted)
// ---------------------------------------------------------

let state = {};
try {
  state = JSON.parse(fs.readFileSync(STATE_PATH, 'utf8'));
} catch {
  log('no state file, first run');
}
let stateDirty = false;

function saveState() {
  if (!stateDirty) return;
  stateDirty = false;
  const tmp = `${STATE_PATH}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(state));
  fs.renameSync(tmp, STATE_PATH);
}

function advanceState(channel, id) {
  if (id > (state[channel] || 0)) {
    state[channel] = id;
    stateDirty = true;
  }
}

// Recently seen ids per channel, so live updates and polling never
// produce the same message twice.
const seen = new Map();

function markSeen(channel, id) {
  let ids = seen.get(channel);
  if (!ids) seen.set(channel, (ids = new Set()));
  if (ids.has(id)) return false;
  ids.add(id);
  if (ids.size > 500) ids.delete(ids.values().next().value);
  advanceState(channel, id);
  return true;
}

// ---------------------------------------------------------
// DEVICES (per-user settings, live from Firestore)
// ---------------------------------------------------------

const devices = new Map(); // uid -> { token, channels:Set, search, exclude, maxLength }

function cleanWords(list) {
  if (!list || !Array.isArray(list.words)) return [];
  return list.words
    .filter((w) => typeof w === 'string')
    .map((w) => w.toLowerCase().trim())
    .filter(Boolean)
    .slice(0, MAX_WORDS);
}

function parseDevice(data) {
  if (!data || typeof data.token !== 'string' || !data.token) return null;
  const channels = (Array.isArray(data.channels) ? data.channels : [])
    .filter((c) => typeof c === 'string')
    .map((c) => c.toLowerCase())
    .filter((c) => CHANNEL_SET.has(c));
  const maxValue = data.maxLength && Number(data.maxLength.value);
  return {
    token: data.token,
    channels: new Set(channels),
    search: { enabled: !!(data.search && data.search.enabled === true), words: cleanWords(data.search) },
    exclude: { enabled: !!(data.exclude && data.exclude.enabled === true), words: cleanWords(data.exclude) },
    maxLength: {
      enabled: !!(data.maxLength && data.maxLength.enabled === true),
      value: Number.isFinite(maxValue) ? maxValue : 0,
    },
  };
}

function watchDevices() {
  db.collection('devices').onSnapshot(
    (snapshot) => {
      for (const change of snapshot.docChanges()) {
        const device = change.type === 'removed' ? null : parseDevice(change.doc.data());
        if (device) devices.set(change.doc.id, device);
        else devices.delete(change.doc.id);
      }
      log(`devices registered: ${devices.size}`);
    },
    (error) => {
      log(`devices listener error: ${error.message}, retrying in 5s`);
      setTimeout(watchDevices, 5000);
    }
  );
}

function matches(device, searchText, length) {
  if (device.maxLength.enabled && device.maxLength.value > 0 && length > device.maxLength.value) {
    return false;
  }
  if (device.exclude.enabled && device.exclude.words.some((w) => searchText.includes(w))) {
    return false;
  }
  if (device.search.enabled && device.search.words.length > 0 &&
      !device.search.words.some((w) => searchText.includes(w))) {
    return false;
  }
  return true;
}

// ---------------------------------------------------------
// TEXT
// ---------------------------------------------------------

// Text shown in the app: original case and punctuation, only junk removed.
function cleanText(raw) {
  return (raw || '')
    .replace(/[\u3164\u200B-\u200D\u2060\uFEFF]/g, '') // invisible fillers (kievreal1 uses them)
    .replace(/[^\S\n]+/g, ' ')   // runs of spaces/tabs -> one space
    .replace(/ ?\n ?/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

// Private copy used only for filtering. Never sent to the phone.
function forSearch(text) {
  return text.toLowerCase().replace(/\//g, ' ').replace(/\s+/g, ' ');
}

function formatDate(ts) {
  return new Date(ts).toLocaleString('uk-UA', { timeZone: 'Europe/Kyiv', hour12: false });
}

// ---------------------------------------------------------
// PUSH
// ---------------------------------------------------------

function removeDevice(uid) {
  log(`removing dead device ${uid.slice(0, 6)}`);
  devices.delete(uid);
  db.collection('devices').doc(uid).delete().catch(() => {});
}

// entries: [{ uid, token }]. All FCM data values must be strings.
// Returns { ok, failed }.
async function push(entries, data) {
  let ok = 0;
  let failed = 0;
  for (let i = 0; i < entries.length; i += 500) {
    const chunk = entries.slice(i, i + 500);
    try {
      const result = await messaging.sendEachForMulticast({
        tokens: chunk.map((e) => e.token),
        data,
        android: { priority: 'high', ttl: PUSH_TTL_MS },
      });
      ok += result.successCount;
      failed += result.failureCount;
      result.responses.forEach((response, index) => {
        if (response.success) return;
        const code = response.error && response.error.code;
        log(`FCM error for device ${chunk[index].uid.slice(0, 6)}: ${code}`);
        if (
          code === 'messaging/registration-token-not-registered' ||
          code === 'messaging/invalid-registration-token'
        ) {
          removeDevice(chunk[index].uid);
        }
      });
    } catch (error) {
      failed += chunk.length;
      log(`FCM batch failed: ${error.message}`);
    }
  }
  return { ok, failed };
}

const allDevices = () => [...devices].map(([uid, d]) => ({ uid, token: d.token }));

// ---------------------------------------------------------
// HANDLE ONE MESSAGE
// source: 'live' (pushed by Telegram), 'poll' (found by polling),
//         'catchup' (found by the first pass after startup)
// ---------------------------------------------------------

let catchupPosts = 0;
let catchupTooOld = 0;

async function handleMessage(channel, id, rawText, ts, source) {
  if (!markSeen(channel, id)) return;

  stats.newPosts++;
  if (source === 'catchup') catchupPosts++;

  const text = cleanText(rawText);
  if (!text) return; // photo-only posts etc.

  const searchText = forSearch(text);

  if (Date.now() - ts > MAX_AGE_MS) {
    if (source === 'catchup') catchupTooOld++;
    else log(`[${source}] too old to push: ${channel}#${id} (lag ${Math.round((Date.now() - ts) / 1000)}s)`);
    return;
  }

  const date = formatDate(ts);
  const lag = Math.round((Date.now() - ts) / 1000);

  const line = `${channel}#${id} - ${date} - ${text.replace(/\n/g, ' ')} (lag ${lag}s)`;

  const recipients = [];
  for (const [uid, device] of devices) {
    if (device.channels.has(channel) && matches(device, searchText, text.length)) {
      recipients.push({ uid, token: device.token });
    }
  }

  if (recipients.length === 0) {
    log(`[${source}] received 1 new message, 0 matching devices (of ${devices.size}), nothing pushed: ${line}`);
    return;
  }

  const result = await push(recipients, {
    type: 'message',
    channel,
    id: String(id),
    text,
    date,
    url: `https://t.me/${channel}/${id}`,
  });

  stats.pushedPosts++;
  stats.devicesMatched += recipients.length;
  stats.pushOk += result.ok;
  stats.pushFailed += result.failed;

  log(
    `[${source}] received 1 new message, found ${recipients.length} subscribed device(s) (of ${devices.size}), ` +
      `${result.ok} push successful, ${result.failed} failed: ${line}`
  );
}

// ---------------------------------------------------------
// TELEGRAM: channel map, live updates, polling
// ---------------------------------------------------------

const channelById = new Map(); // raw channel id (string) -> configured username

async function loadChannels() {
  channelById.clear();

  const dialogs = await client.getDialogs({ limit: 500 });
  const others = [];
  for (const dialog of dialogs) {
    const entity = dialog.entity;
    if (!entity) continue;

    // A channel can have several usernames; match any of them.
    const names = [entity.username, ...(entity.usernames || []).map((u) => u.username)]
      .filter(Boolean)
      .map((n) => n.toLowerCase());
    const match = names.find((n) => CHANNEL_SET.has(n));

    if (match) {
      channelById.set(String(entity.id), match);
    } else {
      others.push(`${entity.className}: ${entity.title || entity.firstName || '?'} (@${names[0] || 'no username'})`);
    }
  }

  const found = new Set(channelById.values());
  const missing = CHANNELS.filter((c) => !found.has(c));
  log(`listening to ${found.size}/${CHANNELS.length} channels`);
  if (missing.length > 0) {
    log(`WARNING not joined or not found (run join.js): ${missing.join(', ')}`);
    log(`other chats this account has (not in config): ${others.join('; ') || 'none'}`);
  }

  // Publish the list the app shows. Only channels we really listen to.
  await db.doc('config/channels').set({
    list: CHANNELS.filter((c) => found.has(c)),
    updatedAt: Date.now(),
  });
}

// Live updates. Telegram does not push big channels to this session, so this
// mostly stays silent, but it costs nothing and markSeen prevents duplicates.
async function onNewMessage(event) {
  try {
    const m = event.message;
    const channelId = m.peerId && m.peerId.channelId && m.peerId.channelId.toString();
    const channel = channelById.get(channelId);
    if (!channel) return;
    await handleMessage(channel, m.id, m.message, m.date * 1000, 'live');
  } catch (error) {
    log(`live handler error: ${error.message}`);
  }
}

// One pass over the dialog list. Each dialog carries its channel's newest post,
// so a quiet pass costs a single request. Dialogs are sorted by latest
// activity, so we stop as soon as we reach ones older than the previous pass.
let lastPollStartedAt = 0;
let firstPass = true;

async function pollOnce() {
  const startedAt = Date.now();
  const cutoffMs = lastPollStartedAt - POLL_MARGIN_MS; // 0 on the first pass: read everything
  const source = firstPass ? 'catchup' : 'poll';
  let checked = 0;

  for await (const d of client.iterDialogs({ limit: DIALOG_LIMIT })) {
    const top = d.message;
    if (!d.pinned && top && top.date * 1000 < cutoffMs) break; // nothing newer below

    const channel = channelById.get(String(d.entity && d.entity.id));
    if (!channel || !top) continue;
    checked++;

    const last = state[channel];

    // First ever run for this channel: remember position, push nothing.
    if (last === undefined) {
      advanceState(channel, top.id);
      log(`[${channel}] first run, starting from #${top.id}`);
      continue;
    }
    if (top.id <= last) continue;

    if (top.id - last === 1) {
      await handleMessage(channel, top.id, top.message, top.date * 1000, source);
    } else {
      const missed = await client.getMessages(channel, { minId: last, limit: 20 });
      for (const m of missed.sort((a, b) => a.id - b.id)) {
        await handleMessage(channel, m.id, m.message, m.date * 1000, source);
      }
    }
    // Deleted posts leave gaps in ids; never re-fetch them every pass.
    advanceState(channel, top.id);
  }

  lastPollStartedAt = startedAt;
  firstPass = false;

  const took = Date.now() - startedAt;
  stats.polls++;
  stats.pollMsTotal += took;
  stats.pollMsMax = Math.max(stats.pollMsMax, took);

  if (source === 'catchup') {
    log(`startup catch-up done in ${took}ms: ${catchupPosts} post(s) since last run, ${catchupTooOld} too old to push`);
  }
  if (took > SLOW_POLL_MS) log(`slow poll: ${took}ms for ${checked} dialog(s)`);
  return took;
}

async function pollLoop() {
  while (true) {
    let took = 0;
    try {
      took = await pollOnce();
    } catch (error) {
      stats.pollErrors++;
      if (error.seconds) {
        stats.floodWaits++;
        log(`poll: FLOOD WAIT ${error.seconds}s, sleeping (consider raising pollSeconds)`);
        await sleep((error.seconds + 1) * 1000);
      } else {
        log(`poll failed: ${error.message}`);
      }
    }
    await sleep(Math.max(0, POLL_INTERVAL_MS - took));
  }
}

// ---------------------------------------------------------
// CONNECTION HEALTH
// ---------------------------------------------------------

let lastSuccessAt = Date.now();
let lastAlertAt = 0;
let outage = false;

async function pingTelegram() {
  try {
    await withTimeout(client.getMe(), 10 * 1000);
    lastSuccessAt = Date.now();
  } catch (error) {
    log(`telegram ping failed: ${error.message}`);
  }
}

async function checkHealth() {
  const now = Date.now();
  const silence = now - lastSuccessAt;

  // Wedged connection: exit so Docker restarts the container. The first poll
  // pass on startup recovers anything posted in the meantime.
  if (silence >= SELF_RESTART_MS) {
    log('no contact with Telegram for 3+ min, exiting for restart');
    saveState();
    process.exit(1);
  }

  const down = silence >= HEALTH_THRESHOLD_MS;
  if (down && now - lastAlertAt >= HEALTH_COOLDOWN_MS) {
    lastAlertAt = now;
    outage = true;
    const r = await push(allDevices(), { type: 'system', text: "Втрачено з'єднання з Telegram" });
    log(`no contact with Telegram for 1+ min, alert sent to ${r.ok} device(s), ${r.failed} failed`);
  } else if (!down && outage) {
    outage = false;
    const r = await push(allDevices(), { type: 'system', text: "З'єднання з Telegram відновлено" });
    log(`connection restored, notice sent to ${r.ok} device(s), ${r.failed} failed`);
  }
}

// ---------------------------------------------------------
// MAIN
// ---------------------------------------------------------

async function main() {
  await client.connect();
  if (!(await client.checkAuthorization())) {
    console.error('Telegram session is not authorized. Run login.js again.');
    process.exit(1);
  }
  lastSuccessAt = Date.now();

  await loadChannels();
  watchDevices();
  client.addEventHandler(onNewMessage, new NewMessage({}));

  log(`server started: polling every ${POLL_INTERVAL_MS / 1000}s, max message age ${MAX_AGE_MS / 60000} min`);

  setInterval(pingTelegram, PING_INTERVAL_MS);
  setInterval(() => {
    checkHealth().catch((e) => log(`health check failed: ${e.message}`));
  }, 10 * 1000);
  setInterval(saveState, 5 * 1000);
  setInterval(printStats, STATS_INTERVAL_MS);

  pollLoop().catch((e) => {
    log(`poll loop stopped: ${e.message}, exiting for restart`);
    process.exit(1);
  });
}

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => {
    log(`${signal} received, shutting down`);
    try {
      saveState();
    } finally {
      process.exit(0);
    }
  });
}

process.on('unhandledRejection', (error) => {
  log(`unhandled rejection: ${error && error.message ? error.message : error}`);
});

main().catch((error) => {
  console.error('fatal:', error);
  process.exit(1);
});