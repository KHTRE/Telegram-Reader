'use strict';

const fs = require('node:fs');
const path = require('node:path');
const cheerio = require('cheerio');
const { initializeApp, cert } = require('firebase-admin/app');
const { getMessaging } = require('firebase-admin/messaging');

// ---------------------------------------------------------
// CONFIG
// ---------------------------------------------------------

const CONFIG_PATH =
  process.env.CONFIG_PATH || path.join(__dirname, 'config.json');
const SERVICE_ACCOUNT_PATH =
  process.env.SERVICE_ACCOUNT_PATH || path.join(__dirname, 'service-account.json');
const STATE_PATH =
  process.env.STATE_PATH || path.join(__dirname, 'state.json');

const config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
const lower = (list) => (list || []).map((w) => w.toLowerCase());

const CHANNELS = config.channels || [];
// Deliver to a topic the app subscribes to (no token copying needed).
// fcmTokens is optional and still works for direct testing.
const TARGETS = [
  ...(config.fcmTopic ? [{ topic: config.fcmTopic }] : []),
  ...(config.fcmTokens || []).map((token) => ({ token })),
];
const FILTERING_ON = config.filteringOn;
const MAX_MESSAGE_LENGTH = config.messageLength;
const POLL_INTERVAL = config.pollInterval || 15000;
// Messages older than this are never pushed (protects against stale
// alerts after a restart or a long outage).
const MAX_AGE_MS = (config.maxAgeMinutes ?? 10) * 60 * 1000;
const SEARCH_FOR = lower(config.searchFor);
const EXCLUDE_FROM_SEARCH = lower(config.excludeFromSearch);
const DEVERZE = 'деверзе';

const HEALTH_THRESHOLD_MS = 60 * 1000; // no successful fetch for 1 min
const HEALTH_COOLDOWN_MS = 10 * 60 * 1000; // at most one alert per 10 min
const PUSH_TTL_MS = 5 * 60 * 1000; // FCM drops undelivered alerts after 5 min

const log = (...args) => console.log(new Date().toISOString(), ...args);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

if (CHANNELS.length === 0 || TARGETS.length === 0) {
  console.error('config.json needs at least one channel and an fcmTopic (or fcmTokens)');
  process.exit(1);
}

initializeApp({
  credential: cert(JSON.parse(fs.readFileSync(SERVICE_ACCOUNT_PATH, 'utf8'))),
});
const messaging = getMessaging();

// ---------------------------------------------------------
// STATE (last seen message id per channel, persisted)
// ---------------------------------------------------------

let state = {};
try {
  state = JSON.parse(fs.readFileSync(STATE_PATH, 'utf8'));
} catch {
  // first run
}

function saveState() {
  const tmp = `${STATE_PATH}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(state));
  fs.renameSync(tmp, STATE_PATH);
}

// ---------------------------------------------------------
// FETCH + PARSE
// ---------------------------------------------------------

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: {
      'User-Agent':
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 Chrome/109 Safari/537.36',
    },
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) throw new Error(`Telegram returned ${res.status}`);
  return res.text();
}

function parseMessages(html) {
  const $ = cheerio.load(html);
  const messages = [];

  $('.tgme_widget_message').each((_, element) => {
    const message = $(element);

    const id = Number((message.attr('data-post') || '').split('/').pop());
    if (!id) return;

    const textElement = message.find('.js-message_text');
    textElement.find('br').replaceWith('. ');

    let text = textElement
      .text()
      .replace(/\//g, ' ')
      .toLowerCase()
      .replace(/дврз/gu, DEVERZE)
      .replace(/бпла/gu, 'бепеела')
      .replace(/[^\p{L}\p{N}\p{P}\p{Z}]/gu, '')
      .replace(/\s+/g, ' ')
      .trim();
    if (text) text += '. ';

    const rawDate = message.find('time').attr('datetime');
    const ts = Date.parse(rawDate);
    const date = Number.isFinite(ts)
      ? new Date(ts).toLocaleString('uk-UA', { timeZone: 'Europe/Kyiv', hour12: false })
      : '';

    const url = message.find('.tgme_widget_message_date').attr('href') || '';

    messages.push({ id, text, ts, date, url });
  });

  return messages;
}

// ---------------------------------------------------------
// FILTER
// ---------------------------------------------------------

function shouldPush(message) {
  if (!message.text) return false; // photo-only posts etc.

  if (Number.isFinite(message.ts) && Date.now() - message.ts > MAX_AGE_MS) {
    return false;
  }

  if (!FILTERING_ON) return true;

  if (MAX_MESSAGE_LENGTH > 0 && message.text.length > MAX_MESSAGE_LENGTH) {
    return false;
  }
  if (EXCLUDE_FROM_SEARCH.some((word) => message.text.includes(word))) {
    return false;
  }
  if (SEARCH_FOR.length > 0 && !SEARCH_FOR.some((word) => message.text.includes(word))) {
    return false;
  }
  return true;
}

// ---------------------------------------------------------
// PUSH
// ---------------------------------------------------------

// All FCM data values must be strings.
async function push(data) {
  await Promise.all(
    TARGETS.map(async (target) => {
      try {
        await messaging.send({
          ...target,
          data,
          android: { priority: 'high', ttl: PUSH_TTL_MS },
        });
      } catch (error) {
        const name = target.topic || `${target.token.slice(0, 8)}…`;
        log(`FCM error for ${name}: ${error.code} ${error.message}`);
      }
    })
  );
}

// ---------------------------------------------------------
// CHECK ONE CHANNEL
// ---------------------------------------------------------

let lastSuccessAt = Date.now();
const warnedEmpty = new Set();

async function checkChannel(channel) {
  const messages = parseMessages(await fetchHtml(`https://t.me/s/${channel}`));
  lastSuccessAt = Date.now();

  if (messages.length === 0) {
    if (!warnedEmpty.has(channel)) {
      warnedEmpty.add(channel);
      log(`[${channel}] no messages parsed (preview disabled or markup changed?)`);
    }
    return;
  }
  warnedEmpty.delete(channel);

  const latest = Math.max(...messages.map((m) => m.id));

  // First ever run for this channel: remember position, push nothing.
  if (state[channel] === undefined) {
    state[channel] = latest;
    saveState();
    log(`[${channel}] connected, latest id ${latest}`);
    return;
  }

  const fresh = messages
    .filter((m) => m.id > state[channel])
    .sort((a, b) => a.id - b.id);

  for (const message of fresh) {
    if (!shouldPush(message)) continue;
    log(`[${channel}] push #${message.id}: ${message.text}`);
    await push({
      type: 'message',
      channel,
      id: String(message.id),
      text: message.text,
      date: message.date,
      url: message.url,
    });
  }

  if (fresh.length > 0) {
    state[channel] = Math.max(state[channel], latest);
    saveState();
  }
}

// ---------------------------------------------------------
// CONNECTION HEALTH (alerts the phone if Telegram stops answering)
// ---------------------------------------------------------

let lastAlertAt = 0;
let outage = false;

async function checkHealth() {
  const now = Date.now();
  const down = now - lastSuccessAt >= HEALTH_THRESHOLD_MS;

  if (down && now - lastAlertAt >= HEALTH_COOLDOWN_MS) {
    lastAlertAt = now;
    outage = true;
    log('no successful channel check for 1+ min, alerting phone');
    await push({ type: 'system', text: "Втрачено з'єднання з Telegram" });
  } else if (!down && outage) {
    outage = false;
    log('connection restored');
    await push({ type: 'system', text: "З'єднання з Telegram відновлено" });
  }
}

// ---------------------------------------------------------
// MAIN
// ---------------------------------------------------------

async function main() {
  log(`channels: ${CHANNELS.map((c) => `@${c}`).join(', ')}`);
  log(`poll every ${POLL_INTERVAL / 1000}s, filtering ${FILTERING_ON ? 'on' : 'off'}`);

  setInterval(() => {
    checkHealth().catch((e) => log(`health check failed: ${e.message}`));
  }, 10 * 1000);

  while (true) {
    for (const channel of CHANNELS) {
      try {
        await checkChannel(channel);
      } catch (error) {
        log(`[${channel}] check failed: ${error.message}`);
      }
    }
    await sleep(POLL_INTERVAL + Math.random() * 1000);
  }
}

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => {
    try {
      saveState();
    } finally {
      process.exit(0);
    }
  });
}

main();