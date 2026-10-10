'use strict';

// Joins every channel from config.json that the account has not joined yet.
// Slow on purpose (Telegram punishes fast mass-joining). Safe to re-run.
//   node join.js

const fs = require('node:fs');
const path = require('node:path');
const { TelegramClient, Api } = require('telegram');
const { StringSession } = require('telegram/sessions');

const CONFIG_PATH = process.env.CONFIG_PATH || path.join(__dirname, 'config.json');
const TELEGRAM_PATH = process.env.TELEGRAM_PATH || path.join(__dirname, 'telegram.json');
const JOIN_DELAY_MS = 30 * 1000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

(async () => {
  const config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
  const tg = JSON.parse(fs.readFileSync(TELEGRAM_PATH, 'utf8'));
  const wanted = (config.channels || []).map((c) => c.toLowerCase());

  const client = new TelegramClient(new StringSession(tg.session), tg.apiId, tg.apiHash, {
    connectionRetries: 5,
  });
  client.setLogLevel('error');
  await client.connect();

  const joined = new Set();
  for (const dialog of await client.getDialogs({ limit: 500 })) {
    const username = dialog.entity && dialog.entity.username;
    if (username) joined.add(username.toLowerCase());
  }

  const todo = wanted.filter((c) => !joined.has(c));
  console.log(`${wanted.length - todo.length} already joined, ${todo.length} to join`);

  for (const username of todo) {
    for (;;) {
      try {
        const channel = await client.getInputEntity(username);
        await client.invoke(new Api.channels.JoinChannel({ channel }));
        console.log(`joined @${username}`);
        break;
      } catch (error) {
        if (error.seconds) {
          console.log(`flood wait ${error.seconds}s, sleeping...`);
          await sleep((error.seconds + 1) * 1000);
          continue;
        }
        console.error(`@${username}: ${error.errorMessage || error.message}`);
        break;
      }
    }
    await sleep(JOIN_DELAY_MS + Math.random() * 5000);
  }

  console.log('done');
  await client.disconnect();
  process.exit(0);
})();
