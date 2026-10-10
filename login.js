'use strict';

// One-time interactive login. Run it on your PC (not in Docker):
//   node login.js        code login (tells you where Telegram sent the code)
//   node login.js sms    code login, force the code to be sent by SMS
//   node login.js qr     QR login: scan with the Telegram app already
//                        signed in to the account (needs: npm i -D qrcode-terminal)
// It writes telegram.json (api id/hash + session string). Treat that file like
// a password: anyone with it has full access to the Telegram account.

const fs = require('node:fs');
const readline = require('node:readline/promises');
const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');

(async () => {
  const mode = process.argv[2] || 'code';
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

  const apiId = Number(await rl.question('api_id (from my.telegram.org): '));
  const apiHash = (await rl.question('api_hash: ')).trim();

  const client = new TelegramClient(new StringSession(''), apiId, apiHash, {
    connectionRetries: 5,
  });
  client.setLogLevel('error');

  const password = () => rl.question('2FA password: ');
  const onError = async (error) => {
    console.error('login error:', error.message);
    return false; // keep going
  };

  if (mode === 'qr') {
    const qrcode = require('qrcode-terminal');
    await client.connect();
    await client.signInUserWithQrCode(
      { apiId, apiHash },
      {
        qrCode: async ({ token }) => {
          console.log('\nOn your phone: Telegram > Settings > Devices > Link Desktop Device, then scan:\n');
          qrcode.generate(`tg://login?token=${token.toString('base64url')}`, { small: true });
          console.log('(the QR refreshes every ~30 s; confirm the login in the app when asked)');
        },
        password,
        onError,
      }
    );
  } else {
    await client.start({
      phoneNumber: () => rl.question('Phone number (+380...): '),
      phoneCode: async (viaApp) => {
        console.log(
          viaApp
            ? 'Telegram says the code was sent INSIDE the Telegram app (chat named "Telegram"), not by SMS.'
            : 'Telegram says the code was sent by SMS (or a call).'
        );
        return rl.question('Code: ');
      },
      forceSMS: mode === 'sms',
      password,
      onError,
    });
  }

  fs.writeFileSync(
    'telegram.json',
    JSON.stringify({ apiId, apiHash, session: client.session.save() }, null, 2)
  );
  console.log('Saved telegram.json');

  await client.disconnect();
  rl.close();
  process.exit(0);
})();