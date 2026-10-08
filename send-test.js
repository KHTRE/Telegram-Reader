'use strict';
const fs = require('node:fs');
const { initializeApp, cert } = require('firebase-admin/app');
const { getMessaging } = require('firebase-admin/messaging');

const config = JSON.parse(fs.readFileSync('config.json', 'utf8'));
initializeApp({
  credential: cert(JSON.parse(fs.readFileSync('service-account.json', 'utf8'))),
});

// getMessaging()
//   .send({
//     topic: config.fcmTopic,
//     data: { type: 'message', channel: 'test', id: '0', text: "Тест. деверзе, дврз, бпла, бепеела", date: '', url: '' },
//     android: { priority: 'high', ttl: 5 * 60 * 1000 },
//   })
// getMessaging()
//   .send({
//     topic: config.fcmTopic,
//     data: { type: 'message', channel: 'test', id: '0', text: "тест два довгий текст для тесту", date: '', url: '' },
//     android: { priority: 'high', ttl: 5 * 60 * 1000 },
//   })
getMessaging()
  .send({
    topic: config.fcmTopic,
    data: { type: 'message', channel: 'test', id: '0', text: "Ну що, гучніше стало?", date: '', url: '' },
    android: { priority: 'high', ttl: 5 * 60 * 1000 },
  })