require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/stt' });

const DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY;
if (!DEEPGRAM_API_KEY) {
  console.error('Missing DEEPGRAM_API_KEY in .env — the server will start but streaming will fail.');
}

// Nova-3, Egyptian Arabic dialect, interim results on (needed for "fast" mode),
// numerals=true asks Deepgram to return spoken numbers as digits directly.
// We stream raw 16kHz mono PCM from the browser (not WebM/Opus chunks —
// MediaRecorder's timesliced chunks aren't independently decodable, which
// silently breaks streaming after the first chunk).
//
// Keyterm Prompting boosts recognition of the exact vocabulary this app
// depends on: both the formal Arabic letter names AND the colloquial
// Egyptian variants actually observed in production logs (e.g. "حا" for
// حاء, "طه" for طاء, "به" for باء) — real model fine-tuning is an
// Enterprise-only feature (~$10k, needs 10-30hrs of labeled audio), so
// this free self-serve route is the practical lever available to us.
const PLATE_LETTER_KEYTERMS = [
  // formal names
  'حاء','باء','طاء','قاف','كاف','لام','ميم','نون','هاء','واو','ياء',
  'الف','دال','راء','سين','صاد','عين',
  // observed colloquial/shortened Egyptian pronunciations
  'حا','طه','به','يه','نو','مي','كا','لا'
];
const baseParams =
  'model=nova-3' +
  '&language=ar-EG' +
  '&interim_results=true' +
  '&numerals=true' +
  '&endpointing=300' +
  '&punctuate=false' +
  '&smart_format=false' +
  '&encoding=linear16' +
  '&sample_rate=16000';
const keytermParams = PLATE_LETTER_KEYTERMS
  .map(term => `keyterm=${encodeURIComponent(term)}`)
  .join('&');
const DEEPGRAM_URL = `wss://api.deepgram.com/v1/listen?${baseParams}&${keytermParams}`;

app.get('/health', (req, res) => res.json({ ok: true }));

wss.on('connection', (clientWs) => {
  console.log('Browser connected');

  const dgSocket = new WebSocket(DEEPGRAM_URL, {
    headers: { Authorization: `Token ${DEEPGRAM_API_KEY}` },
  });

  let dgReady = false;
  const pending = [];

  dgSocket.on('open', () => {
    dgReady = true;
    console.log('Connected to Deepgram');
    // flush any audio chunks that arrived before Deepgram finished connecting
    while (pending.length) dgSocket.send(pending.shift());
  });

  dgSocket.on('message', (data) => {
    // forward Deepgram's transcript JSON straight through to the browser
    const text = data.toString();
    try{
      const parsed = JSON.parse(text);
      console.log('Deepgram message type:', parsed.type, '| transcript:', parsed.channel?.alternatives?.[0]?.transcript);
    }catch(e){ console.log('Deepgram non-JSON message'); }
    if (clientWs.readyState === WebSocket.OPEN) clientWs.send(text);
  });

  dgSocket.on('error', (err) => {
    console.error('Deepgram socket error:', err.message);
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(JSON.stringify({ error: 'deepgram_error', message: err.message }));
    }
  });

  dgSocket.on('close', (code, reason) => {
    console.log('Deepgram socket closed', code, reason.toString());
    if (clientWs.readyState === WebSocket.OPEN) clientWs.close();
  });

  clientWs.on('message', (audioChunk) => {
    if (dgReady && dgSocket.readyState === WebSocket.OPEN) {
      dgSocket.send(audioChunk);
    } else {
      pending.push(audioChunk);
    }
  });

  clientWs.on('close', () => {
    console.log('Browser disconnected');
    if (dgSocket.readyState === WebSocket.OPEN || dgSocket.readyState === WebSocket.CONNECTING) {
      dgSocket.close();
    }
  });

  clientWs.on('error', (err) => console.error('Client socket error:', err.message));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Relay server listening on port ${PORT}`));
