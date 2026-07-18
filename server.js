require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: '/stt' });

// Allow the frontend (hosted on a different domain) to call /transcribe.
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

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
const keytermParams = PLATE_LETTER_KEYTERMS
  .map(term => `keyterm=${encodeURIComponent(term)}`)
  .join('&');

// Shared recognition settings for both modes.
const sharedParams =
  'model=nova-3' +
  '&language=ar-EG' +
  '&numerals=true' +
  '&punctuate=false' +
  '&smart_format=false';

// Streaming (WebSocket): we send raw 16kHz mono PCM (not WebM/Opus chunks —
// MediaRecorder's timesliced chunks aren't independently decodable, which
// silently breaks streaming after the first chunk), so encoding must be
// declared explicitly.
const baseParams = sharedParams + '&interim_results=true&endpointing=300&encoding=linear16&sample_rate=16000';
const DEEPGRAM_URL = `wss://api.deepgram.com/v1/listen?${baseParams}&${keytermParams}`;

// Batch (HTTP upload): the browser uploads a complete, valid WebM/Opus
// file, so Deepgram auto-detects the container — no encoding params needed.
const batchParams = sharedParams;


app.get('/health', (req, res) => res.json({ ok: true }));

// Batch transcription: the browser records one complete plate utterance
// (silence-detected), uploads it whole, and we send it to Deepgram's
// pre-recorded endpoint — which sees the full audio at once instead of
// guessing word-by-word, and is noticeably more accurate for this kind of
// short, spelled-out speech than live streaming is.
app.post('/transcribe', express.raw({ type: '*/*', limit: '10mb' }), async (req, res) => {
  try {
    const contentType = req.headers['content-type'] || 'audio/webm';
    const url = `https://api.deepgram.com/v1/listen?${batchParams}&${keytermParams}`;
    const dgRes = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Token ${DEEPGRAM_API_KEY}`,
        'Content-Type': contentType,
      },
      body: req.body,
    });
    const data = await dgRes.json();
    if (!dgRes.ok) {
      console.error('Deepgram batch error:', JSON.stringify(data));
      return res.status(502).json({ error: 'deepgram_error', message: data.err_msg || 'transcription failed' });
    }
    const transcript = data.results?.channels?.[0]?.alternatives?.[0]?.transcript || '';
    console.log('Batch transcript:', transcript);
    res.json({ transcript });
  } catch (err) {
    console.error('Transcribe endpoint error:', err.message);
    res.status(500).json({ error: 'server_error', message: err.message });
  }
});

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
