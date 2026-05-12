// ═══════════════════════════════════════════════
//  Vault Chat — Backend Server
//  Handles: Chat rooms, messages, WebRTC calls
//
//  Setup:
//    npm install ws
//    node server.js
// ═══════════════════════════════════════════════

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 3001;

// rooms: Map<roomId, Map<ws, userId>>
const rooms = new Map();

// ── HTTP server (needed for Render/Railway hosting) ──
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Vault Chat Server OK');
});

const wss = new WebSocketServer({ server });

// ── Helpers ──
function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcast(roomId, obj, exclude) {
  const room = rooms.get(roomId);
  if (!room) return;
  const str = JSON.stringify(obj);
  room.forEach((uid, ws) => {
    if (ws !== exclude && ws.readyState === WebSocket.OPEN) {
      ws.send(str);
    }
  });
}

function getPeer(roomId, selfWs) {
  const room = rooms.get(roomId);
  if (!room) return null;
  for (const [ws] of room) {
    if (ws !== selfWs && ws.readyState === WebSocket.OPEN) return ws;
  }
  return null;
}

// ── WebSocket connection handler ──
wss.on('connection', ws => {
  ws._room = null;
  ws._uid = null;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // ─── Room management ───────────────────────────
      case 'create_room': {
        const { roomId, userId } = msg;
        if (!roomId || !userId) return;
        if (rooms.has(roomId)) {
          send(ws, { type: 'create_error', message: 'Room already exists.' });
          return;
        }
        rooms.set(roomId, new Map([[ws, userId]]));
        ws._room = roomId;
        ws._uid = userId;
        send(ws, { type: 'room_created', roomId });
        console.log(`[+] Room created: ${roomId} by ${userId}`);
        break;
      }

      case 'join_room': {
        const { roomId, userId } = msg;
        if (!roomId || !userId) return;
        const room = rooms.get(roomId);
        if (!room) {
          send(ws, { type: 'join_error', message: 'Room not found. Check the ID and try again.' });
          return;
        }
        if (room.size >= 2) {
          send(ws, { type: 'join_error', message: 'Room is full (max 2 people).' });
          return;
        }
        room.set(ws, userId);
        ws._room = roomId;
        ws._uid = userId;
        send(ws, { type: 'joined_ok', roomId });
        broadcast(roomId, { type: 'peer_joined' }, ws);
        console.log(`[+] ${userId} joined room ${roomId}`);
        break;
      }

      case 'leave_room':
        handleLeave(ws);
        break;

      // ─── Chat messages ──────────────────────────────
      case 'message': {
        const { roomId, text, time, id, msgType } = msg;
        if (!roomId || text === undefined) return;
        broadcast(roomId, { type: 'message', text, time, id, msgType }, ws);
        break;
      }

      case 'seen': {
        const { roomId, msgId } = msg;
        if (roomId && msgId) broadcast(roomId, { type: 'seen', msgId }, ws);
        break;
      }

      // ─── WebRTC signalling ──────────────────────────
      // Server just forwards these between the two peers

      case 'call_request': {
        const peer = getPeer(msg.roomId, ws);
        if (peer) send(peer, { type: 'call_incoming', callType: msg.callType });
        break;
      }
      case 'call_accepted': {
        const peer = getPeer(msg.roomId, ws);
        if (peer) send(peer, { type: 'call_accepted' });
        break;
      }
      case 'call_rejected': {
        const peer = getPeer(msg.roomId, ws);
        if (peer) send(peer, { type: 'call_rejected' });
        break;
      }
      case 'webrtc_offer': {
        const peer = getPeer(msg.roomId, ws);
        if (peer) send(peer, { type: 'webrtc_offer', sdp: msg.sdp, callType: msg.callType });
        break;
      }
      case 'webrtc_answer': {
        const peer = getPeer(msg.roomId, ws);
        if (peer) send(peer, { type: 'webrtc_answer', sdp: msg.sdp });
        break;
      }
      case 'webrtc_ice': {
        const peer = getPeer(msg.roomId, ws);
        if (peer) send(peer, { type: 'webrtc_ice', candidate: msg.candidate });
        break;
      }
      case 'webrtc_end': {
        const peer = getPeer(msg.roomId, ws);
        if (peer) send(peer, { type: 'webrtc_end' });
        break;
      }
    }
  });

  ws.on('close', () => handleLeave(ws));
  ws.on('error', err => { console.error('WS error:', err.message); handleLeave(ws); });
});

function handleLeave(ws) {
  const roomId = ws._room;
  if (!roomId) return;
  const room = rooms.get(roomId);
  if (room) {
    room.delete(ws);
    broadcast(roomId, { type: 'peer_left' });
    if (room.size === 0) {
      rooms.delete(roomId);
      console.log(`[-] Room ${roomId} deleted (empty)`);
    }
  }
  ws._room = null;
  ws._uid = null;
}

// Clean up empty rooms every hour
setInterval(() => {
  rooms.forEach((room, id) => {
    if (room.size === 0) rooms.delete(id);
  });
}, 3_600_000);

server.listen(PORT, () => {
  console.log(`\n🔐 Vault Chat Server`);
  console.log(`   Port     : ${PORT}`);
  console.log(`   WebSocket: ws://localhost:${PORT}`);
  console.log(`   Ready for connections\n`);
});
