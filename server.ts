import express from 'express';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer as createViteServer } from 'vite';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

interface SignalingMessage {
  type: string;
  role?: 'master' | 'client';
  senderId?: string;
  nodeId?: string;
  target?: string;
  sdp?: unknown;
  candidate?: unknown;
  [key: string]: unknown;
}

interface TrackedSocket {
  ws: WebSocket;
  role: 'master' | 'client' | 'unknown';
  id: string;
}

async function startServer() {
  const app = express();
  app.use(express.json({ limit: '1mb' }));

  const server = http.createServer(app);
  const PORT = Number(process.env.PORT) || 3000;

  // WebSocket Signaling Server for 1-Master to Multi-Client (up to 5 Clients) Auto-Discovery
  const wss = new WebSocketServer({ noServer: true });
  const clientsMap = new Map<string, TrackedSocket>();
  const masterSockets = new Set<WebSocket>();

  // In-memory HTTP signaling message queue fallback (for networks blocking WS upgrades)
  const httpMailboxes = new Map<string, SignalingMessage[]>();
  let masterLastSeenAt = 0;

  const enqueueMailbox = (targetId: string, msg: SignalingMessage) => {
    const queue = httpMailboxes.get(targetId) || [];
    queue.push(msg);
    if (queue.length > 50) queue.shift();
    httpMailboxes.set(targetId, queue);
  };

  server.on('upgrade', (request, socket, head) => {
    const protocolHeader = request.headers['sec-websocket-protocol'] || '';
    const reqUrl = request.url || '/';

    // Handle Vite HMR WebSocket gracefully if an old client tab requests it
    if (protocolHeader.includes('vite-hmr')) {
      wss.handleUpgrade(request, socket, head, (ws) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'connected' }));
        }
      });
      return;
    }

    if (reqUrl.startsWith('/ws') || reqUrl.startsWith('/signaling') || reqUrl === '/') {
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
      });
    } else {
      socket.destroy();
    }
  });

  const sendSafe = (targetWs: WebSocket, payload: unknown) => {
    if (targetWs.readyState === WebSocket.OPEN) {
      try {
        targetWs.send(JSON.stringify(payload));
      } catch {}
    }
  };

  const routeSignalingMessage = (
    msg: SignalingMessage,
    sourceWs?: WebSocket,
    currentRole?: 'master' | 'client' | 'unknown',
    currentId?: string
  ) => {
    const senderId = String(msg.senderId || msg.nodeId || currentId || '');

    // 1. Registrasi Master
    if (msg.role === 'master' || msg.type === 'register_master') {
      masterLastSeenAt = Date.now();
      if (sourceWs) {
        masterSockets.add(sourceWs);
        clientsMap.set('master', { ws: sourceWs, role: 'master', id: 'master' });
      }

      // Beritahu semua Klien yang sedang standby agar langsung mengirim Offer
      const onlineNotice: SignalingMessage = {
        type: 'master_online',
        timestamp: Date.now(),
      };
      clientsMap.forEach((entry) => {
        if (entry.role === 'client') {
          sendSafe(entry.ws, onlineNotice);
          enqueueMailbox(entry.id, onlineNotice);
        }
      });
      return;
    }

    // 2. Registrasi Klien Pasif
    if (msg.role === 'client' || msg.type === 'register_client') {
      if (senderId && sourceWs) {
        clientsMap.set(senderId, { ws: sourceWs, role: 'client', id: senderId });
      }
      if (masterSockets.size > 0 || Date.now() - masterLastSeenAt < 15000) {
        const notice: SignalingMessage = { type: 'master_online', timestamp: Date.now() };
        if (sourceWs) sendSafe(sourceWs, notice);
        if (senderId) enqueueMailbox(senderId, notice);
      }
      return;
    }

    // 3. Routing Pesan ke Master (`target: "master"`)
    if (msg.target === 'master') {
      const enriched: SignalingMessage = {
        ...msg,
        senderId: senderId || msg.senderId,
      };
      masterSockets.forEach((masterWs) => {
        sendSafe(masterWs, enriched);
      });
      enqueueMailbox('master', enriched);
      return;
    }

    // 4. Routing Pesan dari Master ke Klien tertentu (`target: "<senderId>"`)
    if (msg.target && msg.target !== 'master') {
      const targetKey = String(msg.target);
      const enriched: SignalingMessage = {
        ...msg,
        senderId: senderId || 'master',
      };
      const targetClient = clientsMap.get(targetKey);
      if (targetClient) {
        sendSafe(targetClient.ws, enriched);
      }
      enqueueMailbox(targetKey, enriched);
      return;
    }

    // 5. Broadcast dari Master ke semua Klien
    if (currentRole === 'master' && !msg.target) {
      clientsMap.forEach((entry) => {
        if (entry.role === 'client') {
          sendSafe(entry.ws, msg);
          enqueueMailbox(entry.id, msg);
        }
      });
    }
  };

  wss.on('connection', (ws: WebSocket) => {
    let currentId = '';
    let currentRole: 'master' | 'client' | 'unknown' = 'unknown';

    ws.on('message', (rawBuffer) => {
      let msg: SignalingMessage | null = null;
      try {
        msg = JSON.parse(rawBuffer.toString()) as SignalingMessage;
      } catch {
        return;
      }
      if (!msg || typeof msg !== 'object') return;

      const senderId = String(msg.senderId || msg.nodeId || currentId || '');
      if (senderId && !currentId) {
        currentId = senderId;
      }
      if (msg.role === 'master' || msg.type === 'register_master') {
        currentRole = 'master';
        currentId = 'master';
      } else if (senderId && currentRole !== 'master') {
        currentRole = 'client';
        currentId = senderId;
        clientsMap.set(senderId, { ws, role: 'client', id: senderId });
      }

      routeSignalingMessage(msg, ws, currentRole, currentId);
    });

    ws.on('close', () => {
      if (currentRole === 'master') {
        masterSockets.delete(ws);
      }
      if (currentId) {
        const existing = clientsMap.get(currentId);
        if (existing && existing.ws === ws) {
          clientsMap.delete(currentId);
        }
        if (currentRole === 'client') {
          const disconnectMsg: SignalingMessage = {
            type: 'client_disconnected',
            senderId: currentId,
          };
          masterSockets.forEach((masterWs) => {
            sendSafe(masterWs, disconnectMsg);
          });
          enqueueMailbox('master', disconnectMsg);
        }
      }
    });

    ws.on('error', () => {
      // Abaikan error socket prematur agar server tetap stabil
    });
  });

  // HTTP Signaling Fallback Endpoints (agar 100% bebas error jika proxy menutup WebSocket)
  app.post('/api/signal', (req, res) => {
    const msg = req.body as SignalingMessage;
    if (!msg || typeof msg !== 'object') {
      res.status(400).json({ ok: false });
      return;
    }
    const role = msg.role === 'master' || msg.type === 'register_master' ? 'master' : 'client';
    const id = String(msg.senderId || msg.nodeId || (role === 'master' ? 'master' : ''));
    if (role === 'master') {
      masterLastSeenAt = Date.now();
    }
    routeSignalingMessage(msg, undefined, role, id);
    res.json({ ok: true, masterOnline: masterSockets.size > 0 || Date.now() - masterLastSeenAt < 15000 });
  });

  app.get('/api/signal/:targetId', (req, res) => {
    const targetId = req.params.targetId;
    if (targetId === 'master') {
      masterLastSeenAt = Date.now();
    }
    const messages = httpMailboxes.get(targetId) || [];
    httpMailboxes.set(targetId, []);
    res.json({
      ok: true,
      messages,
      masterOnline: masterSockets.size > 0 || Date.now() - masterLastSeenAt < 15000,
    });
  });

  app.get('/api/health', (_req, res) => {
    res.json({
      status: 'ok',
      mastersOnline: masterSockets.size,
      clientsOnline: Array.from(clientsMap.values()).filter((c) => c.role === 'client').length,
    });
  });

  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: {
        middlewareMode: true,
        hmr: false,
      },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(__dirname, 'dist');
    app.use(express.static(distPath));
    app.get('*', (_req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`S.I.G.M.A. Signaling & Telemetry Server listening on http://0.0.0.0:${PORT}`);
  });
}

startServer();
