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
  role?: 'master' | 'client' | 'MASTER' | 'CLIENT';
  senderId?: string;
  nodeId?: string;
  peerId?: string;
  clientId?: string;
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

  // ============================================================================
  // 1. LOGIKA SIGNALING SERVER (NODE.JS / WEBSOCKET) — DYNAMIC ROLE ASSIGNMENT
  // ============================================================================
  const wss = new WebSocketServer({ noServer: true });
  const clientsMap = new Map<string, TrackedSocket>();
  const masterSockets = new Set<WebSocket>();

  // Variabel pelacak status Master tunggal
  let masterId: string | null = null;
  let masterSocket: WebSocket | null = null;

  // In-memory HTTP signaling message queue fallback
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

  const sendSafe = (targetWs: WebSocket | null | undefined, payload: unknown) => {
    if (targetWs && targetWs.readyState === WebSocket.OPEN) {
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
    const senderId = String(
      msg.senderId || msg.nodeId || msg.peerId || currentId || ''
    );

    // 1. Registrasi eksplisit Master
    if (
      msg.role === 'master' ||
      msg.role === 'MASTER' ||
      msg.type === 'register_master'
    ) {
      masterLastSeenAt = Date.now();
      if (!masterId) {
        masterId = senderId || 'master';
      }
      if (sourceWs) {
        masterSocket = sourceWs;
        masterSockets.add(sourceWs);
        clientsMap.set('master', { ws: sourceWs, role: 'master', id: masterId });
      }

      const onlineNotice: SignalingMessage = {
        type: 'master_online',
        masterId,
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
    if (
      msg.role === 'client' ||
      msg.role === 'CLIENT' ||
      msg.type === 'register_client'
    ) {
      if (senderId && sourceWs) {
        clientsMap.set(senderId, { ws: sourceWs, role: 'client', id: senderId });
      }
      if (masterSockets.size > 0 || Date.now() - masterLastSeenAt < 15000) {
        const notice: SignalingMessage = {
          type: 'master_online',
          masterId: masterId || 'master',
          timestamp: Date.now(),
        };
        if (sourceWs) sendSafe(sourceWs, notice);
        if (senderId) enqueueMailbox(senderId, notice);
      }
      return;
    }

    // 3. Routing Pesan ke Master (`target: "master"`)
    if (msg.target === 'master' || (masterId && msg.target === masterId)) {
      const enriched: SignalingMessage = {
        ...msg,
        senderId: senderId || msg.senderId,
      };
      masterSockets.forEach((mWs) => {
        sendSafe(mWs, enriched);
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
    let currentId = String(Math.floor(1000 + Math.random() * 9000));
    let currentRole: 'master' | 'client' | 'unknown' = 'unknown';

    ws.on('message', (rawBuffer) => {
      let msg: SignalingMessage | null = null;
      try {
        msg = JSON.parse(rawBuffer.toString()) as SignalingMessage;
      } catch {
        return;
      }
      if (!msg || typeof msg !== 'object') return;

      const requestedId = String(
        msg.peerId || msg.senderId || msg.nodeId || currentId
      );
      if (requestedId) {
        currentId = requestedId;
      }

      // ========================================================================
      // DYNAMIC ROLE ASSIGNMENT HANDLER (`type: "REQUEST_ROLE"`)
      // ========================================================================
      if (msg.type === 'REQUEST_ROLE') {
        // Bersihkan socket Master yang sudah tidak aktif
        masterSockets.forEach((s) => {
          if (s.readyState !== WebSocket.OPEN) {
            masterSockets.delete(s);
          }
        });

        const isMasterAlive = masterId !== null && masterSockets.size > 0;

        if (!isMasterAlive) {
          // a. Jika masterId masih kosong, tetapkan peer ini sebagai MASTER
          masterId = currentId;
          masterSocket = ws;
          currentRole = 'master';
          masterSockets.add(ws);
          clientsMap.set(currentId, { ws, role: 'master', id: currentId });

          sendSafe(ws, {
            type: 'ROLE_ASSIGNED',
            role: 'MASTER',
            peerId: currentId,
            masterId: currentId,
          });
        } else {
          // b. Jika masterId sudah ada, tetapkan peer baru ini sebagai CLIENT
          currentRole = 'client';
          clientsMap.set(currentId, { ws, role: 'client', id: currentId });

          sendSafe(ws, {
            type: 'ROLE_ASSIGNED',
            role: 'CLIENT',
            peerId: currentId,
            masterId,
          });

          // Teruskan info Klien baru ini ke Master
          masterSockets.forEach((mWs) => {
            sendSafe(mWs, {
              type: 'NEW_CLIENT_JOINED',
              clientId: currentId,
              senderId: currentId,
              timestamp: Date.now(),
            });
          });
        }
        return;
      }

      if (msg.role === 'master' || msg.type === 'register_master') {
        currentRole = 'master';
        masterSocket = ws;
        masterSockets.add(ws);
        if (!masterId) {
          masterId = currentId;
        }
      } else if (currentRole !== 'master') {
        currentRole = 'client';
        clientsMap.set(currentId, { ws, role: 'client', id: currentId });
      }

      routeSignalingMessage(msg, ws, currentRole, currentId);
    });

    ws.on('close', () => {
      if (currentRole === 'master') {
        masterSockets.delete(ws);
      }

      // Jika seluruh koneksi Master terputus (disconnect), reset masterId = null
      // agar perangkat lain yang pertama kali merefresh bisa mengambil alih posisi Master
      if (masterSockets.size === 0 && (ws === masterSocket || currentRole === 'master')) {
        masterId = null;
        masterSocket = null;
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
            clientId: currentId,
          };
          masterSockets.forEach((mWs) => {
            sendSafe(mWs, disconnectMsg);
          });
          enqueueMailbox('master', disconnectMsg);
        }
      }
    });

    ws.on('error', () => {});
  });

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
    res.json({
      ok: true,
      masterId,
      masterOnline: masterSockets.size > 0 || Date.now() - masterLastSeenAt < 15000,
    });
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
      masterId,
      masterOnline: masterSockets.size > 0 || Date.now() - masterLastSeenAt < 15000,
    });
  });

  app.get('/api/health', (_req, res) => {
    res.json({
      status: 'ok',
      masterId,
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
    console.log(`S.I.G.M.A. Universal Dynamic Role Server listening on http://0.0.0.0:${PORT}`);
  });
}

startServer();
