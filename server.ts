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
  const server = http.createServer(app);
  const PORT = Number(process.env.PORT) || 3000;

  // WebSocket Signaling Server for 1-Master to Multi-Client (up to 5 Clients) Auto-Discovery
  const wss = new WebSocketServer({ noServer: true });
  const clientsMap = new Map<string, TrackedSocket>();
  const masterSockets = new Set<WebSocket>();

  server.on('upgrade', (request, socket, head) => {
    const reqUrl = request.url || '/';
    if (reqUrl.startsWith('/ws') || reqUrl.startsWith('/signaling')) {
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
      });
    }
  });

  wss.on('connection', (ws: WebSocket) => {
    let currentId = '';
    let currentRole: 'master' | 'client' | 'unknown' = 'unknown';

    const sendSafe = (targetWs: WebSocket, payload: unknown) => {
      if (targetWs.readyState === WebSocket.OPEN) {
        try {
          targetWs.send(JSON.stringify(payload));
        } catch {}
      }
    };

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

      // 1. Registrasi Master
      if (msg.role === 'master' || msg.type === 'register_master') {
        currentRole = 'master';
        currentId = senderId || 'master';
        masterSockets.add(ws);
        clientsMap.set(currentId, { ws, role: 'master', id: currentId });

        // Beritahu semua Klien yang sedang standby agar langsung mengirim Offer
        clientsMap.forEach((entry) => {
          if (entry.role === 'client') {
            sendSafe(entry.ws, {
              type: 'master_online',
              timestamp: Date.now(),
            });
          }
        });

        sendSafe(ws, {
          type: 'master_registered',
          activeClients: Array.from(clientsMap.values())
            .filter((c) => c.role === 'client')
            .map((c) => c.id),
        });
        return;
      }

      // 2. Registrasi Klien Pasif
      if (msg.role === 'client' || msg.type === 'register_client') {
        currentRole = 'client';
        if (senderId) {
          currentId = senderId;
          clientsMap.set(senderId, { ws, role: 'client', id: senderId });
        }
        if (masterSockets.size > 0) {
          sendSafe(ws, { type: 'master_online', timestamp: Date.now() });
        }
        return;
      }

      // Simpan referensi socket Klien jika mengirim offer/candidate langsung
      if (senderId && currentRole !== 'master') {
        currentRole = 'client';
        currentId = senderId;
        clientsMap.set(senderId, { ws, role: 'client', id: senderId });
      }

      // 3. Routing Pesan ke Master (`target: "master"`)
      if (msg.target === 'master') {
        masterSockets.forEach((masterWs) => {
          sendSafe(masterWs, {
            ...msg,
            senderId: currentId || msg.senderId,
          });
        });
        return;
      }

      // 4. Routing Pesan dari Master ke Klien tertentu (`target: "<senderId>"`)
      if (msg.target && msg.target !== 'master') {
        const targetClient = clientsMap.get(String(msg.target));
        if (targetClient) {
          sendSafe(targetClient.ws, {
            ...msg,
            senderId: currentId || 'master',
          });
        } else {
          // Fallback broadcast ke semua klien jika ID cocok sebagian
          clientsMap.forEach((entry) => {
            if (entry.role === 'client' && entry.id.includes(String(msg?.target))) {
              sendSafe(entry.ws, {
                ...msg,
                senderId: currentId || 'master',
              });
            }
          });
        }
        return;
      }

      // 5. Broadcast dari Master ke semua Klien (misal EMERGENCY_WIPE semua node)
      if (currentRole === 'master' && !msg.target) {
        clientsMap.forEach((entry) => {
          if (entry.role === 'client') {
            sendSafe(entry.ws, msg);
          }
        });
      }
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
          masterSockets.forEach((masterWs) => {
            sendSafe(masterWs, {
              type: 'client_disconnected',
              senderId: currentId,
            });
          });
        }
      }
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
      server: { middlewareMode: true },
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
