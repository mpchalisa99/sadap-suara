import express from 'express';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer as createViteServer } from 'vite';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

interface SignalingPacket {
  type: string;
  senderId?: string;
  targetId?: string;
  target?: string;
  role?: string;
  peerId?: string;
  nodeId?: string;
  clientId?: string;
  data?: unknown;
  sdp?: unknown;
  candidate?: unknown;
  [key: string]: unknown;
}

async function startServer() {
  const app = express();
  app.use(express.json({ limit: '1mb' }));

  const server = http.createServer(app);
  const PORT = Number(process.env.PORT) || 3000;

  // ============================================================================
  // 1. WEBSOCKET ROUTING YANG KETAT (NODE.JS SERVER)
  // ============================================================================
  // Menyimpan daftar koneksi aktif di dalam Map<ID, WebSocket>
  // - Master terdaftar dengan key "MASTER"
  // - Setiap Klien terdaftar dengan ID uniknya masing-masing (misal: "client-x9a2b")
  const connections = new Map<string, WebSocket>();
  const socketToId = new Map<WebSocket, string>();

  // Antrean pesan HTTP fallback (opsional untuk diagnostik)
  const httpMailboxes = new Map<string, SignalingPacket[]>();

  const sendSafe = (targetWs: WebSocket | undefined, payload: unknown) => {
    if (targetWs && targetWs.readyState === WebSocket.OPEN) {
      try {
        targetWs.send(JSON.stringify(payload));
      } catch {}
    }
  };

  const normalizeTargetId = (rawTarget?: string): string => {
    if (!rawTarget) return '';
    if (rawTarget.toUpperCase() === 'MASTER') return 'MASTER';
    return String(rawTarget);
  };

  const wss = new WebSocketServer({ noServer: true });

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

    if (
      reqUrl.startsWith('/ws') ||
      reqUrl.startsWith('/signaling') ||
      reqUrl === '/'
    ) {
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
      });
    } else {
      socket.destroy();
    }
  });

  wss.on('connection', (ws: WebSocket) => {
    ws.on('message', (rawBuffer) => {
      let msg: SignalingPacket | null = null;
      try {
        msg = JSON.parse(rawBuffer.toString()) as SignalingPacket;
      } catch {
        return;
      }
      if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') {
        return;
      }

      const rawSenderId = String(
        msg.senderId || msg.peerId || msg.nodeId || socketToId.get(ws) || ''
      );
      const isMasterRegistration =
        msg.senderId === 'MASTER' ||
        msg.senderId === 'master' ||
        msg.role === 'MASTER' ||
        msg.role === 'master' ||
        msg.type === 'register_master' ||
        (msg.type === 'REGISTER' &&
          (String(msg.senderId).toUpperCase() === 'MASTER' ||
            String(msg.role).toUpperCase() === 'MASTER'));

      // ======================================================================
      // A. DYNAMIC ROLE ASSIGNMENT (`REQUEST_ROLE`)
      // ======================================================================
      if (msg.type === 'REQUEST_ROLE') {
        const currentMasterWs = connections.get('MASTER');
        const isMasterAlive =
          currentMasterWs !== undefined &&
          currentMasterWs.readyState === WebSocket.OPEN;

        if (!isMasterAlive) {
          connections.set('MASTER', ws);
          socketToId.set(ws, 'MASTER');
          sendSafe(ws, {
            type: 'ROLE_ASSIGNED',
            role: 'MASTER',
            peerId: 'MASTER',
            masterId: 'MASTER',
          });
        } else {
          const assignedClientId =
            rawSenderId && rawSenderId.toUpperCase() !== 'MASTER'
              ? rawSenderId
              : `client-${Math.random().toString(36).substring(2, 8)}`;
          connections.set(assignedClientId, ws);
          socketToId.set(ws, assignedClientId);

          sendSafe(ws, {
            type: 'ROLE_ASSIGNED',
            role: 'CLIENT',
            peerId: assignedClientId,
            masterId: 'MASTER',
          });
        }
        return;
      }

      // ======================================================================
      // B. REGISTRASI IDENTITAS KE `Map<ID, ws>`
      // ======================================================================
      if (isMasterRegistration) {
        connections.set('MASTER', ws);
        socketToId.set(ws, 'MASTER');

        // Jika ini pesan registrasi murni (tanpa targetId spesifik), beri tahu Klien bahwa MASTER aktif
        if (
          msg.type === 'REGISTER' ||
          msg.type === 'register_master'
        ) {
          connections.forEach((clientWs, id) => {
            if (id !== 'MASTER' && clientWs.readyState === WebSocket.OPEN) {
              sendSafe(clientWs, {
                type: 'master_online',
                senderId: 'MASTER',
                targetId: id,
              });
            }
          });
          return;
        }
      } else if (rawSenderId) {
        // Simpan koneksi Klien dengan ID uniknya di Map<ID, ws>
        connections.set(rawSenderId, ws);
        socketToId.set(ws, rawSenderId);

        if (msg.type === 'REGISTER' || msg.type === 'register_client') {
          const masterWs = connections.get('MASTER');
          if (masterWs && masterWs.readyState === WebSocket.OPEN) {
            sendSafe(ws, {
              type: 'master_online',
              senderId: 'MASTER',
              targetId: rawSenderId,
            });
          }
          return;
        }
      }

      // ======================================================================
      // C. TARGETED ROUTING KETAT (TIDAK BOLEH BROADCAST!)
      // Hanya kirim pesan spesifik ke `connections.get(targetId)`
      // ======================================================================
      const targetId = normalizeTargetId(
        String(msg.targetId || msg.target || '')
      );
      if (!targetId) return;

      const senderId = isMasterRegistration ? 'MASTER' : rawSenderId;
      const targetWs = connections.get(targetId);

      const routedPacket: SignalingPacket = {
        ...msg,
        senderId,
        targetId,
        target: targetId,
        data:
          msg.data !== undefined
            ? msg.data
            : msg.sdp !== undefined
            ? msg.sdp
            : msg.candidate !== undefined
            ? msg.candidate
            : undefined,
      };

      if (targetWs && targetWs.readyState === WebSocket.OPEN) {
        sendSafe(targetWs, routedPacket);
      }

      // Simpan juga di mailbox HTTP jika diperlukan oleh diagnostik
      const queue = httpMailboxes.get(targetId) || [];
      queue.push(routedPacket);
      if (queue.length > 50) queue.shift();
      httpMailboxes.set(targetId, queue);
    });

    ws.on('close', () => {
      const closedId = socketToId.get(ws);
      socketToId.delete(ws);

      if (!closedId) return;

      // Hapus dari Map HANYA jika socket di Map memang socket yang baru saja tertutup
      // (mencegah penghapusan koneksi baru jika ID yang sama baru saja reconnect)
      if (connections.get(closedId) === ws) {
        connections.delete(closedId);

        // Jika yang terputus adalah Klien, kirim notifikasi terarah HANYA ke MASTER
        if (closedId !== 'MASTER') {
          const masterWs = connections.get('MASTER');
          sendSafe(masterWs, {
            type: 'client_disconnected',
            senderId: closedId,
            clientId: closedId,
            targetId: 'MASTER',
          });
        }
      }
    });

    ws.on('error', () => {});
  });

  app.get('/api/health', (_req, res) => {
    const activeIds = Array.from(connections.keys());
    res.json({
      status: 'ok',
      masterOnline: connections.has('MASTER'),
      connectedIds: activeIds,
      clientsCount: activeIds.filter((id) => id !== 'MASTER').length,
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
    console.log(
      `S.I.G.M.A. Targeted Multi-Peer WebSocket Server listening on http://0.0.0.0:${PORT}`
    );
  });
}

startServer();
