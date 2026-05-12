// @ts-nocheck
/**
 * Live Capture — webcam preview component + useCamera hook.
 *
 * The hook manages:
 *   - getUserMedia stream lifecycle (request / release)
 *   - camera + mic device enumeration (for the device picker)
 *   - canvas snapshot helper (returns a Blob for upload)
 *   - MediaRecorder lifecycle (start/stop with operator-set chunk
 *     length so each ondataavailable fires a per-chunk upload)
 *
 * The component is a passive <video> render bound to the stream;
 * everything else (capture buttons, upload state, progress display)
 * lives in LiveCaptureTab so the camera surface stays reusable.
 */

import { useCallback, useEffect, useRef, useState } from "react";

export type CaptureBlob = {
  blob: Blob;
  mimeType: string;
  width: number;
  height: number;
  durationMs?: number;
};

export interface UseCameraOptions {
  audio?: boolean;
  width?: number;  // requested resolution; browser may downscale
  height?: number;
  deviceId?: string;
}

export interface UseCameraResult {
  videoRef: React.RefObject<HTMLVideoElement>;
  stream: MediaStream | null;
  error: string | null;
  ready: boolean;
  devices: MediaDeviceInfo[];
  start: () => Promise<void>;
  stop: () => void;
  takeSnapshot: (quality?: number) => Promise<CaptureBlob | null>;
  startRecording: (chunkMs: number, onChunk: (chunk: CaptureBlob) => void) => void;
  stopRecording: () => void;
  isRecording: boolean;
}

// Pick the best webm/mp4 mime the browser will actually record.
// Order matters: we prefer webm/vp9 (best quality) → webm/vp8 →
// generic webm → mp4 fallback for Safari.
function pickRecorderMime(): string {
  if (typeof MediaRecorder === "undefined") return "";
  const candidates = [
    "video/webm;codecs=vp9",
    "video/webm;codecs=vp8",
    "video/webm",
    "video/mp4",
  ];
  for (const c of candidates) {
    if (MediaRecorder.isTypeSupported(c)) return c;
  }
  return "";
}

export function useCamera(opts: UseCameraOptions = {}): UseCameraResult {
  const videoRef = useRef<HTMLVideoElement>(null);
  const streamRef = useRef<MediaStream | null>(null);
  const recorderRef = useRef<MediaRecorder | null>(null);
  const recordStartRef = useRef<number>(0);

  const [stream, setStream] = useState<MediaStream | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [ready, setReady] = useState(false);
  const [devices, setDevices] = useState<MediaDeviceInfo[]>([]);
  const [isRecording, setIsRecording] = useState(false);

  const start = useCallback(async () => {
    setError(null);
    try {
      if (!navigator.mediaDevices?.getUserMedia) {
        throw new Error(
          "Camera API not available — Live Capture needs https:// or localhost.",
        );
      }
      const constraints: MediaStreamConstraints = {
        video: {
          width: opts.width ? { ideal: opts.width } : undefined,
          height: opts.height ? { ideal: opts.height } : undefined,
          deviceId: opts.deviceId ? { exact: opts.deviceId } : undefined,
        },
        audio: !!opts.audio,
      };
      const s = await navigator.mediaDevices.getUserMedia(constraints);
      streamRef.current = s;
      setStream(s);
      setReady(true);
      // Enumerate devices for the picker (labels only available
      // after permission is granted).
      try {
        const list = await navigator.mediaDevices.enumerateDevices();
        setDevices(list.filter((d) => d.kind === "videoinput"));
      } catch {
        // Non-fatal — picker just stays empty.
      }
    } catch (e: any) {
      setError(e?.message || "Failed to access camera");
      setReady(false);
    }
  }, [opts.audio, opts.width, opts.height, opts.deviceId]);

  const stop = useCallback(() => {
    if (recorderRef.current && recorderRef.current.state !== "inactive") {
      try {
        recorderRef.current.stop();
      } catch {
        /* ignore */
      }
      recorderRef.current = null;
    }
    if (streamRef.current) {
      streamRef.current.getTracks().forEach((t) => t.stop());
      streamRef.current = null;
    }
    setStream(null);
    setReady(false);
    setIsRecording(false);
  }, []);

  // Wire the stream into the <video> element when both are present.
  // Using effect keeps the component declarative — no imperative
  // re-binding when the user remounts.
  useEffect(() => {
    if (videoRef.current && stream) {
      videoRef.current.srcObject = stream;
    }
  }, [stream]);

  // Release tracks on unmount so the camera light goes off when the
  // user navigates away. Some browsers leave the indicator on
  // otherwise.
  useEffect(() => {
    return () => {
      stop();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const takeSnapshot = useCallback(
    async (quality = 0.85): Promise<CaptureBlob | null> => {
      const video = videoRef.current;
      if (!video || !streamRef.current) return null;
      const w = video.videoWidth || video.clientWidth;
      const h = video.videoHeight || video.clientHeight;
      if (!w || !h) return null;
      const canvas = document.createElement("canvas");
      canvas.width = w;
      canvas.height = h;
      const ctx = canvas.getContext("2d");
      if (!ctx) return null;
      ctx.drawImage(video, 0, 0, w, h);
      return await new Promise<CaptureBlob | null>((resolve) => {
        canvas.toBlob(
          (b) => {
            if (!b) {
              resolve(null);
              return;
            }
            resolve({ blob: b, mimeType: "image/jpeg", width: w, height: h });
          },
          "image/jpeg",
          quality,
        );
      });
    },
    [],
  );

  const startRecording = useCallback(
    (chunkMs: number, onChunk: (chunk: CaptureBlob) => void) => {
      if (!streamRef.current) return;
      const mime = pickRecorderMime();
      if (!mime) {
        setError("This browser doesn't support MediaRecorder for video.");
        return;
      }
      const video = videoRef.current;
      const w = video?.videoWidth || 1280;
      const h = video?.videoHeight || 720;
      try {
        const rec = new MediaRecorder(streamRef.current, { mimeType: mime });
        rec.ondataavailable = (ev) => {
          if (!ev.data || ev.data.size === 0) return;
          const now = performance.now();
          const durationMs = Math.max(0, Math.round(now - recordStartRef.current));
          recordStartRef.current = now;
          onChunk({
            blob: ev.data,
            mimeType: mime.split(";")[0],  // strip codec param for backend
            width: w,
            height: h,
            durationMs,
          });
        };
        rec.onstop = () => {
          setIsRecording(false);
        };
        recorderRef.current = rec;
        recordStartRef.current = performance.now();
        // timeslice arg makes ondataavailable fire every chunkMs ms.
        rec.start(chunkMs);
        setIsRecording(true);
      } catch (e: any) {
        setError(e?.message || "Failed to start MediaRecorder");
      }
    },
    [],
  );

  const stopRecording = useCallback(() => {
    const rec = recorderRef.current;
    if (rec && rec.state !== "inactive") {
      try {
        rec.stop();
      } catch {
        /* ignore */
      }
    }
    recorderRef.current = null;
    setIsRecording(false);
  }, []);

  return {
    videoRef,
    stream,
    error,
    ready,
    devices,
    start,
    stop,
    takeSnapshot,
    startRecording,
    stopRecording,
    isRecording,
  };
}

interface PreviewProps {
  videoRef: React.RefObject<HTMLVideoElement>;
  className?: string;
}

export default function CaptureCameraPreview({ videoRef, className }: PreviewProps) {
  return (
    <video
      ref={videoRef}
      autoPlay
      muted
      playsInline
      className={
        className ||
        "w-full max-h-[480px] aspect-video bg-black rounded-md object-cover"
      }
    />
  );
}
