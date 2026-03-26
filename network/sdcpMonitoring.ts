type UnknownRecord = Record<string, unknown>;

export type SdcpMonitoringSnapshot = {
  connected: boolean;
  stateText: string;
  isPrinting: boolean;
  isPaused: boolean;
  cancelLatched: boolean;
  pauseLatched: boolean;
  finished: boolean;
  progressPct: number | null;
  currentLayer: number | null;
  totalLayers: number | null;
  plateId: number | null;
  jobName: string | null;
  etaSec: number | null;
  thumbnailPath?: string | null;
  taskId?: string | null;
  taskStatus?: number | null;
};

export type SdcpWebcamFeedInfo = {
  available: boolean;
  streamUrl: string | null;
  snapshotUrl: string | null;
  message: string;
};

function toFiniteNumber(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toBoolean(value: unknown): boolean {
  if (value === true) return true;
  if (value === false || value == null) return false;
  if (typeof value === 'number') return Number.isFinite(value) && value !== 0;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true' || normalized === 'yes' || normalized === 'on') return true;
    if (normalized === 'false' || normalized === 'no' || normalized === 'off') return false;
    const numeric = Number(normalized);
    if (Number.isFinite(numeric)) return numeric !== 0;
  }
  return Boolean(value);
}

function normalizePercent(value: number | null): number | null {
  if (value == null) return null;
  if (!Number.isFinite(value)) return null;
  if (value <= 1) return Math.max(0, Math.min(100, value * 100));
  return Math.max(0, Math.min(100, value));
}

function toAbsoluteUrl(candidate: string, host: string, port: number): string | null {
  const trimmed = candidate.trim();
  if (!trimmed) return null;
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(trimmed)) return trimmed;
  if (/^https?:\/\//i.test(trimmed)) return trimmed;
  if (trimmed.startsWith('//')) return `http:${trimmed}`;
  const base = `http://${host}${port === 80 ? '' : `:${port}`}`;
  if (trimmed.startsWith('/')) return `${base}${trimmed}`;
  return `${base}/${trimmed.replace(/^\/+/, '')}`;
}

function isInlinePreviewUrl(url: string): boolean {
  return /^https?:\/\//i.test(url)
    || /^wss?:\/\//i.test(url)
    || /^data:/i.test(url)
    || /^blob:/i.test(url);
}

function coerceBool(value: unknown): boolean | null {
  if (value == null) return null;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return Number.isFinite(value) ? value !== 0 : null;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['true', 'yes', 'on', 'printing', 'paused'].includes(normalized)) return true;
    if (['false', 'no', 'off', 'idle', 'stopped'].includes(normalized)) return false;
    const numeric = Number(normalized);
    if (Number.isFinite(numeric)) return numeric !== 0;
  }
  return null;
}

export function resolveSdcpMonitoringSnapshot(payload: unknown): SdcpMonitoringSnapshot {
  const root = (payload ?? {}) as UnknownRecord;
  const hasError = typeof root.error === 'string' && root.error.trim().length > 0;
  const hasExplicitConnected = Object.prototype.hasOwnProperty.call(root, 'connected');
  const hasExplicitOk = Object.prototype.hasOwnProperty.call(root, 'ok');
  const connected = hasError
    ? false
    : root.ok === false
      ? false
      : hasExplicitConnected
        ? toBoolean(root.connected)
        : hasExplicitOk
          ? toBoolean(root.ok)
          : toBoolean(root.connected ?? true);

  const stateTextRaw = [
    root.stateText,
    root.statusText,
    root.state,
    root.Status,
  ].find((value) => typeof value === 'string' && value.trim().length > 0) as string | undefined;
  const stateText = stateTextRaw?.trim() || (connected ? 'Online' : 'Offline');

  const stateNormalized = stateText.toLowerCase();
  const parsedPaused = coerceBool(root.isPaused ?? root.paused);
  const parsedPrinting = coerceBool(root.isPrinting ?? root.printing);
  const isPaused = parsedPaused ?? /\bpaused\b/.test(stateNormalized);
  const isPrinting = parsedPrinting ?? (/\bprinting\b/.test(stateNormalized) && !isPaused);

  const progressPct = normalizePercent(toFiniteNumber(root.progressPct ?? root.progress ?? root.percent ?? root.completion));
  const currentLayer = toFiniteNumber(root.currentLayer ?? root.layer);
  const totalLayers = toFiniteNumber(root.totalLayers ?? root.layers);
  const plateId = toFiniteNumber(root.plateId ?? root.plate_id);
  const etaSec = toFiniteNumber(root.etaSec ?? root.eta ?? root.remainingSec);
  const thumbnailPathRaw = root.thumbnailPath ?? root.thumbnail ?? root.thumbnailUrl;
  const thumbnailPath = typeof thumbnailPathRaw === 'string' && thumbnailPathRaw.trim().length > 0
    ? thumbnailPathRaw.trim()
    : null;
  const taskIdRaw = root.taskId ?? root.TaskId;
  const taskId = typeof taskIdRaw === 'string' && taskIdRaw.trim().length > 0
    ? taskIdRaw.trim()
    : null;
  const taskStatus = toFiniteNumber(root.taskStatus ?? root.TaskStatus ?? root.statusCode);

  const jobNameRaw = root.jobName ?? root.path ?? root.fileName ?? root.name;
  const jobName = typeof jobNameRaw === 'string' && jobNameRaw.trim().length > 0
    ? jobNameRaw.trim()
    : null;

  return {
    connected,
    stateText,
    isPrinting,
    isPaused,
    cancelLatched: false,
    pauseLatched: false,
    finished: false,
    progressPct,
    currentLayer,
    totalLayers,
    plateId: plateId != null && plateId > 0 ? Math.round(plateId) : null,
    jobName,
    etaSec: etaSec != null && etaSec >= 0 ? etaSec : null,
    thumbnailPath,
    taskId,
    taskStatus,
  };
}

export function resolveSdcpWebcamFeedInfo(payload: unknown, host: string, port: number): SdcpWebcamFeedInfo {
  const root = (payload ?? {}) as UnknownRecord;

  const explicitCandidates = [
    root.streamUrl,
    root.snapshotUrl,
    root.externalStreamUrl,
    root.mjpegUrl,
    root.cameraUrl,
    ...(Array.isArray(root.candidates) ? root.candidates : []),
  ]
    .filter((value): value is string => typeof value === 'string' && value.trim().length > 0)
    .map((value) => toAbsoluteUrl(value, host, port))
    .filter((value): value is string => Boolean(value));

  const streamUrl = explicitCandidates.find((value) => /stream|mjpeg|video/i.test(value))
    ?? explicitCandidates[0]
    ?? null;
  const snapshotUrl = explicitCandidates.find((value) => /snapshot|jpg|jpeg|png/i.test(value))
    ?? explicitCandidates[0]
    ?? null;

  if (!streamUrl && !snapshotUrl) {
    return {
      available: false,
      streamUrl: null,
      snapshotUrl: null,
      message: typeof root.message === 'string' && root.message.trim().length > 0
        ? root.message.trim()
        : 'No webcam endpoint reported by this SDCP printer.',
    };
  }

  const previewable = [streamUrl, snapshotUrl]
    .filter((value): value is string => typeof value === 'string' && value.length > 0)
    .some((value) => isInlinePreviewUrl(value));

  const preferredMessage = typeof root.message === 'string' && root.message.trim().length > 0
    ? root.message.trim()
    : 'Webcam feed detected.';

  if (!previewable) {
    const normalizedMessage = preferredMessage.toLowerCase();
    const alreadyExplainsProxyFailure = normalizedMessage.includes('proxy failed')
      || normalizedMessage.includes('not found');
    return {
      available: false,
      streamUrl,
      snapshotUrl,
      message: alreadyExplainsProxyFailure
        ? preferredMessage
        : `${preferredMessage} Stream protocol is not inline-previewable in this monitor (e.g. RTSP).`,
    };
  }

  return {
    available: true,
    streamUrl,
    snapshotUrl,
    message: preferredMessage,
  };
}
