import { pluginNetworkFetch, readNativeFileChunk, readNativeFileSize } from '@/utils/pluginNetworkBridge';

type UploadProgressEvent = {
  loaded: number;
  total: number;
  uploadSpeed: string;
  remainingTime: string;
  transferred: string;
  percentComplete: number;
};

type UploadCallbacks = {
  onProgress: (event: UploadProgressEvent) => void;
  onStatusUpdate: (update: {
    stage: 'uploading' | 'processing' | 'complete' | 'error';
    message: string;
    progress?: UploadProgressEvent;
    plateId?: number | null;
    error?: string;
  }) => void;
  onComplete?: (plateId: number | null) => void;
  onError?: (error: string) => void;
};

const DEFAULT_SDCP_PORT = 3030;
const SDCP_UPLOAD_CHUNK_BYTES = 1024 * 1024;

function bytesToStringRep(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB'];
  let size = bytes;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(size >= 100 || unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

function secondsToTimeString(seconds: number): string {
  const safeSeconds = Number.isFinite(seconds) ? Math.max(0, seconds) : 0;
  const h = Math.floor(safeSeconds / 3600);
  const m = Math.floor((safeSeconds % 3600) / 60);
  const s = Math.floor(safeSeconds % 60);
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function toHostUrl(rawHostUrl: string): string {
  const trimmed = rawHostUrl.trim();
  if (!trimmed) throw new Error('Host URL is required for SDCP upload');

  const withProtocol = /^https?:\/\//i.test(trimmed) ? trimmed : `http://${trimmed}`;
  const parsed = new URL(withProtocol);

  if (!parsed.port) parsed.port = String(DEFAULT_SDCP_PORT);
  parsed.pathname = '';
  parsed.search = '';
  parsed.hash = '';

  return parsed.toString().replace(/\/+$/, '');
}

async function uploadSdcpChunk(args: {
  hostUrl: string;
  uuid: string;
  fileName: string;
  totalSize: number;
  offset: number;
  chunkBytes: Uint8Array;
}): Promise<void> {
  const { hostUrl, uuid, fileName, totalSize, offset, chunkBytes } = args;
  const parsed = new URL(hostUrl);
  const chunkBase64 = (() => {
    let binary = '';
    const step = 0x8000;
    for (let i = 0; i < chunkBytes.length; i += step) {
      const view = chunkBytes.subarray(i, i + step);
      binary += String.fromCharCode(...view);
    }
    return btoa(binary);
  })();

  const response = await pluginNetworkFetch({
    pluginId: 'sdcp-v3',
    operation: 'sdcp/upload/chunk',
    host: parsed.hostname,
    port: parsed.port ? Number(parsed.port) : DEFAULT_SDCP_PORT,
    uuid,
    fileName,
    totalSize,
    offset,
    chunkBase64,
  });

  const bodyRaw: unknown = await response.json().catch((): unknown => ({}));
  const body = (bodyRaw && typeof bodyRaw === 'object'
    ? bodyRaw
    : null) as { ok?: unknown; error?: unknown } | null;

  if (!response.ok || body?.ok !== true) {
    const error = typeof body?.error === 'string'
      ? body.error
      : `SDCP upload chunk failed (HTTP ${response.status})`;
    throw new Error(error);
  }
}

export async function uploadPrintJobWithProgress(args: {
  hostUrl: string;
  zipBlob?: Blob | null;
  zipFilePath?: string | null;
  path: string;
  profileId: string;
  callbacks: UploadCallbacks;
}): Promise<{ ok: boolean; plateId: number | null }> {
  const { hostUrl, zipBlob, zipFilePath, path, callbacks } = args;

  const resolvedHostUrl = toHostUrl(hostUrl);
  const normalizedPath = typeof zipFilePath === 'string' ? zipFilePath.trim() : '';
  const hasBlobPayload = Boolean(zipBlob && zipBlob.size > 0);
  const hasPathPayload = normalizedPath.length > 0;
  if (!hasBlobPayload && !hasPathPayload) {
    throw new Error('No SDCP upload payload available.');
  }

  const totalSize = hasBlobPayload
    ? Number(zipBlob!.size)
    : await (async () => {
      const size = await readNativeFileSize(normalizedPath);
      if (size == null || !Number.isFinite(size) || size <= 0) {
        throw new Error('Failed to read SDCP upload file size from native path.');
      }
      return Number(size);
    })();

  const fileName = `${(path || 'dragonfruit_job').trim() || 'dragonfruit_job'}.ctb`;
  const uploadUuid = typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : `sdcp-${Date.now()}`;

  const startedAt = Date.now();

  callbacks.onStatusUpdate({
    stage: 'uploading',
    message: 'Uploading print job to SDCP device…',
  });

  try {
    let offset = 0;
    while (offset < totalSize) {
      const chunkEnd = Math.min(totalSize, offset + SDCP_UPLOAD_CHUNK_BYTES);
      const chunkBytes = hasBlobPayload
        ? new Uint8Array(await zipBlob!.slice(offset, chunkEnd).arrayBuffer())
        : await (async () => {
          const bytes = await readNativeFileChunk(normalizedPath, offset, chunkEnd - offset);
          if (!bytes || bytes.byteLength === 0) {
            throw new Error('Failed to read SDCP upload chunk from native file path.');
          }
          return bytes;
        })();

      await uploadSdcpChunk({
        hostUrl: resolvedHostUrl,
        uuid: uploadUuid,
        fileName,
        totalSize,
        offset,
        chunkBytes,
      });

      offset = chunkEnd;
      const elapsedSec = Math.max(0.001, (Date.now() - startedAt) / 1000);
      const bytesPerSecond = offset / elapsedSec;
      const remainingSec = bytesPerSecond > 0 ? (totalSize - offset) / bytesPerSecond : 0;
      const percentComplete = totalSize > 0 ? Math.round((offset / totalSize) * 10000) / 100 : 100;

      const progress: UploadProgressEvent = {
        loaded: offset,
        total: totalSize,
        uploadSpeed: `${bytesToStringRep(bytesPerSecond)}/s`,
        remainingTime: secondsToTimeString(remainingSec),
        transferred: `${bytesToStringRep(offset)} / ${bytesToStringRep(totalSize)}`,
        percentComplete,
      };

      callbacks.onProgress(progress);
      callbacks.onStatusUpdate({
        stage: offset >= totalSize ? 'processing' : 'uploading',
        message: offset >= totalSize ? 'File uploaded, finalizing on SDCP device…' : 'Uploading print job to SDCP device…',
        progress,
      });
    }

    callbacks.onStatusUpdate({
      stage: 'complete',
      message: 'Upload complete',
      plateId: null,
    });
    callbacks.onComplete?.(null);

    return { ok: true, plateId: null };
  } catch (error) {
    const message = error instanceof Error ? error.message : 'SDCP upload failed';
    callbacks.onStatusUpdate({
      stage: 'error',
      message: 'Upload failed',
      error: message,
    });
    callbacks.onError?.(message);
    throw error;
  }
}
