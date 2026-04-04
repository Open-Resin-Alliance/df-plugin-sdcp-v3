import dgram from 'node:dgram';
import os from 'node:os';

type HandlerResult = {
  status: number;
  body: unknown;
};

type DiscoveryScope = 'all' | 'local-hostnames' | 'subnet';

type SdcpDiscoveredDevice = {
  ipAddress: string;
  port: number;
  hostName: string;
  printerName: string;
  printerModel: string;
  statusText: string;
  state: string;
  firmwareVersion: string;
};

type SdcpWsFrame = {
  Id?: unknown;
  Data?: {
    Cmd?: unknown;
    Data?: Record<string, unknown>;
    RequestID?: unknown;
    MainboardID?: unknown;
    TimeStamp?: unknown;
    From?: unknown;
  };
  MainboardID?: unknown;
  Topic?: unknown;
  TimeStamp?: unknown;
};

const DEFAULT_SDCP_PORT = 3030;
const DEFAULT_SDCP_DISCOVERY_PORT = 3000;
const MAINBOARD_ID_CACHE_TTL_MS = 5 * 60 * 1000;

type MainboardIdCacheEntry = {
  mainboardId: string;
  updatedAt: number;
};

const MAINBOARD_ID_CACHE = new Map<string, MainboardIdCacheEntry>();

function getMainboardCacheKey(host: string, port: number): string {
  return `${host.trim().toLowerCase()}:${Math.max(1, Math.round(port))}`;
}

function readCachedMainboardId(host: string, port: number): string {
  const key = getMainboardCacheKey(host, port);
  const cached = MAINBOARD_ID_CACHE.get(key);
  if (!cached) return '';

  if ((Date.now() - cached.updatedAt) > MAINBOARD_ID_CACHE_TTL_MS) {
    MAINBOARD_ID_CACHE.delete(key);
    return '';
  }

  return looksLikeMainboardId(cached.mainboardId) ? cached.mainboardId : '';
}

function storeCachedMainboardId(host: string, port: number, mainboardId: string): void {
  const normalized = mainboardId.trim();
  if (!looksLikeMainboardId(normalized)) return;
  MAINBOARD_ID_CACHE.set(getMainboardCacheKey(host, port), {
    mainboardId: normalized,
    updatedAt: Date.now(),
  });
}

function parseHostAndPort(value: string): { host: string; port: number } | null {
  const trimmed = value.trim();
  if (!trimmed) return null;

  const withProtocol = /^https?:\/\//i.test(trimmed) ? trimmed : `http://${trimmed}`;

  try {
    const parsed = new URL(withProtocol);
    const host = parsed.hostname.trim();
    if (!host) return null;
    const parsedPort = parsed.port ? Number(parsed.port) : DEFAULT_SDCP_PORT;
    const port = Number.isFinite(parsedPort) && parsedPort >= 1 && parsedPort <= 65535
      ? Math.round(parsedPort)
      : DEFAULT_SDCP_PORT;
    return { host, port };
  } catch {
    return null;
  }
}

function isPlainIpv4(value: string): boolean {
  const parts = value.split('.');
  if (parts.length !== 4) return false;

  return parts.every((part) => {
    if (!/^\d{1,3}$/.test(part)) return false;
    const n = Number(part);
    return Number.isFinite(n) && n >= 0 && n <= 255;
  });
}

function toBroadcastAddress(ipAddress: string, netmask: string): string | null {
  const ipParts = ipAddress.split('.').map((part) => Number(part));
  const maskParts = netmask.split('.').map((part) => Number(part));
  if (ipParts.length !== 4 || maskParts.length !== 4) return null;
  if (ipParts.some((part) => !Number.isFinite(part) || part < 0 || part > 255)) return null;
  if (maskParts.some((part) => !Number.isFinite(part) || part < 0 || part > 255)) return null;

  const broadcast = ipParts.map((part, index) => ((part & maskParts[index]) | (~maskParts[index] & 255)) & 255);
  return `${broadcast[0]}.${broadcast[1]}.${broadcast[2]}.${broadcast[3]}`;
}

function getLocalBroadcastAddresses(): string[] {
  const interfaces = os.networkInterfaces();
  const addresses = new Set<string>(['255.255.255.255']);

  for (const values of Object.values(interfaces)) {
    for (const entry of values ?? []) {
      const family = String((entry as { family?: unknown }).family ?? '');
      const isIpv4 = family === 'IPv4' || family === '4';
      if (!isIpv4 || entry.internal) continue;
      const broadcast = toBroadcastAddress(entry.address, (entry as { netmask?: string }).netmask ?? '');
      if (broadcast && isPlainIpv4(broadcast)) addresses.add(broadcast);
    }
  }

  return Array.from(addresses);
}

function clampNumber(value: unknown, fallback: number, min: number, max: number): number {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return fallback;
  return Math.max(min, Math.min(max, Math.round(numeric)));
}

async function runWithConcurrency<T, R>(
  items: T[],
  limit: number,
  worker: (item: T) => Promise<R | null>,
): Promise<R[]> {
  const results: R[] = [];
  let index = 0;

  const runners = Array.from({ length: Math.max(1, limit) }, async () => {
    while (index < items.length) {
      const currentIndex = index;
      index += 1;

      const result = await worker(items[currentIndex]);
      if (result) results.push(result);
    }
  });

  await Promise.all(runners);
  return results;
}

function parseSdcpDiscoveryResponse(message: string): Partial<SdcpDiscoveredDevice> {
  const trimmed = message.trim();
  if (!trimmed) return {};

  const findDeepString = (value: unknown, keys: string[]): string => {
    const normalizedKeys = keys.map((key) => key.toLowerCase());

    const visit = (node: unknown): string => {
      if (Array.isArray(node)) {
        for (const item of node) {
          const found = visit(item);
          if (found) return found;
        }
        return '';
      }

      if (!node || typeof node !== 'object') return '';

      const entries = Object.entries(node as Record<string, unknown>);
      for (const [key, value] of entries) {
        if (normalizedKeys.includes(key.toLowerCase()) && typeof value === 'string' && value.trim().length > 0) {
          return value.trim();
        }
      }

      for (const [, value] of entries) {
        const found = visit(value);
        if (found) return found;
      }

      return '';
    };

    return visit(value);
  };

  const extractFromLooseText = (text: string, keys: string[]): string => {
    for (const key of keys) {
      const keyValuePattern = new RegExp(`(?:^|[\\s,;{])${key}\\s*[:=]\\s*"?([^",;\\n\\r}]+)`, 'i');
      const keyValueMatch = keyValuePattern.exec(text);
      if (keyValueMatch && keyValueMatch[1]) {
        const value = keyValueMatch[1].trim();
        if (value) return value;
      }

      const jsonLikePattern = new RegExp(`"${key}"\\s*:\\s*"([^"]+)"`, 'i');
      const jsonLikeMatch = jsonLikePattern.exec(text);
      if (jsonLikeMatch && jsonLikeMatch[1]) {
        const value = jsonLikeMatch[1].trim();
        if (value) return value;
      }
    }
    return '';
  };

  try {
    const parsed = JSON.parse(trimmed) as Record<string, unknown>;
    return {
      ipAddress: findDeepString(parsed, ['MainboardIP', 'ipAddress', 'ip']),
      hostName: findDeepString(parsed, ['MainboardID', 'hostName', 'hostname']),
      printerName: findDeepString(parsed, ['Name', 'PrinterName', 'printerName', 'machineName']),
      printerModel: findDeepString(parsed, ['Model', 'printerModel']) || 'SDCP 3.0.0',
      firmwareVersion: findDeepString(parsed, ['Version', 'firmwareVersion']),
    };
  } catch {
    return {
      ipAddress: extractFromLooseText(trimmed, ['MainboardIP', 'ipAddress', 'ip']),
      hostName: extractFromLooseText(trimmed, ['MainboardID', 'hostName', 'hostname']),
      printerName: extractFromLooseText(trimmed, ['Name', 'PrinterName', 'printerName', 'machineName']),
      printerModel: extractFromLooseText(trimmed, ['Model', 'printerModel']) || 'SDCP 3.0.0',
      firmwareVersion: extractFromLooseText(trimmed, ['Version', 'firmwareVersion']),
    };
  }
}

function nowUnixSeconds(): number {
  return Math.floor(Date.now() / 1000);
}

function looksLikeMainboardId(value: string): boolean {
  const normalized = value.trim();
  if (!normalized) return false;
  if (isPlainIpv4(normalized)) return false;
  if (normalized.length < 4 || normalized.length > 128) return false;
  if (/\s/.test(normalized)) return false;
  if (normalized.includes('/')) return false;
  return /^[a-z0-9_-]+$/i.test(normalized);
}

function hashPlateIdFromPath(path: string): number {
  let hash = 2166136261;
  for (let i = 0; i < path.length; i += 1) {
    hash ^= path.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  const normalized = (hash >>> 0) & 0x7fffffff;
  return Math.max(1, normalized);
}

function normalizeSdcpComparablePath(value: unknown): string {
  if (typeof value !== 'string') return '';
  return value.trim().replace(/\\+/g, '/').replace(/\/+/g, '/').toLowerCase();
}

function getSdcpPathTail(value: unknown): string {
  const normalized = normalizeSdcpComparablePath(value);
  if (!normalized) return '';
  const parts = normalized.split('/').filter(Boolean);
  return parts[parts.length - 1] ?? normalized;
}

function extractSdcpAck(value: unknown): number | null {
  const candidates: unknown[] = [];

  if (value && typeof value === 'object') {
    const root = value as Record<string, unknown>;
    candidates.push(root.Ack, root.ack, root.Code, root.code);

    const nestedData = root.Data;
    if (nestedData && typeof nestedData === 'object') {
      const nested = nestedData as Record<string, unknown>;
      candidates.push(nested.Ack, nested.ack, nested.Code, nested.code);
    }
  }

  for (const candidate of candidates) {
    const parsed = Number(candidate);
    if (Number.isFinite(parsed)) return parsed;
  }

  return null;
}

function parseSdcpUnknownRecordArray(value: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === 'object'));
}

function collectSdcpRecordArraysByKeys(
  value: unknown,
  keys: string[],
  depth: number = 0,
): Array<Record<string, unknown>> {
  if (!value || typeof value !== 'object' || depth > 6) return [];
  const keySet = new Set(keys.map((key) => key.toLowerCase()));
  const collected: Array<Record<string, unknown>> = [];

  if (Array.isArray(value)) {
    for (const item of value) {
      collected.push(...collectSdcpRecordArraysByKeys(item, keys, depth + 1));
    }
    return collected;
  }

  const record = value as Record<string, unknown>;
  for (const [key, child] of Object.entries(record)) {
    if (keySet.has(key.toLowerCase())) {
      collected.push(...parseSdcpUnknownRecordArray(child));
    }
    collected.push(...collectSdcpRecordArraysByKeys(child, keys, depth + 1));
  }

  return collected;
}

function parseSdcpMaybeObject(value: unknown): Record<string, unknown> | null {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  if (typeof value !== 'string') return null;

  const trimmed = value.trim();
  if (!trimmed) return null;
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
  } catch {
    return null;
  }

  return null;
}

function selectSdcpFirstDefined(record: Record<string, unknown>, keys: string[]): unknown {
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(record, key) && record[key] != null) {
      return record[key];
    }
  }
  return undefined;
}

function parseSdcpPositiveInteger(value: unknown): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return Math.round(parsed);
}

function parseSdcpNonNegativeInteger(value: unknown): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return null;
  return Math.round(parsed);
}

function parseSdcpLooseNumber(value: unknown): number | null {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  if (!trimmed) return null;

  const normalized = (trimmed.includes(',') && trimmed.includes('.'))
    ? trimmed.replace(/,/g, '')
    : trimmed.replace(/,/g, '.');

  const direct = Number(normalized);
  if (Number.isFinite(direct)) return direct;

  const token = normalized.match(/-?\d+(?:\.\d+)?/);
  if (!token) return null;
  const parsed = Number(token[0]);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseSdcpPositiveNumber(value: unknown): number | null {
  const parsed = parseSdcpLooseNumber(value);
  if (parsed == null || !Number.isFinite(parsed) || parsed <= 0) return null;
  return parsed;
}

function parseSdcpPlatePathAndName(entry: Record<string, unknown>): { fullPath: string; name: string } {
  const rawPath = selectSdcpFirstDefined(entry, ['name', 'Name', 'path', 'Path', 'file', 'File', 'Filename', 'filename']);
  const fullPathRaw = typeof rawPath === 'string' ? rawPath.trim() : '';
  const fullPath = fullPathRaw || 'unknown-file';
  const name = derivePlateNameFromPath(fullPath);
  return { fullPath, name };
}

function parseSdcpPlateRecord(entry: Record<string, unknown>): Record<string, unknown> {
  const { fullPath, name } = parseSdcpPlatePathAndName(entry);
  const parsedFileData = parseSdcpMaybeObject(selectSdcpFirstDefined(entry, ['file_data', 'fileData']));

  const profileName = selectSdcpFirstDefined(entry, [
    'ProfileName', 'profileName', 'MaterialName', 'materialName', 'ResinName', 'resinName', 'Profile', 'profile',
  ]);
  const profileId = selectSdcpFirstDefined(entry, [
    'ProfileID', 'profileId', 'profile_id', 'MaterialID', 'materialId',
  ]);

  const lastModified = selectSdcpFirstDefined(entry, [
    'LastModified', 'lastModified', 'last_modified', 'MTime', 'mtime', 'ModifyTime', 'modifyTime',
  ]);

  const layerCount = selectSdcpFirstDefined(entry, [
    'LayersCount', 'layerCount', 'layer_count', 'TotalLayer', 'totalLayer', 'LayerCount', 'layercount',
  ]);

  const printTime = selectSdcpFirstDefined(entry, [
    'PrintTime', 'printTime', 'print_time', 'EstimatedTime', 'estimatedTime', 'estimated_time', 'Duration', 'duration',
  ]);

  const usedMaterial = selectSdcpFirstDefined(entry, [
    'UsedMaterial', 'usedMaterial', 'used_material', 'MaterialUsage', 'materialUsage', 'material_usage',
  ]);

  return {
    PlateID: parseSdcpPositiveInteger(selectSdcpFirstDefined(entry, ['plateId', 'PlateID', 'plate_id', 'id']))
      ?? hashPlateIdFromPath(fullPath || name),
    plateId: parseSdcpPositiveInteger(selectSdcpFirstDefined(entry, ['plateId', 'PlateID', 'plate_id', 'id']))
      ?? hashPlateIdFromPath(fullPath || name),
    Path: fullPath,
    path: fullPath,
    Name: name,
    name,
    ProfileName: typeof profileName === 'string' ? profileName.trim() : undefined,
    profileName: typeof profileName === 'string' ? profileName.trim() : undefined,
    ProfileID: profileId,
    profileId,
    file_data: parsedFileData ?? undefined,
    lastModified,
    LayersCount: layerCount,
    PrintTime: printTime,
    UsedMaterial: usedMaterial,
  };
}

function parseSdcpTaskDetailsFromResponse(frame: SdcpWsFrame | null): Array<Record<string, unknown>> {
  const data = (frame?.Data?.Data ?? {}) as Record<string, unknown>;
  const directTaskDetails = collectSdcpRecordArraysByKeys(data, [
    'TaskDetailList', 'taskDetailList', 'HistoryDetailList', 'historyDetailList', 'TaskList', 'taskList', 'HistoryList', 'historyList', 'Data', 'data',
  ]);

  if (directTaskDetails.length > 0) return directTaskDetails;

  return parseSdcpUnknownRecordArray(
    data.TaskDetailList
    ?? data.taskDetailList
    ?? data.HistoryDetailList
    ?? data.historyDetailList,
  );
}

function parseSdcpTaskIdsFromResponse(frame: SdcpWsFrame | null): string[] {
  const data = (frame?.Data?.Data ?? {}) as Record<string, unknown>;
  const arrays = [
    ...(Array.isArray(data.HistoryData) ? [data.HistoryData] : []),
    ...(Array.isArray(data.historyData) ? [data.historyData] : []),
    ...(Array.isArray(data.TaskIdList) ? [data.TaskIdList] : []),
    ...(Array.isArray(data.taskIdList) ? [data.taskIdList] : []),
    ...(Array.isArray(data.HistoryTaskIdList) ? [data.HistoryTaskIdList] : []),
    ...(Array.isArray(data.historyTaskIdList) ? [data.historyTaskIdList] : []),
    ...(Array.isArray(data.TaskList) ? [data.TaskList] : []),
    ...collectSdcpRecordArraysByKeys(data, ['HistoryData', 'historyData', 'TaskIdList', 'taskIdList', 'HistoryTaskIdList', 'historyTaskIdList']),
  ];

  const ids = new Set<string>();

  for (const arrayLike of arrays) {
    const values = Array.isArray(arrayLike) ? arrayLike : [arrayLike];
    for (const value of values) {
      if (typeof value === 'string') {
        const trimmed = value.trim();
        if (trimmed) ids.add(trimmed);
        continue;
      }

      if (value && typeof value === 'object') {
        const record = value as Record<string, unknown>;
        const candidate = selectSdcpFirstDefined(record, ['TaskId', 'taskId', 'ID', 'id']);
        if (typeof candidate === 'string' && candidate.trim()) ids.add(candidate.trim());
      }
    }
  }

  return Array.from(ids);
}

function mergeSdcpTaskDetailIntoPlate(
  plate: Record<string, unknown>,
  detail: Record<string, unknown>,
): Record<string, unknown> {
  const printTime = selectSdcpFirstDefined(detail, [
    'PrintTime', 'printTime', 'print_time', 'EstimatedTime', 'estimatedTime', 'Duration', 'duration',
  ]);
  const usedMaterial = selectSdcpFirstDefined(detail, [
    'UsedMaterial', 'usedMaterial', 'used_material', 'MaterialUsage', 'materialUsage',
  ]);
  const layerCount = selectSdcpFirstDefined(detail, [
    'LayersCount', 'layerCount', 'layer_count', 'TotalLayer', 'totalLayer',
  ]);
  const profileName = selectSdcpFirstDefined(detail, [
    'ProfileName', 'profileName', 'MaterialName', 'materialName', 'ResinName', 'resinName',
  ]);
  const profileId = selectSdcpFirstDefined(detail, [
    'ProfileID', 'profileId', 'profile_id', 'MaterialID', 'materialId',
  ]);
  const lastModified = selectSdcpFirstDefined(detail, [
    'LastModified', 'lastModified', 'last_modified', 'MTime', 'mtime', 'CompleteTime', 'completeTime',
  ]);

  const rawFileData = parseSdcpMaybeObject(plate.file_data)
    ?? parseSdcpMaybeObject(selectSdcpFirstDefined(detail, ['file_data', 'fileData']))
    ?? {};

  const mergedFileData = {
    ...rawFileData,
    ...(layerCount != null ? { layer_count: layerCount } : {}),
    ...(printTime != null ? { printTime } : {}),
    ...(usedMaterial != null ? { usedMaterial } : {}),
    ...(lastModified != null ? { last_modified: lastModified } : {}),
  };

  return {
    ...plate,
    ...(printTime != null ? { PrintTime: printTime } : {}),
    ...(usedMaterial != null ? { UsedMaterial: usedMaterial } : {}),
    ...(layerCount != null ? { LayersCount: layerCount } : {}),
    ...(lastModified != null ? { lastModified } : {}),
    ...(typeof profileName === 'string' && profileName.trim().length > 0
      ? {
          ProfileName: profileName.trim(),
          profileName: profileName.trim(),
        }
      : {}),
    ...(profileId != null ? { ProfileID: profileId, profileId } : {}),
    file_data: mergedFileData,
  };
}

function extractSdcpStringFromRecord(record: Record<string, unknown>, keys: string[]): string | null {
  const raw = selectSdcpFirstDefined(record, keys);
  if (typeof raw !== 'string') return null;
  const trimmed = raw.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function isSdcpTaskStatusActive(taskStatus: number | null): boolean {
  if (taskStatus == null) return false;
  // SDCP v3 docs describe: 0=Other, 1=Completed, 2=Exceptional, 3=Stopped.
  // In practice, some firmware reports TaskStatus=1 while the job is still active.
  // Treat only explicit terminal failure/stop states as non-active.
  return taskStatus !== 2 && taskStatus !== 3;
}

function isSdcpTaskDetailLikelyActive(detail: Record<string, unknown> | null): boolean {
  if (!detail) return false;

  const endTime = parseSdcpNonNegativeInteger(selectSdcpFirstDefined(detail, [
    'EndTime', 'endTime', 'FinishTime', 'finishTime', 'CompleteTime', 'completeTime',
  ]));
  if (endTime != null && endTime > 0) return false;

  const taskStatus = parseSdcpNonNegativeInteger(selectSdcpFirstDefined(detail, [
    'TaskStatus', 'taskStatus', 'Status', 'status',
  ]));
  if (taskStatus === 2 || taskStatus === 3) return false;

  const hasTaskIdentity = Boolean(
    extractSdcpStringFromRecord(detail, ['TaskId', 'taskId', 'Id', 'id'])
    || extractSdcpStringFromRecord(detail, ['Filename', 'filename', 'FileName', 'fileName', 'Path', 'path', 'File', 'file'])
    || extractSdcpStringFromRecord(detail, ['TaskName', 'taskName', 'Name', 'name']),
  );
  const beginTime = parseSdcpNonNegativeInteger(selectSdcpFirstDefined(detail, ['BeginTime', 'beginTime', 'StartTime', 'startTime']));
  const printedLayer = parseSdcpNonNegativeInteger(selectSdcpFirstDefined(detail, [
    'AlreadyPrintLayer', 'alreadyPrintLayer', 'CurrentLayer', 'currentLayer',
  ]));

  return hasTaskIdentity || beginTime != null || printedLayer != null || taskStatus != null;
}

function parseSdcpProgressPercent(raw: unknown): number | null {
  const value = Number(raw);
  if (!Number.isFinite(value)) return null;
  if (value < 0) return null;
  if (value <= 1) return Math.max(0, Math.min(100, value * 100));
  return Math.max(0, Math.min(100, value));
}

function isLikelySdcpTotalLayerKey(key: string): boolean {
  const normalized = key.toLowerCase().replace(/[^a-z0-9]/g, '');
  if (!normalized) return false;

  if (
    normalized.includes('bottom')
    || normalized.includes('firstlayer')
    || normalized.includes('transition')
    || normalized.includes('current')
    || normalized.includes('already')
    || normalized.includes('printed')
  ) {
    return false;
  }

  if ([
    'totallayer', 'totallayers', 'totallayercount', 'layercount', 'layerscount',
    'layertotal', 'totalprintlayer', 'slicelayercount',
  ].includes(normalized)) {
    return true;
  }

  return normalized.includes('layer')
    && (normalized.includes('count') || normalized.includes('total'));
}

function collectSdcpTotalLayerCandidates(value: unknown, depth: number = 0): number[] {
  if (!value || depth > 8) return [];

  if (Array.isArray(value)) {
    return value.flatMap((item) => collectSdcpTotalLayerCandidates(item, depth + 1));
  }

  if (typeof value !== 'object') return [];

  const out: number[] = [];
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    if (isLikelySdcpTotalLayerKey(key)) {
      const parsed = parseSdcpPositiveInteger(child);
      if (parsed != null) out.push(parsed);
    }
    out.push(...collectSdcpTotalLayerCandidates(child, depth + 1));
  }

  return out;
}

function selectSdcpBestTotalLayerCandidate(candidates: number[], currentLayer: number | null): number | null {
  const unique = Array.from(new Set(candidates.filter((v) => Number.isFinite(v) && v > 0)));
  if (unique.length === 0) return null;

  if (currentLayer != null && currentLayer >= 0) {
    const compatible = unique.filter((candidate) => candidate >= currentLayer);
    if (compatible.length === 0) return null;
    return Math.max(...compatible);
  }

  return Math.max(...unique);
}

function isLikelySdcpCurrentLayerKey(key: string): boolean {
  const normalized = key.toLowerCase().replace(/[^a-z0-9]/g, '');
  if (!normalized) return false;

  if (
    normalized.includes('bottom')
    || normalized.includes('firstlayer')
    || normalized.includes('transition')
    || normalized.includes('count')
    || normalized.includes('total')
    || normalized === 'layer'
    || normalized === 'layers'
  ) {
    return false;
  }

  return normalized === 'currentlayer'
    || normalized === 'printedlayer'
    || (normalized.includes('current') && normalized.includes('layer'))
    || (normalized.includes('printed') && normalized.includes('layer'));
}

function collectSdcpCurrentLayerCandidates(value: unknown, depth: number = 0): number[] {
  if (!value || depth > 8) return [];

  if (Array.isArray(value)) {
    return value.flatMap((item) => collectSdcpCurrentLayerCandidates(item, depth + 1));
  }

  if (typeof value !== 'object') return [];

  const out: number[] = [];
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    if (isLikelySdcpCurrentLayerKey(key)) {
      const parsed = parseSdcpNonNegativeInteger(child);
      if (parsed != null) out.push(parsed);
    }
    out.push(...collectSdcpCurrentLayerCandidates(child, depth + 1));
  }

  return out;
}

function selectSdcpBestCurrentLayerCandidate(candidates: number[], totalLayers: number | null): number | null {
  const unique = Array.from(new Set(candidates.filter((v) => Number.isFinite(v) && v >= 0)));
  if (unique.length === 0) return null;

  if (totalLayers != null && totalLayers > 0) {
    const compatible = unique.filter((candidate) => candidate <= totalLayers);
    if (compatible.length > 0) return Math.max(...compatible);
  }

  return Math.max(...unique);
}

function extractSdcpLayerProgressFromTaskDetail(detail: Record<string, unknown> | null): {
  currentLayer: number | null;
  totalLayers: number | null;
  progressPct: number | null;
} {
  if (!detail) {
    return { currentLayer: null, totalLayers: null, progressPct: null };
  }

  const sliceInformation = parseSdcpMaybeObject(selectSdcpFirstDefined(detail, [
    'SliceInformation', 'sliceInformation', 'SliceInfo', 'sliceInfo',
  ]));

  const explicitProgress = parseSdcpProgressPercent(selectSdcpFirstDefined(detail, [
    'Progress', 'progress', 'ProgressPct', 'progressPct', 'PrintProgress', 'printProgress', 'TaskProgress', 'taskProgress',
  ]));

  const endTime = parseSdcpNonNegativeInteger(selectSdcpFirstDefined(detail, [
    'EndTime', 'endTime', 'FinishTime', 'finishTime', 'CompleteTime', 'completeTime',
  ]));
  const taskLikelyEnded = endTime != null && endTime > 0;

  const directCurrentLayer = parseSdcpNonNegativeInteger(selectSdcpFirstDefined(detail, [
    'CurrentLayer', 'currentLayer', 'current_layer',
    'PrintedLayer', 'printedLayer', 'printed_layer',
  ]));
  const alreadyPrintedLayer = parseSdcpNonNegativeInteger(selectSdcpFirstDefined(detail, [
    'AlreadyPrintLayer', 'alreadyPrintLayer', 'already_print_layer',
  ]));

  const directTotalLayers = parseSdcpPositiveInteger(
    selectSdcpFirstDefined(detail, [
      'TotalLayer', 'totalLayer', 'LayerCount', 'layerCount', 'LayersCount', 'layersCount', 'layer_count', 'TotalLayers', 'totalLayers',
      'total_layer', 'total_layers',
      'total_layer_numbers', 'totalLayerNumbers',
      'TotalLayerCount', 'totalLayerCount', 'LayerTotal', 'layerTotal', 'TotalPrintLayer', 'totalPrintLayer',
      'SliceLayerCount', 'sliceLayerCount', 'slice_layer_count',
    ])
      ?? selectSdcpFirstDefined(sliceInformation ?? {}, [
        'TotalLayer', 'totalLayer', 'LayerCount', 'layerCount', 'LayersCount', 'layersCount', 'layer_count', 'TotalLayers', 'totalLayers',
        'total_layer', 'total_layers',
        'total_layer_numbers', 'totalLayerNumbers',
        'TotalLayerCount', 'totalLayerCount', 'LayerTotal', 'layerTotal', 'TotalPrintLayer', 'totalPrintLayer',
        'SliceLayerCount', 'sliceLayerCount', 'slice_layer_count',
      ]),
  );

  const recursiveTotalLayers = selectSdcpBestTotalLayerCandidate(
    [
      ...collectSdcpTotalLayerCandidates(detail),
      ...collectSdcpTotalLayerCandidates(sliceInformation),
    ],
    directCurrentLayer,
  );

  const totalLayers = directTotalLayers ?? recursiveTotalLayers;
  const recursiveCurrentLayer = selectSdcpBestCurrentLayerCandidate(
    [
      ...collectSdcpCurrentLayerCandidates(detail),
      ...collectSdcpCurrentLayerCandidates(sliceInformation),
    ],
    totalLayers,
  );
  const printedVolumeMl = parseSdcpPositiveNumber(selectSdcpFirstDefined(detail, [
    'CurrentLayerTalVolume', 'CurrentLayerTotalVolume',
    'currentLayerTalVolume', 'currentLayerTotalVolume',
    'PrintedVolume', 'printedVolume',
  ]));
  const modelVolumeMl = parseSdcpPositiveNumber(selectSdcpFirstDefined(sliceInformation ?? {}, [
    'volume', 'Volume', 'modelVolume', 'model_volume',
  ]));
  const volumeProgressPct = (
    printedVolumeMl != null
    && modelVolumeMl != null
    && modelVolumeMl > 0
  ) ? Math.max(0, Math.min(100, (printedVolumeMl / modelVolumeMl) * 100)) : null;
  const volumeDerivedCurrentLayer = (() => {
    if (volumeProgressPct == null || totalLayers == null || totalLayers <= 0) return null;
    const raw = Math.min(totalLayers, Math.round((volumeProgressPct / 100) * totalLayers));
    return raw > 0 ? raw : null;
  })();
  const progressDerivedCurrentLayer = (() => {
    if (explicitProgress == null || totalLayers == null || totalLayers <= 0) return null;
    const raw = Math.min(totalLayers, Math.round((explicitProgress / 100) * totalLayers));
    return raw > 0 ? raw : null;
  })();
  const alreadyPrintedLayerCandidate = (() => {
    if (alreadyPrintedLayer == null) return null;
    if (totalLayers == null || totalLayers <= 0) return alreadyPrintedLayer;
    if (alreadyPrintedLayer < totalLayers) return alreadyPrintedLayer;
    if (alreadyPrintedLayer > totalLayers) return null;

    const nearComplete = (
      (explicitProgress != null && explicitProgress >= 99.5)
      || (volumeProgressPct != null && volumeProgressPct >= 99.5)
      || taskLikelyEnded
    );
    return nearComplete ? alreadyPrintedLayer : null;
  })();
  const suspiciousDirectCurrent = (
    directCurrentLayer != null
    && totalLayers != null
    && directCurrentLayer >= totalLayers
    && (
      (explicitProgress != null && explicitProgress < 99.5)
      || (volumeProgressPct != null && volumeProgressPct < 99.5)
      || (!taskLikelyEnded && explicitProgress == null && volumeProgressPct == null)
    )
  );
  const currentLayer = (suspiciousDirectCurrent ? null : directCurrentLayer)
    ?? recursiveCurrentLayer
    ?? volumeDerivedCurrentLayer
    ?? progressDerivedCurrentLayer
    ?? alreadyPrintedLayerCandidate;

  const derivedProgress = (
    currentLayer != null
    && totalLayers != null
    && totalLayers > 0
  ) ? Math.max(0, Math.min(100, (currentLayer / totalLayers) * 100)) : null;

  return {
    currentLayer,
    totalLayers,
    progressPct: explicitProgress ?? derivedProgress ?? volumeProgressPct,
  };
}

function resolveSdcpActiveTaskDetail(args: {
  taskDetails: Array<Record<string, unknown>>;
  taskId: string | null;
  jobName: string | null;
}): Record<string, unknown> | null {
  const { taskDetails, taskId, jobName } = args;
  if (taskDetails.length === 0) return null;

  if (taskId) {
    const matchedByTaskId = taskDetails.find((detail) => {
      const detailTaskId = extractSdcpStringFromRecord(detail, ['TaskId', 'taskId', 'Id', 'id']);
      return detailTaskId?.toLowerCase() === taskId.toLowerCase();
    });
    if (matchedByTaskId) return matchedByTaskId;
  }

  const normalizedJobName = normalizeSdcpComparablePath(jobName);
  if (normalizedJobName) {
    const matchedByFilename = taskDetails.find((detail) => {
      const detailPath = extractSdcpStringFromRecord(detail, ['Filename', 'filename', 'FileName', 'fileName', 'Path', 'path', 'File', 'file']);
      const detailTaskName = extractSdcpStringFromRecord(detail, ['TaskName', 'taskName', 'Name', 'name']);
      const comparablePath = normalizeSdcpComparablePath(detailPath);
      const comparableTail = getSdcpPathTail(detailPath);
      const comparableTaskName = normalizeSdcpComparablePath(detailTaskName);
      return comparablePath.includes(normalizedJobName)
        || comparableTail === normalizedJobName
        || comparableTaskName.includes(normalizedJobName);
    });
    if (matchedByFilename) return matchedByFilename;
  }

  const matchedByActiveHeuristic = taskDetails.find((detail) => isSdcpTaskDetailLikelyActive(detail));
  if (matchedByActiveHeuristic) return matchedByActiveHeuristic;

  return taskDetails[0] ?? null;
}

function parseSdcpWsFrame(data: string): SdcpWsFrame | null {
  try {
    const parsed = JSON.parse(data) as SdcpWsFrame;
    if (!parsed || typeof parsed !== 'object') return null;
    return parsed;
  } catch {
    return null;
  }
}

function getFrameTopic(frame: SdcpWsFrame | null | undefined): string {
  const topic = typeof frame?.Topic === 'string' ? frame.Topic.trim() : '';
  return topic.toLowerCase();
}

function getFrameMainboardId(frame: SdcpWsFrame | null | undefined): string {
  const direct = typeof frame?.Data?.MainboardID === 'string'
    ? frame.Data.MainboardID.trim()
    : typeof frame?.MainboardID === 'string'
      ? frame.MainboardID.trim()
      : '';
  if (looksLikeMainboardId(direct)) return direct;

  const topic = getFrameTopic(frame);
  const lastSegment = topic.split('/').pop()?.trim() ?? '';
  return looksLikeMainboardId(lastSegment) ? lastSegment : '';
}

async function resolveSdcpDeviceViaDiscovery(host: string, timeoutMs: number): Promise<SdcpDiscoveredDevice | null> {
  const normalizedTarget = host.trim().toLowerCase();
  if (!normalizedTarget) return null;

  const devices = await discoverSdcpDevicesViaUdp(Math.max(600, Math.min(timeoutMs, 4000)));
  return devices.find((device) => {
    const ipMatches = device.ipAddress.trim().toLowerCase() === normalizedTarget;
    const hostMatches = device.hostName.trim().toLowerCase() === normalizedTarget;
    return ipMatches || hostMatches;
  }) ?? null;
}

async function resolveMainboardIdForHost(host: string, port: number): Promise<string> {
  const wsDiscovered = await resolveMainboardIdViaWebSocket(host, port, 1800);
  const discovery = await resolveSdcpDeviceViaDiscovery(host, 1800);
  if (discovery) {
    const candidate = discovery.hostName?.trim() ?? '';
    if (looksLikeMainboardId(candidate)) {
      return candidate;
    }
  }
  if (looksLikeMainboardId(wsDiscovered)) return wsDiscovered;

  const udpDiscovered = await resolveMainboardIdViaUdp(host, 1400);
  if (looksLikeMainboardId(udpDiscovered)) return udpDiscovered;

  const probe = await probeSdcpHost(host, port, 1800);
  if (!probe) return '';
  const enriched = await enrichSdcpDeviceIdentityViaWebSocket(probe, 1400);
  const candidate = enriched.hostName?.trim() ?? '';
  if (!looksLikeMainboardId(candidate)) return '';
  if (candidate.toLowerCase() === host.toLowerCase()) return '';
  return candidate;
}

async function resolveMainboardIdViaUdp(targetHost: string, timeoutMs: number): Promise<string> {
  const devices = await discoverSdcpDevicesViaUdp(Math.max(400, Math.min(timeoutMs, 4000)));
  const target = devices.find((device) => device.ipAddress.trim().toLowerCase() === targetHost.trim().toLowerCase()) ?? null;
  if (!target) return '';
  const candidate = target.hostName?.trim() ?? '';
  return looksLikeMainboardId(candidate) ? candidate : '';
}

async function resolveMainboardIdViaWebSocket(host: string, port: number, timeoutMs: number): Promise<string> {
  const url = `ws://${host}:${port}/websocket`;

  return new Promise((resolve) => {
    let settled = false;
    let ws: WebSocket | null = null;

    const finish = (value: string) => {
      if (settled) return;
      settled = true;
      try {
        ws?.close();
      } catch {
        // no-op
      }
      resolve(value);
    };

    const timer = setTimeout(() => finish(''), Math.max(500, Math.min(timeoutMs, 5000)));

    try {
      ws = new WebSocket(url);
    } catch {
      clearTimeout(timer);
      finish('');
      return;
    }

    ws.addEventListener('open', () => {
      try {
        ws?.send('ping');
      } catch {
        // no-op
      }
    });

    ws.addEventListener('message', (event) => {
      const payload = decodeWsMessageData((event as MessageEvent).data);
      if (!payload || payload === 'pong') return;
      const frame = parseSdcpWsFrame(payload);
      if (!frame) return;
      const mainboardId = getFrameMainboardId(frame);
      if (!looksLikeMainboardId(mainboardId)) return;
      clearTimeout(timer);
      finish(mainboardId.trim());
    });

    ws.addEventListener('error', () => {
      clearTimeout(timer);
      finish('');
    });

    ws.addEventListener('close', () => {
      clearTimeout(timer);
      finish('');
    });
  });
}

async function handleSdcpUploadChunk(payload: unknown): Promise<HandlerResult> {
  const rawHost = typeof (payload as any)?.host === 'string'
    ? (payload as any).host
    : typeof (payload as any)?.ipAddress === 'string'
      ? (payload as any).ipAddress
      : '';
  const parsedHost = parseHostAndPort(rawHost);
  if (!parsedHost) {
    return { status: 400, body: { ok: false, error: 'Invalid host or IP address' } };
  }

  const port = clampNumber((payload as any)?.port, parsedHost.port, 1, 65535);
  const uuid = typeof (payload as any)?.uuid === 'string' ? (payload as any).uuid.trim() : '';
  const fileName = typeof (payload as any)?.fileName === 'string' ? (payload as any).fileName.trim() : '';
  const totalSize = clampNumber((payload as any)?.totalSize, 0, 0, Number.MAX_SAFE_INTEGER);
  const offset = clampNumber((payload as any)?.offset, 0, 0, Number.MAX_SAFE_INTEGER);
  const chunkBase64 = typeof (payload as any)?.chunkBase64 === 'string' ? (payload as any).chunkBase64.trim() : '';
  if (!uuid || !fileName || !chunkBase64) {
    return { status: 400, body: { ok: false, error: 'Missing required SDCP upload chunk fields' } };
  }

  let chunkBuffer: Buffer;
  try {
    chunkBuffer = Buffer.from(chunkBase64, 'base64');
  } catch {
    return { status: 400, body: { ok: false, error: 'Invalid chunkBase64 payload' } };
  }

  if (!chunkBuffer || chunkBuffer.length === 0) {
    return { status: 400, body: { ok: false, error: 'Decoded upload chunk is empty' } };
  }

  const form = new FormData();
  form.set('S-File-MD5', '');
  form.set('Check', '0');
  form.set('Offset', String(offset));
  form.set('Uuid', uuid);
  form.set('TotalSize', String(totalSize));
  const blobPart = chunkBuffer as unknown as BlobPart;
  form.set('File', new Blob([blobPart]), fileName);

  try {
    const response = await fetch(`http://${parsedHost.host}:${port}/uploadFile/upload`, {
      method: 'POST',
      body: form,
      cache: 'no-store',
      signal: AbortSignal.timeout(15000),
    });

    if (!response.ok) {
      return {
        status: response.status,
        body: {
          ok: false,
          error: `SDCP upload chunk failed (HTTP ${response.status})`,
        },
      };
    }

    return {
      status: 200,
      body: {
        ok: true,
      },
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : 'SDCP upload chunk request failed';
    return {
      status: 502,
      body: {
        ok: false,
        error: message,
      },
    };
  }
}

async function sendSdcpCommandAndAwaitResponse(args: {
  host: string;
  port: number;
  mainboardId: string;
  cmd: number;
  data?: Record<string, unknown>;
  timeoutMs: number;
}): Promise<SdcpWsFrame | null> {
  const { host, port, mainboardId, cmd, data = {}, timeoutMs } = args;
  const startedAt = Date.now();
  const requestId = typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : `sdcp-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  const requestPayload = {
    Id: 'dragonfruit',
    Data: {
      Cmd: cmd,
      Data: data,
      RequestID: requestId,
      MainboardID: mainboardId,
      TimeStamp: nowUnixSeconds(),
      From: 0,
    },
    Topic: `sdcp/request/${mainboardId}`,
  };

  return new Promise((resolve) => {
    let settled = false;
    let ws: WebSocket | null = null;

    const finish = (value: SdcpWsFrame | null) => {
      if (settled) return;
      settled = true;
      try {
        ws?.close();
      } catch {
        // no-op
      }
      resolve(value);
    };

    const effectiveTimeoutMs = Math.max(700, Math.min(timeoutMs, 7000));
    const timer = setTimeout(() => {
      finish(null);
    }, effectiveTimeoutMs);

    try {
      ws = new WebSocket(`ws://${host}:${port}/websocket`);
    } catch {
      clearTimeout(timer);
      finish(null);
      return;
    }

    ws.addEventListener('open', () => {
      try {
        ws?.send('ping');
      } catch {
        // no-op
      }
      try {
        ws?.send(JSON.stringify(requestPayload));
      } catch {
        clearTimeout(timer);
        finish(null);
      }
    });

    ws.addEventListener('message', (event) => {
      const payload = decodeWsMessageData((event as MessageEvent).data);
      if (!payload || payload === 'pong') return;
      const frame = parseSdcpWsFrame(payload);
      if (!frame) return;
      const topic = getFrameTopic(frame);
      if (!topic.startsWith(`sdcp/response/${mainboardId.toLowerCase()}`)) return;
      if (Number(frame.Data?.Cmd) !== cmd) return;
      if (String(frame.Data?.RequestID ?? '').trim() !== requestId) return;
      clearTimeout(timer);
      finish(frame);
    });

    ws.addEventListener('error', () => {
      clearTimeout(timer);
      finish(null);
    });

    ws.addEventListener('close', () => {
      clearTimeout(timer);
      finish(null);
    });
  });
}

async function requestSdcpStatusAndAttributes(args: {
  host: string;
  port: number;
  mainboardId: string;
  timeoutMs: number;
}): Promise<{ statusFrame: SdcpWsFrame | null; attributesFrame: SdcpWsFrame | null }> {
  const { host, port, mainboardId, timeoutMs } = args;

  return new Promise((resolve) => {
    let settled = false;
    let ws: WebSocket | null = null;
    let statusFrame: SdcpWsFrame | null = null;
    let attributesFrame: SdcpWsFrame | null = null;

    const requestIds = {
      status: `sdcp-status-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      attr: `sdcp-attr-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
    };

    const finish = () => {
      if (settled) return;
      settled = true;
      try {
        ws?.close();
      } catch {
        // no-op
      }
      resolve({ statusFrame, attributesFrame });
    };

    const timer = setTimeout(finish, Math.max(900, Math.min(timeoutMs, 7000)));

    try {
      ws = new WebSocket(`ws://${host}:${port}/websocket`);
    } catch {
      clearTimeout(timer);
      finish();
      return;
    }

    const sendCommand = (cmd: number, requestId: string) => {
      ws?.send(JSON.stringify({
        Id: 'dragonfruit',
        Data: {
          Cmd: cmd,
          Data: {},
          RequestID: requestId,
          MainboardID: mainboardId,
          TimeStamp: nowUnixSeconds(),
          From: 0,
        },
        Topic: `sdcp/request/${mainboardId}`,
      }));
    };

    ws.addEventListener('open', () => {
      try {
        ws?.send('ping');
      } catch {
        // no-op
      }
      try {
        sendCommand(0, requestIds.status);
        sendCommand(1, requestIds.attr);
      } catch {
        clearTimeout(timer);
        finish();
      }
    });

    ws.addEventListener('message', (event) => {
      const payload = decodeWsMessageData((event as MessageEvent).data);
      if (!payload || payload === 'pong') return;
      const frame = parseSdcpWsFrame(payload);
      if (!frame) return;

      const topic = getFrameTopic(frame);
      if (topic.startsWith(`sdcp/status/${mainboardId.toLowerCase()}`)) {
        statusFrame = frame;
      }
      if (topic.startsWith(`sdcp/attributes/${mainboardId.toLowerCase()}`)) {
        attributesFrame = frame;
      }

      if (statusFrame && attributesFrame) {
        clearTimeout(timer);
        finish();
      }
    });

    ws.addEventListener('error', () => {
      clearTimeout(timer);
      finish();
    });

    ws.addEventListener('close', () => {
      clearTimeout(timer);
      finish();
    });
  });
}

function extractPrintInfoFromStatusFrame(frame: SdcpWsFrame | null): {
  stateText: string;
  state: string;
  isPrinting: boolean;
  isPaused: boolean;
  progressPct: number | null;
  currentLayer: number | null;
  totalLayers: number | null;
  etaSec: number | null;
  jobName: string | null;
  taskId: string | null;
} {
  const frameRecord = parseSdcpMaybeObject(frame as unknown);
  const frameData = parseSdcpMaybeObject(frameRecord?.Data);
  const status = parseSdcpMaybeObject(frameData?.Status)
    ?? parseSdcpMaybeObject(frameRecord?.Status);
  const printInfo = parseSdcpMaybeObject(status?.PrintInfo)
    ?? parseSdcpMaybeObject(frameData?.PrintInfo)
    ?? parseSdcpMaybeObject(frameRecord?.PrintInfo)
    ?? {};
  const currentMachineStatusRaw = status?.CurrentStatus;
  const currentMachineStatuses = Array.isArray(currentMachineStatusRaw)
    ? currentMachineStatusRaw.map((value) => Number(value)).filter((value) => Number.isFinite(value))
    : [Number(currentMachineStatusRaw)].filter((value) => Number.isFinite(value));
  const machineStatusPrinting = currentMachineStatuses.includes(1);
  const machineStatusProcessing = currentMachineStatuses.some((value) => value === 2 || value === 3 || value === 4);

  const printStatus = Number(printInfo.Status);
  const currentLayer = parseSdcpNonNegativeInteger(selectSdcpFirstDefined(printInfo, [
    'CurrentLayer', 'currentLayer', 'current_layer',
    'AlreadyPrintLayer', 'alreadyPrintLayer', 'already_print_layer',
    'PrintedLayer', 'printedLayer', 'printed_layer',
  ]));
  const totalLayers = parseSdcpPositiveInteger(selectSdcpFirstDefined(printInfo, [
    'TotalLayer', 'totalLayer', 'LayerCount', 'layerCount', 'LayersCount', 'layersCount', 'layer_count', 'TotalLayers', 'totalLayers',
    'total_layer', 'total_layers',
    'TotalLayerCount', 'totalLayerCount', 'LayerTotal', 'layerTotal', 'TotalPrintLayer', 'totalPrintLayer',
    'SliceLayerCount', 'sliceLayerCount', 'slice_layer_count',
  ]));
  const currentTicks = Number.isFinite(Number(printInfo.CurrentTicks)) ? Number(printInfo.CurrentTicks) : null;
  const totalTicks = Number.isFinite(Number(printInfo.TotalTicks)) ? Number(printInfo.TotalTicks) : null;
  const fileName = typeof printInfo.Filename === 'string' && printInfo.Filename.trim().length > 0
    ? printInfo.Filename.trim()
    : null;
  const taskId = typeof printInfo.TaskId === 'string' && printInfo.TaskId.trim().length > 0
    ? printInfo.TaskId.trim()
    : null;

  const progressFromLayer = (
    currentLayer != null
    && totalLayers != null
    && totalLayers > 0
  ) ? Math.max(0, Math.min(100, (currentLayer / totalLayers) * 100)) : null;

  const progressFromTime = (
    currentTicks != null
    && totalTicks != null
    && totalTicks > 0
  ) ? Math.max(0, Math.min(100, (currentTicks / totalTicks) * 100)) : null;

  const remainingMs = (
    currentTicks != null
    && totalTicks != null
    && totalTicks >= currentTicks
  ) ? (totalTicks - currentTicks) : null;

  const statusMap: Record<number, { text: string; printing: boolean; paused: boolean; state: string }> = {
    0: { text: 'Idle', printing: false, paused: false, state: 'idle' },
    1: { text: 'Homing', printing: true, paused: false, state: 'printing' },
    2: { text: 'Dropping', printing: true, paused: false, state: 'printing' },
    3: { text: 'Exposing', printing: true, paused: false, state: 'printing' },
    4: { text: 'Lifting', printing: true, paused: false, state: 'printing' },
    5: { text: 'Pausing', printing: true, paused: false, state: 'printing' },
    6: { text: 'Paused', printing: true, paused: true, state: 'paused' },
    7: { text: 'Stopping', printing: false, paused: false, state: 'canceling' },
    8: { text: 'Stopped', printing: false, paused: false, state: 'idle' },
    9: { text: 'Complete', printing: false, paused: false, state: 'idle' },
    10: { text: 'File Checking', printing: false, paused: false, state: 'processing' },
  };

  const mapped = Number.isFinite(printStatus) ? statusMap[printStatus] : undefined;
  const isPaused = mapped?.paused ?? false;
  const isPrinting = isPaused ? true : ((mapped?.printing ?? false) || machineStatusPrinting);
  const state = isPaused
    ? 'paused'
    : isPrinting
      ? 'printing'
      : (mapped?.state || (machineStatusProcessing ? 'processing' : 'online'));
  const stateText = isPaused
    ? 'Paused'
    : isPrinting
      ? 'Printing'
      : (mapped?.text || (machineStatusProcessing ? 'Processing' : 'Online'));

  return {
    stateText,
    state,
    isPrinting,
    isPaused,
    progressPct: progressFromLayer ?? progressFromTime,
    currentLayer,
    totalLayers,
    etaSec: remainingMs != null ? Math.max(0, Math.round(remainingMs / 1000)) : null,
    jobName: fileName,
    taskId,
  };
}

function parseSdcpFileListFromResponse(frame: SdcpWsFrame | null): Array<Record<string, unknown>> {
  const payload = (frame?.Data?.Data ?? {}) as Record<string, unknown>;
  const fileList = Array.isArray(payload.FileList)
    ? payload.FileList
    : Array.isArray(payload.fileList)
      ? payload.fileList
      : [];
  return fileList.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === 'object'));
}

function derivePlateNameFromPath(path: string): string {
  const normalized = path.trim();
  if (!normalized) return 'Unknown File';
  const segments = normalized.split('/').filter(Boolean);
  return segments[segments.length - 1] ?? normalized;
}

function hasUsefulIdentityFields(identity: Partial<SdcpDiscoveredDevice> | null | undefined): boolean {
  if (!identity) return false;
  return Boolean(
    (identity.printerName && identity.printerName.trim().length > 0)
    || (identity.hostName && identity.hostName.trim().length > 0)
    || (identity.printerModel && identity.printerModel.trim().length > 0),
  );
}

function decodeWsMessageData(data: unknown): string {
  if (typeof data === 'string') return data;
  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(data)) return data.toString('utf8');
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString('utf8');
  if (ArrayBuffer.isView(data)) {
    const view = data as ArrayBufferView;
    return Buffer.from(view.buffer, view.byteOffset, view.byteLength).toString('utf8');
  }
  return '';
}

async function resolveSdcpIdentityViaWebSocket(host: string, port: number, timeoutMs: number): Promise<Partial<SdcpDiscoveredDevice>> {
  const url = `ws://${host}:${port}/websocket`;

  return new Promise((resolve) => {
    let settled = false;
    let ws: WebSocket | null = null;

    const finish = (value: Partial<SdcpDiscoveredDevice>) => {
      if (settled) return;
      settled = true;
      try {
        ws?.close();
      } catch {
        // no-op
      }
      resolve(value);
    };

    const timer = setTimeout(() => finish({}), Math.max(350, Math.min(timeoutMs, 4000)));

    try {
      ws = new WebSocket(url);
    } catch {
      clearTimeout(timer);
      finish({});
      return;
    }

    ws.addEventListener('open', () => {
      try {
        ws?.send('ping');
      } catch {
        // no-op
      }
    });

    ws.addEventListener('message', (event) => {
      const payload = decodeWsMessageData((event as MessageEvent).data);
      if (!payload) return;
      const identity = parseSdcpDiscoveryResponse(payload);
      if (hasUsefulIdentityFields(identity)) {
        clearTimeout(timer);
        finish(identity);
      }
    });

    ws.addEventListener('error', () => {
      clearTimeout(timer);
      finish({});
    });

    ws.addEventListener('close', () => {
      clearTimeout(timer);
      finish({});
    });
  });
}

async function enrichSdcpDeviceIdentityViaWebSocket(
  device: SdcpDiscoveredDevice,
  timeoutMs: number,
): Promise<SdcpDiscoveredDevice> {
  const identity = await resolveSdcpIdentityViaWebSocket(device.ipAddress, device.port, timeoutMs);
  if (!hasUsefulIdentityFields(identity)) return device;

  return {
    ...device,
    hostName: identity.hostName?.trim() || device.hostName,
    printerName: identity.printerName?.trim() || device.printerName,
    printerModel: identity.printerModel?.trim() || device.printerModel,
    firmwareVersion: identity.firmwareVersion?.trim() || device.firmwareVersion,
  };
}

async function discoverSdcpDevicesViaUdp(timeoutMs: number): Promise<SdcpDiscoveredDevice[]> {
  return new Promise((resolve) => {
    const devices = new Map<string, SdcpDiscoveredDevice>();
    const socket = dgram.createSocket('udp4');
    const discoveryPacket = Buffer.from('M99999', 'utf8');
    const broadcasts = getLocalBroadcastAddresses();
    const safeTimeout = Math.max(300, Math.min(timeoutMs, 12_000));

    const finalize = () => {
      try {
        socket.close();
      } catch {
        // no-op
      }
      resolve(Array.from(devices.values()));
    };

    socket.on('message', (message, remoteInfo) => {
      const parsed = parseSdcpDiscoveryResponse(message.toString('utf8'));
      const ipAddress = parsed.ipAddress && isPlainIpv4(parsed.ipAddress)
        ? parsed.ipAddress
        : remoteInfo.address;

      if (!ipAddress || !isPlainIpv4(ipAddress)) return;

      devices.set(ipAddress, {
        ipAddress,
        port: DEFAULT_SDCP_PORT,
        hostName: parsed.hostName || ipAddress,
        printerName: parsed.printerName || 'SDCP Printer',
        printerModel: parsed.printerModel || 'SDCP 3.0.0',
        statusText: 'Discovered via SDCP UDP broadcast',
        state: 'online',
        firmwareVersion: parsed.firmwareVersion || '',
      });
    });

    socket.once('error', () => finalize());

    socket.bind(0, '0.0.0.0', () => {
      try {
        socket.setBroadcast(true);
      } catch {
        finalize();
        return;
      }

      for (const address of broadcasts) {
        try {
          socket.send(discoveryPacket, DEFAULT_SDCP_DISCOVERY_PORT, address);
        } catch {
          // ignore failed send targets
        }
      }

      setTimeout(finalize, safeTimeout);
    });
  });
}

async function probeSdcpHost(hostOrIp: string, port: number, timeoutMs: number): Promise<SdcpDiscoveredDevice | null> {
  try {
    const response = await fetch(`http://${hostOrIp}:${port}/uploadFile/upload`, {
      method: 'HEAD',
      cache: 'no-store',
      signal: AbortSignal.timeout(timeoutMs),
    });

    if (!response || typeof response.status !== 'number') return null;

    return {
      ipAddress: hostOrIp,
      port,
      hostName: hostOrIp,
      printerName: 'SDCP Printer',
      printerModel: 'SDCP 3.0.0',
      statusText: `Reachable (HTTP ${response.status})`,
      state: 'online',
      firmwareVersion: '',
    };
  } catch {
    return null;
  }
}

async function handleSdcpConnect(payload: unknown): Promise<HandlerResult> {
  const rawHost = typeof (payload as any)?.host === 'string'
    ? (payload as any).host
    : typeof (payload as any)?.ipAddress === 'string'
      ? (payload as any).ipAddress
      : '';
  const parsedHost = parseHostAndPort(rawHost);
  if (!parsedHost) {
    return { status: 400, body: { error: 'Invalid host or IP address' } };
  }

  const port = clampNumber((payload as any)?.port, parsedHost.port, 1, 65535);
  const probed = await probeSdcpHost(parsedHost.host, port, 3500);

  if (!probed) {
    return {
      status: 200,
      body: {
        connected: false,
        mode: 'sdcp',
        hostName: parsedHost.host,
        printerName: '',
        ipAddress: parsedHost.host,
        port,
        statusText: 'SDCP host unreachable',
        state: '',
        firmwareVersion: '',
      },
    };
  }

  const enriched = await enrichSdcpDeviceIdentityViaWebSocket(probed, 1800);

  return {
    status: 200,
    body: {
      connected: true,
      mode: 'sdcp',
      hostName: enriched.hostName,
      printerName: enriched.printerName,
      printerModel: enriched.printerModel,
      ipAddress: enriched.ipAddress,
      port: enriched.port,
      statusText: enriched.statusText,
      state: enriched.state,
      firmwareVersion: enriched.firmwareVersion,
    },
  };
}

async function handleSdcpDiscover(payload: unknown): Promise<HandlerResult> {
  const mode = (payload as any)?.mode;
  if (mode && mode !== 'sdcp') {
    return { status: 400, body: { error: 'Unsupported network mode' } };
  }

  const scopeRaw = (payload as any)?.scanScope;
  const scanScope: DiscoveryScope = scopeRaw === 'local-hostnames' || scopeRaw === 'subnet' || scopeRaw === 'all'
    ? scopeRaw
    : 'all';

  const progressive = (payload as any)?.progressive === true;
  const requestedBatchStart = clampNumber((payload as any)?.batchStart, 0, 0, Number.MAX_SAFE_INTEGER);
  const probeTimeoutMs = clampNumber((payload as any)?.probeTimeoutMs, 1200, 250, 8000);

  const rawHost = typeof (payload as any)?.host === 'string'
    ? (payload as any).host
    : typeof (payload as any)?.ipAddress === 'string'
      ? (payload as any).ipAddress
      : '';
  const forcedHostParsed = rawHost.trim().length > 0 ? parseHostAndPort(rawHost) : null;
  const forcedHost = forcedHostParsed?.host ?? null;

  const udpDevices = await discoverSdcpDevicesViaUdp(probeTimeoutMs);
  const foundByAddress = new Map<string, SdcpDiscoveredDevice>();
  udpDevices.forEach((device) => foundByAddress.set(device.ipAddress, device));

  const seedIps = Array.isArray((payload as any)?.seedIps)
    ? (payload as any).seedIps.filter((value: unknown): value is string => typeof value === 'string' && value.trim().length > 0)
    : [];
  const fallbackSeeds = Array.from(new Set([...(forcedHost ? [forcedHost] : []), ...seedIps]));

  await runWithConcurrency(fallbackSeeds, 8, async (seedHost) => {
    if (!seedHost) return null;
    if (foundByAddress.has(seedHost)) return null;
    const result = await probeSdcpHost(seedHost, DEFAULT_SDCP_PORT, probeTimeoutMs);
    if (!result) return null;
    foundByAddress.set(result.ipAddress, result);
    return result;
  });

  const enrichedDevices = await runWithConcurrency(
    Array.from(foundByAddress.values()),
    6,
    async (device) => enrichSdcpDeviceIdentityViaWebSocket(device, 1500),
  );
  foundByAddress.clear();
  enrichedDevices.forEach((device) => {
    foundByAddress.set(device.ipAddress, device);
  });

  return {
    status: 200,
    body: {
      mode: 'sdcp',
      devices: Array.from(foundByAddress.values()),
      scannedHosts: foundByAddress.size,
      scannedEndpoints: foundByAddress.size,
      scannedLocalHostnames: 0,
      scannedSubnetHosts: 0,
      scanScope,
      progressive,
      totalEndpoints: foundByAddress.size,
      batchStart: requestedBatchStart,
      batchSize: foundByAddress.size,
      nextBatchStart: requestedBatchStart,
      done: true,
    },
  };
}

async function handleSdcpPrinterStatus(payload: unknown): Promise<HandlerResult> {
  const rawHost = typeof (payload as any)?.host === 'string'
    ? (payload as any).host
    : typeof (payload as any)?.ipAddress === 'string'
      ? (payload as any).ipAddress
      : '';
  const parsedHost = parseHostAndPort(rawHost);
  if (!parsedHost) {
    console.info('[sdcp-v3][printer/status] invalid-host', {
      rawHost,
      payloadMainboardId: typeof (payload as any)?.mainboardId === 'string' ? String((payload as any).mainboardId).trim() : '',
    });
    return { status: 400, body: { ok: false, connected: false, error: 'Invalid host or IP address' } };
  }

  const port = clampNumber((payload as any)?.port, parsedHost.port, 1, 65535);
  const probed = await probeSdcpHost(parsedHost.host, port, 6500);
  if (!probed) {
    console.info('[sdcp-v3][printer/status] probe-failed', {
      host: parsedHost.host,
      port,
    });
    return {
      status: 503,
      body: {
        ok: false,
        connected: false,
        mode: 'sdcp',
        hostName: parsedHost.host,
        printerName: '',
        ipAddress: parsedHost.host,
        port,
        stateText: 'Offline',
        statusText: 'SDCP discovery did not respond',
        state: 'offline',
        isPrinting: false,
        isPaused: false,
        progressPct: null,
        currentLayer: null,
        totalLayers: null,
        plateId: null,
        jobName: null,
        etaSec: null,
      },
    };
  }

  const payloadMainboardId = typeof (payload as any)?.mainboardId === 'string' && looksLikeMainboardId((payload as any).mainboardId)
    ? String((payload as any).mainboardId).trim()
    : '';
  if (payloadMainboardId) {
    storeCachedMainboardId(parsedHost.host, port, payloadMainboardId);
  }

  const cachedMainboardId = readCachedMainboardId(parsedHost.host, port);
  const resolvedMainboardId = payloadMainboardId
    || cachedMainboardId
    || await resolveMainboardIdForHost(parsedHost.host, port);
  const mainboardId = looksLikeMainboardId(resolvedMainboardId)
    ? resolvedMainboardId.trim()
    : '';
  if (mainboardId) {
    storeCachedMainboardId(parsedHost.host, port, mainboardId);
  }

  const telemetry = mainboardId
    ? await requestSdcpStatusAndAttributes({ host: parsedHost.host, port, mainboardId, timeoutMs: 6500 })
    : { statusFrame: null, attributesFrame: null };
  const printInfo = extractPrintInfoFromStatusFrame(telemetry.statusFrame);

  let taskDetailAck: number | null = null;
  let activeTaskDetail: Record<string, unknown> | null = null;
  let taskIdsUsed: string[] = [];
  let taskDetailsCount = 0;

  if (mainboardId) {
    const explicitTaskIds = printInfo.taskId ? [printInfo.taskId] : [];
    const taskIds = explicitTaskIds.length > 0
      ? explicitTaskIds
      : parseSdcpTaskIdsFromResponse(await sendSdcpCommandAndAwaitResponse({
        host: parsedHost.host,
        port,
        mainboardId,
        cmd: 320,
        data: {},
        timeoutMs: 3200,
      })).slice(0, 20);
    taskIdsUsed = taskIds;

    if (taskIds.length > 0) {
      const detailResponse = await sendSdcpCommandAndAwaitResponse({
        host: parsedHost.host,
        port,
        mainboardId,
        cmd: 321,
        data: { TaskIdList: taskIds, Id: taskIds },
        timeoutMs: 4200,
      });
      taskDetailAck = extractSdcpAck(detailResponse);
      const taskDetails = parseSdcpTaskDetailsFromResponse(detailResponse);
      taskDetailsCount = taskDetails.length;
      activeTaskDetail = resolveSdcpActiveTaskDetail({
        taskDetails,
        taskId: printInfo.taskId,
        jobName: printInfo.jobName,
      });
    }
  }

  const activeTaskStatus = activeTaskDetail
    ? parseSdcpNonNegativeInteger(selectSdcpFirstDefined(activeTaskDetail, ['TaskStatus', 'taskStatus', 'Status', 'status']))
    : null;
  const activeTaskThumbnailPath = activeTaskDetail
    ? extractSdcpStringFromRecord(activeTaskDetail, ['Thumbnail', 'thumbnail', 'ThumbnailUrl', 'thumbnailUrl', 'ThumbnailPath', 'thumbnailPath'])
    : null;
  const activeTaskId = activeTaskDetail
    ? extractSdcpStringFromRecord(activeTaskDetail, ['TaskId', 'taskId', 'Id', 'id'])
    : null;
  const activeTaskPath = activeTaskDetail
    ? extractSdcpStringFromRecord(activeTaskDetail, ['Filename', 'filename', 'FileName', 'fileName', 'Path', 'path', 'File', 'file'])
    : null;
  const activeTaskName = activeTaskDetail
    ? extractSdcpStringFromRecord(activeTaskDetail, ['TaskName', 'taskName', 'Name', 'name'])
    : null;
  const activeTaskSliceInformation = activeTaskDetail
    ? parseSdcpMaybeObject(selectSdcpFirstDefined(activeTaskDetail, ['SliceInformation', 'sliceInformation', 'SliceInfo', 'sliceInfo']))
    : null;
  const activeTaskKeys = activeTaskDetail ? Object.keys(activeTaskDetail) : [];
  const activeTaskSliceInfoKeys = activeTaskSliceInformation ? Object.keys(activeTaskSliceInformation) : [];
  const activeTaskAlreadyPrintLayer = activeTaskDetail
    ? parseSdcpNonNegativeInteger(selectSdcpFirstDefined(activeTaskDetail, [
      'AlreadyPrintLayer', 'alreadyPrintLayer', 'already_print_layer',
    ]))
    : null;
  const activeTaskCurrentLayerTalVolume = activeTaskDetail
    ? parseSdcpPositiveNumber(selectSdcpFirstDefined(activeTaskDetail, [
      'CurrentLayerTalVolume', 'CurrentLayerTotalVolume',
      'currentLayerTalVolume', 'currentLayerTotalVolume',
      'PrintedVolume', 'printedVolume',
    ]))
    : null;
  const activeTaskSliceVolume = activeTaskSliceInformation
    ? parseSdcpPositiveNumber(selectSdcpFirstDefined(activeTaskSliceInformation, [
      'volume', 'Volume', 'modelVolume', 'model_volume',
    ]))
    : null;
  const hasCurrentTaskIdentity = Boolean(printInfo.taskId || printInfo.jobName);
  const hasActiveTaskHeuristic = isSdcpTaskDetailLikelyActive(activeTaskDetail);
  const hasResolvedActiveTaskIdentity = Boolean(activeTaskId || activeTaskPath || activeTaskName);
  // If we have task detail and it explicitly says the task ended (EndTime set), don't treat as active.
  const activeTaskDefinitelyEnded = activeTaskDetail != null && !hasActiveTaskHeuristic;
  const activeTaskRunning = !activeTaskDefinitelyEnded
    && (hasCurrentTaskIdentity || hasActiveTaskHeuristic || hasResolvedActiveTaskIdentity)
    && isSdcpTaskStatusActive(activeTaskStatus);
  const taskDetailLayerProgress = extractSdcpLayerProgressFromTaskDetail(activeTaskDetail);

  const resolvedCurrentLayerRaw = printInfo.currentLayer ?? taskDetailLayerProgress.currentLayer;
  const resolvedCurrentLayer = resolvedCurrentLayerRaw != null && resolvedCurrentLayerRaw > 0
    ? Math.round(resolvedCurrentLayerRaw)
    : null;
  const totalLayersFromPrintInfo = printInfo.totalLayers != null && printInfo.totalLayers > 0
    ? Math.round(printInfo.totalLayers)
    : null;
  const totalLayersFromTaskDetail = taskDetailLayerProgress.totalLayers != null && taskDetailLayerProgress.totalLayers > 0
    ? Math.round(taskDetailLayerProgress.totalLayers)
    : null;
  const suspiciousTaskDetailTotal = totalLayersFromPrintInfo == null
    && activeTaskRunning
    && resolvedCurrentLayer != null
    && resolvedCurrentLayer > 0
    && totalLayersFromTaskDetail != null
    && totalLayersFromTaskDetail <= resolvedCurrentLayer;
  const resolvedTotalLayers = totalLayersFromPrintInfo
    ?? (suspiciousTaskDetailTotal ? null : totalLayersFromTaskDetail);
  const resolvedProgressPct = printInfo.progressPct
    ?? (
      resolvedCurrentLayer != null
      && resolvedTotalLayers != null
      && resolvedTotalLayers > 0
    ? Math.max(0, Math.min(100, (resolvedCurrentLayer / resolvedTotalLayers) * 100))
    : (resolvedTotalLayers != null ? taskDetailLayerProgress.progressPct : null)
    );

  const resolvedIsPrinting = printInfo.isPrinting || activeTaskRunning;
  const resolvedState = printInfo.isPaused
    ? 'paused'
    : resolvedIsPrinting
      ? 'printing'
      : printInfo.state;
  const resolvedStateText = printInfo.isPaused
    ? 'Paused'
    : resolvedIsPrinting
      ? 'Printing'
      : printInfo.stateText;
  const resolvedJobName = printInfo.jobName
    ?? activeTaskName
    ?? (activeTaskPath ? derivePlateNameFromPath(activeTaskPath) : null);
  const resolvedPlateKey = activeTaskPath
    ?? activeTaskId
    ?? printInfo.taskId
    ?? resolvedJobName
    ?? '';

  const enriched = await enrichSdcpDeviceIdentityViaWebSocket(probed, 1400);

  const attributesFrameRecord = parseSdcpMaybeObject(telemetry.attributesFrame as unknown);
  const attributesData = parseSdcpMaybeObject(attributesFrameRecord?.Data);
  const attributes = parseSdcpMaybeObject(attributesData?.Attributes)
    ?? parseSdcpMaybeObject(attributesFrameRecord?.Attributes);
  const firmwareVersionFromAttrs = typeof attributes?.FirmwareVersion === 'string'
    ? attributes.FirmwareVersion.trim()
    : '';

  return {
    status: 200,
    body: {
      ok: true,
      connected: true,
      mode: 'sdcp',
      hostName: enriched.hostName,
      printerName: enriched.printerName,
      printerModel: enriched.printerModel,
      ipAddress: enriched.ipAddress,
      port: enriched.port,
      mainboardId,
      firmwareVersion: firmwareVersionFromAttrs || enriched.firmwareVersion,
      stateText: resolvedStateText || enriched.statusText || 'Online',
      statusText: enriched.statusText,
      state: resolvedState || enriched.state || 'online',
      isPrinting: resolvedIsPrinting,
      isPaused: printInfo.isPaused,
      progressPct: resolvedProgressPct,
      currentLayer: resolvedCurrentLayer,
      totalLayers: resolvedTotalLayers,
      plateId: resolvedPlateKey ? hashPlateIdFromPath(resolvedPlateKey) : null,
      jobName: resolvedJobName,
      etaSec: printInfo.etaSec,
      taskId: activeTaskId ?? printInfo.taskId,
      taskStatus: activeTaskStatus,
      thumbnailPath: activeTaskThumbnailPath,
      taskDetailOk: taskDetailAck === null ? null : taskDetailAck === 0,
    },
  };
}

async function handleSdcpWebcamInfo(payload: unknown): Promise<HandlerResult> {
  const rawHost = typeof (payload as any)?.host === 'string'
    ? (payload as any).host
    : typeof (payload as any)?.ipAddress === 'string'
      ? (payload as any).ipAddress
      : '';
  const parsedHost = parseHostAndPort(rawHost);
  if (!parsedHost) {
    return { status: 400, body: { ok: false, available: false, message: 'Invalid host or IP address' } };
  }

  const directRtspUrl = `rtsp://${parsedHost.host}:554/video`;

  return {
    status: 200,
    body: {
      ok: true,
      available: true,
      streamUrl: directRtspUrl,
      snapshotUrl: null,
      message: 'Using direct SDCP RTSP stream (no local proxy).',
    },
  };
}

async function handleSdcpWebcamDisable(payload: unknown): Promise<HandlerResult> {
  return handleSdcpToggleFeature(payload, 386, 'webcam', false);
}

function normalizeSdcpStoragePath(value: unknown): string {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return '/local/';

  const lower = raw.toLowerCase();
  if (lower === 'local' || lower === '/local') return '/local/';
  if (lower === 'usb' || lower === '/usb') return '/usb/';
  if (lower.startsWith('/usb/')) return `/usb/${raw.slice(5).replace(/^\/+/, '')}`;
  if (lower.startsWith('/local/')) return `/local/${raw.slice(7).replace(/^\/+/, '')}`;
  return raw.startsWith('/') ? raw : `/${raw}`;
}

async function handleSdcpToggleFeature(payload: unknown, cmd: 386 | 387, featureLabel: string, enabled: boolean): Promise<HandlerResult> {
  const rawHost = typeof (payload as any)?.host === 'string'
    ? (payload as any).host
    : typeof (payload as any)?.ipAddress === 'string'
      ? (payload as any).ipAddress
      : '';
  const parsedHost = parseHostAndPort(rawHost);
  if (!parsedHost) {
    return { status: 400, body: { ok: false, error: 'Invalid host or IP address' } };
  }

  const port = clampNumber((payload as any)?.port, parsedHost.port, 1, 65535);
  const mainboardId = typeof (payload as any)?.mainboardId === 'string' && looksLikeMainboardId((payload as any).mainboardId)
    ? String((payload as any).mainboardId).trim()
    : await resolveMainboardIdForHost(parsedHost.host, port);
  if (!mainboardId) {
    return { status: 200, body: { ok: false, error: `Unable to resolve SDCP mainboard ID for ${featureLabel} command.` } };
  }

  const response = await sendSdcpCommandAndAwaitResponse({
    host: parsedHost.host,
    port,
    mainboardId,
    cmd,
    data: { Enable: enabled ? 1 : 0 },
    timeoutMs: 3200,
  });
  const ack = Number((response?.Data?.Data as any)?.Ack);
  const ackDescription = cmd === 386
    ? (ack === 0
      ? 'success'
      : ack === 1
        ? 'exceeded maximum simultaneous streaming limit'
        : ack === 2
          ? 'camera does not exist'
          : ack === 3
            ? 'unknown error'
            : Number.isFinite(ack)
              ? 'unknown Ack'
              : 'no/invalid Ack in SDCP response')
    : (ack === 0
      ? 'success'
      : ack === 1
        ? 'unknown error'
        : Number.isFinite(ack)
          ? 'unknown Ack'
          : 'no/invalid Ack in SDCP response');
  const ackLabel = Number.isFinite(ack) ? String(ack) : 'unknown';

  return {
    status: 200,
    body: {
      ok: ack === 0,
      ack,
      ackDescription,
      message: ack === 0
        ? `SDCP command ${featureLabel} ${enabled ? 'enable' : 'disable'} accepted.`
        : `SDCP command ${featureLabel} ${enabled ? 'enable' : 'disable'} rejected (Ack ${ackLabel}: ${ackDescription}).`,
      error: ack === 0 ? undefined : `SDCP ${featureLabel} ${enabled ? 'enable' : 'disable'} failed (Ack ${ackLabel}: ${ackDescription}).`,
      rawResponse: response ?? null,
    },
  };
}

async function handleSdcpPlatesList(payload: unknown): Promise<HandlerResult> {
  const rawHost = typeof (payload as any)?.host === 'string'
    ? (payload as any).host
    : typeof (payload as any)?.ipAddress === 'string'
      ? (payload as any).ipAddress
      : '';
  const parsedHost = parseHostAndPort(rawHost);
  if (!parsedHost) {
    return { status: 400, body: { ok: false, metadataReady: false, error: 'Invalid host or IP address', plates: [] } };
  }

  const port = clampNumber((payload as any)?.port, parsedHost.port, 1, 65535);
  const mainboardId = typeof (payload as any)?.mainboardId === 'string' && looksLikeMainboardId((payload as any).mainboardId)
    ? String((payload as any).mainboardId).trim()
    : await resolveMainboardIdForHost(parsedHost.host, port);
  if (!mainboardId) {
    return {
      status: 200,
      body: {
        ok: false,
        metadataReady: false,
        error: 'Unable to resolve SDCP mainboard ID for file list command.',
        matchedPlate: null,
        plates: [],
      },
    };
  }

  const response = await sendSdcpCommandAndAwaitResponse({
    host: parsedHost.host,
    port,
    mainboardId,
    cmd: 258,
    data: { Url: normalizeSdcpStoragePath((payload as any)?.storagePath ?? (payload as any)?.url ?? (payload as any)?.source) },
    timeoutMs: 3200,
  });
  const listAck = extractSdcpAck(response);
  const baseList = parseSdcpFileListFromResponse(response)
    .filter((entry) => {
      const rawType = Number(entry.type ?? entry.Type ?? 1);
      return !Number.isFinite(rawType) || rawType === 1;
    })
    .map((entry) => parseSdcpPlateRecord(entry));

  const historyResponse = await sendSdcpCommandAndAwaitResponse({
    host: parsedHost.host,
    port,
    mainboardId,
    cmd: 320,
    data: {},
    timeoutMs: 3200,
  });

  const historyAck = extractSdcpAck(historyResponse);
  const historyTaskIds = parseSdcpTaskIdsFromResponse(historyResponse).slice(0, 60);

  const detailResponse = historyTaskIds.length > 0
    ? await sendSdcpCommandAndAwaitResponse({
      host: parsedHost.host,
      port,
      mainboardId,
      cmd: 321,
      data: { TaskIdList: historyTaskIds, Id: historyTaskIds },
      timeoutMs: 4200,
    })
    : null;

  const detailAck = extractSdcpAck(detailResponse);
  const taskDetails = parseSdcpTaskDetailsFromResponse(detailResponse);

  const detailByFullPath = new Map<string, Record<string, unknown>>();
  const detailByTail = new Map<string, Record<string, unknown>>();

  for (const detail of taskDetails) {
    const detailPathCandidate = selectSdcpFirstDefined(detail, [
      'Filename', 'filename', 'FileName', 'fileName', 'Path', 'path', 'File', 'file',
    ]);
    const normalizedPath = normalizeSdcpComparablePath(detailPathCandidate);
    const normalizedTail = getSdcpPathTail(detailPathCandidate);
    if (normalizedPath && !detailByFullPath.has(normalizedPath)) detailByFullPath.set(normalizedPath, detail);
    if (normalizedTail && !detailByTail.has(normalizedTail)) detailByTail.set(normalizedTail, detail);
  }

  const list = baseList.map((plate) => {
    const normalizedPath = normalizeSdcpComparablePath(plate.Path ?? plate.path ?? plate.Name ?? plate.name);
    const normalizedTail = getSdcpPathTail(plate.Path ?? plate.path ?? plate.Name ?? plate.name);
    const matchedDetail = detailByFullPath.get(normalizedPath)
      ?? detailByTail.get(normalizedTail)
      ?? null;
    if (!matchedDetail) return plate;
    return mergeSdcpTaskDetailIntoPlate(plate, matchedDetail);
  });

  const requestedPlateId = Number((payload as any)?.plateId);
  const requestedJobName = typeof (payload as any)?.jobName === 'string' ? (payload as any).jobName.trim().toLowerCase() : '';
  const matchedPlate = list.find((entry) => {
    if (Number.isFinite(requestedPlateId) && requestedPlateId > 0) {
      return Number((entry as any).PlateID) === Math.round(requestedPlateId);
    }
    if (requestedJobName) {
      const path = String((entry as any).Path ?? '').toLowerCase();
      const name = String((entry as any).Name ?? '').toLowerCase();
      return path.includes(requestedJobName) || name.includes(requestedJobName);
    }
    return false;
  }) ?? null;

  return {
    status: 200,
    body: {
      ok: listAck === 0,
      metadataReady: matchedPlate != null || requestedJobName.length === 0,
      matchedPlate,
      plates: list,
      error: listAck === 0 ? undefined : 'SDCP file list request failed.',
      taskHistoryOk: historyAck === null ? null : historyAck === 0,
      taskDetailOk: detailAck === null ? null : detailAck === 0,
    },
  };
}

async function handleSdcpPlateDelete(payload: unknown): Promise<HandlerResult> {
  const rawHost = typeof (payload as any)?.host === 'string'
    ? (payload as any).host
    : typeof (payload as any)?.ipAddress === 'string'
      ? (payload as any).ipAddress
      : '';
  const parsedHost = parseHostAndPort(rawHost);
  if (!parsedHost) {
    return { status: 400, body: { ok: false, error: 'Invalid host or IP address' } };
  }

  const port = clampNumber((payload as any)?.port, parsedHost.port, 1, 65535);
  const mainboardId = typeof (payload as any)?.mainboardId === 'string' && looksLikeMainboardId((payload as any).mainboardId)
    ? String((payload as any).mainboardId).trim()
    : await resolveMainboardIdForHost(parsedHost.host, port);
  if (!mainboardId) {
    return { status: 200, body: { ok: false, error: 'Unable to resolve SDCP mainboard ID for delete command.' } };
  }

  const plateIdRaw = parseSdcpPositiveInteger((payload as any)?.plateId);
  const directFilename = typeof (payload as any)?.filename === 'string' ? (payload as any).filename.trim() : '';
  const directPath = typeof (payload as any)?.path === 'string' ? (payload as any).path.trim() : '';
  const directName = typeof (payload as any)?.jobName === 'string' ? (payload as any).jobName.trim() : '';

  let resolvedPath = directPath || directFilename;

  if (!resolvedPath) {
    const listResponse = await sendSdcpCommandAndAwaitResponse({
      host: parsedHost.host,
      port,
      mainboardId,
      cmd: 258,
      data: { Url: normalizeSdcpStoragePath((payload as any)?.storagePath ?? (payload as any)?.url ?? '/local/') },
      timeoutMs: 3200,
    });

    const plates = parseSdcpFileListFromResponse(listResponse)
      .filter((entry) => {
        const rawType = Number(entry.type ?? entry.Type ?? 1);
        return !Number.isFinite(rawType) || rawType === 1;
      })
      .map((entry) => parseSdcpPlateRecord(entry));

    const normalizedName = normalizeSdcpComparablePath(directName);

    const matched = plates.find((plate) => {
      const candidatePath = String(plate.Path ?? plate.path ?? '').trim();
      const candidateId = parseSdcpPositiveInteger(plate.PlateID ?? plate.plateId);
      if (plateIdRaw != null && candidateId != null && candidateId === plateIdRaw) return true;
      if (!normalizedName) return false;
      const comparablePath = normalizeSdcpComparablePath(candidatePath);
      const comparableTail = getSdcpPathTail(candidatePath);
      return comparablePath.includes(normalizedName) || comparableTail === normalizedName;
    }) ?? null;

    resolvedPath = matched ? String(matched.Path ?? matched.path ?? '').trim() : '';
  }

  if (!resolvedPath) {
    return {
      status: 400,
      body: {
        ok: false,
        error: 'Unable to resolve SDCP file path for delete command. Provide path or filename.',
      },
    };
  }

  const response = await sendSdcpCommandAndAwaitResponse({
    host: parsedHost.host,
    port,
    mainboardId,
    cmd: 259,
    data: {
      FileList: [resolvedPath],
      FolderList: [],
    },
    timeoutMs: 3200,
  });

  const ack = extractSdcpAck(response);
  return {
    status: 200,
    body: {
      ok: ack === 0,
      ack,
      path: resolvedPath,
      message: ack === 0
        ? `Deleted SDCP plate file ${resolvedPath}.`
        : `SDCP plate delete rejected (Ack ${ack == null ? 'unknown' : ack}).`,
      error: ack === 0 ? undefined : 'SDCP plate delete failed.',
    },
  };
}

async function handleSdcpTaskHistoryList(payload: unknown): Promise<HandlerResult> {
  const rawHost = typeof (payload as any)?.host === 'string'
    ? (payload as any).host
    : typeof (payload as any)?.ipAddress === 'string'
      ? (payload as any).ipAddress
      : '';
  const parsedHost = parseHostAndPort(rawHost);
  if (!parsedHost) {
    return { status: 400, body: { ok: false, error: 'Invalid host or IP address' } };
  }

  const port = clampNumber((payload as any)?.port, parsedHost.port, 1, 65535);
  const mainboardId = typeof (payload as any)?.mainboardId === 'string' && looksLikeMainboardId((payload as any).mainboardId)
    ? String((payload as any).mainboardId).trim()
    : await resolveMainboardIdForHost(parsedHost.host, port);
  if (!mainboardId) {
    return { status: 200, body: { ok: false, error: 'Unable to resolve SDCP mainboard ID for task history command.' } };
  }

  const response = await sendSdcpCommandAndAwaitResponse({
    host: parsedHost.host,
    port,
    mainboardId,
    cmd: 320,
    data: {},
    timeoutMs: 3200,
  });
  const ack = extractSdcpAck(response);
  const taskIds = parseSdcpTaskIdsFromResponse(response);

  return {
    status: 200,
    body: {
      ok: ack === 0,
      ack,
      taskIds,
      error: ack === 0 ? undefined : 'SDCP task history request failed.',
      rawResponse: response ?? null,
    },
  };
}

async function handleSdcpTaskDetails(payload: unknown): Promise<HandlerResult> {
  const rawHost = typeof (payload as any)?.host === 'string'
    ? (payload as any).host
    : typeof (payload as any)?.ipAddress === 'string'
      ? (payload as any).ipAddress
      : '';
  const parsedHost = parseHostAndPort(rawHost);
  if (!parsedHost) {
    return { status: 400, body: { ok: false, error: 'Invalid host or IP address' } };
  }

  const port = clampNumber((payload as any)?.port, parsedHost.port, 1, 65535);
  const mainboardId = typeof (payload as any)?.mainboardId === 'string' && looksLikeMainboardId((payload as any).mainboardId)
    ? String((payload as any).mainboardId).trim()
    : await resolveMainboardIdForHost(parsedHost.host, port);
  if (!mainboardId) {
    return { status: 200, body: { ok: false, error: 'Unable to resolve SDCP mainboard ID for task details command.' } };
  }

  const providedTaskIds = Array.isArray((payload as any)?.taskIds)
    ? (payload as any).taskIds
      .filter((value: unknown): value is string => typeof value === 'string' && value.trim().length > 0)
      .map((value: string) => value.trim())
    : [];

  const taskIds = providedTaskIds.length > 0
    ? providedTaskIds.slice(0, 60)
    : parseSdcpTaskIdsFromResponse(await sendSdcpCommandAndAwaitResponse({
      host: parsedHost.host,
      port,
      mainboardId,
      cmd: 320,
      data: {},
      timeoutMs: 3200,
    })).slice(0, 60);

  if (taskIds.length === 0) {
    return {
      status: 200,
      body: {
        ok: true,
        ack: 0,
        taskIds: [],
        taskDetails: [],
      },
    };
  }

  const response = await sendSdcpCommandAndAwaitResponse({
    host: parsedHost.host,
    port,
    mainboardId,
    cmd: 321,
    data: { TaskIdList: taskIds, Id: taskIds },
    timeoutMs: 4200,
  });
  const ack = extractSdcpAck(response);
  const taskDetails = parseSdcpTaskDetailsFromResponse(response);

  return {
    status: 200,
    body: {
      ok: ack === 0,
      ack,
      taskIds,
      taskDetails,
      error: ack === 0 ? undefined : 'SDCP task detail request failed.',
      rawResponse: response ?? null,
    },
  };
}

async function resolveSdcpStartFilename(
  payload: unknown,
  host: string,
  port: number,
  mainboardId: string,
): Promise<string> {
  const directFilename = typeof (payload as any)?.filename === 'string'
    ? (payload as any).filename.trim()
    : '';
  if (directFilename) return directFilename;

  const directPath = typeof (payload as any)?.path === 'string'
    ? (payload as any).path.trim()
    : '';
  if (directPath) return directPath;

  const jobName = typeof (payload as any)?.jobName === 'string'
    ? (payload as any).jobName.trim()
    : '';
  if (jobName) {
    const withExt = jobName.replace(/\.[^.]+$/i, '');
    return `${withExt}.ctb`;
  }

  const plateId = parseSdcpPositiveInteger((payload as any)?.plateId);
  const plateName = typeof (payload as any)?.plateName === 'string'
    ? (payload as any).plateName.trim()
    : '';
  const normalizedPlateName = normalizeSdcpComparablePath(plateName);

  if (plateId == null && !normalizedPlateName) return '';

  const listResponse = await sendSdcpCommandAndAwaitResponse({
    host,
    port,
    mainboardId,
    cmd: 258,
    data: { Url: normalizeSdcpStoragePath((payload as any)?.storagePath ?? (payload as any)?.url ?? '/local/') },
    timeoutMs: 3200,
  });

  const plates = parseSdcpFileListFromResponse(listResponse)
    .filter((entry) => {
      const rawType = Number(entry.type ?? entry.Type ?? 1);
      return !Number.isFinite(rawType) || rawType === 1;
    })
    .map((entry) => parseSdcpPlateRecord(entry));

  const matched = plates.find((plate) => {
    const candidatePath = String(plate.Path ?? plate.path ?? '').trim();
    const candidateId = parseSdcpPositiveInteger(plate.PlateID ?? plate.plateId);
    if (plateId != null && candidateId != null && candidateId === plateId) return true;
    if (!normalizedPlateName) return false;
    const comparablePath = normalizeSdcpComparablePath(candidatePath);
    const comparableTail = getSdcpPathTail(candidatePath);
    return comparablePath.includes(normalizedPlateName) || comparableTail === normalizedPlateName;
  }) ?? null;

  if (!matched) return '';
  return String(matched.Path ?? matched.path ?? '').trim();
}

async function handleSdcpControlOperation(payload: unknown, cmd: number, opLabel: string): Promise<HandlerResult> {
  const rawHost = typeof (payload as any)?.host === 'string'
    ? (payload as any).host
    : typeof (payload as any)?.ipAddress === 'string'
      ? (payload as any).ipAddress
      : '';
  const parsedHost = parseHostAndPort(rawHost);
  if (!parsedHost) {
    return { status: 400, body: { ok: false, error: 'Invalid host or IP address' } };
  }

  const port = clampNumber((payload as any)?.port, parsedHost.port, 1, 65535);
  const mainboardId = typeof (payload as any)?.mainboardId === 'string' && looksLikeMainboardId((payload as any).mainboardId)
    ? String((payload as any).mainboardId).trim()
    : await resolveMainboardIdForHost(parsedHost.host, port);
  if (!mainboardId) {
    return { status: 200, body: { ok: false, error: 'Unable to resolve SDCP mainboard ID for control command.' } };
  }

  const controlData: Record<string, unknown> = {};
  if (cmd === 128) {
    const filename = await resolveSdcpStartFilename(payload, parsedHost.host, port, mainboardId);
    if (!filename) {
      return {
        status: 400,
        body: {
          ok: false,
          error: 'Start printing requires filename/path/jobName, or a resolvable plateId for SDCP Cmd 128.',
        },
      };
    }
    controlData.Filename = filename;
    controlData.StartLayer = 0;
  }

  const response = await sendSdcpCommandAndAwaitResponse({
    host: parsedHost.host,
    port,
    mainboardId,
    cmd,
    data: controlData,
    timeoutMs: 3200,
  });
  const ack = Number((response?.Data?.Data as any)?.Ack);

  return {
    status: 200,
    body: {
      ok: ack === 0,
      ack,
      message: ack === 0
        ? `SDCP command ${opLabel} accepted.`
        : `SDCP command ${opLabel} rejected (Ack ${Number.isFinite(ack) ? ack : 'unknown'}).`,
      error: ack === 0 ? undefined : `SDCP ${opLabel} failed.`,
    },
  };
}

function handleUnsupportedSdcpOperation(op: string): HandlerResult {
  return {
    status: 404,
    body: {
      error: `Unsupported SDCP operation: ${op}`,
      note: 'SDCP backend does not expose remote material profile operations.',
    },
  };
}

export async function handleSdcpV3NetworkOperation(operationPath: string[], payload: unknown): Promise<HandlerResult> {
  if (operationPath.length === 0 || operationPath[0] !== 'sdcp') {
    return { status: 404, body: { error: 'Unknown SDCP network operation' } };
  }

  const op = operationPath.slice(1).join('/');

  if (op === 'connect') return handleSdcpConnect(payload);
  if (op === 'discover') return handleSdcpDiscover(payload);
  if (op === 'printer/status') return handleSdcpPrinterStatus(payload);
  if (op === 'printer/webcam/info') return handleSdcpWebcamInfo(payload);
  if (op === 'printer/webcam/disable') return handleSdcpWebcamDisable(payload);
  if (op === 'printer/webcam/enable') return handleSdcpToggleFeature(payload, 386, 'webcam', true);
  if (op === 'printer/timelapse/enable') return handleSdcpToggleFeature(payload, 387, 'timelapse', true);
  if (op === 'printer/timelapse/disable') return handleSdcpToggleFeature(payload, 387, 'timelapse', false);
  if (op === 'plates/list/json') return handleSdcpPlatesList(payload);
  if (op === 'printer/start') return handleSdcpControlOperation(payload, 128, op);
  if (op === 'printer/pause') return handleSdcpControlOperation(payload, 129, op);
  if (op === 'printer/cancel') return handleSdcpControlOperation(payload, 130, op);
  if (op === 'printer/stop' || op === 'printer/force-stop') return handleSdcpControlOperation(payload, 130, op);
  if (op === 'printer/resume') return handleSdcpControlOperation(payload, 131, op);
  if (op === 'printer/unpause') return handleSdcpControlOperation(payload, 131, op);
  if (op === 'upload/chunk') return handleSdcpUploadChunk(payload);
  if (op === 'plate/delete') return handleSdcpPlateDelete(payload);
  if (op === 'task/history/list') return handleSdcpTaskHistoryList(payload);
  if (op === 'task/details') return handleSdcpTaskDetails(payload);
  if (op === 'materials' || op === 'materials/edit' || op === 'unsupported') return handleUnsupportedSdcpOperation(op);

  return { status: 404, body: { error: `Unknown SDCP operation: ${op}` } };
}

export const handlePluginNetworkOperation = handleSdcpV3NetworkOperation;
