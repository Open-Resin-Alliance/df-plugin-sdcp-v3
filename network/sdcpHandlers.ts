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

const DEFAULT_SDCP_PORT = 3030;
const DEFAULT_SDCP_DISCOVERY_PORT = 3000;

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
  if (op === 'materials' || op === 'materials/edit' || op === 'unsupported') return handleUnsupportedSdcpOperation(op);

  return { status: 404, body: { error: `Unknown SDCP operation: ${op}` } };
}

export const handlePluginNetworkOperation = handleSdcpV3NetworkOperation;
