import type { PrinterPreset } from '../../src/features/profiles/profileStore';

export const SDCP_V3_PLUGIN_MANIFEST = {
  schemaVersion: 1,
  id: 'sdcp-v3-builtin',
  name: 'SDCP v3 Plugin',
  version: '0.1.0',
  description: 'Native SDCP 3.0.0 network backend integration for DragonFruit.',
  homepage: 'https://github.com/Open-Resin-Alliance/df-plugin-sdcp-v3',
  printerPresets: [] as PrinterPreset[],
  materialTemplates: [],
};
