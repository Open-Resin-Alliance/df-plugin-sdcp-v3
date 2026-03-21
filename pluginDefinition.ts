import type {
  ComplexPluginDefinition,
  PluginMonitoringUiAdapterContract,
  PluginNetworkUiAdapterContract,
} from '@/features/plugins/complexPluginContracts';
import { SDCP_V3_PLUGIN_MANIFEST } from './pluginManifest';
import {
  resolveSdcpMonitoringSnapshot,
  resolveSdcpWebcamFeedInfo,
} from './network/sdcpMonitoring';

const SDCP_NETWORK_ADAPTER: PluginNetworkUiAdapterContract = {
  mode: 'sdcp',
  pluginId: 'sdcp-v3',
  displayName: 'SDCP 3.0.0',
  operationNamespace: 'sdcp',
  supportsRemoteMaterialProfiles: false,
  operations: {
    connect: 'sdcp/connect',
    discover: 'sdcp/discover',
    materials: 'sdcp/unsupported',
    materialsEdit: 'sdcp/unsupported',
  },
  defaultLocalHostnames: ['sdcp.local', 'printer.local', 'photon.local'],
  primaryEditFields: [],
  basicSections: [],
  advancedSections: [],
  resolveEditDraftFromMeta: () => ({}),
  resolveMaterialProcessValues: () => ({}),
  denormalizeEditDraftForBackend: () => ({}),
  resolveAdvancedSectionId: () => 'general',
  getFieldHelpText: () => 'This SDCP backend does not expose remote material editing yet.',
  isDynamicWaitEnabled: () => false,
};

const SDCP_MONITORING_ADAPTER: PluginMonitoringUiAdapterContract = {
  mode: 'sdcp',
  pluginId: 'sdcp-v3',
  displayName: 'SDCP Monitoring',
  available: true,
  operations: {
    status: 'sdcp/printer/status',
    webcamInfo: 'sdcp/printer/webcam/info',
    platesList: 'sdcp/plates/list/json',
    start: 'sdcp/printer/start',
    deletePlate: 'sdcp/plate/delete',
    pause: 'sdcp/printer/pause',
    resume: 'sdcp/printer/unpause',
    cancel: 'sdcp/printer/stop',
    emergencyStop: 'sdcp/printer/force-stop',
  },
  parseStatusPayload: (payload: unknown) => resolveSdcpMonitoringSnapshot(payload),
  parseWebcamInfoPayload: (payload: unknown, host: string, port: number) => resolveSdcpWebcamFeedInfo(payload, host, port),
};

export const SDCP_V3_COMPLEX_PLUGIN_DEFINITION: ComplexPluginDefinition = {
  id: 'sdcp-v3',
  manifest: SDCP_V3_PLUGIN_MANIFEST,
  capabilities: {
    networkOperations: true,
    uploadWithProgress: true,
    slicerEncoder: false,
    tauriRuntimePlugin: true,
  },
  networkAdaptersByMode: {
    [SDCP_NETWORK_ADAPTER.mode]: SDCP_NETWORK_ADAPTER,
  },
  monitoringAdaptersByMode: {
    [SDCP_MONITORING_ADAPTER.mode]: SDCP_MONITORING_ADAPTER,
  },
};

export default SDCP_V3_COMPLEX_PLUGIN_DEFINITION;
