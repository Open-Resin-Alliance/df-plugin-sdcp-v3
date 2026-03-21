import type {
  ComplexPluginDefinition,
  PluginNetworkUiAdapterContract,
} from '@/features/plugins/complexPluginContracts';
import { SDCP_V3_PLUGIN_MANIFEST } from './pluginManifest';

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
};

export default SDCP_V3_COMPLEX_PLUGIN_DEFINITION;
