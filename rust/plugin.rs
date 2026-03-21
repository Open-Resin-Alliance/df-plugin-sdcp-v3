use std::sync::Arc;

use crate::plugin_registry::{FormatProvider, PluginRegistration};

pub struct SdcpV3FormatProvider;

impl FormatProvider for SdcpV3FormatProvider {
    fn default_export_format(&self) -> &'static str {
        "print"
    }
}

pub fn get_plugin_registration() -> PluginRegistration {
    PluginRegistration {
        name: "sdcp-v3".to_string(),
        network_handler: None,
        format_provider: Some(Arc::new(SdcpV3FormatProvider)),
    }
}
