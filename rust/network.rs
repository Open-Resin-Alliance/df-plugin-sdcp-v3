use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tungstenite::{stream::MaybeTlsStream, Message};

#[derive(Clone, Serialize)]
pub struct PluginNetworkResponse {
    pub status: u16,
    pub body: Value,
}

static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();

const DEFAULT_SDCP_PORT: u16 = 3030;
const DEFAULT_SDCP_DISCOVERY_PORT: u16 = 3000;

fn http_client() -> &'static Client {
    HTTP_CLIENT.get_or_init(|| {
        Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(4)
            .no_proxy()
            .build()
            .expect("failed to create SDCP HTTP client")
    })
}

fn parse_host_and_port(input: &str) -> Option<(String, u16)> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }

    let without_scheme = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))
        .unwrap_or(trimmed);

    let authority = without_scheme.split('/').next().unwrap_or("");
    if authority.is_empty() {
        return None;
    }

    if let Some(colon_idx) = authority.rfind(':') {
        let host_part = authority[..colon_idx].trim();
        let port_part = authority[colon_idx + 1..].trim();
        if !host_part.is_empty() {
            if let Ok(port) = port_part.parse::<u16>() {
                if port >= 1 {
                    return Some((host_part.to_string(), port));
                }
            }
        }
    }

    Some((authority.to_string(), DEFAULT_SDCP_PORT))
}

fn resolve_port(value: Option<&Value>, fallback: u16) -> u16 {
    value
        .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
        .and_then(|v| u16::try_from(v).ok())
        .filter(|&p| p >= 1)
        .unwrap_or(fallback)
}

fn resolve_raw_host(payload: &Value) -> String {
    payload
        .get("host")
        .or_else(|| payload.get("ipAddress"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn to_broadcast_address(ip: Ipv4Addr, mask: Ipv4Addr) -> Ipv4Addr {
    let ip_u32 = u32::from(ip);
    let mask_u32 = u32::from(mask);
    Ipv4Addr::from((ip_u32 & mask_u32) | (!mask_u32))
}

fn find_deep_string(node: &Value, keys: &[&str]) -> String {
    fn visit(node: &Value, keys: &[&str]) -> String {
        match node {
            Value::Array(items) => {
                for item in items {
                    let found = visit(item, keys);
                    if !found.is_empty() {
                        return found;
                    }
                }
                String::new()
            }
            Value::Object(map) => {
                for key in keys {
                    if let Some(value) = map.get(*key).and_then(|v| v.as_str()) {
                        let trimmed = value.trim();
                        if !trimmed.is_empty() {
                            return trimmed.to_string();
                        }
                    }
                }

                for value in map.values() {
                    let found = visit(value, keys);
                    if !found.is_empty() {
                        return found;
                    }
                }

                String::new()
            }
            _ => String::new(),
        }
    }

    let normalized_keys: Vec<String> = keys.iter().map(|k| k.to_lowercase()).collect();

    fn visit_case_insensitive(node: &Value, normalized_keys: &[String]) -> String {
        match node {
            Value::Array(items) => {
                for item in items {
                    let found = visit_case_insensitive(item, normalized_keys);
                    if !found.is_empty() {
                        return found;
                    }
                }
                String::new()
            }
            Value::Object(map) => {
                for (key, value) in map {
                    if normalized_keys.iter().any(|k| k == &key.to_lowercase()) {
                        if let Some(text) = value.as_str() {
                            let trimmed = text.trim();
                            if !trimmed.is_empty() {
                                return trimmed.to_string();
                            }
                        }
                    }
                }

                for value in map.values() {
                    let found = visit_case_insensitive(value, normalized_keys);
                    if !found.is_empty() {
                        return found;
                    }
                }

                String::new()
            }
            _ => String::new(),
        }
    }

    let exact = visit(node, keys);
    if !exact.is_empty() {
        return exact;
    }

    visit_case_insensitive(node, &normalized_keys)
}

fn parse_sdcp_discovery_response(text: &str) -> Value {
    let trimmed = text.trim();
    match serde_json::from_str::<Value>(trimmed) {
        Ok(parsed) => {
            let ip_address = find_deep_string(&parsed, &["MainboardIP", "ipAddress", "ip"]);
            let host_name = find_deep_string(&parsed, &["MainboardID", "hostName", "hostname"]);
            let printer_name =
                find_deep_string(&parsed, &["Name", "PrinterName", "printerName", "machineName"]);
            let printer_model = find_deep_string(&parsed, &["Model", "printerModel"]);
            let firmware_version = find_deep_string(&parsed, &["Version", "firmwareVersion"]);

            json!({
                "MainboardIP": ip_address,
                "MainboardID": host_name,
                "Name": printer_name,
                "Model": if printer_model.is_empty() { "SDCP 3.0.0" } else { printer_model.as_str() },
                "Version": firmware_version,
            })
        }
        Err(_) => {
            fn extract_loose_value(text: &str, keys: &[&str]) -> String {
                let lower_text = text.to_lowercase();

                for key in keys {
                    let lower_key = key.to_lowercase();
                    let mut search_from = 0usize;

                    while let Some(relative_pos) = lower_text[search_from..].find(&lower_key) {
                        let key_pos = search_from + relative_pos;
                        let mut cursor = key_pos + lower_key.len();

                        while cursor < lower_text.len() {
                            let c = lower_text.as_bytes()[cursor] as char;
                            if c.is_whitespace() || c == '"' {
                                cursor += 1;
                                continue;
                            }
                            break;
                        }

                        if cursor >= lower_text.len() {
                            break;
                        }

                        let separator = lower_text.as_bytes()[cursor] as char;
                        if separator != ':' && separator != '=' {
                            search_from = key_pos + 1;
                            continue;
                        }
                        cursor += 1;

                        while cursor < lower_text.len() {
                            let c = lower_text.as_bytes()[cursor] as char;
                            if c.is_whitespace() || c == '"' {
                                cursor += 1;
                                continue;
                            }
                            break;
                        }

                        let start = cursor;
                        while cursor < lower_text.len() {
                            let c = lower_text.as_bytes()[cursor] as char;
                            if c == '"' || c == ',' || c == ';' || c == '\n' || c == '\r' || c == '}' {
                                break;
                            }
                            cursor += 1;
                        }

                        if start < cursor {
                            let raw_value = text[start..cursor].trim();
                            if !raw_value.is_empty() {
                                return raw_value.to_string();
                            }
                        }

                        search_from = key_pos + 1;
                    }
                }

                String::new()
            }

            let ip_address = extract_loose_value(trimmed, &["MainboardIP", "ipAddress", "ip"]);
            let host_name = extract_loose_value(trimmed, &["MainboardID", "hostName", "hostname"]);
            let printer_name = extract_loose_value(trimmed, &["Name", "PrinterName", "printerName", "machineName"]);
            let printer_model = extract_loose_value(trimmed, &["Model", "printerModel"]);
            let firmware_version = extract_loose_value(trimmed, &["Version", "firmwareVersion"]);

            json!({
                "MainboardIP": ip_address,
                "MainboardID": host_name,
                "Name": printer_name,
                "Model": if printer_model.is_empty() { "SDCP 3.0.0" } else { printer_model.as_str() },
                "Version": firmware_version,
            })
        }
    }
}

fn has_useful_identity_fields(identity: &Value) -> bool {
    let host_name = identity
        .get("MainboardID")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    let printer_name = identity
        .get("Name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    let printer_model = identity
        .get("Model")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();

    !host_name.is_empty() || !printer_name.is_empty() || !printer_model.is_empty()
}

fn resolve_sdcp_identity_via_websocket(host: &str, port: u16, timeout_ms: u64) -> Option<Value> {
    let url = format!("ws://{host}:{port}/websocket");
    let (mut socket, _) = tungstenite::connect(url.as_str()).ok()?;

    if let MaybeTlsStream::Plain(stream) = socket.get_mut() {
        let _ = stream.set_read_timeout(Some(Duration::from_millis(timeout_ms.clamp(300, 4000))));
    }

    let _ = socket.send(Message::Text("ping".into()));

    let started = Instant::now();
    let deadline = Duration::from_millis(timeout_ms.clamp(300, 4000));
    while started.elapsed() < deadline {
        match socket.read() {
            Ok(Message::Text(text)) => {
                let parsed = parse_sdcp_discovery_response(&text);
                if has_useful_identity_fields(&parsed) {
                    return Some(parsed);
                }
            }
            Ok(Message::Binary(data)) => {
                let text = String::from_utf8_lossy(&data);
                let parsed = parse_sdcp_discovery_response(&text);
                if has_useful_identity_fields(&parsed) {
                    return Some(parsed);
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }

    None
}

fn enrich_sdcp_device_identity_via_websocket(mut device: Value, timeout_ms: u64) -> Value {
    let host = device
        .get("ipAddress")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if host.is_empty() {
        return device;
    }

    let port = device
        .get("port")
        .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
        .and_then(|v| u16::try_from(v).ok())
        .filter(|&p| p >= 1)
        .unwrap_or(DEFAULT_SDCP_PORT);

    let Some(identity) = resolve_sdcp_identity_via_websocket(&host, port, timeout_ms) else {
        return device;
    };

    let host_name = identity
        .get("MainboardID")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    let printer_name = identity
        .get("Name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    let printer_model = identity
        .get("Model")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    let firmware_version = identity
        .get("Version")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();

    if let Some(obj) = device.as_object_mut() {
        if !host_name.is_empty() {
            obj.insert("hostName".to_string(), Value::String(host_name.to_string()));
        }
        if !printer_name.is_empty() {
            obj.insert("printerName".to_string(), Value::String(printer_name.to_string()));
        }
        if !printer_model.is_empty() {
            obj.insert("printerModel".to_string(), Value::String(printer_model.to_string()));
        }
        if !firmware_version.is_empty() {
            obj.insert(
                "firmwareVersion".to_string(),
                Value::String(firmware_version.to_string()),
            );
        }
    }

    device
}

fn discover_sdcp_devices_via_udp(timeout_ms: u64) -> Vec<Value> {
    let socket = match UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)) {
        Ok(socket) => socket,
        Err(_) => return Vec::new(),
    };

    if socket.set_broadcast(true).is_err() {
        return Vec::new();
    }

    let _ = socket.set_read_timeout(Some(Duration::from_millis(120)));

    let mut targets: Vec<Ipv4Addr> = vec![Ipv4Addr::new(255, 255, 255, 255)];
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            if iface.is_loopback() {
                continue;
            }

            if let if_addrs::IfAddr::V4(v4) = iface.addr {
                targets.push(to_broadcast_address(v4.ip, v4.netmask));
            }
        }
    }

    let packet = b"M99999";
    for target in targets {
        let _ = socket.send_to(packet, SocketAddrV4::new(target, DEFAULT_SDCP_DISCOVERY_PORT));
    }

    let mut seen = HashSet::new();
    let mut found = Vec::new();
    let start = Instant::now();
    let total_timeout = Duration::from_millis(timeout_ms.clamp(300, 12_000));

    while start.elapsed() < total_timeout {
        let mut buffer = [0u8; 4096];
        match socket.recv_from(&mut buffer) {
            Ok((size, sender)) => {
                let ip = sender.ip().to_string();
                if !seen.insert(ip.clone()) {
                    continue;
                }

                let parsed = parse_sdcp_discovery_response(std::str::from_utf8(&buffer[..size]).unwrap_or(""));

                let host_name = parsed
                    .get("MainboardID")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                let printer_name = parsed
                    .get("Name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();
                let printer_model = parsed
                    .get("Model")
                    .and_then(|v| v.as_str())
                    .unwrap_or("SDCP 3.0.0")
                    .trim()
                    .to_string();
                let firmware_version = parsed
                    .get("Version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_string();

                found.push(json!({
                    "ipAddress": ip,
                    "port": DEFAULT_SDCP_PORT,
                    "hostName": if host_name.is_empty() { sender.ip().to_string() } else { host_name },
                    "printerName": if printer_name.is_empty() { "SDCP Printer" } else { printer_name.as_str() },
                    "printerModel": printer_model,
                    "statusText": "Discovered via SDCP UDP broadcast",
                    "state": "online",
                    "firmwareVersion": firmware_version,
                }));
            }
            Err(_) => {
                // timeout tick; continue until total timeout elapses
            }
        }
    }

    found
}

async fn probe_sdcp_host(host: &str, port: u16, timeout_ms: u64) -> Option<Value> {
    let response = http_client()
        .head(format!("http://{host}:{port}/uploadFile/upload"))
        .timeout(Duration::from_millis(timeout_ms))
        .send()
        .await
        .ok()?;

    let status = response.status().as_u16();

    Some(json!({
        "ipAddress": host,
        "port": port,
        "hostName": host,
        "printerName": "SDCP Printer",
        "printerModel": "SDCP 3.0.0",
        "statusText": format!("Reachable (HTTP {status})"),
        "state": "online",
        "firmwareVersion": "",
    }))
}

async fn sdcp_connect(payload: &Value) -> (u16, Value) {
    let raw_host = resolve_raw_host(payload);
    let parsed = match parse_host_and_port(&raw_host) {
        Some(parsed) => parsed,
        None => return (400, json!({ "error": "Invalid host or IP address" })),
    };

    let port = resolve_port(payload.get("port"), parsed.1);
    match probe_sdcp_host(&parsed.0, port, 3500).await {
        Some(device) => {
            let enriched = enrich_sdcp_device_identity_via_websocket(device, 1800);
            (
            200,
            json!({
                "connected": true,
                "mode": "sdcp",
                "hostName": enriched.get("hostName").cloned().unwrap_or(Value::String(parsed.0.clone())),
                "printerName": enriched.get("printerName").cloned().unwrap_or(Value::String("SDCP Printer".to_string())),
                "printerModel": enriched.get("printerModel").cloned().unwrap_or(Value::String("SDCP 3.0.0".to_string())),
                "ipAddress": enriched.get("ipAddress").cloned().unwrap_or(Value::String(parsed.0.clone())),
                "port": port,
                "statusText": enriched.get("statusText").cloned().unwrap_or(Value::String("Reachable".to_string())),
                "state": enriched.get("state").cloned().unwrap_or(Value::String("online".to_string())),
                "firmwareVersion": enriched.get("firmwareVersion").cloned().unwrap_or(Value::String("".to_string())),
            }),
        )
        }
        None => (
            200,
            json!({
                "connected": false,
                "mode": "sdcp",
                "hostName": parsed.0,
                "printerName": "",
                "ipAddress": parsed.0,
                "port": port,
                "statusText": "SDCP host unreachable",
                "state": "",
                "firmwareVersion": "",
            }),
        ),
    }
}

async fn sdcp_discover(payload: &Value) -> (u16, Value) {
    let mode = payload.get("mode").and_then(|v| v.as_str()).unwrap_or("sdcp");
    if mode != "sdcp" {
        return (400, json!({ "error": "Unsupported network mode" }));
    }

    let scan_scope = match payload.get("scanScope").and_then(|v| v.as_str()) {
        Some("all") | Some("local-hostnames") | Some("subnet") => {
            payload.get("scanScope").and_then(|v| v.as_str()).unwrap_or("all")
        }
        _ => "all",
    };

    let progressive = payload
        .get("progressive")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let batch_start = payload
        .get("batchStart")
        .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
        .unwrap_or(0);

    let probe_timeout_ms = payload
        .get("probeTimeoutMs")
        .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
        .map(|v| v.clamp(250, 8000))
        .unwrap_or(1200);

    let mut devices = discover_sdcp_devices_via_udp(probe_timeout_ms);

    // Optional fallback probe for manually supplied host when UDP is blocked.
    let raw_host = resolve_raw_host(payload);
    if let Some((host, port)) = parse_host_and_port(&raw_host) {
        let already_present = devices.iter().any(|device| {
            let ip_matches = device
                .get("ipAddress")
                .and_then(|v| v.as_str())
                .map(|ip| ip.eq_ignore_ascii_case(&host))
                .unwrap_or(false);
            let host_matches = device
                .get("hostName")
                .and_then(|v| v.as_str())
                .map(|name| name.eq_ignore_ascii_case(&host))
                .unwrap_or(false);
            ip_matches || host_matches
        });

        if !already_present {
            if let Some(device) = probe_sdcp_host(&host, port, probe_timeout_ms).await {
                devices.push(device);
            }
        }
    }

    devices = devices
        .into_iter()
        .map(|device| enrich_sdcp_device_identity_via_websocket(device, 1500))
        .collect();

    (
        200,
        json!({
            "mode": "sdcp",
            "devices": devices,
            "scannedHosts": devices.len(),
            "scannedEndpoints": devices.len(),
            "scannedLocalHostnames": 0,
            "scannedSubnetHosts": 0,
            "scanScope": scan_scope,
            "progressive": progressive,
            "totalEndpoints": devices.len(),
            "batchStart": batch_start,
            "batchSize": devices.len(),
            "nextBatchStart": batch_start,
            "done": true,
        }),
    )
}

fn unsupported_operation_response(operation: &str) -> (u16, Value) {
    (
        404,
        json!({
            "error": format!("Unsupported SDCP operation: {operation}"),
            "note": "SDCP backend does not expose remote material profile operations.",
        }),
    )
}

async fn handle_sdcp_network(operation: &str, payload: &Value) -> (u16, Value) {
    let normalized_operation = operation.trim().trim_start_matches('/').trim_end_matches('/');
    let op = normalized_operation
        .strip_prefix("sdcp/")
        .unwrap_or(normalized_operation);

    match op {
        "connect" => sdcp_connect(payload).await,
        "discover" => sdcp_discover(payload).await,
        "materials" | "materials/edit" | "unsupported" => unsupported_operation_response(op),
        _ => (
            404,
            json!({ "error": format!("Unknown SDCP operation: {normalized_operation}") }),
        ),
    }
}

pub async fn dispatch_plugin_network_request(request_json: String) -> Result<PluginNetworkResponse, String> {
    let request: Value =
        serde_json::from_str(&request_json).map_err(|e| format!("Invalid request JSON: {e}"))?;

    let plugin_id = request
        .get("pluginId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_lowercase();
    let operation = request
        .get("operation")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if plugin_id.is_empty() {
        return Ok(PluginNetworkResponse {
            status: 400,
            body: json!({ "error": "pluginId is required" }),
        });
    }

    if operation.is_empty() {
        return Ok(PluginNetworkResponse {
            status: 400,
            body: json!({ "error": "operation is required" }),
        });
    }

    let (status, body) = match plugin_id.as_str() {
        "sdcp-v3" => handle_sdcp_network(&operation, &request).await,
        _ => (
            404,
            json!({ "error": format!("Unknown network plugin: {plugin_id}") }),
        ),
    };

    Ok(PluginNetworkResponse { status, body })
}
