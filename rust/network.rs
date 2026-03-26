use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tungstenite::{stream::MaybeTlsStream, Message};
use base64::Engine;

#[derive(Clone, Serialize)]
pub struct PluginNetworkResponse {
    pub status: u16,
    pub body: Value,
}

static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();
static WEBCAM_STREAM_CACHE: OnceLock<Mutex<HashMap<String, WebcamStreamCacheEntry>>> = OnceLock::new();
static MAINBOARD_ID_CACHE: OnceLock<Mutex<HashMap<String, MainboardIdCacheEntry>>> = OnceLock::new();

const DEFAULT_SDCP_PORT: u16 = 3030;
const DEFAULT_SDCP_DISCOVERY_PORT: u16 = 3000;
// const SDCP_STATUS_PROBE_TIMEOUT_MS: u64 = 6_500;
const SDCP_STATUS_WS_TIMEOUT_MS: u64 = 6_500;
const MAINBOARD_ID_CACHE_TTL_MS: u64 = 5 * 60 * 1000;

struct WebcamStreamCacheEntry {
    external_stream_url: String,
    updated_at: Instant,
}

struct MainboardIdCacheEntry {
    mainboard_id: String,
    updated_at: Instant,
}

const WEBCAM_STREAM_CACHE_TTL_MS: u64 = 45_000;

fn webcam_stream_cache() -> &'static Mutex<HashMap<String, WebcamStreamCacheEntry>> {
    WEBCAM_STREAM_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn mainboard_id_cache() -> &'static Mutex<HashMap<String, MainboardIdCacheEntry>> {
    MAINBOARD_ID_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn webcam_cache_key(host: &str, port: u16, mainboard_id: &str) -> String {
    format!("{}:{}:{}", host.trim().to_lowercase(), port, mainboard_id.trim().to_lowercase())
}

fn mainboard_cache_key(host: &str, port: u16) -> String {
    format!("{}:{}", host.trim().to_lowercase(), port)
}

fn read_cached_mainboard_id(host: &str, port: u16) -> Option<String> {
    let mut cache = mainboard_id_cache().lock().ok()?;
    let ttl = Duration::from_millis(MAINBOARD_ID_CACHE_TTL_MS);
    cache.retain(|_, entry| entry.updated_at.elapsed() <= ttl);

    let key = mainboard_cache_key(host, port);
    let entry = cache.get_mut(&key)?;
    entry.updated_at = Instant::now();

    let candidate = entry.mainboard_id.trim().to_string();
    if looks_like_mainboard_id(&candidate) {
        Some(candidate)
    } else {
        None
    }
}

fn store_cached_mainboard_id(host: &str, port: u16, mainboard_id: &str) {
    let candidate = mainboard_id.trim().to_string();
    if !looks_like_mainboard_id(&candidate) {
        return;
    }

    if let Ok(mut cache) = mainboard_id_cache().lock() {
        cache.insert(
            mainboard_cache_key(host, port),
            MainboardIdCacheEntry {
                mainboard_id: candidate,
                updated_at: Instant::now(),
            },
        );
    }
}

fn read_cached_webcam_urls(cache_key: &str) -> Option<String> {
    let mut cache = webcam_stream_cache().lock().ok()?;
    let ttl = Duration::from_millis(WEBCAM_STREAM_CACHE_TTL_MS);
    cache.retain(|_, entry| entry.updated_at.elapsed() <= ttl);

    let entry = cache.get_mut(cache_key)?;
    entry.updated_at = Instant::now();
    let external = entry.external_stream_url.trim().to_string();
    if external.is_empty() {
        return None;
    }

    Some(external)
}

fn store_cached_webcam_urls(cache_key: &str, external_stream_url: &str) {
    let external = external_stream_url.trim().to_string();
    if external.is_empty() {
        return;
    }

    if let Ok(mut cache) = webcam_stream_cache().lock() {
        cache.insert(
            cache_key.to_string(),
            WebcamStreamCacheEntry {
                external_stream_url: external,
                updated_at: Instant::now(),
            },
        );
    }
}

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

fn now_unix_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn now_unix_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn looks_like_mainboard_id(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.parse::<Ipv4Addr>().is_ok() {
        return false;
    }
    !trimmed.is_empty()
        && trimmed.len() >= 4
        && trimmed.len() <= 128
        && !trimmed.chars().any(|c| c.is_whitespace())
        && !trimmed.contains('/')
        && trimmed.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn hash_plate_id_from_path(path: &str) -> i64 {
    let mut hash: u32 = 2_166_136_261;
    for b in path.as_bytes() {
        hash ^= *b as u32;
        hash = hash.wrapping_mul(16_777_619);
    }

    let normalized = (hash & 0x7fff_ffff) as i64;
    if normalized <= 0 { 1 } else { normalized }
}

fn normalize_sdcp_comparable_path(value: &str) -> String {
    value
        .trim()
        .replace('\\', "/")
        .split('/')
        .filter(|segment| !segment.trim().is_empty())
        .collect::<Vec<_>>()
        .join("/")
        .to_lowercase()
}

fn sdcp_path_tail(value: &str) -> String {
    let normalized = normalize_sdcp_comparable_path(value);
    normalized
        .rsplit('/')
        .next()
        .unwrap_or("")
        .trim()
        .to_string()
}

fn resolve_sdcp_file_list(frame: &Value) -> Vec<Value> {
    frame
        .pointer("/Data/Data/FileList")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
}

fn find_sdcp_plate_path_from_file_list(files: &[Value], plate_id: Option<i64>, name_hint: &str) -> Option<String> {
    let normalized_hint = normalize_sdcp_comparable_path(name_hint);

    for file in files {
        let file_type = file
            .get("type")
            .and_then(|v| v.as_i64().or_else(|| v.as_u64().map(|n| n as i64)))
            .unwrap_or(1);
        if file_type != 1 {
            continue;
        }

        let full_path = file
            .get("name")
            .or_else(|| file.get("Name"))
            .or_else(|| file.get("path"))
            .or_else(|| file.get("Path"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if full_path.is_empty() {
            continue;
        }

        if let Some(target_id) = plate_id {
            let derived = hash_plate_id_from_path(&full_path);
            if derived == target_id {
                return Some(full_path);
            }
        }

        if !normalized_hint.is_empty() {
            let normalized_path = normalize_sdcp_comparable_path(&full_path);
            let normalized_tail = sdcp_path_tail(&full_path);
            if normalized_path.contains(&normalized_hint) || normalized_tail == normalized_hint {
                return Some(full_path);
            }
        }
    }

    None
}

fn extract_sdcp_string_field(record: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = record.get(*key).and_then(|v| v.as_str()) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn extract_sdcp_i64_field(record: &Value, keys: &[&str]) -> Option<i64> {
    for key in keys {
        if let Some(value) = record.get(*key) {
            if let Some(parsed) = value.as_i64() {
                return Some(parsed);
            }
            if let Some(parsed) = value.as_u64() {
                return i64::try_from(parsed).ok();
            }
            if let Some(parsed) = value.as_f64() {
                if parsed.is_finite() {
                    return Some(parsed.round() as i64);
                }
            }
        }
    }
    None
}

fn parse_sdcp_task_ids_from_response(frame: &Value) -> Vec<String> {
    let data = frame.pointer("/Data/Data").unwrap_or(&Value::Null);
    let mut ids = Vec::<String>::new();

    let candidate_arrays = [
        "HistoryData",
        "historyData",
        "TaskIdList",
        "taskIdList",
        "HistoryTaskIdList",
        "historyTaskIdList",
        "TaskList",
    ];

    for key in candidate_arrays {
        if let Some(values) = data.get(key).and_then(|v| v.as_array()) {
            for value in values {
                if let Some(text) = value.as_str() {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        ids.push(trimmed.to_string());
                        continue;
                    }
                }

                if let Some(obj) = value.as_object() {
                    for candidate_key in ["TaskId", "taskId", "ID", "id"] {
                        if let Some(text) = obj.get(candidate_key).and_then(|v| v.as_str()) {
                            let trimmed = text.trim();
                            if !trimmed.is_empty() {
                                ids.push(trimmed.to_string());
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    let mut seen = HashSet::<String>::new();
    ids.into_iter().filter(|id| seen.insert(id.to_lowercase())).collect()
}

fn parse_sdcp_task_details_from_response(frame: &Value) -> Vec<Value> {
    let data = frame.pointer("/Data/Data").unwrap_or(&Value::Null);

    for key in [
        "HistoryDetailList",
        "historyDetailList",
        "TaskDetailList",
        "taskDetailList",
        "TaskList",
        "taskList",
        "HistoryList",
        "historyList",
    ] {
        if let Some(values) = data.get(key).and_then(|v| v.as_array()) {
            return values
                .iter()
                .filter(|item| item.is_object())
                .cloned()
                .collect();
        }
    }

    Vec::new()
}

fn is_sdcp_task_status_active(task_status: Option<i64>) -> bool {
    match task_status {
        Some(status) => status != 1 && status != 2 && status != 3,
        None => false,
    }
}

fn resolve_sdcp_active_task_detail(task_details: &[Value], task_id: Option<&str>, job_name: Option<&str>) -> Option<Value> {
    if task_details.is_empty() {
        return None;
    }

    let normalized_task_id = task_id
        .map(|v| v.trim().to_lowercase())
        .filter(|v| !v.is_empty());
    if let Some(target_task_id) = normalized_task_id {
        if let Some(found) = task_details.iter().find(|detail| {
            extract_sdcp_string_field(detail, &["TaskId", "taskId", "Id", "id"])
                .map(|value| value.to_lowercase() == target_task_id)
                .unwrap_or(false)
        }) {
            return Some(found.clone());
        }
    }

    let normalized_job_name = job_name
        .map(normalize_sdcp_comparable_path)
        .filter(|value| !value.is_empty());
    if let Some(target_job_name) = normalized_job_name {
        if let Some(found) = task_details.iter().find(|detail| {
            let detail_path = extract_sdcp_string_field(detail, &["Filename", "filename", "FileName", "fileName", "Path", "path", "File", "file"])
                .unwrap_or_default();
            let detail_task_name = extract_sdcp_string_field(detail, &["TaskName", "taskName", "Name", "name"])
                .unwrap_or_default();
            let comparable_path = normalize_sdcp_comparable_path(&detail_path);
            let comparable_tail = sdcp_path_tail(&detail_path);
            let comparable_task_name = normalize_sdcp_comparable_path(&detail_task_name);
            comparable_path.contains(&target_job_name)
                || comparable_tail == target_job_name
                || comparable_task_name.contains(&target_job_name)
        }) {
            return Some(found.clone());
        }
    }

    task_details.first().cloned()
}

    fn resolve_sdcp_device_via_udp_discovery(host: &str, timeout_ms: u64) -> Option<Value> {
        let normalized_host = host.trim().to_lowercase();
        if normalized_host.is_empty() {
            return None;
        }

        let devices = discover_sdcp_devices_via_udp(timeout_ms);
        devices.into_iter().find(|device| {
            let ip_matches = device
                .get("ipAddress")
                .and_then(|v| v.as_str())
                .map(|ip| ip.trim().eq_ignore_ascii_case(&normalized_host))
                .unwrap_or(false);
            let name_matches = device
                .get("hostName")
                .and_then(|v| v.as_str())
                .map(|name| name.trim().eq_ignore_ascii_case(&normalized_host))
                .unwrap_or(false);
            ip_matches || name_matches
        })
    }

fn frame_topic_lower(frame: &Value) -> String {
    frame
        .get("Topic")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_lowercase()
}

async fn resolve_mainboard_id_for_host(host: &str, port: u16) -> String {
    if let Some(device) = resolve_sdcp_device_via_udp_discovery(host, 1800) {
        let candidate = device
            .get("hostName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if looks_like_mainboard_id(&candidate) {
            return candidate;
        }
    }

    let ws_discovered = resolve_mainboard_id_via_websocket(host, port, 1800);
    if looks_like_mainboard_id(&ws_discovered) {
        return ws_discovered;
    }

    let udp_discovered = resolve_mainboard_id_via_udp(host, 1400);
    if looks_like_mainboard_id(&udp_discovered) {
        return udp_discovered;
    }

    let Some(device) = probe_sdcp_host(host, port, 1800).await else {
        return String::new();
    };
    let enriched = enrich_sdcp_device_identity_via_websocket(device, 1400);
    let host_name = enriched
        .get("hostName")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if looks_like_mainboard_id(&host_name) && !host_name.eq_ignore_ascii_case(host) {
        host_name
    } else {
        String::new()
    }
}

fn resolve_mainboard_id_via_udp(target_host: &str, timeout_ms: u64) -> String {
    let devices = discover_sdcp_devices_via_udp(timeout_ms.clamp(400, 4000));
    let normalized_target = target_host.trim().to_lowercase();
    let matched = devices.into_iter().find(|device| {
        device
            .get("ipAddress")
            .and_then(|v| v.as_str())
            .map(|ip| ip.trim().eq_ignore_ascii_case(&normalized_target))
            .unwrap_or(false)
    });

    let Some(device) = matched else {
        return String::new();
    };

    let candidate = device
        .get("hostName")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    if looks_like_mainboard_id(&candidate) {
        candidate
    } else {
        String::new()
    }
}

fn resolve_mainboard_id_via_websocket(host: &str, port: u16, timeout_ms: u64) -> String {
    let url = format!("ws://{host}:{port}/websocket");
    let Ok((mut socket, _)) = tungstenite::connect(url.as_str()) else {
        return String::new();
    };

    if let MaybeTlsStream::Plain(stream) = socket.get_mut() {
        let _ = stream.set_read_timeout(Some(Duration::from_millis(timeout_ms.clamp(500, 5000))));
    }

    let _ = socket.send(Message::Text("ping".into()));

    let started = Instant::now();
    let deadline = Duration::from_millis(timeout_ms.clamp(500, 5000));
    while started.elapsed() < deadline {
        match socket.read() {
            Ok(Message::Text(text)) => {
                if text.trim().eq_ignore_ascii_case("pong") {
                    continue;
                }

                let Ok(frame) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let mainboard = frame
                    .pointer("/Data/MainboardID")
                    .and_then(|v| v.as_str())
                    .or_else(|| frame.get("MainboardID").and_then(|v| v.as_str()))
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default();
                if looks_like_mainboard_id(&mainboard) {
                    return mainboard;
                }

                let topic = frame_topic_lower(&frame);
                let from_topic = topic.split('/').last().unwrap_or("").trim().to_string();
                if looks_like_mainboard_id(&from_topic) {
                    return from_topic;
                }
            }
            Ok(Message::Binary(data)) => {
                let text = String::from_utf8_lossy(&data).to_string();
                if text.trim().eq_ignore_ascii_case("pong") {
                    continue;
                }

                let Ok(frame) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let mainboard = frame
                    .pointer("/Data/MainboardID")
                    .and_then(|v| v.as_str())
                    .or_else(|| frame.get("MainboardID").and_then(|v| v.as_str()))
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default();
                if looks_like_mainboard_id(&mainboard) {
                    return mainboard;
                }

                let topic = frame_topic_lower(&frame);
                let from_topic = topic.split('/').last().unwrap_or("").trim().to_string();
                if looks_like_mainboard_id(&from_topic) {
                    return from_topic;
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }

    String::new()
}

async fn sdcp_upload_chunk(payload: &Value) -> (u16, Value) {
    let raw_host = resolve_raw_host(payload);
    let parsed = match parse_host_and_port(&raw_host) {
        Some(parsed) => parsed,
        None => return (400, json!({ "ok": false, "error": "Invalid host or IP address" })),
    };

    let port = resolve_port(payload.get("port"), parsed.1);
    let uuid = payload.get("uuid").and_then(|v| v.as_str()).unwrap_or("").trim().to_string();
    let file_name = payload
        .get("fileName")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let total_size = payload
        .get("totalSize")
        .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f.max(0.0) as u64)))
        .unwrap_or(0);
    let offset = payload
        .get("offset")
        .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f.max(0.0) as u64)))
        .unwrap_or(0);
    let chunk_base64 = payload
        .get("chunkBase64")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    if uuid.is_empty() || file_name.is_empty() || chunk_base64.is_empty() {
        return (
            400,
            json!({ "ok": false, "error": "Missing required SDCP upload chunk fields" }),
        );
    }

    let chunk_bytes = match base64::engine::general_purpose::STANDARD.decode(chunk_base64.as_bytes()) {
        Ok(bytes) if !bytes.is_empty() => bytes,
        _ => return (400, json!({ "ok": false, "error": "Invalid chunkBase64 payload" })),
    };

    let file_part = match reqwest::multipart::Part::bytes(chunk_bytes)
        .file_name(file_name.clone())
        .mime_str("application/octet-stream")
    {
        Ok(part) => part,
        Err(_) => return (500, json!({ "ok": false, "error": "Failed to prepare SDCP upload chunk" })),
    };

    let form = reqwest::multipart::Form::new()
        .text("S-File-MD5", "")
        .text("Check", "0")
        .text("Offset", offset.to_string())
        .text("Uuid", uuid)
        .text("TotalSize", total_size.to_string())
        .part("File", file_part);

    let response = http_client()
        .post(format!("http://{}:{}/uploadFile/upload", parsed.0, port))
        .multipart(form)
        .timeout(Duration::from_millis(15_000))
        .send()
        .await;

    match response {
        Ok(resp) if resp.status().is_success() => (200, json!({ "ok": true })),
        Ok(resp) => (
            resp.status().as_u16(),
            json!({
                "ok": false,
                "error": format!("SDCP upload chunk failed (HTTP {})", resp.status().as_u16()),
            }),
        ),
        Err(err) => (502, json!({ "ok": false, "error": err.to_string() })),
    }
}

fn send_sdcp_command_and_await_response(
    host: &str,
    port: u16,
    mainboard_id: &str,
    cmd: u64,
    data: Value,
    timeout_ms: u64,
) -> Option<Value> {
    let url = format!("ws://{host}:{port}/websocket");
    let (mut socket, _) = match tungstenite::connect(url.as_str()) {
        Ok(connection) => connection,
        Err(_) => return None,
    };

    let effective_timeout_ms = timeout_ms.clamp(700, 7000);

    if let MaybeTlsStream::Plain(stream) = socket.get_mut() {
        let _ = stream.set_read_timeout(Some(Duration::from_millis(effective_timeout_ms)));
    }

    let request_id = format!("sdcp-{}-{}-{}", now_unix_millis(), std::process::id(), cmd);
    let payload = json!({
        "Id": "dragonfruit",
        "Data": {
            "Cmd": cmd,
            "Data": data,
            "RequestID": request_id,
            "MainboardID": mainboard_id,
            "TimeStamp": now_unix_seconds(),
            "From": 0
        },
        "Topic": format!("sdcp/request/{mainboard_id}")
    });

    let _ = socket.send(Message::Text("ping".into()));
    let _ = socket.send(Message::Text(payload.to_string().into()));

    let started = Instant::now();
    let deadline = Duration::from_millis(effective_timeout_ms);
    let expected_topic_prefix = format!("sdcp/response/{}", mainboard_id.to_lowercase());

    while started.elapsed() < deadline {
        match socket.read() {
            Ok(Message::Text(text)) => {
                if text.trim().eq_ignore_ascii_case("pong") {
                    continue;
                }

                let Ok(frame) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let topic = frame_topic_lower(&frame);
                if !topic.starts_with(&expected_topic_prefix) {
                    continue;
                }

                let frame_cmd = frame.pointer("/Data/Cmd").and_then(|v| v.as_u64()).unwrap_or(u64::MAX);
                if frame_cmd != cmd {
                    continue;
                }

                let frame_request_id = frame
                    .pointer("/Data/RequestID")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim();
                if frame_request_id != request_id {
                    continue;
                }

                return Some(frame);
            }
            Ok(Message::Binary(data)) => {
                let text = String::from_utf8_lossy(&data).to_string();
                if text.trim().eq_ignore_ascii_case("pong") {
                    continue;
                }

                let Ok(frame) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let topic = frame_topic_lower(&frame);
                if !topic.starts_with(&expected_topic_prefix) {
                    continue;
                }

                let frame_cmd = frame.pointer("/Data/Cmd").and_then(|v| v.as_u64()).unwrap_or(u64::MAX);
                if frame_cmd != cmd {
                    continue;
                }

                let frame_request_id = frame
                    .pointer("/Data/RequestID")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim();
                if frame_request_id != request_id {
                    continue;
                }

                return Some(frame);
            }
            Ok(_) => {}
            Err(err) => {
                let _ = err;
                break;
            }
        }
    }

    None
}

fn request_sdcp_status_and_attributes(
    host: &str,
    port: u16,
    mainboard_id: &str,
    timeout_ms: u64,
) -> (Option<Value>, Option<Value>) {
    let url = format!("ws://{host}:{port}/websocket");
    let Ok((mut socket, _)) = tungstenite::connect(url.as_str()) else {
        return (None, None);
    };

    if let MaybeTlsStream::Plain(stream) = socket.get_mut() {
        let _ = stream.set_read_timeout(Some(Duration::from_millis(timeout_ms.clamp(900, 7000))));
    }

    let _ = socket.send(Message::Text("ping".into()));

    let status_request = json!({
        "Id": "dragonfruit",
        "Data": {
            "Cmd": 0,
            "Data": {},
            "RequestID": format!("sdcp-status-{}", now_unix_seconds()),
            "MainboardID": mainboard_id,
            "TimeStamp": now_unix_seconds(),
            "From": 0
        },
        "Topic": format!("sdcp/request/{mainboard_id}")
    });
    let attr_request = json!({
        "Id": "dragonfruit",
        "Data": {
            "Cmd": 1,
            "Data": {},
            "RequestID": format!("sdcp-attr-{}", now_unix_seconds()),
            "MainboardID": mainboard_id,
            "TimeStamp": now_unix_seconds(),
            "From": 0
        },
        "Topic": format!("sdcp/request/{mainboard_id}")
    });
    let _ = socket.send(Message::Text(status_request.to_string().into()));
    let _ = socket.send(Message::Text(attr_request.to_string().into()));

    let started = Instant::now();
    let deadline = Duration::from_millis(timeout_ms.clamp(900, 7000));
    let status_prefix = format!("sdcp/status/{}", mainboard_id.to_lowercase());
    let attr_prefix = format!("sdcp/attributes/{}", mainboard_id.to_lowercase());
    let mut status_frame: Option<Value> = None;
    let mut attributes_frame: Option<Value> = None;

    while started.elapsed() < deadline {
        match socket.read() {
            Ok(Message::Text(text)) => {
                if text.trim().eq_ignore_ascii_case("pong") {
                    continue;
                }
                let Ok(frame) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let topic = frame_topic_lower(&frame);
                if topic.starts_with(&status_prefix) {
                    status_frame = Some(frame);
                } else if topic.starts_with(&attr_prefix) {
                    attributes_frame = Some(frame);
                }
                if status_frame.is_some() && attributes_frame.is_some() {
                    break;
                }
            }
            Ok(Message::Binary(data)) => {
                let text = String::from_utf8_lossy(&data).to_string();
                if text.trim().eq_ignore_ascii_case("pong") {
                    continue;
                }
                let Ok(frame) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let topic = frame_topic_lower(&frame);
                if topic.starts_with(&status_prefix) {
                    status_frame = Some(frame);
                } else if topic.starts_with(&attr_prefix) {
                    attributes_frame = Some(frame);
                }
                if status_frame.is_some() && attributes_frame.is_some() {
                    break;
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }

    (status_frame, attributes_frame)
}

fn extract_print_info_from_status_frame(status_frame: Option<&Value>) -> Value {
    let status_obj = status_frame
        .and_then(|f| f.pointer("/Data/Status"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    let print_info = status_frame
        .and_then(|f| f.pointer("/Data/Status/PrintInfo"))
        .cloned()
        .unwrap_or_else(|| json!({}));

    let current_machine_statuses: Vec<i64> = if let Some(values) = status_obj.get("CurrentStatus").and_then(|v| v.as_array()) {
        values
            .iter()
            .filter_map(|value| value.as_i64().or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok())))
            .collect()
    } else {
        status_obj
            .get("CurrentStatus")
            .and_then(|value| value.as_i64().or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok())))
            .map(|value| vec![value])
            .unwrap_or_default()
    };

    let machine_status_printing = current_machine_statuses.iter().any(|status| *status == 1);
    let machine_status_processing = current_machine_statuses
        .iter()
        .any(|status| *status == 2 || *status == 3 || *status == 4);

    let print_status = print_info.get("Status").and_then(|v| v.as_i64()).unwrap_or(-1);
    let current_layer = print_info.get("CurrentLayer").and_then(|v| v.as_i64());
    let total_layers = print_info.get("TotalLayer").and_then(|v| v.as_i64());
    let current_ticks = print_info.get("CurrentTicks").and_then(|v| v.as_i64());
    let total_ticks = print_info.get("TotalTicks").and_then(|v| v.as_i64());
    let filename = print_info
        .get("Filename")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let task_id = print_info
        .get("TaskId")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let progress_from_layer = match (current_layer, total_layers) {
        (Some(current), Some(total)) if total > 0 => {
            Some(((current as f64 / total as f64) * 100.0).clamp(0.0, 100.0))
        }
        _ => None,
    };
    let progress_from_ticks = match (current_ticks, total_ticks) {
        (Some(current), Some(total)) if total > 0 => {
            Some(((current as f64 / total as f64) * 100.0).clamp(0.0, 100.0))
        }
        _ => None,
    };
    let eta_sec = match (current_ticks, total_ticks) {
        (Some(current), Some(total)) if total >= current => Some(((total - current) / 1000).max(0)),
        _ => None,
    };

    let (base_state_text, base_state, base_is_printing, base_is_paused) = match print_status {
        0 => ("Idle", "idle", false, false),
        1..=5 => ("Printing", "printing", true, false),
        6 => ("Paused", "paused", true, true),
        7 => ("Stopping", "canceling", false, false),
        8 | 9 => ("Idle", "idle", false, false),
        10 => ("Processing", "processing", false, false),
        _ => ("Online", "online", false, false),
    };

    let is_paused = base_is_paused;
    let is_printing = if is_paused {
        true
    } else {
        base_is_printing || machine_status_printing
    };
    let state = if is_paused {
        "paused"
    } else if is_printing {
        "printing"
    } else if base_state == "online" && machine_status_processing {
        "processing"
    } else {
        base_state
    };
    let state_text = if is_paused {
        "Paused"
    } else if is_printing {
        "Printing"
    } else if base_state_text == "Online" && machine_status_processing {
        "Processing"
    } else {
        base_state_text
    };

    json!({
        "stateText": state_text,
        "state": state,
        "isPrinting": is_printing,
        "isPaused": is_paused,
        "progressPct": progress_from_layer.or(progress_from_ticks),
        "currentLayer": current_layer,
        "totalLayers": total_layers,
        "etaSec": eta_sec,
        "jobName": filename,
        "taskId": task_id,
    })
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

async fn sdcp_printer_status(payload: &Value) -> (u16, Value) {
    let raw_host = resolve_raw_host(payload);
    let parsed = match parse_host_and_port(&raw_host) {
        Some(parsed) => parsed,
        None => {
            return (
                400,
                json!({
                    "ok": false,
                    "connected": false,
                    "error": "Invalid host or IP address",
                }),
            )
        }
    };

    let port = resolve_port(payload.get("port"), parsed.1);

    let Some(probed_device) = probe_sdcp_host(&parsed.0, port, 6500).await else {
        return (
            503,
            json!({
                "ok": false,
                "connected": false,
                "mode": "sdcp",
                "hostName": parsed.0,
                "printerName": "",
                "ipAddress": parsed.0,
                "port": port,
                "stateText": "Offline",
                "statusText": "SDCP discovery did not respond",
                "state": "offline",
                "isPrinting": false,
                "isPaused": false,
                "progressPct": Value::Null,
                "currentLayer": Value::Null,
                "totalLayers": Value::Null,
                "plateId": Value::Null,
                "jobName": Value::Null,
                "etaSec": Value::Null,
            }),
        );
    };

    let device = enrich_sdcp_device_identity_via_websocket(probed_device, 1400);

    let payload_mainboard = payload
        .get("mainboardId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    if looks_like_mainboard_id(&payload_mainboard) {
        store_cached_mainboard_id(&parsed.0, port, &payload_mainboard);
    }

    let mainboard_id = if looks_like_mainboard_id(&payload_mainboard) {
        payload_mainboard
    } else if let Some(cached) = read_cached_mainboard_id(&parsed.0, port) {
        cached
    } else {
        let resolved = resolve_mainboard_id_via_websocket(&parsed.0, port, 1600);
        if looks_like_mainboard_id(&resolved) {
            store_cached_mainboard_id(&parsed.0, port, &resolved);
            resolved
        } else {
            String::new()
        }
    };

    if looks_like_mainboard_id(&mainboard_id) {
        store_cached_mainboard_id(&parsed.0, port, &mainboard_id);
    }

    let (status_frame, attributes_frame) = if mainboard_id.is_empty() {
        (None, None)
    } else {
        request_sdcp_status_and_attributes(&parsed.0, port, &mainboard_id, SDCP_STATUS_WS_TIMEOUT_MS)
    };

    let print_info = extract_print_info_from_status_frame(status_frame.as_ref());
    let mut task_detail_ack: Option<i64> = None;
    let mut active_task_detail: Option<Value> = None;

    if !mainboard_id.is_empty() {
        let explicit_task_id = print_info
            .get("taskId")
            .and_then(|v| v.as_str())
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());

        let task_ids = if let Some(task_id) = explicit_task_id.clone() {
            vec![task_id]
        } else {
            let history_response = send_sdcp_command_and_await_response(
                &parsed.0,
                port,
                &mainboard_id,
                320,
                json!({}),
                3200,
            );
            history_response
                .as_ref()
                .map(parse_sdcp_task_ids_from_response)
                .unwrap_or_default()
                .into_iter()
                .take(20)
                .collect::<Vec<_>>()
        };

        if !task_ids.is_empty() {
            let detail_response = send_sdcp_command_and_await_response(
                &parsed.0,
                port,
                &mainboard_id,
                321,
                json!({
                    "TaskIdList": task_ids,
                    "Id": task_ids,
                }),
                4200,
            );

            task_detail_ack = detail_response
                .as_ref()
                .and_then(|f| f.pointer("/Data/Data/Ack"))
                .and_then(|v| v.as_i64());

            let task_details = detail_response
                .as_ref()
                .map(parse_sdcp_task_details_from_response)
                .unwrap_or_default();

            let print_task_id = print_info.get("taskId").and_then(|v| v.as_str());
            let print_job_name = print_info.get("jobName").and_then(|v| v.as_str());
            active_task_detail = resolve_sdcp_active_task_detail(&task_details, print_task_id, print_job_name);
        }
    }

    let active_task_status = active_task_detail
        .as_ref()
        .and_then(|detail| extract_sdcp_i64_field(detail, &["TaskStatus", "taskStatus", "Status", "status"]));
    let active_task_running = is_sdcp_task_status_active(active_task_status);
    let active_task_thumbnail = active_task_detail
        .as_ref()
        .and_then(|detail| extract_sdcp_string_field(detail, &["Thumbnail", "thumbnail", "ThumbnailUrl", "thumbnailUrl", "ThumbnailPath", "thumbnailPath"]));
    let active_task_id = active_task_detail
        .as_ref()
        .and_then(|detail| extract_sdcp_string_field(detail, &["TaskId", "taskId", "Id", "id"]));
    let active_task_path = active_task_detail
        .as_ref()
        .and_then(|detail| extract_sdcp_string_field(detail, &["Filename", "filename", "FileName", "fileName", "Path", "path", "File", "file"]));
    let active_task_name = active_task_detail
        .as_ref()
        .and_then(|detail| extract_sdcp_string_field(detail, &["TaskName", "taskName", "Name", "name"]));

    let print_info_is_printing = print_info
        .get("isPrinting")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let print_info_is_paused = print_info
        .get("isPaused")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let resolved_is_printing = print_info_is_printing || active_task_running;
    let resolved_state = if print_info_is_paused {
        "paused".to_string()
    } else if resolved_is_printing {
        "printing".to_string()
    } else {
        print_info
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("online")
            .to_string()
    };
    let resolved_state_text = if print_info_is_paused {
        "Paused".to_string()
    } else if resolved_is_printing {
        "Printing".to_string()
    } else {
        print_info
            .get("stateText")
            .and_then(|v| v.as_str())
            .unwrap_or("Online")
            .to_string()
    };
    let resolved_job_name = print_info
        .get("jobName")
        .and_then(|v| v.as_str())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or(active_task_name)
        .or_else(|| {
            active_task_path.as_ref().map(|path| {
                path
                    .split('/')
                    .filter(|s| !s.is_empty())
                    .last()
                    .unwrap_or(path.as_str())
                    .to_string()
            })
        });
    let resolved_plate_key = active_task_path
        .clone()
        .or(active_task_id.clone())
        .or_else(|| {
            print_info
                .get("taskId")
                .and_then(|v| v.as_str())
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
        .or_else(|| resolved_job_name.clone());

    let firmware_from_attrs = attributes_frame
        .as_ref()
        .and_then(|f| f.pointer("/Data/Attributes/FirmwareVersion"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    (
        200,
        json!({
            "ok": true,
            "connected": true,
            "mode": "sdcp",
            "hostName": device.get("hostName").cloned().unwrap_or(Value::String(parsed.0.clone())),
            "printerName": device.get("printerName").cloned().unwrap_or(Value::String("SDCP Printer".to_string())),
            "printerModel": device.get("printerModel").cloned().unwrap_or(Value::String("SDCP 3.0.0".to_string())),
            "ipAddress": device.get("ipAddress").cloned().unwrap_or(Value::String(parsed.0.clone())),
            "port": port,
            "mainboardId": mainboard_id,
            "firmwareVersion": if firmware_from_attrs.is_empty() {
                device.get("firmwareVersion").cloned().unwrap_or(Value::String("".to_string()))
            } else {
                Value::String(firmware_from_attrs)
            },
            "stateText": resolved_state_text,
            "statusText": device.get("statusText").cloned().unwrap_or(Value::String("Online".to_string())),
            "state": resolved_state,
            "isPrinting": resolved_is_printing,
            "isPaused": print_info_is_paused,
            "progressPct": print_info.get("progressPct").cloned().unwrap_or(Value::Null),
            "currentLayer": print_info.get("currentLayer").cloned().unwrap_or(Value::Null),
            "totalLayers": print_info.get("totalLayers").cloned().unwrap_or(Value::Null),
            "plateId": resolved_plate_key
                .as_ref()
                .map(|v| Value::from(hash_plate_id_from_path(v)))
                .unwrap_or(Value::Null),
            "jobName": resolved_job_name.map(Value::String).unwrap_or(Value::Null),
            "etaSec": print_info.get("etaSec").cloned().unwrap_or(Value::Null),
            "taskId": active_task_id
                .or_else(|| {
                    print_info
                        .get("taskId")
                        .and_then(|v| v.as_str())
                        .map(|v| v.trim().to_string())
                        .filter(|v| !v.is_empty())
                })
                .map(Value::String)
                .unwrap_or(Value::Null),
            "taskStatus": active_task_status.map(Value::from).unwrap_or(Value::Null),
            "thumbnailPath": active_task_thumbnail.map(Value::String).unwrap_or(Value::Null),
            "taskDetailOk": task_detail_ack.map(|ack| ack == 0).map(Value::Bool).unwrap_or(Value::Null),
        }),
    )
}

async fn sdcp_plates_list(payload: &Value) -> (u16, Value) {
    let raw_host = resolve_raw_host(payload);
    let parsed = match parse_host_and_port(&raw_host) {
        Some(parsed) => parsed,
        None => {
            return (
                400,
                json!({ "ok": false, "metadataReady": false, "error": "Invalid host or IP address", "plates": [] }),
            )
        }
    };
    let port = resolve_port(payload.get("port"), parsed.1);
    let payload_mainboard = payload
        .get("mainboardId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let mainboard_id = if looks_like_mainboard_id(&payload_mainboard) {
        payload_mainboard
    } else {
        resolve_mainboard_id_for_host(&parsed.0, port).await
    };

    if mainboard_id.is_empty() {
        return (
            200,
            json!({
                "ok": false,
                "metadataReady": false,
                "error": "Unable to resolve SDCP mainboard ID for file list command.",
                "matchedPlate": Value::Null,
                "plates": [],
            }),
        );
    }

    let storage_path = normalize_sdcp_storage_path(
        payload.get("storagePath")
            .or_else(|| payload.get("source"))
            .or_else(|| payload.get("url")),
    );

    let response = send_sdcp_command_and_await_response(
        &parsed.0,
        port,
        &mainboard_id,
        258,
        json!({ "Url": storage_path }),
        3200,
    );
    let ack = response
        .as_ref()
        .and_then(|f| f.pointer("/Data/Data/Ack"))
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);

    let files = response
        .as_ref()
        .and_then(|f| f.pointer("/Data/Data/FileList"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut plates: Vec<Value> = Vec::new();
    for file in files {
        let file_type = file.get("type").and_then(|v| v.as_i64()).unwrap_or(-1);
        if file_type != 1 {
            continue;
        }

        let full_path = file
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if full_path.is_empty() {
            continue;
        }
        let plate_name = full_path
            .split('/')
            .filter(|s| !s.is_empty())
            .last()
            .unwrap_or(full_path.as_str())
            .to_string();
        let plate_id = hash_plate_id_from_path(&full_path);

        plates.push(json!({
            "PlateID": plate_id,
            "plateId": plate_id,
            "Path": full_path,
            "path": full_path,
            "Name": plate_name,
            "name": plate_name,
        }));
    }

    let requested_plate_id = payload.get("plateId").and_then(|v| v.as_i64());
    let requested_job = payload
        .get("jobName")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_lowercase();

    let matched_plate = plates.iter().find(|plate| {
        if let Some(id) = requested_plate_id {
            return plate
                .get("PlateID")
                .and_then(|v| v.as_i64())
                .map(|pid| pid == id)
                .unwrap_or(false);
        }

        if !requested_job.is_empty() {
            let path = plate
                .get("Path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();
            let name = plate
                .get("Name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();
            return path.contains(&requested_job) || name.contains(&requested_job);
        }

        false
    }).cloned();

    (
        200,
        json!({
            "ok": ack == 0,
            "metadataReady": matched_plate.is_some() || requested_job.is_empty(),
            "matchedPlate": matched_plate.unwrap_or(Value::Null),
            "plates": plates,
            "error": if ack == 0 { Value::Null } else { Value::String("SDCP file list request failed.".to_string()) },
        }),
    )
}

async fn sdcp_webcam_info(payload: &Value) -> (u16, Value) {
    let raw_host = resolve_raw_host(payload);
    let parsed = match parse_host_and_port(&raw_host) {
        Some(parsed) => parsed,
        None => {
            return (
                400,
                json!({ "ok": false, "available": false, "message": "Invalid host or IP address" }),
            )
        }
    };

    let port = resolve_port(payload.get("port"), parsed.1);
    let payload_mainboard = payload
        .get("mainboardId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    let key_mainboard = if payload_mainboard.is_empty() {
        "passive-rtsp".to_string()
    } else {
        payload_mainboard
    };
    let cache_key = webcam_cache_key(&parsed.0, port, &key_mainboard);

    if let Some(cached_stream_url) = read_cached_webcam_urls(&cache_key) {
        let cached_stream_is_rtsp = cached_stream_url.starts_with("rtsp://")
            || cached_stream_url.starts_with("rtsps://");

        if cached_stream_is_rtsp {
            return (
                200,
                json!({
                    "ok": true,
                    "available": true,
                    "streamUrl": Value::String(cached_stream_url.clone()),
                    "externalStreamUrl": Value::String(cached_stream_url),
                    "snapshotUrl": Value::Null,
                    "message": "Using cached SDCP RTSP stream (direct).".to_string(),
                }),
            );
        }

        return (
            200,
            json!({
                "ok": true,
                "available": true,
                "streamUrl": Value::String(cached_stream_url.clone()),
                "externalStreamUrl": Value::String(cached_stream_url),
                "snapshotUrl": Value::Null,
                "message": "Using cached SDCP webcam stream.".to_string(),
            }),
        );
    }

    let direct_rtsp_url = format!("rtsp://{}:554/video", parsed.0);
    store_cached_webcam_urls(&cache_key, &direct_rtsp_url);

    (
        200,
        json!({
            "ok": true,
            "available": true,
            "streamUrl": Value::String(direct_rtsp_url.clone()),
            "externalStreamUrl": Value::String(direct_rtsp_url),
            "snapshotUrl": Value::Null,
            "message": "Using direct SDCP RTSP stream (no local proxy).".to_string(),
        }),
    )
}

fn normalize_sdcp_storage_path(value: Option<&Value>) -> String {
    let raw = value
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if raw.is_empty() {
        return "/local/".to_string();
    }

    let lower = raw.to_lowercase();
    match lower.as_str() {
        "local" | "/local" => "/local/".to_string(),
        "usb" | "/usb" => "/usb/".to_string(),
        _ if lower.starts_with("/usb/") => {
            let remainder = raw.get(5..).unwrap_or("").trim_start_matches('/');
            if remainder.is_empty() {
                "/usb/".to_string()
            } else {
                format!("/usb/{}", remainder)
            }
        }
        _ if lower.starts_with("/local/") => {
            let remainder = raw.get(7..).unwrap_or("").trim_start_matches('/');
            if remainder.is_empty() {
                "/local/".to_string()
            } else {
                format!("/local/{}", remainder)
            }
        }
        _ if raw.starts_with('/') => raw,
        _ => format!("/{}", raw),
    }
}

async fn sdcp_toggle_feature(payload: &Value, cmd: u64, feature_label: &str, enabled: bool) -> (u16, Value) {
    let raw_host = resolve_raw_host(payload);
    let parsed = match parse_host_and_port(&raw_host) {
        Some(parsed) => parsed,
        None => return (400, json!({ "ok": false, "error": "Invalid host or IP address" })),
    };
    let port = resolve_port(payload.get("port"), parsed.1);

    let payload_mainboard = payload
        .get("mainboardId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let mainboard_id = if looks_like_mainboard_id(&payload_mainboard) {
        payload_mainboard
    } else {
        resolve_mainboard_id_for_host(&parsed.0, port).await
    };

    if mainboard_id.is_empty() {
        return (
            200,
            json!({ "ok": false, "error": format!("Unable to resolve SDCP mainboard ID for {feature_label} command.") }),
        );
    }

    let response = send_sdcp_command_and_await_response(
        &parsed.0,
        port,
        &mainboard_id,
        cmd,
        json!({ "Enable": if enabled { 1 } else { 0 } }),
        3200,
    );
    let ack = response
        .as_ref()
        .and_then(|f| f.pointer("/Data/Data/Ack"))
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);
    let ack_description = match (cmd, ack) {
        (386, 0) => "success",
        (386, 1) => "exceeded maximum simultaneous streaming limit",
        (386, 2) => "camera does not exist",
        (386, 3) => "unknown error",
        (387, 0) => "success",
        (387, 1) => "unknown error",
        (_, -1) => "no/invalid Ack in SDCP response",
        _ => "unknown Ack",
    };
    let normalized_feature_label = if feature_label.trim().is_empty() {
        "feature"
    } else {
        feature_label.trim()
    };
    let ack_label = if ack < 0 { "unknown".to_string() } else { ack.to_string() };

    (
        200,
        json!({
            "ok": ack == 0,
            "ack": ack,
            "ackDescription": ack_description,
            "message": if ack == 0 {
                format!("SDCP command {normalized_feature_label} {} accepted.", if enabled { "enable" } else { "disable" })
            } else {
                format!("SDCP command {normalized_feature_label} {} rejected (Ack {}: {}).", if enabled { "enable" } else { "disable" }, ack_label, ack_description)
            },
            "error": if ack == 0 {
                Value::Null
            } else {
                Value::String(format!("SDCP {normalized_feature_label} {} failed (Ack {}: {}).", if enabled { "enable" } else { "disable" }, ack_label, ack_description))
            },
            "rawResponse": response.unwrap_or(Value::Null),
        }),
    )
}

async fn sdcp_control_operation(payload: &Value, cmd: u64, op_label: &str) -> (u16, Value) {
    let raw_host = resolve_raw_host(payload);
    let parsed = match parse_host_and_port(&raw_host) {
        Some(parsed) => parsed,
        None => return (400, json!({ "ok": false, "error": "Invalid host or IP address" })),
    };
    let port = resolve_port(payload.get("port"), parsed.1);

    let payload_mainboard = payload
        .get("mainboardId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let mainboard_id = if looks_like_mainboard_id(&payload_mainboard) {
        payload_mainboard
    } else {
        resolve_mainboard_id_for_host(&parsed.0, port).await
    };
    if mainboard_id.is_empty() {
        return (200, json!({ "ok": false, "error": "Unable to resolve SDCP mainboard ID for control command." }));
    }

    let mut data = json!({});
    if cmd == 128 {
        let filename = payload
            .get("filename")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let job_name = payload
            .get("jobName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let path = payload
            .get("path")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let resolved_filename = if !filename.is_empty() {
            filename
        } else if !path.is_empty() {
            path
        } else if !job_name.is_empty() {
            let base = job_name.rsplit_once('.').map(|(b, _)| b).unwrap_or(job_name.as_str());
            format!("{base}.ctb")
        } else {
            let requested_plate_id = payload
                .get("plateId")
                .and_then(|v| v.as_i64().or_else(|| v.as_u64().map(|n| n as i64)));
            let plate_name_hint = payload
                .get("plateName")
                .or_else(|| payload.get("jobName"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim()
                .to_string();

            let storage_path = normalize_sdcp_storage_path(
                payload
                    .get("storagePath")
                    .or_else(|| payload.get("source"))
                    .or_else(|| payload.get("url")),
            );

            let list_response = send_sdcp_command_and_await_response(
                &parsed.0,
                port,
                &mainboard_id,
                258,
                json!({ "Url": storage_path }),
                3200,
            );

            list_response
                .as_ref()
                .map(resolve_sdcp_file_list)
                .and_then(|files| find_sdcp_plate_path_from_file_list(&files, requested_plate_id, &plate_name_hint))
                .unwrap_or_default()
        };

        if resolved_filename.is_empty() {
            return (
                400,
                json!({
                    "ok": false,
                    "error": "Start printing requires filename/path/jobName, or a resolvable plateId for SDCP Cmd 128.",
                }),
            );
        }

        data = json!({
            "Filename": resolved_filename,
            "StartLayer": 0
        });
    }

    let response = send_sdcp_command_and_await_response(&parsed.0, port, &mainboard_id, cmd, data, 3200);
    let ack = response
        .as_ref()
        .and_then(|f| f.pointer("/Data/Data/Ack"))
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);

    (
        200,
        json!({
            "ok": ack == 0,
            "ack": ack,
            "message": if ack == 0 {
                format!("SDCP command {op_label} accepted.")
            } else {
                format!("SDCP command {op_label} rejected (Ack {}).", if ack < 0 { "unknown".to_string() } else { ack.to_string() })
            },
            "error": if ack == 0 { Value::Null } else { Value::String(format!("SDCP {op_label} failed.")) },
        }),
    )
}

async fn sdcp_plate_delete(payload: &Value) -> (u16, Value) {
    let raw_host = resolve_raw_host(payload);
    let parsed = match parse_host_and_port(&raw_host) {
        Some(parsed) => parsed,
        None => return (400, json!({ "ok": false, "error": "Invalid host or IP address" })),
    };
    let port = resolve_port(payload.get("port"), parsed.1);

    let payload_mainboard = payload
        .get("mainboardId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let mainboard_id = if looks_like_mainboard_id(&payload_mainboard) {
        payload_mainboard
    } else {
        resolve_mainboard_id_for_host(&parsed.0, port).await
    };
    if mainboard_id.is_empty() {
        return (200, json!({ "ok": false, "error": "Unable to resolve SDCP mainboard ID for delete command." }));
    }

    let direct_path = payload
        .get("path")
        .or_else(|| payload.get("filename"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    let requested_plate_id = payload
        .get("plateId")
        .and_then(|v| v.as_i64().or_else(|| v.as_u64().map(|n| n as i64)));
    let name_hint = payload
        .get("jobName")
        .or_else(|| payload.get("plateName"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    let resolved_path = if !direct_path.is_empty() {
        direct_path
    } else {
        let storage_path = normalize_sdcp_storage_path(
            payload
                .get("storagePath")
                .or_else(|| payload.get("source"))
                .or_else(|| payload.get("url")),
        );

        let list_response = send_sdcp_command_and_await_response(
            &parsed.0,
            port,
            &mainboard_id,
            258,
            json!({ "Url": storage_path }),
            3200,
        );

        list_response
            .as_ref()
            .map(resolve_sdcp_file_list)
            .and_then(|files| find_sdcp_plate_path_from_file_list(&files, requested_plate_id, &name_hint))
            .unwrap_or_default()
    };

    if resolved_path.is_empty() {
        return (
            400,
            json!({
                "ok": false,
                "error": "Unable to resolve SDCP file path for delete command. Provide path or filename.",
            }),
        );
    }

    let response = send_sdcp_command_and_await_response(
        &parsed.0,
        port,
        &mainboard_id,
        259,
        json!({
            "FileList": [resolved_path.clone()],
            "FolderList": [],
        }),
        3200,
    );

    let ack = response
        .as_ref()
        .and_then(|f| f.pointer("/Data/Data/Ack"))
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);

    (
        200,
        json!({
            "ok": ack == 0,
            "ack": ack,
            "path": resolved_path,
            "message": if ack == 0 {
                format!("Deleted SDCP plate file {}.", resolved_path)
            } else {
                format!("SDCP plate delete rejected (Ack {}).", if ack < 0 { "unknown".to_string() } else { ack.to_string() })
            },
            "error": if ack == 0 { Value::Null } else { Value::String("SDCP plate delete failed.".to_string()) },
        }),
    )
}

async fn sdcp_task_history_list(payload: &Value) -> (u16, Value) {
    let raw_host = resolve_raw_host(payload);
    let parsed = match parse_host_and_port(&raw_host) {
        Some(parsed) => parsed,
        None => return (400, json!({ "ok": false, "error": "Invalid host or IP address" })),
    };
    let port = resolve_port(payload.get("port"), parsed.1);

    let payload_mainboard = payload
        .get("mainboardId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let mainboard_id = if looks_like_mainboard_id(&payload_mainboard) {
        payload_mainboard
    } else {
        resolve_mainboard_id_for_host(&parsed.0, port).await
    };
    if mainboard_id.is_empty() {
        return (200, json!({ "ok": false, "error": "Unable to resolve SDCP mainboard ID for task history command." }));
    }

    let response = send_sdcp_command_and_await_response(
        &parsed.0,
        port,
        &mainboard_id,
        320,
        json!({}),
        3200,
    );
    let ack = response
        .as_ref()
        .and_then(|f| f.pointer("/Data/Data/Ack"))
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);
    let task_ids = response
        .as_ref()
        .map(parse_sdcp_task_ids_from_response)
        .unwrap_or_default();

    (
        200,
        json!({
            "ok": ack == 0,
            "ack": ack,
            "taskIds": task_ids,
            "error": if ack == 0 { Value::Null } else { Value::String("SDCP task history request failed.".to_string()) },
            "rawResponse": response.unwrap_or(Value::Null),
        }),
    )
}

async fn sdcp_task_details(payload: &Value) -> (u16, Value) {
    let raw_host = resolve_raw_host(payload);
    let parsed = match parse_host_and_port(&raw_host) {
        Some(parsed) => parsed,
        None => return (400, json!({ "ok": false, "error": "Invalid host or IP address" })),
    };
    let port = resolve_port(payload.get("port"), parsed.1);

    let payload_mainboard = payload
        .get("mainboardId")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let mainboard_id = if looks_like_mainboard_id(&payload_mainboard) {
        payload_mainboard
    } else {
        resolve_mainboard_id_for_host(&parsed.0, port).await
    };
    if mainboard_id.is_empty() {
        return (200, json!({ "ok": false, "error": "Unable to resolve SDCP mainboard ID for task details command." }));
    }

    let mut task_ids: Vec<String> = payload
        .get("taskIds")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|value| value.as_str())
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if task_ids.is_empty() {
        let history_response = send_sdcp_command_and_await_response(
            &parsed.0,
            port,
            &mainboard_id,
            320,
            json!({}),
            3200,
        );
        task_ids = history_response
            .as_ref()
            .map(parse_sdcp_task_ids_from_response)
            .unwrap_or_default();
    }

    if task_ids.is_empty() {
        return (
            200,
            json!({
                "ok": true,
                "ack": 0,
                "taskIds": [],
                "taskDetails": [],
            }),
        );
    }

    task_ids.truncate(60);
    let response = send_sdcp_command_and_await_response(
        &parsed.0,
        port,
        &mainboard_id,
        321,
        json!({
            "TaskIdList": task_ids,
            "Id": task_ids,
        }),
        4200,
    );
    let ack = response
        .as_ref()
        .and_then(|f| f.pointer("/Data/Data/Ack"))
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);
    let task_details = response
        .as_ref()
        .map(parse_sdcp_task_details_from_response)
        .unwrap_or_default();

    (
        200,
        json!({
            "ok": ack == 0,
            "ack": ack,
            "taskIds": task_ids,
            "taskDetails": task_details,
            "error": if ack == 0 { Value::Null } else { Value::String("SDCP task detail request failed.".to_string()) },
            "rawResponse": response.unwrap_or(Value::Null),
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
        "printer/status" => sdcp_printer_status(payload).await,
        "printer/webcam/info" => sdcp_webcam_info(payload).await,
        "printer/webcam/enable" => sdcp_toggle_feature(payload, 386, "video-stream", true).await,
        "printer/webcam/disable" => sdcp_toggle_feature(payload, 386, "video-stream", false).await,
        "printer/timelapse/enable" => sdcp_toggle_feature(payload, 387, "time-lapse", true).await,
        "printer/timelapse/disable" => sdcp_toggle_feature(payload, 387, "time-lapse", false).await,
        "plates/list/json" => sdcp_plates_list(payload).await,
        "printer/start" => sdcp_control_operation(payload, 128, op).await,
        "printer/pause" => sdcp_control_operation(payload, 129, op).await,
        "printer/cancel" => sdcp_control_operation(payload, 130, op).await,
        "printer/stop" | "printer/force-stop" => sdcp_control_operation(payload, 130, op).await,
        "printer/resume" => sdcp_control_operation(payload, 131, op).await,
        "printer/unpause" => sdcp_control_operation(payload, 131, op).await,
        "upload/chunk" => sdcp_upload_chunk(payload).await,
        "plate/delete" => sdcp_plate_delete(payload).await,
        "task/history/list" => sdcp_task_history_list(payload).await,
        "task/details" => sdcp_task_details(payload).await,
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
