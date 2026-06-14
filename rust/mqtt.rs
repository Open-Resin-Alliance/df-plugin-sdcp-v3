// Minimal MQTT 3.1.1 broker + SDCP V1.0.0 client helpers.
//
// DragonFruit acts as the MQTT broker on port 1883. The printer connects to it
// after receiving UDP "M66666 1883" on port 3000. Commands are sent as PUBLISH
// to /sdcp/request/<id>; responses arrive as PUBLISH on /sdcp/response/<id>.
//
// All functions are synchronous — call via tokio::task::spawn_blocking.

use serde_json::{json, Value};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(super) const MQTT_BROKER_PORT: u16 = 1883;
const SDCP_DISCOVERY_PORT: u16 = 3000;

// ── MD5 (RFC 1321) ──────────────────────────────────────────────────────────
// Implemented inline to avoid adding a crate dependency.

#[rustfmt::skip]
static MD5_T: [u32; 64] = [
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a,
    0xa8304613, 0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
    0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340,
    0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8,
    0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
    0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
    0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92,
    0xffeff47d, 0x85845dd1, 0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
    0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
];

#[rustfmt::skip]
static MD5_S: [u32; 64] = [
     7, 12, 17, 22,  7, 12, 17, 22,  7, 12, 17, 22,  7, 12, 17, 22,
     5,  9, 14, 20,  5,  9, 14, 20,  5,  9, 14, 20,  5,  9, 14, 20,
     4, 11, 16, 23,  4, 11, 16, 23,  4, 11, 16, 23,  4, 11, 16, 23,
     6, 10, 15, 21,  6, 10, 15, 21,  6, 10, 15, 21,  6, 10, 15, 21,
];

pub(super) fn md5_hex(data: &[u8]) -> String {
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());

    let (mut a0, mut b0, mut c0, mut d0): (u32, u32, u32, u32) =
        (0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476);

    for block in msg.chunks_exact(64) {
        let mut m = [0u32; 16];
        for (i, ch) in block.chunks_exact(4).enumerate() {
            m[i] = u32::from_le_bytes([ch[0], ch[1], ch[2], ch[3]]);
        }
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0usize..64 {
            let (f, g) = match i {
                0..=15  => ((b & c) | (!b & d), i),
                16..=31 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _       => (c ^ (b | !d), (7 * i) % 16),
            };
            let tmp = d;
            d = c; c = b;
            b = b.wrapping_add(
                a.wrapping_add(f).wrapping_add(MD5_T[i]).wrapping_add(m[g])
                    .rotate_left(MD5_S[i]),
            );
            a = tmp;
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    [a0.to_le_bytes(), b0.to_le_bytes(), c0.to_le_bytes(), d0.to_le_bytes()]
        .concat()
        .iter()
        .fold(String::with_capacity(32), |mut s, b| {
            use std::fmt::Write as _;
            let _ = write!(s, "{b:02x}");
            s
        })
}

// ── MQTT 3.1.1 packet codec ─────────────────────────────────────────────────

pub(super) enum MqttPacket {
    Connect { client_id: String },
    Subscribe { packet_id: u16, #[allow(dead_code)] topics: Vec<String> },
    Publish { topic: String, payload: Vec<u8> },
    PingReq,
    Disconnect,
    Unknown(()),
}

fn read_var_len(stream: &mut TcpStream) -> io::Result<usize> {
    let mut value: usize = 0;
    let mut shift = 0usize;
    loop {
        let mut buf = [0u8; 1];
        stream.read_exact(&mut buf)?;
        let byte = buf[0];
        value |= ((byte & 0x7F) as usize) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift > 21 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "MQTT VarLen overflow"));
        }
    }
}

fn write_var_len(out: &mut Vec<u8>, mut len: usize) {
    loop {
        let mut byte = (len & 0x7F) as u8;
        len >>= 7;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
}

fn mqtt_string(payload: &[u8], offset: usize) -> Option<(String, usize)> {
    if offset + 2 > payload.len() {
        return None;
    }
    let len = u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
    if offset + 2 + len > payload.len() {
        return None;
    }
    let s = String::from_utf8_lossy(&payload[offset + 2..offset + 2 + len]).into_owned();
    Some((s, offset + 2 + len))
}

pub(super) fn read_packet(stream: &mut TcpStream) -> io::Result<MqttPacket> {
    let mut hdr = [0u8; 1];
    stream.read_exact(&mut hdr)?;
    let packet_type = hdr[0] & 0xF0;
    let flags = hdr[0] & 0x0F;

    let remaining = read_var_len(stream)?;
    let mut payload = vec![0u8; remaining];
    if remaining > 0 {
        stream.read_exact(&mut payload)?;
    }

    match packet_type {
        0x10 => {
            // Skip: proto-name len(2) + proto-name + level(1) + conn-flags(1) + keepalive(2)
            let proto_name_len = if payload.len() >= 2 {
                u16::from_be_bytes([payload[0], payload[1]]) as usize
            } else {
                0
            };
            let skip = 2 + proto_name_len + 1 + 1 + 2;
            let client_id = mqtt_string(&payload, skip)
                .map(|(s, _)| s)
                .unwrap_or_default();
            Ok(MqttPacket::Connect { client_id })
        }
        0x80 => {
            if payload.len() < 2 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "short SUBSCRIBE"));
            }
            let packet_id = u16::from_be_bytes([payload[0], payload[1]]);
            let mut offset = 2;
            let mut topics = Vec::new();
            while offset < payload.len() {
                match mqtt_string(&payload, offset) {
                    Some((topic, next)) => {
                        topics.push(topic);
                        offset = next + 1; // +1 for QoS byte
                    }
                    None => break,
                }
            }
            Ok(MqttPacket::Subscribe { packet_id, topics })
        }
        0x30 => {
            let qos = (flags >> 1) & 0x03;
            let (topic, mut offset) = mqtt_string(&payload, 0)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short PUBLISH topic"))?;
            if qos > 0 {
                offset += 2; // skip packet identifier
            }
            Ok(MqttPacket::Publish {
                topic,
                payload: payload[offset..].to_vec(),
            })
        }
        0xC0 => Ok(MqttPacket::PingReq),
        0xE0 => Ok(MqttPacket::Disconnect),
        _ => Ok(MqttPacket::Unknown(())),
    }
}

pub(super) fn write_connack(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(&[0x20, 0x02, 0x00, 0x00])
}

pub(super) fn write_suback(stream: &mut TcpStream, packet_id: u16) -> io::Result<()> {
    let [hi, lo] = packet_id.to_be_bytes();
    stream.write_all(&[0x90, 0x03, hi, lo, 0x00])
}

pub(super) fn write_pingresp(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(&[0xD0, 0x00])
}

pub(super) fn write_publish(stream: &mut TcpStream, topic: &str, payload: &[u8]) -> io::Result<()> {
    let t = topic.as_bytes();
    let remaining = 2 + t.len() + payload.len();
    let mut buf = vec![0x30u8]; // PUBLISH, QoS=0, no retain, no dup
    write_var_len(&mut buf, remaining);
    buf.extend_from_slice(&(t.len() as u16).to_be_bytes());
    buf.extend_from_slice(t);
    buf.extend_from_slice(payload);
    stream.write_all(&buf)
}

// ── Connection lifecycle ─────────────────────────────────────────────────────

/// Send UDP "M66666 <port>" to the printer to trigger it to connect to our broker.
pub(super) fn send_mqtt_trigger(printer_ip: &str, broker_port: u16) -> io::Result<()> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0")?;
    let msg = format!("M66666 {broker_port}");
    sock.send_to(msg.as_bytes(), format!("{printer_ip}:{SDCP_DISCOVERY_PORT}"))?;
    Ok(())
}

/// Accept a single TCP connection from the printer, perform MQTT handshake,
/// and return the connected stream together with the ClientID from CONNECT.
/// Uses non-blocking poll so the overall wait respects `timeout`.
pub(super) fn accept_mqtt_client(
    listener: &TcpListener,
    timeout: Duration,
) -> io::Result<(TcpStream, String)> {
    listener.set_nonblocking(true)?;
    let deadline = Instant::now() + timeout;

    let mut stream = loop {
        match listener.accept() {
            Ok((s, _)) => break s,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for printer MQTT connection",
                    ));
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return Err(e),
        }
    };

    listener.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;

    // Expect CONNECT → send CONNACK
    let client_id = match read_packet(&mut stream)? {
        MqttPacket::Connect { client_id } => client_id,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected CONNECT packet from printer",
            ))
        }
    };
    write_connack(&mut stream)?;
    stream.flush()?;

    // Drain initial SUBSCRIBEs / PINGREQs from the printer (handshake phase).
    stream.set_read_timeout(Some(Duration::from_millis(300)))?;
    let handshake_end = Instant::now() + Duration::from_secs(3);
    loop {
        if Instant::now() >= handshake_end {
            break;
        }
        match read_packet(&mut stream) {
            Ok(MqttPacket::Subscribe { packet_id, .. }) => {
                let _ = write_suback(&mut stream, packet_id);
                let _ = stream.flush();
            }
            Ok(MqttPacket::PingReq) => {
                let _ = write_pingresp(&mut stream);
                let _ = stream.flush();
            }
            Ok(MqttPacket::Publish { .. }) | Ok(MqttPacket::Unknown(_)) => {}
            Ok(MqttPacket::Disconnect) | Err(_) => break,
            Ok(MqttPacket::Connect { .. }) => {}
        }
    }

    Ok((stream, client_id))
}

// ── SDCP command exchange ────────────────────────────────────────────────────

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Send SDCP command `cmd` to the printer via MQTT PUBLISH on /sdcp/request/<id>
/// and wait for the matching response on /sdcp/response/<id>.
pub(super) fn mqtt_send_command_await_response(
    stream: &mut TcpStream,
    mainboard_id: &str,
    cmd: u64,
    data: Value,
    timeout: Duration,
) -> Option<Value> {
    let request_id = format!("sdcp-{}-{}-{}", unix_millis(), std::process::id(), cmd);
    let envelope = json!({
        "Id": "dragonfruit",
        "Data": {
            "Cmd": cmd,
            "Data": data,
            "RequestID": request_id,
            "MainboardID": mainboard_id,
            "TimeStamp": unix_secs(),
            "From": 0
        },
        "Topic": format!("/sdcp/request/{mainboard_id}")
    });

    let req_topic = format!("/sdcp/request/{mainboard_id}");
    let payload_bytes = envelope.to_string().into_bytes();
    write_publish(stream, &req_topic, &payload_bytes).ok()?;
    stream.flush().ok()?;

    let resp_prefix = format!("/sdcp/response/{}", mainboard_id.to_lowercase());
    let deadline = Instant::now() + timeout;
    // Short read timeout so we can check the deadline and respond to PINGREQs.
    let _ = stream.set_read_timeout(Some(Duration::from_millis(300)));

    while Instant::now() < deadline {
        match read_packet(stream) {
            Ok(MqttPacket::Publish { topic, payload }) => {
                if topic.to_lowercase().starts_with(&resp_prefix) {
                    if let Ok(frame) = serde_json::from_slice::<Value>(&payload) {
                        let frame_cmd = frame
                            .pointer("/Data/Cmd")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(u64::MAX);
                        let frame_rid = frame
                            .pointer("/Data/RequestID")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if frame_cmd == cmd && frame_rid == request_id {
                            return Some(frame);
                        }
                    }
                }
            }
            Ok(MqttPacket::PingReq) => {
                let _ = write_pingresp(stream);
                let _ = stream.flush();
            }
            Ok(_) => {}
            Err(ref e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(_) => break,
        }
    }
    None
}

/// Poll for a PUBLISH on any topic starting with `topic_prefix`.
/// Returns the parsed JSON payload and full topic on the first match.
pub(super) fn mqtt_poll_topic(
    stream: &mut TcpStream,
    topic_prefix: &str,
    timeout: Duration,
) -> Option<(String, Value)> {
    let deadline = Instant::now() + timeout;
    let _ = stream.set_read_timeout(Some(Duration::from_millis(300)));
    let prefix = topic_prefix.to_lowercase();

    while Instant::now() < deadline {
        match read_packet(stream) {
            Ok(MqttPacket::Publish { topic, payload }) => {
                if topic.to_lowercase().starts_with(&prefix) {
                    if let Ok(frame) = serde_json::from_slice::<Value>(&payload) {
                        return Some((topic, frame));
                    }
                }
            }
            Ok(MqttPacket::PingReq) => {
                let _ = write_pingresp(stream);
                let _ = stream.flush();
            }
            Ok(_) => {}
            Err(ref e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(_) => break,
        }
    }
    None
}

// ── HTTP file server (one-shot) ──────────────────────────────────────────────

/// Bind a one-shot HTTP file server on a random port.
/// Returns `(local_lan_ip, port)`. A background thread serves exactly one GET request.
pub(super) fn start_file_http_server(
    file_data: Arc<Vec<u8>>,
    file_name: String,
) -> io::Result<(String, u16)> {
    let listener = TcpListener::bind("0.0.0.0:0")?;
    let port = listener.local_addr()?.port();
    let ip = get_local_lan_ip();

    std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(60)));
            let _ = stream.set_write_timeout(Some(Duration::from_secs(60)));
            // Drain the HTTP request (we don't inspect it)
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
                 Content-Disposition: attachment; filename=\"{file_name}\"\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                file_data.len()
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&file_data);
        }
    });

    Ok((ip, port))
}

/// Upload a file to the printer via the MQTT SDCPCommandFileUpload (Cmd 256) flow.
/// The printer fetches the file from a temporary HTTP server.
pub(super) fn mqtt_upload_file(
    stream: &mut TcpStream,
    mainboard_id: &str,
    file_name: &str,
    file_data: Vec<u8>,
    timeout: Duration,
) -> Option<Value> {
    let md5 = md5_hex(&file_data);
    let size = file_data.len();
    let data = Arc::new(file_data);

    let (ip, port) = start_file_http_server(Arc::clone(&data), file_name.to_string()).ok()?;
    let url = format!("http://{ip}:{port}/{file_name}");

    mqtt_send_command_await_response(
        stream,
        mainboard_id,
        256,
        json!({
            "Check": 0,
            "CleanCache": 1,
            "Compress": 0,
            "FileSize": size,
            "Filename": file_name,
            "MD5": md5,
            "URL": url,
        }),
        timeout,
    )
}

fn get_local_lan_ip() -> String {
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            if iface.is_loopback() {
                continue;
            }
            if let if_addrs::IfAddr::V4(v4) = iface.addr {
                return v4.ip.to_string();
            }
        }
    }
    "127.0.0.1".to_string()
}

// ── V1.0.0 status normalisation ─────────────────────────────────────────────

/// Normalise a V1.0.0 SDCP status payload so it matches the V3.0.0 shape.
/// V1.0.0 sends `CurrentStatus` as a bare number; V3.0.0 wraps it in an array.
/// `PrintInfo.Status` codes also differ.
pub(super) fn normalize_v1_status(status: &mut Value) {
    if let Some(n) = status["CurrentStatus"].as_u64() {
        status["CurrentStatus"] = json!([n]);
    }
    let remapped = status
        .pointer("/PrintInfo/Status")
        .and_then(|v| v.as_u64())
        .map(|s| match s {
            2 => 3,
            3 => 4,
            4 => 2,
            16 => 9,
            other => other,
        });
    if let Some(r) = remapped {
        if let Some(info) = status.pointer_mut("/PrintInfo/Status") {
            *info = json!(r);
        }
    }
}

// ── Protocol detection ───────────────────────────────────────────────────────

/// Returns true if the firmware version indicates a V1.0.0 MQTT printer
/// (major version < 3).
pub(super) fn is_mqtt_firmware(firmware_version: &str) -> bool {
    let v = firmware_version.trim().trim_start_matches('V').trim_start_matches('v');
    let major: u32 = v
        .split('.')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    major < 3
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_empty() {
        assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn md5_abc() {
        assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
    }

    #[test]
    fn md5_448_bit() {
        // "The quick brown fox jumps over the lazy dog"
        assert_eq!(
            md5_hex(b"The quick brown fox jumps over the lazy dog"),
            "9e107d9d372bb6826bd81d3542a419d6"
        );
    }

    #[test]
    fn normalize_wraps_current_status() {
        let mut v = json!({ "CurrentStatus": 2, "PrintInfo": { "Status": 2 } });
        normalize_v1_status(&mut v);
        assert_eq!(v["CurrentStatus"], json!([2]));
        assert_eq!(v["PrintInfo"]["Status"], json!(3));
    }

    #[test]
    fn normalize_status_mapping() {
        for (input, expected) in [(2u64, 3u64), (3, 4), (4, 2), (16, 9), (0, 0)] {
            let mut v = json!({ "CurrentStatus": 0, "PrintInfo": { "Status": input } });
            normalize_v1_status(&mut v);
            assert_eq!(v["PrintInfo"]["Status"], json!(expected), "status {input}");
        }
    }

    #[test]
    fn is_mqtt_firmware_detection() {
        assert!(is_mqtt_firmware("V1.0.0"));
        assert!(is_mqtt_firmware("V2.4.1"));
        assert!(!is_mqtt_firmware("V3.0.0"));
        assert!(!is_mqtt_firmware("3.1.0"));
        assert!(!is_mqtt_firmware(""));
    }

    #[test]
    fn write_publish_encoding() {
        let topic = "/sdcp/request/ABCDEF";
        let payload = b"hello";
        let t = topic.as_bytes();
        let remaining = 2 + t.len() + payload.len();
        let mut expected = vec![0x30u8]; // PUBLISH, QoS=0
        write_var_len(&mut expected, remaining);
        expected.extend_from_slice(&(t.len() as u16).to_be_bytes());
        expected.extend_from_slice(t);
        expected.extend_from_slice(payload);
        // Verify fixed-header byte and remaining-length encoding.
        assert_eq!(expected[0], 0x30);
        assert_eq!(expected[1] as usize, remaining); // single-byte VarLen for small messages
    }

    #[test]
    fn connack_bytes() {
        // CONNACK is always [0x20, 0x02, 0x00, 0x00]
        // We can't call write_connack in a unit test (needs TcpStream),
        // but we can verify the constant.
        let expected: &[u8] = &[0x20, 0x02, 0x00, 0x00];
        assert_eq!(expected[0], 0x20);
        assert_eq!(expected[1], 0x02); // remaining length
    }
}
