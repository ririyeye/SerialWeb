// Copyright (C) 电导实验室（Conductance-lab）
// SPDX-License-Identifier: AGPL-3.0-only

mod serial_hub;

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Json;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use serial_hub::{Hub, HubEvent, OpenReq, PortDto};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Notify};

const COOKIE_NAME: &str = "serialweb_token";

#[derive(Clone)]
struct AppState {
    token: String,
    index_path: PathBuf,
    hub: Hub,
    generation: Arc<AtomicU64>,
    notify: Arc<Notify>,
}

#[derive(Debug, Deserialize)]
struct TokenQuery {
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClientText {
    t: String,
    id: Option<u64>,
    path: Option<String>,
    #[serde(rename = "baudRate")]
    baud_rate: Option<u32>,
    #[serde(rename = "dataBits")]
    data_bits: Option<u8>,
    #[serde(rename = "stopBits")]
    stop_bits: Option<u8>,
    parity: Option<String>,
    #[serde(rename = "flowControl")]
    flow_control: Option<String>,
    dtr: Option<bool>,
    rts: Option<bool>,
    #[serde(rename = "break")]
    brk: Option<bool>,
}

struct Args {
    bind: String,
    index: Option<PathBuf>,
    extra: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let index_path = find_index(args.index);
    if !index_path.is_file() {
        eprintln!("找不到 index.html：{}", index_path.display());
        eprintln!("请在 SerialWeb 目录下启动，或用 --index 指定页面文件。");
        std::process::exit(1);
    }
    let bind: SocketAddr = match args.bind.parse() {
        Ok(addr) => addr,
        Err(_) => {
            eprintln!("无法解析监听地址 {}", args.bind);
            std::process::exit(1);
        }
    };
    let token = random_token();
    let hub = Hub::spawn(args.extra);
    let app = AppState {
        token: token.clone(),
        index_path,
        hub,
        generation: Arc::new(AtomicU64::new(0)),
        notify: Arc::new(Notify::new()),
    };
    let listener = match TcpListener::bind(bind).await {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("监听 {bind} 失败: {err}");
            std::process::exit(1);
        }
    };
    let local = listener.local_addr().unwrap_or(bind);
    print_banner(local, &token);
    let router = build_router(app);
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .unwrap_or_else(|err| {
            eprintln!("服务退出: {err}");
        });
}

fn build_router(app: AppState) -> Router {
    Router::new()
        .route("/", get(index_page))
        .route("/index.html", get(index_page))
        .route("/api/transport", get(transport))
        .route("/api/serial", get(serial_ws))
        .with_state(app)
}

fn parse_args() -> Args {
    let mut args = Args {
        bind: "0.0.0.0:8787".into(),
        index: None,
        extra: Vec::new(),
    };
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--bind" => {
                args.bind = expect_value("--bind", iter.next());
            }
            "--index" => {
                args.index = Some(PathBuf::from(expect_value("--index", iter.next())));
            }
            "--extra-port" => {
                args.extra.push(expect_value("--extra-port", iter.next()));
            }
            other => {
                eprintln!("未知参数 {other}");
                print_help();
                std::process::exit(2);
            }
        }
    }
    args
}

fn expect_value(flag: &str, value: Option<String>) -> String {
    match value {
        Some(value) if !value.starts_with('-') => value,
        _ => {
            eprintln!("{flag} 缺少参数");
            print_help();
            std::process::exit(2);
        }
    }
}

fn print_help() {
    eprintln!(
        "\
用法: serialweb-server [--bind 0.0.0.0:8787] [--index index.html] [--extra-port /dev/pts/N]

在接了串口的调试机上启动。远程浏览器打开终端里打印的地址，即可操作这台机器上的串口。
--extra-port 可重复，用来补上枚举不到的设备节点。"
    );
}

fn find_index(explicit: Option<PathBuf>) -> PathBuf {
    if let Some(path) = explicit {
        return path;
    }
    let mut candidates = vec![
        PathBuf::from("index.html"),
        PathBuf::from("../index.html"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../index.html"),
    ];
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("index.html"));
            if let Some(parent) = dir.parent() {
                candidates.push(parent.join("index.html"));
            }
        }
    }
    candidates
        .into_iter()
        .find(|path| path.is_file())
        .unwrap_or_else(|| PathBuf::from("index.html"))
}

fn print_banner(addr: SocketAddr, token: &str) {
    println!("SerialWeb 串口服务已启动");
    println!("在别的电脑上用浏览器打开：");
    let port = addr.port();
    if addr.ip().is_unspecified() {
        let mut printed = std::collections::HashSet::new();
        for ip in local_ipv4() {
            if printed.insert(ip) {
                println!("  http://{ip}:{port}/?token={token}");
            }
        }
        println!("  http://127.0.0.1:{port}/?token={token}");
    } else {
        println!("  http://{addr}/?token={token}");
    }
    println!("串口在这台机器上打开。这个地址带有访问令牌，只在调试网络里使用。");
}

fn local_ipv4() -> Vec<Ipv4Addr> {
    let mut ips = Vec::new();
    if let Ok(socket) = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)) {
        if socket.connect("8.8.8.8:80").is_ok() {
            if let Ok(addr) = socket.local_addr() {
                if let IpAddr::V4(ip) = addr.ip() {
                    if !ip.is_loopback() && !ip.is_unspecified() {
                        ips.push(ip);
                    }
                }
            }
        }
    }
    ips
}

fn random_token() -> String {
    let mut buf = [0u8; 24];
    let read = std::fs::File::open("/dev/urandom").and_then(|mut file| {
        use std::io::Read;
        file.read_exact(&mut buf)
    });
    if read.is_ok() {
        return buf.iter().map(|byte| format!("{byte:02x}")).collect();
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{nanos:032x}")
}

fn cookie_token(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    for part in raw.split(';') {
        let part = part.trim();
        let prefix = format!("{COOKIE_NAME}=");
        if let Some(value) = part.strip_prefix(&prefix) {
            return Some(value.to_string());
        }
    }
    None
}

fn authorized(app: &AppState, headers: &HeaderMap, query: &TokenQuery) -> bool {
    query.token.as_deref() == Some(app.token.as_str())
        || cookie_token(headers).as_deref() == Some(app.token.as_str())
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        "<!doctype html><meta charset=\"utf-8\"><title>SerialWeb</title><p>请使用服务器启动时打印的地址打开这个页面。</p>",
    )
        .into_response()
}

async fn transport(
    State(app): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<TokenQuery>,
) -> Response {
    if !authorized(&app, &headers, &query) {
        return unauthorized();
    }
    Json(json!({ "mode": "rust", "serial": "websocket" })).into_response()
}

async fn index_page(
    State(app): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<TokenQuery>,
) -> Response {
    if !authorized(&app, &headers, &query) {
        return unauthorized();
    }
    let bytes = match tokio::fs::read(&app.index_path).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("无法读取 index.html：{err}"),
            )
                .into_response();
        }
    };
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(bytes))
        .unwrap();
    if query.token.as_deref() == Some(app.token.as_str()) {
        let cookie = format!(
            "{COOKIE_NAME}={}; Path=/; HttpOnly; SameSite=Lax",
            app.token
        );
        if let Ok(value) = HeaderValue::from_str(&cookie) {
            response.headers_mut().insert(header::SET_COOKIE, value);
        }
    }
    response
}

async fn serial_ws(
    State(app): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<TokenQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    if !authorized(&app, &headers, &query) {
        return unauthorized();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, app))
}

async fn handle_socket(socket: WebSocket, app: AppState) {
    let session = app.generation.fetch_add(1, Ordering::SeqCst) + 1;
    app.notify.notify_waiters();
    if let Err(err) = app.hub.adopt(session).await {
        eprintln!("接管串口会话失败: {err}");
        return;
    }
    eprintln!("网页已连接");

    let (event_tx, mut event_rx) = mpsc::channel::<HubEvent>(256);
    app.hub.set_event_sender(session, event_tx);
    let (mut sink, mut stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(32);

    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                event = event_rx.recv() => {
                    match event {
                        Some(event) => {
                            if sink.send(event_to_message(event)).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                message = out_rx.recv() => {
                    match message {
                        Some(message) => {
                            if sink.send(message).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
            }
        }
    });

    let ports = tokio::task::spawn_blocking({
        let hub = app.hub.clone();
        move || hub.list()
    })
    .await
    .unwrap_or_default();
    let _ = out_tx
        .send(json_msg(json!({ "t": "hello", "ports": ports })))
        .await;

    loop {
        let notified = app.notify.notified();
        tokio::pin!(notified);
        if app.generation.load(Ordering::SeqCst) != session {
            break;
        }
        tokio::select! {
            _ = &mut notified => continue,
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(message)) => {
                        if handle_client_message(&app, session, message, &out_tx).await.is_err() {
                            break;
                        }
                    }
                    _ => break,
                }
            }
        }
    }

    drop(out_tx);
    writer.abort();
    app.hub.clear_event_sender(session);
    let _ = app.hub.end_session(session).await;
    eprintln!("网页已断开");
}

async fn handle_client_message(
    app: &AppState,
    session: u64,
    message: Message,
    out_tx: &mpsc::Sender<Message>,
) -> Result<(), ()> {
    match message {
        Message::Text(text) => {
            let parsed: ClientText = match serde_json::from_str(text.as_str()) {
                Ok(parsed) => parsed,
                Err(_) => return Ok(()),
            };
            let id = parsed.id;
            let reply = match parsed.t.as_str() {
                "list" => {
                    let items = tokio::task::spawn_blocking({
                        let hub = app.hub.clone();
                        move || hub.list()
                    })
                    .await
                    .unwrap_or_default();
                    ok_reply(id, json!({ "t": "ports", "items": items }))
                }
                "open" => {
                    let req = OpenReq {
                        path: parsed.path.unwrap_or_default(),
                        baud_rate: parsed.baud_rate.unwrap_or(115200),
                        data_bits: parsed.data_bits.unwrap_or(8),
                        stop_bits: parsed.stop_bits.unwrap_or(1),
                        parity: parsed.parity.unwrap_or_else(|| "none".into()),
                        flow_control: parsed.flow_control.unwrap_or_else(|| "none".into()),
                    };
                    match app.hub.open(session, req).await {
                        Ok(port) => ok_reply(
                            id,
                            serde_json::to_value(opened_body(id, &port))
                                .unwrap_or_else(|_| json!({})),
                        ),
                        Err(err) => ok_reply(id, json!({ "t": "error", "message": err })),
                    }
                }
                "close" => match app.hub.close_port(session).await {
                    Ok(()) => ok_reply(id, json!({ "t": "closed", "reason": "manual" })),
                    Err(err) => ok_reply(id, json!({ "t": "error", "message": err })),
                },
                "setSignals" => match app
                    .hub
                    .set_signals(session, parsed.dtr, parsed.rts, parsed.brk)
                    .await
                {
                    Ok(()) => ok_reply(id, json!({ "t": "signalsSet" })),
                    Err(err) => ok_reply(id, json!({ "t": "error", "message": err })),
                },
                "getSignals" => match app.hub.get_signals(session).await {
                    Ok(signals) => ok_reply(
                        id,
                        json!({
                            "t": "signals",
                            "cts": signals.cts,
                            "dsr": signals.dsr,
                            "dcd": signals.dcd,
                            "ri": signals.ri
                        }),
                    ),
                    Err(err) => ok_reply(id, json!({ "t": "error", "message": err })),
                },
                "ping" => ok_reply(id, json!({ "t": "pong" })),
                _ => ok_reply(id, json!({ "t": "error", "message": "未知指令" })),
            };
            out_tx.send(reply).await.map_err(|_| ())?;
            Ok(())
        }
        Message::Binary(bytes) => {
            if bytes.len() < 5 || bytes[0] != 0x02 {
                return Ok(());
            }
            let id = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as u64;
            let data = bytes[5..].to_vec();
            let reply = match app.hub.write(session, data).await {
                Ok(size) => json_msg(json!({ "t": "wrote", "id": id, "n": size })),
                Err(err) => json_msg(json!({ "t": "error", "id": id, "message": err })),
            };
            out_tx.send(reply).await.map_err(|_| ())?;
            Ok(())
        }
        Message::Ping(payload) => {
            out_tx.send(Message::Pong(payload)).await.map_err(|_| ())?;
            Ok(())
        }
        Message::Pong(_) => Ok(()),
        Message::Close(_) => Err(()),
    }
}

fn opened_body(id: Option<u64>, port: &PortDto) -> serde_json::Value {
    let mut body = serde_json::to_value(port).unwrap_or_else(|_| json!({}));
    if let Some(map) = body.as_object_mut() {
        map.insert("t".into(), json!("opened"));
        if let Some(id) = id {
            map.insert("id".into(), json!(id));
        }
    }
    body
}

fn ok_reply(id: Option<u64>, mut body: serde_json::Value) -> Message {
    if let Some(id) = id {
        if let Some(map) = body.as_object_mut() {
            map.entry("id").or_insert(json!(id));
        }
    }
    json_msg(body)
}

fn json_msg(value: serde_json::Value) -> Message {
    Message::Text(value.to_string().into())
}

fn event_to_message(event: HubEvent) -> Message {
    match event {
        HubEvent::Rx { ts_ms, data } => {
            let mut frame = Vec::with_capacity(9 + data.len());
            frame.push(0x01);
            frame.extend_from_slice(&ts_ms.to_le_bytes());
            frame.extend_from_slice(&data);
            Message::Binary(frame.into())
        }
        HubEvent::Closed { reason } => json_msg(json!({ "t": "closed", "reason": reason })),
        HubEvent::Attach(port) => json_msg(json!({ "t": "attach", "port": port })),
        HubEvent::Detach { path } => json_msg(json!({ "t": "detach", "path": path })),
    }
}
