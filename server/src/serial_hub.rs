// Copyright (C) 电导实验室（Conductance-lab）
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serialport::{DataBits, FlowControl, Parity, SerialPort, SerialPortType, StopBits};
use tokio::sync::mpsc::error::TrySendError;

#[derive(Clone, Debug, Serialize)]
pub struct PortDto {
    pub path: String,
    pub device: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vid: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manufacturer: Option<String>,
}

#[derive(Clone, Debug)]
pub struct OpenReq {
    pub path: String,
    pub baud_rate: u32,
    pub data_bits: u8,
    pub stop_bits: u8,
    pub parity: String,
    pub flow_control: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct SignalState {
    pub cts: Option<bool>,
    pub dsr: Option<bool>,
    pub dcd: Option<bool>,
    pub ri: Option<bool>,
}

#[derive(Debug)]
pub enum HubEvent {
    Rx { ts_ms: u64, data: Vec<u8> },
    Closed { reason: String },
    Attach(PortDto),
    Detach { path: String },
}

struct EventSlot {
    session: u64,
    tx: tokio::sync::mpsc::Sender<HubEvent>,
}

enum Cmd {
    Adopt {
        session: u64,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    EndSession {
        session: u64,
        reply: tokio::sync::oneshot::Sender<()>,
    },
    Open {
        session: u64,
        req: OpenReq,
        reply: tokio::sync::oneshot::Sender<Result<PortDto, String>>,
    },
    ClosePort {
        session: u64,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    Write {
        session: u64,
        data: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<Result<usize, String>>,
    },
    SetSignals {
        session: u64,
        dtr: Option<bool>,
        rts: Option<bool>,
        brk: Option<bool>,
        reply: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    GetSignals {
        session: u64,
        reply: tokio::sync::oneshot::Sender<Result<SignalState, String>>,
    },
    Shutdown,
}

struct HubInner {
    cmd_tx: Sender<Cmd>,
    event_slot: Arc<Mutex<Option<EventSlot>>>,
    extra: Vec<String>,
    stop: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
    hotplug: Mutex<Option<tokio::task::AbortHandle>>,
}

#[derive(Clone)]
pub struct Hub {
    inner: Arc<HubInner>,
}

impl Drop for HubInner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.hotplug.lock().unwrap().take() {
            handle.abort();
        }
        let _ = self.cmd_tx.send(Cmd::Shutdown);
        if let Some(handle) = self.join.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

impl Hub {
    pub fn spawn(extra: Vec<String>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let event_slot = Arc::new(Mutex::new(None));
        let open_path = Arc::new(Mutex::new(None));
        let stop = Arc::new(AtomicBool::new(false));
        let inner = Arc::new(HubInner {
            cmd_tx,
            event_slot: Arc::clone(&event_slot),
            extra: extra.clone(),
            stop: Arc::clone(&stop),
            join: Mutex::new(None),
            hotplug: Mutex::new(None),
        });

        let thread_slot = Arc::clone(&event_slot);
        let thread_open = Arc::clone(&open_path);
        let thread_extra = extra.clone();
        let join = thread::Builder::new()
            .name("serialweb-serial".into())
            .spawn(move || serial_thread(cmd_rx, thread_slot, thread_open, thread_extra))
            .expect("启动串口线程失败");
        *inner.join.lock().unwrap() = Some(join);

        let hotplug_slot = Arc::clone(&event_slot);
        let hotplug_open = Arc::clone(&open_path);
        let hotplug_stop = Arc::clone(&stop);
        let hotplug_extra = extra;
        let hotplug = tokio::spawn(async move {
            let mut known = port_key_set(&list_ports(&hotplug_extra));
            while !hotplug_stop.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if hotplug_stop.load(Ordering::SeqCst) {
                    break;
                }
                let extra = hotplug_extra.clone();
                let listed = tokio::task::spawn_blocking(move || list_ports(&extra))
                    .await
                    .unwrap_or_default();
                let map = port_map(listed);
                let open = hotplug_open.lock().unwrap().clone();
                for (key, port) in &map {
                    if !known.contains(key) {
                        emit_hotplug(&hotplug_slot, HubEvent::Attach(port.clone()));
                    }
                }
                for key in &known {
                    if map.contains_key(key) {
                        continue;
                    }
                    if open.as_deref() == Some(key.as_str()) {
                        continue;
                    }
                    emit_hotplug(&hotplug_slot, HubEvent::Detach { path: key.clone() });
                }
                known = map.into_keys().collect();
            }
        });
        *inner.hotplug.lock().unwrap() = Some(hotplug.abort_handle());
        drop(hotplug);

        Self { inner }
    }

    pub fn list(&self) -> Vec<PortDto> {
        list_ports(&self.inner.extra)
    }

    pub fn set_event_sender(&self, session: u64, tx: tokio::sync::mpsc::Sender<HubEvent>) {
        *self.inner.event_slot.lock().unwrap() = Some(EventSlot { session, tx });
    }

    pub fn clear_event_sender(&self, session: u64) {
        let mut slot = self.inner.event_slot.lock().unwrap();
        if slot.as_ref().map(|item| item.session) == Some(session) {
            *slot = None;
        }
    }

    pub async fn adopt(&self, session: u64) -> Result<(), String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.send(Cmd::Adopt {
            session,
            reply: reply_tx,
        })?;
        wait_reply(reply_rx).await
    }

    pub async fn end_session(&self, session: u64) -> Result<(), String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.send(Cmd::EndSession {
            session,
            reply: reply_tx,
        })?;
        wait_reply(reply_rx).await
    }

    pub async fn open(&self, session: u64, req: OpenReq) -> Result<PortDto, String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.send(Cmd::Open {
            session,
            req,
            reply: reply_tx,
        })?;
        wait_reply(reply_rx).await?
    }

    pub async fn close_port(&self, session: u64) -> Result<(), String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.send(Cmd::ClosePort {
            session,
            reply: reply_tx,
        })?;
        wait_reply(reply_rx).await?
    }

    pub async fn write(&self, session: u64, data: Vec<u8>) -> Result<usize, String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.send(Cmd::Write {
            session,
            data,
            reply: reply_tx,
        })?;
        wait_reply(reply_rx).await?
    }

    pub async fn set_signals(
        &self,
        session: u64,
        dtr: Option<bool>,
        rts: Option<bool>,
        brk: Option<bool>,
    ) -> Result<(), String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.send(Cmd::SetSignals {
            session,
            dtr,
            rts,
            brk,
            reply: reply_tx,
        })?;
        wait_reply(reply_rx).await?
    }

    pub async fn get_signals(&self, session: u64) -> Result<SignalState, String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.send(Cmd::GetSignals {
            session,
            reply: reply_tx,
        })?;
        wait_reply(reply_rx).await?
    }

    fn send(&self, cmd: Cmd) -> Result<(), String> {
        self.inner
            .cmd_tx
            .send(cmd)
            .map_err(|_| "串口线程已退出".to_string())
    }
}

async fn wait_reply<T>(rx: tokio::sync::oneshot::Receiver<T>) -> Result<T, String> {
    match tokio::time::timeout(Duration::from_secs(8), rx).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(_)) => Err("串口线程无响应".into()),
        Err(_) => Err("串口操作超时".into()),
    }
}

fn port_map(ports: Vec<PortDto>) -> HashMap<String, PortDto> {
    ports
        .into_iter()
        .map(|port| (port.path.clone(), port))
        .collect()
}

fn port_key_set(ports: &[PortDto]) -> std::collections::HashSet<String> {
    ports.iter().map(|port| port.path.clone()).collect()
}

fn emit_hotplug(slot: &Mutex<Option<EventSlot>>, event: HubEvent) {
    let guard = slot.lock().unwrap();
    if let Some(slot) = guard.as_ref() {
        let _ = slot.tx.try_send(event);
    }
}

pub fn list_ports(extra: &[String]) -> Vec<PortDto> {
    let by_id = by_id_map();
    let mut keyed: HashMap<String, PortDto> = HashMap::new();
    if let Ok(listed) = serialport::available_ports() {
        for port in listed {
            let device = port.port_name;
            let canonical =
                std::fs::canonicalize(&device).unwrap_or_else(|_| PathBuf::from(&device));
            let path = by_id
                .get(&canonical)
                .map(|item| item.to_string_lossy().into_owned())
                .unwrap_or_else(|| device.clone());
            let (vid, pid, product, manufacturer) = match port.port_type {
                SerialPortType::UsbPort(usb) => (
                    Some(usb.vid),
                    Some(usb.pid),
                    nonempty(usb.product),
                    nonempty(usb.manufacturer),
                ),
                _ => (None, None, None, None),
            };
            keyed.insert(
                canonical.to_string_lossy().into_owned(),
                PortDto {
                    path,
                    device,
                    vid,
                    pid,
                    product,
                    manufacturer,
                },
            );
        }
    }
    for path in extra {
        let trimmed = path.trim();
        if trimmed.is_empty() || !Path::new(trimmed).exists() {
            continue;
        }
        let canonical = std::fs::canonicalize(trimmed).unwrap_or_else(|_| PathBuf::from(trimmed));
        let key = canonical.to_string_lossy().into_owned();
        keyed.entry(key).or_insert_with(|| PortDto {
            path: trimmed.to_string(),
            device: trimmed.to_string(),
            vid: None,
            pid: None,
            product: None,
            manufacturer: None,
        });
    }
    let mut ports: Vec<PortDto> = keyed.into_values().collect();
    ports.sort_by(|left, right| {
        port_rank(&left.path)
            .cmp(&port_rank(&right.path))
            .then(left.path.cmp(&right.path))
    });
    ports
}

fn port_rank(path: &str) -> u8 {
    if path.contains("/by-id/") {
        0
    } else if path.contains("ttyUSB") || path.contains("ttyACM") || path.contains("tty.usb") {
        1
    } else if path.contains("ttyS") {
        3
    } else {
        2
    }
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|text| {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

#[cfg(unix)]
fn by_id_map() -> HashMap<PathBuf, PathBuf> {
    let mut map = HashMap::new();
    let entries = match std::fs::read_dir("/dev/serial/by-id") {
        Ok(entries) => entries,
        Err(_) => return map,
    };
    for entry in entries.flatten() {
        let link = entry.path();
        if let Ok(canonical) = std::fs::canonicalize(&link) {
            map.insert(canonical, link);
        }
    }
    map
}

#[cfg(not(unix))]
fn by_id_map() -> HashMap<PathBuf, PathBuf> {
    HashMap::new()
}

fn serial_thread(
    cmd_rx: Receiver<Cmd>,
    event_slot: Arc<Mutex<Option<EventSlot>>>,
    open_path: Arc<Mutex<Option<String>>>,
    extra: Vec<String>,
) {
    let mut port: Option<Box<dyn SerialPort>> = None;
    let mut session = 0u64;
    let mut pending: VecDeque<HubEvent> = VecDeque::new();
    loop {
        while let Ok(cmd) = cmd_rx.try_recv() {
            if handle_cmd(
                cmd,
                &mut port,
                &mut session,
                &mut pending,
                &open_path,
                &extra,
            ) {
                return;
            }
        }
        if !flush_pending(&event_slot, session, &mut pending) {
            thread::sleep(Duration::from_millis(5));
            continue;
        }
        if port.is_some() {
            match read_available(port.as_mut().unwrap().as_mut()) {
                ReadOutcome::Data(data) => {
                    pending.push_back(HubEvent::Rx {
                        ts_ms: unix_ms(),
                        data,
                    });
                }
                ReadOutcome::Idle => {}
                ReadOutcome::Lost(message) => {
                    eprintln!("串口读失败: {message}");
                    close_port_handle(&mut port, &open_path);
                    pending.push_back(HubEvent::Closed {
                        reason: "lost".into(),
                    });
                }
            }
        } else {
            match cmd_rx.recv_timeout(Duration::from_millis(200)) {
                Ok(cmd) => {
                    if handle_cmd(
                        cmd,
                        &mut port,
                        &mut session,
                        &mut pending,
                        &open_path,
                        &extra,
                    ) {
                        return;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }
}

fn handle_cmd(
    cmd: Cmd,
    port: &mut Option<Box<dyn SerialPort>>,
    session: &mut u64,
    pending: &mut VecDeque<HubEvent>,
    open_path: &Mutex<Option<String>>,
    extra: &[String],
) -> bool {
    match cmd {
        Cmd::Shutdown => true,
        Cmd::Adopt {
            session: next,
            reply,
        } => {
            close_port_handle(port, open_path);
            pending.clear();
            *session = next;
            let _ = reply.send(());
            false
        }
        Cmd::EndSession {
            session: ending,
            reply,
        } => {
            if *session == ending {
                close_port_handle(port, open_path);
                pending.clear();
                *session = 0;
            }
            let _ = reply.send(());
            false
        }
        Cmd::Open {
            session: owner,
            req,
            reply,
        } => {
            if *session != owner {
                let _ = reply.send(Err("串口已由其他网页接管".into()));
                return false;
            }
            close_port_handle(port, open_path);
            match open_port(&req) {
                Ok(opened) => {
                    let dto = describe_open(&req.path, extra);
                    eprintln!(
                        "打开 {} @ {} {}{}{} {}",
                        dto.path,
                        req.baud_rate,
                        req.data_bits,
                        match req.parity.as_str() {
                            "even" => "E",
                            "odd" => "O",
                            _ => "N",
                        },
                        req.stop_bits,
                        req.flow_control
                    );
                    *open_path.lock().unwrap() = Some(dto.path.clone());
                    *port = Some(opened);
                    let _ = reply.send(Ok(dto));
                }
                Err(err) => {
                    eprintln!("打开串口失败: {err}");
                    let _ = reply.send(Err(err));
                }
            }
            false
        }
        Cmd::ClosePort {
            session: owner,
            reply,
        } => {
            if *session == owner {
                if port.is_some() {
                    eprintln!("关闭串口");
                }
                close_port_handle(port, open_path);
            }
            let _ = reply.send(Ok(()));
            false
        }
        Cmd::Write {
            session: owner,
            data,
            reply,
        } => {
            if *session != owner {
                let _ = reply.send(Err("串口已由其他网页接管".into()));
                return false;
            }
            let Some(opened) = port.as_mut() else {
                let _ = reply.send(Err("串口未打开".into()));
                return false;
            };
            let _ = reply.send(write_all_port(opened.as_mut(), &data).map(|_| data.len()));
            false
        }
        Cmd::SetSignals {
            session: owner,
            dtr,
            rts,
            brk,
            reply,
        } => {
            if *session != owner {
                let _ = reply.send(Err("串口已由其他网页接管".into()));
                return false;
            }
            let Some(opened) = port.as_mut() else {
                let _ = reply.send(Err("串口未打开".into()));
                return false;
            };
            let _ = reply.send(apply_signals(opened.as_mut(), dtr, rts, brk));
            false
        }
        Cmd::GetSignals {
            session: owner,
            reply,
        } => {
            if *session != owner {
                let _ = reply.send(Err("串口已由其他网页接管".into()));
                return false;
            }
            let Some(opened) = port.as_mut() else {
                let _ = reply.send(Ok(SignalState {
                    cts: None,
                    dsr: None,
                    dcd: None,
                    ri: None,
                }));
                return false;
            };
            let _ = reply.send(Ok(read_signals(opened.as_mut())));
            false
        }
    }
}

fn close_port_handle(port: &mut Option<Box<dyn SerialPort>>, open_path: &Mutex<Option<String>>) {
    *port = None;
    *open_path.lock().unwrap() = None;
}

fn describe_open(path: &str, extra: &[String]) -> PortDto {
    list_ports(extra)
        .into_iter()
        .find(|item| item.path == path || item.device == path)
        .unwrap_or(PortDto {
            path: path.to_string(),
            device: path.to_string(),
            vid: None,
            pid: None,
            product: None,
            manufacturer: None,
        })
}

fn flush_pending(
    slot: &Mutex<Option<EventSlot>>,
    session: u64,
    pending: &mut VecDeque<HubEvent>,
) -> bool {
    while let Some(event) = pending.pop_front() {
        if let Some(event) = try_emit(slot, session, event) {
            pending.push_front(event);
            return false;
        }
    }
    true
}

fn try_emit(slot: &Mutex<Option<EventSlot>>, session: u64, event: HubEvent) -> Option<HubEvent> {
    let guard = slot.lock().unwrap();
    let Some(slot) = guard.as_ref() else {
        return None;
    };
    if slot.session != session {
        return None;
    }
    match slot.tx.try_send(event) {
        Ok(()) => None,
        Err(TrySendError::Full(event)) => Some(event),
        Err(TrySendError::Closed(_)) => None,
    }
}

enum ReadOutcome {
    Data(Vec<u8>),
    Idle,
    Lost(String),
}

fn read_available(port: &mut dyn SerialPort) -> ReadOutcome {
    let mut buf = [0u8; 8192];
    let first = match port.read(&mut buf) {
        Ok(0) => return ReadOutcome::Idle,
        Ok(size) => size,
        Err(err) if is_io_timeout(&err) => return ReadOutcome::Idle,
        Err(err) => return ReadOutcome::Lost(humanize_io(err)),
    };
    let mut total = first;
    let started = Instant::now();
    while total < buf.len() && started.elapsed() < Duration::from_millis(15) {
        match port.bytes_to_read() {
            Ok(0) | Err(_) => break,
            Ok(_) => match port.read(&mut buf[total..]) {
                Ok(0) => break,
                Ok(size) => total += size,
                Err(err) if is_io_timeout(&err) => break,
                Err(err) => return ReadOutcome::Lost(humanize_io(err)),
            },
        }
    }
    ReadOutcome::Data(buf[..total].to_vec())
}

fn write_all_port(port: &mut dyn SerialPort, data: &[u8]) -> Result<(), String> {
    let mut offset = 0;
    let mut stalls = 0;
    while offset < data.len() {
        match port.write(&data[offset..]) {
            Ok(0) => {
                stalls += 1;
                if stalls > 50 {
                    return Err("写入串口超时".into());
                }
            }
            Ok(size) => {
                offset += size;
                stalls = 0;
            }
            Err(err) if is_io_timeout(&err) => {
                stalls += 1;
                if stalls > 50 {
                    return Err("写入串口超时".into());
                }
            }
            Err(err) => return Err(humanize_io(err)),
        }
    }
    Ok(())
}

fn apply_signals(
    port: &mut dyn SerialPort,
    dtr: Option<bool>,
    rts: Option<bool>,
    brk: Option<bool>,
) -> Result<(), String> {
    if let Some(level) = dtr {
        port.write_data_terminal_ready(level).map_err(humanize)?;
    }
    if let Some(level) = rts {
        port.write_request_to_send(level).map_err(humanize)?;
    }
    if let Some(level) = brk {
        if level {
            port.set_break().map_err(humanize)?;
        } else {
            port.clear_break().map_err(humanize)?;
        }
    }
    Ok(())
}

fn read_signals(port: &mut dyn SerialPort) -> SignalState {
    SignalState {
        cts: port.read_clear_to_send().ok(),
        dsr: port.read_data_set_ready().ok(),
        dcd: port.read_carrier_detect().ok(),
        ri: port.read_ring_indicator().ok(),
    }
}

fn open_port(req: &OpenReq) -> Result<Box<dyn SerialPort>, String> {
    if req.path.trim().is_empty() {
        return Err("未指定串口路径".into());
    }
    if req.baud_rate == 0 {
        return Err("波特率无效".into());
    }
    let data_bits = match req.data_bits {
        7 => DataBits::Seven,
        _ => DataBits::Eight,
    };
    let stop_bits = match req.stop_bits {
        2 => StopBits::Two,
        _ => StopBits::One,
    };
    let parity = match req.parity.as_str() {
        "even" => Parity::Even,
        "odd" => Parity::Odd,
        _ => Parity::None,
    };
    let flow = match req.flow_control.as_str() {
        "hardware" => FlowControl::Hardware,
        _ => FlowControl::None,
    };
    serialport::new(&req.path, req.baud_rate)
        .data_bits(data_bits)
        .stop_bits(stop_bits)
        .parity(parity)
        .flow_control(flow)
        .timeout(Duration::from_millis(20))
        .open()
        .map_err(humanize)
}

fn is_io_timeout(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

fn humanize_io(err: std::io::Error) -> String {
    let raw = err.to_string();
    let lower = raw.to_lowercase();
    if lower.contains("permission denied") || lower.contains("access denied") {
        return format!("没有权限打开串口（{raw}）。Linux 上把当前用户加入 dialout 组后重新登录。");
    }
    if lower.contains("busy") || lower.contains("resource busy") {
        return format!("串口已被占用（{raw}）。");
    }
    if err.kind() == std::io::ErrorKind::NotFound || lower.contains("no such file") {
        return format!("串口不存在或已拔出（{raw}）。");
    }
    raw
}

fn humanize(err: serialport::Error) -> String {
    let raw = err.to_string();
    let lower = raw.to_lowercase();
    if lower.contains("permission denied") || lower.contains("access denied") {
        return format!("没有权限打开串口（{raw}）。Linux 上把当前用户加入 dialout 组后重新登录。");
    }
    if lower.contains("busy") || lower.contains("resource busy") {
        return format!("串口已被占用（{raw}）。");
    }
    if lower.contains("no such file") || matches!(err.kind(), serialport::ErrorKind::NoDevice) {
        return format!("串口不存在或已拔出（{raw}）。");
    }
    raw
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn open_pty() -> (i32, String) {
        let mut master = -1;
        let mut slave = -1;
        let rc = unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(rc, 0, "openpty 失败");
        let mut name = [0u8; 128];
        let name_rc =
            unsafe { libc::ttyname_r(slave, name.as_mut_ptr() as *mut libc::c_char, name.len()) };
        assert_eq!(name_rc, 0, "ttyname_r 失败");
        unsafe { libc::close(slave) };
        let end = name
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(name.len());
        let slave_name = String::from_utf8_lossy(&name[..end]).into_owned();
        (master, slave_name)
    }

    fn make_raw(fd: i32) {
        unsafe {
            let mut termios: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(fd, &mut termios) == 0 {
                libc::cfmakeraw(&mut termios);
                libc::tcsetattr(fd, libc::TCSANOW, &termios);
            }
        }
    }

    #[tokio::test]
    async fn pty_roundtrip() {
        let (master, slave_name) = open_pty();
        let hub = Hub::spawn(vec![slave_name.clone()]);
        hub.adopt(7).await.expect("adopt");
        hub.open(
            7,
            OpenReq {
                path: slave_name,
                baud_rate: 115200,
                data_bits: 8,
                stop_bits: 1,
                parity: "none".into(),
                flow_control: "none".into(),
            },
        )
        .await
        .expect("open pty");
        make_raw(master);

        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        hub.set_event_sender(7, tx);
        let payload = b"hi\n";
        let written = unsafe {
            libc::write(
                master,
                payload.as_ptr() as *const libc::c_void,
                payload.len(),
            )
        };
        assert!(written > 0, "写入伪终端失败");

        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("等待接收超时")
            .expect("事件通道已关闭");
        match event {
            HubEvent::Rx { data, .. } => {
                assert!(
                    data.windows(2).any(|item| item == b"hi")
                        || data.windows(3).any(|item| item == b"hi\n"),
                    "收到的数据不是 hi: {data:?}"
                );
            }
            other => panic!("期望收到数据，实际是 {other:?}"),
        }

        hub.write(7, b"pong".to_vec()).await.expect("write");
        let flags = unsafe { libc::fcntl(master, libc::F_GETFL) };
        unsafe { libc::fcntl(master, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        let mut got = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && !got.windows(4).any(|item| item == b"pong") {
            let mut buf = [0u8; 64];
            let size =
                unsafe { libc::read(master, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if size > 0 {
                got.extend_from_slice(&buf[..size as usize]);
            } else {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        assert!(
            got.windows(4).any(|item| item == b"pong"),
            "主端没有读到 pong: {got:?}"
        );
        hub.clear_event_sender(7);
        hub.end_session(7).await.expect("end");
        unsafe { libc::close(master) };
        drop(hub);
    }
}
