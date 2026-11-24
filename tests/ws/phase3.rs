use confluxion_ws_native::{
    protocol::{Admission, Outbound, Rejection},
    worker,
};
use serde_json::{json, Value};
use std::{
    env, fs,
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream},
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tungstenite::{Message, WebSocket};

type Result<T> = std::result::Result<T, String>;

fn need(value: bool, reason: &str) -> Result<()> {
    if value {
        Ok(())
    } else {
        Err(reason.to_owned())
    }
}
fn io<T>(value: std::io::Result<T>) -> Result<T> {
    value.map_err(|e| e.to_string())
}
fn save(path: &Path, value: &Value) -> Result<()> {
    io(fs::write(
        path,
        serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?,
    ))
}
fn load(path: &Path) -> Result<Value> {
    serde_json::from_slice(&io(fs::read(path))?).map_err(|e| format!("{}: {e}", path.display()))
}
fn integer(v: &Value, key: &str) -> Result<usize> {
    v[key]
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| format!("missing/invalid {key}"))
}
fn ms(v: &Value, key: &str) -> Result<Duration> {
    Ok(Duration::from_millis(integer(v, key)? as u64))
}
fn address(port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)
}
fn listener() -> Result<(TcpListener, SocketAddrV4)> {
    let l = io(TcpListener::bind(address(0)))?;
    let port = io(l.local_addr())?.port();
    io(l.set_nonblocking(true))?;
    Ok((l, address(port)))
}
fn accept(l: &TcpListener, deadline: Instant) -> Result<TcpStream> {
    loop {
        match l.accept() {
            Ok((s, _)) => {
                io(s.set_read_timeout(Some(Duration::from_secs(2))))?;
                io(s.set_write_timeout(Some(Duration::from_secs(2))))?;
                io(s.set_nodelay(true))?;
                return Ok(s);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                thread::sleep(Duration::from_micros(100))
            }
            Err(e) => return Err(format!("peer accept: {e}")),
        }
    }
}
fn payload(sequence: u64, direction: u8) -> [u8; 256] {
    let mut p = [0; 256];
    p[0] = direction;
    p[1..9].copy_from_slice(&sequence.to_be_bytes());
    for (i, b) in p.iter_mut().enumerate().skip(9) {
        *b = (sequence
            .wrapping_mul(17)
            .wrapping_add(i as u64 * 31)
            .wrapping_add(direction as u64 * 7)
            & 255) as u8;
    }
    p
}
fn retry(e: &tungstenite::Error) -> bool {
    matches!(e, tungstenite::Error::Io(e) if e.kind() == io::ErrorKind::WouldBlock)
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        unsafe {
            libc::kill(-(self.0.id() as i32), libc::SIGKILL);
        }
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Process {
    fn spawn(mut command: Command, directory: &Path, label: &str) -> Result<Self> {
        save(
            &directory.join(format!("{label}-command.json")),
            &json!({"program":command.get_program().to_string_lossy(),"args":command.get_args().map(|a|a.to_string_lossy().into_owned()).collect::<Vec<_>>(),"environment":command.get_envs().map(|(k,v)|(k.to_string_lossy().into_owned(),v.map(|v|v.to_string_lossy().into_owned()))).collect::<Vec<_>>()}),
        )?;
        command
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(io(fs::File::create(
                directory.join(format!("{label}.stdout.log")),
            ))?)
            .stderr(io(fs::File::create(
                directory.join(format!("{label}.stderr.log")),
            ))?);
        Ok(Self(io(command.spawn())?))
    }
    fn finish(&mut self, deadline: Instant) -> Result<()> {
        loop {
            if let Some(s) = io(self.0.try_wait())? {
                return need(s.success(), &format!("peer/build exit {s}"));
            }
            need(Instant::now() < deadline, "child process deadline")?;
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn listening(&mut self, path: &Path, deadline: Instant) -> Result<()> {
        loop {
            if fs::read_to_string(path)
                .unwrap_or_default()
                .contains("LISTENING")
            {
                return Ok(());
            }
            need(
                io(self.0.try_wait())?.is_none(),
                "peer exited before listening",
            )?;
            need(Instant::now() < deadline, "peer startup deadline")?;
            thread::sleep(Duration::from_millis(1));
        }
    }
}

struct Gate {
    root: PathBuf,
    output: PathBuf,
    profile: Value,
    rows: Vec<Value>,
    scope: &'static str,
}
impl Gate {
    fn new(scope: &'static str) -> Result<Self> {
        let root = io(Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .canonicalize())?;
        let profile = load(&root.join("bench/ws/profiles/local-v1.json"))?;
        let run = format!(
            "cargo-{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_nanos(),
            std::process::id()
        );
        let output = root.join("results/ws").join(run).join("phase-3");
        io(fs::create_dir_all(&output))?;
        save(&output.join("profile.json"), &profile)?;
        Ok(Self {
            root,
            output,
            profile,
            rows: Vec::new(),
            scope,
        })
    }
    fn config(&self) -> Result<worker::Config> {
        let p = &self.profile["phase3"];
        need(
            integer(p, "incoming_queue_bytes")? == integer(p, "outgoing_queue_bytes")?
                && integer(p, "incoming_queue_descriptors")?
                    == integer(p, "outgoing_queue_descriptors")?,
            "worker requires symmetric queues",
        )?;
        need(
            ms(p, "incoming_partial_timeout_ms")? == ms(p, "outgoing_partial_timeout_ms")?,
            "worker requires matching independent deadline durations",
        )?;
        Ok(worker::Config {
            connect_timeout: ms(p, "connect_timeout_ms")?,
            upgrade_timeout: ms(p, "upgrade_timeout_ms")?,
            close_timeout: ms(p, "close_timeout_ms")?,
            partial_timeout: ms(p, "incoming_partial_timeout_ms")?,
            max_frame: integer(p, "max_frame_bytes")?,
            max_message: integer(p, "max_message_bytes")?,
            queue_bytes: integer(p, "incoming_queue_bytes")?,
            queue_descriptors: integer(p, "incoming_queue_descriptors")?,
            ..worker::Config::default()
        })
    }
    fn record(&mut self, id: &str, result: Result<Value>) -> Result<()> {
        let (row, failure) = match result {
            Ok(v) => (json!({"id":id,"pass":true,"evidence":v}), None),
            Err(e) => (json!({"id":id,"pass":false,"failure":e}), Some(e)),
        };
        self.rows.push(row);
        self.summary(false)?;
        if let Some(e) = failure {
            Err(format!("{id}: {e}; evidence {}", self.output.display()))
        } else {
            Ok(())
        }
    }
    fn summary(&self, complete: bool) -> Result<()> {
        save(
            &self.output.join("summary.json"),
            &json!({"schema":1,"phase":3,"scope":self.scope,"complete":complete,"pass":complete && self.rows.iter().all(|r|r["pass"] == true),"profile":"profile.json","checks":self.rows}),
        )
    }
    fn build_peer(&self) -> Result<PathBuf> {
        let build = self.output.join("build");
        io(fs::create_dir_all(&build))?;
        let cache = env::var_os("WS_CACHE")
            .map(PathBuf::from)
            .unwrap_or_else(|| self.root.join(".ws-cache"));
        let clang = env::var_os("CC")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/usr/lib/llvm-19/bin/clang"));
        let flags = env::var("CXXFLAGS")
            .unwrap_or_else(|_| "--gcc-install-dir=/usr/lib/gcc/x86_64-linux-gnu/13".into());
        let mut sources = Vec::new();
        for dir in ["src", "src/eventing", "src/crypto", "src/io_uring"] {
            for item in io(fs::read_dir(cache.join("uSockets").join(dir)))? {
                let p = io(item)?.path();
                if p.extension().is_some_and(|x| x == "c") {
                    sources.push(p);
                }
            }
        }
        sources.sort();
        need(!sources.is_empty(), "missing prepared uSockets sources")?;
        sources.push(self.root.join("bench/ws/phase3-peer.cpp"));
        let mut objects = Vec::new();
        for (i, source) in sources.iter().enumerate() {
            let object = build.join(format!("peer-{i}.o"));
            let mut c = Command::new(&clang);
            c.args(["-O3", "-g", "-DLIBUS_NO_SSL", "-DUWS_NO_ZLIB", "-pthread"])
                .arg(format!("-I{}", cache.join("uSockets/src").display()))
                .arg(format!("-I{}", cache.join("uWebSockets/src").display()));
            if source.extension().is_some_and(|x| x == "cpp") {
                c.args(["-x", "c++", "-std=c++20"])
                    .args(flags.split_whitespace());
            } else {
                c.arg("-std=c11");
            }
            c.arg("-c").arg(source).arg("-o").arg(&object);
            Process::spawn(c, &build, &format!("compile-{i}"))?
                .finish(Instant::now() + Duration::from_secs(120))?;
            objects.push(object);
        }
        let binary = build.join("phase3-peer");
        let mut c = Command::new(clang);
        c.args(flags.split_whitespace())
            .args(objects)
            .args(["-lstdc++", "-pthread"])
            .arg("-o")
            .arg(&binary);
        Process::spawn(c, &build, "link")?.finish(Instant::now() + Duration::from_secs(120))?;
        Ok(binary)
    }
}

fn offer(client: &worker::Client, bytes: &[u8], deadline: Instant) -> Result<()> {
    loop {
        match client
            .offer(Outbound::Binary(bytes))
            .map_err(|e| format!("offer {e:?}"))?
        {
            Admission::Accepted => return Ok(()),
            Admission::Rejected(Rejection::Busy) => {}
            other => return Err(format!("unexpected admission {other:?}")),
        }
        need(Instant::now() < deadline, "admission deadline")?;
        thread::yield_now();
    }
}
fn receive(client: &worker::Client, deadline: Instant) -> Result<worker::Received> {
    loop {
        if let Some(v) = client.receive().map_err(|e| format!("receive {e:?}"))? {
            return Ok(v);
        }
        need(Instant::now() < deadline, "receive deadline")?;
        thread::yield_now();
    }
}
fn duplex(client: worker::Client, count: usize) -> Result<Value> {
    let client = Arc::new(client);
    let sender = Arc::clone(&client);
    let deadline = Instant::now() + Duration::from_secs(180);
    let (tx, rx) = mpsc::sync_channel(1);
    let producer = thread::spawn(move || {
        let r: Result<()> = (|| {
            for n in 0..count {
                offer(&sender, &payload(n as u64, 0), deadline)?;
            }
            Ok(())
        })();
        let _ = tx.send(r);
    });
    let r = (|| {
        for n in 0..count {
            let msg = receive(&client, deadline)?;
            need(
                matches!(msg.kind, worker::Kind::Binary) && msg.bytes == payload(n as u64, 1),
                &format!("incoming identity/order/payload at {n}"),
            )?;
        }
        rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .map_err(|e| e.to_string())??;
        while client.status().tx_messages < count as u64 {
            need(
                Instant::now() < deadline && !client.status().finished,
                "million admitted messages did not reach wire completion",
            )?;
            thread::yield_now();
        }
        Ok(
            json!({"outgoing_admitted":count,"incoming_validated":count,"payload_bytes":256,"independent_producer":true}),
        )
    })();
    client.close();
    producer.join().map_err(|_| "producer panic".to_owned())?;
    r
}
fn rust_duplex_peer(l: TcpListener, count: usize) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut ws = tungstenite::accept(accept(&l, deadline)?).map_err(|e| e.to_string())?;
    io(ws.get_mut().set_nonblocking(true))?;
    let mut sent = 0;
    let mut validated = 0;
    let mut pending = false;
    while sent < count || validated < count || pending {
        need(Instant::now() < deadline, "Tungstenite duplex deadline")?;
        if pending {
            match ws.flush() {
                Ok(()) => pending = false,
                Err(e) if retry(&e) => {}
                Err(e) => return Err(e.to_string()),
            }
        }
        for _ in 0..256 {
            if sent == count || pending {
                break;
            }
            match ws.write(Message::Binary(payload(sent as u64, 1).to_vec().into())) {
                Ok(()) => {
                    sent += 1;
                    pending = true;
                }
                Err(e) if retry(&e) => {
                    sent += 1;
                    pending = true;
                }
                Err(e) => return Err(e.to_string()),
            }
            match ws.flush() {
                Ok(()) => pending = false,
                Err(e) if retry(&e) => {}
                Err(e) => return Err(e.to_string()),
            }
        }
        for _ in 0..256 {
            if validated == count {
                break;
            }
            match ws.read() {
                Ok(Message::Binary(v)) => {
                    need(
                        v.as_ref() == payload(validated as u64, 0),
                        "Tungstenite peer identity/order/payload",
                    )?;
                    validated += 1;
                }
                Err(e) if retry(&e) => break,
                other => return Err(format!("Tungstenite peer read {other:?}")),
            }
        }
        thread::yield_now();
    }
    Ok(json!({"sent":sent,"validated":validated,"failure":null}))
}

fn real_peers(gate: &Gate, binary: &Path) -> Result<Value> {
    let count = integer(&gate.profile["phase3"], "validated_messages_per_direction")?;
    need(count >= 1_000_000, "frozen million-message gate")?;
    let (l, addr) = listener()?;
    let (tx, rx) = mpsc::sync_channel(1);
    let peer = thread::spawn(move || {
        let _ = tx.send(rust_duplex_peer(l, count));
    });
    let result = worker::Client::connect(addr, "/", None, gate.config()?)
        .map_err(|e| format!("connect {e:?}"))
        .and_then(|c| duplex(c, count));
    let peer_result = rx
        .recv_timeout(Duration::from_secs(181))
        .map_err(|e| e.to_string())?;
    peer.join()
        .map_err(|_| "Tungstenite peer panic".to_owned())?;
    let rust = json!({"client":result?,"peer":peer_result?});
    save(&gate.output.join("tungstenite-duplex.json"), &rust)?;
    let (l, addr) = listener()?;
    drop(l);
    let report = gate.output.join("uwebsockets-peer.json");
    let mut command = Command::new(binary);
    command
        .arg(addr.port().to_string())
        .arg(count.to_string())
        .arg(&report);
    let mut peer = Process::spawn(command, &gate.output, "uwebsockets")?;
    peer.listening(
        &gate.output.join("uwebsockets.stdout.log"),
        Instant::now() + Duration::from_secs(5),
    )?;
    let client = worker::Client::connect(addr, "/", None, gate.config()?)
        .map_err(|e| format!("uWS connect {e:?}"))?;
    let native = duplex(client, count)?;
    peer.finish(Instant::now() + Duration::from_secs(2))?;
    let independent = load(&report)?;
    need(
        integer(&independent, "sent")? == count
            && integer(&independent, "validated")? == count
            && independent["failure"].is_null(),
        "uWS complete independent evidence",
    )?;
    Ok(json!({"tungstenite":rust,"uwebsockets":{"client":native,"peer":independent}}))
}

fn snapshot_json(s: &worker::Snapshot) -> Value {
    json!({"connected":s.connected,"finished":s.finished,"joined":s.joined,"terminal":format!("{:?}",s.terminal),
        "tx_admitted":s.tx_admitted,"tx_messages":s.tx_messages,"rx_messages":s.rx_messages,"tx_bytes":s.tx_bytes,"rx_bytes":s.rx_bytes,
        "tx_queue_bytes":s.tx_queue_bytes,"rx_queue_bytes":s.rx_queue_bytes,"tx_queue_descriptors":s.tx_queue_descriptors,"rx_queue_descriptors":s.rx_queue_descriptors,
        "tx_queue_bytes_peak":s.tx_queue_bytes_peak,"rx_queue_bytes_peak":s.rx_queue_bytes_peak,"tx_queue_descriptors_peak":s.tx_queue_descriptors_peak,"rx_queue_descriptors_peak":s.rx_queue_descriptors_peak,
        "native_memory_budget":s.native_memory_budget,"native_memory_peak":s.native_memory_peak,"native_memory_live":s.native_memory_live,"native_memory_idle":s.native_memory_idle,"allocation_invalid":s.allocation_invalid,
        "worker_stack_bytes":s.worker_stack_bytes,"turns":s.turns,"service_gap_peak_ns":s.service_gap_peak.as_nanos(),
        "read_service_gap_peak_ns":s.read_service_gap_peak.as_nanos(),"write_service_gap_peak_ns":s.write_service_gap_peak.as_nanos(),"control_service_gap_peak_ns":s.control_service_gap_peak.as_nanos(),
        "incoming_partial_age_ns":s.incoming_partial_age.map(|v|v.as_nanos()),"outgoing_partial_age_ns":s.outgoing_partial_age.map(|v|v.as_nanos()),"close_elapsed_ns":s.close_elapsed.as_nanos()})
}
fn memory(s: &worker::Snapshot, c: &worker::Config) -> Result<()> {
    need(
        !s.allocation_invalid
            && s.native_memory_peak > 0
            && s.native_memory_idle > 0
            && s.native_memory_peak <= s.native_memory_budget
            && s.native_memory_live <= s.native_memory_budget,
        "actual native allocation ceiling/accounting",
    )?;
    need(
        s.tx_queue_bytes_peak <= c.queue_bytes
            && s.rx_queue_bytes_peak <= c.queue_bytes
            && s.tx_queue_descriptors_peak <= c.queue_descriptors
            && s.rx_queue_descriptors_peak <= c.queue_descriptors,
        "queue admission ceilings",
    )
}
fn raw_pair(config: worker::Config) -> Result<(worker::Client, TcpStream)> {
    let (l, addr) = listener()?;
    let (tx, rx) = mpsc::sync_channel(1);
    let peer = thread::spawn(move || {
        let result = (|| {
            let ws = tungstenite::accept(accept(&l, Instant::now() + Duration::from_secs(3))?)
                .map_err(|e| e.to_string())?;
            io(ws.get_ref().try_clone())
        })();
        let _ = tx.send(result);
    });
    let client = worker::Client::connect(addr, "/", None, config)
        .map_err(|e| format!("fixture connect {e:?}"));
    let socket = rx
        .recv_timeout(Duration::from_secs(4))
        .map_err(|e| e.to_string())?;
    peer.join().map_err(|_| "raw peer panic".to_owned())?;
    Ok((client?, socket?))
}
fn server_frame(op: u8, bytes: &[u8]) -> Vec<u8> {
    let mut v = vec![128 | op];
    if bytes.len() < 126 {
        v.push(bytes.len() as u8);
    } else if bytes.len() <= 65535 {
        v.push(126);
        v.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    } else {
        v.push(127);
        v.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    }
    v.extend_from_slice(bytes);
    v
}
fn idle(
    client: &worker::Client,
    config: &worker::Config,
    deadline: Instant,
) -> Result<worker::Snapshot> {
    loop {
        let s = client.status();
        memory(&s, config)?;
        need(!s.finished, "unexpected terminal while draining")?;
        if s.tx_queue_descriptors == 0
            && s.rx_queue_descriptors == 0
            && s.outgoing_partial_age.is_none()
            && s.incoming_partial_age.is_none()
            && s.native_memory_live == s.native_memory_idle
        {
            return Ok(s);
        }
        need(
            Instant::now() < deadline,
            "established idle allocation restoration deadline",
        )?;
        thread::yield_now();
    }
}
fn service(gate: &Gate) -> Result<Value> {
    let budget = ms(&gate.profile["phase3"], "service_budget_ms")?;
    let (client, mut stream) = raw_pair(gate.config()?)?;
    io(stream.set_read_timeout(Some(Duration::from_secs(1))))?;
    let mut rows = Vec::new();
    for n in 0..100 {
        let bytes = payload(n, 1);
        let ping = n.to_be_bytes();
        let at = Instant::now();
        io(stream.write_all(&[server_frame(2, &bytes), server_frame(9, &ping)].concat()))?;
        offer(&client, &payload(n, 0), at + budget)?;
        let msg = receive(&client, at + budget)?;
        need(
            msg.kind == worker::Kind::Binary && msg.bytes == bytes,
            "eligible readable payload",
        )?;
        let read_elapsed = at.elapsed();
        let mut peer = WebSocket::from_raw_socket(
            io(stream.try_clone())?,
            tungstenite::protocol::Role::Server,
            None,
        );
        let mut pong = false;
        let mut data = false;
        let mut write_elapsed = Duration::ZERO;
        let mut control_elapsed = Duration::ZERO;
        while !pong || !data {
            match peer.read().map_err(|e| e.to_string())? {
                Message::Pong(v) => {
                    need(v.as_ref() == ping, "eligible Pong payload")?;
                    pong = true;
                    control_elapsed = at.elapsed();
                }
                Message::Binary(v) => {
                    need(v.as_ref() == payload(n, 0), "eligible writable payload")?;
                    data = true;
                    write_elapsed = at.elapsed();
                }
                other => return Err(format!("eligible peer message {other:?}")),
            }
        }
        need(
            read_elapsed <= budget && write_elapsed <= budget && control_elapsed <= budget,
            "eligible read/write/control exceeded 10ms wire-observed service budget",
        )?;
        rows.push(json!({"read_ns":read_elapsed.as_nanos(),"write_ns":write_elapsed.as_nanos(),"pong_ns":control_elapsed.as_nanos()}));
    }
    let s = client.close();
    need(s.joined, "service worker bounded join")?;
    Ok(json!({"samples":rows,"snapshot":snapshot_json(&s)}))
}

fn pause_payload(n: usize) -> Vec<u8> {
    let small = payload(n as u64, 0);
    small.into_iter().cycle().take(1024).collect()
}
fn overload(gate: &Gate) -> Result<Value> {
    let mut config = gate.config()?;
    config.socket_buffer_bytes = Some(4096);
    let (client, stream) = raw_pair(config.clone())?;
    io(stream.set_nonblocking(true))?;
    let mut peer = WebSocket::from_raw_socket(
        stream,
        tungstenite::protocol::Role::Server,
        Some(
            tungstenite::protocol::WebSocketConfig::default()
                .write_buffer_size(0)
                .max_write_buffer_size(config.max_frame + 1024),
        ),
    );
    let warmup_deadline = Instant::now() + Duration::from_secs(2);
    let warmup = vec![0x5a; config.max_message];
    offer(&client, &warmup, warmup_deadline)?;
    loop {
        match peer.read() {
            Ok(Message::Binary(v)) => {
                need(v.as_ref() == warmup, "outgoing idle warmup bytes")?;
                break;
            }
            Err(e) if retry(&e) => thread::yield_now(),
            other => return Err(format!("idle warmup {other:?}")),
        }
        need(
            Instant::now() < warmup_deadline,
            "outgoing idle warmup deadline",
        )?;
    }
    match peer.send(Message::Binary(warmup.clone().into())) {
        Ok(()) => (),
        Err(e) if retry(&e) => (),
        Err(e) => return Err(e.to_string()),
    }
    loop {
        match peer.flush() {
            Ok(()) => (),
            Err(e) if retry(&e) => (),
            Err(e) => return Err(e.to_string()),
        }
        if let Some(v) = client.receive().map_err(|e| e.to_string())? {
            need(v.bytes == warmup, "incoming idle warmup bytes")?;
            break;
        }
        need(
            Instant::now() < warmup_deadline,
            "incoming idle warmup deadline",
        )?;
        thread::yield_now();
    }
    let initial = idle(&client, &config, Instant::now() + Duration::from_secs(2))?;
    let cycles = integer(&gate.profile["phase3"], "pause_fill_drain_cycles")?;
    need(cycles == 100, "frozen 100-cycle gate")?;
    let mut rows = Vec::new();
    for cycle in 0..cycles {
        let mut admitted = 0;
        let mut busy = false;
        let fill = Instant::now() + Duration::from_millis(150);
        while Instant::now() < fill {
            match client.offer(Outbound::Binary(&pause_payload(admitted))) {
                Ok(Admission::Accepted) => admitted += 1,
                Ok(Admission::Rejected(Rejection::Busy)) => {
                    busy = true;
                    break;
                }
                other => return Err(format!("outgoing fill {other:?}")),
            }
        }
        need(
            busy && admitted > 0,
            "outgoing pause did not force bounded rejection",
        )?;
        peer.send(Message::Binary(payload(cycle as u64, 1).to_vec().into()))
            .map_err(|e| e.to_string())?;
        let opposite = receive(&client, Instant::now() + Duration::from_millis(10))?;
        need(
            opposite.bytes == payload(cycle as u64, 1),
            "incoming direction starved by outgoing saturation",
        )?;
        thread::sleep(Duration::from_millis(10));
        let paused = client.status();
        memory(&paused, &config)?;
        let drain_at = Instant::now();
        let deadline = drain_at + Duration::from_secs(2);
        let mut validated = 0;
        while validated < admitted {
            match peer.read() {
                Ok(Message::Binary(v)) => {
                    need(
                        v.as_ref() == pause_payload(validated),
                        "outgoing drain identity/order/payload",
                    )?;
                    validated += 1;
                }
                Err(e) if retry(&e) => thread::yield_now(),
                other => return Err(format!("outgoing drain {other:?}")),
            }
            need(Instant::now() < deadline, "outgoing drain exceeded 2sec")?;
        }
        let outgoing_idle = idle(&client, &config, deadline)?;
        need(
            outgoing_idle.native_memory_live == initial.native_memory_idle,
            "outgoing cycle changed established idle allocation state",
        )?;
        let outgoing_ns = drain_at.elapsed().as_nanos();

        let mut sent = 0;
        let mut pending = false;
        let fill = Instant::now() + Duration::from_secs(2);
        while client.status().rx_queue_bytes <= config.queue_bytes - config.max_message {
            need(
                Instant::now() < fill,
                "incoming pause did not fill native receive queue",
            )?;
            if pending {
                match peer.flush() {
                    Ok(()) => pending = false,
                    Err(e) if retry(&e) => {}
                    Err(e) => return Err(e.to_string()),
                }
            }
            if !pending {
                match peer.write(Message::Binary(pause_payload(sent).into())) {
                    Ok(()) => {
                        sent += 1;
                        pending = true;
                    }
                    Err(e) if retry(&e) => {
                        sent += 1;
                        pending = true;
                    }
                    Err(e) => return Err(e.to_string()),
                }
            }
            thread::yield_now();
        }
        let saturated = client.status();
        memory(&saturated, &config)?;
        offer(
            &client,
            &payload(cycle as u64, 1),
            Instant::now() + Duration::from_millis(10),
        )?;
        let opposite_at = Instant::now();
        loop {
            match peer.read() {
                Ok(Message::Binary(v)) => {
                    need(
                        v.as_ref() == payload(cycle as u64, 1),
                        "outgoing direction starved by incoming saturation",
                    )?;
                    break;
                }
                Err(e) if retry(&e) => {}
                other => return Err(format!("incoming saturation opposite read {other:?}")),
            }
            need(
                opposite_at.elapsed() <= Duration::from_millis(10),
                "saturated receive starved eligible outgoing direction",
            )?;
        }
        thread::sleep(Duration::from_millis(10));
        let drain_at = Instant::now();
        let deadline = drain_at + Duration::from_secs(2);
        let mut validated = 0;
        while validated < sent || pending {
            if pending {
                match peer.flush() {
                    Ok(()) => pending = false,
                    Err(e) if retry(&e) => {}
                    Err(e) => return Err(e.to_string()),
                }
            }
            match client
                .receive()
                .map_err(|e| format!("incoming drain {e:?}"))?
            {
                Some(v) => {
                    need(
                        v.kind == worker::Kind::Binary && v.bytes == pause_payload(validated),
                        "incoming drain identity/order/payload",
                    )?;
                    validated += 1;
                }
                None => thread::yield_now(),
            }
            need(Instant::now() < deadline, "incoming drain exceeded 2sec")?;
        }
        let restored = idle(&client, &config, deadline)?;
        need(
            restored.native_memory_live == initial.native_memory_idle,
            "incoming cycle changed established idle allocation state",
        )?;
        rows.push(json!({"cycle":cycle,"outgoing_admitted":admitted,"outgoing_validated":admitted,"incoming_sent":sent,"incoming_validated":validated,"outgoing_drain_ns":outgoing_ns,"incoming_drain_ns":drain_at.elapsed().as_nanos(),"outgoing_paused":snapshot_json(&paused),"incoming_saturated":snapshot_json(&saturated),"restored":snapshot_json(&restored)}));
        save(
            &gate.output.join("pause-fill-drain.json"),
            &json!({"complete_cycles":rows.len(),"initial":snapshot_json(&initial),"cycles":rows}),
        )?;
    }
    let s = client.close();
    need(s.joined, "overload bounded join")?;
    Ok(json!({"cycles":cycles,"evidence":"pause-fill-drain.json","final":snapshot_json(&s)}))
}

fn partial_deadlines(gate: &Gate) -> Result<Value> {
    let mut config = gate.config()?;
    config.socket_buffer_bytes = Some(4096);
    let budget = ms(&gate.profile["phase3"], "service_budget_ms")?;
    let mut rows = Vec::new();
    for outgoing in [false, true] {
        let (client, mut stream) = raw_pair(config.clone())?;
        if outgoing {
            use std::os::fd::AsRawFd;
            let bytes = 4096i32;
            need(
                unsafe {
                    libc::setsockopt(
                        stream.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_RCVBUF,
                        &bytes as *const _ as *const _,
                        std::mem::size_of_val(&bytes) as _,
                    )
                } == 0,
                "peer small socket receive buffer",
            )?;
            offer(
                &client,
                &vec![0x5a; config.max_frame],
                Instant::now() + Duration::from_secs(1),
            )?;
            let mut prefix = [0u8; 14];
            io(stream.read_exact(&mut prefix))?;
            need(
                prefix[0] == 0x82
                    && prefix[1] == 0xff
                    && u64::from_be_bytes(prefix[2..10].try_into().unwrap())
                        == config.max_frame as u64,
                "outgoing partial real masked 1MiB frame prefix",
            )?;
        } else {
            io(stream.write_all(&[0x82, 126, 0, 126, 0x5a]))?;
        }
        io(stream.set_nonblocking(true))?;
        let mut reader = if outgoing {
            None
        } else {
            Some(WebSocket::from_raw_socket(
                io(stream.try_clone())?,
                tungstenite::protocol::Role::Server,
                None,
            ))
        };
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut next_traffic = Instant::now();
        let mut sent = 0usize;
        let mut validated = 0usize;
        let mut max_age = Duration::ZERO;
        let mut last_anchor: Option<Instant> = None;
        let mut previous_age = None;
        loop {
            let now = Instant::now();
            let s = client.status();
            let age = if outgoing {
                s.outgoing_partial_age
            } else {
                s.incoming_partial_age
            };
            if let Some(age) = age {
                max_age = max_age.max(age);
                let observed = now.checked_sub(age).ok_or("invalid native partial age")?;
                last_anchor = Some(if previous_age.is_some_and(|old| age < old) {
                    observed
                } else {
                    last_anchor.map_or(observed, |old| old.min(observed))
                });
                previous_age = Some(age);
            }
            if s.finished {
                let expected = if outgoing {
                    worker::Terminal::OutgoingTimeout
                } else {
                    worker::Terminal::IncomingTimeout
                };
                need(
                    s.terminal == Some(expected),
                    "partial frame did not terminate with its own directional timeout",
                )?;
                let anchor = last_anchor.ok_or("missing real partial-progress age evidence")?;
                need(now.duration_since(anchor)<=config.partial_timeout+budget,"partial timeout exceeded its independent 250ms deadline plus 10ms service budget")?;
                need(
                    max_age >= config.partial_timeout.saturating_sub(budget) && validated >= 10,
                    "unrelated traffic did not span the stalled partial interval",
                )?;
                let closed = client.close();
                need(closed.joined, "partial deadline bounded join")?;
                rows.push(json!({"direction":if outgoing {"outgoing"} else {"incoming"},"unrelated_sent":sent,"unrelated_validated":validated,"maximum_observed_partial_age_ns":max_age.as_nanos(),"termination_from_last_progress_ns":now.duration_since(anchor).as_nanos(),"snapshot":snapshot_json(&closed)}));
                break;
            }
            need(now < deadline, "nonprogressing partial frame remained open")?;
            if now >= next_traffic {
                if outgoing {
                    io(stream.write_all(&server_frame(2, &payload(sent as u64, 1))))?;
                    sent += 1;
                } else {
                    match client
                        .offer(Outbound::Binary(&payload(sent as u64, 0)))
                        .map_err(|e| e.to_string())?
                    {
                        Admission::Accepted => sent += 1,
                        Admission::Rejected(Rejection::Busy) => {}
                        other => return Err(format!("partial unrelated admission {other:?}")),
                    }
                }
                next_traffic = now + Duration::from_millis(2);
            }
            if outgoing {
                if let Some(v) = client.receive().map_err(|e| e.to_string())? {
                    need(
                        v.bytes == payload(validated as u64, 1),
                        "outgoing partial unrelated incoming identity",
                    )?;
                    validated += 1;
                }
            } else {
                match reader.as_mut().unwrap().read() {
                    Ok(Message::Binary(v)) => {
                        need(
                            v.as_ref() == payload(validated as u64, 0),
                            "incoming partial unrelated outgoing identity",
                        )?;
                        validated += 1;
                    }
                    Err(e) if retry(&e) => {}
                    other => return Err(format!("incoming partial unrelated peer {other:?}")),
                }
            }
            thread::sleep(Duration::from_micros(100));
        }
    }
    Ok(json!({"directions":rows}))
}

fn negative_handshakes(gate: &Gate) -> Result<Value> {
    let cases = [
        "status",
        "accept",
        "upgrade",
        "connection",
        "extension",
        "unsolicited-protocol",
        "missing-protocol",
        "wrong-protocol",
        "duplicate-accept",
        "duplicate-upgrade",
        "header-limit",
        "malformed",
        "upgrade-timeout",
    ];
    let mut rows = Vec::new();
    for name in cases {
        let (l, addr) = listener()?;
        let (tx, rx) = mpsc::sync_channel(1);
        let peer = thread::spawn(move || {
            let r: Result<Value> = (|| {
                let mut socket = accept(&l, Instant::now() + Duration::from_secs(2))?;
                let mut header = Vec::new();
                let mut byte = [0];
                while !header.ends_with(b"\r\n\r\n") {
                    io(socket.read_exact(&mut byte))?;
                    header.push(byte[0]);
                    need(
                        header.len() <= 8192,
                        "negative fixture request header limit",
                    )?;
                }
                let request = String::from_utf8(header).map_err(|e| e.to_string())?;
                let key = request
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(k, _)| k.eq_ignore_ascii_case("sec-websocket-key"))
                            .map(|(_, v)| v.trim())
                    })
                    .ok_or("missing request nonce")?;
                let key = tungstenite::handshake::derive_accept_key(key.as_bytes());
                let mut status = "HTTP/1.1 101 Switching Protocols";
                let mut upgrade = "websocket";
                let mut connection = "Upgrade";
                let mut accept = key.clone();
                let mut extra = String::new();
                match name {
                    "status" => status = "HTTP/1.1 200 OK",
                    "accept" => accept = "invalid".into(),
                    "upgrade" => upgrade = "h2c",
                    "connection" => connection = "keep-alive",
                    "extension" => {
                        extra = "Sec-WebSocket-Extensions: permessage-deflate\r\n".into()
                    }
                    "unsolicited-protocol" | "wrong-protocol" => {
                        extra = "Sec-WebSocket-Protocol: wrong\r\n".into()
                    }
                    "duplicate-accept" => extra = format!("Sec-WebSocket-Accept: {key}\r\n"),
                    "duplicate-upgrade" => extra = "Upgrade: websocket\r\n".into(),
                    "header-limit" => extra = format!("X-Fill: {}\r\n", "x".repeat(8192)),
                    "malformed" => {
                        io(socket.write_all(b"garbage\r\n\r\n"))?;
                        return Ok(json!({"request":request,"response":"garbage"}));
                    }
                    "upgrade-timeout" => {
                        thread::sleep(Duration::from_millis(1100));
                        return Ok(json!({"request":request,"response":null}));
                    }
                    _ => {}
                }
                let response=format!("{status}\r\nUpgrade: {upgrade}\r\nConnection: {connection}\r\nSec-WebSocket-Accept: {accept}\r\n{extra}\r\n");
                let write = socket.write_all(response.as_bytes());
                if name != "header-limit" {
                    io(write)?;
                }
                Ok(json!({"request":request,"response":response}))
            })();
            let _ = tx.send(r);
        });
        let protocol = if matches!(name, "missing-protocol" | "wrong-protocol") {
            Some("required")
        } else {
            None
        };
        let at = Instant::now();
        let result = worker::Client::connect(addr, "/", protocol, gate.config()?);
        let elapsed = at.elapsed();
        let evidence = rx
            .recv_timeout(Duration::from_secs(3))
            .map_err(|e| e.to_string())?;
        peer.join().map_err(|_| "negative peer panic".to_owned())?;
        let error = match result {
            Err(e) => e,
            Ok(c) => {
                c.close();
                return Err(format!("negative upgrade {name} accepted"));
            }
        };
        if name == "upgrade-timeout" {
            need(
                error == worker::Error::Terminal(worker::Terminal::UpgradeTimeout),
                "upgrade deadline classified incorrectly",
            )?;
            need(
                elapsed
                    <= ms(&gate.profile["phase3"], "upgrade_timeout_ms")?
                        + ms(&gate.profile["phase3"], "service_budget_ms")?,
                "upgrade deadline exceeded profile service allowance",
            )?;
        }
        rows.push(json!({"id":name,"error":format!("{error:?}"),"elapsed_ns":elapsed.as_nanos(),"wire":evidence?}));
    }
    save(
        &gate.output.join("negative-handshakes.json"),
        &json!({"cases":rows}),
    )?;
    Ok(json!({"cases":cases.len(),"evidence":"negative-handshakes.json"}))
}

fn process_resources() -> Result<(u64, u64, usize, usize)> {
    let s = confluxion_ws_native::ws_native_stats();
    let fds = io(fs::read_dir("/proc/self/fd"))?.count();
    let threads = io(fs::read_dir("/proc/self/task"))?.count();
    Ok((s.live_bytes, s.live_blocks, fds, threads))
}

fn socket_lifecycle(gate: &Gate) -> Result<Value> {
    let (warm, stream) = raw_pair(gate.config()?)?;
    drop(stream);
    need(warm.close().joined, "warm socket worker join")?;
    drop(warm);
    let baseline = process_resources()?;
    for cycle in 0..100 {
        let (client, mut peer) = raw_pair(gate.config()?)?;
        io(peer.write_all(&server_frame(1, b"retained after close")))?;
        let deadline = Instant::now() + Duration::from_secs(1);
        while client.status().rx_messages != 1 {
            need(
                Instant::now() < deadline,
                "lifecycle native receipt deadline",
            )?;
            thread::yield_now();
        }
        drop(peer);
        let at = Instant::now();
        let closed = client.close();
        need(
            closed.joined && at.elapsed() <= Duration::from_secs(1),
            "socket shutdown join bound",
        )?;
        let received = client
            .receive()
            .map_err(|e| e.to_string())?
            .ok_or("close discarded received message")?;
        need(
            received.kind == worker::Kind::Text && received.bytes == b"retained after close",
            "close changed received payload",
        )?;
        need(
            client
                .offer(Outbound::Binary(b"late"))
                .map_err(|e| e.to_string())?
                == Admission::Rejected(Rejection::Closing),
            "terminal session admitted output",
        )?;
        need(client.close().joined, "repeated socket close")?;
        drop(received);
        drop(client);
        let restored = process_resources()?;
        need(restored == baseline, &format!("socket lifecycle {cycle} global allocations/fds/threads changed: {baseline:?} -> {restored:?}"))?;
    }
    Ok(
        json!({"cycles":100,"baseline_live_bytes":baseline.0,"baseline_live_blocks":baseline.1,"baseline_descriptors":baseline.2,"baseline_threads":baseline.3,"global_allocator_includes_thread_bookkeeping_and_tls":true,"post_close_receive":true}),
    )
}

#[test]
fn native_socket_correctness() {
    let mut gate =
        Gate::new("native-correctness").unwrap_or_else(|e| panic!("Phase 3 preparation: {e}"));
    gate.summary(false).unwrap();
    let run = (|| {
        let peer = gate.build_peer()?;
        let r = socket_lifecycle(&gate);
        gate.record("socket-lifecycle-global-resource-restoration", r)?;
        let r = real_peers(&gate, &peer);
        gate.record("independent-million-message-duplex", r)?;
        let r = negative_handshakes(&gate);
        gate.record("negative-real-upgrades", r)?;
        let r = service(&gate);
        gate.record("eligible-read-write-control-service", r)?;
        let r = partial_deadlines(&gate);
        gate.record("independent-partial-progress-deadlines", r)?;
        let r = overload(&gate);
        gate.record("100-pause-fill-drain-cycles", r)?;
        gate.summary(true)
    })();
    if let Err(e) = run {
        if !gate.rows.iter().any(|r| r["pass"] == false) {
            let _ = gate.record("preparation-or-evidence", Err(e.clone()));
        }
        panic!("Phase 3 failed: {e}; retained {}", gate.output.display());
    }
}
