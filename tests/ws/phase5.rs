use serde_json::{json, Value};
use std::{
    env, fs,
    io::{Read, Write},
    net::TcpListener,
    os::{fd::AsRawFd, unix::process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{atomic::{AtomicBool, Ordering}, Arc},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tungstenite::{protocol::WebSocketConfig, Message};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[path = "phase5_bounded.rs"]
mod bounded;
const NS: u64 = 1_000_000_000;
fn require(b: bool, why: &str) -> Result<()> {
    if b {
        Ok(())
    } else {
        Err(why.into())
    }
}
fn n(v: &Value, k: &str) -> Result<u64> {
    v[k].as_u64().ok_or_else(|| format!("missing {k}").into())
}
fn save(p: &Path, v: &Value) -> Result<()> {
    fs::write(p, serde_json::to_vec_pretty(v)?)?;
    Ok(())
}
fn now() -> u64 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t) },
        0
    );
    t.tv_sec as u64 * NS + t.tv_nsec as u64
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PathKind {
    Reference,
    Native,
    Bend,
}
#[derive(Clone, Copy, Debug)]
struct Rates {
    to_peer: u64,
    to_application: u64,
}
impl Rates {
    fn scaled(self, x: u64) -> Self {
        Self {
            to_peer: self.to_peer * x,
            to_application: self.to_application * x,
        }
    }
}
#[derive(Clone, Debug)]
enum Workload {
    W1 { bytes: u64, split: Rates },
    W2 { connections: usize, split: Rates },
    W4 { pause_ms: u64, receiver: usize },
    W5,
}
impl Workload {
    fn dimensions(&self) -> (u64, usize, Rates) {
        match *self {
            Self::W1 { bytes, split } => (bytes, 1, split),
            Self::W2 { connections, split } => (256, connections, split),
            Self::W4 { .. } | Self::W5 => (
                256,
                1,
                Rates {
                    to_peer: 1,
                    to_application: 1,
                },
            ),
        }
    }
}
#[derive(Clone)]
struct Profile {
    raw: Value,
    cpus: [usize; 2],
}
impl Profile {
    fn core(&self) -> Result<Vec<Workload>> {
        let mut cases = vec![];
        for bytes in self.raw["w1_bytes"].as_array().ok_or("w1 bytes")? {
            for split in self.raw["w1_directions"]
                .as_array()
                .ok_or("w1 directions")?
            {
                cases.push(Workload::W1 {
                    bytes: bytes.as_u64().ok_or("size")?,
                    split: rates(split)?,
                });
            }
        }
        for connections in self.raw["w2_connections"].as_array().ok_or("connections")? {
            for split in self.raw["w2_splits"].as_array().ok_or("splits")? {
                cases.push(Workload::W2 {
                    connections: connections.as_u64().ok_or("connection count")? as usize,
                    split: rates(split)?,
                });
            }
        }
        Ok(cases)
    }
}
fn rates(v: &Value) -> Result<Rates> {
    Ok(Rates {
        to_peer: v[0].as_u64().ok_or("rate")?,
        to_application: v[1].as_u64().ok_or("rate")?,
    })
}
#[derive(Clone, Debug)]
struct Trial {
    path: PathKind,
    workload: Workload,
    rates: Rates,
    warm: u64,
    measure: u64,
    token: u64,
    start: u64,
    tcp: bool,
    fault: bool,
}
struct Evidence {
    clients: Vec<Value>,
    peer: Value,
    statuses: Vec<i32>,
    directory: PathBuf,
}
enum Verdict {
    Passed,
    Failed(String),
    Inconclusive(String),
}
struct StreamEvidence<'a> {
    tx: &'a Value,
    rx: &'a Value,
    rate: u64,
    to_peer: bool,
}
fn row(v: &Value, id: usize, epoch: u64) -> Result<&Value> {
    let rows = v["streams"].as_array().ok_or("missing streams")?;
    let mut matches = rows
        .iter()
        .filter(|r| r["connection"] == id as u64 && r["epoch"] == epoch);
    let first = matches.next().ok_or("missing stream")?;
    require(matches.next().is_none(), "duplicate stream")?;
    Ok(first)
}
fn streams<'a>(t: &Trial, e: &'a Evidence, epoch: u64) -> Result<Vec<StreamEvidence<'a>>> {
    let mut out = vec![];
    let (_, connections, _) = t.workload.dimensions();
    for id in 0..connections {
        let c = if t.path == PathKind::Reference {
            &e.clients[0]
        } else {
            e.clients.get(id).ok_or("missing actor")?
        };
        let a = row(c, id, epoch)?;
        let b = row(&e.peer, id, epoch)?;
        out.push(StreamEvidence {
            tx: &a["tx"],
            rx: &b["rx"],
            rate: t.rates.to_peer,
            to_peer: true,
        });
        out.push(StreamEvidence {
            tx: &b["tx"],
            rx: &a["rx"],
            rate: t.rates.to_application,
            to_peer: false,
        });
    }
    Ok(out)
}
fn p99(flow: &Value, key: &str, count: u64) -> Result<u64> {
    let bins = flow[key].as_array().ok_or("histogram missing")?;
    require(bins.len() == 10001, "histogram layout")?;
    let values = bins
        .iter()
        .map(|v| v.as_u64().ok_or("histogram number"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    require(values.iter().sum::<u64>() == count, "histogram count")?;
    if count == 0 {
        return Ok(0);
    }
    let target = (count * 99 + 99) / 100;
    let mut sum = 0;
    for (i, v) in values.iter().enumerate() {
        sum += v;
        if sum >= target {
            return Ok(i as u64);
        }
    }
    Err("empty histogram".into())
}
fn correctness(t: &Trial, e: &Evidence, profile: &Profile, adverse: bool) -> Result<()> {
    let (_, connections, _) = t.workload.dimensions();
    require(
        e.clients.len()
            == if t.path == PathKind::Reference {
                1
            } else {
                connections
            },
        "actor count",
    )?;
    require(e.statuses.len() == e.clients.len() + 1, "exit count")?;
    for (index, v) in e.clients.iter().chain([&e.peer]).enumerate() {
        require(
            n(v, "schema")? == 1
                && n(v, "token")? == t.token
                && n(v, "start_ns")? == t.start
                && n(v, "end_ns")? == t.start + (t.warm + t.measure) * NS,
            "trial metadata",
        )?;
        require(
            n(v, "queue_limit_bytes")? == n(&profile.raw, "queue_bytes")?
                && n(v, "queue_limit_descriptors")? == 4096,
            "effective queue limits",
        )?;
        require(v["failure"].is_null() || adverse, "actor failure")?;
        require(e.statuses[index] == 0 || adverse, "actor exit")?;
        require(
            n(v, "finished_ns")? <= t.start + (t.warm + t.measure + 3) * NS,
            "bounded drain/close",
        )?;
        let rows = v["streams"].as_array().ok_or("streams")?;
        require(
            rows.len()
                == if index == e.clients.len() || t.path == PathKind::Reference {
                    connections * 2
                } else {
                    2
                },
            "stream coverage",
        )?;
        for r in rows {
            require(
                n(r, "queue_highwater_bytes")? <= 2097152
                    && n(r, "queue_highwater_descriptors")? <= 4096,
                "retained queues",
            )?;
            require(n(r, "sndbuf")? > 0 && n(r, "rcvbuf")? > 0, "kernel buffers")?;
        }
        if index < e.clients.len() && t.path != PathKind::Reference {
            let r = &v["resources"];
            require(
                n(r, "native_peak")? > 0 && n(r, "native_peak")? <= n(r, "native_budget")?,
                "native allocation ceiling",
            )?;
            require(
                n(r, "native_budget")?
                    + n(r, "scratch_bytes")?
                    + n(r, "bend_peak")?
                    + n(r, "c_peak")?
                    + n(v, "stack_scratch_ceiling_bytes")?
                    <= n(&profile.raw, "retained_per_connection_ceiling")?,
                "owned memory ceiling",
            )?;
            for k in ["native_live", "layout_overflow", "wake_errors"] {
                require(n(r, k)? == 0, k)?;
            }
            require(n(r,"bend_live")? == n(v,"bend_baseline")?, "Bend idle allocation baseline")?;
            require(
                n(r, "c_live")? == n(v, "c_baseline")?,
                "C idle allocation baseline",
            )?;
            require(
                n(r, "bend_peak")? <= n(&profile.raw, "bend_heap_ceiling")?
                    && n(r, "c_peak")? <= n(&profile.raw, "c_scratch_ceiling")?,
                "Bend/C ceiling",
            )?;
            require(
                r["fds_restored"] == true && r["threads_restored"] == true,
                "cleanup baseline",
            )?;
            require(n(r, "close_ns")? <= NS + 10_000_000, "close join deadline")?;
        } else if v.get("tracked_heap_peak_bytes").is_some() {
            require(
                n(v, "tracked_heap_peak_bytes")? <= n(v, "tracked_heap_limit_bytes")?
                    && n(v, "allocation_count")? > 0,
                "reference allocation accounting",
            )?;
        }
    }
    for epoch in 0..2 {
        for s in streams(t, e, epoch)? {
            let count = s.rate * if epoch == 0 { t.warm } else { t.measure };
            let scheduled = n(s.tx, "scheduled")?;
            require(scheduled == n(s.rx, "scheduled")?, "schedule mismatch")?;
            if !adverse {
                require(scheduled == count, "schedule count")?;
            }
            let attempted = n(s.tx, "attempted")?;
            let admitted = n(s.tx, "admitted")?;
            let transmitted = n(s.tx, "transmitted")?;
            let received = n(s.rx, "validated")?;
            // A failed library flush can transmit a complete prefix before draining its buffer.
            let flush_prefix = adverse && (t.path == PathKind::Reference || !s.to_peer);
            let receive_ceiling = if flush_prefix {admitted} else {transmitted};
            require(admitted+n(s.tx,"rejected")?==attempted && transmitted<=admitted && received<=receive_ceiling,&format!("admission != delivery accounting: attempted={attempted} admitted={admitted} transmitted={transmitted} validated={received}"))?;
            if !adverse {
                require(
                    attempted == count
                        && admitted == count
                        && transmitted == count
                        && received == count,
                    "exact scheduled delivery",
                )?;
            }
            if admitted == received {
                require(s.tx["digest"] == s.rx["digest"], "identity digest")?;
            }
            require(
                n(s.rx, "validated_in_window")? + n(s.rx, "validated_after_window")? == received,
                "window partition",
            )?;
            p99(s.rx, "latency_histogram_us", received)?;
            p99(s.tx, "lateness_histogram_us", attempted)?;
        }
    }
    Ok(())
}
#[derive(Debug)]
enum CapacityFailure {
    Correctness(Box<dyn std::error::Error + Send + Sync>),
    Observation(Box<dyn std::error::Error + Send + Sync>),
    Latency,
    Backlog,
}
impl std::fmt::Display for CapacityFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Correctness(e) | Self::Observation(e) => write!(f,"{e}"),
            Self::Latency => write!(f,"scheduled latency gate"),
            Self::Backlog => write!(f,"frozen tail backlog envelope"),
        }
    }
}
impl std::error::Error for CapacityFailure {}
impl From<Box<dyn std::error::Error + Send + Sync>> for CapacityFailure {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {Self::Observation(e)}
}
impl From<&str> for CapacityFailure {
    fn from(e: &str) -> Self {Self::Observation(e.into())}
}
fn capacity_gate(t: &Trial, e: &Evidence, p: &Profile, latency: u64) -> std::result::Result<(),CapacityFailure> {
    correctness(t, e, p, false).map_err(CapacityFailure::Correctness)?;
    for s in streams(t, e, 1)? {
        if s.rate == 0 {
            continue;
        }
        let budget = if t.path == PathKind::Native && !s.to_peer {
            n(&p.raw, "capacity_p99_us")?
        } else {
            latency
        };
        let tx = s.tx["samples"].as_array().ok_or("tx samples")?;
        let rx = s.rx["samples"].as_array().ok_or("rx samples")?;
        require(
            tx.len() == t.measure as usize && rx.len() == tx.len(),
            "complete samples",
        )?;
        let envelope = (s.rate * n(&p.raw, "capacity_p99_us")? + 999999) / 1000000;
        for (index, (a, b)) in tx.iter().zip(rx).enumerate() {
            let ts = b[0].as_u64().ok_or("receiver sample timestamp")?;
            let boundary = t.start + (t.warm + index as u64 + 1) * NS;
            let allowance = n(&p.raw, "service_allowance_ms")? * 1_000_000;
            let producer = a[0].as_u64().ok_or("producer sample timestamp")?;
            require(ts >= boundary && ts <= boundary + allowance && producer >= boundary && producer <= boundary + allowance, "missing/late scheduled sample boundary")?;
            if index < tx.len() * 3 / 4 { continue; }
            let elapsed = ts.saturating_sub(t.start + t.warm * NS).min(t.measure * NS);
            let due = (elapsed * s.rate / NS + 1).min(t.measure * s.rate);
            let validated = b[2].as_u64().ok_or("sample in-window validated")?;
            if due.saturating_sub(validated) > envelope {return Err(CapacityFailure::Backlog);}
            require(
                ts.abs_diff(a[0].as_u64().ok_or("producer sample timestamp")?) <= 10_000_000,
                "unaligned samples",
            )?;
        }
        if p99(s.rx,"latency_histogram_us",n(s.rx,"validated")?)? > budget {return Err(CapacityFailure::Latency);}
    }
    Ok(())
}

struct Process {
    child: Child,
    label: String,
    out: PathBuf,
    stop: Option<Arc<AtomicBool>>,
    campaign_deadline: Option<Instant>,
}
struct Lifecycle {
    actor: Process,
    listener: TcpListener,
    out: PathBuf,
    cycle: usize,
}
impl Process {
    fn ready(&mut self, expected: &str) -> Result<()> {
        let fd = self.child.stdout.as_ref().ok_or("stdout")?.as_raw_fd();
        require(
            unsafe { libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK) } != -1,
            "readiness fd",
        )?;
        let deadline = self.campaign_deadline.map_or(Instant::now() + Duration::from_secs(5), |d| d.min(Instant::now() + Duration::from_secs(5)));
        let mut bytes = vec![];
        loop {
            require(
                Instant::now() < deadline && bytes.len() < 256,
                "readiness deadline",
            )?;
            let mut b = [0];
            match self.child.stdout.as_mut().unwrap().read(&mut b) {
                Ok(0) => {
                    return Err(format!(
                        "{} exited before readiness: {}",
                        self.label,
                        fs::read_to_string(self.out.join(format!("{}.stderr", self.label)))?
                    )
                    .into())
                }
                Ok(_) if b[0] == b'\n' => {
                    let line = String::from_utf8(bytes)?;
                    require(line.starts_with(expected), "readiness line")?;
                    return Ok(());
                }
                Ok(_) => bytes.push(b[0]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1))
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    fn barrier(&mut self, start: u64) -> Result<()> {
        writeln!(self.child.stdin.as_mut().ok_or("stdin")?, "{start}")?;
        Ok(())
    }
    fn wait(&mut self, deadline: Instant) -> Result<i32> {
        let deadline = self.campaign_deadline.map_or(deadline, |d| d.min(deadline));
        loop {
            require(!self.stop.as_ref().is_some_and(|stop|stop.load(Ordering::Acquire)), "steady soak cancelled after lifecycle failure")?;
            if let Some(s) = self.child.try_wait()? {
                return s
                    .code()
                    .ok_or_else(|| format!("{} signal exit", self.label).into());
            }
            require(
                Instant::now() < deadline,
                &format!("{} deadline", self.label),
            )?;
            thread::sleep(Duration::from_millis(2));
        }
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        unsafe {
            libc::kill(-(self.child.id() as i32), libc::SIGKILL);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
struct Controller {
    root: PathBuf,
    cache: PathBuf,
    out: PathBuf,
    profile: Profile,
    peer: PathBuf,
    native: PathBuf,
    bend: PathBuf,
    next_trial: usize,
    records: Vec<Value>,
    diagnostic: Option<&'static str>,
    qualification_running: bool,
    build_dir: PathBuf,
    _cpu_lease: Option<fs::File>,
    stop: Option<Arc<AtomicBool>>,
    campaign_deadline: Option<Instant>,
}
fn lock_file(path: &Path) -> Result<fs::File> {
    Ok(fs::OpenOptions::new().create(true).truncate(false).read(true).write(true).open(path)?)
}
fn input_files(directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            if !matches!(path.file_name().and_then(|s|s.to_str()), Some(".git" | "target")) {
                input_files(&path, files)?;
            }
        } else if path.extension().is_some_and(|s| matches!(s.to_str(), Some("c" | "cpp" | "h" | "hpp" | "bend" | "js" | "rs" | "toml"))) {
            files.push(path);
        }
    }
    Ok(())
}
fn checksums(files: &[PathBuf]) -> Result<String> {
    let output = Command::new("sha256sum").arg("--").args(files).output()?;
    require(output.status.success(), "fixture input checksums")?;
    Ok(String::from_utf8(output.stdout)?)
}
fn content_key(bytes: &[u8]) -> Result<String> {
    let mut child = Command::new("sha256sum").stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;
    child.stdin.take().ok_or("hash stdin")?.write_all(bytes)?;
    let output = child.wait_with_output()?;
    require(output.status.success(), "fixture identity checksum")?;
    Ok(String::from_utf8(output.stdout)?.split_whitespace().next().ok_or("checksum")?.into())
}
impl Controller {
    fn prepare(scoped: bool) -> Result<Self> {
        let started = Instant::now();
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .canonicalize()?;
        let cache = env::var_os("WS_CACHE")
            .map(PathBuf::from)
            .unwrap_or_else(|| root.join(".ws-cache"))
            .canonicalize()?;
        let extended = env::var("WS_PHASE5_EXTENDED").is_ok_and(|s| s == "1");
        let raw: Value = serde_json::from_slice(&fs::read(root.join(if extended {
            "bench/ws/profiles/local-perf-v1.json"
        } else { "bench/ws/profiles/local-perf-v2.json" }))?)?;
        require(
            n(&raw, "schema")? == 1
                && n(&raw, "paired_trials")? == 5
                && n(&raw, "warm_s")? == if extended {10} else {5}
                && n(&raw, "measure_s")? == if extended {60} else {20},
            "frozen profile timing",
        )?;
        require(n(&raw,"calibration_warm_s")?==1 && n(&raw,"calibration_measure_s")?==3 && n(&raw,"qualification_refinements")?==3,"frozen calibration policy")?;
        let campaign_deadline = if !scoped && !extended {Some(started + Duration::from_secs(n(&raw,"campaign_seconds")?))} else {None};
        let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        require(
            unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&mask), &mut mask) } == 0,
            "affinity",
        )?;
        let cpus = (0..libc::CPU_SETSIZE as usize)
            .filter(|&c| unsafe { libc::CPU_ISSET(c, &mask) })
            .collect::<Vec<_>>();
        require(cpus.len() >= 2, "two endpoint CPUs")?;
        let mut selected = [cpus[0], cpus[1]];
        let slots = root.join("target/ws-fixtures/cpu-slots");
        fs::create_dir_all(&slots)?;
        let cpu_lease = if scoped {
            let deadline = Instant::now() + Duration::from_secs(120);
            Some(loop {
                let mut acquired = None;
                for pair in cpus.chunks_exact(2).take(4) {
                    let lease = lock_file(&slots.join(format!("{}-{}.lock",pair[0],pair[1])))?;
                    if unsafe {libc::flock(lease.as_raw_fd(),libc::LOCK_EX | libc::LOCK_NB)} == 0 {
                        selected = [pair[0],pair[1]];
                        acquired = Some(lease);
                        break;
                    }
                }
                if let Some(lease) = acquired {break lease;}
                require(Instant::now()<deadline,"correctness CPU slot deadline")?;
                thread::sleep(Duration::from_millis(20));
            })
        } else {
            let lease = lock_file(&slots.join(format!("{}-{}.lock",selected[0],selected[1])))?;
            loop {
                if unsafe {libc::flock(lease.as_raw_fd(),libc::LOCK_EX | libc::LOCK_NB)} == 0 {break;}
                require(campaign_deadline.is_none_or(|d| Instant::now() < d), "inconclusive: campaign deadline waiting for endpoint CPUs")?;
                thread::sleep(Duration::from_millis(20));
            }
            Some(lease)
        };
        let out = root.join(format!(
            "results/ws/cargo-{}-{}/phase-5",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
            std::process::id()
        ));
        fs::create_dir_all(out.join("build"))?;
        save(&out.join("profile.json"), &raw)?;
        let pins: Value =
            serde_json::from_slice(&fs::read(root.join("bench/ws/profiles/local-v1.json"))?)?;
        save(&out.join("pins.json"), &pins["pins"])?;
        let uname = Command::new("uname").arg("-a").output()?;
        save(
            &out.join("machine.json"),
            &json!({"cpus":selected,"scoped_cpu_lease":scoped,"kernel":String::from_utf8_lossy(&uname.stdout),"governor":fs::read_to_string(format!("/sys/devices/system/cpu/cpu{}/cpufreq/scaling_governor",selected[0])).ok(),"client_cpu_budget":1,"peer_cpu_budget":1,"native_workers_per_connection":1,"bend_runtime_threads_per_process":1,"reference_client_processes":1,"candidate_processes":"one per connection","build_flags":"clang -O2; reference -O3; Cargo release locked"}),
        )?;
        save(
            &out.join("summary.json"),
            &json!({"scope":"routine","qualification_run":false,"phase_pass":false,"routine_pass":false}),
        )?;
        println!("Phase 5 evidence: {}", out.display());
        let mut c = Self {
            root,
            cache,
            peer: out.join("build/uws-reference"),
            native: out.join("build/phase5-native"),
            bend: out.join("build/bend-phase5"),
            build_dir: out.join("build"),
            out,
            profile: Profile {
                raw,
                cpus: selected,
            },
            next_trial: 0,
            records: vec![],
            diagnostic: None,
            qualification_running: false,
            _cpu_lease: cpu_lease,
            stop: None,
            campaign_deadline,
        };
        c.build()?;
        Ok(c)
    }
    fn spawn(
        &self,
        mut c: Command,
        out: &Path,
        label: &str,
        cpu: Option<usize>,
    ) -> Result<Process> {
        save(
            &out.join(format!("{label}-command.json")),
            &json!({"program":c.get_program(),"args":c.get_args().collect::<Vec<_>>(),"environment":c.get_envs().collect::<Vec<_>>()}),
        )?;
        c.current_dir(&self.root)
            .process_group(0)
            .env_remove("BENDRTS")
            .env("BEND_NO_TELEMETRY", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(fs::File::create(out.join(format!("{label}.stderr")))?);
        unsafe {
            c.pre_exec(move || {
                let limit = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                if libc::setrlimit(libc::RLIMIT_CORE, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if let Some(cpu) = cpu {
                    let mut mask = std::mem::zeroed::<libc::cpu_set_t>();
                    libc::CPU_SET(cpu, &mut mask);
                    if libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        Ok(Process {
            child: c.spawn()?,
            label: label.into(),
            out: out.into(),
            stop: self.stop.clone(),
            campaign_deadline: self.campaign_deadline,
        })
    }
    fn build_command(&self, mut c: Command, label: &str) -> Result<String> {
        let out = self.build_dir.clone();
        save(
            &out.join(format!("{label}-command.json")),
            &json!({"program":c.get_program(),"args":c.get_args().collect::<Vec<_>>()}),
        )?;
        c.current_dir(&self.root)
            .env_remove("BENDRTS")
            .env("BEND_NO_TELEMETRY", "1");
        c.process_group(0).stdout(fs::File::create(out.join(format!("{label}.stdout")))?)
            .stderr(fs::File::create(out.join(format!("{label}.stderr")))?);
        let mut process = Process {child:c.spawn()?,label:label.into(),out:out.clone(),stop:None,campaign_deadline:self.campaign_deadline};
        let status = process.wait(Instant::now()+Duration::from_secs(120))?;
        let stderr = fs::read_to_string(out.join(format!("{label}.stderr")))?;
        require(status==0,&format!("{label}: {stderr}"))?;
        fs::read_to_string(out.join(format!("{label}.stdout"))).map_err(Into::into)
    }
    fn build(&mut self) -> Result<()> {
        let clang = Path::new("/usr/lib/llvm-19/bin/clang");
        let bend = env::var_os("BEND").map(PathBuf::from).unwrap_or_else(|| self.cache.join("bend/bin/bend"));
        let mut c = Command::new(&bend);
        c.arg("version");
        require(
            self.build_command(c, "bend-version")?.trim() == "bend 2.0.40",
            "Bend pin",
        )?;
        let pins: Value = serde_json::from_slice(&fs::read(self.out.join("pins.json"))?)?;
        for directory in ["uSockets", "uWebSockets"] {
            let mut c = Command::new("git");
            c.arg("-C")
                .arg(self.cache.join(directory))
                .args(["rev-parse", "HEAD"]);
            require(
                self.build_command(c, &format!("pin-{directory}"))?.trim()
                    == pins[directory].as_str().ok_or("dependency pin")?,
                "reference dependency pin",
            )?;
        }
        let deps = env::current_exe()?.parent().ok_or("deps")?.to_owned();
        let marker = confluxion_ws_native::build_marker();
        let mut archives = fs::read_dir(deps)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "a")
                && p.file_name().unwrap().to_string_lossy().starts_with("libconfluxion_ws_native-"))
            .filter(|p| fs::read(p).is_ok_and(|b| b.windows(marker.len()).any(|x| x == marker.as_bytes())))
            .collect::<Vec<_>>();
        archives.sort();
        let archive = archives.first().ok_or("current native archive")?;
        let runner = fs::read_to_string(self.root.join("tests/ws/phase5.rs"))?;
        let generator = runner.split_once("\nfn replace(").ok_or("fixture generator source anchor")?.1.split_once("\n#[test]").ok_or("fixture generator end anchor")?.0;
        let generator_key = content_key(generator.as_bytes())?;
        let recipe = runner.split_once("\n    fn build_command(").ok_or("fixture recipe source anchor")?.1.split_once("\n    fn actor_command(").ok_or("fixture recipe end anchor")?.0;
        let recipe_key = content_key(recipe.as_bytes())?;
        let mut files = vec![archive.clone(),bend.clone(),clang.canonicalize()?,self.root.join("Cargo.lock"),PathBuf::from("/usr/lib/gcc/x86_64-linux-gnu/13/libstdc++.a"),PathBuf::from("/usr/lib/gcc/x86_64-linux-gnu/13/libgcc.a")];
        for name in ["reference-native/main.cpp","reference-native/allocator.cpp","phase5-native.c","phase5-common.h","bend-phase5.bend","phase5-schema.bend","phase5-probe.c","phase5-probe.js"] {files.push(self.root.join("bench/ws").join(name));}
        for directory in [self.root.join("src/ws"),self.root.join("src/bytes"),self.root.join("tools/bend-codec"),self.cache.join("uSockets/src"),self.cache.join("uWebSockets/src"),self.cache.join("bend/bend2")] {
            input_files(&directory, &mut files)?;
        }
        files.sort();
        files.dedup();
        let mut tools = vec![];
        for (index,(program,args)) in [("rustc",vec!["-vV"]),(clang.to_str().ok_or("clang path")?,vec!["--version"]),("gcc-13",vec!["--version"]),("ld",vec!["--version"])].into_iter().enumerate() {
            let mut command = Command::new(program);
            command.args(args);
            tools.push(json!({"program":program,"version":self.build_command(command,&format!("toolchain-{index}"))?}));
        }
        let identity = json!({"schema":1,"archive":archive,"marker":marker,"pins":pins,"tools":tools,"inputs":checksums(&files)?,"generator":generator_key,"recipe":recipe_key,"flags":"peer: -O3 -DLIBUS_NO_SSL -DUWS_NO_ZLIB -pthread -std=c11/-x c++ -std=c++20 --gcc-install-dir=/usr/lib/gcc/x86_64-linux-gnu/13; link: --wrap=malloc,free,calloc,realloc -lstdc++ -pthread; actors: -O2 -g -std=c11 -DBEND_POISON(bend) -lpthread -lm -ldl; Bend 2.0.40"});
        let key = content_key(&serde_json::to_vec(&identity)?)?;
        let base = self.root.join("target/ws-fixtures");
        fs::create_dir_all(&base)?;
        let builder = lock_file(&base.join(format!("{key}.lock")))?;
        loop {
            if unsafe {libc::flock(builder.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)} == 0 {break;}
            require(self.campaign_deadline.is_none_or(|d|Instant::now()<d),"inconclusive: campaign deadline waiting for fixture builder")?;
            thread::sleep(Duration::from_millis(20));
        }
        self.build_dir = base.join(&key);
        fs::create_dir_all(&self.build_dir)?;
        self.peer = self.build_dir.join("uws-reference");
        self.native = self.build_dir.join("phase5-native");
        self.bend = self.build_dir.join("bend-phase5");
        let codec_dir = self.build_dir.join("phase5-codec");
        let mut binaries = vec![self.peer.clone(),self.native.clone(),self.bend.clone()];
        for name in ["codec.bend","codec.c","codec.js","contract.json"] {binaries.push(codec_dir.join(name));}
        binaries.push(self.build_dir.join("bend-phase5.bend"));
        let complete = self.build_dir.join("complete.json");
        let reused = fs::read(&complete).ok().and_then(|b|serde_json::from_slice::<Value>(&b).ok())
            .is_some_and(|v|v["identity"]==identity && checksums(&binaries).is_ok_and(|s|v["artifacts"]==s));
        save(&self.out.join("build/identity.json"),&json!({"key":key,"directory":self.build_dir,"cache_hit":reused,"identity":identity}))?;
        println!("Phase 5 fixture cache {}: {key}",if reused {"hit"} else {"miss"});
        if reused {return Ok(());}
        if complete.exists() {fs::remove_file(&complete)?;}
        let schema = bend_codec::load(&self.root.join("bench/ws/phase5-schema.bend"), &["Frame".into()], bend_codec::Format::RawBe)?;
        let outputs = bend_codec::generate(&schema, &codec_dir);
        bend_codec::write_outputs(&codec_dir, &outputs)?;
        let fixture = fs::read_to_string(self.root.join("bench/ws/bend-phase5.bend"))?
            .replace("../../src/", "../../../src/")
            .replace("./phase5-schema.bend", "../../../bench/ws/phase5-schema.bend")
            .replace("\"./phase5-probe.c\"", "\"../../../bench/ws/phase5-probe.c\"")
            .replace("\"./phase5-probe.js\"", "\"../../../bench/ws/phase5-probe.js\"");
        fs::write(self.build_dir.join("bend-phase5.bend"), fixture)?;
        save(&self.build_dir.join("codec-recipe.json"), &json!({"schema":"bench/ws/phase5-schema.bend","roots":["Frame"],"format":"raw-be","generator":"bend_codec::generate/write_outputs","fixture_import_rewrite":"target/ws-fixtures/<input-key> to repository sources; codec remains ./phase5-codec/codec.bend","artifacts":checksums(&binaries[3..])?}))?;
        let mut sources = vec![];
        for d in ["src", "src/eventing", "src/crypto", "src/io_uring"] {
            for e in fs::read_dir(self.cache.join("uSockets").join(d))? {
                let p = e?.path();
                if p.extension().is_some_and(|x| x == "c") {
                    sources.push(p)
                }
            }
        }
        sources.sort();
        sources.extend([
            self.root.join("bench/ws/reference-native/main.cpp"),
            self.root.join("bench/ws/reference-native/allocator.cpp"),
        ]);
        let mut objects = vec![];
        for (i, _) in sources.iter().enumerate() {
            objects.push(self.build_dir.join(format!("peer-{i}.o")));
        }
        let controller = &*self;
        for chunk in (0..sources.len()).collect::<Vec<_>>().chunks(4) {
          thread::scope(|scope| -> Result<()> {
           let mut workers = vec![];
           for &i in chunk {
            let s = &sources[i];
            let o = &objects[i];
            let mut c = Command::new(clang);
            c.args(["-O3", "-DLIBUS_NO_SSL", "-DUWS_NO_ZLIB", "-pthread"])
                .arg(format!("-I{}", self.cache.join("uSockets/src").display()))
                .arg(format!(
                    "-I{}",
                    self.cache.join("uWebSockets/src").display()
                ));
            if s.extension().is_some_and(|x| x == "cpp") {
                c.args([
                    "-x",
                    "c++",
                    "-std=c++20",
                    "--gcc-install-dir=/usr/lib/gcc/x86_64-linux-gnu/13",
                ]);
            } else {
                c.arg("-std=c11");
            }
            c.arg("-c").arg(s).arg("-o").arg(o);
            workers.push(scope.spawn(move || controller.build_command(c, &format!("peer-{i}"))));
           }
           for worker in workers {worker.join().map_err(|_|"peer compiler panic")??;}
           Ok(())
          })?;
        }
        let mut c = Command::new(clang);
        c.arg("--gcc-install-dir=/usr/lib/gcc/x86_64-linux-gnu/13")
            .args(objects)
            .args([
                "-Wl,--wrap=malloc,--wrap=free,--wrap=calloc,--wrap=realloc",
                "-lstdc++",
                "-pthread",
            ])
            .arg("-o")
            .arg(&self.peer);
        self.build_command(c, "peer-link")?;
        let mut c = Command::new(clang);
        c.args(["-O2", "-g", "-std=c11"])
            .arg("-I")
            .arg(self.root.join("src/ws/native"))
            .arg(self.root.join("bench/ws/phase5-native.c"))
            .arg(archive)
            .args(["-lpthread", "-lm", "-ldl"])
            .arg("-o")
            .arg(&self.native);
        self.build_command(c, "native")?;
        let original = self.build_dir.join("original.c");
        let mut c = Command::new(&bend);
        c.arg(self.build_dir.join("bend-phase5.bend")).arg("-o").arg(&original);
        self.build_command(c, "emit")?;
        let source = self.build_dir.join("instrumented.c");
        fs::write(&source, instrument(fs::read_to_string(original)?)?)?;
        let mut c = Command::new(clang);
        c.args(["-O2", "-g", "-std=c11", "-DBEND_POISON"])
            .arg("-I")
            .arg(self.root.join("src/ws/native"))
            .arg("-I")
            .arg(self.root.join("src/bytes"))
            .arg("-I")
            .arg(self.root.join("bench/ws"))
            .arg(source)
            .arg(archive)
            .args(["-lpthread", "-lm", "-ldl"])
            .arg("-o")
            .arg(&self.bend);
        self.build_command(c, "bend")?;
        require(identity["inputs"]==checksums(&files)?,"fixture inputs changed during compilation")?;
        save(&self.build_dir.join("complete.tmp"),&json!({"identity":identity,"artifacts":checksums(&binaries)?}))?;
        fs::rename(self.build_dir.join("complete.tmp"),complete)?;
        Ok(())
    }
    fn actor_command(&self, t: &Trial, port: u16, id: usize, report: &Path) -> Command {
        let binary = match t.path {
            PathKind::Reference => Path::new(env!("CARGO_BIN_EXE_ws-reference")),
            PathKind::Native => &self.native,
            PathKind::Bend => &self.bend,
        };
        let mut c = Command::new(binary);
        if t.path == PathKind::Reference {
            self.reference_args(&mut c, t, port, report);
        } else {
            if t.path == PathKind::Bend {
                c.args(["+RTS", "--threads", "1", "--gpu", "off", "-RTS"]);
            }
            let (size, _, _) = t.workload.dimensions();
            c.env("WS_PORT", port.to_string())
                .env("WS_CONNECTION", id.to_string())
                .env("WS_SIZE", size.to_string())
                .env("WS_TX_RATE", t.rates.to_peer.to_string())
                .env("WS_RX_RATE", t.rates.to_application.to_string())
                .env("WS_WARM", t.warm.to_string())
                .env("WS_MEASURE", t.measure.to_string())
                .env("WS_TOKEN", t.token.to_string())
                .env("WS_REPORT", report)
                .env("WS_SOCKET_BUFFER", "262144")
                .env("WS_SATURATION", "0")
                .env("WS_PAUSE_RECEIVE_MS", "0")
                .env("WS_OFFER_NS", "0");
            if let Workload::W4 { pause_ms, receiver } = t.workload {
                c.env("WS_SOCKET_BUFFER", "16384")
                    .env("WS_SATURATION", "1")
                    .env("WS_OFFER_NS", "100000000")
                    .env("WS_OBSERVED_PAUSE_MS", pause_ms.to_string());
                if receiver == 1 {
                    c.env("WS_PAUSE_RECEIVE_MS", pause_ms.to_string());
                }
            }
        }
        self.diagnostic_command(c, report)
    }
    fn diagnostic_command(&self, mut c:Command, report:&Path) -> Command {
        if let Some(tool) = self.diagnostic {
            let mut wrapped = Command::new(if tool == "strace" {
                env::var_os("WS_PHASE5_STRACE").unwrap_or_else(|| "strace".into())
            } else {env::var_os("WS_PHASE5_PERF").unwrap_or_else(|| "perf".into())});
            if tool == "perf" {
                wrapped
                    .args(["record", "-e", "cpu-clock:u", "-F", "99", "-g", "--user-callchains", "-o"])
                    .arg(report.with_extension("perf.data"))
                    .arg("--");
            } else if tool == "perf-stat" {
                wrapped.args(["stat", "-e", "task-clock,context-switches,cpu-migrations,page-faults", "-x", ",", "-o"])
                    .arg(report.with_extension("perf-stat.txt")).arg("--");
            } else {
                wrapped
                    .args(["-f", "-c", "-o"])
                    .arg(report.with_extension("strace.txt"))
                    .arg("--");
            }
            wrapped.arg(c.get_program()).args(c.get_args());
            for (k, v) in c.get_envs() {
                if let Some(v) = v {
                    wrapped.env(k, v);
                } else {
                    wrapped.env_remove(k);
                }
            }
            c = wrapped;
        }
        c
    }
    fn reference_args(&self, c: &mut Command, t: &Trial, port: u16, report: &Path) {
        let (size, connections, _) = t.workload.dimensions();
        c.args([
            if t.tcp { "tcp" } else { "ws" },
            &port.to_string(),
            &connections.to_string(),
            &size.to_string(),
            &t.rates.to_peer.to_string(),
            &t.rates.to_application.to_string(),
            &t.warm.to_string(),
            &t.measure.to_string(),
            "2",
            &t.token.to_string(),
            if t.fault { "1" } else { "-1" },
            if t.fault { "corruption" } else { "none" },
            "0",
        ])
        .arg(report);
        c.env("WS_PAUSE_RECEIVE_MS", "0")
            .env("WS_EARLY_DRAIN", "1")
            .env(
                "WS_LONG_MEASURE",
                if matches!(t.workload, Workload::W5) {
                    "1"
                } else {
                    "0"
                },
            );
    }
    fn trial(&mut self, mut t: Trial) -> Result<(Trial, Evidence)> {
        require(self.campaign_deadline.is_none_or(|d| Instant::now() + Duration::from_secs(t.warm + t.measure + 8) < d), "inconclusive: insufficient campaign time for complete trial and cleanup")?;
        let out = self.out.join(format!("trial-{:04}", self.next_trial));
        self.next_trial += 1;
        fs::create_dir(&out)?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        drop(listener);
        let mut c = Command::new(&self.peer);
        self.reference_args(&mut c, &t, port, &out.join("peer.json"));
        let mut peer = self.spawn(self.diagnostic_command(c,&out.join("peer.json")), &out, "peer", Some(self.profile.cpus[1]))?;
        peer.ready("LISTENING")?;
        let (_, connections, _) = t.workload.dimensions();
        let mut clients = vec![];
        let actors = if t.path == PathKind::Reference {
            1
        } else {
            connections
        };
        for id in 0..actors {
            let mut a = self.spawn(
                self.actor_command(&t, port, id, &out.join(format!("client-{id}.json"))),
                &out,
                &format!("client-{id}"),
                Some(self.profile.cpus[0]),
            )?;
            a.ready("READY")?;
            clients.push(a);
        }
        t.start = now() + 250_000_000;
        save(
            &out.join("trial.json"),
            &json!({"path":format!("{:?}",t.path),"workload":format!("{:?}",t.workload),"rates":[t.rates.to_peer,t.rates.to_application],"warm":t.warm,"measure":t.measure,"token":t.token,"start":t.start,"tcp":t.tcp}),
        )?;
        peer.barrier(t.start)?;
        for a in &mut clients {
            a.barrier(t.start)?;
        }
        let deadline = Instant::now() + Duration::from_secs(t.warm + t.measure + 6);
        let mut statuses = vec![];
        for a in &mut clients {
            statuses.push(a.wait(deadline)?);
        }
        statuses.push(peer.wait(deadline)?);
        let clients = (0..actors)
            .map(|id| {
                Ok(serde_json::from_slice(&fs::read(
                    out.join(format!("client-{id}.json")),
                )?)?)
            })
            .collect::<Result<Vec<Value>>>()?;
        let peer = serde_json::from_slice(&fs::read(out.join("peer.json"))?)?;
        Ok((
            t,
            Evidence {
                clients,
                peer,
                statuses,
                directory: out,
            },
        ))
    }
    fn record(&mut self, t: &Trial, e: &Evidence, verdict: &str) -> Result<()> {
        let cpu = e.peer["cpu_ticks"]
            .as_u64()
            .map(|ticks| ticks as f64 / ((t.warm + t.measure) as f64 * NS as f64));
        self.records.push(json!({"path":format!("{:?}",t.path),"workload":format!("{:?}",t.workload),"rates":[t.rates.to_peer,t.rates.to_application],"verdict":verdict,"evidence":e.directory,"statuses":e.statuses,"peer_cpu_fraction":cpu}));
        save(&self.out.join("trials.json"), &json!(self.records))
    }
    fn run_case(
        &mut self,
        path: PathKind,
        workload: Workload,
        rate: u64,
        warm: u64,
        measure: u64,
    ) -> Result<(Trial, Evidence)> {
        let split = workload.dimensions().2;
        self.trial(Trial {
            path,
            workload,
            rates: split.scaled(rate),
            warm,
            measure,
            token: now(),
            start: 0,
            tcp: false,
            fault: false,
        })
    }
    fn routine_duplex(&mut self, path: PathKind, connections: usize) -> Result<()> {
        let (t, e) = self.run_case(
            path,
            Workload::W2 {
                connections,
                split: Rates {
                    to_peer: 1,
                    to_application: 1,
                },
            },
            100,
            0,
            1,
        )?;
        correctness(&t, &e, &self.profile, false)?;
        self.record(&t, &e, "routine 256-byte scheduled duplex")?;
        self.routine_summary(&format!("{path:?} duplex, {connections} connections"))
    }
    fn routine_controls(&mut self) -> Result<()> {
        let workload = Workload::W2 {connections:1,split:Rates {to_peer:1,to_application:1}};
        let (t,e) = self.run_case(PathKind::Native,workload,100,0,1)?;
        correctness(&t,&e,&self.profile,false)?;
        let mut broken = Evidence {clients:e.clients.clone(),peer:e.peer.clone(),statuses:e.statuses.clone(),directory:e.directory.clone()};
        broken.peer["streams"][1]["rx"]["validated"] = json!(999);
        require(correctness(&t,&broken,&self.profile,false).is_err(),"damaged count accepted")?;
        broken.peer = e.peer.clone();
        broken.peer["streams"].as_array_mut().unwrap().pop();
        require(correctness(&t,&broken,&self.profile,false).is_err(),"missing stream accepted")?;
        broken.peer = e.peer.clone();
        for actor in broken.clients.iter_mut().chain([&mut broken.peer]) {
            for row in actor["streams"].as_array_mut().ok_or("streams")? {
                for direction in ["tx","rx"] {
                    let flow = &mut row[direction];
                    for (hist,count) in [("latency_histogram_us","validated"),("lateness_histogram_us","attempted")] {
                        let mut bins = vec![0u64;10001];
                        bins[0] = n(flow,count)?;
                        flow[hist] = json!(bins);
                    }
                    let validated = n(flow,"validated")?;
                    for sample in flow["samples"].as_array_mut().ok_or("samples")? {
                        sample[0] = json!(t.start + NS);
                        sample[2] = json!(validated);
                    }
                }
            }
        }
        capacity_gate(&t,&broken,&self.profile,2000)?;
        save(&e.directory.join("sample-evaluator-fixture.json"),&json!({"clients":broken.clients,"peer":broken.peer,"synthetic_timing":true,"purpose":"deterministic evaluator control, not performance evidence"}))?;
        let valid = broken.clients[0].clone();
        broken.clients[0]["streams"][1]["rx"]["samples"][0][0] = json!(t.start + NS + 11_000_000);
        require(matches!(capacity_gate(&t,&broken,&self.profile,2000),Err(CapacityFailure::Observation(_))),"late sample classified as capacity saturation")?;
        broken.clients[0] = valid;
        broken.clients[0]["streams"][1]["rx"]["samples"][0][2] = json!(0);
        require(capacity_gate(&t,&broken,&self.profile,2000).is_err(),"drain total hid in-window backlog")?;
        self.record(&t,&e,"damaged-report controls; timing evaluator uses synthetic fixture")?;
        let t = Trial {
            path: PathKind::Bend,
            workload: Workload::W2 {
                connections: 1,
                split: Rates {
                    to_peer: 1,
                    to_application: 1,
                },
            },
            rates: Rates {
                to_peer: 100,
                to_application: 100,
            },
            warm: 0,
            measure: 1,
            token: now(),
            start: 0,
            tcp: false,
            fault: true,
        };
        let (actual, e) = self.trial(t.clone())?;
        require(
            correctness(&actual, &e, &self.profile, false).is_err(),
            "actual corruption accepted",
        )?;
        require(
            e.clients[0]["failure"]["sequence"] == 50,
            "Bend corruption identity",
        )?;
        self.record(&actual, &e, "real Bend corruption rejected at 50")?;
        self.routine_summary("real corruption and damaged-report controls")
    }
    fn routine_summary(&self, scope: &str) -> Result<()> {
        save(
            &self.out.join("summary.json"),
            &json!({"scope":scope,"qualification_run":false,"phase_pass":false,"routine_pass":true,"trials":self.records,"profile":"profile.json","long_qualification":"separately selectable"}),
        )?;
        Ok(())
    }
    fn capacity_checkpoint(&self, selected: [u64;2], status: &str) -> Result<()> {
        let qualified = status.starts_with("five long");
        let workload = Workload::W2 {connections:1,split:Rates {to_peer:1,to_application:1}};
        let mut evidence = vec![];
        for (index,path) in [PathKind::Native,PathKind::Bend].into_iter().enumerate() {
            let record = self.records.iter().rev().find(|r|r["path"]==format!("{path:?}") && r["workload"]==format!("{workload:?}")
                && r["verdict"]==if qualified {"long capacity qualification passed"} else {"calibration candidate passed, not qualification"}
                && r["validated_capacity_rate"].as_u64().is_some_and(|rate|rate>=selected[index]))
                .ok_or("measured soak capacity evidence")?;
            require(selected[index]>=2,"nonzero half-load soak rate")?;
            evidence.push(json!({"path":format!("{path:?}"),"capacity":selected[index],"soak_rate":selected[index]/2,"record":record}));
        }
        let identity:Value = serde_json::from_slice(&fs::read(self.out.join("build/identity.json"))?)?;
        let checkpoint = json!({"schema":1,"profile":self.profile.raw,"fixture_key":identity["key"],"status":status,"qualified_capacity_trials":qualified,"phase_pass":false,"evidence":evidence});
        save(&self.out.join("capacities.tmp"),&checkpoint)?;
        fs::rename(self.out.join("capacities.tmp"),self.out.join("capacities.json"))?;
        println!("Phase 5 measured capacities checkpoint: {}",self.out.join("capacities.json").display());
        Ok(())
    }
    fn standalone_soak(&mut self, checkpoint: &Path, selected_path: &str) -> Result<()> {
        require(matches!(selected_path,"Native"|"Bend"|"both"),"WS_PHASE5_SOAK_PATH must be Native, Bend, or both")?;
        let checkpoint = checkpoint.canonicalize()?;
        let source:Value = serde_json::from_slice(&fs::read(&checkpoint)?)?;
        let identity:Value = serde_json::from_slice(&fs::read(self.out.join("build/identity.json"))?)?;
        require(n(&source,"schema")?==1 && source["profile"]==self.profile.raw && source["fixture_key"]==identity["key"],"soak checkpoint profile/build mismatch")?;
        let mut measured = vec![];
        for path in [PathKind::Native,PathKind::Bend] {
            if selected_path!="both" && selected_path!=format!("{path:?}") {continue;}
            let entries = source["evidence"].as_array().ok_or("soak capacity evidence")?;
            let matches = entries.iter().filter(|r|r["path"]==format!("{path:?}")).collect::<Vec<_>>();
            require(matches.len()==1,"unique measured path capacity")?;
            let entry = matches[0];
            let rate = n(entry,"capacity")?;
            require(rate>=2 && n(entry,"soak_rate")?==rate/2 && n(&entry["record"],"validated_capacity_rate")?>=rate,"soak measured half-load rate")?;
            let directory = PathBuf::from(entry["record"]["evidence"].as_str().ok_or("capacity evidence directory")?);
            let metadata:Value = serde_json::from_slice(&fs::read(directory.join("trial.json"))?)?;
            let workload = Workload::W2 {connections:1,split:Rates {to_peer:1,to_application:1}};
            require(metadata["path"]==format!("{path:?}") && metadata["workload"]==format!("{workload:?}") && metadata["tcp"]==false,"soak measured workload")?;
            let warm = n(&metadata,"warm")?;
            let measure = n(&metadata,"measure")?;
            let qualified = source["qualified_capacity_trials"].as_bool().ok_or("checkpoint capacity status")?;
            require(if qualified {warm==10 && measure==60} else {warm==1 && measure==3},"measured calibration/qualification timing")?;
            let trial = Trial {path,workload,rates:rates(&metadata["rates"])? ,warm,measure,token:n(&metadata,"token")?,start:n(&metadata,"start")?,tcp:false,fault:false};
            let evidence = Evidence {clients:vec![serde_json::from_slice(&fs::read(directory.join("client-0.json"))?)?],peer:serde_json::from_slice(&fs::read(directory.join("peer.json"))?)?,statuses:serde_json::from_value(entry["record"]["statuses"].clone())?,directory};
            capacity_gate(&trial,&evidence,&self.profile,2000)?;
            let measured_rate = streams(&trial,&evidence,1)?.iter().map(|s|Ok(n(s.rx,"validated_in_window")?/measure)).collect::<Result<Vec<_>>>()?.into_iter().min().ok_or("measured active stream")?;
            require(measured_rate>=rate,"checkpoint capacity exceeds actual measured delivery")?;
            measured.push((path,rate/2));
        }
        save(&self.out.join("summary.json"),&json!({"scope":"standalone soak","checkpoint":checkpoint,"checkpoint_status":source["status"],"phase_pass":false,"qualification_run":false,"soak_pass":false,"verdict":"running"}))?;
        self.qualification_running = true;
        let run = measured.into_iter().try_for_each(|(path,rate)|self.soak(path,rate));
        save(&self.out.join("summary.json"),&json!({"scope":"standalone soak","checkpoint":checkpoint,"checkpoint_status":source["status"],"phase_pass":false,"qualification_run":false,"soak_pass":run.is_ok(),"reason":run.as_ref().err().map(|e|e.to_string()),"trials":self.records}))?;
        run
    }
    fn capacity(&mut self, path: PathKind, workload: &Workload, failed_long: Option<u64>) -> Result<u64> {
        let mut low = 0;
        let mut delivered = 0;
        let mut high = failed_long.unwrap_or(n(&self.profile.raw, "search_initial_rate")?);
        let ceiling = n(&self.profile.raw, "search_ceiling_rate")?;
        let resolution = n(&self.profile.raw, "search_resolution_rate")?;
        let warm = n(&self.profile.raw, "calibration_warm_s")?;
        let measure = n(&self.profile.raw, "calibration_measure_s")?;
        let limit = n(&self.profile.raw, "capacity_p99_us")?;
        let mut bracket = failed_long.is_some();
        for _ in 0..n(&self.profile.raw, "search_max_steps")? {
            let rate = if bracket {
                low + (high - low) / 2
            } else {
                high
            };
            let (t, e) = self.run_case(path, workload.clone(), rate, warm, measure)?;
            let pass = capacity_gate(&t, &e, &self.profile, limit).is_ok();
            self.record(
                &t,
                &e,
                if pass {
                    "calibration candidate passed, not qualification"
                } else {
                    "calibration candidate failed, not qualification"
                },
            )?;
            if pass {
                low = rate;
                let split = workload.dimensions().2;
                delivered = streams(&t, &e, 1)?
                    .iter()
                    .filter(|s| s.rate > 0)
                    .map(|s| {
                        let component = if s.to_peer {
                            split.to_peer
                        } else {
                            split.to_application
                        };
                        Ok(n(s.rx, "validated_in_window")? / measure / component)
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .min()
                    .ok_or("missing active capacity stream")?;
                let record = self.records.last_mut().ok_or("capacity record")?;
                record["validated_capacity_rate"] = json!(delivered);
                save(&self.out.join("trials.json"), &json!(self.records))?;
                if !bracket {
                    require(
                        high < ceiling,
                        "inconclusive: capacity ceiling has no failing bracket",
                    )?;
                    high = (high * 2).min(ceiling);
                }
            } else {
                high = rate;
                bracket = true;
            }
            if bracket && high - low <= resolution {
                require(low > 0, "no sustainable capacity")?;
                require(delivered > 0, "no in-window capacity")?;
                save(&self.out.join(format!("calibration-{:04}.json",self.next_trial)),&json!({"path":format!("{path:?}"),"workload":format!("{workload:?}"),"candidate_capacity":delivered,"passing_scheduled_rate":low,"failing_scheduled_rate":high,"failed_long_bracket":failed_long,"qualified":false}))?;
                return Ok(delivered);
            }
        }
        Err("inconclusive: capacity search step limit".into())
    }
    fn qualification(&mut self) -> Result<Verdict> {
        self.qualification_running = true;
        save(
            &self.out.join("summary.json"),
            &json!({"scope":"qualification","qualification_run":true,"phase_pass":false,"verdict":"running"}),
        )?;
        let mut capacities = vec![];
        let mut cases = self.profile.core()?;
        cases.sort_by_key(|c| !matches!(c,Workload::W2 {connections:1,split} if split.to_peer==1 && split.to_application==1));
        for case in cases {
            let paths = [PathKind::Reference,PathKind::Native,PathKind::Bend];
            let mut candidates = [0;3];
            for (index,path) in paths.into_iter().enumerate() {
                candidates[index] = self.capacity(path,&case,None)?;
            }
            let steady = matches!(case,Workload::W2 {connections:1,split} if split.to_peer==1 && split.to_application==1);
            if steady {self.capacity_checkpoint([candidates[1],candidates[2]],"measured calibration candidates")?;}
            let mut reference = vec![];
            let mut native = vec![];
            let mut bend = vec![];
            let mut completed = false;
            for refinement in 0..=n(&self.profile.raw,"qualification_refinements")? {
             reference.clear(); native.clear(); bend.clear();
             let mut failed = None;
             for pair in 0..5 {
                let order = if pair % 2 == 0 {
                    [PathKind::Reference, PathKind::Native, PathKind::Bend]
                } else {
                    [PathKind::Bend, PathKind::Native, PathKind::Reference]
                };
                let mut values = [0; 3];
                for path in order {
                    let index = match path {
                        PathKind::Reference => 0,
                        PathKind::Native => 1,
                        PathKind::Bend => 2,
                    };
                    let (t,e) = self.run_case(path,case.clone(),candidates[index],10,60)?;
                    let pass = capacity_gate(&t,&e,&self.profile,2000).is_ok();
                    self.record(&t,&e,if pass {"long capacity qualification passed"} else {"long capacity qualification failed"})?;
                    self.records.last_mut().ok_or("qualification record")?["refinement"] = json!(refinement);
                    if !pass {failed=Some(index);break;}
                    let split = case.dimensions().2;
                    values[index] = streams(&t,&e,1)?.iter().filter(|s|s.rate>0).map(|s| {
                        Ok(n(s.rx,"validated_in_window")? / 60 / if s.to_peer {split.to_peer} else {split.to_application})
                    }).collect::<Result<Vec<_>>>()?.into_iter().min().ok_or("active capacity stream")?;
                    self.records.last_mut().ok_or("qualification record")?["validated_capacity_rate"] = json!(values[index]);
                    save(&self.out.join("trials.json"),&json!(self.records))?;
                }
                if failed.is_some() {break;}
                reference.push(values[0]);
                native.push(values[1]);
                bend.push(values[2]);
             }
             if let Some(index) = failed {
                if refinement==n(&self.profile.raw,"qualification_refinements")? {return Ok(Verdict::Failed("long capacity failed after frozen refinement limit".into()));}
                candidates[index] = self.capacity(paths[index],&case,Some(candidates[index]))?;
                if steady {self.capacity_checkpoint([candidates[1],candidates[2]],"measured calibration candidates")?;}
             } else {completed=true;break;}
            }
            require(completed && reference.len()==5 && native.len()==5 && bend.len()==5,"five complete long qualification pairs")?;
            let ref_m = median(&reference);
            let native_m = median(&native);
            let bend_m = median(&bend);
            if steady {self.capacity_checkpoint([native_m,bend_m],"five long capacity trials passed; other phase gates pending")?;}
            if (reference.iter().max().unwrap() - reference.iter().min().unwrap()) as f64
                / ref_m as f64
                > self.profile.raw["reference_spread"]
                    .as_f64()
                    .ok_or("spread")?
            {
                return Ok(Verdict::Inconclusive(
                    "reference noise exceeds frozen 5%".into(),
                ));
            }
            let chosen = ref_m.max(native_m).max(bend_m);
            let headroom = (chosen * 5 + 3) / 4;
            let split = case.dimensions().2;
            let (t, e) = self.trial(Trial {
                path: PathKind::Reference,
                workload: case.clone(),
                rates: split.scaled(headroom),
                warm: 10,
                measure: 60,
                token: now(),
                start: 0,
                tcp: true,
                fault: false,
            })?;
            if capacity_gate(&t, &e, &self.profile, 2000).is_err() {
                return Ok(Verdict::Inconclusive(
                    "TCP generator/peer cannot prove 1.25x headroom".into(),
                ));
            }
            for s in streams(&t, &e, 1)? {
                if s.rate > 0 && p99(s.tx, "lateness_histogram_us", n(s.tx, "attempted")?)? > 2000 {
                    return Ok(Verdict::Inconclusive(
                        "generator lateness obscures headroom".into(),
                    ));
                }
            }
            self.record(&t, &e, "1.25x TCP headroom")?;
            let peer_ceiling = self.profile.raw["peer_cpu_fraction_ceiling"]
                .as_f64()
                .ok_or("peer CPU ceiling")?;
            if self.records.iter().any(|r| {
                r["verdict"] == "long capacity qualification passed"
                    && r["peer_cpu_fraction"]
                        .as_f64()
                        .is_some_and(|v| v > peer_ceiling)
            }) {
                return Ok(Verdict::Inconclusive(
                    "WebSocket peer CPU ceiling obscures capacity".into(),
                ));
            }
            let missing = self.limiter_evidence(&case, [ref_m, native_m, bend_m])?;
            if !missing.is_empty() {
                return Ok(Verdict::Inconclusive(missing.join("; ")));
            }
            for (a, b) in [(&reference, &native), (&native, &bend)] {
                let each = self.profile.raw["each_pair_ratio"]
                    .as_f64()
                    .ok_or("pair target")?;
                if median(b) as f64 / (median(a) as f64)
                    < self.profile.raw["median_ratio"]
                        .as_f64()
                        .ok_or("median target")?
                    || a.iter().zip(b).any(|(a, b)| *b as f64 / (*a as f64) < each)
                {
                    return Ok(Verdict::Failed("paired capacity ratio".into()));
                }
                let lower = a.iter().chain(b).copied().min().unwrap() / 2;
                for _ in 0..5 {
                    for path in if std::ptr::eq(a, &reference) {
                        [PathKind::Reference, PathKind::Native]
                    } else {
                        [PathKind::Native, PathKind::Bend]
                    } {
                        let (t, e) = self.run_case(path, case.clone(), lower, 10, 60)?;
                        let latency = if path == PathKind::Native { 1000 } else { 2000 };
                        capacity_gate(&t, &e, &self.profile, latency)?;
                        self.record(&t, &e, "half-load latency")?;
                    }
                }
            }
            capacities.push((case, ref_m, native_m, bend_m));
        }
        let steady=capacities.iter().find(|(c,_,_,_)|matches!(c,Workload::W2 {connections:1,split} if split.to_peer==1&&split.to_application==1)).ok_or("soak W2 capacity")?;
        let native = steady.2;
        let bend = steady.3;
        for (path, rate) in [(PathKind::Native, native), (PathKind::Bend, bend)] {
            for pause in [10, 100, 1000] {
                for receiver in [0, 1] {
                    for _ in 0..100 {
                        self.scripted(path, pause, receiver, rate * 5, false)?;
                    }
                }
            }
            self.soak(path, rate / 2)?;
        }
        for bytes in [1024, 16384] {
            let (t, e) = self.run_case(
                PathKind::Native,
                Workload::W1 {
                    bytes,
                    split: Rates {
                        to_peer: 1,
                        to_application: 0,
                    },
                },
                1000,
                10,
                60,
            )?;
            self.record(&t, &e, "diagnostic only")?;
        }
        let mut reasons = vec![];
        for path in [PathKind::Native, PathKind::Bend] {
            self.partial_expiry(path)?;
        }
        for path in ["Native", "Bend"] {
            if !self
                .records
                .iter()
                .any(|r| r["path"] == path && r["outgoing_partial_expiry_ns"].as_u64().is_some())
            {
                reasons.push(format!(
                    "{path}: finite overload did not expose an outgoing partial-frame expiry"
                ));
            }
        }
        if !reasons.is_empty() {
            return Ok(Verdict::Inconclusive(reasons.join("; ")));
        }
        Ok(Verdict::Passed)
    }
    fn limiter_evidence(
        &mut self,
        workload: &Workload,
        capacities: [u64; 3],
    ) -> Result<Vec<String>> {
        let mut missing = vec![];
        let mut diagnostics = vec![];
        for tool in ["perf", "perf-stat", "strace"] {
            for (index, path) in [PathKind::Reference, PathKind::Native, PathKind::Bend]
                .into_iter()
                .enumerate()
            {
                self.diagnostic = Some(tool);
                let offered = if tool == "strace" {capacities[index].min(1000)} else {capacities[index]};
                let run = self.run_case(path, workload.clone(), offered, 0, 5);
                self.diagnostic = None;
                match run {
                    Ok((t, e)) => {
                        if let Err(error) = correctness(&t, &e, &self.profile, false) {
                            missing.push(format!("{tool} {path:?}: {error}"));
                        }
                        let labels = (0..e.clients.len()).map(|id|format!("client-{id}")).chain(["peer".into()]);
                        for label in labels {
                          if tool == "perf" {
                            let data = e.directory.join(format!("{label}.perf.data"));
                            let mut command = Command::new(env::var_os("WS_PHASE5_PERF").unwrap_or_else(|| "perf".into()));
                            command.args(["report", "--stdio", "--no-children", "--percent-limit", "0", "-i"]).arg(&data);
                            let output = self.build_command(command,&format!("perf-report-{}-{label}",self.next_trial));
                            match output {
                                Ok(text) => {fs::write(e.directory.join(format!("{label}-cpu-profile.txt")),text)?;}
                                _ => missing.push(format!("{path:?} {label}: CPU profile unreadable")),
                            }
                          } else if tool == "strace" {
                            let trace = e.directory.join(format!("{label}.strace.txt"));
                            if !fs::read_to_string(trace).is_ok_and(|s| s.contains("total")) {
                                missing.push(format!(
                                    "{path:?} {label}: syscall/wakeup trace unavailable"
                                ));
                            }
                          } else {
                            let counters = e.directory.join(format!("{label}.perf-stat.txt"));
                            if !fs::read_to_string(counters).is_ok_and(|s| s.contains("task-clock") && s.contains("context-switches") && !s.contains("<not supported>") && !s.contains("<not counted>")) {
                                missing.push(format!("{path:?} {label}: CPU/context-switch counters unavailable"));
                            }
                          }
                        }
                        self.record(&t, &e, "limiter diagnostic, not a capacity sample")?;
                        diagnostics.push(json!({"path":format!("{path:?}"),"tool":tool,"directory":e.directory,"actors":e.clients.len()+1}));
                    }
                    Err(error) => missing.push(format!("{tool} {path:?}: {error}")),
                }
            }
        }
        let case = format!("{workload:?}");
        let assessment_path = env::var_os("WS_PHASE5_ASSESSMENT").map(PathBuf::from).unwrap_or_else(|| if self.profile.raw["name"] == "local-perf-v2" {self.out.join("limiter-assessments.json")} else {self.root.join("bench/ws/profiles/local-perf-v1-limiter-assessments.json")});
        let assessments: Value = fs::read(&assessment_path).ok().and_then(|b|serde_json::from_slice(&b).ok()).unwrap_or(Value::Null);
        let assessment = assessments.as_array().and_then(|rows| rows.iter().find(|r|r["workload"] == case));
        let assessed = assessment.is_some_and(|a| {
            a["capacities"] == json!(capacities) && a["profile"] == self.profile.raw
                && a["actors"].as_array().is_some_and(|actors| {
                    [PathKind::Reference,PathKind::Native,PathKind::Bend].into_iter().all(|path| {
                        let count = if path == PathKind::Reference {1} else {workload.dimensions().1};
                        (0..count).map(|id|format!("client-{id}")).chain(["peer".into()]).all(|label| {
                            actors.iter().any(|actor| actor["path"] == format!("{path:?}") && actor["actor"] == label
                                && ["limiter","why_not_double","source","ruled_out_generator_and_peer"].iter().all(|k|actor[k].as_str().is_some_and(|s|!s.trim().is_empty()))
                                && actor["evidence"].as_array().is_some_and(|refs| {
                                    ["profile","counters","syscalls","actor-report"].iter().all(|kind|refs.iter().any(|r|r["kind"] == *kind && r["file"].as_str().is_some_and(|file| {
                                        let file = self.root.join(file);
                                        let bound = diagnostics.iter().any(|diagnostic| {
                                            if diagnostic["path"] != format!("{path:?}") { return false; }
                                            let (tool, name) = match *kind {
                                                "profile" => ("perf", format!("{label}-cpu-profile.txt")),
                                                "counters" => ("perf-stat", format!("{label}.perf-stat.txt")),
                                                "syscalls" => ("strace", format!("{label}.strace.txt")),
                                                _ => ("perf", format!("{label}.json")),
                                            };
                                            if diagnostic["tool"] != tool { return false; }
                                            diagnostic["directory"].as_str().is_some_and(|directory| {
                                                let expected = Path::new(directory).join(name);
                                                match (file.canonicalize(), expected.canonicalize()) {
                                                    (Ok(actual), Ok(expected)) => actual == expected,
                                                    _ => false,
                                                }
                                            })
                                        });
                                        bound && fs::read_to_string(file).is_ok_and(|text| r["observation"].as_str().is_some_and(|observation|!observation.trim().is_empty()&&text.contains(observation)))
                                    })))
                                }))
                        })
                    })
                })
        });
        if !assessed {missing.push(format!("{case}: missing local evidence-backed limiter assessment in {}",assessment_path.display()));}
        let artifact_hashes = bounded::diagnostic_hashes(&diagnostics)?;
        let report = json!({"workload":case,"capacities":capacities,"profile":self.profile.raw,"missing":missing,"diagnostics":diagnostics,"artifact_hashes":artifact_hashes,"assessment":assessment,"assessment_valid":assessed,"phase_pass":false});
        save(&self.out.join(format!("limiter-case-{:04}.json",self.next_trial)), &report)?;
        save(&self.out.join("limiter.json"), &report)?;
        Ok(missing)
    }
}
fn median(xs: &[u64]) -> u64 {
    let mut v = xs.to_vec();
    v.sort();
    v[v.len() / 2]
}
#[derive(Default)]
struct PeerFacts {
    attempted: u64,
    missed: u64,
    admitted: u64,
    transmitted: u64,
    validated: u64,
    rejected: u64,
    digest: u64,
    in_window: u64,
    latency: Vec<u64>,
    lateness: Vec<u64>,
    identities: Vec<[u64; 2]>,
    receipts: Vec<u64>,
    last_receipt: u64,
    last_offer: u64,
}
impl PeerFacts {
    fn new() -> Self {
        Self {
            latency: vec![0; 10001],
            lateness: vec![0; 10001],
            ..Self::default()
        }
    }
    fn identity(&mut self, seq: u64) -> Result<()> {
        if let Some(last) = self.identities.last_mut() {
            if last[1] == seq {
                last[1] = seq + 1;
                return Ok(());
            }
        }
        require(
            self.identities.len() < 4096,
            "bounded identity range storage exhausted",
        )?;
        self.identities.push([seq, seq + 1]);
        Ok(())
    }
    fn value(&self, count: u64) -> Value {
        json!({"scheduled":count,"generator_missed":self.missed,"attempted":self.attempted,"admitted":self.admitted,"transmitted":self.transmitted,"validated":self.validated,"validated_in_window":self.in_window,"validated_after_window":self.validated-self.in_window,"rejected":self.rejected,"digest":self.digest.to_string(),"latency_histogram_us":self.latency,"lateness_histogram_us":self.lateness,"samples":[],"identities":self.identities,"receipts":self.receipts,"last_receipt_ns":self.last_receipt,"last_offer_ns":self.last_offer})
    }
}
fn payload(size: usize, token: u64, epoch: u64, dir: u64, id: u64, seq: u64, due: u64) -> Vec<u8> {
    let mut p = vec![0; size];
    for (i, x) in [1, token, epoch, dir, id, seq, due].iter().enumerate() {
        p[i * 8..i * 8 + 8].copy_from_slice(&x.to_be_bytes())
    }
    for (i, b) in p.iter_mut().enumerate().skip(56) {
        *b = token.wrapping_add(seq * 17 + i as u64 * 31 + dir * 7 + id * 13 + epoch * 19) as u8
    }
    p
}
fn peer_error(operation: &str, error: &tungstenite::Error) -> Value {
    match error {
        tungstenite::Error::Io(e) => json!({"operation":operation,"kind":"io","io_kind":format!("{:?}",e.kind()),"os_code":e.raw_os_error(),"message":e.to_string(),"timestamp_ns":now()}),
        _ => json!({"operation":operation,"kind":format!("{error:?}"),"message":error.to_string(),"timestamp_ns":now()}),
    }
}
impl Controller {
    fn partial_expiry(&mut self, path: PathKind) -> Result<()> {
        let out = self.out.join(format!("trial-{:04}", self.next_trial));
        self.next_trial += 1;
        fs::create_dir(&out)?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let mut t = Trial {
            path,
            workload: Workload::W2 {
                connections: 1,
                split: Rates {
                    to_peer: 1,
                    to_application: 1,
                },
            },
            rates: Rates {
                to_peer: 100,
                to_application: 100,
            },
            warm: 0,
            measure: 1,
            token: now(),
            start: 0,
            tcp: false,
            fault: false,
        };
        let (send, recv) = std::sync::mpsc::channel();
        let token = t.token;
        let peer = thread::spawn(move || -> Result<Value> {
            listener.set_nonblocking(true)?;
            let deadline = Instant::now() + Duration::from_secs(5);
            let stream = loop {
                match listener.accept() {
                    Ok((s, _)) => break s,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        require(Instant::now() < deadline, "partial peer accept")?;
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(e) => return Err(e.into()),
                }
            };
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            stream.set_write_timeout(Some(Duration::from_secs(2)))?;
            let mut ws = tungstenite::accept(stream)?;
            let start: u64 = recv.recv_timeout(Duration::from_secs(5))?;
            while now() < start {
                thread::sleep(Duration::from_micros(50));
            }
            let partial_start = now();
            let mut frame = vec![0x82, 126, 1, 0];
            frame.extend_from_slice(&payload(256, token, 1, 1, 0, 0, start)[..100]);
            ws.get_mut().write_all(&frame)?;
            let mut progress = 0;
            while now() < partial_start + 300_000_000 {
                match ws.read() {
                    Ok(Message::Binary(p)) => {
                        require(p.len() == 256, "partial opposite length")?;
                        let seq = u64::from_be_bytes(p[40..48].try_into()?);
                        require(
                            p.as_ref() == payload(256, token, 1, 0, 0, seq, start + seq * NS / 100),
                            "partial opposite integrity",
                        )?;
                        progress += 1;
                    }
                    Ok(Message::Close(_)) => {
                        let _ = ws.flush();
                        break;
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
            Ok(
                json!({"partial_start_ns":partial_start,"incomplete_frame_payload_bytes":100,"declared_frame_bytes":256,"opposite_validated":progress}),
            )
        });
        let mut actor = self.spawn(
            self.actor_command(&t, port, 0, &out.join("client-0.json")),
            &out,
            "client-0",
            Some(self.profile.cpus[0]),
        )?;
        actor.ready("READY")?;
        t.start = now() + 100_000_000;
        send.send(t.start)?;
        actor.barrier(t.start)?;
        let status = actor.wait(Instant::now() + Duration::from_secs(4))?;
        let peer = peer.join().map_err(|_| "partial peer panic")??;
        save(&out.join("peer.json"), &peer)?;
        let client: Value = serde_json::from_slice(&fs::read(out.join("client-0.json"))?)?;
        require(
            status == 1 && client["failure"]["code"] == 16,
            "directional incoming partial timeout",
        )?;
        let delta = n(&client, "terminal_ns")?
            .checked_sub(n(&peer, "partial_start_ns")?)
            .ok_or("partial clock")?;
        require(
            (240_000_000..=260_000_000).contains(&delta) && n(&peer, "opposite_validated")? > 0,
            "partial deadline/service allowance and opposite progress",
        )?;
        let r = &client["resources"];
        require(
            n(r, "native_live")? == 0
                 && n(r, "bend_live")? == n(&client, "bend_baseline")?
                && r["threads_restored"] == true
                && r["fds_restored"] == true,
            "partial timeout cleanup",
        )?;
        self.records.push(json!({"path":format!("{path:?}"),"workload":"directional partial expiry","evidence":out,"incoming_partial_expiry_ns":delta,"opposite_progress":peer["opposite_validated"]}));
        save(&self.out.join("trials.json"), &json!(self.records))
    }
    fn soak(&mut self, path: PathKind, rate: u64) -> Result<()> {
        let out = self.out.join(format!("soak-{path:?}"));
        fs::create_dir(&out)?;
        let stop = Arc::new(AtomicBool::new(false));
        let mut actor = Self {
            root: self.root.clone(),
            cache: self.cache.clone(),
            out,
            profile: self.profile.clone(),
            peer: self.peer.clone(),
            native: self.native.clone(),
            bend: self.bend.clone(),
            next_trial: 0,
            records: vec![],
            diagnostic: None,
            qualification_running: true,
            build_dir: self.build_dir.clone(),
            _cpu_lease: None,
            stop: Some(stop.clone()),
            campaign_deadline: self.campaign_deadline,
        };
        let steady = thread::spawn(move || -> Result<(Trial, Evidence)> {
            let seconds = n(&actor.profile.raw,"w5_seconds")?;
            let (t, e) = actor.run_case(path, Workload::W5, rate, 0, seconds)?;
            capacity_gate(&t, &e, &actor.profile, n(&actor.profile.raw, "capacity_p99_us")?)?;
            actor.record(&t, &e, "continuous profile-duration half-load actor")?;
            Ok((t, e))
        });
        let cycles = n(&self.profile.raw,"w5_seconds")? / n(&self.profile.raw,"w5_lifecycle_interval_s")?;
        let lifecycle = self.lifecycle(path, rate.max(50000), cycles as usize, n(&self.profile.raw,"w5_lifecycle_interval_s")?);
        if lifecycle.is_err() {stop.store(true,Ordering::Release);}
        let result = steady.join().map_err(|_| "soak actor thread panic")?;
        lifecycle?;
        let (t, e) = result?;
        self.record(
            &t,
            &e,
            "profile-duration stability with concurrent lifecycle on same endpoint CPU",
        )?;
        Ok(())
    }
    fn lifecycle(&mut self, path: PathKind, rate: u64, cycles: usize, interval: u64) -> Result<()> {
        let out = self.out.join(format!("lifecycle-{path:?}-{}", self.next_trial));
        fs::create_dir(&out)?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let t = Trial {path,workload:Workload::W4 {pause_ms:100,receiver:0},rates:Rates {to_peer:rate,to_application:rate},warm:0,measure:1,token:now(),start:0,tcp:false,fault:false};
        let mut command = self.actor_command(&t, listener.local_addr()?.port(), 0, &out.join("client.json"));
        command.env("WS_CYCLES", (cycles * 2).to_string()).env("WS_PEER_ADMITTED", out.join("peer-admitted"));
        let actor = self.spawn(command, &out, "persistent-client", Some(self.profile.cpus[0]))?;
        let mut state = Lifecycle {actor,listener,out:out.clone(),cycle:0};
        let start = Instant::now();
        for cycle in 0..cycles {
            thread::sleep((start + Duration::from_secs(cycle as u64 * interval)).saturating_duration_since(Instant::now()));
            for disconnect in [false,true] {
                let session_rate = if disconnect {rate} else {n(&self.profile.raw,"routine_steady_rate")?.min(rate)};
                self.scripted_session(path, 100, 0, session_rate, disconnect, Some((&mut state,t.token)))?;
                state.cycle += 1;
            }
        }
        require(state.actor.wait(Instant::now()+Duration::from_secs(2))? == 0, "persistent lifecycle exit")?;
        let reports = (0..cycles*2).map(|i| -> Result<Value> {Ok(serde_json::from_slice(&fs::read(out.join(format!("client.json.{i}")))?)?)}).collect::<Result<Vec<_>>>()?;
        let baseline = n(&reports[0], "c_baseline")?;
        let bend_baseline = n(&reports[0],"bend_baseline")?;
        for (cycle,report) in reports.iter().enumerate() { require(n(report,"pid")? == state.actor.child.id() as u64 && n(report,"cycle")? == cycle as u64 && n(report,"c_baseline")? == baseline && n(&report["resources"],"c_live")? == baseline && n(&report["resources"],"native_live")? == 0 && n(report,"bend_baseline")? == bend_baseline && n(&report["resources"],"bend_live")? == bend_baseline, "persistent idle allocation baseline")?; }
        save(&out.join("cycles.json"), &json!({"pid":state.actor.child.id(),"cycles":cycles,"sessions":reports,"same_process":true,"phase_pass":false}))
    }
    fn scripted(
        &mut self,
        path: PathKind,
        pause_ms: u64,
        receiver: usize,
        rate: u64,
        disconnect: bool,
    ) -> Result<()> {
        self.scripted_session(path,pause_ms,receiver,rate,disconnect,None)
    }
    fn scripted_session(&mut self,path:PathKind,pause_ms:u64,receiver:usize,rate:u64,disconnect:bool,mut lifecycle:Option<(&mut Lifecycle,u64)>) -> Result<()> {
        let out = self.out.join(format!("trial-{:04}", self.next_trial));
        self.next_trial += 1;
        fs::create_dir(&out)?;
        let listener = if let Some((state,_)) = &lifecycle {state.listener.try_clone()?} else {TcpListener::bind("127.0.0.1:0")?};
        let port = listener.local_addr()?.port();
        let mut t = Trial {
            path,
            workload: Workload::W4 { pause_ms, receiver },
            rates: Rates {
                to_peer: rate,
                to_application: rate,
            },
            warm: 0,
            measure: 1,
            token: lifecycle.as_ref().map_or_else(now, |(_,token)|*token),
            start: 0,
            tcp: false,
            fault: false,
        };
        let (send, recv) = std::sync::mpsc::channel::<u64>();
        let token = t.token;
        let cpu = self.profile.cpus[1];
        let peer_out = out.clone();
        let completion = lifecycle.as_ref().map_or_else(||out.join("peer-admitted"), |(state,_)|state.out.join("peer-admitted"));
        let actor_report = lifecycle.as_ref().map_or_else(||out.join("client-0.json"), |(state,_)|state.out.join(format!("client.json.{}",state.cycle)));
        let peer = thread::spawn(move || -> Result<Value> {
            let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
            unsafe { libc::CPU_SET(cpu, &mut mask) };
            require(
                unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask) } == 0,
                "peer affinity",
            )?;
            listener.set_nonblocking(true)?;
            let deadline = Instant::now() + Duration::from_secs(5);
            let stream = loop {
                match listener.accept() {
                    Ok((s, _)) => break s,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        require(Instant::now() < deadline, "scripted accept deadline")?;
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(e) => return Err(e.into()),
                }
            };
            stream.set_nodelay(true)?;
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            stream.set_write_timeout(Some(Duration::from_secs(2)))?;
            let value = 16384i32;
            for opt in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
                require(
                    unsafe {
                        libc::setsockopt(
                            stream.as_raw_fd(),
                            libc::SOL_SOCKET,
                            opt,
                            &value as *const _ as *const _,
                            4,
                        )
                    } == 0,
                    "scripted buffers",
                )?;
            }
            let config = WebSocketConfig::default()
                .write_buffer_size(0)
                .max_write_buffer_size(2097152)
                .max_frame_size(Some(1048576))
                .max_message_size(Some(1048576));
            let mut ws = tungstenite::accept_with_config(stream, Some(config))?;
            ws.get_ref().set_nonblocking(true)?;
            let start = recv.recv_timeout(Duration::from_secs(5))?;
            let count = (rate * 100_000_000 + NS - 1) / NS;
            let end = start + NS;
            let resume = start + pause_ms * 1_000_000;
            let mut tx = PeerFacts::new();
            let mut rx = PeerFacts::new();
            let mut pending = 0u64;
            let mut high = 0;
            let mut blocked = 0;
            let mut closed = false;
            let mut terminal = false;
            let mut failure = Value::Null;
            let mut completion_sent = false;
            while now() < end + 2 * NS {
                let tick = now();
                if tick < start {
                    thread::sleep(Duration::from_micros(50));
                    continue;
                }
                if disconnect && tick >= start + 100_000_000 {
                    terminal = true;
                    failure = json!({"kind":"injected disconnect","operation":"disconnect","timestamp_ns":tick});
                    break;
                }
                if tick >= start + 100_000_000 && !completion_sent {
                    tx.missed = count - tx.attempted;
                    let temporary = completion.with_extension("tmp");
                    fs::write(&temporary, format!("{start} {}\n",tx.admitted))?;
                    fs::rename(temporary,&completion)?;
                    completion_sent = true;
                }
                match ws.flush() {
                    Ok(()) => {
                        tx.transmitted += pending;
                        pending = 0
                    }
                    Err(tungstenite::Error::Io(e))
                        if e.kind() == std::io::ErrorKind::WouldBlock =>
                    {
                        blocked += 1
                    }
                    Err(error) => {
                        terminal = true;
                        failure = peer_error("flush", &error);
                        break;
                    }
                }
                for _ in 0..32 {
                    if now() >= start + 100_000_000 || tx.attempted >= count || start + tx.attempted * NS / rate > tick {
                        break;
                    }
                     let seq = tx.attempted;
                     let due = start + seq * NS / rate;
                     let bytes = payload(256, token, 1, 1, 0, seq, due);
                     let offered = now();
                     if offered >= start + 100_000_000 { break; }
                     tx.attempted += 1;
                     tx.lateness[((offered - due + 999) / 1000).min(10000) as usize] += 1;
                     if pending >= 4096 || (pending + 1) * 270 > 2097152 {
                        tx.rejected += 1;
                         continue;
                     }
                     tx.last_offer = offered;
                    match ws.write(Message::Binary(
                         bytes.into(),
                    )) {
                        Ok(()) => {}
                        Err(tungstenite::Error::Io(e))
                            if e.kind() == std::io::ErrorKind::WouldBlock =>
                        {
                            blocked += 1
                        }
                        Err(tungstenite::Error::WriteBufferFull(_)) => {
                            tx.rejected += 1;
                            continue;
                        }
                        Err(error) => {
                            tx.rejected += 1;
                            terminal = true;
                            failure = peer_error("write", &error);
                            break;
                        }
                    }
                    tx.admitted += 1;
                    tx.digest = tx.digest.wrapping_mul(31).wrapping_add(seq + 1);
                    tx.identity(seq)?;
                    pending += 1;
                    high = high.max(pending * 270);
                    match ws.flush() {
                        Ok(()) => {
                            tx.transmitted += pending;
                            pending = 0
                        }
                        Err(tungstenite::Error::Io(e))
                            if e.kind() == std::io::ErrorKind::WouldBlock =>
                        {
                            blocked += 1
                        }
                        Err(error) => {
                            terminal = true;
                            failure = peer_error("flush", &error);
                            break;
                        }
                    }
                }
                if receiver != 0 || tick >= resume {
                    for _ in 0..32 {
                        match ws.read() {
                            Ok(Message::Binary(p)) => {
                                require(p.len() == 256, "scripted length")?;
                                let seq = u64::from_be_bytes(p[40..48].try_into()?);
                                let due = start + seq * NS / rate;
                                require(
                                    p.as_ref() == payload(256, token, 1, 0, 0, seq, due),
                                    "scripted full-byte integrity",
                                )?;
                                require(
                                    rx.identities.last().is_none_or(|r| seq >= r[1]),
                                    "scripted duplicate",
                                )?;
                                rx.identity(seq)?;
                                rx.validated += 1;
                                rx.digest = rx.digest.wrapping_mul(31).wrapping_add(seq + 1);
                                let receipt = now();
                                rx.last_receipt = receipt;
                                if receipt < end {
                                    rx.in_window += 1
                                }
                                rx.latency[((receipt - due + 999) / 1000).min(10000) as usize] += 1;
                                if rx.receipts.len() < 4096 {
                                    rx.receipts.push(receipt)
                                }
                            }
                            Ok(Message::Close(_)) => {
                                let _ = ws.flush();
                                closed = true;
                                break;
                            }
                            Err(tungstenite::Error::Io(e))
                                if e.kind() == std::io::ErrorKind::WouldBlock =>
                            {
                                break
                            }
                            Err(tungstenite::Error::ConnectionClosed) => {
                                closed = true;
                                break;
                            }
                            Err(error) => {
                                terminal = true;
                                failure = peer_error("read", &error);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                if closed || terminal {
                    break;
                }
                thread::sleep(Duration::from_micros(50));
            }
            tx.missed = count - tx.attempted;
            let empty = PeerFacts::new();
            let report = json!({"schema":1,"actor":"server","token":token,"start_ns":start,"end_ns":end,"finished_ns":now(),"queue_limit_bytes":2097152,"queue_limit_descriptors":4096,"failure":failure,"partial_write_observations":blocked,"resume_ns":resume,"terminal":terminal,"closed":closed,"streams":[{"connection":0,"epoch":0,"queue_highwater_bytes":high,"queue_highwater_descriptors":high/270,"sndbuf":32768,"rcvbuf":32768,"tx":empty.value(0),"rx":empty.value(0)},{"connection":0,"epoch":1,"queue_highwater_bytes":high,"queue_highwater_descriptors":high/270,"sndbuf":32768,"rcvbuf":32768,"tx":tx.value(count),"rx":rx.value(count)}]});
            save(&peer_out.join("peer.json"), &report)?;
            Ok(report)
        });
        let mut command = self.actor_command(&t, port, 0, &actor_report);
        command.env("WS_PEER_ADMITTED", out.join("peer-admitted"));
        let mut local_actor = if lifecycle.is_none() {Some(self.spawn(
            command,
            &out,
            "client-0",
            Some(self.profile.cpus[0]),
        )?)} else {None};
        let persistent = lifecycle.is_some();
        let actor = if let Some((state,_)) = &mut lifecycle {&mut state.actor} else {local_actor.as_mut().unwrap()};
        actor.ready("READY")?;
        t.start = now() + 100_000_000;
        send.send(t.start)?;
        if persistent {
            let receive_pause = if disconnect {1000} else if receiver == 1 {pause_ms} else {0};
            writeln!(actor.child.stdin.as_mut().ok_or("stdin")?, "{} {} {}", t.start, rate, receive_pause)?;
        } else {actor.barrier(t.start)?;}
        let deadline = Instant::now() + Duration::from_secs(5);
        let status = if !persistent {actor.wait(deadline)?} else {
            while !actor_report.exists() { require(Instant::now()<deadline,"persistent cycle report deadline")?;thread::sleep(Duration::from_millis(2)); }
            0
        };
        let report = peer.join().map_err(|_| "scripted peer panic")??;
        let client: Value = serde_json::from_slice(&fs::read(&actor_report)?)?;
        save(&out.join("client-0.json"), &client)?;
        let e = Evidence {
            clients: vec![client],
            peer: report,
            statuses: vec![status, 0],
            directory: out,
        };
        correctness(&t, &e, &self.profile, true)?;
        require(n(&e.clients[0],"last_offer_ns")? < t.start + 100_000_000 && n(&row(&e.peer,0,1)?["tx"],"last_offer_ns")? < t.start + 100_000_000, "actual offer crossed finite burst boundary")?;
        let terminal_code = e.clients[0]["failure"]["code"].as_u64();
        if persistent && !disconnect {
            require(e.clients[0]["failure"].is_null() && e.peer["failure"].is_null(), "persistent normal-close cycle failed")?;
        }
        if matches!(terminal_code,Some(16|17|22)) {
            require(n(&e.clients[0],"terminal_ns")? >= t.start, "missing genuine worker terminal observation")?;
        }
        require(
            e.clients[0]["failure"].is_null()
                || matches!(terminal_code, Some(16 | 17))
                || (disconnect && terminal_code == Some(22)),
            "unexpected overload fault",
        )?;
        if disconnect {
            require(
                n(&e.clients[0], "worker_tx_failed")? > 0
                    || n(&e.clients[0], "terminal_rx_queue_count")? > 0,
                "disconnect injection did not catch queued application data",
            )?;
        }
        if !disconnect {
            require(e.peer["terminal"] != true || matches!(terminal_code,Some(16|17)), "generic peer transport error is not an accepted terminal outcome")?;
            let drain_deadline = t.start + 100_000_000u64.max(pause_ms * 1_000_000) + 2 * NS;
            require(
                n(&e.clients[0], "rx_last_ns")? <= drain_deadline,
                "Bend/native application drain deadline",
            )?;
            let receipts = row(&e.peer, 0, 1)?["rx"]["receipts"]
                .as_array()
                .ok_or("peer receipts")?;
            require(
                n(&row(&e.peer,0,1)?["rx"],"last_receipt_ns")? <= drain_deadline,
                "peer drain deadline",
            )?;
            if receiver == 0 {
                require(
                    n(&e.clients[0], "pause_rx_validated")? > 0,
                    "opposite application consumption during peer pause",
                )?;
            } else {
                require(
                    receipts.iter().any(|v| {
                        v.as_u64()
                            .is_some_and(|v| v >= t.start && v < t.start + pause_ms * 1_000_000)
                    }),
                    "opposite peer validation during application pause",
                )?;
            }
        }
        let mut terminal = vec![];
        for (dir, s) in streams(&t, &e, 1)?.iter().enumerate() {
            let admitted = n(s.tx, "admitted")?;
            let validated = n(s.rx, "validated")?;
            let remaining = admitted - validated;
            require(
                remaining == 0
                    || disconnect
                    || matches!(terminal_code,Some(16|17)),
                "unexplained terminal identities",
            )?;
            let admitted_ranges = identity_ranges(s.tx, admitted)?;
            let validated_ranges = identity_ranges(s.rx, validated)?;
            let terminal_ranges = difference(&admitted_ranges, &validated_ranges)?;
            require(
                terminal_ranges.iter().map(|[a, b]| b - a).sum::<u64>() == remaining,
                "terminal identity partition",
            )?;
            terminal.push(json!({"direction":dir,"admitted":admitted_ranges,"validated":validated_ranges,"terminal":terminal_ranges,"terminal_count":remaining}));
        }
        for s in streams(&t,&e,1)? {
            let missed = n(s.tx,"generator_missed")?;
            require(n(s.tx,"attempted")? + missed == n(s.tx,"scheduled")? || disconnect || matches!(terminal_code,Some(16|17)), "burst schedule partition")?;
            if self.qualification_running && !disconnect && missed > 0 {return Err("inconclusive: 100ms burst generator missed scheduled arrivals".into());}
        }
        save(
            &e.directory.join("terminal-accounting.json"),
            &json!({"injected_disconnect":disconnect,"worker_terminal":e.clients[0]["failure"],"worker_terminal_ns":e.clients[0]["terminal_ns"],"peer_failure":e.peer["failure"],"streams":terminal,"native_restored":true,"phase_pass":false}),
        )?;
        self.record(
            &t,
            &e,
            if disconnect {
                "bounded queued disconnect"
            } else {
                "finite scheduled overload/pause/drain"
            },
        )?;
        let code = e.clients[0]["failure"]["code"].as_u64();
        let mut expiry = Value::Null;
        if code == Some(17) {
            let start = n(&e.clients[0], "outgoing_partial_start_ns")?;
            require(start > 0, "missing outgoing partial start")?;
            let elapsed = n(&e.clients[0], "terminal_ns")?
                .checked_sub(start)
                .ok_or("outgoing partial clock")?;
            require(
                (240_000_000..=260_000_000).contains(&elapsed),
                "outgoing partial expiry/service allowance",
            )?;
            expiry = json!(elapsed);
        }
        let last = self.records.last_mut().ok_or("trial record")?;
        last["partial_write_observations"] = e.clients[0]["partial_write_observations"].clone();
        last["outgoing_partial_expiry_ns"] = expiry;
        save(&self.out.join("trials.json"), &json!(self.records))
    }
}
fn identity_ranges(v: &Value, count: u64) -> Result<Vec<[u64; 2]>> {
    let ranges = v["identities"]
        .as_array()
        .ok_or("missing identity ranges")?;
    require(ranges.len() <= 4096, "identity storage bound")?;
    let mut out = vec![];
    let mut prior = 0;
    for r in ranges {
        let a = r[0].as_u64().ok_or("range begin")?;
        let b = r[1].as_u64().ok_or("range end")?;
        require(a >= prior && b > a, "overlapping identity ranges")?;
        prior = b;
        out.push([a, b]);
    }
    require(
        out.iter().map(|[a, b]| b - a).sum::<u64>() == count,
        "identity range count",
    )?;
    Ok(out)
}
fn difference(admitted: &[[u64; 2]], validated: &[[u64; 2]]) -> Result<Vec<[u64; 2]>> {
    let mut terminal = vec![];
    let mut j = 0;
    for &[begin, end] in admitted {
        let mut at = begin;
        while j < validated.len() && validated[j][0] < end {
            let [a, b] = validated[j];
            require(a >= at && b <= end, "validated identity was not admitted")?;
            if a > at {
                terminal.push([at, a]);
            }
            at = b;
            j += 1;
        }
        if at < end {
            terminal.push([at, end]);
        }
    }
    require(j == validated.len(), "unknown validated identity")?;
    Ok(terminal)
}
fn replace(c: &mut String, from: &str, to: &str) -> Result<()> {
    require(
        c.matches(from).count() == 1,
        &format!("compiler instrumentation anchor {from}"),
    )?;
    *c = c.replacen(from, to, 1);
    Ok(())
}
fn instrument(mut c: String) -> Result<String> {
    replace(&mut c, "static Term ws_offer_run(Env e, Term *f, IoWork *w) {", "static int p5_offer_open(void);\nstatic Term ws_offer_run(Env e, Term *f, IoWork *w) {")?;
    replace(&mut c, "if (code == WS_OK) a = ws_socket_offer_buffer(io_hand_v(f[0]), bytes, f[2]);", "if (code == WS_OK && p5_offer_open()) a = ws_socket_offer_buffer(io_hand_v(f[0]), bytes, f[2]);")?;
    let hooks = r#"
static _Atomic uint64_t ws_bend_blocks,ws_bend_bytes,ws_bend_peak,ws_bend_allocations;
static unsigned char ws_bend_owned[1u<<24];
static uint64_t ws_track_bend(Env e,uint32_t cls,uint64_t loc) {
  if(err_peek(e.mem))return loc;uint64_t i=loc-HEAP_OFF;if(i>=(1u<<24)||ws_bend_owned[i])abort();
  ws_bend_owned[i]=cls+1;atomic_fetch_add(&ws_bend_blocks,1);atomic_fetch_add(&ws_bend_allocations,1);uint64_t n=atomic_fetch_add(&ws_bend_bytes,8ull<<cls)+(8ull<<cls);
  if(n>atomic_load(&ws_bend_peak))atomic_store(&ws_bend_peak,n);return loc;
}
static void ws_free_bend(uint32_t cls,uint64_t loc) {uint64_t i=loc-HEAP_OFF;if(i>=(1u<<24)||ws_bend_owned[i]!=cls+1)abort();ws_bend_owned[i]=0;atomic_fetch_sub(&ws_bend_blocks,1);atomic_fetch_sub(&ws_bend_bytes,8ull<<cls);}
"#;
    replace(
        &mut c,
        "INLINE u64 heap_alloc(Env e, u32 cls) {",
        &format!("{hooks}\nINLINE u64 heap_alloc(Env e, u32 cls) {{"),
    )?;
    replace(&mut c,"    return h;\n  }\n  return heap_alloc_miss(e, cls);","    return ws_track_bend(e, cls, h);\n  }\n  return ws_track_bend(e, cls, heap_alloc_miss(e, cls));")?;
    replace(&mut c,"INLINE void heap_free(Env e, u32 cls, u64 loc) {\n  if (err_peek(e.mem)) {\n    return;\n  }","INLINE void heap_free(Env e, u32 cls, u64 loc) {\n  if (err_peek(e.mem)) {\n    return;\n  }\n  ws_free_bend(cls,loc);")?;
    let hooks = r#"
static uint64_t ws_c_bytes,ws_c_peak,ws_c_allocations;
static void *ws_malloc(size_t n) {size_t*p=malloc(n+sizeof(size_t));if(!p)return NULL;*p=n;ws_c_bytes+=n;ws_c_allocations++;if(ws_c_bytes>ws_c_peak)ws_c_peak=ws_c_bytes;return p+1;}
static void *ws_calloc(size_t n,size_t size) {void*p=ws_malloc(n*size);if(p)memset(p,0,n*size);return p;}
static void ws_free(void*p) {if(p){size_t*q=(size_t*)p-1;ws_c_bytes-=*q;free(q);}}
#define malloc ws_malloc
#define calloc ws_calloc
#define free ws_free
"#;
    replace(&mut c,"typedef struct { u64 closer, deadline; u32 count, bytes, batch, failure; } WsEffectState;",&format!("{hooks}\ntypedef struct {{ u64 closer, deadline; u32 count, bytes, batch, failure; }} WsEffectState;"))?;
    Ok(c)
}

#[test]
fn phase5_reference_duplex() -> Result<()> {
    Controller::prepare(true)?.routine_duplex(PathKind::Reference,1)
}
#[test]
fn phase5_native_duplex() -> Result<()> {
    Controller::prepare(true)?.routine_duplex(PathKind::Native,1)
}
#[test]
fn phase5_bend_duplex() -> Result<()> {
    Controller::prepare(true)?.routine_duplex(PathKind::Bend,1)
}
#[test]
fn phase5_bend_four_connections() -> Result<()> {
    Controller::prepare(true)?.routine_duplex(PathKind::Bend,4)
}
#[test]
fn phase5_bend_partial_word() -> Result<()> {
    let mut c = Controller::prepare(true)?;
    let (t,e) = c.run_case(PathKind::Bend,Workload::W1 {bytes:259,split:Rates {to_peer:1,to_application:1}},100,0,1)?;
    correctness(&t,&e,&c.profile,false)?;
    c.record(&t,&e,"routine 259-byte packed partial-word duplex")?;
    c.routine_summary("Bend partial-word body encoding and full-byte validation")
}
#[test]
fn phase5_evidence_controls() -> Result<()> {
    Controller::prepare(true)?.routine_controls()
}
#[test]
fn phase5_native_overload_lifecycle() -> Result<()> {
    let mut c = Controller::prepare(true)?;
    c.scripted(PathKind::Native,100,1,5000,false)?;
    c.lifecycle(PathKind::Native,50000,1,0)?;
    c.routine_summary("Native scoped overload and same-process normal/disconnect cleanup")
}
#[test]
fn phase5_bend_overload_lifecycle() -> Result<()> {
    let mut c = Controller::prepare(true)?;
    c.scripted(PathKind::Bend,100,1,5000,false)?;
    c.lifecycle(PathKind::Bend,50000,1,0)?;
    c.routine_summary("Bend scoped overload and same-process normal/disconnect cleanup")
}
#[test]
#[ignore = "fixture build/cache probe without actor trials"]
fn phase5_fixture_build_probe() -> Result<()> {
    Controller::prepare(true).map(|_|())
}
#[test]
#[ignore = "explicit measured calibration only, not full qualification"]
fn phase5_calibrate_soak() -> Result<()> {
    let mut c = Controller::prepare(false)?;
    let workload = Workload::W2 {connections:1,split:Rates {to_peer:1,to_application:1}};
    let native = c.capacity(PathKind::Native,&workload,None)?;
    let bend = c.capacity(PathKind::Bend,&workload,None)?;
    c.capacity_checkpoint([native,bend],"measured calibration candidates")?;
    save(&c.out.join("summary.json"),&json!({"scope":"soak calibration","phase_pass":false,"qualification_run":false,"capacities":"capacities.json","trials":c.records}))
}
#[test]
#[ignore = "explicit checkpoint-driven 30-minute soak per selected candidate; never a full phase pass"]
fn phase5_standalone_soak() -> Result<()> {
    let checkpoint = PathBuf::from(env::var_os("WS_PHASE5_SOAK_CHECKPOINT").ok_or("WS_PHASE5_SOAK_CHECKPOINT required")?);
    let selected = env::var("WS_PHASE5_SOAK_PATH").unwrap_or_else(|_|"both".into());
    Controller::prepare(false)?.standalone_soak(&checkpoint,&selected)
}
#[test]
#[ignore = "explicit bounded single-worker qualification; WS_PHASE5_EXTENDED=1 selects historical full matrix"]
fn phase5_full_qualification() -> Result<()> {
    let mut c = Controller::prepare(false)?;
    let verdict = match if c.profile.raw["name"] == "local-perf-v2" {c.bounded_qualification()} else {c.qualification()} {
        Ok(v) => v,
        Err(e) => {
            let reason = e.to_string();
            if reason.starts_with("inconclusive:") {
                Verdict::Inconclusive(reason)
            } else {
                Verdict::Failed(reason)
            }
        }
    };
    let (pass, label, reason) = match verdict {
        Verdict::Passed => (true, "passed", String::new()),
        Verdict::Failed(s) => (false, "failed", s),
        Verdict::Inconclusive(s) => (false, "inconclusive", s),
    };
    if !pass && c.profile.raw["name"] == "local-perf-v2" {c.bounded_failure_diagnostics()?;}
    save(
        &c.out.join("summary.json"),
        &json!({"scope":if c.profile.raw["name"] == "local-perf-v2" {"bounded single-worker qualification"} else {"extended qualification"},"profile":c.profile.raw["name"],"qualification_run":true,"phase_pass":pass,"verdict":label,"reason":reason,"trials":c.records}),
    )?;
    require(pass, &format!("Phase 5 {label}: {reason}"))
}
