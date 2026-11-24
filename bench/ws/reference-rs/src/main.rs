use serde_json::{json, Value};
use std::alloc::{GlobalAlloc, Layout, System};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use tungstenite::{protocol::WebSocketConfig, Error, Message, WebSocket};

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
struct BoundedAllocator;
unsafe impl GlobalAlloc for BoundedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        if live > 134217728 {
            std::process::abort();
        }
        PEAK.fetch_max(live, Ordering::Relaxed);
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        System.dealloc(p, layout);
    }
}
#[global_allocator]
static ALLOCATOR: BoundedAllocator = BoundedAllocator;

const BUDGET: usize = 2 * 1024 * 1024;
const DESCRIPTORS: usize = 4096;
const BINS: usize = 10001;

fn now() -> u64 {
    unsafe {
        let mut t = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t);
        t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
    }
}
fn cpu_now() -> u64 {
    unsafe {
        let mut t = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut t);
        t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
    }
}

fn put(p: &mut [u8], offset: usize, x: u64) {
    p[offset..offset + 8].copy_from_slice(&x.to_be_bytes());
}
fn get(p: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(p[offset..offset + 8].try_into().unwrap())
}
fn payload(
    size: usize,
    token: u64,
    epoch: usize,
    dir: u64,
    conn: usize,
    seq: u64,
    deadline: u64,
) -> Vec<u8> {
    let mut p = vec![0; size];
    for (i, x) in [1, token, epoch as u64, dir, conn as u64, seq, deadline]
        .iter()
        .enumerate()
    {
        put(&mut p, i * 8, *x);
    }
    for (i, b) in p.iter_mut().enumerate().skip(56) {
        *b = (token
            .wrapping_add(seq.wrapping_mul(17))
            .wrapping_add((i as u64).wrapping_mul(31))
            .wrapping_add(dir * 7)
            .wrapping_add(conn as u64 * 13)
            .wrapping_add(epoch as u64 * 19)
            & 255) as u8;
    }
    p
}

struct Facts {
    attempted: u64,
    admitted: u64,
    transmitted: u64,
    validated: u64,
    in_window: u64,
    rejected: u64,
    next: u64,
    digest: u64,
    latency: Vec<u64>,
    lateness: Vec<u64>,
    max_latency: u64,
    samples: Vec<[u64; 5]>,
}
impl Facts {
    fn new(seconds: usize) -> Self {
        Self {
            attempted: 0,
            admitted: 0,
            transmitted: 0,
            validated: 0,
            in_window: 0,
            rejected: 0,
            next: 0,
            digest: 0,
            latency: vec![0; BINS],
            lateness: vec![0; BINS],
            max_latency: 0,
            samples: Vec::with_capacity(seconds),
        }
    }
    fn value(&self, scheduled: u64) -> Value {
        json!({"scheduled":scheduled,"attempted":self.attempted,"admitted":self.admitted,
            "transmitted":self.transmitted,"validated":self.validated,"validated_in_window":self.in_window,
            "validated_after_window":self.validated-self.in_window,"rejected":self.rejected,
            "digest":self.digest.to_string(),"latency_histogram_us":self.latency,
            "lateness_histogram_us":self.lateness,"max_latency_ns":self.max_latency,"samples":self.samples})
    }
}

enum Transport {
    Ws(WebSocket<TcpStream>),
    Tcp(TcpStream, Vec<u8>, usize, Vec<u8>),
}
impl Transport {
    fn send(&mut self, p: Vec<u8>) -> io::Result<bool> {
        match self {
            Self::Ws(w) => match w.write(Message::Binary(p.into())) {
                Ok(()) => Ok(true),
                Err(Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => Ok(true),
                Err(Error::WriteBufferFull(_)) => Ok(false),
                Err(e) => Err(io::Error::other(e)),
            },
            Self::Tcp(_, out, _, _) => {
                out.extend_from_slice(&(p.len() as u32).to_be_bytes());
                out.extend_from_slice(&p);
                Ok(true)
            }
        }
    }
    fn flush(&mut self) -> io::Result<bool> {
        match self {
            Self::Ws(w) => match w.flush() {
                Ok(()) => Ok(true),
                Err(Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => Ok(false),
                Err(e) => Err(io::Error::other(e)),
            },
            Self::Tcp(s, out, offset, _) => {
                if *offset < out.len() {
                    match s.write(&out[*offset..]) {
                        Ok(0) => return Err(io::Error::other("write zero")),
                        Ok(n) => *offset += n,
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
                        Err(e) => return Err(e),
                    }
                }
                if *offset == out.len() {
                    out.clear();
                    *offset = 0;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }
    fn receive(&mut self) -> io::Result<Option<Vec<u8>>> {
        match self {
            Self::Ws(w) => match w.read() {
                Ok(Message::Binary(p)) => Ok(Some(p.to_vec())),
                Ok(_) => Err(io::Error::other("unexpected control/message")),
                Err(Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
                Err(e) => Err(io::Error::other(e)),
            },
            Self::Tcp(s, _, _, input) => {
                if input.len() >= 4 {
                    let n = u32::from_be_bytes(input[..4].try_into().unwrap()) as usize;
                    if n > 1048576 {
                        return Err(io::Error::other("record limit"));
                    }
                    if input.len() >= n + 4 {
                        let p = input[4..n + 4].to_vec();
                        input.drain(..n + 4);
                        return Ok(Some(p));
                    }
                }
                let mut scratch = [0; 16384];
                match s.read(&mut scratch) {
                    Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF")),
                    Ok(n) => input.extend_from_slice(&scratch[..n]),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                    Err(e) => return Err(e),
                }
                if input.len() > 1048576 + 16388 {
                    return Err(io::Error::other("input limit"));
                }
                Ok(None)
            }
        }
    }
}
struct Connection {
    io: Transport,
    tx: [Facts; 2],
    rx: [Facts; 2],
    pending: Vec<usize>,
    queue_bytes: usize,
    highwater: usize,
    highdescriptors: usize,
    sndbuf: i32,
    rcvbuf: i32,
    peer_closed: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 && args[1] == "vectors" {
        println!(
            "{}",
            payload(64, 7, 1, 0, 2, 3, 123456)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        return Ok(());
    }
    if args.len() != 15 {
        return Err("expected mode port n size rates warm measure drain token faultdir fault saturation report".into());
    }
    let port: u16 = args[2].parse()?;
    let n: usize = args[3].parse()?;
    let size: usize = args[4].parse()?;
    let rates: [u64; 2] = [args[5].parse()?, args[6].parse()?];
    let warm: u64 = args[7].parse()?;
    let measure: u64 = args[8].parse()?;
    let drain: u64 = args[9].parse()?;
    let token: u64 = args[10].parse()?;
    let faultdir: i32 = args[11].parse()?;
    let fault = &args[12];
    let saturation = args[13] == "1";
    let pause_ns: u64 = std::env::var("WS_PAUSE_RECEIVE_MS")
        .unwrap_or_else(|_| "0".into())
        .parse::<u64>()?
        * 1_000_000;
    if n == 0 || n > 16 || !(64..=1048576).contains(&size) || measure == 0 || measure > 100 {
        return Err("config limits".into());
    }
    let durations = [warm, measure];
    let mut conns = Vec::new();
    for _ in 0..n {
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
        let stream = TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(1))?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(1)))?;
        stream.set_write_timeout(Some(std::time::Duration::from_secs(1)))?;
        let (sndbuf, rcvbuf) = unsafe {
            use std::os::fd::AsRawFd;
            let value: i32 = 262144;
            for option in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
                if libc::setsockopt(
                    stream.as_raw_fd(),
                    libc::SOL_SOCKET,
                    option,
                    &value as *const _ as *const _,
                    4,
                ) != 0
                {
                    return Err(io::Error::last_os_error().into());
                }
            }
            let mut values = [0i32; 2];
            for (i, option) in [libc::SO_SNDBUF, libc::SO_RCVBUF].iter().enumerate() {
                let mut len = 4;
                libc::getsockopt(
                    stream.as_raw_fd(),
                    libc::SOL_SOCKET,
                    *option,
                    &mut values[i] as *mut _ as *mut _,
                    &mut len,
                );
            }
            (values[0], values[1])
        };
        let io = if args[1] == "ws" {
            let config = WebSocketConfig::default()
                .write_buffer_size(0)
                .max_write_buffer_size(BUDGET)
                .max_message_size(Some(1048576))
                .max_frame_size(Some(1048576));
            let (w, _) = tungstenite::client::client_with_config(
                format!("ws://127.0.0.1:{port}/"),
                stream,
                Some(config),
            )?;
            w.get_ref().set_nonblocking(true)?;
            Transport::Ws(w)
        } else if args[1] == "tcp" {
            stream.set_nonblocking(true)?;
            Transport::Tcp(
                stream,
                Vec::with_capacity(BUDGET),
                0,
                Vec::with_capacity(1048576 + 16388),
            )
        } else {
            return Err("bad transport".into());
        };
        conns.push(Connection {
            io,
            tx: std::array::from_fn(|e| Facts::new(durations[e] as usize)),
            rx: std::array::from_fn(|e| Facts::new(durations[e] as usize)),
            pending: Vec::with_capacity(DESCRIPTORS),
            queue_bytes: 0,
            highwater: 0,
            highdescriptors: 0,
            sndbuf,
            rcvbuf,
            peer_closed: false,
        });
    }
    println!("READY");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let start: u64 = line.trim().parse()?;
    if start < now() + 10_000_000 {
        return Err("late barrier".into());
    }
    let starts = [start, start + warm * 1_000_000_000];
    let end = starts[1] + measure * 1_000_000_000;
    let mut failure = Value::Null;
    let begin_cpu = cpu_now();
    let mut last_sample = [0usize; 2];
    while now() < end + drain * 1_000_000_000 {
        let t = now();
        for (id, c) in conns.iter_mut().enumerate() {
            if c.peer_closed {
                continue;
            }
            match c.io.flush() {
                Ok(true) => {
                    for e in c.pending.drain(..) {
                        c.tx[e].transmitted += 1;
                    }
                    c.queue_bytes = 0;
                }
                Ok(false) => (),
                Err(e) => {
                    failure = json!({"kind":"transport","detail":e.to_string()});
                    break;
                }
            }
            for _ in 0..256 {
                if t < start + pause_ns {
                    break;
                }
                let p = match c.io.receive() {
                    Ok(Some(p)) => p,
                    Ok(None) => break,
                    Err(e)
                        if t >= end
                            && c.pending.is_empty()
                            && matches!(
                                e.kind(),
                                io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
                            ) =>
                    {
                        c.peer_closed = true;
                        break;
                    }
                    Err(e) => {
                        failure = json!({"kind":"transport","detail":e.to_string()});
                        break;
                    }
                };
                if p.len() != size || p.len() < 56 {
                    failure = json!({"kind":"corruption","connection":id,"direction":1});
                    break;
                }
                let epoch = get(&p, 16) as usize;
                let seq = get(&p, 40);
                if epoch > 1 {
                    failure = json!({"kind":"corruption","connection":id,"direction":1});
                    break;
                }
                let f = &mut c.rx[epoch];
                if seq < f.next || (!saturation && seq > f.next) {
                    failure = json!({"kind":if seq<f.next {"duplicate"} else {"sequence"},"connection":id,"direction":1,"epoch":epoch,"sequence":seq,"expected":f.next});
                    break;
                }
                let deadline = if rates[1] > 0 {
                    starts[epoch] + seq * 1_000_000_000 / rates[1]
                } else {
                    0
                };
                if p != payload(size, token, epoch, 1, id, seq, deadline) {
                    failure = json!({"kind":"corruption","connection":id,"direction":1,"epoch":epoch,"sequence":seq});
                    break;
                }
                let received = now();
                if received < deadline {
                    failure = json!({"kind":"clock"});
                    break;
                }
                f.next = seq + 1;
                f.validated += 1;
                f.digest = f.digest.wrapping_mul(31).wrapping_add(seq + 1);
                if received < starts[epoch] + durations[epoch] * 1_000_000_000 {
                    f.in_window += 1;
                }
                let latency = received - deadline;
                f.max_latency = f.max_latency.max(latency);
                f.latency[((latency + 999) / 1000).min((BINS - 1) as u64) as usize] += 1;
            }
            if !failure.is_null() {
                break;
            }
            for epoch in 0..2 {
                if t < starts[epoch] {
                    continue;
                }
                if saturation && t >= starts[epoch] + durations[epoch] * 1_000_000_000 {
                    continue;
                }
                let scheduled = rates[0] * durations[epoch];
                for _ in 0..256 {
                    let seq = c.tx[epoch].attempted;
                    if rates[0] == 0
                        || seq >= scheduled
                        || starts[epoch] + seq * 1_000_000_000 / rates[0] > t
                    {
                        break;
                    }
                    let deadline = starts[epoch] + seq * 1_000_000_000 / rates[0];
                    let f = &mut c.tx[epoch];
                    f.attempted += 1;
                    f.lateness
                        [((now() - deadline + 999) / 1000).min((BINS - 1) as u64) as usize] += 1;
                    let duplicate =
                        faultdir == 0 && fault == "duplicate" && epoch == 1 && id == 0 && seq == 50;
                    let copies = if duplicate { 2 } else { 1 };
                    for _ in 0..copies {
                        if c.queue_bytes + size + 14 > BUDGET || c.pending.len() >= DESCRIPTORS {
                            c.tx[epoch].rejected += 1;
                            continue;
                        }
                        let mut p = payload(size, token, epoch, 0, id, seq, deadline);
                        if faultdir == 0
                            && fault == "corruption"
                            && epoch == 1
                            && id == 0
                            && seq == 50
                        {
                            p[size - 1] ^= 1;
                        }
                        match c.io.send(p) {
                            Ok(true) => {
                                c.tx[epoch].admitted += 1;
                                c.tx[epoch].digest =
                                    c.tx[epoch].digest.wrapping_mul(31).wrapping_add(seq + 1);
                                c.pending.push(epoch);
                                c.queue_bytes += size + 14;
                            }
                            Ok(false) => c.tx[epoch].rejected += 1,
                            Err(e) => {
                                failure = json!({"kind":"transport","detail":e.to_string()});
                                break;
                            }
                        }
                        c.highwater = c.highwater.max(c.queue_bytes);
                        c.highdescriptors = c.highdescriptors.max(c.pending.len());
                        if let Ok(true) = c.io.flush() {
                            for e in c.pending.drain(..) {
                                c.tx[e].transmitted += 1;
                            }
                            c.queue_bytes = 0;
                        }
                    }
                }
            }
        }
        for epoch in 0..2 {
            while last_sample[epoch] < durations[epoch] as usize
                && t >= starts[epoch] + (last_sample[epoch] as u64 + 1) * 1_000_000_000
            {
                for c in &mut conns {
                    let sampled = now();
                    c.tx[epoch].samples.push([
                        sampled,
                        c.tx[epoch].attempted,
                        c.tx[epoch].admitted,
                        c.tx[epoch].transmitted,
                        c.queue_bytes as u64,
                    ]);
                    c.rx[epoch].samples.push([
                        sampled,
                        c.rx[epoch].validated,
                        c.rx[epoch].in_window,
                        0,
                        0,
                    ]);
                }
                last_sample[epoch] += 1;
            }
        }
        if !failure.is_null() {
            break;
        }
        if t >= end
            && std::env::var("WS_EARLY_DRAIN").is_ok_and(|v| v == "1")
            && conns.iter().all(|c| {
                c.pending.is_empty()
                    && (0..2).all(|e| {
                        c.tx[e].transmitted == rates[0] * durations[e]
                            && c.rx[e].validated == rates[1] * durations[e]
                    })
            })
        {
            break;
        }
    }
    let finished = now();
    let cpu = cpu_now() - begin_cpu;
    let streams: Vec<Value> = conns
        .iter()
        .enumerate()
        .flat_map(|(id, c)| {
            (0..2).map(move |e| {
                json!({"connection":id,"epoch":e,
        "tx":c.tx[e].value(rates[0]*durations[e]),"rx":c.rx[e].value(rates[1]*durations[e]),
        "queue_highwater_bytes":c.highwater,"queue_highwater_descriptors":c.highdescriptors,
        "sndbuf":c.sndbuf,"rcvbuf":c.rcvbuf})
            })
        })
        .collect();
    let heap_peak = PEAK.load(Ordering::Relaxed);
    let heap_live = LIVE.load(Ordering::Relaxed);
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let report = json!({"schema":1,"actor":"client","token":token,"start_ns":start,"end_ns":end,"finished_ns":finished,
        "cpu_ticks":cpu,"cpu_ticks_per_second":1000000000,"streams":streams,"failure":failure,
        "queue_limit_bytes":BUDGET,"queue_limit_descriptors":DESCRIPTORS,"histogram_overflow_us":10000,
        "tracked_heap_peak_bytes":heap_peak,"tracked_heap_live_bytes":heap_live,
        "allocation_count":allocations,"tracked_heap_limit_bytes":134217728});
    std::fs::write(&args[14], serde_json::to_vec(&report)?)?;
    if !failure.is_null() {
        std::process::exit(1);
    }
    Ok(())
}
