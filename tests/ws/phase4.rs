use serde_json::{json, Value};
use std::io::{Read, Write};
use std::{
    env, fs,
    net::TcpListener,
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tungstenite::Message;

fn replace(c: &mut String, from: &str, to: &str) {
    assert_eq!(c.matches(from).count(), 1, "pinned compiler anchor: {from}");
    *c = c.replacen(from, to, 1);
}
fn instrument(mut c: String) -> String {
    let hooks = r#"
static _Atomic uint64_t ws_bend_blocks, ws_bend_bytes, ws_bend_peak;
static unsigned char ws_bend_owned[1u << 24];
static uint64_t ws_track_bend(Env e, uint32_t cls, uint64_t loc) {
  if (err_peek(e.mem)) return loc;
  uint64_t i = loc - HEAP_OFF;
  if (i >= (1u << 24) || ws_bend_owned[i]) abort();
  ws_bend_owned[i] = cls + 1; atomic_fetch_add(&ws_bend_blocks, 1);
  uint64_t n = atomic_fetch_add(&ws_bend_bytes, 8ull << cls) + (8ull << cls);
  if (n > atomic_load(&ws_bend_peak)) atomic_store(&ws_bend_peak, n);
  return loc;
}
static void ws_free_bend(uint32_t cls, uint64_t loc) {
  uint64_t i = loc - HEAP_OFF;
  if (i >= (1u << 24) || ws_bend_owned[i] != cls + 1) abort();
  ws_bend_owned[i] = 0; atomic_fetch_sub(&ws_bend_blocks, 1); atomic_fetch_sub(&ws_bend_bytes, 8ull << cls);
}
"#;
    replace(
        &mut c,
        "INLINE u64 heap_alloc(Env e, u32 cls) {",
        &format!("{hooks}\nINLINE u64 heap_alloc(Env e, u32 cls) {{"),
    );
    replace(&mut c, "    return h;\n  }\n  return heap_alloc_miss(e, cls);", "    return ws_track_bend(e, cls, h);\n  }\n  return ws_track_bend(e, cls, heap_alloc_miss(e, cls));");
    replace(&mut c, "INLINE void heap_free(Env e, u32 cls, u64 loc) {\n  if (err_peek(e.mem)) {\n    return;\n  }", "INLINE void heap_free(Env e, u32 cls, u64 loc) {\n  if (err_peek(e.mem)) {\n    return;\n  }\n  ws_free_bend(cls, loc);");
    let hooks = r#"
static uint64_t ws_c_bytes, ws_c_blocks, ws_c_peak;
static void *ws_malloc(size_t n) { size_t *p = malloc(n + sizeof(size_t)); if (!p) return NULL; *p = n; ws_c_blocks++; ws_c_bytes += n; if(ws_c_bytes > ws_c_peak)ws_c_peak=ws_c_bytes; return p+1; }
static void *ws_calloc(size_t n,size_t size) { void*p=ws_malloc(n*size);if(p)memset(p,0,n*size);return p; }
static void ws_free(void *p) { if(p){size_t*q=(size_t*)p-1;ws_c_bytes-=*q;ws_c_blocks--;free(q);} }
static uint64_t ws_clock(void) { struct timespec t; if(clock_gettime(CLOCK_MONOTONIC,&t))abort(); return (uint64_t)t.tv_sec*1000000000ull+t.tv_nsec; }
static u64 ws_interval_start, ws_interval_end, ws_dispatches, ws_interval_dispatches;
static int ws_armed, ws_observer_fd;
static u64 ws_copy_ns, ws_conversion_ns, ws_effect_ns, ws_effect_count;
static u64 ws_single_sent, ws_batch_sent, ws_single_received, ws_batch_received;
static u64 ws_observed_budget;
static Term ws_convert(Env e, const char *bytes, u32 n) {
  u64 start = ws_clock(); Term value = io_list(e, bytes, n);
  ws_conversion_ns += ws_clock() - start; return value;
}
"#;
    replace(
        &mut c,
        "static void io_step(Env e, IoWork* a) {",
        &format!("{hooks}\nstatic void io_step(Env e, IoWork* a) {{"),
    );
    replace(
        &mut c,
        "    Term req = corpus_eval(e.mem, term_tsk(FID_Clo_apply, ap));",
        r#"    int measuring = ws_armed;
    if (measuring) {
      ws_armed = 0; ws_interval_start = ws_clock(); ws_interval_dispatches = ws_dispatches;
      if (write(ws_observer_fd, &ws_interval_start, 8) != 8) abort();
    }
    Term req = corpus_eval(e.mem, term_tsk(FID_Clo_apply, ap));
    if (measuring) { ws_interval_end = ws_clock(); ws_interval_dispatches = ws_dispatches - ws_interval_dispatches; if(term_aux(req) != CID_computed)abort(); }"#,
    );
    replace(&mut c, "    Term x  = io_eff_rows[c](e, fs, a);", "    ws_dispatches++;\n    u64 effect_start = ws_clock();\n    Term x  = io_eff_rows[c](e, fs, a);\n    ws_effect_ns += ws_clock() - effect_start; ws_effect_count++;");
    replace(&mut c, "typedef struct { u64 closer, deadline; u32 count, bytes, batch, failure; } WsEffectState;", "#define malloc ws_malloc\n#define calloc ws_calloc\n#define free ws_free\ntypedef struct { u64 closer, deadline; u32 count, bytes, batch, failure; } WsEffectState;");
    replace(&mut c, "  WsBatchTake r = ws_socket_take_batch(w->hand, state->count, state->bytes);", "  u64 copy_start = ws_clock();\n  WsBatchTake r = ws_socket_take_batch(w->hand, state->count, state->bytes);\n  WsSocketStatus memory = ws_socket_status(w->hand);\n  if(memory.code == WS_OK) {ws_observed_budget = memory.native_budget; if(memory.native_peak > memory.native_budget)abort();}\n  ws_copy_ns += ws_clock() - copy_start;\n  if(r.code == WS_OK) { if(state->batch)ws_batch_received += r.count;else ws_single_received += r.count; }");
    replace(
        &mut c,
        "io_list(e, (char*)r.bytes + span.offset, span.len)",
        "ws_convert(e, (char*)r.bytes + span.offset, span.len)",
    );
    replace(
        &mut c,
        "  if (a.accepted == 1) {",
        "  if (a.accepted == 1) {\n    ws_single_sent++;",
    );
    replace(
        &mut c,
        "  if (a.code == WS_OK) {",
        "  if (a.code == WS_OK) {\n    ws_batch_sent += a.accepted;",
    );
    replace(&mut c, "static Term ws_release_run(Env e, Term *f, IoWork *w) { return ws_socket_release(io_hand_v(f[0])); }", r#"static Term ws_release_run(Env e, Term *f, IoWork *w) {
  static u64 previous;
  if(getenv("WS_OMIT_RELEASE"))return WS_OK;
  if(previous) {u32 old=ws_socket_release(previous);if(old!=WS_CLOSED&&old!=WS_STALE)abort();}
  u64 h=io_hand_v(f[0]);u32 code=ws_socket_release(h);
  if(code==WS_OK) {if(ws_socket_release(h)!=WS_CLOSED)abort();previous=h;}
  return code;
}"#);
    replace(&mut c,"  u32 code = ws_socket_stop_finish(w->hand, w->size);","  u32 code = ws_socket_stop_finish(w->hand, w->size);\n  if(code == WS_OK && ws_socket_stop_begin(w->hand,w->size).code != WS_CLOSED)abort();");
    replace(
        &mut c,
        "w->made = io_until(1000);",
        "w->made = io_until(getenv(\"WS_EARLY_CLOSE_POLL\") ? 1 : 1000);",
    );
    replace(&mut c, "  return code;\n}\nstatic Term ws_stop_with", "  printf(\"{\\\"kind\\\":\\\"close\\\",\\\"role\\\":%lu,\\\"code\\\":%u,\\\"elapsed_ns\\\":%lu}\\n\", w->size, code, io_tick() - ((u64)w->made - (getenv(\"WS_EARLY_CLOSE_POLL\") ? 1000000ull : 1000000000ull)));\n  return code;\n}\nstatic Term ws_stop_with");
    c
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
fn spawn(mut c: Command, root: &Path, output: &Path, label: &str) -> Process {
    fs::write(output.join(format!("{label}-command.json")), serde_json::to_vec_pretty(&json!({"program":c.get_program(),"args":c.get_args().collect::<Vec<_>>(),"environment":c.get_envs().map(|(k,v)|(k,v)).collect::<Vec<_>>()})).unwrap()).unwrap();
    c.current_dir(root)
        .process_group(0)
        .env_remove("BENDRTS")
        .env("BEND_NO_TELEMETRY", "1")
        .stdin(Stdio::null())
        .stdout(fs::File::create(output.join(format!("{label}.stdout"))).unwrap())
        .stderr(fs::File::create(output.join(format!("{label}.stderr"))).unwrap());
    unsafe {
        c.pre_exec(|| {
            let limits = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if libc::setrlimit(libc::RLIMIT_CORE, &limits) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Process(c.spawn().unwrap())
}
fn finish(p: &mut Process, output: &Path, label: &str, seconds: u64) -> String {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    loop {
        if let Some(s) = p.0.try_wait().unwrap() {
            assert!(
                s.success(),
                "{label}: {s}\n{}\n{}",
                fs::read_to_string(output.join(format!("{label}.stdout"))).unwrap(),
                fs::read_to_string(output.join(format!("{label}.stderr"))).unwrap()
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{label} deadline\n{}",
            fs::read_to_string(output.join(format!("{label}.stderr"))).unwrap()
        );
        thread::sleep(Duration::from_millis(2));
    }
    fs::read_to_string(output.join(format!("{label}.stdout"))).unwrap()
}
fn run(c: Command, root: &Path, out: &Path, label: &str) -> String {
    let mut p = spawn(c, root, out, label);
    finish(&mut p, out, label, 120)
}
fn payload(sequence: u32, direction: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| match i {
            0 => direction,
            1..=4 => 0,
            5..=8 => (sequence >> ((8 - i) * 8)) as u8,
            _ => sequence
                .wrapping_mul(17)
                .wrapping_add(i as u32 * 31)
                .wrapping_add(direction as u32 * 7) as u8,
        })
        .collect()
}
fn recurrence(seed: u32, n: u32) -> u32 {
    let mut v = seed;
    for i in 0..n {
        v = v.wrapping_mul(1664525).wrapping_add(1013904223) ^ seed.wrapping_add(i);
    }
    v
}
fn clock() -> u64 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t) },
        0
    );
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}
fn rows(text: &str) -> Vec<Value> {
    text.lines()
        .filter(|l| l.starts_with('{'))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}
fn n(v: &Value, k: &str) -> u64 {
    v[k].as_u64().unwrap_or_else(|| panic!("missing {k}: {v}"))
}
fn validate_compute(compute: &Value, status: &Value, peer: &Value) -> Result<(), &'static str> {
    let (t0, t1) = (n(compute, "start"), n(compute, "end"));
    if t1 < t0 + 100_000_000 {
        return Err("pure interval");
    }
    if n(compute, "dispatches") != 0 {
        return Err("intervening effect");
    }
    if n(compute, "value")
        != recurrence(n(peer, "seed") as u32, n(compute, "iterations") as u32) as u64
        || n(status, "rx") != n(peer, "seed")
    {
        return Err("runtime checksum");
    }
    if n(status, "tx") != recurrence(n(peer, "seed") as u32, 100) as u64 {
        return Err("prior result");
    }
    for k in ["tx_ns", "rx_ns"] {
        let t = status[k].as_u64().ok_or("missing completion")?;
        if !(t0 < t && t < t1) {
            return Err("native event interval");
        }
    }
    for k in ["receipt_ns", "resume_ns"] {
        let t = peer[k].as_u64().ok_or("missing peer event")?;
        if !(t0 < t && t < t1) {
            return Err("peer event interval");
        }
    }
    if n(status, "sent") != 3 || n(status, "received") != 2 {
        return Err("completion identity");
    }
    if n(peer, "pong_ns") - n(peer, "ping_ns") > 10_000_000 || n(status, "control_ns") > 10_000_000
    {
        return Err("control service");
    }
    if peer["bend_validated"] != true {
        return Err("Bend input consumption");
    }
    Ok(())
}
fn damaged_compute(compute: &Value, status: &Value, peer: &Value) {
    let mut short = compute.clone();
    short["end"] = json!(n(compute, "start") + 99_999_999);
    assert_eq!(validate_compute(&short, status, peer), Err("pure interval"));
    let mut shifted = status.clone();
    shifted["tx_ns"] = compute["start"].clone();
    assert_eq!(
        validate_compute(compute, &shifted, peer),
        Err("native event interval")
    );
    let mut missing = status.clone();
    missing["rx_ns"] = Value::Null;
    assert_eq!(
        validate_compute(compute, &missing, peer),
        Err("missing completion")
    );
    let mut wrong = compute.clone();
    wrong["value"] = json!(n(compute, "value") ^ 1);
    assert_eq!(
        validate_compute(&wrong, status, peer),
        Err("runtime checksum")
    );
    let mut missing = peer.clone();
    missing["bend_validated"] = json!(false);
    assert_eq!(
        validate_compute(compute, status, &missing),
        Err("Bend input consumption")
    );
}
fn build_peer(root: &Path, out: &Path, cache: &Path, clang: &Path) -> PathBuf {
    let mut sources = vec![];
    for d in ["src", "src/eventing", "src/crypto", "src/io_uring"] {
        for e in fs::read_dir(cache.join("uSockets").join(d)).unwrap() {
            let p = e.unwrap().path();
            if p.extension().is_some_and(|x| x == "c") {
                sources.push(p);
            }
        }
    }
    sources.sort();
    sources.push(root.join("bench/ws/phase3-peer.cpp"));
    let mut objects = vec![];
    for (i, s) in sources.iter().enumerate() {
        let o = out.join(format!("peer-{i}.o"));
        let mut c = Command::new(clang);
        c.args(["-O3", "-DLIBUS_NO_SSL", "-DUWS_NO_ZLIB", "-pthread"])
            .arg(format!("-I{}", cache.join("uSockets/src").display()))
            .arg(format!("-I{}", cache.join("uWebSockets/src").display()));
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
        c.arg("-c").arg(s).arg("-o").arg(&o);
        run(c, root, out, &format!("peer-build-{i}"));
        objects.push(o);
    }
    let binary = out.join("uws-peer");
    let mut c = Command::new(clang);
    c.arg("--gcc-install-dir=/usr/lib/gcc/x86_64-linux-gnu/13")
        .args(objects)
        .args(["-lstdc++", "-pthread"])
        .arg("-o")
        .arg(&binary);
    run(c, root, out, "peer-link");
    binary
}
fn child(binary: &Path, port: u16, mode: u32, iters: u32) -> Command {
    let mut c = Command::new(binary);
    c.args(["+RTS", "--threads", "1", "--gpu", "off", "-RTS"])
        .env("WS_PORT", port.to_string())
        .env("WS_MODE", mode.to_string())
        .env("WS_ITERATIONS", iters.to_string());
    c
}
#[repr(C)]
#[derive(Default)]
struct PauseRecord {
    phase: u32,
    direction: u32,
    hold: u32,
    sequence: u32,
    time: u64,
}
fn notice(fd: &OwnedFd) -> PauseRecord {
    let mut r = PauseRecord::default();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let n = unsafe {
            libc::read(
                fd.as_raw_fd(),
                &mut r as *mut _ as *mut _,
                std::mem::size_of::<PauseRecord>(),
            )
        };
        if n == std::mem::size_of::<PauseRecord>() as isize {
            return r;
        }
        assert!(
            n < 0 && Instant::now() < deadline,
            "notice deadline/partial record: {n}"
        );
        thread::sleep(Duration::from_micros(100));
    }
}
fn pipe() -> (OwnedFd, OwnedFd) {
    let mut fds = [0; 2];
    assert_eq!(
        unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK) },
        0
    );
    unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
}
fn resource_check(rows: &[Value]) {
    for row in rows {
        if row.get("native_peak").is_some()
            && row.get("native_budget").is_some()
            && n(row, "native_budget") != 0
        {
            assert!(
                n(row, "native_peak") <= n(row, "native_budget"),
                "peak budget: {row}"
            );
        }
    }
    let checkpoints: Vec<_> = rows.iter().filter(|r| r["kind"] == "checkpoint").collect();
    assert!((2..=3).contains(&checkpoints.len()));
    let start = checkpoints[0];
    let end = checkpoints.last().unwrap();
    if checkpoints.len() == 3 {
        let retained = checkpoints[1];
        assert_eq!(n(retained, "tag"), 90);
        assert!(n(retained, "native_bytes") > 0);
        for k in ["fds", "threads"] {
            assert_eq!(retained[k], start[k]);
        }
    }
    for key in ["native_bytes", "native_blocks", "layout_overflow"] {
        assert_eq!(n(end, key), 0, "resource {key}: {end}");
    }
    for key in ["c_bytes", "c_blocks", "fds", "threads"] {
        assert_eq!(end[key], start[key], "resource {key}: {end}");
    }
    assert_eq!(n(end, "bend_bytes"), 0);
    assert_eq!(n(end, "bend_blocks"), 0);
    assert!(n(end, "bend_peak") <= 32 * 1024 * 1024);
    assert!(n(end, "c_peak") <= 1048576 + 64);
}
fn finite(binary: &Path, root: &Path, out: &Path, index: u32, mode: &str) -> Value {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (read, write) = pipe();
    let peer = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_nodelay(true).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
        if index == 20 {
            let mut request = [0; 4096];
            s.read(&mut request).unwrap();
            s.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                .unwrap();
            return json!({"case":index,"upgrade_rejected":true});
        }
        let mut ws = tungstenite::accept(s).unwrap();
        let size = if (8..14).contains(&index) {
            [0, 125, 126, 65535, 65536, 1048576][index as usize - 8]
        } else {
            256
        };
        ws.send(Message::Binary(payload(index, 1, size).into()))
            .unwrap();
        let mut report = json!({"case":index,"validated":0});
        if (14..17).contains(&index) {
            let count = if index == 16 { 2 } else { 8 };
            let length = if index == 16 {
                1048576
            } else if index == 15 {
                0
            } else {
                256
            };
            for i in 0..count + 1 {
                let length = if i == count && index == 16 {
                    256
                } else {
                    length
                };
                ws.send(Message::Binary(
                    payload(30000 + index * 16 + i, 1, length).into(),
                ))
                .unwrap();
            }
        }
        if index < 6 {
            let begin = notice(&read);
            assert_eq!(begin.phase, 0);
            assert_eq!(begin.sequence, index);
            for seq in 20000 + index * 8..20000 + index * 8 + 8 {
                ws.send(Message::Binary(payload(seq, 1, 256).into()))
                    .unwrap();
            }
            if begin.direction == 0 {
                let mut receipt = vec![];
                for seq in 10000 + index * 8..10000 + index * 8 + 8 {
                    assert_eq!(
                        ws.read().unwrap().into_data().as_ref(),
                        payload(seq, 0, 256)
                    );
                    receipt.push(clock());
                }
                let end = notice(&read);
                assert_eq!(end.phase, 1);
                for t in &receipt {
                    assert!(begin.time < *t && *t < end.time);
                }
                let drained = notice(&read);
                assert_eq!(drained.phase, 2);
                assert!(end.time <= drained.time && drained.time - end.time <= 2_000_000_000);
                report = json!({"case":index,"validated":8,"begin":begin.time,"end":end.time,"receipts":receipt,"drain_ns":drained.time-end.time});
            } else {
                let consumed = notice(&read);
                assert_eq!(consumed.phase, 2);
                let end = begin.time + begin.hold as u64 * 1_000_000;
                assert!(
                    begin.time < consumed.time && consumed.time < end,
                    "Bend consumption outside peer pause"
                );
                while clock() < end {
                    thread::sleep(Duration::from_micros(100));
                }
                let resume = clock();
                let len = if begin.hold == 1000 { 256 } else { 1048576 };
                assert_eq!(
                    ws.read().unwrap().into_data().as_ref(),
                    payload(index, 0, len)
                );
                let receipt = clock();
                let ended = notice(&read);
                assert_eq!(ended.phase, 1);
                report = json!({"case":index,"validated":1,"begin":begin.time,"end":resume,"consumed":consumed.time,"receipt":receipt,"drain_ns":receipt-resume});
                assert!(receipt - resume < 2_000_000_000);
            }
        } else if index >= 8 && index < 14 {
            assert_eq!(
                ws.read().unwrap().into_data().as_ref(),
                payload(index, 0, size)
            );
            report["validated"] = json!(1);
        } else if index == 18 {
            assert_eq!(
                ws.read().unwrap().into_data().as_ref(),
                payload(index, 0, 256)
            );
            report["validated"] = json!(1);
        } else if index == 29 {
            for seq in 40000..40008 {
                assert_eq!(
                    ws.read().unwrap().into_data().as_ref(),
                    payload(seq, 0, 256)
                );
            }
            let mut cycles = vec![];
            for cycle in 0..2 {
                let begin = notice(&read);
                assert_eq!(
                    (begin.phase, begin.direction, begin.sequence),
                    (0, 3, cycle)
                );
                for seq in 50000 + cycle * 4608..50000 + (cycle + 1) * 4608 {
                    ws.send(Message::Binary(payload(seq, 1, 256).into()))
                        .unwrap();
                }
                let mut receipts = vec![];
                for seq in 60000 + cycle * 8..60000 + (cycle + 1) * 8 {
                    assert_eq!(
                        ws.read().unwrap().into_data().as_ref(),
                        payload(seq, 0, 256)
                    );
                    receipts.push(clock());
                }
                let end = notice(&read);
                assert_eq!(end.phase, 1);
                assert!(receipts.iter().all(|t| begin.time < *t && *t < end.time));
                let drained = notice(&read);
                assert_eq!(drained.phase, 2);
                assert!(end.time < drained.time && drained.time - end.time < 2_000_000_000);
                cycles.push(json!({"begin":begin.time,"end":end.time,"drained":drained.time,"opposite_receipts":receipts,"validated_in_bend":4608}));
            }
            report = json!({"case":index,"cycles":cycles});
        } else if index >= 22 {
            let start = notice(&read);
            assert_eq!(start.direction, 2);
            let resume = clock();
            let count = start.sequence;
            for seq in 0..count {
                let len = if index == 23 { 65536 } else { 0 };
                assert_eq!(
                    ws.read().unwrap().into_data().as_ref(),
                    payload(seq, 0, len)
                );
            }
            let done = clock();
            assert!(done - resume <= 2_000_000_000);
            report = json!({"case":index,"validated":count,"drain_ns":done-resume});
        }
        loop {
            match ws.read() {
                Ok(Message::Close(_)) => {
                    let _ = ws.flush();
                    break;
                }
                Err(tungstenite::Error::ConnectionClosed) => break,
                Err(tungstenite::Error::Protocol(
                    tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
                )) if index == 19 => {
                    report["panic_disconnect"] = json!(true);
                    break;
                }
                m => panic!("case {index} expected close: {m:?}"),
            }
        }
        report
    });
    let label = format!("{mode}-case-{index}");
    let mut c = child(binary, port, 2, index);
    c.env("WS_OBSERVER_FD", write.as_raw_fd().to_string())
        .env("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1")
        .env("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1");
    let mut b = spawn(c, root, out, &label);
    drop(write);
    let text = finish(&mut b, out, &label, 15);
    let report = peer.join().unwrap();
    let rows = rows(&text);
    resource_check(&rows);
    if index == 29 {
        let idle: Vec<_> = rows
            .iter()
            .filter(|r| r["kind"] == "connected_idle")
            .collect();
        assert_eq!(idle.len(), 3);
        for point in &idle {
            assert_eq!(
                point["native_live"], idle[0]["native_live"],
                "connected baseline: {point}"
            );
            assert_eq!(point["worker_live"], point["worker_idle"]);
            assert_eq!(point["worker_idle"], idle[0]["worker_idle"]);
        }
        let full: Vec<_> = rows.iter().filter(|r| r["kind"] == "saturated").collect();
        assert_eq!(full.len(), 2);
        for point in full {
            assert_eq!(n(point, "rx_count"), 4096);
            assert_eq!(n(point, "rx_bytes"), 4096 * 256);
        }
    }
    if index < 6 {
        let drained = rows
            .iter()
            .find(|r| r["kind"] == "status" && r["tag"] == 1)
            .unwrap();
        for key in ["admitted", "sent"] {
            assert_eq!(n(drained, key), if index < 3 { 8 } else { 1 });
        }
        for key in ["received", "consumed"] {
            assert_eq!(n(drained, key), 9);
        }
        for key in ["tx_count", "rx_count", "leases", "failed"] {
            assert_eq!(n(drained, key), 0);
        }
        let pauses: Vec<_> = rows.iter().filter(|r| r["kind"] == "pause").collect();
        let first = pauses[0];
        let last = pauses.last().unwrap();
        assert!(n(last, "received") > n(first, "received"));
        if index < 3 {
            assert!(n(last, "sent") > n(first, "sent"));
            assert_eq!(n(first, "consumed"), n(pauses[1], "consumed"));
        } else {
            assert!(n(pauses[1], "consumed") > n(first, "consumed"));
        }
        for p in pauses {
            assert!(n(p, "native_live") <= n(p, "native_budget"));
            for k in ["tx_bytes", "rx_bytes"] {
                assert!(n(p, k) <= 2097152);
            }
            for k in ["tx_count", "rx_count"] {
                assert!(n(p, k) <= 4096);
            }
        }
    }
    json!({"mode":mode,"case":index,"rows":rows,"peer":report})
}
fn lifecycle(binary: &Path, root: &Path, out: &Path, mode: &str, cycles: u32) -> Value {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = thread::spawn(move || {
        for _ in 0..cycles {
            let (s, _) = listener.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            s.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut ws = tungstenite::accept(s).unwrap();
            ws.send(Message::Binary(payload(42, 1, 256).into()))
                .unwrap();
            assert_eq!(ws.read().unwrap().into_data().as_ref(), payload(42, 0, 256));
            assert!(matches!(ws.read().unwrap(), Message::Close(_)));
            let _ = ws.flush();
        }
    });
    let label = format!("{mode}-lifecycle");
    let mut c = child(binary, port, 3, cycles);
    c.env("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1")
        .env("UBSAN_OPTIONS", "halt_on_error=1");
    let mut b = spawn(c, root, out, &label);
    let data = rows(&finish(&mut b, out, &label, 120));
    peer.join().unwrap();
    let points: Vec<_> = data
        .iter()
        .filter(|r| r["kind"] == "checkpoint" && r["tag"] == 100)
        .collect();
    assert_eq!(points.len(), cycles as usize);
    for p in &points {
        for k in ["native_bytes", "native_blocks", "layout_overflow"] {
            assert_eq!(n(p, k), 0, "{k}: {p}");
        }
        for k in [
            "bend_bytes",
            "bend_blocks",
            "c_bytes",
            "c_blocks",
            "fds",
            "threads",
        ] {
            assert_eq!(p[k], points[0][k], "cycle baseline {k}: {p}");
        }
    }
    let end = data
        .iter()
        .find(|r| r["kind"] == "checkpoint" && r["tag"] == 101)
        .unwrap();
    assert_eq!(n(end, "bend_bytes"), 0);
    assert_eq!(n(end, "bend_blocks"), 0);
    json!({"mode":mode,"cycles":cycles,"baseline":points[0],"final":end,"peer_validated":cycles})
}
fn integrity(binary: &Path, root: &Path, out: &Path, index: u32) -> Value {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let peer = thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut ws = tungstenite::accept(s).unwrap();
        if index == 28 {
            ws.send(Message::Text("A\0€".into())).unwrap();
            ws.send(Message::Binary(payload(1, 1, 256).into())).unwrap();
            assert!(matches!(ws.read().unwrap(), Message::Close(_)));
            let _ = ws.flush();
            return json!({"case":index,"binary_and_text_validated_in_bend":true});
        } else if index < 26 {
            assert_eq!(ws.read().unwrap().into_data().as_ref(), payload(0, 0, 256));
            let bad = ws.read().unwrap().into_data();
            assert_ne!(bad.as_ref(), payload(1, 0, 256));
            if index == 24 {
                assert_eq!(bad.as_ref(), payload(0, 0, 256));
            } else {
                assert_eq!(bad[0], 1);
                assert_eq!(&bad[1..], &payload(1, 0, 256)[1..]);
            }
            assert!(matches!(ws.read().unwrap(), Message::Close(_)));
            let _ = ws.flush();
        } else {
            ws.send(Message::Binary(payload(0, 1, 256).into())).unwrap();
            let mut bad = payload(if index == 26 { 0 } else { 1 }, 1, 256);
            if index == 27 {
                bad[0] ^= 1;
            }
            ws.send(Message::Binary(bad.into())).unwrap();
            assert!(ws.read().is_err());
        }
        json!({"case":index,"injected_identity":1,"direction":if index<26 {"outbound"}else {"inbound"},"fault":if index%2==0 {"duplicate"}else {"corruption"},"detected":true})
    });
    let label = format!("integrity-{index}");
    let mut b = spawn(child(binary, port, 4, index), root, out, &label);
    if index < 26 || index == 28 {
        let text = finish(&mut b, out, &label, 10);
        resource_check(&rows(&text));
    } else {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(s) = b.0.try_wait().unwrap() {
                assert_eq!(s.code(), Some(1));
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        assert!(fs::read_to_string(out.join(format!("{label}.stderr")))
            .unwrap()
            .contains("check tag 10:"));
    }
    peer.join().unwrap()
}
fn startup_faults(binary: &Path, root: &Path, out: &Path) -> Vec<Value> {
    (31..=35)
        .map(|code| {
            let mut c = child(binary, 1, 2, 20);
            c.env("WS_START_FAULT", code.to_string());
            let label = format!("startup-fault-{code}");
            let data = rows(&run(c, root, out, &label));
            resource_check(&data);
            json!({"owned_step":code,"rows":data})
        })
        .collect()
}
fn omitted_release(binary: &Path, root: &Path, out: &Path) -> Value {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let peer = thread::spawn(move || {
        let (s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut ws = tungstenite::accept(s).unwrap();
        ws.send(Message::Binary(payload(42, 1, 256).into()))
            .unwrap();
        assert_eq!(ws.read().unwrap().into_data().as_ref(), payload(42, 0, 256));
        assert!(matches!(ws.read().unwrap(), Message::Close(_)));
        let _ = ws.flush();
    });
    let mut c = child(binary, port, 3, 1);
    c.env("WS_OMIT_RELEASE", "1");
    let mut b = spawn(c, root, out, "omitted-release");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(s) = b.0.try_wait().unwrap() {
            assert_eq!(s.code(), Some(1));
            break;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    peer.join().unwrap();
    let evidence = fs::read_to_string(out.join("omitted-release.stderr")).unwrap();
    assert!(evidence.contains("omitted lease: native bytes"));
    json!({"detected":true,"observer":evidence})
}

fn adverse_lifecycle(binary: &Path, root: &Path, out: &Path, label: &str, fixed: bool) -> Value {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = thread::spawn(move || {
        let (s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
        let accepted = clock();
        thread::sleep(Duration::from_millis(200));
        let mut ws = tungstenite::accept(s).unwrap();
        let upgraded = clock();
        ws.send(Message::Binary(payload(30, 1, 256).into()))
            .unwrap();
        assert_eq!(ws.read().unwrap().into_data().as_ref(), payload(30, 0, 256));
        assert!(matches!(ws.read().unwrap(), Message::Close(_)));
        let close = clock();
        // Do not flush tungstenite's queued close reply: the native deadline must retire it.
        thread::sleep(Duration::from_millis(1200));
        json!({"accepted":accepted,"upgraded":upgraded,"close_received":close,"close_reply_withheld":true})
    });
    let mut c = child(binary, port, 5, 30);
    c.env("WS_EARLY_CLOSE_POLL", "1");
    let mut b = spawn(c, root, out, label);
    if !fixed {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = b.0.try_wait().unwrap() {
                assert_eq!(status.code(), Some(1));
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        let error = fs::read_to_string(out.join(format!("{label}.stderr"))).unwrap();
        assert!(error.contains("check tag 19: 9 != 0"));
        return json!({"legacy_timeout_detected":true,"stderr":error,"peer":peer.join().unwrap()});
    }
    let data = rows(&finish(&mut b, out, label, 5));
    let peer = peer.join().unwrap();
    resource_check(&data);
    let sibling = data
        .iter()
        .find(|r| r["kind"] == "startup_sibling")
        .unwrap();
    assert!(n(&peer, "accepted") < n(sibling, "time") && n(sibling, "time") < n(&peer, "upgraded"));
    let close = data
        .iter()
        .find(|r| r["kind"] == "close" && r["role"] == 2)
        .unwrap();
    assert_eq!(n(close, "code"), 0);
    assert!((800_000_000..2_000_000_000).contains(&n(close, "elapsed_ns")));
    json!({"rows":data,"peer":peer})
}

#[test]
fn phase4_real_bend() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .unwrap();
    let cache = env::var_os("WS_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".ws-cache"));
    let out = root.join(format!(
        "results/ws/cargo-{}-{}/phase-4",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        std::process::id()
    ));
    fs::create_dir_all(&out).unwrap();
    println!("Phase 4 evidence: {}", out.display());
    fs::write(out.join("summary.json"), "{\"pass\":false}").unwrap();
    let bend = cache.join("bend/bin/bend");
    let clang = PathBuf::from("/usr/lib/llvm-19/bin/clang");
    let mut c = Command::new(&bend);
    c.arg("version");
    assert_eq!(run(c, &root, &out, "bend-version").trim(), "bend 2.0.40");
    let original = out.join("original.c");
    let mut c = Command::new(&bend);
    c.arg("bench/ws/bend-phase4.bend").arg("-o").arg(&original);
    run(c, &root, &out, "emit");
    let source = out.join("instrumented.c");
    fs::write(&source, instrument(fs::read_to_string(&original).unwrap())).unwrap();
    let deps = env::current_exe().unwrap().parent().unwrap().to_owned();
    let marker = confluxion_ws_native::build_marker();
    let archives: Vec<_> = fs::read_dir(deps)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.extension().is_some_and(|x| x == "a")
                && p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("libconfluxion_ws_native-")
        })
        .filter(|p| {
            fs::read(p)
                .unwrap()
                .windows(marker.len())
                .any(|b| b == marker.as_bytes())
        })
        .collect();
    assert_eq!(archives.len(), 1);
    let binary = out.join("bend-phase4");
    let mut c = Command::new(&clang);
    c.args(["-O2", "-g", "-std=c11", "-DBEND_POISON"])
        .arg("-I")
        .arg(root.join("src/ws/native"))
        .arg(&source)
        .arg(&archives[0])
        .args(["-lpthread", "-lm", "-ldl"])
        .arg("-o")
        .arg(&binary);
    run(c, &root, &out, "build-bend");
    assert!(fs::read(&binary).unwrap().starts_with(b"\x7fELF"));
    let legacy_source = out.join("legacy-close.c");
    let mut legacy = fs::read_to_string(&source).unwrap();
    replace(&mut legacy,
        "  if (code == WS_PENDING) return io_wait_on(w, 0, 0, io_tick()+100000ull, ws_stop_more);",
        "  if (code == WS_PENDING && io_tick() < (u64)w->made) return io_wait_on(w, 0, 0, io_tick()+100000ull, ws_stop_more);\n  if (code == WS_PENDING) return WS_FAILED;");
    fs::write(&legacy_source, legacy).unwrap();
    let legacy_binary = out.join("legacy-close");
    let mut c = Command::new(&clang);
    c.args(["-O2", "-std=c11", "-DBEND_POISON"])
        .arg("-I")
        .arg(root.join("src/ws/native"))
        .arg(&legacy_source)
        .arg(&archives[0])
        .args(["-lpthread", "-lm", "-ldl"])
        .arg("-o")
        .arg(&legacy_binary);
    run(c, &root, &out, "build-legacy-close");
    let legacy_close =
        adverse_lifecycle(&legacy_binary, &root, &out, "legacy-close-control", false);
    let adverse = adverse_lifecycle(&binary, &root, &out, "adverse-lifecycle", true);
    let peer = build_peer(&root, &out, &cache, &clang);
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    let mut c = Command::new(peer);
    c.arg(port.to_string())
        .arg("1000000")
        .arg(out.join("peer.json"));
    let mut p = spawn(c, &root, &out, "million-peer");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !fs::read_to_string(out.join("million-peer.stdout"))
        .unwrap_or_default()
        .contains("LISTENING")
    {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    let mut b = spawn(child(&binary, port, 0, 0), &root, &out, "million-bend");
    let text = finish(&mut b, &out, "million-bend", 600);
    finish(&mut p, &out, "million-peer", 5);
    let million = rows(&text);
    let status = million
        .iter()
        .find(|r| r["kind"] == "status" && r["tag"] == 1)
        .unwrap();
    for k in ["tx", "rx", "admitted", "sent", "received", "consumed"] {
        assert_eq!(n(status, k), 1_000_000);
    }
    assert_eq!(n(status, "failed"), 0);
    resource_check(&million);
    let diagnostic = million
        .iter()
        .find(|r| r["kind"] == "diagnostics" && r["tag"] == 1)
        .unwrap();
    for key in ["single_sent", "single_received"] {
        assert_eq!(n(diagnostic, key), 250_000);
    }
    for key in ["batch_sent", "batch_received"] {
        assert_eq!(n(diagnostic, key), 750_000);
    }
    let peer: Value = serde_json::from_slice(&fs::read(out.join("peer.json")).unwrap()).unwrap();
    assert_eq!(n(&peer, "validated"), 1_000_000);
    assert_eq!(n(&peer, "sent"), 1_000_000);
    assert!(peer["failure"].is_null());
    let mut attempts = vec![];
    for iterations in [128_000_000u32, 256_000_000, 512_000_000] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut fds = [0; 2];
        assert_eq!(
            unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK) },
            0
        );
        let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        let seed = unsafe { libc::getpid() } as u32 ^ clock() as u32;
        let peer = thread::spawn(move || {
            let (s, _) = listener.accept().unwrap();
            s.set_nodelay(true).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            s.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut ws = tungstenite::accept(s).unwrap();
            ws.send(Message::Binary(seed.to_be_bytes().to_vec().into()))
                .unwrap();
            let mut start = 0u64;
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let n = unsafe { libc::read(read.as_raw_fd(), &mut start as *mut _ as *mut _, 8) };
                if n == 8 {
                    break;
                }
                assert!(Instant::now() < deadline);
                thread::sleep(Duration::from_micros(100));
            }
            while clock() < start + 10_000_000 {
                thread::sleep(Duration::from_micros(100));
            }
            let resume = clock();
            let prior = recurrence(seed, 100);
            let mut receipt = 0;
            for (seq, len) in [(100, 65536), (101, 65536), (prior, 256)] {
                let m = ws.read().unwrap();
                assert_eq!(m.into_data().as_ref(), payload(seq, 0, len));
                receipt = clock();
            }
            ws.send(Message::Binary(payload(777, 1, 256).into()))
                .unwrap();
            let ping = clock();
            ws.send(Message::Ping(b"phase4".to_vec().into())).unwrap();
            let pong = loop {
                match ws.read().unwrap() {
                    Message::Pong(p) => {
                        assert_eq!(&*p, b"phase4");
                        break clock();
                    }
                    Message::Close(_) => panic!("close before Pong"),
                    m => panic!("unexpected {m:?}"),
                }
            };
            loop {
                match ws.read() {
                    Ok(Message::Close(_)) => {
                        let _ = ws.flush();
                        break;
                    }
                    Err(tungstenite::Error::ConnectionClosed) => break,
                    m => panic!("expected close: {m:?}"),
                }
            }
            json!({"seed":seed,"receipt_ns":receipt,"resume_ns":resume,"ping_ns":ping,"pong_ns":pong})
        });
        let label = format!("compute-{iterations}");
        let mut c = child(&binary, port, 1, iterations);
        c.env("WS_OBSERVER_FD", write.as_raw_fd().to_string());
        let mut b = spawn(c, &root, &out, &label);
        drop(write);
        let text = finish(&mut b, &out, &label, 60);
        let mut peer = peer.join().unwrap();
        let data = rows(&text);
        let compute = data.iter().find(|r| r["kind"] == "compute").unwrap();
        let status = data.iter().find(|r| r["kind"] == "status").unwrap();
        let pending = data.iter().find(|r| r["kind"] == "pending_result").unwrap();
        assert!(n(pending, "time") < n(compute, "start"));
        assert_eq!(n(pending, "admitted"), 3);
        assert!(n(pending, "complete") < 3);
        peer["bend_validated"] = json!(data.iter().any(|r| r["kind"] == "bend_compute_input"
            && r["validated"] == true
            && r["next_identity"] == 778));
        resource_check(&data);
        let duration = n(compute, "end") - n(compute, "start");
        attempts
            .push(json!({"iterations":iterations,"interval_ns":duration,"rows":data,"peer":peer}));
        if duration >= 100_000_000 {
            validate_compute(compute, status, &peer).unwrap();
            damaged_compute(compute, status, &peer);
            break;
        }
    }
    assert!(attempts.iter().any(|v| n(v, "interval_ns") >= 100_000_000));
    let mut finite_cases = vec![];
    for index in (6..21).chain(22..24) {
        finite_cases.push(finite(&binary, &root, &out, index, "ownership"));
    }
    finite_cases.push(finite(&binary, &root, &out, 29, "ownership"));
    for cycle in 0..100 {
        finite_cases.push(finite(
            &binary,
            &root,
            &out,
            (cycle / 2) % 3 + (cycle % 2) * 3,
            &format!("pause-{cycle}"),
        ));
    }
    let owned_lifecycle = lifecycle(&binary, &root, &out, "ownership", 1000);
    let sanitizer = out.join("bend-phase4-sanitized");
    let mut c = Command::new(&clang);
    c.args([
        "-O0",
        "-g",
        "-std=c11",
        "-DBEND_POISON",
        "-fsanitize=address,undefined",
        "-fno-omit-frame-pointer",
    ])
    .arg("-I")
    .arg(root.join("src/ws/native"))
    .arg(&source)
    .arg(&archives[0])
    .args(["-lpthread", "-lm", "-ldl"])
    .arg("-o")
    .arg(&sanitizer);
    run(c, &root, &out, "build-sanitizer");
    for index in 6..21 {
        finite_cases.push(finite(&sanitizer, &root, &out, index, "asan-ubsan"));
    }
    finite_cases.push(finite(&sanitizer, &root, &out, 29, "asan-ubsan"));
    let adverse_sanitized =
        adverse_lifecycle(&sanitizer, &root, &out, "adverse-lifecycle-sanitized", true);
    let sanitized_lifecycle = lifecycle(&sanitizer, &root, &out, "asan-ubsan", 1000);
    let integrity_cases: Vec<_> = (24..=28)
        .map(|i| integrity(&binary, &root, &out, i))
        .collect();
    let startup_faults = startup_faults(&binary, &root, &out);
    let omitted = omitted_release(&binary, &root, &out);
    fs::write(out.join("summary.json"),serde_json::to_vec_pretty(&json!({"pass":true,"million":million,"compute_attempts":attempts,"peer":peer,"archive":archives[0],"marker":marker,"finite":finite_cases,"lifecycle":[owned_lifecycle,sanitized_lifecycle],"adverse_lifecycle":[adverse,adverse_sanitized],"legacy_close_control":legacy_close,"integrity":integrity_cases,"damaged_compute_controls":5,"startup_faults":startup_faults,"omitted_release":omitted})).unwrap()).unwrap();
}
