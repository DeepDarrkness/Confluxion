use serde_json::{json, Value};
use std::{
    env, fs,
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn replace_once(text: &mut String, from: &str, to: &str) {
    assert_eq!(
        text.matches(from).count(),
        1,
        "compiler instrumentation anchor changed: {from}"
    );
    *text = text.replacen(from, to, 1);
}

fn instrument(mut c: String) -> String {
    let hook = r#"
static _Atomic uint64_t ws_bend_blocks, ws_bend_bytes;
static unsigned char ws_bend_owned[1u << 24];
static uint64_t ws_track_bend(Env e, uint32_t cls, uint64_t loc) {
  if (err_peek(e.mem)) return loc;
  uint64_t index = loc - HEAP_OFF;
  if (index >= (1u << 24) || ws_bend_owned[index]) { fprintf(stderr, "bad alloc %llu cls %u\n", (unsigned long long)loc, cls); abort(); }
  ws_bend_owned[index] = cls + 1;
  atomic_fetch_add(&ws_bend_blocks, 1);
  atomic_fetch_add(&ws_bend_bytes, 8ull << cls);
  return loc;
}
static void ws_free_bend(uint32_t cls, uint64_t loc) {
  uint64_t index = loc - HEAP_OFF;
  if (index >= (1u << 24) || ws_bend_owned[index] != cls + 1) { fprintf(stderr, "bad free %llu cls %u\n", (unsigned long long)loc, cls); abort(); }
  ws_bend_owned[index] = 0;
  if (atomic_fetch_sub(&ws_bend_blocks, 1) == 0) abort();
  if (atomic_fetch_sub(&ws_bend_bytes, 8ull << cls) < (8ull << cls)) abort();
}
"#;
    replace_once(
        &mut c,
        "INLINE u64 heap_alloc(Env e, u32 cls) {",
        &format!("{hook}\nINLINE u64 heap_alloc(Env e, u32 cls) {{"),
    );
    replace_once(&mut c, "    return h;\n  }\n  return heap_alloc_miss(e, cls);", "    return ws_track_bend(e, cls, h);\n  }\n  return ws_track_bend(e, cls, heap_alloc_miss(e, cls));");
    replace_once(&mut c, "INLINE void heap_free(Env e, u32 cls, u64 loc) {\n  if (err_peek(e.mem)) {\n    return;\n  }", "INLINE void heap_free(Env e, u32 cls, u64 loc) {\n  if (err_peek(e.mem)) {\n    return;\n  }\n  ws_free_bend(cls, loc);");
    let allocations = r#"
static pthread_mutex_t ws_c_lock = PTHREAD_MUTEX_INITIALIZER;
static struct { void* ptr; size_t bytes; } ws_c_owned[4096];
static uint64_t ws_c_blocks, ws_c_bytes, ws_c_allocations, ws_c_peak;
static void ws_c_add(void* p, size_t n) {
  if (!p) return;
  pthread_mutex_lock(&ws_c_lock);
  unsigned i = 0;
  while (i < 4096 && ws_c_owned[i].ptr) i++;
  if (i == 4096) abort();
  ws_c_owned[i].ptr = p; ws_c_owned[i].bytes = n;
  ws_c_blocks++; ws_c_bytes += n; ws_c_allocations++;
  if (ws_c_bytes > ws_c_peak) ws_c_peak = ws_c_bytes;
  pthread_mutex_unlock(&ws_c_lock);
}
static void ws_c_remove(void* p) {
  if (!p) return;
  pthread_mutex_lock(&ws_c_lock);
  unsigned i = 0;
  while (i < 4096 && ws_c_owned[i].ptr != p) i++;
  if (i == 4096 || ws_c_blocks == 0) { fprintf(stderr, "unknown native free %p\n", p); abort(); }
  ws_c_blocks--; ws_c_bytes -= ws_c_owned[i].bytes; ws_c_owned[i].ptr = NULL;
  pthread_mutex_unlock(&ws_c_lock);
}
static void* ws_malloc(size_t n) { void* p = malloc(n); ws_c_add(p, n); return p; }
static void* ws_calloc(size_t n, size_t size) { void* p = calloc(n, size); ws_c_add(p, n * size); return p; }
static void ws_free(void* p) { ws_c_remove(p); free(p); }
static void* ws_realloc(void* p, size_t n) {
  void* q = realloc(p, n);
  if (q || n == 0) { ws_c_remove(p); ws_c_add(q, n); }
  return q;
}
static char* ws_strdup(const char* s) { size_t n = strlen(s) + 1; char* p = malloc(n); if (p) memcpy(p, s, n); ws_c_add(p, n); return p; }
#define malloc ws_malloc
#define calloc ws_calloc
#define realloc ws_realloc
#define free ws_free
#define strdup ws_strdup
"#;
    replace_once(
        &mut c,
        "// Dialect\n// =======",
        &format!("{allocations}\n// Dialect\n// ======="),
    );
    c
}

fn optional_executable(name: std::ffi::OsString) -> Option<PathBuf> {
    let path = PathBuf::from(&name);
    if path.components().count() > 1 {
        return path.canonicalize().ok();
    }
    env::split_paths(&env::var_os("PATH").unwrap_or_default())
        .map(|directory| directory.join(&name))
        .find(|candidate| candidate.is_file())
}

fn executable(name: std::ffi::OsString) -> PathBuf {
    optional_executable(name.clone())
        .unwrap_or_else(|| panic!("required executable not found: {}", name.to_string_lossy()))
}

fn select_archive(deps: &Path, marker: &[u8]) -> PathBuf {
    assert!(!marker.is_empty());
    let mut matching: Vec<_> = fs::read_dir(deps)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("libconfluxion_ws_native-")
                && p.extension().is_some_and(|e| e == "a")
                && p.is_file()
        })
        .filter(|p| {
            let bytes = fs::read(p).unwrap();
            bytes.starts_with(b"!<arch>\n")
                && bytes.windows(marker.len()).any(|part| part == marker)
        })
        .collect();
    matching.sort();
    assert!(
        !matching.is_empty(),
        "missing archive for the CURRENT linked native build"
    );
    matching.into_iter().next().unwrap()
}

struct Run {
    root: PathBuf,
    output: PathBuf,
    clang: PathBuf,
    commands: Vec<Value>,
}
impl Run {
    fn command(&mut self, program: &Path, args: &[String], name: &str, success: bool) -> String {
        let mut c = Command::new(program);
        let mut paths = vec![self.clang.parent().unwrap().to_path_buf()];
        paths.extend(env::split_paths(&env::var_os("PATH").unwrap_or_default()));
        c.current_dir(&self.root)
            .args(args)
            .env_remove("BENDRTS")
            .env("BEND_NO_TELEMETRY", "1")
            .env("PATH", env::join_paths(paths).unwrap())
            .env("CC", &self.clang)
            .env("RUST_BACKTRACE", "0")
            .env("RUST_LIB_BACKTRACE", "0")
            .env("ASAN_OPTIONS", "detect_leaks=1:halt_on_error=1")
            .env("UBSAN_OPTIONS", "halt_on_error=1:print_stacktrace=1")
            .stdin(Stdio::null())
            .stdout(fs::File::create(self.output.join(format!("{name}.stdout"))).unwrap())
            .stderr(fs::File::create(self.output.join(format!("{name}.stderr"))).unwrap());
        self.commands.push(json!({"program":program,"args":args,"name":name,"deadline_seconds":60,
            "environment":c.get_envs().map(|(k,v)| (k.to_string_lossy(),v.map(|v|v.to_string_lossy()))).collect::<Vec<_>>()}));
        unsafe {
            c.pre_exec(|| {
                let limits = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                if libc::setrlimit(libc::RLIMIT_CORE, &limits) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::prctl(libc::PR_SET_DUMPABLE, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        fs::write(
            self.output.join("commands.json"),
            serde_json::to_vec_pretty(&self.commands).unwrap(),
        )
        .unwrap();
        let mut child = c.spawn().unwrap_or_else(|error| panic!("{name}: {error}"));
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("fixture deadline: {name}");
            }
            thread::sleep(Duration::from_millis(5));
        };
        let out = fs::read_to_string(self.output.join(format!("{name}.stdout"))).unwrap();
        let err = fs::read_to_string(self.output.join(format!("{name}.stderr"))).unwrap();
        assert_eq!(status.success(), success, "{name}: {status}\n{out}\n{err}");
        if success {
            out
        } else {
            out + &err
        }
    }
    fn build(
        &mut self,
        clang: &Path,
        c: &Path,
        archive: &Path,
        name: &str,
        sanitize: bool,
    ) -> PathBuf {
        let binary = self.output.join(name);
        let mut args = vec![
            "-std=c11".into(),
            "-O2".into(),
            "-g".into(),
            "-DBEND_POISON".into(),
            "-I".into(),
            self.root.join("src/ws/native").display().to_string(),
            c.display().to_string(),
            archive.display().to_string(),
            "-lpthread".into(),
            "-lm".into(),
            "-ldl".into(),
            "-o".into(),
            binary.display().to_string(),
        ];
        if sanitize {
            args[1] = "-O0".into();
            args.extend([
                "-fsanitize=address,undefined".into(),
                "-fno-omit-frame-pointer".into(),
            ]);
        }
        self.command(clang, &args, &format!("build-{name}"), true);
        assert!(fs::read(&binary).unwrap().starts_with(b"\x7fELF"));
        binary
    }
}

fn n(v: &Value, key: &str) -> u64 {
    v[key]
        .as_u64()
        .unwrap_or_else(|| panic!("missing {key}: {v}"))
}
fn validate(rows: &[Value]) {
    let checkpoints: Vec<_> = rows.iter().filter(|r| r["kind"] == "checkpoint").collect();
    assert_eq!(checkpoints.len(), 1014);
    let baseline = checkpoints.iter().find(|r| r["tag"] == 0).unwrap();
    let retained: Vec<_> = rows.iter().filter(|r| r["kind"] == "retained").collect();
    assert_eq!(retained.len(), 2);
    assert_eq!(n(retained[0], "tag"), 1);
    assert_eq!(n(retained[1], "tag"), 2);
    assert!(n(retained[0], "bend_blocks") >= n(baseline, "bend_blocks") + 64);
    assert!(n(retained[0], "bend_bytes") >= n(baseline, "bend_bytes") + 64 * 16);
    for key in ["bend_blocks", "bend_bytes"] {
        assert_eq!(retained[0][key], retained[1][key]);
    }
    let cycles: Vec<_> = checkpoints
        .iter()
        .filter(|r| (1..=1000).contains(&n(r, "tag")))
        .collect();
    assert_eq!(cycles.len(), 1000);
    for (i, row) in cycles.iter().enumerate() {
        assert_eq!(n(row, "tag"), 1000 - i as u64);
    }
    for r in &checkpoints {
        for key in [
            "entries",
            "descriptors",
            "workers",
            "cells",
            "native_bytes",
            "native_blocks",
        ] {
            assert_eq!(n(r, key), 0, "{key}: {r}");
        }
        assert!(n(r, "join_ns") < 1_000_000_000);
        assert!(n(r, "native_peak") <= 4096);
        assert_eq!(n(r, "layout_overflow"), 0);
        assert_eq!(n(r, "wake_errors"), 0);
        assert!(
            n(r, "join_started_ns") > 0 && n(r, "join_completed_ns") >= n(r, "join_started_ns")
        );
        assert!(n(r, "join_completed_ns") - n(r, "join_started_ns") < 1_000_000_000);
        assert_eq!(n(r, "stack_bytes"), 262144);
        for key in ["os_fds", "os_threads"] {
            assert_eq!(r[key], baseline[key], "{key}: {r}");
        }
    }
    let bend_baseline = checkpoints.iter().find(|r| r["tag"] == 1001).unwrap();
    for (i, r) in cycles.iter().enumerate() {
        for key in ["bend_blocks", "bend_bytes", "c_blocks", "c_bytes"] {
            assert_eq!(r[key], bend_baseline[key], "{key}: {r}");
        }
        assert_eq!(
            n(r, "validated_messages"),
            n(bend_baseline, "validated_messages") + 3 * (i as u64 + 1)
        );
        assert_eq!(
            n(r, "validated_bytes"),
            n(bend_baseline, "validated_bytes") + 260 * (i as u64 + 1)
        );
        let previous = if i == 0 {
            *bend_baseline
        } else {
            *cycles[i - 1]
        };
        assert!(n(r, "join_started_ns") >= n(previous, "join_completed_ns"));
    }
    for r in checkpoints
        .iter()
        .filter(|r| n(r, "tag") != 3000 && (n(r, "tag") > 1001 || n(r, "tag") == 0))
    {
        for key in ["bend_blocks", "bend_bytes", "c_blocks", "c_bytes"] {
            assert_eq!(r[key], baseline[key], "{key}: {r}");
        }
    }
    let terminal = checkpoints.iter().find(|r| r["tag"] == 3000).unwrap();
    assert_eq!(n(terminal, "bend_blocks"), 0);
    assert_eq!(n(terminal, "bend_bytes"), 0);
    let layouts: Vec<_> = rows
        .iter()
        .filter(|r| r["kind"] == "allocation_layout")
        .collect();
    assert!(!layouts.is_empty() && layouts.len() <= 16);
    assert_eq!(
        layouts.iter().map(|r| n(r, "allocated")).sum::<u64>(),
        n(terminal, "allocations")
    );
    let mut layout_bound = 0;
    for r in layouts {
        assert_eq!(n(r, "allocated"), n(r, "freed"));
        assert_eq!(n(r, "live"), 0);
        layout_bound += n(r, "size") * n(r, "peak");
    }
    assert!(layout_bound >= n(terminal, "native_peak") && layout_bound <= 4096);
    assert!(
        n(baseline, "allocations") > 0 && n(baseline, "native_peak") > n(baseline, "core_size")
    );
    let faults: Vec<_> = rows.iter().filter(|r| r["kind"] == "fault").collect();
    const CASES: &[(&str, u64)] = &[
        ("start_busy", 8),
        ("wrong_role_offer", 6),
        ("foreign_handle", 4),
        ("wrong_role_stop", 6),
        ("oversize_native", 4),
        ("byte_full", 2),
        ("cell_full", 2),
        ("release_restores_admission", 0),
        ("repeated_shutdown", 7),
        ("post_close_offer", 3),
        ("leases_survive_stop", 8),
        ("repeat_release", 7),
        ("retired_generation", 7),
        ("stale_generation", 5),
        ("queued_close_abandoned", 1),
        ("confirmed_parked_receiver", 1),
        ("startup_1", 9),
        ("startup_2", 9),
        ("startup_3", 9),
        ("startup_4", 9),
        ("startup_5", 9),
        ("held_bend_lease", 1),
    ];
    assert_eq!(faults.len(), CASES.len());
    for (case, expected) in CASES {
        let found: Vec<_> = faults.iter().filter(|r| r["case"] == *case).collect();
        assert_eq!(found.len(), 1, "missing/duplicate {case}");
        assert_eq!(n(found[0], "actual"), *expected);
    }
    for tag in 2001..=2010 {
        assert_eq!(checkpoints.iter().filter(|r| n(r, "tag") == tag).count(), 1);
    }
    let boundary: Vec<_> = rows
        .iter()
        .filter(|r| r["kind"] == "bend_boundary")
        .collect();
    let expected = [
        (20, 4),
        (21, 0),
        (20, 4),
        (21, 0),
        (18, 2),
        (19, 0),
        (24, 0),
        (25, 0),
        (20, 10),
        (21, 0),
        (29, 10),
    ];
    assert_eq!(boundary.len(), expected.len());
    for (r, (tag, code)) in boundary.into_iter().zip(expected) {
        assert_eq!(n(r, "tag"), tag);
        assert_eq!(n(r, "actual"), code);
    }
}

#[test]
fn phase1_ownership() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .unwrap();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let output = root.join(format!(
        "results/ws/cargo-{stamp}-{}/phase-1",
        std::process::id()
    ));
    fs::create_dir_all(&output).unwrap();
    println!("Phase 1 evidence: {}", output.display());
    let cache = env::var_os("WS_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".ws-cache"))
        .canonicalize()
        .unwrap();
    let bend = executable(
        env::var_os("WS_BEND")
            .or_else(|| env::var_os("BEND"))
            .unwrap_or_else(|| {
                let cached = cache.join("bend/bin/bend");
                if cached.is_file() {
                    cached.into_os_string()
                } else {
                    "bend".into()
                }
            }),
    );
    let clang = executable(
        env::var_os("CC")
            .or_else(|| env::var_os("CLANG"))
            .unwrap_or_else(|| "clang".into()),
    );
    let mut run = Run {
        root: root.clone(),
        output: output.clone(),
        clang: clang.clone(),
        commands: vec![],
    };
    fs::write(output.join("summary.json"), "{\"phase_pass\":false}").unwrap();
    assert_eq!(
        run.command(&bend, &["version".into()], "bend-version", true)
            .trim(),
        "bend 2.0.40"
    );
    assert!(run
        .command(&clang, &["--version".into()], "clang-version", true)
        .contains("clang version"));
    let rust_version = run.command(
        Path::new("rustc"),
        &["--version".into(), "--verbose".into()],
        "rust-version",
        true,
    );
    let deps = env::current_exe().unwrap().parent().unwrap().to_path_buf();
    let selected_archive = select_archive(&deps, confluxion_ws_native::build_marker().as_bytes());
    let archive = &selected_archive;
    let digest = run.command(
        Path::new("sha256sum"),
        &[archive.display().to_string()],
        "archive-sha256",
        true,
    );
    let negative_bend = output.join("affine.c");
    let verdict = run.command(
        &bend,
        &[
            "tests/ws/affine.bend".into(),
            "-o".into(),
            negative_bend.display().to_string(),
        ],
        "affine-negative",
        false,
    );
    assert!(
        verdict.contains("consumed more than once") && !negative_bend.exists(),
        "affine reuse was not rejected: {verdict}"
    );
    let c = output.join("original.c");
    run.command(
        &bend,
        &[
            "tests/ws/phase1.bend".into(),
            "-o".into(),
            c.display().to_string(),
        ],
        "emit",
        true,
    );
    let instrumented = instrument(fs::read_to_string(&c).unwrap());
    let c = output.join("instrumented.c");
    fs::write(&c, &instrumented).unwrap();
    let rts = ["+RTS", "--threads", "1", "--gpu", "off", "-RTS"].map(str::to_owned);
    let mut modes = vec![];
    for (mode, sanitize) in [("ownership", false), ("asan-ubsan", true)] {
        let binary = run.build(&clang, &c, archive, mode, sanitize);
        let out = run.command(&binary, &rts, &format!("run-{mode}"), true);
        let rows: Vec<Value> = out
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        validate(&rows);
        fs::write(
            output.join(format!("{mode}.json")),
            serde_json::to_vec_pretty(&rows).unwrap(),
        )
        .unwrap();
        if !sanitize {
            let mut missing = rows.clone();
            missing.pop();
            assert!(std::panic::catch_unwind(|| validate(&missing)).is_err());
            let mut wrong = rows.clone();
            wrong
                .iter_mut()
                .find(|r| r["kind"] == "checkpoint" && r["tag"] == 10)
                .unwrap()["native_bytes"] = json!(1);
            assert!(std::panic::catch_unwind(|| validate(&wrong)).is_err());
            let mut bad_count = rows.clone();
            bad_count
                .iter_mut()
                .find(|r| r["kind"] == "checkpoint" && r["tag"] == 10)
                .unwrap()["validated_messages"] = json!(1);
            assert!(std::panic::catch_unwind(|| validate(&bad_count)).is_err());
            let mut bad_fault = rows.clone();
            bad_fault
                .iter_mut()
                .find(|r| r["case"] == "stale_generation")
                .unwrap()["actual"] = json!(0);
            assert!(std::panic::catch_unwind(|| validate(&bad_fault)).is_err());
        }
        let baseline = rows
            .iter()
            .find(|r| r["kind"] == "checkpoint" && r["tag"] == 1001)
            .unwrap();
        let layout_bound: u64 = rows
            .iter()
            .filter(|r| r["kind"] == "allocation_layout")
            .map(|r| n(r, "size") * n(r, "peak"))
            .sum();
        modes.push(json!({"mode":mode,"cycles":1000,"case_count":22,"bend_boundary_checks":11,
            "bend_idle_blocks":n(baseline,"bend_blocks"),"bend_idle_bytes":n(baseline,"bend_bytes"),"layout_derived_native_bound":layout_bound}));
    }
    let negative = output.join("negative.c");
    let mut damaged = instrumented;
    replace_once(
        &mut damaged,
        "spare_free(e, cls_fit(2), ctr_take(e, s, 2, fields));",
        "ctr_take(e, s, 2, fields);",
    );
    fs::write(&negative, damaged).unwrap();
    let binary = run.build(&clang, &negative, archive, "leak-control", false);
    let out = run.command(&binary, &rts, "run-leak-control", true);
    let rows: Vec<Value> = out
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert!(
        std::panic::catch_unwind(|| validate(&rows)).is_err(),
        "observer missed omitted argument release"
    );
    fs::write(output.join("summary.json"), serde_json::to_vec_pretty(&json!({"phase_pass":true,"bend":"2.0.40",
            "archive":archive,"archive_sha256":digest.split_whitespace().next(),"linked_build_marker":confluxion_ws_native::build_marker(),"compiler_cache":cache,"bend_executable":bend,"clang_executable":clang,"instrumentation_version":1,"modes":modes,
        "damaged_evidence_rejected":["missing_record","native_live_bytes","validated_count","fault_outcome"],
        "negative_allocator_control_rejected":true,"affine_duplication_rejected":true,"rust_version":rust_version,"rust_compiler_sanitizers":false,
        "byte_storage":2048,"queue_bytes_per_direction":512,"queue_descriptors_per_direction":4,"native_observed_budget":4096,"worker_stack_requested":262144,
        "sanitizer_build_constraint":"Clang 19 optimized preserve_* calling conventions fail ASan backend stack realignment; -O0 builds the same emitted fixture without those conventions",
        "allocation_attribution":"all Rust GlobalAlloc events and direct emitted-C allocator calls; observer and libc-internal caches are outside client attribution and are checked by OS baseline and sanitizer interceptors",
        "sanitizer_coverage":"clang-instrumented Bend runtime and C adapters; Rust System allocations intercepted by ASan, Rust code not compiler-instrumented"})).unwrap()).unwrap();
}
