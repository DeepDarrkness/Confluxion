use serde_json::{json, Value};
use std::{
    env, fs,
    io::{Read, Write},
    net::TcpListener,
    os::unix::{io::AsRawFd, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const ACTORS: [&str; 2] = ["client", "server"];
const NS: u64 = 1_000_000_000;
const QUEUE: u64 = 2_097_152;
const DESCRIPTORS: u64 = 4096;
const HEAP: u64 = 134_217_728;

fn require(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn number(value: &Value, key: &str) -> Result<u64> {
    value[key]
        .as_u64()
        .ok_or_else(|| format!("invalid/missing {key}").into())
}

fn save(path: &Path, value: &Value) -> Result<()> {
    fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

fn monotonic_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) },
        0
    );
    time.tv_sec as u64 * NS + time.tv_nsec as u64
}

struct Plan {
    id: String,
    mode: &'static str,
    size: u64,
    rate: u64,
    seconds: u64,
    drain: u64,
    token: u64,
    start: u64,
    end: u64,
    fault: Option<(&'static str, usize)>,
    paused: Option<usize>,
}

struct Trial {
    plan: Plan,
    actor_reports: Vec<Value>,
    statuses: Vec<i32>,
}

fn validate_reports(trial: &Trial) -> Result<()> {
    require(
        trial.actor_reports.len() == 2 && trial.statuses.len() == 2,
        "missing actor report/status",
    )?;
    let p = &trial.plan;
    for (actor, report) in ACTORS.iter().zip(&trial.actor_reports) {
        require(report["actor"] == *actor, "actor mismatch")?;
        for (key, expected) in [
            ("schema", 1),
            ("token", p.token),
            ("start_ns", p.start),
            ("end_ns", p.end),
            ("queue_limit_bytes", QUEUE),
            ("queue_limit_descriptors", DESCRIPTORS),
            ("tracked_heap_limit_bytes", HEAP),
        ] {
            require(number(report, key)? == expected, key)?;
        }
        let peak = number(report, "tracked_heap_peak_bytes")?;
        require(
            number(report, "allocation_count")? > 0
                && peak > 0
                && peak <= HEAP
                && number(report, "tracked_heap_live_bytes")? <= peak,
            "heap accounting/budget",
        )?;
        let finished = number(report, "finished_ns")?;
        require(
            finished >= p.start && finished <= p.end + (p.drain + 1) * NS,
            "finish deadline",
        )?;
        let failure = report.get("failure").ok_or("missing failure field")?;
        require(
            failure.is_null() || failure["kind"].is_string(),
            "malformed failure",
        )?;
        let rows = report["streams"].as_array().ok_or("missing streams")?;
        require(rows.len() == 2, "missing/duplicate stream")?;
        let mut seen = [false; 2];
        for row in rows {
            require(number(row, "connection")? == 0, "connection mismatch")?;
            let epoch = number(row, "epoch")?;
            require(
                epoch < 2 && !seen[epoch as usize],
                "missing/duplicate epoch",
            )?;
            seen[epoch as usize] = true;
            require(
                number(row, "queue_highwater_bytes")? <= QUEUE
                    && number(row, "queue_highwater_descriptors")? <= DESCRIPTORS
                    && number(row, "sndbuf")? > 0
                    && number(row, "rcvbuf")? > 0,
                "queue/kernel buffer evidence",
            )?;
            for role in ["tx", "rx"] {
                let flow = &row[role];
                for key in [
                    "scheduled",
                    "attempted",
                    "admitted",
                    "transmitted",
                    "validated",
                    "validated_in_window",
                    "validated_after_window",
                    "rejected",
                ] {
                    number(flow, key)?;
                }
                let digest = flow["digest"].as_str().ok_or("missing digest")?;
                require(
                    !digest.is_empty() && digest.bytes().all(|b| b.is_ascii_digit()),
                    "invalid digest",
                )?;
                digest.parse::<u64>()?;
                require(
                    number(flow, "validated_in_window")?
                        .checked_add(number(flow, "validated_after_window")?)
                        == Some(number(flow, "validated")?),
                    "window/drain count mismatch",
                )?;
            }
        }
    }
    Ok(())
}

fn stream(report: &Value, epoch: u64) -> &Value {
    report["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["epoch"] == epoch)
        .unwrap()
}

fn evaluate(trial: &Trial) -> Result<()> {
    validate_reports(trial)?;
    let p = &trial.plan;
    for (report, status) in trial.actor_reports.iter().zip(&trial.statuses) {
        require(
            *status == 0 && report["failure"].is_null(),
            "actor failure/exit",
        )?;
        require(
            number(report, "finished_ns")? >= p.end + p.drain * NS,
            "shortened trial",
        )?;
    }
    for epoch in 0..2 {
        let count = if epoch == 0 { 0 } else { p.rate * p.seconds };
        for direction in 0..2 {
            let tx = &stream(&trial.actor_reports[direction], epoch)["tx"];
            let rx = &stream(&trial.actor_reports[1 - direction], epoch)["rx"];
            require(
                number(tx, "scheduled")? == count && number(rx, "scheduled")? == count,
                "scheduled count mismatch",
            )?;
            let attempted = number(tx, "attempted")?;
            let admitted = number(tx, "admitted")?;
            let transmitted = number(tx, "transmitted")?;
            let validated = number(rx, "validated")?;
            let rejected = number(tx, "rejected")?;
            if p.paused.is_some() {
                require(
                    validated <= transmitted
                        && transmitted <= admitted
                        && admitted <= attempted
                        && attempted <= count
                        && admitted.checked_add(rejected) == Some(attempted)
                        && admitted == transmitted
                        && transmitted == validated,
                    "saturation accounting/drain mismatch",
                )?;
            } else {
                require(
                    attempted == count
                        && admitted == count
                        && transmitted == count
                        && validated == count
                        && rejected == 0,
                    "counts mismatch",
                )?;
            }
            require(tx["digest"] == rx["digest"], "identity digest mismatch")?;
        }
    }
    Ok(())
}

fn validate_fault(trial: &Trial, kind: &str, direction: usize) -> Result<()> {
    validate_reports(trial)?;
    require(evaluate(trial).is_err(), "false successful fault")?;
    let receiver = 1 - direction;
    let failure = &trial.actor_reports[receiver]["failure"];
    require(
        trial.statuses[receiver] == 1 && failure["kind"] == kind,
        "wrong fault/status",
    )?;
    for (key, expected) in [
        ("direction", direction as u64),
        ("connection", 0),
        ("epoch", 1),
        ("sequence", 50),
    ] {
        require(number(failure, key)? == expected, "wrong fault identity")?;
    }
    require(
        number(
            &stream(&trial.actor_reports[receiver], 1)["rx"],
            "validated",
        )? == if kind == "duplicate" { 51 } else { 50 },
        "wrong fault receive count",
    )?;
    let other = &trial.actor_reports[direction]["failure"];
    require(
        (other.is_null() && trial.statuses[direction] == 0)
            || (other["kind"] == "transport" && trial.statuses[direction] == 1),
        "unexpected sender fault/status",
    )
}

struct Process(Child);

impl Process {
    fn wait(&mut self, deadline: Instant) -> Result<i32> {
        loop {
            if let Some(status) = self.0.try_wait()? {
                return status
                    .code()
                    .ok_or_else(|| "process terminated by signal".into());
            }
            require(Instant::now() < deadline, "process deadline expired")?;
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn ready(&mut self, expected: &str, timeout: u64) -> Result<()> {
        let stdout = self.0.stdout.as_mut().ok_or("missing readiness pipe")?;
        let fd = stdout.as_raw_fd();
        require(
            unsafe { libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK) } != -1,
            "readiness nonblocking setup",
        )?;
        let deadline = Instant::now() + Duration::from_secs(timeout);
        let mut line = Vec::new();
        loop {
            require(
                Instant::now() < deadline && line.len() < 256,
                "readiness deadline/line limit",
            )?;
            let mut byte = [0];
            match stdout.read(&mut byte) {
                Ok(0) => return Err("actor exited before readiness".into()),
                Ok(_) if byte[0] == b'\n' => {
                    return require(line == expected.as_bytes(), "unexpected readiness")
                }
                Ok(_) => line.push(byte[0]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5))
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Session {
    root: PathBuf,
    cache: PathBuf,
    output: PathBuf,
    profile: Value,
    clang: PathBuf,
    server: PathBuf,
    cpus: [usize; 2],
    commands: Vec<Value>,
    trials: Vec<Value>,
}

impl Session {
    fn new() -> Result<Self> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .canonicalize()?;
        let cache = env::var_os("WS_CACHE")
            .map(PathBuf::from)
            .unwrap_or_else(|| root.join(".ws-cache"))
            .canonicalize()?;
        let run = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let output = root.join(format!(
            "results/ws/cargo-{run}-{}/phase-0",
            std::process::id()
        ));
        fs::create_dir_all(output.join("build"))?;
        fs::create_dir(output.join("home"))?;
        fs::create_dir(output.join("tmp"))?;
        let profile: Value =
            serde_json::from_slice(&fs::read(root.join("bench/ws/profiles/local-v1.json"))?)?;
        let mut mask = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
        require(
            unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&mask), &mut mask) } == 0,
            "CPU affinity unavailable",
        )?;
        let cpus: Vec<_> = (0..libc::CPU_SETSIZE as usize)
            .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &mask) })
            .collect();
        require(cpus.len() >= 2, "two allowed endpoint CPUs required")?;
        let clang = executable(
            env::var_os("CC")
                .or_else(|| env::var_os("CLANG"))
                .unwrap_or_else(|| "clang".into()),
        )?;
        println!("Phase 0 evidence: {}", output.display());
        save(&output.join("profile.json"), &profile)?;
        save(
            &output.join("summary.json"),
            &json!({"phase_pass":false,"scope":"correctness/build only"}),
        )?;
        Ok(Self {
            server: output.join("build/ws-native"),
            root,
            cache,
            output,
            profile,
            clang,
            cpus: [cpus[0], cpus[1]],
            commands: Vec::new(),
            trials: Vec::new(),
        })
    }

    fn command(&self, program: &Path) -> Result<Command> {
        let mut command = Command::new(program);
        let mut paths = vec![self.clang.parent().unwrap().to_path_buf()];
        paths.extend(env::split_paths(&env::var_os("PATH").unwrap_or_default()));
        command
            .current_dir(&self.root)
            .env("PATH", env::join_paths(paths)?)
            .env("CC", &self.clang)
            .env("BEND_NO_TELEMETRY", "1")
            .env("HOME", self.output.join("home"))
            .env("TMPDIR", self.output.join("tmp"))
            .env("XDG_CACHE_HOME", self.output.join("home/.cache"))
            .env("XDG_CONFIG_HOME", self.output.join("home/.config"));
        Ok(command)
    }

    fn record(&mut self, command: &Command) -> Result<usize> {
        let id = self.commands.len();
        self.commands.push(json!({"program":command.get_program().to_string_lossy(),
            "args":command.get_args().map(|a| a.to_string_lossy()).collect::<Vec<_>>(),
            "cwd":command.get_current_dir().map(|p| p.to_string_lossy()),
            "environment":command.get_envs().map(|(k,v)| (k.to_string_lossy(),v.map(|v|v.to_string_lossy()))).collect::<Vec<_>>()}));
        save(&self.output.join("commands.json"), &json!(self.commands))?;
        Ok(id)
    }

    fn run(&mut self, mut command: Command) -> Result<String> {
        let id = self.record(&command)?;
        let stdout = self.output.join(format!("build/{id}.stdout"));
        let stderr = self.output.join(format!("build/{id}.stderr"));
        command
            .stdin(Stdio::null())
            .stdout(fs::File::create(&stdout)?)
            .stderr(fs::File::create(&stderr)?);
        let status = Process(command.spawn()?).wait(Instant::now() + Duration::from_secs(600))?;
        require(
            status == 0,
            &format!(
                "command {id} failed ({status}): {}",
                fs::read_to_string(stderr)?
            ),
        )?;
        Ok(fs::read_to_string(stdout)?)
    }

    fn build(&mut self) -> Result<()> {
        let bend = executable(
            env::var_os("WS_BEND")
                .or_else(|| env::var_os("BEND"))
                .unwrap_or_else(|| {
                    if self.cache.join("bend/bin/bend").is_file() {
                        self.cache.join("bend/bin/bend").into_os_string()
                    } else {
                        "bend".into()
                    }
                }),
        )?;
        let mut version = self.command(&bend)?;
        version.arg("version");
        require(
            self.run(version)?.trim()
                == format!(
                    "bend {}",
                    self.profile["pins"]["bend_version"]
                        .as_str()
                        .ok_or("missing Bend pin")?
                ),
            "Bend version mismatch",
        )?;
        let mut version = self.command(&self.clang)?;
        version.arg("--version");
        require(
            self.run(version)?.contains("clang version"),
            "CC must be clang",
        )?;
        let build = self.output.join("build");
        let sentinel = format!(
            "CONFLUXION_PHASE0_NATIVE_OK_{}",
            self.output
                .parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_string_lossy()
        );
        let source = build.join("sentinel.bend");
        fs::write(
            &source,
            fs::read_to_string(self.root.join("bench/ws/bend-native/sentinel.bend"))?
                .replace("CONFLUXION_PHASE0_NATIVE_OK", &sentinel),
        )?;
        let binary = build.join("sentinel");
        let mut compile = self.command(&bend)?;
        compile.arg(source).arg("-o").arg(&binary);
        self.run(compile)?;
        require(
            fs::read(&binary)?.starts_with(b"\x7fELF"),
            "Bend output is not ELF",
        )?;
        let mut run = self.command(&binary)?;
        run.args(["+RTS", "--threads", "1", "-RTS"]);
        require(self.run(run)? == sentinel + "\n", "Bend sentinel mismatch")?;
        let mut sources = Vec::new();
        for directory in ["src", "src/eventing", "src/crypto", "src/io_uring"] {
            for entry in fs::read_dir(self.cache.join("uSockets").join(directory))? {
                let path = entry?.path();
                if path.extension().is_some_and(|ext| ext == "c") {
                    sources.push(path);
                }
            }
        }
        sources.sort();
        require(!sources.is_empty(), "missing uSockets C sources")?;
        let mut objects = Vec::new();
        let cxxflags: Vec<_> = env::var("CXXFLAGS")
            .unwrap_or_default()
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        for (index, source) in sources
            .iter()
            .chain(
                [
                    self.root.join("bench/ws/reference-native/main.cpp"),
                    self.root.join("bench/ws/reference-native/allocator.cpp"),
                ]
                .iter(),
            )
            .enumerate()
        {
            let object = build.join(format!("native-{index}.o"));
            let mut compile = self.command(&self.clang)?;
            compile
                .args(["-O3", "-g", "-DLIBUS_NO_SSL", "-DUWS_NO_ZLIB", "-pthread"])
                .arg(format!("-I{}", self.cache.join("uSockets/src").display()))
                .arg(format!(
                    "-I{}",
                    self.cache.join("uWebSockets/src").display()
                ));
            if source.extension().is_some_and(|ext| ext == "cpp") {
                compile.args(["-x", "c++", "-std=c++20"]).args(&cxxflags);
            } else {
                compile.arg("-std=c11");
            }
            compile.arg("-c").arg(source).arg("-o").arg(&object);
            self.run(compile)?;
            objects.push(object);
        }
        let mut link = self.command(&self.clang)?;
        link.args(&cxxflags)
            .args(objects)
            .args([
                "-lstdc++",
                "-pthread",
                "-Wl,--wrap=malloc,--wrap=free,--wrap=calloc,--wrap=realloc",
            ])
            .arg("-o")
            .arg(&self.server);
        self.run(link)?;
        let mut vector = Vec::new();
        for word in [1_u64, 7, 1, 0, 2, 3, 123456] {
            vector.extend(word.to_be_bytes());
        }
        for i in 56..64 {
            vector.push(((7 + 3 * 17 + i * 31 + 2 * 13 + 19) & 255) as u8);
        }
        let expected: String = vector.iter().map(|b| format!("{b:02x}")).collect();
        for binary in [
            PathBuf::from(env!("CARGO_BIN_EXE_ws-reference")),
            self.server.clone(),
        ] {
            let mut command = self.command(&binary)?;
            command.arg("vectors");
            require(
                self.run(command)?.trim() == expected,
                "payload vector mismatch",
            )?;
        }
        save(
            &build.join("vectors.json"),
            &json!({"expected":expected,"both_actors_match":true}),
        )
    }

    fn trial(&mut self, mut plan: Plan) -> Result<Trial> {
        let directory = self.output.join("trials").join(&plan.id);
        fs::create_dir_all(&directory)?;
        let port = TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port();
        let tail: Vec<String> = [
            port as u64,
            1,
            plan.size,
            plan.rate,
            plan.rate,
            0,
            plan.seconds,
            plan.drain,
            plan.token,
        ]
        .iter()
        .map(ToString::to_string)
        .chain([
            plan.fault.map_or(-1, |(_, d)| d as i32).to_string(),
            plan.fault.map_or("none", |(kind, _)| kind).to_owned(),
            u8::from(plan.paused.is_some()).to_string(),
        ])
        .collect();
        let mut processes = Vec::new();
        for actor in [1, 0] {
            let binary = if actor == 0 {
                Path::new(env!("CARGO_BIN_EXE_ws-reference"))
            } else {
                &self.server
            };
            let mut command = self.command(binary)?;
            command
                .arg(plan.mode)
                .args(&tail)
                .arg(directory.join(format!("{}.json", ACTORS[actor])))
                .env_remove("WS_PAUSE_RECEIVE_MS");
            if plan.paused == Some(actor) {
                command.env("WS_PAUSE_RECEIVE_MS", "200");
            }
            let cpu = self.cpus[actor];
            unsafe {
                command.pre_exec(move || {
                    let mut mask = std::mem::zeroed::<libc::cpu_set_t>();
                    libc::CPU_SET(cpu, &mut mask);
                    if libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask) == 0 {
                        Ok(())
                    } else {
                        Err(std::io::Error::last_os_error())
                    }
                });
            }
            self.record(&command)?;
            command
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(fs::File::create(
                    directory.join(format!("{}.stderr", ACTORS[actor])),
                )?);
            processes.push(Process(command.spawn()?));
            let expected = if actor == 0 {
                "READY".to_owned()
            } else {
                format!("LISTENING {port}")
            };
            processes
                .last_mut()
                .unwrap()
                .ready(&expected, number(&self.profile, "startup_timeout_s")?)?;
        }
        plan.start = monotonic_ns() + 500_000_000;
        plan.end = plan.start + plan.seconds * NS;
        save(
            &directory.join("plan.json"),
            &json!({"id":plan.id,"mode":plan.mode,"size":plan.size,
            "rates":[plan.rate,plan.rate],"warmup_s":0,"measurement_s":plan.seconds,"drain_s":plan.drain,
            "token":plan.token,"start_ns":plan.start,"end_ns":plan.end,"fault":plan.fault,
            "paused_receiver":plan.paused.map(|a|ACTORS[a]),"pause_receive_ms":if plan.paused.is_some(){200}else{0},"cpus":self.cpus}),
        )?;
        for process in &mut processes {
            writeln!(process.0.stdin.take().unwrap(), "{}", plan.start)?;
        }
        let deadline = Instant::now()
            + Duration::from_secs(
                plan.seconds + plan.drain + number(&self.profile, "close_timeout_s")? + 2,
            );
        let mut statuses = Vec::new();
        for process in processes.iter_mut().rev() {
            statuses.push(process.wait(deadline)?);
        }
        save(
            &directory.join("statuses.json"),
            &json!({"client":statuses[0],"server":statuses[1]}),
        )?;
        let mut actor_reports = Vec::new();
        for actor in ACTORS {
            actor_reports.push(serde_json::from_slice(&fs::read(
                directory.join(format!("{actor}.json")),
            )?)?);
        }
        Ok(Trial {
            plan,
            actor_reports,
            statuses,
        })
    }

    fn check(&mut self, trial: &Trial) -> Result<()> {
        let result = match trial.plan.fault {
            Some((kind, direction)) => validate_fault(trial, kind, direction),
            None => evaluate(trial),
        };
        let record = json!({"id":trial.plan.id,"passed":result.is_ok(),"error":result.as_ref().err().map(ToString::to_string)});
        save(
            &self
                .output
                .join("trials")
                .join(&trial.plan.id)
                .join("result.json"),
            &record,
        )?;
        self.trials.push(record);
        save(
            &self.output.join("summary.json"),
            &json!({"phase_pass":false,"scope":"correctness/build only","trials":self.trials}),
        )?;
        result.map_err(|e| format!("{}: {e}", trial.plan.id).into())
    }

    fn plan(&self, id: String, size: u64, rate: u64, seconds: u64) -> Result<Plan> {
        Ok(Plan {
            id,
            mode: "ws",
            size,
            rate,
            seconds,
            drain: number(&self.profile, "drain_s")?,
            token: monotonic_ns(),
            start: 0,
            end: 0,
            fault: None,
            paused: None,
        })
    }
}

fn executable(name: std::ffi::OsString) -> Result<PathBuf> {
    let path = PathBuf::from(&name);
    if path.components().count() > 1 {
        return Ok(path.canonicalize()?);
    }
    env::split_paths(&env::var_os("PATH").unwrap_or_default())
        .map(|directory| directory.join(&name))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            format!(
                "required executable not found: {}; prepare Bend/clang before cargo test",
                path.display()
            )
            .into()
        })
}

#[test]
fn phase0_native_correctness() -> Result<()> {
    let mut session = Session::new()?;
    session.build()?;
    let mut million =
        session.trial(session.plan("integrity-million".into(), 256, 50_000, 20)?)?;
    session.check(&million)?;
    for report in &million.actor_reports {
        assert_eq!(number(&stream(report, 1)["tx"], "transmitted")?, 1_000_000);
        assert_eq!(number(&stream(report, 1)["rx"], "validated")?, 1_000_000);
    }
    let server = million.actor_reports.pop().unwrap();
    assert!(evaluate(&million).is_err(), "missing report accepted");
    million.actor_reports.push(server);
    let original = million.actor_reports[1].clone();
    million.actor_reports[1]["streams"][1]["rx"]["validated"] = json!(999_999);
    assert!(evaluate(&million).is_err(), "count mismatch accepted");
    million.actor_reports[1] = json!([]);
    assert!(evaluate(&million).is_err(), "malformed report accepted");
    million.actor_reports[1] = original;
    assert!(
        validate_fault(&million, "duplicate", 0).is_err(),
        "false successful fault accepted"
    );
    for kind in ["duplicate", "corruption"] {
        for direction in 0..2 {
            let mut plan = session.plan(format!("integrity.{kind}.{direction}"), 256, 1000, 1)?;
            plan.fault = Some((kind, direction));
            let trial = session.trial(plan)?;
            session.check(&trial)?;
            assert!(evaluate(&trial).is_err());
        }
    }
    for mode in ["ws", "tcp"] {
        for receiver in 0..2 {
            let mut plan = session.plan(
                format!("bounds/{mode}/{}", ACTORS[receiver]),
                1_048_576,
                100,
                1,
            )?;
            plan.mode = mode;
            plan.paused = Some(receiver);
            let trial = session.trial(plan)?;
            session.check(&trial)?;
            let sender = stream(&trial.actor_reports[1 - receiver], 1);
            assert!(
                number(&sender["tx"], "rejected")? > 0,
                "pause did not force rejection"
            );
            assert!(
                number(sender, "queue_highwater_bytes")? > 0,
                "missing queue occupancy"
            );
        }
    }
    save(
        &session.output.join("summary.json"),
        &json!({"phase_pass":true,"scope":"correctness/build only; throughput qualification deferred",
        "trial_count":session.trials.len(),"trials":session.trials,"damaged_evidence_rejected":true}),
    )?;
    Ok(())
}
