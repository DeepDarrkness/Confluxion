use crate::protocol::{self, Admission, FlushStep, Outbound, ReadStep, Rejection, UpgradeStep};
use std::{
    fmt, io,
    net::{SocketAddrV4, TcpStream},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
pub struct Config {
    pub connect_timeout: Duration,
    pub upgrade_timeout: Duration,
    pub close_timeout: Duration,
    pub partial_timeout: Duration,
    pub socket_buffer_bytes: Option<usize>,
    pub max_message: usize,
    pub max_frame: usize,
    pub queue_bytes: usize,
    pub queue_descriptors: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(1),
            upgrade_timeout: Duration::from_secs(1),
            close_timeout: Duration::from_secs(1),
            partial_timeout: Duration::from_millis(250),
            socket_buffer_bytes: None,
            max_message: protocol::MAXIMUM,
            max_frame: protocol::MAXIMUM,
            queue_bytes: 2 * protocol::MAXIMUM,
            queue_descriptors: 4096,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Binary,
    Text,
}
#[derive(Debug)]
pub struct Received {
    pub kind: Kind,
    pub bytes: Vec<u8>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Terminal {
    LocalClose,
    PeerClose,
    ConnectTimeout,
    UpgradeTimeout,
    IncomingTimeout,
    OutgoingTimeout,
    CloseTimeout,
    Protocol,
    Utf8,
    Limit,
    Io(io::ErrorKind),
    Panic,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Input,
    Protocol(protocol::Error),
    Io(io::ErrorKind),
    Terminal(Terminal),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WebSocket worker: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e.kind())
    }
}
impl From<protocol::Error> for Error {
    fn from(e: protocol::Error) -> Self {
        Self::Protocol(e)
    }
}
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub connected: bool,
    pub finished: bool,
    pub joined: bool,
    pub terminal: Option<Terminal>,
    pub tx_admitted: u64,
    pub tx_messages: u64,
    pub rx_messages: u64,
    pub tx_bytes: u64,
    pub rx_bytes: u64,
    pub tx_queue_bytes: usize,
    pub rx_queue_bytes: usize,
    pub tx_queue_descriptors: usize,
    pub rx_queue_descriptors: usize,
    pub tx_queue_bytes_peak: usize,
    pub rx_queue_bytes_peak: usize,
    pub tx_queue_descriptors_peak: usize,
    pub rx_queue_descriptors_peak: usize,
    pub native_memory_budget: usize,
    pub native_memory_peak: usize,
    pub native_memory_live: usize,
    pub native_memory_idle: usize,
    pub allocation_invalid: bool,
    pub worker_stack_bytes: usize,
    pub turns: u64,
    pub service_gap_peak: Duration,
    pub read_service_gap_peak: Duration,
    pub write_service_gap_peak: Duration,
    pub control_service_gap_peak: Duration,
    pub incoming_partial_age: Option<Duration>,
    pub outgoing_partial_age: Option<Duration>,
    pub close_elapsed: Duration,
}

#[derive(Clone, Copy)]
struct Descriptor {
    kind: Kind,
    len: usize,
}
struct Ring {
    bytes: Box<[u8]>,
    descriptors: Box<[Descriptor]>,
    byte_head: usize,
    byte_len: usize,
    head: usize,
    len: usize,
}
impl Ring {
    fn allocated_bytes(&self) -> usize {
        self.bytes.len() + std::mem::size_of_val(&*self.descriptors)
    }
    fn new(c: &Config) -> Self {
        Self {
            bytes: vec![0; c.queue_bytes].into_boxed_slice(),
            descriptors: vec![
                Descriptor {
                    kind: Kind::Binary,
                    len: 0
                };
                c.queue_descriptors
            ]
            .into_boxed_slice(),
            byte_head: 0,
            byte_len: 0,
            head: 0,
            len: 0,
        }
    }
    fn room(&self, n: usize) -> bool {
        self.len < self.descriptors.len() && n <= self.bytes.len() - self.byte_len
    }
    fn push(&mut self, kind: Kind, bytes: &[u8]) {
        assert!(self.room(bytes.len()));
        let at = (self.byte_head + self.byte_len) % self.bytes.len();
        let first = bytes.len().min(self.bytes.len() - at);
        self.bytes[at..at + first].copy_from_slice(&bytes[..first]);
        self.bytes[..bytes.len() - first].copy_from_slice(&bytes[first..]);
        self.descriptors[(self.head + self.len) % self.descriptors.len()] = Descriptor {
            kind,
            len: bytes.len(),
        };
        self.len += 1;
        self.byte_len += bytes.len();
    }
    fn front(&self, out: &mut [u8]) -> Descriptor {
        let d = self.descriptors[self.head];
        let first = d.len.min(self.bytes.len() - self.byte_head);
        out[..first].copy_from_slice(&self.bytes[self.byte_head..self.byte_head + first]);
        out[first..d.len].copy_from_slice(&self.bytes[..d.len - first]);
        d
    }
    fn pop(&mut self) {
        let d = self.descriptors[self.head];
        self.byte_head = (self.byte_head + d.len) % self.bytes.len();
        self.byte_len -= d.len;
        self.head = (self.head + 1) % self.descriptors.len();
        self.len -= 1;
    }
}
enum Lifecycle {
    Starting,
    Open,
    Closing(Instant),
    Finished,
}
struct State {
    lifecycle: Lifecycle,
    tx: Option<Ring>,
    rx: Option<Ring>,
    snapshot: Snapshot,
    startup_error: Option<Error>,
}
struct Core {
    state: Mutex<State>,
    changed: Condvar,
    wake: Mutex<Option<OwnedFd>>,
    join: Mutex<Option<JoinHandle<()>>>,
    config: Config,
    fixed_bytes: usize,
}
pub struct Client {
    core: Arc<Core>,
}

impl Client {
    pub fn connect(
        addr: SocketAddrV4,
        target: &str,
        subprotocol: Option<&str>,
        config: Config,
    ) -> Result<Self, Error> {
        protocol::Limits::new(config.max_frame, config.max_message)?;
        if config.queue_bytes < config.max_message
            || target.len() > 1024
            || subprotocol.is_some_and(|p| p.len() > 64)
            || config.queue_descriptors == 0
            || config.queue_bytes > 64 * protocol::MAXIMUM
            || config.queue_descriptors > 65536
            || config
                .socket_buffer_bytes
                .is_some_and(|n| n == 0 || n > i32::MAX as usize)
            || [
                config.connect_timeout,
                config.upgrade_timeout,
                config.close_timeout,
                config.partial_timeout,
            ]
            .iter()
            .any(|d| d.is_zero() || *d > Duration::from_secs(60))
        {
            return Err(Error::Input);
        }
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let wake = unsafe { OwnedFd::from_raw_fd(fd) };
        let tx = Ring::new(&config);
        let rx = Ring::new(&config);
        let target = target.to_owned();
        let subprotocol = subprotocol.map(str::to_owned);
        let startup_bytes = target.capacity() + subprotocol.as_ref().map_or(0, String::capacity);
        let fixed_bytes = tx.allocated_bytes()
            + rx.allocated_bytes()
            + std::mem::size_of::<Core>()
            + 2 * std::mem::size_of::<usize>()
            + startup_bytes;
        let core = Arc::new(Core {
            state: Mutex::new(State {
                lifecycle: Lifecycle::Starting,
                tx: Some(tx),
                rx: Some(rx),
                snapshot: Snapshot {
                    native_memory_budget: 19_974_752 + fixed_bytes + config.max_frame + 128,
                    native_memory_live: fixed_bytes,
                    native_memory_peak: fixed_bytes,
                    worker_stack_bytes: 256 * 1024,
                    ..Snapshot::default()
                },
                startup_error: None,
            }),
            changed: Condvar::new(),
            wake: Mutex::new(Some(wake)),
            join: Mutex::new(None),
            config,
            fixed_bytes,
        });
        let worker = core.clone();
        let h = thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(move || {
                crate::allocation_scope_start();
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    run(&worker, addr, &target, subprotocol.as_deref())
                }));
                let error = match outcome {
                    Ok(v) => v.err(),
                    Err(payload) => {
                        drop(payload);
                        Some(Error::Terminal(Terminal::Panic))
                    }
                };
                let memory = crate::allocation_scope_finish();
                drop(target);
                drop(subprotocol);
                let mut s = crate::lock(&worker.state);
                s.snapshot.native_memory_peak = s
                    .snapshot
                    .native_memory_peak
                    .max(worker.fixed_bytes + memory.peak);
                s.snapshot.native_memory_live = worker.fixed_bytes - startup_bytes + memory.live;
                s.snapshot.allocation_invalid |= memory.invalid;
                if let Some(e) = error {
                    s.startup_error = Some(e);
                    s.snapshot.terminal.get_or_insert(reason(e));
                }
                s.snapshot.finished = true;
                s.lifecycle = Lifecycle::Finished;
                worker.changed.notify_all();
            })?;
        *crate::lock(&core.join) = Some(h);
        let client = Self { core };
        let mut s = crate::lock(&client.core.state);
        while matches!(s.lifecycle, Lifecycle::Starting) {
            s = client
                .core
                .changed
                .wait(s)
                .unwrap_or_else(|p| p.into_inner());
        }
        if !s.snapshot.connected {
            let e = s
                .startup_error
                .unwrap_or(Error::Terminal(Terminal::Protocol));
            drop(s);
            client.close();
            return Err(e);
        }
        drop(s);
        Ok(client)
    }
    pub fn offer(&self, data: Outbound<'_>) -> Result<Admission, Error> {
        let (kind, bytes) = match data {
            Outbound::Binary(b) => (Kind::Binary, b),
            Outbound::Text(t) => (Kind::Text, t.as_bytes()),
        };
        let mut s = crate::lock(&self.core.state);
        if !matches!(s.lifecycle, Lifecycle::Open) {
            return Ok(Admission::Rejected(Rejection::Closing));
        }
        if bytes.len() > self.core.config.max_frame.min(self.core.config.max_message) {
            return Ok(Admission::Rejected(Rejection::Oversize));
        }
        let q = s.tx.as_mut().unwrap();
        if !q.room(bytes.len()) {
            return Ok(Admission::Rejected(Rejection::Busy));
        }
        q.push(kind, bytes);
        s.snapshot.tx_admitted += 1;
        queues(&mut s);
        drop(s);
        wake(&self.core);
        Ok(Admission::Accepted)
    }
    pub fn receive(&self) -> Result<Option<Received>, Error> {
        let mut s = crate::lock(&self.core.state);
        if let Some(q) = s.rx.as_mut().filter(|q| q.len != 0) {
            let mut bytes = vec![0; q.descriptors[q.head].len];
            let d = q.front(&mut bytes);
            q.pop();
            if s.snapshot.joined && s.rx.as_ref().is_some_and(|q| q.len == 0) {
                let q = s.rx.take().unwrap();
                s.snapshot.native_memory_live -= q.allocated_bytes();
            }
            queues(&mut s);
            drop(s);
            wake(&self.core);
            return Ok(Some(Received {
                kind: d.kind,
                bytes,
            }));
        }
        if s.snapshot.finished {
            match s.snapshot.terminal {
                Some(Terminal::PeerClose | Terminal::LocalClose) => Ok(None),
                Some(t) => Err(Error::Terminal(t)),
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
    pub fn status(&self) -> Snapshot {
        crate::lock(&self.core.state).snapshot.clone()
    }
    pub fn request_close(&self) {
        let mut s = crate::lock(&self.core.state);
        if matches!(s.lifecycle, Lifecycle::Starting | Lifecycle::Open) {
            s.lifecycle = Lifecycle::Closing(Instant::now());
        }
        drop(s);
        wake(&self.core);
    }
    pub fn close(&self) -> Snapshot {
        let at = Instant::now();
        self.request_close();
        let bound = Duration::from_secs(1);
        let mut s = crate::lock(&self.core.state);
        while !s.snapshot.finished && at.elapsed() < bound {
            let remaining = bound.saturating_sub(at.elapsed());
            let (next, _) = self
                .core
                .changed
                .wait_timeout(s, remaining)
                .unwrap_or_else(|p| p.into_inner());
            s = next;
        }
        drop(s);
        let mut join = crate::lock(&self.core.join);
        while join.as_ref().is_some_and(|h| !h.is_finished()) && at.elapsed() < bound {
            thread::sleep(Duration::from_micros(100));
        }
        if join.as_ref().is_some_and(JoinHandle::is_finished) {
            let _ = join.take().unwrap().join();
            crate::lock(&self.core.wake).take();
            let mut s = crate::lock(&self.core.state);
            s.snapshot.joined = true;
            if let Some(q) = s.tx.take() {
                s.snapshot.native_memory_live -= q.allocated_bytes();
            }
            if s.rx.as_ref().is_some_and(|q| q.len == 0) {
                let q = s.rx.take().unwrap();
                s.snapshot.native_memory_live -= q.allocated_bytes();
            }
            queues(&mut s);
            s.snapshot.close_elapsed = at.elapsed();
        }
        drop(join);
        self.status()
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.close();
    }
}

fn queues(s: &mut State) {
    let p = &mut s.snapshot;
    p.tx_queue_bytes = s.tx.as_ref().map_or(0, |q| q.byte_len);
    p.rx_queue_bytes = s.rx.as_ref().map_or(0, |q| q.byte_len);
    p.tx_queue_descriptors = s.tx.as_ref().map_or(0, |q| q.len);
    p.rx_queue_descriptors = s.rx.as_ref().map_or(0, |q| q.len);
    p.tx_queue_bytes_peak = p.tx_queue_bytes_peak.max(p.tx_queue_bytes);
    p.rx_queue_bytes_peak = p.rx_queue_bytes_peak.max(p.rx_queue_bytes);
    p.tx_queue_descriptors_peak = p.tx_queue_descriptors_peak.max(p.tx_queue_descriptors);
    p.rx_queue_descriptors_peak = p.rx_queue_descriptors_peak.max(p.rx_queue_descriptors);
}
fn reason(e: Error) -> Terminal {
    match e {
        Error::Terminal(t) => t,
        Error::Io(k) | Error::Protocol(protocol::Error::Io(k)) => Terminal::Io(k),
        Error::Protocol(protocol::Error::Limit) => Terminal::Limit,
        _ => Terminal::Protocol,
    }
}
fn wake(c: &Core) {
    let guard = crate::lock(&c.wake);
    let Some(fd) = guard.as_ref() else {
        return;
    };
    let n = 1u64;
    loop {
        let rc = unsafe { libc::write(fd.as_raw_fd(), &n as *const _ as *const _, 8) };
        if rc >= 0 || io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            break;
        }
    }
}
fn wait(c: &Core, fd: i32, events: i16, timeout: Duration) -> Result<i16, Error> {
    let wake_fd = crate::lock(&c.wake).as_ref().map_or(-1, AsRawFd::as_raw_fd);
    let mut fds = [
        libc::pollfd {
            fd,
            events,
            revents: 0,
        },
        libc::pollfd {
            fd: wake_fd,
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    let millis = timeout.as_millis().min(i32::MAX as u128) as i32;
    let n = unsafe { libc::poll(fds.as_mut_ptr(), 2, millis) };
    if n < 0 {
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::Interrupted {
            return Ok(0);
        }
        return Err(e.into());
    }
    if fds[1].revents != 0 {
        let mut n = 0u64;
        unsafe {
            libc::read(wake_fd, &mut n as *mut _ as *mut _, 8);
        }
    }
    Ok(fds[0].revents)
}
fn tcp(c: &Core, addr: SocketAddrV4) -> Result<TcpStream, Error> {
    let fd = unsafe {
        libc::socket(
            libc::AF_INET,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let socket = unsafe { TcpStream::from_raw_fd(fd) };
    socket.set_nodelay(true)?;
    if let Some(n) = c.config.socket_buffer_bytes {
        let n = n as libc::c_int;
        for option in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
            if unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    option,
                    &n as *const _ as *const _,
                    std::mem::size_of_val(&n) as _,
                )
            } < 0
            {
                return Err(io::Error::last_os_error().into());
            }
        }
    }
    let address = libc::sockaddr_in {
        sin_family: libc::AF_INET as _,
        sin_port: addr.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes(addr.ip().octets()),
        },
        sin_zero: [0; 8],
    };
    if unsafe {
        libc::connect(
            fd,
            &address as *const _ as *const _,
            std::mem::size_of_val(&address) as _,
        )
    } < 0
    {
        let e = io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::EINPROGRESS) {
            return Err(e.into());
        }
        let at = Instant::now();
        loop {
            if at.elapsed() >= c.config.connect_timeout {
                return Err(Error::Terminal(Terminal::ConnectTimeout));
            }
            if closing(c).is_some() {
                return Err(Error::Terminal(Terminal::LocalClose));
            }
            if wait(c, fd, libc::POLLOUT, Duration::from_millis(5))? != 0 {
                if let Some(e) = socket.take_error()? {
                    return Err(e.into());
                }
                break;
            }
        }
    }
    Ok(socket)
}
fn closing(c: &Core) -> Option<Instant> {
    match crate::lock(&c.state).lifecycle {
        Lifecycle::Closing(at) => Some(at),
        _ => None,
    }
}

fn run(c: &Core, addr: SocketAddrV4, target: &str, subprotocol: Option<&str>) -> Result<(), Error> {
    let socket = tcp(c, addr)?;
    let fd = socket.as_raw_fd();
    let authority = addr.to_string();
    let limits = protocol::Limits::new(c.config.max_frame, c.config.max_message)?;
    let mut upgrade = protocol::Upgrade::start(socket, &authority, target, subprotocol, limits)?;
    let at = Instant::now();
    let mut protocol = loop {
        if at.elapsed() >= c.config.upgrade_timeout {
            return Err(Error::Terminal(Terminal::UpgradeTimeout));
        }
        if closing(c).is_some() {
            return Err(Error::Terminal(Terminal::LocalClose));
        }
        match upgrade.resume()? {
            UpgradeStep::Ready(p) => break p,
            UpgradeStep::Pending(u) => upgrade = u,
        }
        let events = if upgrade.wants_write() {
            libc::POLLOUT
        } else {
            libc::POLLIN
        };
        wait(c, fd, events, Duration::from_millis(5))?;
    };
    let mut scratch = vec![0; c.config.max_frame].into_boxed_slice();
    {
        let mut s = crate::lock(&c.state);
        if matches!(s.lifecycle, Lifecycle::Starting) {
            s.lifecycle = Lifecycle::Open;
        }
        s.snapshot.connected = true;
        c.changed.notify_all();
    }
    let mut pending_message = false;
    let mut needs_flush = false;
    let mut close: Option<(Instant, Terminal, Option<u16>)> = None;
    let mut previous = Instant::now();
    let mut read_at = previous;
    let mut write_at = previous;
    let mut control_at: Option<Instant> = None;
    loop {
        protocol.begin_turn();
        let now = Instant::now();
        if close.is_none() {
            if let Some(at) = closing(c) {
                close = Some((at, Terminal::LocalClose, Some(1000)));
            }
        }
        let progress = protocol.progress();
        if let Some((at, why, _)) = close {
            if now.duration_since(at) >= c.config.close_timeout.min(Duration::from_millis(900)) {
                let mut s = crate::lock(&c.state);
                s.snapshot.terminal = Some(if why == Terminal::LocalClose {
                    Terminal::CloseTimeout
                } else {
                    why
                });
                break;
            }
        } else {
            for (last, terminal) in [
                (progress.incoming, Terminal::IncomingTimeout),
                (progress.outgoing, Terminal::OutgoingTimeout),
            ] {
                if last.is_some_and(|t| now.duration_since(t) >= c.config.partial_timeout) {
                    crate::lock(&c.state).snapshot.terminal = Some(terminal);
                    return Err(Error::Terminal(terminal));
                }
            }
        }
        let mut work = false;
        let mut drained = false;
        if let Some((_, _, Some(code))) = close {
            match protocol.failure_close(code) {
                Ok(FlushStep::Drained) => {
                    drained = true;
                    close.as_mut().unwrap().2 = None;
                }
                Ok(FlushStep::Pending) => needs_flush = true,
                Err(e) => return Err(e.into()),
            }
        } else if needs_flush {
            match protocol.flush() {
                Ok(FlushStep::Drained) => {
                    drained = true;
                    needs_flush = false;
                }
                Ok(FlushStep::Pending) => (),
                Err(e) => {
                    if matches!(
                        e,
                        protocol::Error::Io(io::ErrorKind::NotConnected)
                            | protocol::Error::Terminal
                    ) && close.is_some()
                    {
                        break;
                    }
                    return Err(e.into());
                }
            }
        }
        if drained {
            needs_flush = false;
            let mut s = crate::lock(&c.state);
            if pending_message {
                s.snapshot.tx_messages += 1;
                pending_message = false;
            }
            if let Some(at) = control_at.take() {
                s.snapshot.control_service_gap_peak =
                    s.snapshot.control_service_gap_peak.max(at.elapsed());
            }
        }
        if close.is_none() && !needs_flush && !pending_message {
            for _ in 0..32 {
                let d = {
                    let s = crate::lock(&c.state);
                    s.tx.as_ref()
                        .filter(|q| q.len != 0)
                        .map(|q| q.front(&mut scratch))
                };
                let Some(d) = d else {
                    break;
                };
                let data = match d.kind {
                    Kind::Binary => Outbound::Binary(&scratch[..d.len]),
                    Kind::Text => Outbound::Text(
                        std::str::from_utf8(&scratch[..d.len]).expect("admitted text"),
                    ),
                };
                match protocol.offer(data)? {
                    Admission::Accepted => {
                        let mut s = crate::lock(&c.state);
                        s.tx.as_mut().unwrap().pop();
                        queues(&mut s);
                        pending_message = true;
                        needs_flush = true;
                        work = true;
                    }
                    Admission::Rejected(_) => break,
                }
                match protocol.flush()? {
                    FlushStep::Pending => break,
                    FlushStep::Drained => {
                        needs_flush = false;
                        pending_message = false;
                        crate::lock(&c.state).snapshot.tx_messages += 1;
                    }
                }
            }
        }
        let can_read = close
            .as_ref()
            .is_none_or(|(_, why, _)| matches!(why, Terminal::LocalClose | Terminal::PeerClose))
            && crate::lock(&c.state)
                .rx
                .as_ref()
                .unwrap()
                .room(c.config.max_message);
        if can_read {
            for _ in 0..32 {
                if !crate::lock(&c.state)
                    .rx
                    .as_ref()
                    .unwrap()
                    .room(c.config.max_message)
                {
                    break;
                }
                let result = protocol.read();
                needs_flush = true;
                match result {
                    Ok(ReadStep::Pending) => break,
                    Ok(ReadStep::Closed) => {
                        let mut s = crate::lock(&c.state);
                        let p = protocol.progress();
                        s.snapshot.rx_bytes = p.rx_bytes;
                        s.snapshot.tx_bytes = p.tx_bytes;
                        s.snapshot
                            .terminal
                            .get_or_insert(close.map_or(Terminal::PeerClose, |v| v.1));
                        return Ok(());
                    }
                    Ok(ReadStep::Binary(bytes)) => {
                        let mut s = crate::lock(&c.state);
                        s.rx.as_mut().unwrap().push(Kind::Binary, bytes);
                        s.snapshot.rx_messages += 1;
                        queues(&mut s);
                        work = true;
                    }
                    Ok(ReadStep::Text(text)) => {
                        let mut s = crate::lock(&c.state);
                        s.rx.as_mut().unwrap().push(Kind::Text, text.as_bytes());
                        s.snapshot.rx_messages += 1;
                        queues(&mut s);
                        work = true;
                    }
                    Ok(ReadStep::Ping(_) | ReadStep::Pong(_)) => {
                        control_at.get_or_insert(Instant::now());
                        work = true;
                    }
                    Ok(ReadStep::Close(_)) => {
                        close.get_or_insert((Instant::now(), Terminal::PeerClose, None));
                        let mut s = crate::lock(&c.state);
                        s.lifecycle = Lifecycle::Closing(Instant::now());
                        s.snapshot.terminal = Some(Terminal::PeerClose);
                        control_at.get_or_insert(Instant::now());
                        break;
                    }
                    Err(e) => {
                        let code = protocol.failure_code();
                        let why = match reason(e.into()) {
                            Terminal::Protocol if code == 1007 => Terminal::Utf8,
                            t => t,
                        };
                        if !matches!(why, Terminal::Utf8 | Terminal::Limit | Terminal::Protocol) {
                            return Err(e.into());
                        }
                        close = Some((Instant::now(), why, Some(code)));
                        let mut s = crate::lock(&c.state);
                        s.lifecycle = Lifecycle::Closing(Instant::now());
                        s.snapshot.terminal = Some(why);
                        break;
                    }
                }
            }
        }
        if needs_flush && close.as_ref().is_none_or(|(_, _, code)| code.is_none()) {
            match protocol.flush() {
                Ok(FlushStep::Drained) => {
                    needs_flush = false;
                    if pending_message {
                        crate::lock(&c.state).snapshot.tx_messages += 1;
                        pending_message = false;
                    }
                    if let Some(at) = control_at.take() {
                        let mut s = crate::lock(&c.state);
                        s.snapshot.control_service_gap_peak =
                            s.snapshot.control_service_gap_peak.max(at.elapsed());
                    }
                }
                Ok(FlushStep::Pending) => (),
                Err(e) => return Err(e.into()),
            }
        }
        let p = protocol.progress();
        let memory = crate::allocation_scope_snapshot();
        {
            let mut s = crate::lock(&c.state);
            let snap = &mut s.snapshot;
            snap.turns += 1;
            snap.service_gap_peak = snap.service_gap_peak.max(now.duration_since(previous));
            if can_read {
                snap.read_service_gap_peak =
                    snap.read_service_gap_peak.max(now.duration_since(read_at));
                read_at = now;
            } else {
                read_at = now;
            }
            if needs_flush || pending_message {
                snap.write_service_gap_peak = snap
                    .write_service_gap_peak
                    .max(now.duration_since(write_at));
            }
            write_at = now;
            snap.rx_bytes = p.rx_bytes;
            snap.tx_bytes = p.tx_bytes;
            snap.incoming_partial_age = p.incoming.map(|t| t.elapsed());
            snap.outgoing_partial_age = p.outgoing.map(|t| t.elapsed());
            snap.native_memory_live = c.fixed_bytes + memory.live;
            snap.native_memory_peak = snap.native_memory_peak.max(c.fixed_bytes + memory.peak);
            snap.allocation_invalid |= memory.invalid;
            if !pending_message
                && snap.tx_queue_descriptors == 0
                && snap.rx_queue_descriptors == 0
                && p.incoming.is_none()
                && p.outgoing.is_none()
            {
                snap.native_memory_idle = snap.native_memory_live;
            }
        }
        previous = now;
        if !needs_flush
            && close.as_ref().is_some_and(|(_, why, code)| {
                code.is_none()
                    && matches!(why, Terminal::Protocol | Terminal::Utf8 | Terminal::Limit)
            })
        {
            break;
        }
        let events = (if can_read { libc::POLLIN } else { 0 })
            | (if needs_flush { libc::POLLOUT } else { 0 });
        let timeout = if work {
            Duration::ZERO
        } else {
            Duration::from_millis(5)
        };
        wait(c, fd, events, timeout)?;
    }
    Ok(())
}
