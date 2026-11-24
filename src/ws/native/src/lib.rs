use std::{
    alloc::{GlobalAlloc, Layout, System},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering::SeqCst},
        Arc, Condvar, Mutex, MutexGuard,
    },
    thread::{self, JoinHandle},
};

pub const OK: u32 = 0;
pub const EMPTY: u32 = 1;
pub const FULL: u32 = 2;
pub const END: u32 = 3;
pub const INVALID: u32 = 4;
pub const STALE: u32 = 5;
pub const ROLE: u32 = 6;
pub const CLOSED: u32 = 7;
pub const BUSY: u32 = 8;
pub const FAILED: u32 = 9;
pub const PANIC: u32 = 10;
pub const PENDING: u32 = 11;
const MAX: usize = 256;
const BYTE_LIMIT: usize = 512;

struct Tracked;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicU64 = AtomicU64::new(0);
static BLOCKS: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
struct AllocationClass {
    key: AtomicU64,
    allocated: AtomicU64,
    freed: AtomicU64,
    live: AtomicU64,
    peak: AtomicU64,
}
static LAYOUTS: [AllocationClass; 16] = [const {
    AllocationClass {
        key: AtomicU64::new(0),
        allocated: AtomicU64::new(0),
        freed: AtomicU64::new(0),
        live: AtomicU64::new(0),
        peak: AtomicU64::new(0),
    }
}; 16];
static LAYOUT_OVERFLOW: AtomicU64 = AtomicU64::new(0);
fn allocation_event(l: Layout, allocate: bool) {
    let key = (l.align() as u64) << 32 | l.size() as u64;
    for class in &LAYOUTS {
        let old = class.key.load(SeqCst);
        if old == key
            || (old == 0
                && match class.key.compare_exchange(0, key, SeqCst, SeqCst) {
                    Ok(_) => true,
                    Err(now) => now == key,
                })
        {
            if allocate {
                class.allocated.fetch_add(1, SeqCst);
                let live = class.live.fetch_add(1, SeqCst) + 1;
                class.peak.fetch_max(live, SeqCst);
            } else {
                class.freed.fetch_add(1, SeqCst);
                class.live.fetch_sub(1, SeqCst);
            }
            return;
        }
    }
    LAYOUT_OVERFLOW.fetch_add(1, SeqCst);
}
unsafe impl GlobalAlloc for Tracked {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            allocation_event(l, true);
            ALLOCS.fetch_add(1, SeqCst);
            BLOCKS.fetch_add(1, SeqCst);
            let n = LIVE.fetch_add(l.size() as u64, SeqCst) + l.size() as u64;
            PEAK.fetch_max(n, SeqCst);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        allocation_event(l, false);
        LIVE.fetch_sub(l.size() as u64, SeqCst);
        BLOCKS.fetch_sub(1, SeqCst);
        System.dealloc(p, l);
    }
}
#[global_allocator]
static ALLOCATOR: Tracked = Tracked;
static FAULT: AtomicU32 = AtomicU32::new(0);
static WORKERS: AtomicU64 = AtomicU64::new(0);
static JOIN_NS: AtomicU64 = AtomicU64::new(0);
static JOIN_STARTED: AtomicU64 = AtomicU64::new(0);
static JOIN_COMPLETED: AtomicU64 = AtomicU64::new(0);
static WAKE_ERRORS: AtomicU64 = AtomicU64::new(0);
static ABANDONED: AtomicU64 = AtomicU64::new(0);
static STACK_BYTES: AtomicU64 = AtomicU64::new(0);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}
fn fault(n: u32) -> bool {
    FAULT.compare_exchange(n, 0, SeqCst, SeqCst).is_ok()
}

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Running,
    Closing,
    Joined,
}
#[derive(Clone, Copy, PartialEq)]
enum CellState {
    Free,
    Outgoing,
    Incoming,
    Leased,
}
#[derive(Clone, Copy)]
struct Cell {
    state: CellState,
    len: usize,
    bytes: [u8; MAX],
}
impl Cell {
    const EMPTY: Self = Self {
        state: CellState::Free,
        len: 0,
        bytes: [0; MAX],
    };
}
struct Ring {
    values: [usize; 4],
    head: usize,
    len: usize,
    bytes: usize,
}
impl Ring {
    const fn new() -> Self {
        Self {
            values: [0; 4],
            head: 0,
            len: 0,
            bytes: 0,
        }
    }
    fn push(&mut self, i: usize, bytes: usize) {
        self.values[(self.head + self.len) % 4] = i;
        self.len += 1;
        self.bytes += bytes;
    }
    fn pop(&mut self, bytes: usize) -> usize {
        let i = self.values[self.head];
        self.head = (self.head + 1) % 4;
        self.len -= 1;
        self.bytes -= bytes;
        i
    }
    fn first(&self) -> usize {
        self.values[self.head]
    }
}
struct State {
    phase: Phase,
    failed: bool,
    paused: bool,
    cells: [Cell; 8],
    outgoing: Ring,
    incoming: Ring,
    ready: Option<OwnedFd>,
    done: Option<OwnedFd>,
    parked: bool,
    join_mask: u8,
    close_at: Option<u64>,
}
struct Core {
    state: Mutex<State>,
    wake: Condvar,
    worker: Mutex<Option<JoinHandle<()>>>,
    tid: AtomicI32,
}
enum Entry {
    Vacant,
    Session(Arc<Core>),
    Closer(Arc<Core>),
    Lease(Arc<Core>, usize),
}
struct Slot {
    generation: u32,
    retired: u64,
    entry: Entry,
}
struct Registry {
    slots: [Slot; 10],
}
static REGISTRY: Mutex<Registry> = Mutex::new(Registry {
    slots: [const {
        Slot {
            generation: 1,
            retired: 0,
            entry: Entry::Vacant,
        }
    }; 10],
});

impl Registry {
    fn key(&self, i: usize) -> u64 {
        ((self.slots[i].generation as u64) << 32) | 0x8000_0000 | i as u64
    }
    fn index(&self, key: u64, role: u32) -> Result<usize, u32> {
        let i = (key & 0xffff_ffff) ^ 0x8000_0000;
        if i >= 10 || key >> 56 != 0 || key >> 32 == 0 {
            return Err(INVALID);
        }
        let i = i as usize;
        let s = &self.slots[i];
        if s.retired == key {
            return Err(CLOSED);
        }
        if self.key(i) != key {
            return Err(STALE);
        }
        let actual = match s.entry {
            Entry::Vacant => return Err(STALE),
            Entry::Session(_) => 1,
            Entry::Closer(_) => 2,
            Entry::Lease(_, _) => 3,
        };
        if role != actual {
            return Err(ROLE);
        }
        Ok(i)
    }
    fn vacant(&self) -> Option<usize> {
        self.slots
            .iter()
            .position(|s| matches!(s.entry, Entry::Vacant) && s.generation < 0x100_0000)
    }
    fn retire(&mut self, i: usize) {
        self.slots[i].retired = self.key(i);
        self.slots[i].entry = Entry::Vacant;
        self.slots[i].generation += 1;
    }
}
fn core(key: u64, role: u32) -> Result<Arc<Core>, u32> {
    let r = lock(&REGISTRY);
    let i = r.index(key, role)?;
    match &r.slots[i].entry {
        Entry::Session(c) | Entry::Closer(c) | Entry::Lease(c, _) => Ok(c.clone()),
        Entry::Vacant => Err(STALE),
    }
}
fn signal(fd: &Option<OwnedFd>) -> bool {
    if let Some(fd) = fd {
        let n = 1u64;
        loop {
            let result = unsafe { libc::write(fd.as_raw_fd(), &n as *const _ as *const _, 8) };
            if result == 8 {
                return true;
            }
            let errno = unsafe { *libc::__errno_location() };
            if result < 0 && errno == libc::EINTR {
                continue;
            }
            if result < 0 && errno == libc::EAGAIN {
                return true;
            }
            WAKE_ERRORS.fetch_add(1, SeqCst);
            return false;
        }
    }
    true
}
fn drain(fd: &Option<OwnedFd>) -> bool {
    if let Some(fd) = fd {
        let mut n = 0u64;
        loop {
            let result = unsafe { libc::read(fd.as_raw_fd(), &mut n as *mut _ as *mut _, 8) };
            if result == 8 {
                return true;
            }
            let errno = unsafe { *libc::__errno_location() };
            if result < 0 && errno == libc::EINTR {
                continue;
            }
            if result < 0 && errno == libc::EAGAIN {
                return true;
            }
            WAKE_ERRORS.fetch_add(1, SeqCst);
            return false;
        }
    }
    true
}
fn clock() -> u64 {
    let mut t = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t);
    }
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}
fn joined(started: u64) {
    let completed = clock();
    JOIN_STARTED.store(started, SeqCst);
    JOIN_COMPLETED.store(completed, SeqCst);
    JOIN_NS.fetch_max(completed - started, SeqCst);
}
fn terminal(c: &Core, failed: bool) {
    let mut s = lock(&c.state);
    if s.phase == Phase::Running {
        s.phase = Phase::Closing;
        s.close_at = Some(clock());
    }
    s.failed |= failed;
    s.failed |= !signal(&s.ready);
    c.wake.notify_one();
}
fn contain<T>(fallback: T, f: impl FnOnce() -> T) -> T {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => {
            let r = lock(&REGISTRY);
            for s in &r.slots {
                match &s.entry {
                    Entry::Session(c) | Entry::Closer(c) | Entry::Lease(c, _) => terminal(c, true),
                    Entry::Vacant => (),
                }
            }
            fallback
        }
    }
}
fn worker(c: Arc<Core>) {
    c.tid
        .store(unsafe { libc::syscall(libc::SYS_gettid) as i32 }, SeqCst);
    unsafe {
        let mut attr = std::mem::zeroed();
        if libc::pthread_getattr_np(libc::pthread_self(), &mut attr) == 0 {
            let mut size = 0;
            if libc::pthread_attr_getstacksize(&attr, &mut size) == 0 {
                STACK_BYTES.fetch_max(size as u64, SeqCst);
            }
            libc::pthread_attr_destroy(&mut attr);
        }
    }
    WORKERS.fetch_add(1, SeqCst);
    let failed = catch_unwind(AssertUnwindSafe(|| {
        let mut s = lock(&c.state);
        loop {
            if fault(20) {
                panic!("injected worker panic");
            }
            if s.phase != Phase::Running {
                break;
            }
            if !s.paused && s.outgoing.len > 0 && s.incoming.len < 4 {
                let i = s.outgoing.first();
                let len = s.cells[i].len;
                if s.incoming.bytes + len <= BYTE_LIMIT {
                    s.outgoing.pop(len);
                    s.cells[i].state = CellState::Incoming;
                    s.incoming.push(i, len);
                    if !signal(&s.ready) {
                        s.failed = true;
                        break;
                    }
                    continue;
                }
            }
            s = c.wake.wait(s).unwrap_or_else(|p| p.into_inner());
        }
    }))
    .is_err();
    terminal(&c, failed);
    let mut s = lock(&c.state);
    for cell in &mut s.cells {
        if matches!(cell.state, CellState::Outgoing | CellState::Incoming) {
            cell.state = CellState::Free;
            ABANDONED.fetch_add(1, SeqCst);
        }
    }
    s.outgoing = Ring::new();
    s.incoming = Ring::new();
    signal(&s.done);
    WORKERS.fetch_sub(1, SeqCst);
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Start {
    pub session: u64,
    pub closer: u64,
    pub code: u32,
    pub reserved: u32,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Take {
    pub lease: u64,
    pub code: u32,
    pub len: u32,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Wait {
    pub code: u32,
    pub fd: i32,
}
const _: () = assert!(
    std::mem::size_of::<Start>() == 24
        && std::mem::size_of::<Take>() == 16
        && std::mem::size_of::<Wait>() == 8
);

fn eventfd() -> Result<OwnedFd, u32> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if fd < 0 {
        Err(FAILED)
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}
fn start() -> Result<Start, u32> {
    let mut r = lock(&REGISTRY);
    if r.slots.iter().any(|s| !matches!(s.entry, Entry::Vacant)) {
        return Err(BUSY);
    }
    if fault(1) {
        return Err(FAILED);
    }
    let a = r.vacant().ok_or(BUSY)?;
    let b = (a + 1..10)
        .find(|&i| r.slots[i].generation < 0x100_0000)
        .ok_or(BUSY)?;
    let ready = eventfd()?;
    if fault(2) {
        return Err(FAILED);
    }
    let done = eventfd()?;
    if fault(3) {
        return Err(FAILED);
    }
    let c = Arc::new(Core {
        state: Mutex::new(State {
            phase: Phase::Running,
            failed: false,
            paused: false,
            cells: [Cell::EMPTY; 8],
            outgoing: Ring::new(),
            incoming: Ring::new(),
            ready: Some(ready),
            done: Some(done),
            parked: false,
            join_mask: 0,
            close_at: None,
        }),
        wake: Condvar::new(),
        worker: Mutex::new(None),
        tid: AtomicI32::new(0),
    });
    if fault(4) {
        return Err(FAILED);
    }
    let w = c.clone();
    let handle = thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(move || worker(w))
        .map_err(|_| FAILED)?;
    *lock(&c.worker) = Some(handle);
    if fault(5) {
        terminal(&c, false);
        let at = clock();
        lock(&c.worker)
            .take()
            .ok_or(FAILED)?
            .join()
            .map_err(|_| FAILED)?;
        while task_exists(c.tid.load(SeqCst)) {
            if clock() - at >= 1_000_000_000 {
                return Err(FAILED);
            }
            thread::yield_now();
        }
        joined(at);
        return Err(FAILED);
    }
    r.slots[a].entry = Entry::Session(c.clone());
    r.slots[b].entry = Entry::Closer(c);
    Ok(Start {
        session: r.key(a),
        closer: r.key(b),
        code: OK,
        reserved: 0,
    })
}
#[no_mangle]
pub extern "C" fn ws_native_start() -> Start {
    contain(
        Start {
            session: 0,
            closer: 0,
            code: PANIC,
            reserved: 0,
        },
        || {
            start().unwrap_or_else(|code| Start {
                session: 0,
                closer: 0,
                code,
                reserved: 0,
            })
        },
    )
}
#[no_mangle]
pub unsafe extern "C" fn ws_native_offer(key: u64, p: *const u8, len: u32) -> u32 {
    contain(PANIC, || {
        if len as usize > MAX || p.is_null() {
            return INVALID;
        }
        let c = match core(key, 1) {
            Ok(c) => c,
            Err(e) => return e,
        };
        let mut s = lock(&c.state);
        if s.phase != Phase::Running {
            return if s.failed { PANIC } else { END };
        }
        if s.outgoing.len == 4 || s.outgoing.bytes + len as usize > BYTE_LIMIT {
            return FULL;
        }
        let i = match s.cells.iter().position(|c| c.state == CellState::Free) {
            Some(i) => i,
            None => return FULL,
        };
        std::ptr::copy_nonoverlapping(p, s.cells[i].bytes.as_mut_ptr(), len as usize);
        s.cells[i].len = len as usize;
        s.cells[i].state = CellState::Outgoing;
        s.outgoing.push(i, len as usize);
        if fault(21) {
            panic!("injected ABI panic after admission");
        }
        c.wake.notify_one();
        OK
    })
}
#[no_mangle]
pub unsafe extern "C" fn ws_native_take(key: u64, dst: *mut u8, capacity: u32) -> Take {
    contain(
        Take {
            lease: 0,
            code: PANIC,
            len: 0,
        },
        || {
            let mut r = lock(&REGISTRY);
            let result = (|| {
                let i = r.index(key, 1)?;
                let c = match &r.slots[i].entry {
                    Entry::Session(c) => c.clone(),
                    _ => return Err(ROLE),
                };
                let mut s = lock(&c.state);
                if s.phase != Phase::Running {
                    return Err(if s.failed { PANIC } else { END });
                }
                if s.incoming.len == 0 {
                    return Err(EMPTY);
                }
                let cell = s.incoming.first();
                let len = s.cells[cell].len;
                if (capacity as usize) < len || dst.is_null() {
                    return Err(INVALID);
                }
                let slot = r.vacant().ok_or(FULL)?;
                std::ptr::copy_nonoverlapping(s.cells[cell].bytes.as_ptr(), dst, len);
                s.incoming.pop(len);
                s.cells[cell].state = CellState::Leased;
                c.wake.notify_one();
                drop(s);
                r.slots[slot].entry = Entry::Lease(c, cell);
                Ok(Take {
                    lease: r.key(slot),
                    code: OK,
                    len: len as u32,
                })
            })();
            result.unwrap_or_else(|code| Take {
                lease: 0,
                code,
                len: 0,
            })
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_native_wait_begin(key: u64) -> Wait {
    contain(
        Wait {
            code: PANIC,
            fd: -1,
        },
        || {
            let c = match core(key, 1) {
                Ok(c) => c,
                Err(code) => return Wait { code, fd: -1 },
            };
            let mut s = lock(&c.state);
            if s.parked {
                return Wait { code: BUSY, fd: -1 };
            }
            if s.phase != Phase::Running || s.incoming.len > 0 {
                return Wait { code: OK, fd: -1 };
            }
            if !drain(&s.ready) {
                s.phase = Phase::Closing;
                s.failed = true;
                s.close_at = Some(clock());
                c.wake.notify_one();
                return Wait {
                    code: PANIC,
                    fd: -1,
                };
            }
            s.parked = true;
            Wait {
                code: PENDING,
                fd: s.ready.as_ref().map_or(-1, AsRawFd::as_raw_fd),
            }
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_native_wait_end(key: u64) -> u32 {
    contain(PANIC, || {
        let c = match core(key, 1) {
            Ok(c) => c,
            Err(e) => return e,
        };
        lock(&c.state).parked = false;
        OK
    })
}
#[no_mangle]
pub extern "C" fn ws_native_release(key: u64) -> u32 {
    contain(PANIC, || {
        let mut r = lock(&REGISTRY);
        let i = match r.index(key, 3) {
            Ok(i) => i,
            Err(e) => return e,
        };
        if let Entry::Lease(c, cell) = &r.slots[i].entry {
            lock(&c.state).cells[*cell].state = CellState::Free;
            c.wake.notify_one();
        }
        r.retire(i);
        OK
    })
}
#[no_mangle]
pub extern "C" fn ws_native_stop_begin(key: u64, role: u32) -> Wait {
    contain(
        Wait {
            code: PANIC,
            fd: -1,
        },
        || {
            if role != 1 && role != 2 {
                return Wait { code: ROLE, fd: -1 };
            }
            let c = match core(key, role) {
                Ok(c) => c,
                Err(code) => return Wait { code, fd: -1 },
            };
            let mut s = lock(&c.state);
            if role == 1 && s.parked {
                return Wait { code: BUSY, fd: -1 };
            }
            if s.join_mask & role as u8 != 0 {
                return Wait { code: BUSY, fd: -1 };
            }
            s.join_mask |= role as u8;
            if s.phase == Phase::Running {
                s.phase = Phase::Closing;
                s.close_at = Some(clock());
            }
            s.failed |= !signal(&s.ready);
            c.wake.notify_one();
            Wait {
                code: OK,
                fd: s.done.as_ref().map_or(-1, AsRawFd::as_raw_fd),
            }
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_native_stop_finish(key: u64, role: u32) -> u32 {
    contain(PANIC, || {
        if role != 1 && role != 2 {
            return ROLE;
        }
        let c = match core(key, role) {
            Ok(c) => c,
            Err(e) => return e,
        };
        let mut worker = lock(&c.worker);
        if let Some(h) = worker.as_ref() {
            if !h.is_finished() {
                return PENDING;
            }
        }
        if let Some(h) = worker.take() {
            if h.join().is_err() {
                terminal(&c, true);
            }
        }
        drop(worker);
        if task_exists(c.tid.load(SeqCst)) {
            return PENDING;
        }
        let mut s = lock(&c.state);
        if s.join_mask & role as u8 == 0 {
            return BUSY;
        }
        s.phase = Phase::Joined;
        if let Some(t) = s.close_at.take() {
            joined(t);
        }
        s.join_mask &= !(role as u8);
        if s.join_mask == 0 {
            s.done.take();
        }
        if role == 1 {
            s.ready.take();
        }
        drop(s);
        let mut r = lock(&REGISTRY);
        let i = match r.index(key, role) {
            Ok(i) => i,
            Err(e) => return e,
        };
        r.retire(i);
        OK
    })
}

fn task_exists(tid: i32) -> bool {
    let mut path = [0u8; 32];
    path[..16].copy_from_slice(b"/proc/self/task/");
    let mut digits = [0u8; 10];
    let mut n = tid as u32;
    let mut at = 10;
    loop {
        at -= 1;
        digits[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    path[16..16 + 10 - at].copy_from_slice(&digits[at..]);
    unsafe { libc::access(path.as_ptr() as *const _, libc::F_OK) == 0 }
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct Stats {
    pub allocations: u64,
    pub live_bytes: u64,
    pub live_blocks: u64,
    pub peak_bytes: u64,
    pub entries: u64,
    pub descriptors: u64,
    pub workers: u64,
    pub parked: u64,
    pub cells: u64,
    pub cell_bytes: u64,
    pub join_ns: u64,
    pub abandoned: u64,
    pub core_size: u64,
    pub registry_size: u64,
    pub stack_bytes: u64,
    pub layout_overflow: u64,
    pub join_started: u64,
    pub join_completed: u64,
    pub wake_errors: u64,
}
#[no_mangle]
pub extern "C" fn ws_native_stats() -> Stats {
    contain(
        Stats {
            entries: u64::MAX,
            ..Stats::default()
        },
        || {
            let r = lock(&REGISTRY);
            let mut out = Stats {
                allocations: ALLOCS.load(SeqCst),
                live_bytes: LIVE.load(SeqCst),
                live_blocks: BLOCKS.load(SeqCst),
                peak_bytes: PEAK.load(SeqCst),
                workers: WORKERS.load(SeqCst),
                join_ns: JOIN_NS.load(SeqCst),
                abandoned: ABANDONED.load(SeqCst),
                core_size: std::mem::size_of::<Core>() as u64,
                registry_size: std::mem::size_of::<Registry>() as u64,
                stack_bytes: STACK_BYTES.load(SeqCst),
                layout_overflow: LAYOUT_OVERFLOW.load(SeqCst),
                join_started: JOIN_STARTED.load(SeqCst),
                join_completed: JOIN_COMPLETED.load(SeqCst),
                wake_errors: WAKE_ERRORS.load(SeqCst),
                ..Stats::default()
            };
            let mut seen = false;
            for slot in &r.slots {
                let c = match &slot.entry {
                    Entry::Vacant => continue,
                    Entry::Session(c) | Entry::Closer(c) | Entry::Lease(c, _) => c,
                };
                out.entries += 1;
                if !seen {
                    seen = true;
                    let s = lock(&c.state);
                    out.descriptors = s.ready.is_some() as u64 + s.done.is_some() as u64;
                    out.parked = s.parked as u64;
                    for cell in &s.cells {
                        if cell.state != CellState::Free {
                            out.cells += 1;
                            out.cell_bytes += cell.len as u64;
                        }
                    }
                }
            }
            out
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_native_fault(n: u32) -> u32 {
    contain(PANIC, || {
        FAULT.store(n, SeqCst);
        let r = lock(&REGISTRY);
        for slot in &r.slots {
            if let Entry::Session(c) = &slot.entry {
                let _state = lock(&c.state);
                c.wake.notify_one();
            }
        }
        OK
    })
}
#[no_mangle]
pub extern "C" fn ws_native_pause(key: u64, pause: u32) -> u32 {
    contain(PANIC, || {
        let c = match core(key, 1) {
            Ok(c) => c,
            Err(e) => return e,
        };
        lock(&c.state).paused = pause != 0;
        c.wake.notify_one();
        OK
    })
}
#[no_mangle]
pub extern "C" fn ws_native_probe(key: u64) -> u64 {
    contain(0, || key)
}

pub fn build_marker() -> &'static str {
    env!("CONFLUXION_WS_BUILD_MARKER")
}

#[no_mangle]
pub extern "C" fn ws_native_build_marker_byte(index: u32) -> u32 {
    contain(256, || {
        build_marker()
            .as_bytes()
            .get(index as usize)
            .map_or(256, |byte| *byte as u32)
    })
}

#[repr(C)]
#[derive(Default)]
pub struct AllocationLayout {
    pub size: u64,
    pub alignment: u64,
    pub allocated: u64,
    pub freed: u64,
    pub live: u64,
    pub peak: u64,
}
#[no_mangle]
pub extern "C" fn ws_native_layout(index: u32) -> AllocationLayout {
    contain(AllocationLayout::default(), || {
        let Some(class) = LAYOUTS.get(index as usize) else {
            return AllocationLayout::default();
        };
        let key = class.key.load(SeqCst);
        AllocationLayout {
            size: key & 0xffff_ffff,
            alignment: key >> 32,
            allocated: class.allocated.load(SeqCst),
            freed: class.freed.load(SeqCst),
            live: class.live.load(SeqCst),
            peak: class.peak.load(SeqCst),
        }
    })
}
