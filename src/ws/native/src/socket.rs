use crate::{
    worker::{Client, Config, Error, Kind, Terminal},
    *,
};
use std::net::{Ipv4Addr, SocketAddrV4};

pub(crate) const HEAD_TOO_LARGE: u32 = 24;
const OVERSIZE: u32 = 25;
const TAG: u64 = 0x40000000;
static FIXTURE_BUFFER: AtomicU32 = AtomicU32::new(0);
static FIXTURE_FAULT: AtomicU32 = AtomicU32::new(0);
#[no_mangle]
pub extern "C" fn ws_socket_fixture_fault(code: u32) {
    FIXTURE_FAULT.store(code, SeqCst);
}
pub(crate) fn fixture_fault(code: u32) -> bool {
    FIXTURE_FAULT
        .compare_exchange(code, 0, SeqCst, SeqCst)
        .is_ok()
}
#[no_mangle]
pub extern "C" fn ws_socket_fixture_buffer(bytes: u32) {
    FIXTURE_BUFFER.store(bytes, SeqCst);
}
const LEASE_BYTES: usize = 2 * 1024 * 1024;
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(crate) struct Span {
    pub kind: u32,
    pub offset: u32,
    pub len: u32,
    pub reserved: u32,
}
#[repr(C)]
pub struct Endpoint {
    port: u32,
    host_len: u32,
    target_len: u32,
    protocol_len: u32,
    host: *const u8,
    target: *const u8,
    protocol: *const u8,
}
#[repr(C)]
pub struct BatchAdmission {
    code: u32,
    accepted: u32,
}
#[repr(C)]
pub struct BatchTake {
    lease: u64,
    code: u32,
    count: u32,
    bytes_len: u32,
    terminal: u32,
    bytes: *const u8,
    messages: *const Span,
}
impl BatchTake {
    fn error(code: u32) -> Self {
        Self {
            lease: 0,
            code,
            count: 0,
            bytes_len: 0,
            terminal: 0,
            bytes: std::ptr::null(),
            messages: std::ptr::null(),
        }
    }
}
#[repr(C)]
#[derive(Default)]
pub struct Status {
    code: u32,
    state: u32,
    terminal: u32,
    reserved: u32,
    tx_admitted: u64,
    tx_complete: u64,
    rx_complete: u64,
    rx_consumed: u64,
    tx_failed: u64,
    rx_discarded: u64,
    tx_bytes: u32,
    tx_count: u32,
    rx_bytes: u32,
    rx_count: u32,
    lease_bytes: u32,
    leases: u32,
    tx_completed_ns: u64,
    rx_completed_ns: u64,
    control_ns: u64,
    native_live: u64,
    native_peak: u64,
    native_budget: u64,
    socket_send_buffer: u32,
    socket_receive_buffer: u32,
}
#[derive(Default)]
struct Totals {
    bytes: usize,
    count: usize,
}
struct Session {
    client: Client,
    leases: Arc<Mutex<Totals>>,
}
struct Lease {
    account: Arc<Mutex<Totals>>,
    bytes: Box<[u8]>,
    spans: Box<[Span]>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut a = lock(&self.account);
        a.bytes -= self.bytes.len();
        a.count -= 1;
    }
}
enum Entry {
    Vacant,
    Reserved,
    Session(Arc<Session>),
    Closer(Arc<Session>),
    Lease(Lease),
}
struct Slot {
    generation: u32,
    retired: u64,
    entry: Entry,
}
static SOCKETS: Mutex<[Slot; 10]> = Mutex::new(
    [const {
        Slot {
            generation: 0,
            retired: 0,
            entry: Entry::Vacant,
        }
    }; 10],
);
struct Reservation {
    slot: usize,
    account: Arc<Mutex<Totals>>,
    credit: Option<usize>,
    committed: bool,
}
struct StartupReservation {
    slots: [usize; 2],
    committed: bool,
}
impl Drop for StartupReservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut r = lock(&SOCKETS);
            for i in self.slots {
                retire(&mut r, i);
            }
        }
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.committed {
            retire(&mut lock(&SOCKETS), self.slot);
            if let Some(bytes) = self.credit {
                let mut a = lock(&self.account);
                a.bytes -= bytes;
                a.count -= 1;
            }
        }
    }
}
fn key(i: usize, generation: u32) -> u64 {
    (generation as u64) << 32 | TAG | (i as u64 + 1)
}
fn index(h: u64) -> Result<usize, u32> {
    if h & 0xfffffff0 != TAG || h >> 56 != 0 {
        return Err(INVALID);
    }
    let i = (h & 15) as usize;
    if i == 0 || i > 10 {
        Err(INVALID)
    } else {
        Ok(i - 1)
    }
}
fn lookup(slots: &[Slot; 10], h: u64) -> Result<usize, u32> {
    let i = index(h)?;
    if key(i, slots[i].generation) != h {
        return Err(STALE);
    }
    if matches!(slots[i].entry, Entry::Vacant) {
        return Err(if slots[i].retired == h {
            CLOSED
        } else {
            INVALID
        });
    }
    Ok(i)
}
fn reserve(slots: &mut [Slot; 10]) -> Result<(usize, u64), u32> {
    for (i, s) in slots.iter_mut().enumerate() {
        if matches!(s.entry, Entry::Vacant) && s.generation < 0xffffff {
            s.generation += 1;
            s.entry = Entry::Reserved;
            return Ok((i, key(i, s.generation)));
        }
    }
    Err(FULL)
}
fn retire(slots: &mut [Slot; 10], i: usize) -> Entry {
    slots[i].retired = key(i, slots[i].generation);
    std::mem::replace(&mut slots[i].entry, Entry::Vacant)
}
fn session(h: u64, role: u32) -> Result<Arc<Session>, u32> {
    let r = lock(&SOCKETS);
    let i = lookup(&r, h)?;
    match (&r[i].entry, role) {
        (Entry::Session(s), 1) | (Entry::Closer(s), 2) => Ok(s.clone()),
        _ => Err(ROLE),
    }
}
fn terminal(t: Terminal) -> u32 {
    match t {
        Terminal::LocalClose => 12,
        Terminal::PeerClose => 13,
        Terminal::ConnectTimeout => 14,
        Terminal::UpgradeTimeout => 15,
        Terminal::IncomingTimeout => 16,
        Terminal::OutgoingTimeout => 17,
        Terminal::CloseTimeout => 18,
        Terminal::Protocol => 19,
        Terminal::Utf8 => 20,
        Terminal::Limit => 21,
        Terminal::Io(_) => 22,
        Terminal::Panic => 23,
    }
}
fn error(e: Error) -> u32 {
    match e {
        Error::Input => INVALID,
        Error::Terminal(t) => terminal(t),
        _ => FAILED,
    }
}
fn contain<T>(fallback: impl FnOnce() -> T, f: impl FnOnce() -> T) -> T {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(p) => {
            drop(p);
            fallback()
        }
    }
}
unsafe fn bytes<'a>(p: *const u8, n: u32) -> Result<&'a [u8], u32> {
    if n == 0 {
        Ok(&[])
    } else if p.is_null() {
        Err(INVALID)
    } else {
        Ok(std::slice::from_raw_parts(p, n as usize))
    }
}
#[no_mangle]
pub unsafe extern "C" fn ws_socket_connect_begin(p: *const Endpoint) -> Start {
    contain(
        || Start {
            session: 0,
            closer: 0,
            code: PANIC,
            reserved: 0,
        },
        || {
            let result = (|| {
                let p = p.as_ref().ok_or(INVALID)?;
                if p.port == 0
                    || p.port > 65535
                    || p.host_len > 15
                    || p.target_len > 1024
                    || p.protocol_len > 64
                {
                    return Err(INVALID);
                }
                let host = std::str::from_utf8(bytes(p.host, p.host_len)?).map_err(|_| INVALID)?;
                let host: Ipv4Addr = host.parse().map_err(|_| INVALID)?;
                if !host.is_loopback() {
                    return Err(INVALID);
                }
                let target =
                    std::str::from_utf8(bytes(p.target, p.target_len)?).map_err(|_| INVALID)?;
                let protocol =
                    std::str::from_utf8(bytes(p.protocol, p.protocol_len)?).map_err(|_| INVALID)?;
                let (a, b, h, c) = {
                    let mut r = lock(&SOCKETS);
                    if r.iter().any(|s| !matches!(s.entry, Entry::Vacant)) {
                        return Err(BUSY);
                    }
                    let (a, h) = reserve(&mut r)?;
                    let (b, c) = match reserve(&mut r) {
                        Ok(v) => v,
                        Err(e) => {
                            retire(&mut r, a);
                            return Err(e);
                        }
                    };
                    (a, b, h, c)
                };
                let mut config = Config::default();
                let mut reservation = StartupReservation {
                    slots: [a, b],
                    committed: false,
                };
                let buffer = FIXTURE_BUFFER.load(SeqCst);
                if buffer != 0 {
                    config.socket_buffer_bytes = Some(buffer as usize);
                }
                let client = Client::begin_connect(
                    SocketAddrV4::new(host, p.port as u16),
                    target,
                    if protocol.is_empty() {
                        None
                    } else {
                        Some(protocol)
                    },
                    config,
                );
                let mut r = lock(&SOCKETS);
                match client {
                    Ok(client) => {
                        let s = Arc::new(Session {
                            client,
                            leases: Arc::new(Mutex::new(Totals::default())),
                        });
                        r[a].entry = Entry::Session(s.clone());
                        r[b].entry = Entry::Closer(s);
                        reservation.committed = true;
                        Ok((h, c))
                    }
                    Err(e) => Err(error(e)),
                }
            })();
            match result {
                Ok((session, closer)) => Start {
                    session,
                    closer,
                    code: PENDING,
                    reserved: 0,
                },
                Err(code) => Start {
                    session: 0,
                    closer: 0,
                    code,
                    reserved: 0,
                },
            }
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_socket_connect_finish(h: u64) -> u32 {
    contain(
        || PANIC,
        || match session(h, 1) {
            Ok(s) => match s.client.startup() {
                Ok(true) => OK,
                Ok(false) => PENDING,
                Err(e) => error(e),
            },
            Err(e) => e,
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_socket_connect_wait(h: u64) -> Wait {
    contain(
        || Wait {
            code: PANIC,
            fd: -1,
        },
        || match session(h, 1) {
            Ok(s) => Wait {
                code: PENDING,
                fd: s.client.ready_fd(),
            },
            Err(code) => Wait { code, fd: -1 },
        },
    )
}
#[no_mangle]
pub unsafe extern "C" fn ws_socket_offer_batch(
    h: u64,
    p: *const Span,
    count: u32,
    data: *const u8,
    len: u32,
) -> BatchAdmission {
    contain(
        || BatchAdmission {
            code: PANIC,
            accepted: 0,
        },
        || {
            let result = (|| {
                if count > 128
                    || (count != 1 && len > 65536)
                    || len > 1048576
                    || (count != 0 && p.is_null())
                {
                    return Err(OVERSIZE);
                }
                let data = bytes(data, len)?;
                let spans = if count == 0 {
                    &[]
                } else {
                    std::slice::from_raw_parts(p, count as usize)
                };
                let mut messages = [(Kind::Binary, &[][..]); 128];
                let mut offset = 0usize;
                for (i, span) in spans.iter().enumerate() {
                    let end = (span.offset as usize)
                        .checked_add(span.len as usize)
                        .ok_or(INVALID)?;
                    if span.reserved != 0 || span.offset as usize != offset || end > data.len() {
                        return Err(INVALID);
                    }
                    let b = &data[offset..end];
                    offset = end;
                    messages[i] = (
                        match span.kind {
                            0 => Kind::Binary,
                            1 => {
                                std::str::from_utf8(b).map_err(|_| 20u32)?;
                                Kind::Text
                            }
                            _ => return Err(INVALID),
                        },
                        b,
                    );
                }
                if offset != data.len() {
                    return Err(INVALID);
                }
                let s = session(h, 1)?;
                match s
                    .client
                    .offer_batch(&messages[..count as usize])
                    .map_err(error)?
                {
                    protocol::Admission::Accepted => Ok(()),
                    protocol::Admission::Rejected(protocol::Rejection::Busy) => Err(FULL),
                    protocol::Admission::Rejected(protocol::Rejection::Oversize) => Err(OVERSIZE),
                    _ => Err(END),
                }
            })();
            match result {
                Ok(()) => BatchAdmission {
                    code: OK,
                    accepted: count,
                },
                Err(code) => BatchAdmission { code, accepted: 0 },
            }
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_socket_take_batch(h: u64, count: u32, bytes: u32) -> BatchTake {
    contain(
        || BatchTake::error(PANIC),
        || {
            if count == 0
                || count > 128
                || bytes == 0
                || bytes > if count == 1 { 1048576 } else { 65536 }
            {
                return BatchTake::error(INVALID);
            }
            let s = match session(h, 1) {
                Ok(s) => s,
                Err(e) => return BatchTake::error(e),
            };
            let (i, lease) = match reserve(&mut lock(&SOCKETS)) {
                Ok(v) => v,
                Err(e) => return BatchTake::error(e),
            };
            let mut reservation = Reservation {
                slot: i,
                account: s.leases.clone(),
                credit: None,
                committed: false,
            };
            let read = s.client.receive_batch(count as usize, bytes as usize, |n| {
                let mut a = lock(&s.leases);
                if a.count == 8 || n > LEASE_BYTES - a.bytes {
                    false
                } else {
                    a.count += 1;
                    a.bytes += n;
                    reservation.credit = Some(n);
                    true
                }
            });
            match read {
                Ok(Some((bytes, spans))) => {
                    let owned = Lease {
                        account: s.leases.clone(),
                        bytes,
                        spans,
                    };
                    let value = BatchTake {
                        lease,
                        code: OK,
                        count: owned.spans.len() as u32,
                        bytes_len: owned.bytes.len() as u32,
                        terminal: 0,
                        bytes: owned.bytes.as_ptr(),
                        messages: owned.spans.as_ptr(),
                    };
                    lock(&SOCKETS)[i].entry = Entry::Lease(owned);
                    reservation.committed = true;
                    value
                }
                result => {
                    let mut value = BatchTake::error(match result {
                        Ok(None) => EMPTY,
                        Err(e) => e,
                        _ => unreachable!(),
                    });
                    value.terminal = s.client.status().terminal.map_or(0, terminal);
                    value
                }
            }
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_socket_release(h: u64) -> u32 {
    contain(
        || PANIC,
        || {
            let old = {
                let mut r = lock(&SOCKETS);
                let i = match lookup(&r, h) {
                    Ok(i) => i,
                    Err(e) => return e,
                };
                if !matches!(&r[i].entry, Entry::Lease(lease) if lease.spans.len() <= 128) {
                    return ROLE;
                }
                retire(&mut r, i)
            };
            drop(old);
            OK
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_socket_wait_begin(h: u64) -> Wait {
    contain(
        || Wait {
            code: PANIC,
            fd: -1,
        },
        || match session(h, 1) {
            Ok(s) => match s.client.wait_begin() {
                Ok(pending) => Wait {
                    code: if pending { PENDING } else { OK },
                    fd: s.client.ready_fd(),
                },
                Err(_) => Wait { code: BUSY, fd: -1 },
            },
            Err(code) => Wait { code, fd: -1 },
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_socket_wait_end(h: u64) -> u32 {
    contain(
        || PANIC,
        || match session(h, 1) {
            Ok(s) => {
                s.client.wait_end();
                OK
            }
            Err(e) => e,
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_socket_stop_begin(h: u64, role: u32) -> Wait {
    contain(
        || Wait {
            code: PANIC,
            fd: -1,
        },
        || match session(h, role) {
            Ok(s) => {
                if role == 1 && s.client.parked() {
                    return Wait { code: BUSY, fd: -1 };
                }
                s.client.request_close();
                Wait {
                    code: OK,
                    fd: s.client.done_fd(),
                }
            }
            Err(code) => Wait { code, fd: -1 },
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_socket_stop_finish(h: u64, role: u32) -> u32 {
    contain(
        || PANIC,
        || {
            let s = match session(h, role) {
                Ok(s) => s,
                Err(e) => return e,
            };
            if !s.client.finish_close() {
                return PENDING;
            }
            if role == 1 {
                s.client.discard_received();
            }
            let old = {
                let mut r = lock(&SOCKETS);
                let i = match lookup(&r, h) {
                    Ok(i) => i,
                    Err(e) => return e,
                };
                retire(&mut r, i)
            };
            drop(old);
            OK
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_socket_status(h: u64) -> Status {
    contain(
        || Status {
            code: PANIC,
            ..Status::default()
        },
        || {
            let s = match session(h, 1) {
                Ok(s) => s,
                Err(code) => {
                    return Status {
                        code,
                        ..Status::default()
                    }
                }
            };
            let p = s.client.status();
            let a = lock(&s.leases);
            Status {
                code: OK,
                state: if p.joined {
                    3
                } else if p.finished {
                    2
                } else if p.terminal.is_some() {
                    1
                } else {
                    0
                },
                terminal: p.terminal.map_or(0, terminal),
                reserved: 0,
                tx_admitted: p.tx_admitted,
                tx_complete: p.tx_messages,
                rx_complete: p.rx_messages,
                rx_consumed: p.rx_consumed,
                tx_failed: p.tx_failed,
                rx_discarded: p.rx_discarded,
                tx_bytes: p.tx_queue_bytes as u32,
                tx_count: p.tx_queue_descriptors as u32,
                rx_bytes: p.rx_queue_bytes as u32,
                rx_count: p.rx_queue_descriptors as u32,
                lease_bytes: a.bytes as u32,
                leases: a.count as u32,
                tx_completed_ns: p.tx_completed_ns,
                rx_completed_ns: p.rx_completed_ns,
                control_ns: p.control_service_gap_peak.as_nanos() as u64,
                native_live: LIVE.load(SeqCst),
                native_peak: PEAK.load(SeqCst),
                native_budget: p.native_memory_budget as u64
                    + LEASE_BYTES as u64
                    + 8 * 128 * 16
                    + std::mem::size_of::<Session>() as u64
                    + std::mem::size_of::<Totals>() as u64
                    + 64,
                socket_send_buffer: p.socket_send_buffer,
                socket_receive_buffer: p.socket_receive_buffer,
            }
        },
    )
}
#[no_mangle]
pub extern "C" fn ws_socket_parked() -> u32 {
    contain(
        || u32::MAX,
        || {
            let s = {
                let r = lock(&SOCKETS);
                r.iter().find_map(|slot| match &slot.entry {
                    Entry::Session(s) => Some(s.clone()),
                    _ => None,
                })
            };
            s.map_or(0, |s| u32::from(s.client.parked()))
        },
    )
}
#[no_mangle]
pub unsafe extern "C" fn ws_socket_fixture_memory(h: u64, idle: *mut u64, live: *mut u64) -> u32 {
    match session(h, 1) {
        Ok(s) => {
            let p = s.client.status();
            *idle = p.native_memory_idle as u64;
            *live = p.native_memory_live as u64;
            OK
        }
        Err(code) => code,
    }
}
#[no_mangle]
pub unsafe extern "C" fn ws_socket_fixture_evidence(h: u64, values: *mut u64, count: u32) -> u32 {
    if values.is_null() || count != 9 {
        return INVALID;
    }
    match session(h, 1) {
        Ok(s) => {
            let p = s.client.status();
            let evidence = [
                p.incoming_partial_age.map_or(0, |v| v.as_nanos() as u64),
                p.outgoing_partial_age.map_or(0, |v| v.as_nanos() as u64),
                p.turns,
                p.service_gap_peak.as_nanos() as u64,
                p.read_service_gap_peak.as_nanos() as u64,
                p.write_service_gap_peak.as_nanos() as u64,
                p.control_service_gap_peak.as_nanos() as u64,
                p.worker_tid as u64,
                u64::from(p.allocation_invalid),
            ];
            std::ptr::copy_nonoverlapping(evidence.as_ptr(), values, evidence.len());
            OK
        }
        Err(code) => code,
    }
}
const _: () = {
    assert!(std::mem::size_of::<Span>() == 16);
    assert!(std::mem::size_of::<BatchTake>() == 40);
    assert!(std::mem::size_of::<Status>() == 144);
};
