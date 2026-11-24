use confluxion_ws_native::protocol::*;
use confluxion_ws_native::{
    allocation_scope_finish, allocation_scope_snapshot, allocation_scope_start,
};
use serde_json::{json, Value};
use std::{
    cell::RefCell,
    fs,
    io::{self, Read, Write},
    rc::Rc,
    time::{SystemTime, UNIX_EPOCH},
};

const RESPONSE: &str = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
const SEED: u64 = 0x58_7328_6a04;
struct Wire {
    input: Vec<u8>,
    at: usize,
    chunks: Vec<usize>,
    chunk: usize,
    left: usize,
    output: Vec<u8>,
    quantum: usize,
    write_block: bool,
    read_block: bool,
    alternate: bool,
    eof: bool,
    fatal: bool,
}
#[derive(Clone)]
struct Io(Rc<RefCell<Wire>>);
impl Io {
    fn new(input: Vec<u8>, chunks: Vec<usize>) -> Self {
        Self(Rc::new(RefCell::new(Wire {
            input,
            at: 0,
            chunks,
            chunk: 0,
            left: 0,
            output: Vec::with_capacity(2 * MAXIMUM + 32768),
            quantum: usize::MAX,
            write_block: false,
            read_block: false,
            alternate: false,
            eof: false,
            fatal: false,
        })))
    }
}
impl Read for Io {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut w = self.0.borrow_mut();
        if w.fatal {
            return Err(io::ErrorKind::ConnectionReset.into());
        }
        if w.read_block {
            w.read_block = false;
            return Err(io::ErrorKind::WouldBlock.into());
        }
        if w.at == w.input.len() {
            return if w.eof {
                Ok(0)
            } else {
                Err(io::ErrorKind::WouldBlock.into())
            };
        }
        if w.left == 0 {
            w.left = w.chunks.get(w.chunk).copied().unwrap_or(READ);
            w.chunk += 1;
        }
        let n = buf.len().min(w.left).min(w.input.len() - w.at);
        buf[..n].copy_from_slice(&w.input[w.at..w.at + n]);
        w.at += n;
        w.left -= n;
        if w.left == 0 {
            w.read_block = true;
        }
        Ok(n)
    }
}
impl Write for Io {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut w = self.0.borrow_mut();
        if w.fatal {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if w.write_block {
            if w.alternate {
                w.write_block = false;
            }
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let n = buf.len().min(w.quantum);
        assert!(w.output.len() + n <= w.output.capacity());
        w.output.extend_from_slice(&buf[..n]);
        if w.alternate {
            w.write_block = true;
        }
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn frame(op: u8, fin: bool, p: &[u8], mask: Option<[u8; 4]>) -> Vec<u8> {
    let mut v = vec![op | if fin { 128 } else { 0 }];
    let m = if mask.is_some() { 128 } else { 0 };
    match p.len() {
        0..=125 => v.push(m | p.len() as u8),
        126..=65535 => {
            v.push(m | 126);
            v.extend_from_slice(&(p.len() as u16).to_be_bytes());
        }
        _ => {
            v.push(m | 127);
            v.extend_from_slice(&(p.len() as u64).to_be_bytes());
        }
    }
    if let Some(m) = mask {
        v.extend_from_slice(&m);
    }
    v.extend(
        p.iter()
            .enumerate()
            .map(|(i, b)| b ^ mask.map_or(0, |m| m[i % 4])),
    );
    v
}
fn decode(bytes: &[u8], expected: &[(u8, &[u8])]) {
    let mut at = 0;
    for &(op, p) in expected {
        assert_eq!(bytes[at], 128 | op);
        let start = at;
        at += 1;
        let tag = bytes[at];
        at += 1;
        assert_ne!(tag & 128, 0);
        let n = match tag & 127 {
            126 => {
                let n = u16::from_be_bytes(bytes[at..at + 2].try_into().unwrap()) as usize;
                at += 2;
                assert!(n >= 126);
                n
            }
            127 => {
                let n = u64::from_be_bytes(bytes[at..at + 8].try_into().unwrap()) as usize;
                at += 8;
                assert!(n >= 65536);
                n
            }
            n => n as usize,
        };
        assert_eq!(n, p.len());
        let mask: [u8; 4] = bytes[at..at + 4].try_into().unwrap();
        at += 4;
        for (i, &b) in p.iter().enumerate() {
            assert_eq!(bytes[at + i] ^ mask[i % 4], b);
        }
        at += n;
        assert_eq!(&bytes[start..at], frame(op, true, p, Some(mask)));
    }
    assert_eq!(at, bytes.len(), "extra or duplicated frame bytes");
}
fn open(io: Io, limits: Limits, policy: Option<&str>) -> Result<Protocol<Io>, Error> {
    let mut u = Upgrade::fixture(io, policy, limits)?;
    for _ in 0..30000 {
        match u.resume()? {
            UpgradeStep::Pending(next) => u = next,
            UpgradeStep::Ready(p) => return Ok(p),
        }
    }
    panic!("upgrade did not progress")
}
fn drain(p: &mut Protocol<Io>) {
    for _ in 0..2000000 {
        if p.flush().unwrap() == FlushStep::Drained {
            return;
        }
    }
    panic!("flush did not progress")
}
fn receive(p: &mut Protocol<Io>, expected: &[u8], text: bool) {
    for _ in 0..30000 {
        match p.read().unwrap() {
            ReadStep::Pending => {}
            ReadStep::Text(v) if text => {
                assert_eq!(v.as_bytes(), expected);
                return;
            }
            ReadStep::Binary(v) if !text => {
                assert_eq!(v, expected);
                return;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    panic!("read did not progress")
}
fn input(frames: &[u8]) -> Vec<u8> {
    [RESPONSE.as_bytes(), frames].concat()
}
fn partitions(n: usize) -> Vec<usize> {
    let mut seed = SEED;
    let mut left = n;
    let mut v = Vec::new();
    while left > 0 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let k = (1 + (seed as usize % 4096)).min(left);
        v.push(k);
        left -= k;
    }
    v
}

#[test]
fn bounded_protocol_gate() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let run = format!(
        "cargo-{}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        std::process::id()
    );
    let dir = root.join("results/ws").join(run).join("phase-2");
    fs::create_dir_all(&dir).unwrap();
    let summary = dir.join("summary.json");
    fs::write(
        &summary,
        json!({"status":"fail","reason":"gate incomplete","seed":SEED}).to_string(),
    )
    .unwrap();
    let limits = Limits::new(MAXIMUM, MAXIMUM).unwrap();
    let mut cases: Vec<Value> = Vec::new();
    let rng_layout = std::alloc::Layout::new::<[usize; 2]>()
        .extend(std::alloc::Layout::new::<
            rand::rngs::ReseedingRng<rand_chacha::ChaCha12Core, rand::rngs::OsRng>,
        >())
        .unwrap()
        .0
        .pad_to_align();
    let cold_io = Io::new(vec![], vec![]);
    allocation_scope_start();
    drop(Upgrade::start(cold_io.clone(), "127.0.0.1", "/", None, limits).unwrap());
    let cold = allocation_scope_finish();
    assert!(!cold.invalid);
    assert_eq!(cold.blocks, 1);
    assert_eq!(cold.live, rng_layout.size());
    let mut memory = vec![
        json!({"name":"cold-rng-and-upgrade","peak":cold.peak,"live_thread_rng":cold.live,"rng_layout":rng_layout.size(),"blocks":cold.blocks,"allocations":cold.allocations}),
    ];

    let raw = frame(2, true, b"x", None);
    let literal = [
        0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58,
    ];
    assert_eq!(
        frame(1, true, b"Hello", Some([0x37, 0xfa, 0x21, 0x3d])),
        literal
    );
    decode(&literal, &[(1, b"Hello")]);
    assert_eq!(
        frame(1, true, b"Hello", None),
        [0x81, 5, b'H', b'e', b'l', b'l', b'o']
    );
    for split in 0..=RESPONSE.len() {
        let io = Io::new(
            input(&raw),
            if split == 0 {
                vec![RESPONSE.len()]
            } else {
                vec![split, RESPONSE.len() - split + raw.len()]
            },
        );
        let mut p = open(io.clone(), limits, None).unwrap();
        receive(&mut p, b"x", false);
        assert_eq!(std::str::from_utf8(&io.0.borrow().output).unwrap(),"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n");
    }
    let mut p = open(
        Io::new(input(&raw), vec![1; RESPONSE.len() + raw.len()]),
        limits,
        None,
    )
    .unwrap();
    receive(&mut p, b"x", false);
    cases.push(json!({"name":"rfc-accept-header-every-split-and-tiny-reads","splits":RESPONSE.len()+1,"status":"pass"}));
    for quantum in 1..=7 {
        let io = Io::new(RESPONSE.as_bytes().to_vec(), vec![1; RESPONSE.len()]);
        {
            let mut w = io.0.borrow_mut();
            w.quantum = quantum;
            w.alternate = true;
        }
        drop(open(io.clone(), limits, None).unwrap());
        assert_eq!(std::str::from_utf8(&io.0.borrow().output).unwrap(),"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n");
    }
    cases.push(json!({"name":"upgrade-partial-writes-one-through-seven-no-duplicate-request","status":"pass"}));

    let negatives = vec![
        (
            "status",
            RESPONSE.replace("101 Switching Protocols", "200 OK"),
            None,
        ),
        ("version", RESPONSE.replace("HTTP/1.1", "HTTP/1.0"), None),
        (
            "accept-wrong",
            RESPONSE.replace("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", "wrong"),
            None,
        ),
        (
            "accept-missing",
            RESPONSE.replace("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n", ""),
            None,
        ),
        (
            "upgrade-missing",
            RESPONSE.replace("Upgrade: websocket\r\n", ""),
            None,
        ),
        (
            "upgrade-wrong",
            RESPONSE.replace("Upgrade: websocket", "Upgrade: other"),
            None,
        ),
        (
            "connection-missing",
            RESPONSE.replace("Connection: Upgrade\r\n", ""),
            None,
        ),
        (
            "connection-wrong",
            RESPONSE.replace("Connection: Upgrade", "Connection: close"),
            None,
        ),
        (
            "malformed",
            RESPONSE.replace("Upgrade: websocket", "Bad header"),
            None,
        ),
        (
            "extension",
            RESPONSE.replace(
                "\r\n\r\n",
                "\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n",
            ),
            None,
        ),
        (
            "duplicate-accept",
            RESPONSE.replace(
                "\r\n\r\n",
                "\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n",
            ),
            None,
        ),
        (
            "duplicate-upgrade",
            RESPONSE.replace("\r\n\r\n", "\r\nUpgrade: websocket\r\n\r\n"),
            None,
        ),
        (
            "duplicate-connection",
            RESPONSE.replace("\r\n\r\n", "\r\nConnection: Upgrade\r\n\r\n"),
            None,
        ),
        (
            "protocol-unrequested",
            RESPONSE.replace("\r\n\r\n", "\r\nSec-WebSocket-Protocol: alpha\r\n\r\n"),
            None,
        ),
        ("protocol-missing", RESPONSE.to_owned(), Some("alpha")),
        (
            "protocol-wrong",
            RESPONSE.replace("\r\n\r\n", "\r\nSec-WebSocket-Protocol: beta\r\n\r\n"),
            Some("alpha"),
        ),
        (
            "protocol-multiple",
            RESPONSE.replace(
                "\r\n\r\n",
                "\r\nSec-WebSocket-Protocol: alpha, beta\r\n\r\n",
            ),
            Some("alpha"),
        ),
        (
            "protocol-duplicate",
            RESPONSE.replace(
                "\r\n\r\n",
                "\r\nSec-WebSocket-Protocol: alpha\r\nSec-WebSocket-Protocol: alpha\r\n\r\n",
            ),
            Some("alpha"),
        ),
    ];
    for (name, bytes, policy) in negatives {
        assert!(
            open(Io::new(bytes.into_bytes(), vec![]), limits, policy).is_err(),
            "{name}"
        );
        cases.push(json!({"name":name,"status":"pass"}));
    }
    let response = RESPONSE.replace("\r\n\r\n", "\r\nSec-WebSocket-Protocol: alpha\r\n\r\n");
    assert!(open(
        Io::new(response.into_bytes(), vec![]),
        limits,
        Some("alpha")
    )
    .is_ok());
    for extra in [0, 1] {
        let prefix = RESPONSE.trim_end_matches("\r\n");
        let padding = "a".repeat(HEADER + extra - prefix.len() - 9);
        let response = format!("{prefix}\r\nX: {padding}\r\n\r\n");
        assert_eq!(response.len(), HEADER + extra);
        assert_eq!(
            open(Io::new(response.into_bytes(), vec![]), limits, None).is_ok(),
            extra == 0
        );
    }
    let truncated = Io::new(RESPONSE.as_bytes()[..60].to_vec(), vec![]);
    truncated.0.borrow_mut().eof = true;
    assert!(open(truncated, limits, None).is_err());
    cases.push(
        json!({"name":"header-cap-exact-plus-one-truncated-and-policy-success","status":"pass"}),
    );

    for n in [0, 125, 126, 65535, 65536, MAXIMUM] {
        let payload: Vec<u8> = (0..n).map(|i| (i.wrapping_mul(31) % 251) as u8).collect();
        let f = frame(2, true, &payload, None);
        let io = Io::new(input(&f), partitions(RESPONSE.len() + f.len()));
        let mut p = open(io.clone(), limits, None).unwrap();
        receive(&mut p, &payload, false);
        drain(&mut p);
        io.0.borrow_mut().output.clear();
        assert_eq!(
            p.offer(Outbound::Binary(&payload)).unwrap(),
            Admission::Accepted
        );
        drain(&mut p);
        decode(&io.0.borrow().output, &[(2, &payload)]);
        cases.push(json!({"name":format!("binary-length-{n}-both-directions"),"seed":SEED,"status":"pass"}));
    }
    for (op, payload) in [
        (2, &b"small"[..]),
        (1, &b"text"[..]),
        (9, &b"ping"[..]),
        (10, &b"pong"[..]),
        (8, &[3, 232][..]),
    ] {
        let f = frame(op, true, payload, None);
        for split in 1..f.len() {
            let io = Io::new(input(&f), vec![RESPONSE.len(), split, f.len() - split]);
            let mut p = open(io.clone(), limits, None).unwrap();
            let mut observed = false;
            for _ in 0..100 {
                match p.read().unwrap() {
                    ReadStep::Pending => continue,
                    ReadStep::Binary(v) if op == 2 => assert_eq!(v, payload),
                    ReadStep::Text(v) if op == 1 => assert_eq!(v.as_bytes(), payload),
                    ReadStep::Ping(v) if op == 9 => assert_eq!(v, payload),
                    ReadStep::Pong(v) if op == 10 => assert_eq!(v, payload),
                    ReadStep::Close(Some((1000, ""))) if op == 8 => {}
                    other => panic!("{other:?}"),
                }
                observed = true;
                break;
            }
            assert!(observed);
        }
        cases.push(json!({"name":format!("small-opcode-{op}-every-split"),"splits":f.len()-1,"status":"pass"}));
    }
    let small = Limits::new(128, 192).unwrap();
    for (name, bytes) in [
        ("reserved-data-opcode", vec![0x83, 0]),
        ("reserved-control-opcode", vec![0x8b, 0]),
        ("rsv1", vec![0xc2, 0]),
        ("rsv2", vec![0xa2, 0]),
        ("rsv3", vec![0x92, 0]),
        ("masked-server", frame(2, true, b"x", Some([1, 2, 3, 4]))),
        ("nonminimal-16", vec![0x82, 126, 0, 1, b'x']),
        (
            "nonminimal-64",
            vec![0x82, 127, 0, 0, 0, 0, 0, 0, 0, 1, b'x'],
        ),
        ("high-bit-64", vec![0x82, 127, 128, 0, 0, 0, 0, 0, 0, 0]),
        ("unexpected-continuation", frame(0, true, b"x", None)),
        (
            "overlapping-fragments",
            [frame(2, false, b"a", None), frame(1, true, b"b", None)].concat(),
        ),
        ("fragmented-ping", frame(9, false, b"x", None)),
        ("control-126", frame(9, true, &[0; 126], None)),
        ("close-one-byte", frame(8, true, &[1], None)),
        ("close-invalid-utf8", frame(8, true, &[3, 232, 255], None)),
        ("utf8-invalid", frame(1, true, &[255], None)),
        (
            "utf8-incomplete-final",
            [
                frame(1, false, &[0xf0, 0x9f], None),
                frame(0, true, &[0x92], None),
            ]
            .concat(),
        ),
        ("frame-limit-plus-one-header", vec![0x82, 126, 0, 129]),
        (
            "fragment-message-limit",
            [
                frame(2, false, &[0; 128], None),
                frame(0, true, &[0; 65], None),
            ]
            .concat(),
        ),
    ] {
        let io = Io::new(input(&[]), vec![]);
        let mut p = open(io.clone(), small, None).unwrap();
        {
            let mut w = io.0.borrow_mut();
            w.input.extend_from_slice(&bytes);
        }
        let mut failed = false;
        for _ in 0..100 {
            match p.read() {
                Err(Error::Protocol | Error::Limit) => {
                    failed = true;
                    break;
                }
                Ok(ReadStep::Pending) => {}
                other => panic!("{name}: {other:?}"),
            }
        }
        assert!(failed, "{name}");
        assert_eq!(p.offer(Outbound::Binary(b"x")), Err(Error::Terminal));
        cases.push(json!({"name":name,"status":"pass"}));
    }
    for (name, bytes) in [
        ("eof-header", vec![0x82, 126, 0]),
        ("eof-payload", vec![0x82, 3, b'x']),
    ] {
        let io = Io::new(input(&[]), vec![]);
        let mut p = open(io.clone(), limits, None).unwrap();
        {
            let mut w = io.0.borrow_mut();
            w.input.extend(bytes);
            w.eof = true;
        }
        assert_eq!(p.read(), Err(Error::Protocol));
        cases.push(json!({"name":name,"status":"pass"}));
    }
    let exact = [
        frame(2, false, &[7; 128], None),
        frame(0, true, &[7; 64], None),
    ]
    .concat();
    let mut p = open(
        Io::new(input(&exact), partitions(RESPONSE.len() + exact.len())),
        small,
        None,
    )
    .unwrap();
    receive(&mut p, &[7; 192], false);
    cases.push(json!({"name":"exact-frame-and-fragment-message-limits","status":"pass"}));
    for maximum_message in [64, 128] {
        let bounds = Limits::new(128, maximum_message).unwrap();
        let exact = vec![8; maximum_message];
        let excess = vec![8; maximum_message + 1];
        let io = Io::new(input(&frame(2, true, &exact, None)), vec![]);
        let mut p = open(io.clone(), bounds, None).unwrap();
        receive(&mut p, &exact, false);
        drain(&mut p);
        io.0.borrow_mut().output.clear();
        allocation_scope_start();
        assert_eq!(
            p.offer(Outbound::Binary(&excess)).unwrap(),
            Admission::Rejected(Rejection::Oversize)
        );
        let rejected = allocation_scope_finish();
        assert_eq!(rejected.allocations, 0);
        assert_eq!(
            p.offer(Outbound::Binary(&exact)).unwrap(),
            Admission::Accepted
        );
        drain(&mut p);
        decode(&io.0.borrow().output, &[(2, &exact)]);
        let io = Io::new(input(&frame(2, true, &excess, None)), vec![]);
        let mut p = open(io, bounds, None).unwrap();
        assert_eq!(p.read(), Err(Error::Limit));
    }
    cases.push(
        json!({"name":"independent-configured-frame-message-caps-both-directions","status":"pass"}),
    );

    let fragments = [
        frame(1, false, &[0xf0], None),
        frame(9, true, b"interleave", None),
        frame(0, false, &[0x9f, 0x92], None),
        frame(0, true, &[0xa9], None),
        frame(10, true, b"unsolicited", None),
    ]
    .concat();
    let io = Io::new(input(&fragments), vec![1; RESPONSE.len() + fragments.len()]);
    let mut p = open(io.clone(), limits, None).unwrap();
    let mut ping = false;
    let mut text = false;
    let mut pong = false;
    for _ in 0..1000 {
        match p.read().unwrap() {
            ReadStep::Pending => {}
            ReadStep::Ping(b"interleave") => {
                assert!(!ping);
                ping = true
            }
            ReadStep::Text("💩") => {
                assert!(!text);
                text = true
            }
            ReadStep::Pong(b"unsolicited") => {
                assert!(!pong);
                pong = true;
                break;
            }
            other => panic!("{other:?}"),
        }
    }
    assert!(ping && text && pong);
    drain(&mut p);
    let out = io.0.borrow();
    let start = out
        .output
        .windows(4)
        .position(|b| b == b"\r\n\r\n")
        .unwrap()
        + 4;
    decode(&out.output[start..], &[(10, b"interleave")]);
    drop(out);
    let invalid_binary = [255, 0xf0, 0x9f];
    let mut p = open(
        Io::new(input(&frame(2, true, &invalid_binary, None)), vec![]),
        limits,
        None,
    )
    .unwrap();
    receive(&mut p, &invalid_binary, false);
    cases.push(json!({"name":"fragmented-utf8-ping-unsolicited-pong-and-binary","status":"pass"}));

    for q in 1..=7 {
        let io = Io::new(input(&[]), vec![]);
        let mut p = open(io.clone(), limits, None).unwrap();
        {
            let mut w = io.0.borrow_mut();
            w.output.clear();
            w.quantum = q;
            w.alternate = true;
            w.write_block = true;
        }
        assert_eq!(
            p.offer(Outbound::Binary(b"first")).unwrap(),
            Admission::Accepted
        );
        assert_eq!(
            p.offer(Outbound::Binary(b"not admitted")).unwrap(),
            Admission::Rejected(Rejection::Busy)
        );
        io.0.borrow_mut()
            .input
            .extend(frame(9, true, b"control", None));
        assert_eq!(p.read().unwrap(), ReadStep::Ping(b"control"));
        drain(&mut p);
        assert_eq!(p.offer(Outbound::Binary(b"")).unwrap(), Admission::Accepted);
        assert_eq!(
            p.offer(Outbound::Binary(b"no")).unwrap(),
            Admission::Rejected(Rejection::Busy)
        );
        drain(&mut p);
        let large = vec![9; 126];
        assert_eq!(
            p.offer(Outbound::Binary(&large)).unwrap(),
            Admission::Accepted
        );
        drain(&mut p);
        let huge = vec![5; 65536];
        assert_eq!(
            p.offer(Outbound::Binary(&huge)).unwrap(),
            Admission::Accepted
        );
        drain(&mut p);
        assert_eq!(
            p.offer(Outbound::Text("last")).unwrap(),
            Admission::Accepted
        );
        drain(&mut p);
        for _ in 0..100 {
            if p.close(Some((1000, "bye"))).unwrap() == FlushStep::Drained {
                break;
            }
        }
        assert_eq!(
            p.offer(Outbound::Binary(b"no")).unwrap(),
            Admission::Rejected(Rejection::Closing)
        );
        let wire = io.0.borrow();
        decode(
            &wire.output,
            &[
                (2, b"first"),
                (10, b"control"),
                (2, b""),
                (2, &large),
                (2, &huge),
                (1, b"last"),
                (8, &[3, 232, b'b', b'y', b'e']),
            ],
        );
        drop(wire);
        {
            let mut w = io.0.borrow_mut();
            w.input.extend(frame(8, true, &[3, 232], None));
            w.eof = true;
        }
        assert_eq!(p.read().unwrap(), ReadStep::Close(Some((1000, ""))));
        assert_eq!(p.read().unwrap(), ReadStep::Closed);
        cases.push(json!({"name":format!("partial-writes-{q}-wouldblock-exact-transcript-admission-local-close"),"status":"pass"}));
    }
    for (name, received, reply, code) in [
        ("remote-empty-close", vec![], vec![], None),
        (
            "remote-normal-close",
            vec![3, 232, b'o', b'k'],
            vec![3, 232, b'o', b'k'],
            Some((1000, "ok")),
        ),
        (
            "remote-protocol-error-close",
            vec![3, 234],
            vec![3, 234],
            Some((1002, "")),
        ),
        (
            "remote-invalid-code-fails-connection",
            vec![3, 237],
            [vec![3, 234], b"Protocol violation".to_vec()].concat(),
            Some((1002, "Protocol violation")),
        ),
    ] {
        let io = Io::new(input(&[]), vec![]);
        let mut p = open(io.clone(), limits, None).unwrap();
        {
            let mut w = io.0.borrow_mut();
            w.output.clear();
            w.quantum = 1;
            w.alternate = true;
            w.input.extend(frame(8, true, &received, None));
        }
        assert_eq!(p.read().unwrap(), ReadStep::Close(code));
        assert_eq!(
            p.offer(Outbound::Binary(b"no")).unwrap(),
            Admission::Rejected(Rejection::Closing)
        );
        drain(&mut p);
        decode(&io.0.borrow().output, &[(8, &reply)]);
        io.0.borrow_mut().eof = true;
        assert_eq!(p.read().unwrap(), ReadStep::Closed);
        cases.push(json!({"name":name,"status":"pass"}));
    }
    let io = Io::new(input(&[]), vec![]);
    let mut p = open(io.clone(), limits, None).unwrap();
    io.0.borrow_mut().eof = true;
    assert_eq!(p.read(), Err(Error::Protocol));
    cases.push(json!({"name":"abrupt-eof","status":"pass"}));
    for suffix in [1, raw.len()] {
        let bytes = input(&[raw.clone(), frame(2, true, b"next", None)].concat());
        let mut p = open(
            Io::new(bytes, vec![RESPONSE.len() + suffix, READ]),
            limits,
            None,
        )
        .unwrap();
        receive(&mut p, b"x", false);
        receive(&mut p, b"next", false);
    }
    let padding = "x".repeat(6000);
    let big_header = RESPONSE.replace("\r\n\r\n", &format!("\r\nX-long: {padding}\r\n\r\n"));
    let io = Io::new(
        [big_header.as_bytes(), &frame(2, true, b"tail", None), &raw].concat(),
        vec![],
    );
    let mut p = open(io, limits, None).unwrap();
    receive(&mut p, b"tail", false);
    receive(&mut p, b"x", false);
    cases.push(json!({"name":"coalesced-complete-partial-and-replay-chunk-tail","status":"pass"}));

    let name_size = std::mem::size_of::<tungstenite::http::HeaderName>();
    let value_size = std::mem::size_of::<tungstenite::http::HeaderValue>();
    let bucket = 8 + name_size + value_size + std::mem::size_of::<Option<(usize, usize)>>();
    let extra = value_size + 2 * std::mem::size_of::<(usize, usize)>();
    let shared = std::mem::size_of::<Vec<u8>>() + 2 * std::mem::size_of::<usize>();
    let http =
        (1024 + 512) * 4 + (768 + 384) * bucket + (128 + 64) * extra + 2 * HEADER + 248 * shared;
    let q = 2048;
    let request = 6 * q + http + 3 * 64 + 28 + 24 + 8 * shared;
    let n = MAXIMUM + READ + 14;
    let w = MAXIMUM + 14 + 278;
    let diagnostics = 3 * 128;
    let metadata = 8 * shared;
    let upgrade_budget = HEADER
        + READ
        + 3 * (HEADER + READ)
        + READ
        + http
        + request
        + 4 * n
        + metadata
        + diagnostics;
    let open_budget = 12 * n + 3 * MAXIMUM + 3 * w + MAXIMUM + 4 * 139 + metadata + diagnostics;
    let budget = rng_layout.size() + upgrade_budget.max(open_budget);
    assert!(cold.peak <= budget);
    let oversized = vec![0; MAXIMUM + 1];
    let maximum = vec![17; MAXIMUM];
    let fragmented = [
        frame(2, false, &maximum[..MAXIMUM / 2], None),
        frame(9, true, b"retained-control", None),
        frame(0, true, &maximum[MAXIMUM / 2..], None),
    ]
    .concat();
    let utf8 = "💩".repeat(MAXIMUM / 4);
    let text_frames = [
        frame(1, false, &utf8.as_bytes()[..MAXIMUM / 2 + 1], None),
        frame(0, true, &utf8.as_bytes()[MAXIMUM / 2 + 1..], None),
    ]
    .concat();
    let header_many = RESPONSE.replace(
        "\r\n\r\n",
        &format!(
            "{}\r\n\r\n",
            (0..122)
                .map(|i| format!("\r\nX-{i}: {}", "v".repeat(32)))
                .collect::<String>()
        ),
    );
    let header_unique = RESPONSE.replace(
        "\r\n\r\n",
        &format!(
            "{}\r\n\r\n",
            (0..121)
                .map(|i| format!("\r\nX-{i}: {}", "v".repeat(32)))
                .collect::<String>()
        ),
    );
    let header_duplicates = RESPONSE.replace(
        "\r\n\r\n",
        &format!("{}\r\n\r\n", "\r\nX-repeat: v".repeat(121)),
    );
    let long_name = RESPONSE.replace(
        "\r\n\r\n",
        &format!("\r\n{}: {}\r\n\r\n", "X".repeat(3000), "v".repeat(3000)),
    );
    for (name, bytes, action) in [
        (
            "fragment-assembly-saturated-output-control",
            input(&fragmented),
            0,
        ),
        ("maximum-utf8-assembly", input(&text_frames), 1),
        ("header-many-lines-limit", header_many.into_bytes(), 2),
        (
            "failed-upgrade",
            RESPONSE
                .replace("101 Switching Protocols", "400 Nope")
                .into_bytes(),
            2,
        ),
        ("blocked-upgrade-drop", vec![], 3),
        ("fatal-io-drop", input(&[]), 4),
        (
            "maximum-frame-close",
            input(&frame(2, true, &maximum, None)),
            5,
        ),
        ("header-124-unique-lines", header_unique.into_bytes(), 6),
        (
            "header-124-duplicate-values",
            header_duplicates.into_bytes(),
            6,
        ),
        ("header-large-name-and-value", long_name.into_bytes(), 6),
        ("header-cap-exhaustion", vec![b'x'; HEADER + 1], 2),
        (
            "malformed-length-terminal-drop",
            input(&[0x82, 126, 0, 1, b'x']),
            2,
        ),
    ] {
        let io = Io::new(bytes, vec![]);
        allocation_scope_start();
        match action {
            2 => {
                assert!(open(io.clone(), limits, None).is_err());
            }
            3 => {
                let u = Upgrade::fixture(io.clone(), None, limits).unwrap();
                match u.resume().unwrap() {
                    UpgradeStep::Pending(u) => drop(u),
                    UpgradeStep::Ready(_) => panic!("unexpected ready"),
                }
            }
            6 => {
                drop(open(io.clone(), limits, None).unwrap());
            }
            _ => {
                let mut p = open(io.clone(), limits, None).unwrap();
                if action == 0 {
                    io.0.borrow_mut().write_block = true;
                    assert_eq!(
                        p.offer(Outbound::Binary(&maximum)).unwrap(),
                        Admission::Accepted
                    );
                    let before = allocation_scope_snapshot().allocations;
                    assert_eq!(
                        p.offer(Outbound::Binary(&maximum)).unwrap(),
                        Admission::Rejected(Rejection::Busy)
                    );
                    assert_eq!(allocation_scope_snapshot().allocations, before);
                    let mut observed = false;
                    for _ in 0..10000 {
                        match p.read().unwrap() {
                            ReadStep::Pending => {}
                            ReadStep::Ping(b"retained-control") => {
                                observed = true;
                                break;
                            }
                            other => panic!("{other:?}"),
                        }
                    }
                    assert!(observed);
                    receive(&mut p, &maximum, false);
                    io.0.borrow_mut().write_block = false;
                    drain(&mut p);
                } else if action == 1 {
                    receive(&mut p, utf8.as_bytes(), true);
                } else if action == 4 {
                    io.0.borrow_mut().fatal = true;
                    assert!(p.read().is_err());
                } else {
                    receive(&mut p, &maximum, false);
                    drain(&mut p);
                    assert_eq!(p.close(Some((1000, "done"))).unwrap(), FlushStep::Drained);
                }
                if action == 0 || action == 5 {
                    let before = allocation_scope_snapshot().allocations;
                    assert_eq!(
                        p.offer(Outbound::Binary(&oversized)).unwrap(),
                        Admission::Rejected(if action == 5 {
                            Rejection::Closing
                        } else {
                            Rejection::Oversize
                        })
                    );
                    assert_eq!(allocation_scope_snapshot().allocations, before);
                    if action == 0 {
                        assert_eq!(p.close(Some((1005, ""))), Err(Error::Input));
                        assert_eq!(p.close(Some((1000, &utf8))), Err(Error::Input));
                    }
                }
            }
        }
        let m = allocation_scope_finish();
        assert!(!m.invalid);
        assert_eq!((m.live, m.blocks), (0, 0));
        assert_eq!(m.allocations, m.frees);
        assert!(m.peak + rng_layout.size() <= budget);
        memory.push(json!({"name":name,"peak":m.peak,"allocations":m.allocations,"frees":m.frees,"reallocations":m.reallocations,"zeroed":m.zeroed,"largest_layout":m.largest,"max_alignment":m.alignment,"live":m.live,"blocks":m.blocks}));
    }
    let io = Io::new(input(&[]), vec![]);
    allocation_scope_start();
    let mut p = open(io.clone(), limits, None).unwrap();
    let mut idle = None;
    for cycle in 0..20 {
        assert_eq!(
            p.offer(Outbound::Binary(b"warm")).unwrap(),
            Admission::Accepted
        );
        drain(&mut p);
        let current = allocation_scope_snapshot().live;
        if cycle == 0 {
            idle = Some(current);
        } else {
            assert_eq!(Some(current), idle);
        }
        assert!(current <= budget);
    }
    drop(p);
    let cycles = allocation_scope_finish();
    assert_eq!((cycles.live, cycles.blocks), (0, 0));
    assert!(!cycles.invalid);
    memory.push(json!({"name":"repeated-idle-cycles","peak":cycles.peak,"live":cycles.live,"blocks":cycles.blocks}));
    let authority = "a".repeat(128);
    let target = format!("/{}", "a".repeat(1023));
    allocation_scope_start();
    drop(Upgrade::start(io.clone(), &authority, &target, Some("alpha"), limits).unwrap());
    let request_memory = allocation_scope_finish();
    assert_eq!((request_memory.live, request_memory.blocks), (0, 0));
    assert!(!request_memory.invalid);
    assert!(request_memory.peak + rng_layout.size() <= budget);
    memory.push(json!({"name":"maximum-request-construction","peak":request_memory.peak,"live":request_memory.live,"blocks":request_memory.blocks}));
    let invalid_target = format!("/{}", "a".repeat(1024));
    allocation_scope_start();
    assert!(Upgrade::start(io.clone(), &authority, &invalid_target, None, limits).is_err());
    let invalid = allocation_scope_finish();
    assert_eq!(invalid.allocations, 0);
    assert!(Limits::new(0, 1).is_err());
    assert!(Limits::new(MAXIMUM + 1, MAXIMUM).is_err());
    allocation_scope_start();
    let retained = Box::new([7u8; 37]);
    std::hint::black_box(&retained);
    let control = allocation_scope_snapshot();
    assert_eq!((control.live, control.blocks), (37, 1));
    drop(retained);
    let released = allocation_scope_finish();
    assert_eq!((released.live, released.blocks), (0, 0));
    assert!(!released.invalid);
    cases.push(json!({"name":"dependency-allocation-budget-cleanup-admission-before-allocation-and-retained-control","status":"pass"}));
    println!("phase2 evidence: {}", summary.display());
    fs::write(&summary,serde_json::to_vec_pretty(&json!({"status":"pass","cases":cases,"seed":SEED,"memory":memory,"budget":{"bytes":budget,"upgrade":upgrade_budget,"established":open_budget,"rng":rng_layout.size(),"http":http,"request":request,"bucket_stride_bound":bucket,"extra_stride":extra,"bytes_mut_shared":shared,"header_name":name_size,"header_value":value_size,"protocol_stack":std::mem::size_of::<Protocol<Io>>(),"upgrade_stack":std::mem::size_of::<Upgrade<Io>>(),"frame_payload":MAXIMUM,"message_payload":MAXIMUM,"read":READ,"header":HEADER,"wire":w,"diagnostics":diagnostics,"metadata":metadata},"observer_control":{"retained_bytes":control.live,"retained_blocks":control.blocks,"released_bytes":released.live}})).unwrap()).unwrap();
}
