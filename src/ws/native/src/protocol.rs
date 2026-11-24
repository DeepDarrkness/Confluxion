use std::io::{self, Read, Write};
use tungstenite::{
    client::IntoClientRequest,
    handshake::{client::ClientHandshake, HandshakeError, MidHandshake},
    protocol::{
        frame::{coding::CloseCode, CloseFrame},
        WebSocketConfig,
    },
    Message, WebSocket,
};

pub const HEADER: usize = 8192;
pub const READ: usize = 4096;
pub const MAXIMUM: usize = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Input,
    Upgrade,
    HeaderLimit,
    Protocol,
    Limit,
    Io(io::ErrorKind),
    Terminal,
}
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    frame: usize,
    message: usize,
}
impl Limits {
    pub fn new(frame: usize, message: usize) -> Result<Self, Error> {
        if frame == 0 || message == 0 || frame > MAXIMUM || message > MAXIMUM {
            return Err(Error::Input);
        }
        Ok(Self { frame, message })
    }
    fn config(self) -> WebSocketConfig {
        WebSocketConfig::default()
            .read_buffer_size(READ)
            .write_buffer_size(0)
            .max_write_buffer_size(self.frame + 14 + 2 * 139)
            .max_frame_size(Some(self.frame))
            .max_message_size(Some(self.message))
            .accept_unmasked_frames(false)
    }
}

pub struct Upgrade<S: Read + Write> {
    mid: MidHandshake<ClientHandshake<BoundedIo<S>>>,
    limits: Limits,
    protocol: Option<String>,
}
pub enum UpgradeStep<S: Read + Write> {
    Pending(Upgrade<S>),
    Ready(Protocol<S>),
}
pub struct Protocol<S> {
    socket: WebSocket<BoundedIo<S>>,
    limits: Limits,
    delivered: Option<Message>,
    pending: bool,
    terminal: bool,
    closing: bool,
}
pub enum Outbound<'a> {
    Binary(&'a [u8]),
    Text(&'a str),
}
#[derive(Debug, PartialEq, Eq)]
pub enum Rejection {
    Busy,
    Oversize,
    Closing,
}
#[derive(Debug, PartialEq, Eq)]
pub enum Admission {
    Accepted,
    Rejected(Rejection),
}
#[derive(Debug, PartialEq, Eq)]
pub enum FlushStep {
    Pending,
    Drained,
}
#[derive(Debug, PartialEq, Eq)]
pub enum ReadStep<'a> {
    Pending,
    Closed,
    Binary(&'a [u8]),
    Text(&'a str),
    Ping(&'a [u8]),
    Pong(&'a [u8]),
    Close(Option<(u16, &'a str)>),
}

impl<S: Read + Write> Upgrade<S> {
    pub fn start(
        io: S,
        authority: &str,
        target: &str,
        protocol: Option<&str>,
        limits: Limits,
    ) -> Result<Self, Error> {
        Self::build(io, authority, target, protocol, limits, None)
    }
    #[doc(hidden)]
    pub fn fixture(io: S, protocol: Option<&str>, limits: Limits) -> Result<Self, Error> {
        Self::build(
            io,
            "127.0.0.1",
            "/",
            protocol,
            limits,
            Some("dGhlIHNhbXBsZSBub25jZQ=="),
        )
    }
    fn build(
        io: S,
        authority: &str,
        target: &str,
        protocol: Option<&str>,
        limits: Limits,
        key: Option<&str>,
    ) -> Result<Self, Error> {
        if authority.is_empty()
            || authority.len() > 128
            || !authority
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".:-".contains(&b))
            || !target.starts_with('/')
            || target.len() > 1024
            || !target.bytes().all(|b| (33..127).contains(&b) && b != b'#')
            || protocol.is_some_and(|p| {
                p.is_empty()
                    || p.len() > 64
                    || !p
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
            })
        {
            return Err(Error::Input);
        }
        let mut request = format!("ws://{authority}{target}")
            .into_client_request()
            .map_err(|_| Error::Input)?;
        if let Some(k) = key {
            request
                .headers_mut()
                .insert("sec-websocket-key", k.parse().unwrap());
        }
        if let Some(p) = protocol {
            request.headers_mut().insert(
                "sec-websocket-protocol",
                p.parse().map_err(|_| Error::Input)?,
            );
        }
        let mid = ClientHandshake::start(BoundedIo::new(io), request, Some(limits.config()))
            .map_err(map_error)?;
        Ok(Self {
            mid,
            limits,
            protocol: protocol.map(str::to_owned),
        })
    }
    pub fn resume(self) -> Result<UpgradeStep<S>, Error> {
        match self.mid.handshake() {
            Err(HandshakeError::Interrupted(mid)) => Ok(UpgradeStep::Pending(Self { mid, ..self })),
            Err(HandshakeError::Failure(e)) => Err(map_error(e)),
            Ok((mut socket, response)) => {
                let h = response.headers();
                if h.contains_key("sec-websocket-extensions")
                    || [
                        "upgrade",
                        "connection",
                        "sec-websocket-accept",
                        "sec-websocket-protocol",
                    ]
                    .iter()
                    .any(|k| h.get_all(*k).iter().count() > 1)
                    || h.get("sec-websocket-protocol").map(|v| v.as_bytes())
                        != self.protocol.as_deref().map(str::as_bytes)
                {
                    return Err(Error::Upgrade);
                }
                socket.get_mut().established = true;
                Ok(UpgradeStep::Ready(Protocol {
                    socket,
                    limits: self.limits,
                    delivered: None,
                    pending: false,
                    terminal: false,
                    closing: false,
                }))
            }
        }
    }
}

fn map_error(e: tungstenite::Error) -> Error {
    match e {
        tungstenite::Error::Io(e) => match e.kind() {
            io::ErrorKind::InvalidData => Error::Protocol,
            io::ErrorKind::FileTooLarge => Error::HeaderLimit,
            kind => Error::Io(kind),
        },
        tungstenite::Error::Capacity(_) => Error::Limit,
        tungstenite::Error::Http(_) | tungstenite::Error::HttpFormat(_) => Error::Upgrade,
        _ => Error::Protocol,
    }
}
impl<S: Read + Write> Protocol<S> {
    pub fn offer(&mut self, data: Outbound<'_>) -> Result<Admission, Error> {
        if self.terminal {
            return Err(Error::Terminal);
        }
        if self.closing || !self.socket.can_write() {
            return Ok(Admission::Rejected(Rejection::Closing));
        }
        let len = match data {
            Outbound::Binary(p) => p.len(),
            Outbound::Text(p) => p.len(),
        };
        if len > self.limits.frame.min(self.limits.message) {
            return Ok(Admission::Rejected(Rejection::Oversize));
        }
        if self.pending {
            return Ok(Admission::Rejected(Rejection::Busy));
        }
        let message = match data {
            Outbound::Binary(p) => Message::Binary(p.to_vec().into()),
            Outbound::Text(p) => Message::Text(p.to_owned().into()),
        };
        match self.socket.write(message) {
            Ok(()) => {}
            Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(tungstenite::Error::WriteBufferFull(_)) => {
                return Ok(Admission::Rejected(Rejection::Busy))
            }
            Err(e) => {
                self.terminal = true;
                return Err(map_error(e));
            }
        }
        self.pending = true;
        Ok(Admission::Accepted)
    }
    pub fn flush(&mut self) -> Result<FlushStep, Error> {
        if self.terminal {
            return Err(Error::Terminal);
        }
        // Tungstenite can defer additional_send while draining the existing wire buffer.
        for _ in 0..2 {
            match self.socket.flush() {
                Ok(()) => {}
                Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(FlushStep::Pending)
                }
                Err(e) => {
                    self.terminal = true;
                    return Err(map_error(e));
                }
            }
        }
        self.pending = false;
        Ok(FlushStep::Drained)
    }
    pub fn read(&mut self) -> Result<ReadStep<'_>, Error> {
        if self.terminal {
            return Err(Error::Terminal);
        }
        self.delivered = None;
        self.pending = true;
        match self.socket.read() {
            Ok(m) => self.delivered = Some(m),
            Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(ReadStep::Pending)
            }
            Err(tungstenite::Error::ConnectionClosed) => {
                self.terminal = true;
                return Ok(ReadStep::Closed);
            }
            Err(e) => {
                self.terminal = true;
                return Err(map_error(e));
            }
        }
        Ok(match self.delivered.as_ref().unwrap() {
            Message::Binary(p) => ReadStep::Binary(p),
            Message::Text(p) => ReadStep::Text(p.as_str()),
            Message::Ping(p) => ReadStep::Ping(p),
            Message::Pong(p) => ReadStep::Pong(p),
            Message::Close(p) => {
                self.closing = true;
                ReadStep::Close(p.as_ref().map(|c| (u16::from(c.code), c.reason.as_str())))
            }
            Message::Frame(_) => unreachable!(),
        })
    }
    pub fn close(&mut self, value: Option<(u16, &str)>) -> Result<FlushStep, Error> {
        if self.terminal {
            return Err(Error::Terminal);
        }
        if self.closing {
            return self.flush();
        }
        if value
            .is_some_and(|(code, reason)| !CloseCode::from(code).is_allowed() || reason.len() > 123)
        {
            return Err(Error::Input);
        }
        if self.flush()? == FlushStep::Pending {
            return Ok(FlushStep::Pending);
        }
        self.closing = true;
        let close = value.map(|(code, reason)| CloseFrame {
            code: code.into(),
            reason: reason.to_owned().into(),
        });
        match self.socket.close(close) {
            Ok(()) => {}
            Err(tungstenite::Error::Io(e)) if e.kind() == io::ErrorKind::WouldBlock => {
                return Ok(FlushStep::Pending)
            }
            Err(e) => {
                self.terminal = true;
                return Err(map_error(e));
            }
        }
        self.flush()
    }
}

struct HeaderGuard {
    header: [u8; 14],
    used: usize,
    need: usize,
    remaining: u64,
}
impl HeaderGuard {
    fn new() -> Self {
        Self {
            header: [0; 14],
            used: 0,
            need: 2,
            remaining: 0,
        }
    }
    fn inspect(&mut self, bytes: &[u8]) -> io::Result<()> {
        for &b in bytes {
            if self.remaining != 0 {
                self.remaining -= 1;
                continue;
            }
            self.header[self.used] = b;
            self.used += 1;
            if self.used == 2 {
                self.need = match b & 127 {
                    126 => 4,
                    127 => 10,
                    _ => 2,
                } + if b & 128 != 0 { 4 } else { 0 };
            }
            if self.used == self.need {
                let short = self.header[1] & 127;
                let n = match short {
                    126 => u16::from_be_bytes(self.header[2..4].try_into().unwrap()) as u64,
                    127 => u64::from_be_bytes(self.header[2..10].try_into().unwrap()),
                    n => n as u64,
                };
                // The pinned FrameHeader parser omits RFC 6455 canonical-length/high-bit checks.
                if (short == 126 && n < 126) || (short == 127 && (n < 65536 || n >> 63 != 0)) {
                    return Err(io::ErrorKind::InvalidData.into());
                }
                self.remaining = n;
                self.used = 0;
                self.need = 2;
            }
        }
        Ok(())
    }
}
struct BoundedIo<S> {
    inner: S,
    stage: Option<Box<[u8]>>,
    filled: usize,
    cursor: usize,
    end: Option<usize>,
    established: bool,
    guard: HeaderGuard,
}
impl<S> BoundedIo<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            stage: Some(vec![0; HEADER + READ].into_boxed_slice()),
            filled: 0,
            cursor: 0,
            end: None,
            established: false,
            guard: HeaderGuard::new(),
        }
    }
}
impl<S: Read> Read for BoundedIo<S> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.end.is_none() {
            loop {
                let old = self.filled;
                let stage = self.stage.as_mut().unwrap();
                let n = self.inner.read(&mut stage[old..old + READ])?;
                if n == 0 {
                    return Ok(0);
                }
                self.filled += n;
                if let Some(i) = stage[old.saturating_sub(3)..self.filled]
                    .windows(4)
                    .position(|s| s == b"\r\n\r\n")
                {
                    let end = old.saturating_sub(3) + i + 4;
                    if end > HEADER {
                        return Err(io::ErrorKind::FileTooLarge.into());
                    }
                    self.guard.inspect(&stage[end..self.filled])?;
                    self.end = Some(end);
                    break;
                }
                if self.filled >= HEADER {
                    return Err(io::ErrorKind::FileTooLarge.into());
                }
            }
        }
        if self.cursor < self.filled {
            let n = out.len().min(self.filled - self.cursor);
            out[..n].copy_from_slice(&self.stage.as_ref().unwrap()[self.cursor..self.cursor + n]);
            self.cursor += n;
            if self.cursor == self.filled {
                self.stage = None;
            }
            return Ok(n);
        }
        let count = out.len().min(READ);
        let n = self.inner.read(&mut out[..count])?;
        if self.established {
            self.guard.inspect(&out[..n])?;
        }
        Ok(n)
    }
}
impl<S: Write> Write for BoundedIo<S> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self.inner.write(bytes)? {
            0 if !bytes.is_empty() => Err(io::ErrorKind::WriteZero.into()),
            n => Ok(n),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
