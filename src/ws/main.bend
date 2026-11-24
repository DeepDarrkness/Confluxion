import Base
import ./effects/native.bend as Native

type Session is Type:
  Session{handle: File}
type Closer is Type:
  Closer{handle: File}
type Lease is Type:
  Lease{handle: File}
type Start is Type:
  Started{session: Session, closer: Closer}
  StartFailed{code: U32}
type Receive is Type:
  Empty{}
  End{}
  Payload{lease: Lease, bytes: List<U32>}
  ReceiveFailed{code: U32}

def Admission() -> Type:
  Native.Admission

def wrap_start(r: Native.Start) -> Start:
  match r:
    case Native.Started{s, c}:
      Started{Session{s}, Closer{c}}
    case Native.StartFailed{code}:
      StartFailed{code}

def start() -> IO(Start):
  do IO<Start>:
    r : Native.Start <- Native.start()
    return wrap_start(r)

def wrap_offer(pair: File & Native.Admission) -> Session & Admission:
  (h, a) = pair
  (Session{h}, a)

def offer(s: Session, bytes: List<U32>) -> IO(Session & Admission):
  match s:
    case Session{h}:
      do IO<Session & Admission>:
        pair : File & Native.Admission <- Native.offer(h, bytes)
        return wrap_offer(pair)

def wrap_receive(r: Native.Receive) -> Receive:
  match r:
    case Native.Empty{}:
      Empty{}
    case Native.End{}:
      End{}
    case Native.Payload{h, bytes}:
      Payload{Lease{h}, bytes}
    case Native.ReceiveFailed{code}:
      ReceiveFailed{code}

def wrap_take(pair: File & Native.Receive) -> Session & Receive:
  (h, r) = pair
  (Session{h}, wrap_receive(r))

def take(s: Session, wait_ms: U32) -> IO(Session & Receive):
  match s:
    case Session{h}:
      do IO<Session & Receive>:
        pair : File & Native.Receive <- Native.take(h, wait_ms)
        return wrap_take(pair)

def Lease.release(l: Lease) -> IO(U32):
  match l:
    case Lease{h}:
      Native.release(h)

def stop(c: Closer) -> IO(U32):
  match c:
    case Closer{h}:
      Native.stop(h)

def Session.release(s: Session) -> IO(U32):
  match s:
    case Session{h}:
      Native.retire(h)

def close(s: Session, c: Closer) -> IO(U32 & U32):
  do IO<U32 & U32>:
    a : U32 <- stop(c)
    b : U32 <- Session.release(s)
    return (a, b)
