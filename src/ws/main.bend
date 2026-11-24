import Base
import ./effects/native.bend as Native

type Session is Type:
  Session{handle: File}
type Closer is Type:
  Closer{handle: File}
type Lease is Type:
  Lease{handle: File}
type Endpoint is Type:
  Endpoint{host: String, port: U32, target: String, protocol: String}
type BatchLimits is Data:
  BatchLimits{messages: U32, bytes: U32}
type Start is Type:
  Started{session: Session, closer: Closer}
  StartFailed{code: U32}
type Admission is Type:
  Accepted{}
  Full{bytes: List<U32>}
  Rejected{code: U32, bytes: List<U32>}
type BatchAdmission is Type:
  BatchAccepted{count: U32}
  BatchFull{messages: List<List<U32>>}
  BatchRejected{code: U32, messages: List<List<U32>>}
type Message is Type:
  Message{kind: U32, bytes: List<U32>}
type Receive is Type:
  Empty{}
  End{reason: U32}
  Payload{lease: Lease, message: Message}
  ReceiveFailed{code: U32}
type BatchReceive is Type:
  BatchEmpty{}
  BatchEnd{reason: U32}
  Messages{lease: Lease, messages: List<Message>}
  BatchReceiveFailed{code: U32}
def Status() -> Data:
  Native.Status
def wrap_start(r: Native.Start) -> Start:
  match r:
    case Native.Started{s, c}:
      Started{Session{s}, Closer{c}}
    case Native.StartFailed{code}:
      StartFailed{code}
def connect(endpoint: Endpoint) -> IO(Start):
  match endpoint:
    case Endpoint{host, port, target, protocol}:
      do IO<Start>:
        r : Native.Start <- Native.connect(host, port, target, protocol)
        return wrap_start(r)
def wrap_admission(r: Native.Admission) -> Admission:
  match r:
    case Native.Accepted{}:
      Accepted{}
    case Native.Full{bytes}:
      Full{bytes}
    case Native.Rejected{code, bytes}:
      Rejected{code, bytes}
def wrap_offer(pair: File & Native.Admission) -> Session & Admission:
  (h, a) = pair
  (Session{h}, wrap_admission(a))
def offer(s: Session, bytes: List<U32>) -> IO(Session & Admission):
  match s:
    case Session{h}:
      do IO<Session & Admission>:
        pair : File & Native.Admission <- Native.offer(h, bytes)
        return wrap_offer(pair)
def wrap_batch_admission(r: Native.BatchAdmission) -> BatchAdmission:
  match r:
    case Native.BatchAccepted{count}:
      BatchAccepted{count}
    case Native.BatchFull{messages}:
      BatchFull{messages}
    case Native.BatchRejected{code, messages}:
      BatchRejected{code, messages}
def wrap_batch_offer(pair: File & Native.BatchAdmission) -> Session & BatchAdmission:
  (h, a) = pair
  (Session{h}, wrap_batch_admission(a))
def offer_batch(s: Session, messages: List<List<U32>>) -> IO(Session & BatchAdmission):
  match s:
    case Session{h}:
      do IO<Session & BatchAdmission>:
        pair : File & Native.BatchAdmission <- Native.offer_batch(h, messages)
        return wrap_batch_offer(pair)
def wrap_message(m: Native.Message) -> Message:
  match m:
    case Native.Message{kind, bytes}:
      Message{kind, bytes}
def wrap_messages(ms: List<Native.Message>) -> List<Message>:
  match ms:
    case Nil{}:
      []
    case Con{m, rest}:
      wrap_message(m) <> wrap_messages(rest)
def wrap_receive(r: Native.Receive) -> Receive:
  match r:
    case Native.Empty{}:
      Empty{}
    case Native.End{reason}:
      End{reason}
    case Native.Payload{h, message}:
      Payload{Lease{h}, wrap_message(message)}
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
def wrap_batch_receive(r: Native.BatchReceive) -> BatchReceive:
  match r:
    case Native.BatchEmpty{}:
      BatchEmpty{}
    case Native.BatchEnd{reason}:
      BatchEnd{reason}
    case Native.Messages{h, messages}:
      Messages{Lease{h}, wrap_messages(messages)}
    case Native.BatchReceiveFailed{code}:
      BatchReceiveFailed{code}
def wrap_batch_take(pair: File & Native.BatchReceive) -> Session & BatchReceive:
  (h, r) = pair
  (Session{h}, wrap_batch_receive(r))
def take_batch(s: Session, limits: BatchLimits, wait_ms: U32) -> IO(Session & BatchReceive):
  match s limits:
    case Session{h} BatchLimits{count, bytes}:
      do IO<Session & BatchReceive>:
        pair : File & Native.BatchReceive <- Native.take_batch(h, count, bytes, wait_ms)
        return wrap_batch_take(pair)
def wrap_status(pair: File & Native.Status) -> Session & Status:
  (h, r) = pair
  (Session{h}, r)
def status(s: Session) -> IO(Session & Status):
  match s:
    case Session{h}:
      do IO<Session & Status>:
        pair : File & Native.Status <- Native.status(h)
        return wrap_status(pair)
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
