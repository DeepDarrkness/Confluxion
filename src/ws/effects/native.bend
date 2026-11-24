import Base

type Start is Type:
  Started{session: File, closer: File}
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
  Payload{lease: File, message: Message}
  ReceiveFailed{code: U32}
type BatchReceive is Type:
  BatchEmpty{}
  BatchEnd{reason: U32}
  Messages{lease: File, messages: List<Message>}
  BatchReceiveFailed{code: U32}
type Counter is Data:
  Counter{hi: U32, lo: U32}
type Counters is Data:
  Counters{admitted: Counter, sent: Counter, received: Counter, consumed: Counter, failed: Counter, discarded: Counter}
type Queues is Data:
  Queues{tx_bytes: U32, tx_count: U32, rx_bytes: U32, rx_count: U32, lease_bytes: U32, leases: U32}
type Status is Data:
  Status{code: U32, state: U32, terminal: U32, counters: Counters, queues: Queues}

def connect(host: String, port: U32, target: String, protocol: String) -> IO(Start):
  import "./native.c"
  import "./native.js"
def offer(session: File, bytes: List<U32>) -> IO(File & Admission):
  import "./native.c"
  import "./native.js"
def offer_batch(session: File, messages: List<List<U32>>) -> IO(File & BatchAdmission):
  import "./native.c"
  import "./native.js"
def take(session: File, wait_ms: U32) -> IO(File & Receive):
  import "./native.c"
  import "./native.js"
def take_batch(session: File, count: U32, bytes: U32, wait_ms: U32) -> IO(File & BatchReceive):
  import "./native.c"
  import "./native.js"
def status(session: File) -> IO(File & Status):
  import "./native.c"
  import "./native.js"
def release(lease: File) -> IO(U32):
  import "./native.c"
  import "./native.js"
def stop(closer: File) -> IO(U32):
  import "./native.c"
  import "./native.js"
def retire(session: File) -> IO(U32):
  import "./native.c"
  import "./native.js"
