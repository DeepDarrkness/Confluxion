import Base

type Start is Type:
  Started{session: File, closer: File}
  StartFailed{code: U32}

type Admission is Type:
  Accepted{}
  Full{bytes: List<U32>}
  Rejected{code: U32, bytes: List<U32>}

type Receive is Type:
  Empty{}
  End{}
  Payload{lease: File, bytes: List<U32>}
  ReceiveFailed{code: U32}

def start() -> IO(Start):
  import "./loopback.c"
  import "./loopback.js"

def offer(session: File, bytes: List<U32>) -> IO(File & Admission):
  import "./loopback.c"
  import "./loopback.js"

def take(session: File, wait_ms: U32) -> IO(File & Receive):
  import "./loopback.c"
  import "./loopback.js"

def release(lease: File) -> IO(U32):
  import "./loopback.c"
  import "./loopback.js"

def stop(closer: File) -> IO(U32):
  import "./loopback.c"
  import "./loopback.js"

def retire(session: File) -> IO(U32):
  import "./loopback.c"
  import "./loopback.js"
