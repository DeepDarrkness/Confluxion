import Base
import ../../src/ws/main.bend as WS
import ../../src/ws/effects/native.bend as Native

def check(actual: U32, expected: U32, tag: U32) -> IO(Unit):
  import "./phase1_probe.c"
  import "./phase1_probe.js"

def checkpoint(tag: U32) -> IO(Unit):
  import "./phase1_probe.c"
  import "./phase1_probe.js"

def observe(bytes: List<U32>, tag: U32) -> IO(List<U32>):
  import "./phase1_probe.c"
  import "./phase1_probe.js"

def fault(n: U32) -> IO(Unit):
  import "./phase1_probe.c"
  import "./phase1_probe.js"

def pause(s: File, n: U32) -> IO(File):
  import "./phase1_probe.c"
  import "./phase1_probe.js"

def raw_cases() -> IO(Unit):
  import "./phase1_probe.c"
  import "./phase1_probe.js"

def parked() -> IO(Unit):
  import "./phase1_probe.c"
  import "./phase1_probe.js"

def held() -> IO(Unit):
  import "./phase1_probe.c"
  import "./phase1_probe.js"

def equal(xs: List<U32>, ys: List<U32>) -> U32:
  match xs ys:
    case Nil{} Nil{}:
      0
    case Con{x, xt} Con{y, yt}:
      ((x .^. y) .|. equal(xt, yt) : U32)
    case _ _:
      1

def payload(n: Nat, +x: U32) -> List<U32>:
  match n:
    case 0n:
      []
    case 1n+p:
      x <> payload(p, ((x + 73) % 256 : U32))

def checked_close(pair: U32 & U32) -> IO(Unit):
  (a, b) = pair
  do IO<Unit>:
    check(a, 0, 1)
    check(b, 0, 2)

def close(s: WS.Session, c: WS.Closer) -> IO(Unit):
  do IO<Unit>:
    pair : U32 & U32 <- WS.close(s, c)
    checked_close(pair)

def received(s: WS.Session, r: WS.Receive, expected: List<U32>) -> IO(WS.Session):
  match r:
    case WS.Payload{lease, bytes}:
      do IO<WS.Session>:
        check(equal(bytes, expected), 0, 3)
        code : U32 <- WS.Lease.release(lease)
        check(code, 0, 4)
        return s
    case _:
      do IO<WS.Session>:
        check(1, 0, 5)
        return s

def received_pair(pair: WS.Session & WS.Receive, expected: List<U32>) -> IO(WS.Session):
  (s, r) = pair
  received(s, r, expected)

def admitted(s: WS.Session, a: WS.Admission, expected: List<U32>) -> IO(WS.Session):
  match a:
    case Native.Accepted{}:
      do IO<WS.Session>:
        pair : WS.Session & WS.Receive <- WS.take(s, 1000)
        received_pair(pair, expected)
    case _:
      do IO<WS.Session>:
        check(1, 0, 6)
        return s

def admitted_pair(pair: WS.Session & WS.Admission, expected: List<U32>) -> IO(WS.Session):
  (s, a) = pair
  admitted(s, a, expected)

def transfer(s: WS.Session, bytes: List<U32>, expected: List<U32>) -> IO(WS.Session):
  do IO<WS.Session>:
    pair : WS.Session & WS.Admission <- WS.offer(s, bytes)
    admitted_pair(pair, expected)

def empty(s: WS.Session, r: WS.Receive) -> IO(WS.Session):
  match r:
    case WS.Empty{}:
      IO.pure(WS.Session, s)
    case _:
      do IO<WS.Session>:
        check(1, 0, 7)
        return s

def empty_pair(pair: WS.Session & WS.Receive) -> IO(WS.Session):
  (s, r) = pair
  empty(s, r)

def cycle_started(r: WS.Start) -> IO(Unit):
  match r:
    case WS.Started{s, c}:
      do IO<Unit>:
        pair : WS.Session & WS.Receive <- WS.take(s, 0)
        s1 : WS.Session <- empty_pair(pair)
        s2 : WS.Session <- transfer(s1, [], [])
        s3 : WS.Session <- transfer(s2, [0, 255, 128, 0], [0, 255, 128, 0])
        s4 : WS.Session <- transfer(s3, payload(256n, 0), payload(256n, 0))
        close(s4, c)
    case WS.StartFailed{code}:
      check(code, 0, 8)

def cycle() -> IO(Unit):
  do IO<Unit>:
    r : WS.Start <- WS.start()
    cycle_started(r)

def pause_session(s: WS.Session) -> IO(WS.Session):
  match s:
    case WS.Session{h}:
      do IO<WS.Session>:
        h1 : File <- pause(h, 1)
        return WS.Session{h1}

def accepted_only(pair: WS.Session & WS.Admission) -> IO(WS.Session):
  (s, a) = pair
  match a:
    case Native.Accepted{}:
      IO.pure(WS.Session, s)
    case _:
      do IO<WS.Session>:
        check(1, 0, 17)
        return s

def fill(s: WS.Session, bytes: List<U32>) -> IO(WS.Session):
  do IO<WS.Session>:
    pair : WS.Session & WS.Admission <- WS.offer(s, bytes)
    accepted_only(pair)

def rejected(pair: WS.Session & WS.Admission, expected: List<U32>, code: U32) -> IO(WS.Session):
  (s, a) = pair
  match a:
    case Native.Full{bytes}:
      do IO<WS.Session>:
        check(code, 2, 18)
        check(equal(bytes, expected), 0, 19)
        return s
    case Native.Rejected{actual, bytes}:
      do IO<WS.Session>:
        check(actual, code, 20)
        check(equal(bytes, expected), 0, 21)
        return s
    case Native.Accepted{}:
      do IO<WS.Session>:
        check(0, code, 22)
        return s

def reject(s: WS.Session, bytes: List<U32>, expected: List<U32>, code: U32) -> IO(WS.Session):
  do IO<WS.Session>:
    pair : WS.Session & WS.Admission <- WS.offer(s, bytes)
    rejected(pair, expected, code)

def bounded_started(r: WS.Start) -> IO(Unit):
  match r:
    case WS.Started{s, c}:
      do IO<Unit>:
        s0 : WS.Session <- pause_session(s)
        s1 : WS.Session <- reject(s0, [256], [256], 4)
        s2 : WS.Session <- reject(s1, payload(257n, 0), payload(257n, 0), 4)
        s3 : WS.Session <- fill(s2, [])
        s4 : WS.Session <- fill(s3, [])
        s5 : WS.Session <- fill(s4, [])
        s6 : WS.Session <- fill(s5, [])
        s7 : WS.Session <- reject(s6, [0, 255], [0, 255], 2)
        close(s7, c)
    case WS.StartFailed{code}:
      check(code, 0, 23)

def held_receive(s: WS.Session, c: WS.Closer, r: WS.Receive) -> IO(Unit):
  match r:
    case WS.Payload{lease, bytes}:
      do IO<Unit>:
        close(s, c)
        held()
        check(equal(bytes, [0, 255, 128, 0]), 0, 24)
        code : U32 <- WS.Lease.release(lease)
        check(code, 0, 25)
    case _:
      do IO<Unit>:
        close(s, c)
        check(1, 0, 26)

def held_pair(pair: WS.Session & WS.Receive, c: WS.Closer) -> IO(Unit):
  (s, r) = pair
  held_receive(s, c, r)

def held_started(r: WS.Start) -> IO(Unit):
  match r:
    case WS.Started{s, c}:
      do IO<Unit>:
        s1 : WS.Session <- fill(s, [0, 255, 128, 0])
        pair : WS.Session & WS.Receive <- WS.take(s1, 1000)
        held_pair(pair, c)
    case WS.StartFailed{code}:
      check(code, 0, 27)

def panic_started(r: WS.Start) -> IO(Unit):
  match r:
    case WS.Started{s, c}:
      do IO<Unit>:
        fault(21)
        s1 : WS.Session <- reject(s, [0, 255, 128, 0], [0, 255, 128, 0], 10)
        close(s1, c)
    case WS.StartFailed{code}:
      check(code, 0, 28)

def failed_response(s: WS.Session, c: WS.Closer, r: WS.Receive) -> IO(Unit):
  match r:
    case WS.ReceiveFailed{code}:
      do IO<Unit>:
        close(s, c)
        check(code, 10, 29)
    case _:
      do IO<Unit>:
        close(s, c)
        check(1, 0, 30)

def failed_pair(pair: WS.Session & WS.Receive, c: WS.Closer) -> IO(Unit):
  (s, r) = pair
  failed_response(s, c, r)

def worker_panic_started(r: WS.Start) -> IO(Unit):
  match r:
    case WS.Started{s, c}:
      do IO<Unit>:
        fault(20)
        pair : WS.Session & WS.Receive <- WS.take(s, 1000)
        failed_pair(pair, c)
    case WS.StartFailed{code}:
      check(code, 0, 31)

def startup_failed(r: WS.Start) -> IO(Unit):
  match r:
    case WS.StartFailed{code}:
      check(code, 9, 9)
    case WS.Started{s, c}:
      do IO<Unit>:
        close(s, c)
        check(0, 9, 10)

def startup(+n: U32) -> IO(Unit):
  do IO<Unit>:
    fault(n)
    r : WS.Start <- WS.start()
    startup_failed(r)
    checkpoint((2000 + n : U32))

def terminal(s: WS.Session, r: WS.Receive) -> IO(Unit):
  match r:
    case WS.End{}:
      do IO<Unit>:
        code : U32 <- WS.Session.release(s)
        check(code, 0, 11)
    case _:
      do IO<Unit>:
        code : U32 <- WS.Session.release(s)
        check(code, 0, 12)
        check(1, 0, 13)

def terminal_pair(pair: WS.Session & WS.Receive) -> IO(Unit):
  (s, r) = pair
  terminal(s, r)

def waiter(s: WS.Session) -> IO(Unit):
  do IO<Unit>:
    pair : WS.Session & WS.Receive <- WS.take(s, 1000)
    terminal_pair(pair)

def waiting_started(r: WS.Start) -> IO(Unit):
  match r:
    case WS.Started{s, c}:
      do IO<Unit>:
        chan : Chan(Unit) <- IO.fork(Unit, waiter(s))
        parked()
        code : U32 <- WS.stop(c)
        check(code, 0, 14)
        IO.join(Unit, chan)
    case WS.StartFailed{code}:
      check(code, 0, 15)

def maybe_cycle(skip: Bool) -> IO(Unit):
  match skip:
    case True{}:
      IO.pure(Unit, Unit{})
    case False{}:
      cycle()

@unsafe def cycles(done: Bool, +n: U32) -> IO(Unit):
  match done:
    case True{}:
      checkpoint(3000)
    case False{}:
      do IO<Unit>:
        maybe_cycle(U32.is_eq(n, 1001))
        checkpoint(n)
        cycles(U32.is_eq(n, 1), (n - 1 : U32))

def main() -> IO(Unit):
  do IO<Unit>:
    cycle()
    raw_cases()
    checkpoint(0)
    xs : List<U32> <- observe(payload(64n, 7), 1)
    xs1 : List<U32> <- observe(xs, 2)
    check(equal(xs1, payload(64n, 7)), 0, 16)
    checkpoint(4000)
    startup(1)
    startup(2)
    startup(3)
    startup(4)
    startup(5)
    r : WS.Start <- WS.start()
    waiting_started(r)
    checkpoint(2006)
    bounded : WS.Start <- WS.start()
    bounded_started(bounded)
    checkpoint(2007)
    leased : WS.Start <- WS.start()
    held_started(leased)
    checkpoint(2008)
    panicking : WS.Start <- WS.start()
    panic_started(panicking)
    checkpoint(2009)
    worker_panicking : WS.Start <- WS.start()
    worker_panic_started(worker_panicking)
    checkpoint(2010)
    cycles(False{}, 1001)
