import Base
import ../../src/ws/main.bend as WS

type Recipe is Type:
  Recipe{header: List<U32>, seed: U32, length: U32}
type Step is Type:
  ScheduledDue{recipe: Recipe}
  ScheduleWait{}
  ScheduleDone{}
def settings() -> IO(U32):
  import "./phase5-probe.c"
  import "./phase5-probe.js"
def ready(h: File) -> IO(File):
  import "./phase5-probe.c"
  import "./phase5-probe.js"
def next(h: File) -> IO(File & Step):
  import "./phase5-probe.c"
  import "./phase5-probe.js"
def admitted(accepted: U32) -> IO(Unit):
  import "./phase5-probe.c"
  import "./phase5-probe.js"
def expect(bytes: List<U32>) -> IO(List<U32> & Recipe):
  import "./phase5-probe.c"
  import "./phase5-probe.js"
def consumed(valid: U32) -> IO(Unit):
  import "./phase5-probe.c"
  import "./phase5-probe.js"
def receive_enabled() -> IO(U32):
  import "./phase5-probe.c"
  import "./phase5-probe.js"
def ended(code: U32) -> IO(Unit):
  import "./phase5-probe.c"
  import "./phase5-probe.js"
def closing(h: File) -> IO(File):
  import "./phase5-probe.c"
  import "./phase5-probe.js"
def finished() -> IO(U32):
  import "./phase5-probe.c"
  import "./phase5-probe.js"
def body(n: Nat, +seed: U32, +i: U32) -> List<U32>:
  match n:
    case 0n:
      []
    case 1n+p:
      ((seed + i * 31) % 256 : U32) <> body(p, seed, (i + 1 : U32))
def append(xs: List<U32>, tail: List<U32>) -> List<U32>:
  match xs:
    case Nil{}:
      tail
    case Con{x, rest}:
      x <> append(rest, tail)
def build(recipe: Recipe) -> List<U32>:
  match recipe:
    case Recipe{header, seed, length}:
      append(header, body(U32.to_nat((length - 56 : U32)), seed, 56))
def equal(xs: List<U32>, ys: List<U32>) -> U32:
  match xs ys:
    case Nil{} Nil{}:
      0
    case Con{x, xt} Con{y, yt}:
      ((x .^. y) .|. equal(xt, yt) : U32)
    case _ _:
      1
def checked(pair: List<U32> & Recipe, kind: U32) -> IO(Unit):
  (bytes, recipe) = pair
  consumed((kind .|. equal(bytes, build(recipe)) : U32))
def released(s: WS.Session, r: WS.Receive) -> IO(WS.Session):
  match r:
    case WS.Payload{lease, WS.Message{kind, bytes}}:
      do IO<WS.Session>:
        pair : List<U32> & Recipe <- expect(bytes)
        checked(pair, kind)
        code : U32 <- WS.Lease.release(lease)
        ended(code)
        return s
    case WS.Empty{}:
      IO.pure(WS.Session, s)
    case WS.End{reason}:
      do IO<WS.Session>:
        ended(reason)
        return s
    case WS.ReceiveFailed{code}:
      do IO<WS.Session>:
        ended(code)
        return s
def received(pair: WS.Session & WS.Receive) -> IO(WS.Session):
  (s, r) = pair
  released(s, r)
def receive(active: Bool, s: WS.Session) -> IO(WS.Session):
  match active:
    case True{}:
      do IO<WS.Session>:
        pair : WS.Session & WS.Receive <- WS.take(s, 0)
        received(pair)
    case False{}:
      IO.pure(WS.Session, s)
def offered(pair: WS.Session & WS.Admission) -> IO(WS.Session):
  (s, a) = pair
  match a:
    case WS.Accepted{}:
      do IO<WS.Session>:
        admitted(1)
        return s
    case WS.Full{bytes}:
      do IO<WS.Session>:
        admitted(0)
        return s
    case WS.Rejected{code, bytes}:
      do IO<WS.Session>:
        admitted(0)
        ended(code)
        return s
@unsafe def pump(s: WS.Session, c: WS.Closer) -> IO(Unit):
  match s:
    case WS.Session{h}:
      do IO<Unit>:
        pair : File & Step <- next(h)
        step(pair, c)
@unsafe def continue(s: WS.Session, c: WS.Closer) -> IO(Unit):
  do IO<Unit>:
    enabled : U32 <- receive_enabled()
    s1 : WS.Session <- receive(U32.is_eq(enabled, 1), s)
    pump(s1, c)
@unsafe def sent(pair: WS.Session & WS.Admission, c: WS.Closer) -> IO(Unit):
  do IO<Unit>:
    s : WS.Session <- offered(pair)
    continue(s, c)
@unsafe def step(pair: File & Step, c: WS.Closer) -> IO(Unit):
  (h, action) = pair
  match action:
    case ScheduledDue{recipe}:
      do IO<Unit>:
        offered_pair : WS.Session & WS.Admission <- WS.offer(WS.Session{h}, build(recipe))
        sent(offered_pair, c)
    case ScheduleWait{}:
      continue(WS.Session{h}, c)
    case ScheduleDone{}:
      do IO<Unit>:
        h1 : File <- closing(h)
        codes : U32 & U32 <- WS.close(WS.Session{h1}, c)
        close_checked(codes)
        more : U32 <- finished()
        again(more)
def close_checked(pair: U32 & U32) -> IO(Unit):
  (a, b) = pair
  do IO<Unit>:
    ended(a)
    ended(b)
@unsafe def connected(r: WS.Start) -> IO(Unit):
  match r:
    case WS.Started{WS.Session{h}, c}:
      do IO<Unit>:
        h1 : File <- ready(h)
        pump(WS.Session{h1}, c)
    case WS.StartFailed{code}:
      ended(code)
@unsafe def again(more: U32) -> IO(Unit):
  match more:
    case 0:
      IO.pure(Unit, Unit{})
    case _:
      main()
@unsafe def main() -> IO(Unit):
  do IO<Unit>:
    port : U32 <- settings()
    r : WS.Start <- WS.connect(WS.Endpoint{"127.0.0.1", port, "/", ""})
    connected(r)
