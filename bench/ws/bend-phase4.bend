import Base
import ../../src/ws/main.bend as WS
import ../../src/ws/effects/native.bend as Native
import ../../src/bytes/main.bend as Bytes
import ./fixture-bytes.bend as Fixture

type Settings is Data:
  Settings{port: U32, mode: U32, iterations: U32}
def settings() -> IO(Settings):
  import "./phase4-probe.c"
  import "./phase4-probe.js"
def check(actual: U32, expected: U32, tag: U32) -> IO(Unit):
  import "./phase4-probe.c"
  import "./phase4-probe.js"
def report(s: File, tag: U32, tx: U32, rx: U32) -> IO(File):
  import "./phase4-probe.c"
  import "./phase4-probe.js"
def arm(s: File) -> IO(File):
  import "./phase4-probe.c"
  import "./phase4-probe.js"
def computed(value: U32) -> IO(Unit):
  import "./phase4-probe.c"
  import "./phase4-probe.js"
def checkpoint(tag: U32) -> IO(Unit):
  import "./phase4-probe.c"
  import "./phase4-probe.js"
def notice(s: File, phase: U32, direction: U32, hold: U32, sequence: U32) -> IO(File):
  import "./phase4-probe.c"
  import "./phase4-probe.js"
def await_parked() -> IO(Unit):
  import "./phase4-probe.c"
  import "./phase4-probe.js"
def inject(code: U32) -> IO(Unit):
  import "./phase4-probe.c"
  import "./phase4-probe.js"
def boundary(s: File, tag: U32) -> IO(File):
  import "./phase4-probe.c"
  import "./phase4-probe.js"
def flushed(s: File) -> IO(File):
  import "./phase4-probe.c"
  import "./phase4-probe.js"

@unsafe def byte(+sequence: U32, +direction: U32, +i: U32) -> U32:
  select_byte((i < 9 : U32), sequence, direction, i)
@unsafe def select_byte(small: Bool, +sequence: U32, direction: U32, +i: U32) -> U32:
  match small:
    case True{}:
      metadata(i, sequence, direction)
    case False{}:
      ((sequence * 17 + i * 31 + direction * 7) % 256 : U32)
@unsafe def metadata(+i: U32, sequence: U32, direction: U32) -> U32:
  metadata_zero((i < 5 : U32), i, sequence, direction)
@unsafe def metadata_zero(zero: Bool, +i: U32, sequence: U32, direction: U32) -> U32:
  match zero:
    case True{}:
      metadata_first(U32.is_eq(i, 0), direction)
    case False{}:
      ((sequence >> U32.to_nat(((8 - i) * 8 : U32))) % 256 : U32)
def metadata_first(first: Bool, direction: U32) -> U32:
  match first:
    case True{}:
      direction
    case False{}:
      0
def payload(n: Nat, +sequence: U32, +direction: U32, +i: U32) -> List<U32>:
  match n:
    case 0n:
      []
    case 1n+p:
      byte(sequence, direction, i) <> payload(p, sequence, direction, (i + 1 : U32))
def validate_length(xs: List<U32>, +sequence: U32, +direction: U32, +i: U32, +length: U32) -> U32:
  match xs:
    case Nil{}:
      (i .^. length : U32)
    case Con{x, rest}:
      ((x .^. byte(sequence, direction, i)) .|. validate_length(rest, sequence, direction, (i + 1 : U32), length) : U32)
def validate(xs: List<U32>, sequence: U32, direction: U32, i: U32) -> U32:
  validate_length(xs, sequence, direction, i, 256)
def batch(n: Nat, +sequence: U32) -> List<List<U32>>:
  match n:
    case 0n:
      []
    case 1n+p:
      payload(256n, sequence, 0, 0) <> batch(p, (sequence + 1 : U32))
def validated_message(pair: File & List<U32>, kind: U32, length: U32, sequence: U32) -> WS.Lease & U32:
  (h, bytes) = pair
  (WS.Lease{h}, (kind .|. (length .^. 256) .|. validate(bytes, sequence, 1, 0) : U32))
def validate_message(lease: WS.Lease, m: WS.Message, sequence: U32) -> IO(WS.Lease & U32):
  match lease m:
    case WS.Lease{h} WS.Message{kind, index, length}:
      do IO<WS.Lease & U32>:
        pair : File & List<U32> <- Fixture.lease_list(h, index)
        return validated_message(pair, kind, length, sequence)
@unsafe def validate_batch(ms: List<WS.Message>, lease: WS.Lease, +sequence: U32, +count: U32, +valid: U32) -> IO(WS.Lease & (U32 & U32)):
  match ms:
    case Nil{}:
      IO.pure(WS.Lease & (U32 & U32), (lease, (valid, count)))
    case Con{m, rest}:
      do IO<WS.Lease & (U32 & U32)>:
        pair : WS.Lease & U32 <- validate_message(lease, m, sequence)
        validate_batch_next(pair, rest, sequence, count, valid)
@unsafe def validate_batch_next(pair: WS.Lease & U32, rest: List<WS.Message>, sequence: U32, count: U32, valid: U32) -> IO(WS.Lease & (U32 & U32)):
  (lease, result) = pair
  validate_batch(rest, lease, (sequence + 1 : U32), (count + 1 : U32), (valid .|. result : U32))
def release_validated(pair: WS.Lease & U32, s: WS.Session, sequence: U32) -> IO(WS.Session & U32):
  (lease, valid) = pair
  do IO<WS.Session & U32>:
    check(valid, 0, 10)
    code : U32 <- WS.Lease.release(lease)
    check(code, 0, 11)
    return (s, (sequence + 1 : U32))
def released(s: WS.Session, r: WS.Receive, +sequence: U32) -> IO(WS.Session & U32):
  match r:
    case WS.Payload{lease, message}:
      do IO<WS.Session & U32>:
        pair : WS.Lease & U32 <- validate_message(lease, message, sequence)
        release_validated(pair, s, sequence)
    case WS.Empty{}:
      IO.pure(WS.Session & U32, (s, sequence))
    case _:
      do IO<WS.Session & U32>:
        check(1, 0, 12)
        return (s, sequence)
def received(pair: WS.Session & WS.Receive, sequence: U32) -> IO(WS.Session & U32):
  (s, r) = pair
  released(s, r, sequence)
def batch_checked(pair: U32 & U32, s: WS.Session, lease: WS.Lease, sequence: U32) -> IO(WS.Session & U32):
  (valid, count) = pair
  do IO<WS.Session & U32>:
    check(valid, 0, 13)
    code : U32 <- WS.Lease.release(lease)
    check(code, 0, 14)
    return (s, (sequence + count : U32))
def release_validated_batch(pair: WS.Lease & (U32 & U32), s: WS.Session, sequence: U32) -> IO(WS.Session & U32):
  (lease, valid) = pair
  batch_checked(valid, s, lease, sequence)
def released_batch(s: WS.Session, r: WS.BatchReceive, +sequence: U32) -> IO(WS.Session & U32):
  match r:
    case WS.Messages{lease, messages}:
      do IO<WS.Session & U32>:
        pair : WS.Lease & (U32 & U32) <- validate_batch(messages, lease, sequence, 0, 0)
        release_validated_batch(pair, s, sequence)
    case WS.BatchEmpty{}:
      IO.pure(WS.Session & U32, (s, sequence))
    case _:
      do IO<WS.Session & U32>:
        check(1, 0, 15)
        return (s, sequence)
def received_batch(pair: WS.Session & WS.BatchReceive, sequence: U32) -> IO(WS.Session & U32):
  (s, r) = pair
  released_batch(s, r, sequence)
@unsafe def send_one(s: WS.Session, bytes: List<U32>) -> IO(WS.Session):
  do IO<WS.Session>:
    buffer : Bytes.Buffer <- Fixture.from_list(bytes)
    send_buffer(s, buffer)
@unsafe def send_buffer(s: WS.Session, bytes: Bytes.Buffer) -> IO(WS.Session):
  do IO<WS.Session>:
    pair : WS.Session & WS.Admission <- WS.offer(s, bytes)
    admitted_one(pair)
@unsafe def admitted_one(pair: WS.Session & WS.Admission) -> IO(WS.Session):
  (s, a) = pair
  match a:
    case WS.Accepted{bytes}:
      do IO<WS.Session>:
        code : U32 <- Bytes.release(bytes)
        check(code, 0, 16)
        return s
    case WS.Full{bytes}:
      do IO<WS.Session>:
        IO.sleep(1)
        send_buffer(s, bytes)
    case WS.Rejected{code, bytes}:
      do IO<WS.Session>:
        check(code, 0, 16)
        released : U32 <- Bytes.release(bytes)
        return s
@unsafe def send_batch(s: WS.Session, messages: List<List<U32>>, count: U32) -> IO(WS.Session):
  do IO<WS.Session>:
    buffers : List<Bytes.Buffer> <- Fixture.from_lists(messages)
    send_buffers(s, buffers, count)
@unsafe def send_buffers(s: WS.Session, messages: List<Bytes.Buffer>, count: U32) -> IO(WS.Session):
  do IO<WS.Session>:
    pair : WS.Session & WS.BatchAdmission <- WS.offer_batch(s, messages)
    admitted_batch(pair, count)
@unsafe def admitted_batch(pair: WS.Session & WS.BatchAdmission, +count: U32) -> IO(WS.Session):
  (s, a) = pair
  match a:
    case WS.BatchAccepted{actual, messages}:
      do IO<WS.Session>:
        Fixture.release_all(messages)
        check(actual, count, 17)
        return s
    case WS.BatchFull{messages}:
      do IO<WS.Session>:
        IO.sleep(1)
        send_buffers(s, messages, count)
    case WS.BatchRejected{code, messages}:
      do IO<WS.Session>:
        check(code, 0, 18)
        Fixture.release_all(messages)
        return s
@unsafe def pump(s: WS.Session, c: WS.Closer, +tx: U32, +rx: U32, total: U32, single_cutoff: U32) -> IO(Unit):
  pump_progress(U32.is_eq((tx % 10000 : U32), 0), s, c, tx, rx, total, single_cutoff)
@unsafe def pump_progress(progress: Bool, s: WS.Session, c: WS.Closer, +tx: U32, +rx: U32, +total: U32, single_cutoff: U32) -> IO(Unit):
  match progress:
    case True{}:
      do IO<Unit>:
        s1 : WS.Session <- report_session(s, 9, tx, rx)
        pump_done(U32.is_eq(tx, total) && U32.is_eq(rx, total), s1, c, tx, rx, total, single_cutoff)
    case False{}:
      pump_done(U32.is_eq(tx, total) && U32.is_eq(rx, total), s, c, tx, rx, total, single_cutoff)
@unsafe def pump_done(done: Bool, s: WS.Session, c: WS.Closer, +tx: U32, +rx: U32, +total: U32, +single_cutoff: U32) -> IO(Unit):
  match done:
    case True{}:
      finish(s, c, tx, rx)
    case False{}:
      do IO<Unit>:
        sent : WS.Session & U32 <- pump_send((tx < total : U32), s, tx, total, single_cutoff)
        pump_sent(sent, c, rx, total, single_cutoff)
@unsafe def pump_received(pair: WS.Session & U32, c: WS.Closer, tx: U32, total: U32, single_cutoff: U32) -> IO(Unit):
  (s, rx) = pair
  pump(s, c, tx, rx, total, single_cutoff)
@unsafe def pump_sent(pair: WS.Session & U32, c: WS.Closer, +rx: U32, total: U32, +single_cutoff: U32) -> IO(Unit):
  (s, tx) = pair
  do IO<Unit>:
    recv : WS.Session & U32 <- pump_receive((rx < single_cutoff : U32), s, rx)
    pump_received(recv, c, tx, total, single_cutoff)
@unsafe def pump_send(active: Bool, s: WS.Session, +tx: U32, total: U32, single_cutoff: U32) -> IO(WS.Session & U32):
  match active:
    case False{}:
      IO.pure(WS.Session & U32, (s, tx))
    case True{}:
      pump_send_size((tx < single_cutoff : U32), s, tx, total)
def pump_send_size(single: Bool, s: WS.Session, +tx: U32, total: U32) -> IO(WS.Session & U32):
  match single:
    case True{}:
      do IO<WS.Session & U32>:
        s1 : WS.Session <- send_one(s, payload(256n, tx, 0, 0))
        return (s1, (tx + 1 : U32))
    case False{}:
      do IO<WS.Session & U32>:
        +count : U32 = U32.min(32, (total - tx : U32))
        s1 : WS.Session <- send_batch(s, batch(U32.to_nat(count), tx), count)
        return (s1, (tx + count : U32))
def pump_receive(single: Bool, s: WS.Session, rx: U32) -> IO(WS.Session & U32):
  match single:
    case True{}:
      do IO<WS.Session & U32>:
        pair : WS.Session & WS.Receive <- WS.take(s, 1000)
        received(pair, rx)
    case False{}:
      do IO<WS.Session & U32>:
        pair : WS.Session & WS.BatchReceive <- WS.take_batch(s, WS.BatchLimits{32, 65536}, 1000)
        received_batch(pair, rx)
def report_session(s: WS.Session, tag: U32, tx: U32, rx: U32) -> IO(WS.Session):
  match s:
    case WS.Session{h}:
      do IO<WS.Session>:
        h1 : File <- report(h, tag, tx, rx)
        return WS.Session{h1}
@unsafe def finish(s: WS.Session, c: WS.Closer, tx: U32, rx: U32) -> IO(Unit):
  do IO<Unit>:
    s0 : WS.Session <- flush_session(s)
    s1 : WS.Session <- report_session(s0, 1, tx, rx)
    pair : U32 & U32 <- WS.close(s1, c)
    close_checked(pair)
    checkpoint(1)
def close_checked(pair: U32 & U32) -> IO(Unit):
  (a, b) = pair
  do IO<Unit>:
    check(a, 0, 19)
    check(b, 0, 20)
def flush_session(s: WS.Session) -> IO(WS.Session):
  match s:
    case WS.Session{h}:
      do IO<WS.Session>:
        h1 : File <- flushed(h)
        return WS.Session{h1}
def recurrence(n: Nat, +seed: U32, +value: U32, +index: U32) -> U32:
  match n:
    case 0n:
      value
    case 1n+p:
      recurrence(p, seed, (((value * 1664525 + 1013904223) .^. (seed + index)) : U32), (index + 1 : U32))
def challenge_bytes(xs: List<U32>, +value: U32, +count: U32) -> U32:
  match xs:
    case Nil{}:
      value
    case Con{h, t}:
      challenge_bytes(t, (value * 256 + h : U32), (count + 1 : U32))
def challenge_read(pair: File & List<U32>, s: WS.Session, kind: U32) -> IO(WS.Session & U32):
  (h, bytes) = pair
  do IO<WS.Session & U32>:
    value : U32 = challenge_bytes(bytes, 0, 0)
    code : U32 <- WS.Lease.release(WS.Lease{h})
    check(kind, 0, 21)
    check(code, 0, 22)
    return (s, value)
def challenge(s: WS.Session, r: WS.Receive) -> IO(WS.Session & U32):
  match r:
    case WS.Payload{WS.Lease{h}, WS.Message{kind, index, length}}:
      do IO<WS.Session & U32>:
        pair : File & List<U32> <- Fixture.lease_list(h, index)
        challenge_read(pair, s, kind)
    case _:
      do IO<WS.Session & U32>:
        check(1, 0, 23)
        return (s, 0)
def challenge_pair(pair: WS.Session & WS.Receive) -> IO(WS.Session & U32):
  (s, r) = pair
  challenge(s, r)
def arm_session(s: WS.Session) -> IO(WS.Session):
  match s:
    case WS.Session{h}:
      do IO<WS.Session>:
        h1 : File <- arm(h)
        return WS.Session{h1}
def compute_consumed(pair: WS.Session & U32, c: WS.Closer) -> IO(Unit):
  (s, count) = pair
  do IO<Unit>:
    check(count, 778, 24)
    pair2 : U32 & U32 <- WS.close(s, c)
    close_checked(pair2)
    checkpoint(2)
def compute_input(input: WS.Session & U32, c: WS.Closer, iterations: U32) -> IO(Unit):
  (s0, +seed) = input
  do IO<Unit>:
    +prior : U32 = recurrence(100n, seed, seed, 0)
    s1 : WS.Session <- send_one(s0, payload(65536n, 100, 0, 0))
    s2 : WS.Session <- send_one(s1, payload(65536n, 101, 0, 0))
    s3 : WS.Session <- send_one(s2, payload(256n, prior, 0, 0))
    s4 : WS.Session <- arm_session(s3)
    computed(recurrence(U32.to_nat(iterations), seed, seed, 0))
    s5 : WS.Session <- report_session(s4, 2, prior, seed)
    incoming : WS.Session & WS.Receive <- WS.take(s5, 1000)
    consumed : WS.Session & U32 <- received(incoming, 777)
    compute_consumed(consumed, c)
def compute_case(s: WS.Session, c: WS.Closer, iterations: U32) -> IO(Unit):
  do IO<Unit>:
    pair : WS.Session & WS.Receive <- WS.take(s, 1000)
    input : WS.Session & U32 <- challenge_pair(pair)
    compute_input(input, c, iterations)
@unsafe def connected(r: WS.Start, mode: U32, iterations: U32) -> IO(Unit):
  match r:
    case WS.Started{s, c}:
      choose_mode(U32.is_eq(mode, 0), s, c, iterations)
    case WS.StartFailed{code}:
      do IO<Unit>:
        check(code, startup_expected(iterations), 25)
        checkpoint(9)
@unsafe def startup_expected(iterations: U32) -> U32:
  startup_failure(U32.is_eq(iterations, 20))
def startup_failure(failure: Bool) -> U32:
  match failure:
    case True{}:
      9
    case False{}:
      0
@unsafe def choose_mode(transfer: Bool, s: WS.Session, c: WS.Closer, +iterations: U32) -> IO(Unit):
  match transfer:
    case True{}:
      pump(s, c, 0, 0, iterations, (iterations / 4 : U32))
    case False{}:
      choose_finite((iterations < 1000 : U32), s, c, iterations)
@unsafe def choose_finite(qualification: Bool, s: WS.Session, c: WS.Closer, iterations: U32) -> IO(Unit):
  match qualification:
    case False{}:
      compute_case(s, c, iterations)
    case True{}:
      qualification_case(s, c, iterations)

def notify(s: WS.Session, phase: U32, direction: U32, hold: U32, sequence: U32) -> IO(WS.Session):
  match s:
    case WS.Session{h}:
      do IO<WS.Session>:
        h1 : File <- notice(h, phase, direction, hold, sequence)
        return WS.Session{h1}
def bounded(s: WS.Session, tag: U32) -> IO(WS.Session):
  match s:
    case WS.Session{h}:
      do IO<WS.Session>:
        h1 : File <- boundary(h, tag)
        return WS.Session{h1}
@unsafe def take_n(s: WS.Session, +sequence: U32, n: Nat) -> IO(WS.Session):
  match n:
    case 0n:
      IO.pure(WS.Session, s)
    case 1n+p:
      do IO<WS.Session>:
        pair : WS.Session & WS.Receive <- WS.take(s, 1000)
        input : WS.Session & U32 <- received(pair, sequence)
        take_n_next(input, p)
@unsafe def take_n_next(pair: WS.Session & U32, n: Nat) -> IO(WS.Session):
  (s, sequence) = pair
  take_n(s, sequence, n)
@unsafe def hold_ms(+index: U32) -> U32:
  select_hold(U32.is_eq((index % 3 : U32), 0), index)
@unsafe def select_hold(first: Bool, index: U32) -> U32:
  match first:
    case True{}:
      10
    case False{}:
      last_hold(U32.is_eq((index % 3 : U32), 1))
def last_hold(second: Bool) -> U32:
  match second:
    case True{}:
      100
    case False{}:
      1000
@unsafe def pause_case(s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  do IO<Unit>:
    s0 : WS.Session <- take_n(s, index, 1n)
    paused_receiver(U32.is_eq((index / 3 : U32), 0), s0, c, index, hold_ms(index))
@unsafe def paused_receiver(bend: Bool, s: WS.Session, c: WS.Closer, +index: U32, +hold: U32) -> IO(Unit):
  match bend:
    case True{}:
      do IO<Unit>:
        s0 : WS.Session <- notify(s, 0, 0, hold, index)
        s1 : WS.Session <- send_batch(s0, batch(8n, (10000 + index * 8 : U32)), 8)
        IO.sleep(hold)
        s2 : WS.Session <- notify(s1, 1, 0, hold, index)
        s3 : WS.Session <- take_n(s2, (20000 + index * 8 : U32), 8n)
        s4 : WS.Session <- notify(s3, 2, 0, hold, index)
        finish(s4, c, 8, 9)
    case False{}:
      do IO<Unit>:
        s0 : WS.Session <- pause_filler(U32.is_eq(hold, 1000), s, index)
        s1 : WS.Session <- notify(s0, 0, 1, hold, index)
        s2 : WS.Session <- take_n(s1, (20000 + index * 8 : U32), 8n)
        s3 : WS.Session <- notify(s2, 2, 1, hold, index)
        IO.sleep(hold)
        s4 : WS.Session <- notify(s3, 1, 1, hold, index)
        finish(s4, c, 1, 9)
def pause_filler(idle: Bool, s: WS.Session, index: U32) -> IO(WS.Session):
  match idle:
    case True{}:
      send_one(s, payload(256n, index, 0, 0))
    case False{}:
      send_one(s, payload(1048576n, index, 0, 0))
def terminal_receive(s: WS.Session, r: WS.Receive) -> IO(Unit):
  match r:
    case WS.End{reason}:
      do IO<Unit>:
        code : U32 <- WS.Session.release(s)
        check(code, 0, 30)
    case _:
      check(1, 0, 31)
@unsafe def waiter(s: WS.Session) -> IO(Unit):
  do IO<Unit>:
    pair : WS.Session & WS.Receive <- WS.take(s, 1000)
    waiter_pair(pair)
@unsafe def waiter_pair(pair: WS.Session & WS.Receive) -> IO(Unit):
  (s, r) = pair
  terminal_receive(s, r)
def terminal_batch(s: WS.Session, r: WS.BatchReceive) -> IO(Unit):
  match r:
    case WS.BatchEnd{reason}:
      do IO<Unit>:
        code : U32 <- WS.Session.release(s)
        check(code, 0, 32)
    case _:
      check(1, 0, 33)
@unsafe def batch_waiter(s: WS.Session) -> IO(Unit):
  do IO<Unit>:
    pair : WS.Session & WS.BatchReceive <- WS.take_batch(s, WS.BatchLimits{32, 65536}, 1000)
    batch_waiter_pair(pair)
def batch_waiter_pair(pair: WS.Session & WS.BatchReceive) -> IO(Unit):
  (s, r) = pair
  terminal_batch(s, r)
def waiting(single: Bool, s: WS.Session, c: WS.Closer) -> IO(Unit):
  match single:
    case True{}:
      do IO<Unit>:
        task : Chan(Unit) <- IO.fork(Unit, waiter(s))
        await_parked()
        code : U32 <- WS.stop(c)
        check(code, 0, 34)
        IO.join(Unit, task)
        checkpoint(3)
    case False{}:
      do IO<Unit>:
        task : Chan(Unit) <- IO.fork(Unit, batch_waiter(s))
        await_parked()
        code : U32 <- WS.stop(c)
        check(code, 0, 35)
        IO.join(Unit, task)
        checkpoint(4)
def waiting_ready(s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  do IO<Unit>:
    s0 : WS.Session <- take_n(s, index, 1n)
    waiting(U32.is_eq(index, 6), s0, c)
def large_read(read: File & List<U32>, s: WS.Session, c: WS.Closer, kind: U32, +index: U32, +length: U32) -> IO(Unit):
  (h, bytes) = read
  do IO<Unit>:
    check(validate_length(bytes, index, 1, 0, length), 0, 36)
    check(kind, 0, 37)
    s1 : WS.Session <- send_one(s, payload(U32.to_nat(length), index, 0, 0))
    s2 : WS.Session <- flush_session(s1)
    pair : U32 & U32 <- WS.close(s2, c)
    close_checked(pair)
    code : U32 <- WS.Lease.release(WS.Lease{h})
    check(code, 0, 38)
    checkpoint(5)
def large_receive(s: WS.Session, c: WS.Closer, r: WS.Receive, +index: U32, +length: U32) -> IO(Unit):
  match r:
    case WS.Payload{WS.Lease{h}, WS.Message{kind, message_index, actual_length}}:
      do IO<Unit>:
        read : File & List<U32> <- Fixture.lease_list(h, message_index)
        large_read(read, s, c, kind, index, length)
    case _:
      check(1, 0, 39)
def large_pair(pair: WS.Session & WS.Receive, c: WS.Closer, index: U32, length: U32) -> IO(Unit):
  (s, r) = pair
  large_receive(s, c, r, index, length)
@unsafe def qualification_case(s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  qualification_saturation(U32.is_eq(index, 29), s, c, index)
@unsafe def qualification_saturation(saturation: Bool, s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  match saturation:
    case True{}:
      do IO<Unit>:
        s0 : WS.Session <- take_n(s, 29, 1n)
        s1 : WS.Session <- send_batch(s0, batch(8n, 40000), 8)
        s2 : WS.Session <- flush_session(s1)
        s3 : WS.Session <- bounded(s2, 201)
        saturation_cycles(2n, s3, c, 0)
    case False{}:
      qualification_pause((index < 6 : U32), s, c, index)
@unsafe def saturation_cycles(n: Nat, s: WS.Session, c: WS.Closer, +cycle: U32) -> IO(Unit):
  match n:
    case 0n:
      finish(s, c, 24, 9217)
    case 1n+p:
      do IO<Unit>:
        s0 : WS.Session <- notify(s, 0, 3, 10, cycle)
        s1 : WS.Session <- send_batch(s0, batch(8n, (60000 + cycle * 8 : U32)), 8)
        s2 : WS.Session <- bounded(s1, 200)
        IO.sleep(10)
        s3 : WS.Session <- notify(s2, 1, 3, 10, cycle)
        s4 : WS.Session <- take_n(s3, (50000 + cycle * 4608 : U32), 4608n)
        s5 : WS.Session <- flush_session(s4)
        s6 : WS.Session <- bounded(s5, 201)
        s7 : WS.Session <- notify(s6, 2, 3, 10, cycle)
        saturation_cycles(p, s7, c, (cycle + 1 : U32))
@unsafe def qualification_pause(pause: Bool, s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  match pause:
    case True{}:
      pause_case(s, c, index)
    case False{}:
      qualification_wait((index < 8 : U32), s, c, index)
@unsafe def qualification_wait(wait: Bool, s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  match wait:
    case True{}:
      waiting_ready(s, c, index)
    case False{}:
      qualification_boundary((index < 14 : U32), s, c, index)
@unsafe def qualification_boundary(bulk: Bool, s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  match bulk:
    case True{}:
      do IO<Unit>:
        +length : U32 = boundary_size(index)
        s0 : WS.Session <- bounded(s, index)
        pair : WS.Session & WS.Receive <- WS.take(s0, 1000)
        large_pair(pair, c, index, length)
    case False{}:
      qualification_leases((index < 17 : U32), s, c, index)
@unsafe def qualification_leases(leases: Bool, s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  match leases:
    case True{}:
      do IO<Unit>:
        s0 : WS.Session <- take_n(s, index, 1n)
        held : WS.Session & List<WS.Lease> <- hold_n(U32.to_nat(lease_count(index)), s0, index, 0)
        held_ready(held, c, index)
    case False{}:
      do IO<Unit>:
        s0 : WS.Session <- take_n(s, index, 1n)
        qualification_admission((index >= 22 : U32), s0, c, index)
@unsafe def qualification_admission(full: Bool, s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  match full:
    case True{}:
      fill_until_full(1000n, s, c, index, 0)
    case False{}:
      fault_case(s, c, index)
def corrupt_payload(xs: List<U32>) -> List<U32>:
  match xs:
    case Nil{}:
      []
    case Con{x, rest}:
      (x .^. 1 : U32) <> rest
def validate_text(xs: List<U32>) -> U32:
  match xs:
    case Con{a, Con{b, Con{c, Con{d, Con{e, Nil{}}}}}}:
      ((a .^. 65) .|. b .|. (c .^. 226) .|. (d .^. 130) .|. (e .^. 172) : U32)
    case _:
      1
def mixed_read(read: File & List<U32>, s: WS.Session, kind: U32) -> IO(WS.Session):
  (h, bytes) = read
  do IO<WS.Session>:
    check(kind, 1, 76)
    check(validate_text(bytes), 0, 77)
    code : U32 <- WS.Lease.release(WS.Lease{h})
    check(code, 0, 78)
    return s
def mixed_text(pair: WS.Session & WS.Receive) -> IO(WS.Session):
  (s, r) = pair
  match r:
    case WS.Payload{WS.Lease{h}, WS.Message{kind, index, length}}:
      do IO<WS.Session>:
        read : File & List<U32> <- Fixture.lease_list(h, index)
        mixed_read(read, s, kind)
    case _:
      do IO<WS.Session>:
        check(1, 0, 79)
        return s
@unsafe def mixed_case(s: WS.Session, c: WS.Closer) -> IO(Unit):
  do IO<Unit>:
    pair : WS.Session & WS.Receive <- WS.take(s, 1000)
    s0 : WS.Session <- mixed_text(pair)
    second : WS.Session & WS.BatchReceive <- WS.take_batch(s0, WS.BatchLimits{32, 65536}, 1000)
    consumed : WS.Session & U32 <- received_batch(second, 1)
    mixed_finish(consumed, c)
def mixed_finish(pair: WS.Session & U32, c: WS.Closer) -> IO(Unit):
  (s, count) = pair
  do IO<Unit>:
    check(count, 2, 80)
    closed : U32 & U32 <- WS.close(s, c)
    close_checked(closed)
    checkpoint(103)
@unsafe def integrity_case(s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  integrity_direction((index < 26 : U32), s, c, index)
@unsafe def integrity_direction(outbound: Bool, s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  match outbound:
    case True{}:
      do IO<Unit>:
        s0 : WS.Session <- send_one(s, payload(256n, 0, 0, 0))
        s1 : WS.Session <- send_one(s0, integrity_payload(U32.is_eq(index, 24)))
        s2 : WS.Session <- flush_session(s1)
        closed : U32 & U32 <- WS.close(s2, c)
        close_checked(closed)
        checkpoint(102)
    case False{}:
      do IO<Unit>:
        s0 : WS.Session <- take_n(s, 0, 2n)
        closed : U32 & U32 <- WS.close(s0, c)
        close_checked(closed)
def integrity_payload(duplicate: Bool) -> List<U32>:
  match duplicate:
    case True{}:
      payload(256n, 0, 0, 0)
    case False{}:
      corrupt_payload(payload(256n, 1, 0, 0))
def zeros(n: Nat) -> List<List<U32>>:
  match n:
    case 0n:
      []
    case 1n+p:
      [] <> zeros(p)
def fill_messages(large: Bool, sequence: U32) -> List<List<U32>>:
  match large:
    case True{}:
      [payload(65536n, sequence, 0, 0)]
    case False{}:
      zeros(128n)
@unsafe def fill_count(index: U32) -> U32:
  select_count(U32.is_eq(index, 23))
def select_count(large: Bool) -> U32:
  match large:
    case True{}:
      1
    case False{}:
      128
@unsafe def fill_until_full(n: Nat, s: WS.Session, c: WS.Closer, +index: U32, +sequence: U32) -> IO(Unit):
  match n:
    case 0n:
      check(1, 0, 62)
    case 1n+p:
      do IO<Unit>:
        buffers : List<Bytes.Buffer> <- Fixture.from_lists(fill_messages(U32.is_eq(index, 23), sequence))
        offered : WS.Session & WS.BatchAdmission <- WS.offer_batch(s, buffers)
        fill_result(offered, p, c, index, sequence)
@unsafe def fill_result(pair: WS.Session & WS.BatchAdmission, n: Nat, c: WS.Closer, +index: U32, +sequence: U32) -> IO(Unit):
  (s, admission) = pair
  match admission:
    case WS.BatchAccepted{+count, messages}:
      do IO<Unit>:
        Fixture.release_all(messages)
        check(count, fill_count(index), 63)
        fill_until_full(n, s, c, index, (sequence + count : U32))
    case WS.BatchFull{messages}:
      do IO<Unit>:
        lists : List<List<U32>> <- Fixture.owned_lists(messages)
        check(validate_rejected(lists, sequence, fill_length(index), fill_count(index)), 0, 64)
        s0 : WS.Session <- checked_status(s, sequence)
        s1 : WS.Session <- notify(s0, 0, 2, index, sequence)
        finish(s1, c, sequence, 1)
    case _:
      check(1, 0, 65)
@unsafe def fill_length(index: U32) -> U32:
  select_length(U32.is_eq(index, 23))
def select_length(large: Bool) -> U32:
  match large:
    case True{}:
      65536
    case False{}:
      0
def validate_rejected(ms: List<List<U32>>, +sequence: U32, +length: U32, +count: U32) -> U32:
  match ms:
    case Nil{}:
      count
    case Con{bytes, rest}:
      (validate_length(bytes, sequence, 0, 0, length) .|. validate_rejected(rest, (sequence + 1 : U32), length, (count - 1 : U32)) : U32)
@unsafe def checked_status(s: WS.Session, expected: U32) -> IO(WS.Session):
  do IO<WS.Session>:
    pair : WS.Session & WS.Status <- WS.status(s)
    status_checked(pair, expected)
def status_checked(pair: WS.Session & WS.Status, expected: U32) -> IO(WS.Session):
  (s, status) = pair
  match status:
    case Native.Status{code, state, terminal, Native.Counters{Native.Counter{hi, lo}, sent, received, consumed, failed, discarded}, queues}:
      do IO<WS.Session>:
        check(code, 0, 66)
        check(hi, 0, 67)
        check(lo, expected, 68)
        return s
@unsafe def lease_count(index: U32) -> U32:
  count_large(U32.is_eq(index, 16))
def count_large(large: Bool) -> U32:
  match large:
    case True{}:
      2
    case False{}:
      8
@unsafe def lease_length(+index: U32) -> U32:
  length_zero(U32.is_eq(index, 15), index)
@unsafe def length_zero(zero: Bool, index: U32) -> U32:
  match zero:
    case True{}:
      0
    case False{}:
      length_large(U32.is_eq(index, 16))
def length_large(large: Bool) -> U32:
  match large:
    case True{}:
      1048576
    case False{}:
      256
@unsafe def hold_n(n: Nat, s: WS.Session, +index: U32, +ordinal: U32) -> IO(WS.Session & List<WS.Lease>):
  match n:
    case 0n:
      IO.pure(WS.Session & List<WS.Lease>, (s, []))
    case 1n+p:
      do IO<WS.Session & List<WS.Lease>>:
        pair : WS.Session & WS.Receive <- WS.take(s, 1000)
        hold_received(pair, p, index, ordinal)
@unsafe def hold_received(pair: WS.Session & WS.Receive, n: Nat, +index: U32, +ordinal: U32) -> IO(WS.Session & List<WS.Lease>):
  (s, r) = pair
  match r:
    case WS.Payload{WS.Lease{h}, WS.Message{kind, message_index, length}}:
      do IO<WS.Session & List<WS.Lease>>:
        read : File & List<U32> <- Fixture.lease_list(h, message_index)
        hold_read(read, s, n, kind, index, ordinal)
    case _:
      do IO<WS.Session & List<WS.Lease>>:
        check(1, 0, 42)
        return (s, [])
def hold_join(pair: WS.Session & List<WS.Lease>, lease: WS.Lease) -> WS.Session & List<WS.Lease>:
  (s, leases) = pair
  (s, lease <> leases)
@unsafe def hold_read(read: File & List<U32>, s: WS.Session, n: Nat, kind: U32, +index: U32, +ordinal: U32) -> IO(WS.Session & List<WS.Lease>):
  (h, bytes) = read
  do IO<WS.Session & List<WS.Lease>>:
    check(kind, 0, 40)
    check(validate_length(bytes, (30000 + index * 16 + ordinal : U32), 1, 0, lease_length(index)), 0, 41)
    rest : WS.Session & List<WS.Lease> <- hold_n(n, s, index, (ordinal + 1 : U32))
    return hold_join(rest, WS.Lease{h})
@unsafe def release_all(leases: List<WS.Lease>) -> IO(Unit):
  match leases:
    case Nil{}:
      IO.pure(Unit, Unit{})
    case Con{lease, rest}:
      do IO<Unit>:
        code : U32 <- WS.Lease.release(lease)
        check(code, 0, 43)
        release_all(rest)
@unsafe def held_ready(pair: WS.Session & List<WS.Lease>, c: WS.Closer, +index: U32) -> IO(Unit):
  (s, leases) = pair
  do IO<Unit>:
    s0 : WS.Session <- bounded(s, index)
    failed : WS.Session & WS.Receive <- WS.take(s0, 1000)
    lease_full(failed, leases, c, index)
@unsafe def lease_full(pair: WS.Session & WS.Receive, leases: List<WS.Lease>, c: WS.Closer, +index: U32) -> IO(Unit):
  (s, r) = pair
  match r leases:
    case WS.ReceiveFailed{code} Con{lease, rest}:
      do IO<Unit>:
        check(code, 2, 44)
        s0 : WS.Session <- bounded(s, (index + 100 : U32))
        released : U32 <- WS.Lease.release(lease)
        check(released, 0, 45)
        next : WS.Session & WS.Receive <- WS.take(s0, 1000)
        lease_restored(next, rest, c, index)
    case _ _:
      check(1, 0, 46)
@unsafe def lease_restored(pair: WS.Session & WS.Receive, leases: List<WS.Lease>, c: WS.Closer, +index: U32) -> IO(Unit):
  (s, r) = pair
  match r:
    case WS.Payload{WS.Lease{h}, WS.Message{kind, message_index, length}}:
      do IO<Unit>:
        read : File & List<U32> <- Fixture.lease_list(h, message_index)
        restored_read(read, s, leases, c, kind, index)
    case _:
      check(1, 0, 50)
@unsafe def restored_read(read: File & List<U32>, s: WS.Session, leases: List<WS.Lease>, c: WS.Closer, kind: U32, +index: U32) -> IO(Unit):
  (h, bytes) = read
  do IO<Unit>:
    check(kind, 0, 47)
    check(validate_length(bytes, (30000 + index * 16 + lease_count(index) : U32), 1, 0, restored_length(index)), 0, 48)
    code : U32 <- WS.Lease.release(WS.Lease{h})
    check(code, 0, 49)
    closed : U32 & U32 <- WS.close(s, c)
    close_checked(closed)
    checkpoint(90)
    release_all(leases)
    checkpoint(6)
@unsafe def restored_length(index: U32) -> U32:
  restored_zero(U32.is_eq(index, 15))
def restored_zero(zero: Bool) -> U32:
  match zero:
    case True{}:
      0
    case False{}:
      256
@unsafe def fault_case(s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  fault_worker(U32.is_eq(index, 19), s, c, index)
@unsafe def fault_worker(worker: Bool, s: WS.Session, c: WS.Closer, +index: U32) -> IO(Unit):
  match worker:
    case True{}:
      do IO<Unit>:
        inject(3)
        code : U32 <- WS.stop(c)
        check(code, 0, 51)
        pair : WS.Session & WS.Receive <- WS.take(s, 1000)
        panic_end(pair)
        checkpoint(7)
    case False{}:
      do IO<Unit>:
        inject(admission_fault(index))
        buffer : Bytes.Buffer <- Fixture.from_list(payload(256n, index, 0, 0))
        pair : WS.Session & WS.Admission <- WS.offer(s, buffer)
        panic_admission(pair, c, index)
@unsafe def admission_fault(index: U32) -> U32:
  select_fault(U32.is_eq(index, 17))
def select_fault(post: Bool) -> U32:
  match post:
    case True{}:
      2
    case False{}:
      1
@unsafe def panic_admission(pair: WS.Session & WS.Admission, c: WS.Closer, +index: U32) -> IO(Unit):
  (s, admission) = pair
  match admission:
    case WS.Accepted{bytes}:
      do IO<Unit>:
        released : U32 <- Bytes.release(bytes)
        check(index, 17, 52)
        code : U32 <- WS.stop(c)
        check(code, 0, 53)
        s0 : WS.Session <- bounded(s, 117)
        retired : U32 <- WS.Session.release(s0)
        check(retired, 0, 54)
        checkpoint(8)
    case WS.Rejected{code, bytes}:
      do IO<Unit>:
        check(index, 18, 55)
        check(code, 10, 56)
        list : List<U32> <- Fixture.owned_list(bytes)
        check(validate(list, index, 0, 0), 0, 57)
        s0 : WS.Session <- send_one(s, payload(256n, index, 0, 0))
        finish(s0, c, 1, 1)
    case _:
      check(1, 0, 58)
def panic_end(pair: WS.Session & WS.Receive) -> IO(Unit):
  (s, r) = pair
  match r:
    case WS.End{reason}:
      do IO<Unit>:
        check(reason, 23, 59)
        code : U32 <- WS.Session.release(s)
        check(code, 0, 60)
    case _:
      check(1, 0, 61)
@unsafe def boundary_size(+index: U32) -> U32:
  boundary_0(U32.is_eq(index, 8), index)
@unsafe def boundary_0(zero: Bool, +index: U32) -> U32:
  match zero:
    case True{}:
      0
    case False{}:
      boundary_1(U32.is_eq(index, 9), index)
@unsafe def boundary_1(small: Bool, +index: U32) -> U32:
  match small:
    case True{}:
      125
    case False{}:
      boundary_2(U32.is_eq(index, 10), index)
@unsafe def boundary_2(small: Bool, +index: U32) -> U32:
  match small:
    case True{}:
      126
    case False{}:
      boundary_3(U32.is_eq(index, 11), index)
@unsafe def boundary_3(small: Bool, +index: U32) -> U32:
  match small:
    case True{}:
      65535
    case False{}:
      boundary_4(U32.is_eq(index, 12))
def boundary_4(small: Bool) -> U32:
  match small:
    case True{}:
      65536
    case False{}:
      1048576
@unsafe def configured(config: Settings) -> IO(Unit):
  match config:
    case Settings{port, +mode, iterations}:
      do IO<Unit>:
        checkpoint(0)
        configured_integrity(U32.is_eq(mode, 4), port, mode, iterations)
@unsafe def configured_integrity(integrity: Bool, +port: U32, +mode: U32, iterations: U32) -> IO(Unit):
  match integrity:
    case True{}:
      do IO<Unit>:
        r : WS.Start <- WS.connect(WS.Endpoint{"127.0.0.1", port, "/", ""})
        integrity_connected(r, iterations)
    case False{}:
      configured_mode(U32.is_eq(mode, 3), port, mode, iterations)
@unsafe def integrity_connected(r: WS.Start, +index: U32) -> IO(Unit):
  match r:
    case WS.Started{s, c}:
      integrity_mixed(U32.is_eq(index, 28), s, c, index)
    case _:
      check(1, 0, 75)
@unsafe def integrity_mixed(mixed: Bool, s: WS.Session, c: WS.Closer, index: U32) -> IO(Unit):
  match mixed:
    case True{}:
      mixed_case(s, c)
    case False{}:
      integrity_case(s, c, index)
@unsafe def lifecycle_received(pair: WS.Session & WS.Receive, c: WS.Closer, +port: U32, n: Nat) -> IO(Unit):
  (s, r) = pair
  match r:
    case WS.Payload{WS.Lease{h}, WS.Message{kind, index, length}}:
      do IO<Unit>:
        read : File & List<U32> <- Fixture.lease_list(h, index)
        lifecycle_read(read, s, c, kind, port, n)
    case _:
      check(1, 0, 73)
@unsafe def lifecycle_read(read: File & List<U32>, s: WS.Session, c: WS.Closer, kind: U32, +port: U32, n: Nat) -> IO(Unit):
  (h, bytes) = read
  do IO<Unit>:
    check(kind, 0, 70)
    check(validate(bytes, 42, 1, 0), 0, 71)
    s1 : WS.Session <- send_one(s, payload(256n, 42, 0, 0))
    s2 : WS.Session <- flush_session(s1)
    closed : U32 & U32 <- WS.close(s2, c)
    close_checked(closed)
    copied : File & Bytes.Allocation <- Bytes.copy_message(h, 0)
    lifecycle_copied(copied, port, n)
@unsafe def lifecycle_copied(pair: File & Bytes.Allocation, port: U32, n: Nat) -> IO(Unit):
  (h, allocation) = pair
  match allocation:
    case Bytes.Allocated{words, length}:
      do IO<Unit>:
        bytes : List<U32> <- Fixture.owned_list(Bytes.ByteBuffer{words, length})
        check(validate(bytes, 42, 1, 0), 0, 71)
        code : U32 <- WS.Lease.release(WS.Lease{h})
        check(code, 0, 72)
        checkpoint(100)
        lifecycle(n, port)
    case Bytes.AllocationFailed{code}:
      check(code, 0, 72)
@unsafe def lifecycle_connected(r: WS.Start, port: U32, n: Nat) -> IO(Unit):
  match r:
    case WS.Started{s, c}:
      do IO<Unit>:
        pair : WS.Session & WS.Receive <- WS.take(s, 1000)
        lifecycle_received(pair, c, port, n)
    case _:
      check(1, 0, 74)
@unsafe def lifecycle(n: Nat, +port: U32) -> IO(Unit):
  match n:
    case 0n:
      checkpoint(101)
    case 1n+p:
      do IO<Unit>:
        r : WS.Start <- WS.connect(WS.Endpoint{"127.0.0.1", port, "/", ""})
        lifecycle_connected(r, port, p)
@unsafe def configured_mode(cycles: Bool, +port: U32, +mode: U32, iterations: U32) -> IO(Unit):
  match cycles:
    case True{}:
      lifecycle(U32.to_nat(iterations), port)
    case False{}:
      configured_adverse(U32.is_eq(mode, 5), port, mode, iterations)
@unsafe def adverse_connected(r: WS.Start) -> IO(Unit):
  match r:
    case WS.Started{s, c}:
      do IO<Unit>:
        pair : WS.Session & WS.Receive <- WS.take(s, 1000)
        large_pair(pair, c, 30, 256)
    case _:
      check(1, 0, 82)
@unsafe def configured_adverse(adverse: Bool, +port: U32, mode: U32, iterations: U32) -> IO(Unit):
  match adverse:
    case True{}:
      do IO<Unit>:
        task : Chan(WS.Start) <- IO.fork(WS.Start, WS.connect(WS.Endpoint{"127.0.0.1", port, "/", ""}))
        IO.sleep(50)
        check(0, 0, 81)
        r : WS.Start <- IO.join(WS.Start, task)
        adverse_connected(r)
    case False{}:
      do IO<Unit>:
        r : WS.Start <- WS.connect(WS.Endpoint{"127.0.0.1", port, "/", ""})
        connected(r, mode, iterations)
def main() -> IO(Unit):
  do IO<Unit>:
    config : Settings <- settings()
    configured(config)
