import Base
import ../../src/ws/main.bend as WS
import ../../src/bytes/main.bend as Bytes
import ../../src/bytes/types.bend as Wire
import ./phase5-schema.bend as Schema
import ./phase5-codec/codec.bend as Codec

type Recipe is Data:
  Recipe{vhi: U32, vlo: U32, thi: U32, tlo: U32, ehi: U32, elo: U32, dhi: U32, dlo: U32, chi: U32, clo: U32, shi: U32, slo: U32, uhi: U32, ulo: U32, seed: U32, length: U32}
type Step is Type:
  ScheduledDue{recipe: Recipe}
  ScheduleWait{}
  ScheduleDone{}
type Expected is Type:
  Expected{lease: File, recipe: Recipe}
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
def expect(lease: File, index: U32) -> IO(Expected):
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
def header(recipe: Recipe) -> Schema.Header:
  match recipe:
    case Recipe{vhi, vlo, thi, tlo, ehi, elo, dhi, dlo, chi, clo, shi, slo, uhi, ulo, _, _}:
      Schema.Header{Wire.U64{vhi, vlo}, Wire.U64{thi, tlo}, Wire.U64{ehi, elo}, Wire.U64{dhi, dlo}, Wire.U64{chi, clo}, Wire.U64{shi, slo}, Wire.U64{uhi, ulo}}
def byte(+seed: U32, i: U32) -> U32:
  ((seed + i * 31) % 256 : U32)
def word(+seed: U32, +i: U32) -> U32:
  (byte(seed, i) .|. (byte(seed, (i + 1 : U32)) << 8n) .|. (byte(seed, (i + 2 : U32)) << 16n) .|. (byte(seed, (i + 3 : U32)) << 24n) : U32)
def mask_full(full: Bool, length: U32) -> U32:
  match full:
    case True{}:
      4294967295
    case False{}:
      ((1 << U32.to_nat((length * 8 : U32))) - 1 : U32)
def mask(+length: U32) -> U32:
  mask_full((length >= 4 : U32), length)
@unsafe def depth(+words: U32, +capacity: U32, n: Nat) -> Nat:
  depth_done((capacity >= words : U32), words, capacity, n)
@unsafe def depth_done(done: Bool, words: U32, capacity: U32, n: Nat) -> Nat:
  match done:
    case True{}:
      n
    case False{}:
      depth(words, (capacity * 2 : U32), 1n+n)
def fill(n: Nat, words: Array<U32>, +seed: U32, +i: U32, +length: U32) -> Array<U32>:
  match n:
    case 0n:
      words
    case 1n+p:
      words[i] <- (word(seed, (56 + i * 4 : U32)) .&. mask((length - i * 4 : U32)) : U32)
      fill(p, words, seed, (i + 1 : U32), length)
@unsafe def build(+recipe: Recipe) -> Schema.Frame:
  match recipe:
    case Recipe{_, _, _, _, _, _, _, _, _, _, _, _, _, _, +seed, +length}:
      +body_length = (length - 56 : U32)
      +count = ((body_length + 3) / 4 : U32)
      words = [0 : U32^depth(count, 1, 0n)]
      Schema.Frame{header(recipe), Bytes.ByteBuffer{fill(U32.to_nat(count), words, seed, 0, body_length), body_length}}
def header_equal(a: Schema.Header, b: Schema.Header) -> U32:
  match a b:
    case Schema.Header{Wire.U64{a0, a1}, Wire.U64{a2, a3}, Wire.U64{a4, a5}, Wire.U64{a6, a7}, Wire.U64{a8, a9}, Wire.U64{a10, a11}, Wire.U64{a12, a13}} Schema.Header{Wire.U64{b0, b1}, Wire.U64{b2, b3}, Wire.U64{b4, b5}, Wire.U64{b6, b7}, Wire.U64{b8, b9}, Wire.U64{b10, b11}, Wire.U64{b12, b13}}:
      ((a0 .^. b0) .|. (a1 .^. b1) .|. (a2 .^. b2) .|. (a3 .^. b3) .|. (a4 .^. b4) .|. (a5 .^. b5) .|. (a6 .^. b6) .|. (a7 .^. b7) .|. (a8 .^. b8) .|. (a9 .^. b9) .|. (a10 .^. b10) .|. (a11 .^. b11) .|. (a12 .^. b12) .|. (a13 .^. b13) : U32)
@unsafe def validate_words(n: Nat, words: Array<U32>, +seed: U32, +i: U32, +length: U32, +valid: U32) -> Array<U32> & U32:
  match n:
    case 0n:
      (words, valid)
    case 1n+p:
      validate_word(words[i], p, seed, i, length, valid)
@unsafe def validate_word(pair: Array<U32> & U32, n: Nat, +seed: U32, +i: U32, +length: U32, valid: U32) -> Array<U32> & U32:
  (words, actual) = pair
  validate_words(n, words, seed, (i + 1 : U32), length, (valid .|. ((actual .^. word(seed, (56 + i * 4 : U32))) .&. mask((length - i * 4 : U32))) : U32))
def validated(pair: Array<U32> & U32, actual_length: U32) -> IO(Unit):
  (words, valid) = pair
  do IO<Unit>:
    code : U32 <- Bytes.release(Bytes.ByteBuffer{words, actual_length})
    consumed((valid .|. code : U32))
def validate_frame(frame: Schema.Frame, +recipe: Recipe, kind: U32) -> IO(Unit):
  match frame recipe:
    case Schema.Frame{actual_header, Bytes.ByteBuffer{words, +actual_length}} Recipe{_, _, _, _, _, _, _, _, _, _, _, _, _, _, seed, length}:
      do IO<Unit>:
        pair : Array<U32> & U32 = validate_words(U32.to_nat(((actual_length + 3) / 4 : U32)), words, seed, 0, actual_length, (kind .|. (actual_length .^. (length - 56)) .|. header_equal(actual_header, header(recipe)) : U32))
        validated(pair, actual_length)
def decoded(pair: File & Codec.Decode_Frame, recipe: Recipe, kind: U32) -> IO(File):
  (lease, result) = pair
  match result:
    case Codec.Decoded_Frame{frame}:
      do IO<File>:
        validate_frame(frame, recipe, kind)
        return lease
    case Codec.DecodeFailed_Frame{code}:
      do IO<File>:
        consumed(1)
        ended(code)
        return lease
def expected(result: Expected, +index: U32, kind: U32) -> IO(File):
  match result:
    case Expected{lease, recipe}:
      do IO<File>:
        pair : File & Codec.Decode_Frame <- Codec.decode_lease_Frame(lease, index)
        decoded(pair, recipe, kind)
def released(s: WS.Session, r: WS.Receive) -> IO(WS.Session):
  match r:
    case WS.Payload{WS.Lease{lease}, WS.Message{kind, +index, length}}:
      do IO<WS.Session>:
        result : Expected <- expect(lease, index)
        lease1 : File <- expected(result, index, kind)
        code : U32 <- WS.Lease.release(WS.Lease{lease1})
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
def offered(pair: WS.Session & WS.Admission) -> IO(WS.Session & Bytes.Buffer):
  (s, a) = pair
  match a:
    case WS.Accepted{bytes}:
      do IO<WS.Session & Bytes.Buffer>:
        admitted(1)
        return (s, bytes)
    case WS.Full{bytes}:
      do IO<WS.Session & Bytes.Buffer>:
        admitted(0)
        return (s, bytes)
    case WS.Rejected{code, bytes}:
      do IO<WS.Session & Bytes.Buffer>:
        admitted(0)
        ended(code)
        return (s, bytes)
@unsafe def pump(s: WS.Session, c: WS.Closer, buffer: Bytes.Buffer) -> IO(Unit):
  match s:
    case WS.Session{h}:
      do IO<Unit>:
        pair : File & Step <- next(h)
        step(pair, c, buffer)
@unsafe def continue(s: WS.Session, c: WS.Closer, buffer: Bytes.Buffer) -> IO(Unit):
  do IO<Unit>:
    enabled : U32 <- receive_enabled()
    s1 : WS.Session <- receive(U32.is_eq(enabled, 1), s)
    pump(s1, c, buffer)
@unsafe def sent(pair: WS.Session & WS.Admission, c: WS.Closer) -> IO(Unit):
  do IO<Unit>:
    result : WS.Session & Bytes.Buffer <- offered(pair)
    sent_result(result, c)
@unsafe def sent_result(result: WS.Session & Bytes.Buffer, c: WS.Closer) -> IO(Unit):
  (s, buffer) = result
  continue(s, c, buffer)
@unsafe def encoded(pair: Bytes.Buffer & U32, h: File, c: WS.Closer) -> IO(Unit):
  (buffer, code) = pair
  match code:
    case 0:
      do IO<Unit>:
        offered_pair : WS.Session & WS.Admission <- WS.offer(WS.Session{h}, buffer)
        sent(offered_pair, c)
    case _:
      do IO<Unit>:
        admitted(0)
        ended(code)
        continue(WS.Session{h}, c, buffer)
@unsafe def step(pair: File & Step, c: WS.Closer, buffer: Bytes.Buffer) -> IO(Unit):
  (h, action) = pair
  match action:
    case ScheduledDue{recipe}:
      do IO<Unit>:
        encoded_pair : Bytes.Buffer & U32 <- Codec.encode_Frame(buffer, build(recipe))
        encoded(encoded_pair, h, c)
    case ScheduleWait{}:
      continue(WS.Session{h}, c, buffer)
    case ScheduleDone{}:
      do IO<Unit>:
        released : U32 <- Bytes.release(buffer)
        ended(released)
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
        buffer : Bytes.Buffer <- Bytes.new(16384)
        pump(WS.Session{h1}, c, buffer)
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
