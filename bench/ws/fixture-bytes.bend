import Base
import ../../src/bytes/main.bend as Bytes

type Packed is Type:
  Packed{words: Array<U32>, length: U32}
type ReadLease is Type:
  ReadLease{lease: File, bytes: List<U32>}
def pack(bytes: List<U32>) -> IO(Packed):
  import "./fixture-bytes.c"
  import "./fixture-bytes.js"
def unpack(packed: Packed) -> Bytes.Buffer:
  match packed:
    case Packed{words, length}:
      Bytes.ByteBuffer{words, length}
def from_list(bytes: List<U32>) -> IO(Bytes.Buffer):
  do IO<Bytes.Buffer>:
    packed : Packed <- pack(bytes)
    return unpack(packed)
def read(lease: File, index: U32) -> IO(ReadLease):
  import "./fixture-bytes.c"
  import "./fixture-bytes.js"
def read_result(result: ReadLease) -> File & List<U32>:
  match result:
    case ReadLease{lease, bytes}:
      (lease, bytes)
def lease_list(lease: File, index: U32) -> IO(File & List<U32>):
  do IO<File & List<U32>>:
    result : ReadLease <- read(lease, index)
    return read_result(result)
def owned_list(buffer: Bytes.Buffer) -> IO(List<U32>):
  import "./fixture-bytes.c"
  import "./fixture-bytes.js"
def from_lists(messages: List<List<U32>>) -> IO(List<Bytes.Buffer>):
  match messages:
    case Nil{}:
      IO.pure(List<Bytes.Buffer>, [])
    case Con{bytes, rest}:
      do IO<List<Bytes.Buffer>>:
        buffer : Bytes.Buffer <- from_list(bytes)
        buffers : List<Bytes.Buffer> <- from_lists(rest)
        return buffer <> buffers
def owned_lists(messages: List<Bytes.Buffer>) -> IO(List<List<U32>>):
  match messages:
    case Nil{}:
      IO.pure(List<List<U32>>, [])
    case Con{bytes, rest}:
      do IO<List<List<U32>>>:
        list : List<U32> <- owned_list(bytes)
        lists : List<List<U32>> <- owned_lists(rest)
        return list <> lists
def release_all(messages: List<Bytes.Buffer>) -> IO(Unit):
  match messages:
    case Nil{}:
      IO.pure(Unit, Unit{})
    case Con{bytes, rest}:
      do IO<Unit>:
        code : U32 <- Bytes.release(bytes)
        release_all(rest)
