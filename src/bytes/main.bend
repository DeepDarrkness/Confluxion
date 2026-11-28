import Base

type Buffer is Type:
  ByteBuffer{words: Array<U32>, length: U32}
type Allocation is Type:
  Allocated{words: Array<U32>, length: U32}
  AllocationFailed{code: U32}

def allocate(capacity: U32) -> IO(Allocation):
  import "./native.c"
  import "./native.js"
def checked(allocation: Allocation) -> IO(Buffer):
  match allocation:
    case Allocated{words, length}:
      IO.pure(Buffer, ByteBuffer{words, length})
    case AllocationFailed{code}:
      IO.die(Buffer, code, "Byte buffer allocation failed")
def new(capacity: U32) -> IO(Buffer):
  do IO<Buffer>:
    allocation : Allocation <- allocate(capacity)
    checked(allocation)
def release(buffer: Buffer) -> IO(U32):
  import "./native.c"
  import "./native.js"
def copy_message(lease: File, index: U32) -> IO(File & Allocation):
  import "./native.c"
  import "./native.js"
