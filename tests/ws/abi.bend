import Base

type Session is Type:
  Session{handle: File}

def open() -> IO(File):
  import "./abi.c"
  import "./abi.js"

def consume(handle: File) -> IO(U32):
  import "./abi.c"
  import "./abi.js"

def finish(s: Session) -> IO(Unit):
  match s:
    case Session{handle}:
      do IO<Unit>:
        code : U32 <- consume(handle)
        IO.print(U32.show(code))

def main() -> IO(Unit):
  do IO<Unit>:
    handle : File <- open()
    finish(Session{handle})
