import Base
import ../../src/ws/main.bend as WS

def duplicate(s: WS.Session) -> WS.Session & WS.Session:
  (s, s)
