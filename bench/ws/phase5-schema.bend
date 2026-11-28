import Base
import ../../src/bytes/main.bend as Bytes
import ../../src/bytes/types.bend as Wire

type Header is Data:
  Header{version: Wire.Word64, token: Wire.Word64, epoch: Wire.Word64, direction: Wire.Word64, connection: Wire.Word64, sequence: Wire.Word64, due: Wire.Word64}
type Frame is Type:
  Frame{header: Header, body: Bytes.Buffer}
