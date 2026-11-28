use crate::schema::{Format, Model, Shape, MAX_BYTES, MAX_DEPTH, MAX_NODES};

#[derive(Clone, Debug, PartialEq)]
pub enum Value { U32(u32), Bool(bool), String(String), Words(Vec<u32>), Blob(Vec<u8>), Word64(u64), Product { variant: usize, fields: Vec<Value> } }
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error { Shape, Bound, Truncated, Tag, Utf8, Trailing, ArrayCount }
struct Cursor { bytes: Vec<u8>, at: usize, nodes: usize, format: Format }
impl Cursor {
    fn node(&mut self, depth: usize) -> Result<(), Error> { self.nodes += 1; if depth > MAX_DEPTH || self.nodes > MAX_NODES { Err(Error::Bound) } else { Ok(()) } }
    fn put(&mut self, bytes: &[u8]) -> Result<(), Error> { if self.bytes.len() + bytes.len() > MAX_BYTES { return Err(Error::Bound); } self.bytes.extend_from_slice(bytes); Ok(()) }
    fn get(&mut self, n: usize) -> Result<&[u8], Error> { let end = self.at.checked_add(n).ok_or(Error::Bound)?; let out = self.bytes.get(self.at..end).ok_or(Error::Truncated)?; self.at = end; Ok(out) }
    fn put32(&mut self, n: u32) -> Result<(), Error> { self.put(&if self.format == Format::Bsatn { n.to_le_bytes() } else { n.to_be_bytes() }) }
    fn get32(&mut self) -> Result<u32, Error> { let be = self.format == Format::RawBe; let x = self.get(4)?.try_into().unwrap(); Ok(if be { u32::from_be_bytes(x) } else { u32::from_le_bytes(x) }) }
}
pub fn encode(model: &Model, root: usize, value: &Value) -> Result<Vec<u8>, Error> {
    let mut c = Cursor { bytes: Vec::new(), at: 0, nodes: 0, format: model.format };
    put(model, &Shape::Ref(root), value, &mut c, 0)?; Ok(c.bytes)
}
fn put(m: &Model, shape: &Shape, value: &Value, c: &mut Cursor, depth: usize) -> Result<(), Error> {
    c.node(depth)?;
    match (shape, value) {
        (Shape::U32, Value::U32(n)) => c.put32(*n),
        (Shape::Bool, Value::Bool(b)) => c.put(&[*b as u8]),
        (Shape::Word64 | Shape::Signed64, Value::Word64(n)) => c.put(&if c.format == Format::Bsatn { n.to_le_bytes() } else { n.to_be_bytes() }),
        (Shape::String, Value::String(s)) => { c.put32(u32::try_from(s.len()).map_err(|_| Error::Bound)?)?; c.put(s.as_bytes()) },
        (Shape::Blob, Value::Blob(b)) => { if c.format == Format::Bsatn { c.put32(u32::try_from(b.len()).map_err(|_| Error::Bound)?)?; } c.put(b) },
        (Shape::ListU32 | Shape::ArrayU32, Value::Words(xs)) => {
            if xs.len() > MAX_NODES { return Err(Error::Bound); }
            if *shape == Shape::ArrayU32 && !xs.len().is_power_of_two() { return Err(Error::ArrayCount); }
            c.put32(xs.len() as u32)?; for x in xs { c.node(depth + 1)?; c.put32(*x)?; } Ok(())
        }
        (Shape::Ref(id), Value::Product { variant, fields }) => {
            let t = &m.types[*id]; let v = t.variants.get(*variant).ok_or(Error::Tag)?;
            if v.fields.len() != fields.len() { return Err(Error::Shape); }
            if t.variants.len() > 1 { c.put(&[*variant as u8])?; }
            for (field, value) in v.fields.iter().zip(fields) { put(m, &field.shape, value, c, depth + 1)?; } Ok(())
        }
        _ => Err(Error::Shape),
    }
}
pub fn decode(model: &Model, root: usize, bytes: &[u8]) -> Result<Value, Error> {
    if bytes.len() > MAX_BYTES { return Err(Error::Bound); }
    let mut c = Cursor { bytes: bytes.into(), at: 0, nodes: 0, format: model.format };
    let out = get(model, &Shape::Ref(root), &mut c, 0)?;
    if c.at != c.bytes.len() { return Err(Error::Trailing); } Ok(out)
}
fn get(m: &Model, shape: &Shape, c: &mut Cursor, depth: usize) -> Result<Value, Error> {
    c.node(depth)?;
    Ok(match shape {
        Shape::U32 => Value::U32(c.get32()?),
        Shape::Bool => Value::Bool(match c.get(1)?[0] { 0 => false, 1 => true, _ => return Err(Error::Tag) }),
        Shape::Word64 | Shape::Signed64 => { let be = c.format == Format::RawBe; let x = c.get(8)?.try_into().unwrap(); Value::Word64(if be { u64::from_be_bytes(x) } else { u64::from_le_bytes(x) }) },
        Shape::String | Shape::Blob => {
            let n = if *shape == Shape::Blob && c.format == Format::RawBe { c.bytes.len() - c.at } else { c.get32()? as usize };
            if n > MAX_BYTES { return Err(Error::Bound); }
            let b = c.get(n)?;
            if *shape == Shape::String { Value::String(std::str::from_utf8(b).map_err(|_| Error::Utf8)?.into()) } else { Value::Blob(b.into()) }
        }
        Shape::ListU32 | Shape::ArrayU32 => {
            let n = c.get32()? as usize;
            if n > MAX_NODES || c.nodes + n > MAX_NODES { return Err(Error::Bound); }
            if *shape == Shape::ArrayU32 && !n.is_power_of_two() { return Err(Error::ArrayCount); }
            if n > (c.bytes.len() - c.at) / 4 { return Err(Error::Truncated); }
            c.nodes += n; let mut words = Vec::with_capacity(n); for _ in 0..n { words.push(c.get32()?); } Value::Words(words)
        }
        Shape::Ref(id) => {
            let t = &m.types[*id]; let variant = if t.variants.len() > 1 { c.get(1)?[0] as usize } else { 0 };
            let v = t.variants.get(variant).ok_or(Error::Tag)?;
            let mut fields = Vec::new(); for f in &v.fields { fields.push(get(m, &f.shape, c, depth + 1)?); } Value::Product { variant, fields }
        }
    })
}
