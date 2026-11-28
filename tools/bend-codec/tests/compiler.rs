use bend_codec::{generate, load, write_outputs, Format, Shape};
use bend_codec::wire::{decode, encode, Error, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new(source: &str) -> Self {
        let path=std::env::temp_dir().join(format!("bend_codec_{}_{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
        std::fs::create_dir_all(path.join("src/bytes")).unwrap();
        std::fs::write(path.join("Cargo.toml"),"[workspace]\n").unwrap();
        std::fs::write(path.join("src/bytes/main.bend"),"import Base\ntype Buffer is Type:\n  ByteBuffer{words: Array<U32>, length: U32}\n").unwrap();
        std::fs::write(path.join("src/bytes/types.bend"),"import Base\ntype Word64 is Data:\n  U64{hi: U32, lo: U32}\ntype Signed64 is Data:\n  I64{hi: U32, lo: U32}\n").unwrap();
        std::fs::write(path.join("schema.bend"),source).unwrap(); Self(path)
    }
    fn model(&self, format: Format) -> bend_codec::Model { load(&self.0.join("schema.bend"), &["Root".into()],format).unwrap() }
}
impl Drop for Fixture { fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); } }
fn product(variant: usize, fields: Vec<Value>) -> Value { Value::Product { variant, fields } }
#[test]
fn source_order_aliases_and_nested_sum() {
    let f=Fixture::new("import Base\nimport ./part.bend as Alias\ntype Root is Type:\n  Packet{second: Alias.Choice, first: U32}\ndef main() -> U32:\n  999\n");
    std::fs::write(f.0.join("part.bend"),"import Base\ntype Choice is Data:\n  NoneHere{}\n  SomeHere{payload: U32}\n").unwrap();
    let m=f.model(Format::Bsatn);
    assert_eq!(m.types[0].variants[0].fields[0].name,"second");
    assert_eq!(m.types[1].variants[1].name,"SomeHere");
    let value=product(0,vec![product(1,vec![Value::U32(0x11223344)]),Value::U32(9)]);
    let bytes=encode(&m,0,&value).unwrap(); assert_eq!(bytes,vec![1,0x44,0x33,0x22,0x11,9,0,0,0]); assert_eq!(decode(&m,0,&bytes).unwrap(),value);
    let out=generate(&m,&f.0.join("generated")); assert!(out.bend.contains("choice")==false); assert!(out.bend.contains("case Schema1.SomeHere")); assert!(!out.c.contains("CID(Alias."));
}
#[test]
fn unicode_containers_and_malformed_wire() {
    let f=Fixture::new("import Base\ntype Root is Type:\n  Packet{flag: Bool, text: String, list: List<U32>, array: Array<U32>}\n");
    let m=f.model(Format::Bsatn);
    let value=product(0,vec![Value::Bool(true),Value::String("λ🦀".into()),Value::Words(vec![7]),Value::Words(vec![42,43])]);
    let expected=vec![1,6,0,0,0,0xce,0xbb,0xf0,0x9f,0xa6,0x80,1,0,0,0,7,0,0,0,2,0,0,0,42,0,0,0,43,0,0,0];
    assert_eq!(encode(&m,0,&value).unwrap(),expected); assert_eq!(decode(&m,0,&expected).unwrap(),value);
    let mut bad=expected.clone(); bad[0]=2; assert_eq!(decode(&m,0,&bad),Err(Error::Tag));
    bad=expected.clone(); bad[5]=0xc0; assert_eq!(decode(&m,0,&bad),Err(Error::Utf8));
    bad=expected.clone(); bad.push(0); assert_eq!(decode(&m,0,&bad),Err(Error::Trailing));
    for n in 0..expected.len() { assert!(decode(&m,0,&expected[..n]).is_err()); }
    bad=expected.clone(); bad[1..5].copy_from_slice(&u32::MAX.to_le_bytes()); assert_eq!(decode(&m,0,&bad),Err(Error::Bound));
    let g=Fixture::new("import Base\ntype Root is Type:\n  Packet{array: Array<U32>}\n"); let m=g.model(Format::Bsatn);
    assert_eq!(decode(&m,0,&[0,0,0,0]),Err(Error::ArrayCount)); assert_eq!(decode(&m,0,&[3,0,0,0]),Err(Error::ArrayCount));
    assert_eq!(decode(&m,0,&u32::MAX.to_le_bytes()),Err(Error::Bound));
}
#[test]
fn raw_seven_words_and_blob_golden() {
    let f=Fixture::new("import Base\nimport ./src/bytes/main.bend as B\nimport ./src/bytes/types.bend as W\ntype Header is Data:\n  Head{version: W.Word64, token: W.Word64, epoch: W.Word64, direction: W.Word64, connection: W.Word64, sequence: W.Word64, due: W.Word64}\ntype Root is Type:\n  Frame{header: Header, body: B.Buffer}\n");
    let m=f.model(Format::RawBe);
    let value=product(0,vec![product(0,(1..=7).map(Value::Word64).collect()),Value::Blob(vec![0,0xff,0x7f])]);
    let mut expected=Vec::new(); for n in 1u64..=7 { expected.extend_from_slice(&n.to_be_bytes()); } expected.extend_from_slice(&[0,0xff,0x7f]);
    assert_eq!(encode(&m,0,&value).unwrap(),expected); assert_eq!(decode(&m,0,&expected).unwrap(),value);
    let b=f.model(Format::Bsatn); let encoded=encode(&b,0,&value).unwrap(); assert_eq!(&encoded[56..],&[3,0,0,0,0,0xff,0x7f]);
    assert_eq!(m.types[0].variants[0].fields[1].shape,Shape::Blob);
}
#[test]
fn invalid_source_and_canonical_identity() {
    for (source, text) in [
        ("type Root is Type:\n  Packet{value: F32}\n","unsupported"),
        ("type Root is Type:\n  Packet{value: Root}\n","recursive"),
        ("type Root<A: Type> is Type:\n  Packet{value: A}\n","generic"),
        ("type Root is Type:\n  Packet{value: List<Bool>}\n","only Array"),
        ("type Root is Type:\n  Packet{value: U32, value: U32}\n","duplicate"),
    ] {
        let f=Fixture::new(source); let e=load(&f.0.join("schema.bend"), &["Root".into()],Format::Bsatn).unwrap_err(); assert!(e.message.contains(text),"{e}"); assert!(e.location.line>0);
    }
    let f=Fixture::new("import ./src/bytes/main.bend as B\ntype Root is Type:\n  Packet{body: B.Buffer, after: U32}\n");
    assert!(load(&f.0.join("schema.bend"), &["Root".into()],Format::RawBe).unwrap_err().message.contains("final"));
    std::fs::write(f.0.join("src/bytes/main.bend"),"type Buffer is Type:\n  ByteBuffer{words: U32, length: U32}\n").unwrap();
    assert!(load(&f.0.join("schema.bend"), &["Root".into()],Format::Bsatn).unwrap_err().message.contains("canonical"));
    let mut source = "import Base\ntype Leaf is Type:\n  LeafValue{".to_string();
    source += &(0..64).map(|i| format!("f{i}: U32")).collect::<Vec<_>>().join(",");
    source += "}\ntype Root is Type:\n  Packet{a: Leaf,b: Leaf,c: Leaf,d: Leaf}\n";
    let f=Fixture::new(&source);
    assert!(load(&f.0.join("schema.bend"), &["Root".into()],Format::Bsatn).unwrap_err().message.contains("248 inline slots"));
}
#[test]
fn deterministic_outputs_replace_stale_generation() {
    let f=Fixture::new("import Base\ntype Root is Data:\n  Packet{x: U32}\n"); let out=f.0.join("out"); let m=f.model(Format::Bsatn);
    let a=generate(&m,&out); let b=generate(&m,&out); assert_eq!(a.bend,b.bend); assert_eq!(a.c,b.c); assert_eq!(a.contract,b.contract);
    write_outputs(&out,&a).unwrap(); std::fs::write(out.join("codec.js"),"stale").unwrap(); write_outputs(&out,&b).unwrap(); assert_eq!(std::fs::read_to_string(out.join("codec.js")).unwrap(),b.js);
    std::fs::write(f.0.join("schema.bend"),"import Base\ntype Root is Data:\n  Packet{x: U32, y: U32}\n").unwrap(); let c=generate(&f.model(Format::Bsatn),&out); write_outputs(&out,&c).unwrap(); assert_ne!(a.contract,c.contract); assert_eq!(std::fs::read_to_string(out.join("contract.json")).unwrap(),c.contract);
}
