use bend_codec::{generate, load, Format};
use std::{collections::hash_map::DefaultHasher, hash::{Hash, Hasher}, path::{Path, PathBuf}, process::Command, time::Instant};

struct BuilderLock(PathBuf);
impl Drop for BuilderLock { fn drop(&mut self) { std::fs::remove_dir(&self.0).unwrap(); } }
fn builder_lock(path: PathBuf) -> BuilderLock {
    let start = Instant::now();
    loop {
        match std::fs::create_dir(&path) {
            Ok(()) => return BuilderLock(path),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && start.elapsed().as_secs()<30 => std::thread::sleep(std::time::Duration::from_millis(25)),
            Err(e) => panic!("builder lock {}: {e}",path.display()),
        }
    }
}

fn command(c: &mut Command) -> String {
    let out = c.output().unwrap();
    assert!(out.status.success(), "{c:?}\n{}\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    String::from_utf8(out.stdout).unwrap()
}

pub fn run(format: Format) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap();
    let bend = std::env::var_os("BEND").unwrap_or_else(|| root.join(".ws-cache/bend/bin/bend").into_os_string());
    let version = command(Command::new(&bend).arg("version"));
    assert!(version.contains("2.0.40"));
    let clang = command(Command::new("clang-19").arg("--version"));
    let native_path = root.join("src/bytes");
    let native = native_path.as_path();
    let mut inputs = format!("{version}{clang}{format:?}-O2-pthread");
    for file in ["main.bend", "types.bend", "native.c", "native.js", "byte_buffer.h"] {
        inputs += &std::fs::read_to_string(native.join(file)).unwrap();
    }
    inputs += &command(Command::new("sha256sum").arg(&bend));
    inputs += &std::fs::read_to_string(root.join("src/ws/native/abi.h")).unwrap();
    inputs += include_str!("../../src/generate.rs");
    inputs += include_str!("../../src/schema.rs");
    inputs += include_str!("../../src/main.rs");
    inputs += include_str!("../../src/runtime.c");
    inputs += include_str!("../../src/runtime.js");
    inputs += include_str!("native.rs");
    let mut hash = DefaultHasher::new(); inputs.hash(&mut hash);
    let parent = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/codec-fixtures");
    std::fs::create_dir_all(&parent).unwrap();
    let parent = std::fs::canonicalize(parent).unwrap();
    let dir = parent.join(format!("fixture_{:016x}", hash.finish()));
    let _lock = builder_lock(parent.join(format!("builder_{:016x}", hash.finish())));
    std::fs::create_dir_all(&dir).unwrap();
    let executable = dir.join("native");
    let digest = |p: &Path| command(Command::new("sha256sum").arg(p));
    let start = Instant::now();
    let hit = executable.is_file() && std::fs::read_to_string(dir.join("inputs")).ok().as_ref() == Some(&inputs)
        && std::fs::read_to_string(dir.join("artifact.sha256")).ok() == Some(digest(&executable));
    if !hit {
        std::fs::create_dir_all(dir.join("src/bytes")).unwrap();
        for file in ["main.bend", "types.bend", "native.c", "native.js", "byte_buffer.h"] {
            std::fs::copy(native.join(file), dir.join("src/bytes").join(file)).unwrap();
        }
        std::fs::write(dir.join("schema.bend"), if format == Format::Bsatn { BSATN_SCHEMA } else { RAW_SCHEMA }).unwrap();
        let model = load(&dir.join("schema.bend"), &["Root".into()], format).unwrap();
        let outputs = generate(&model, &dir.join("codec"));
        command(Command::new(env!("CARGO_BIN_EXE_bend-codec")).arg(dir.join("schema.bend")).arg("--out").arg(dir.join("codec")).args(["--root", "Root", "--format", if format == Format::Bsatn { "bsatn" } else { "raw-be" }]).env("BEND", &bend));
        assert_eq!(std::fs::read_to_string(dir.join("codec/codec.bend")).unwrap(), outputs.bend);
        assert_eq!(std::fs::read_to_string(dir.join("codec/codec.c")).unwrap(), outputs.c);
        let mut codec = outputs.bend;
        codec += CHECK_BEND;
        std::fs::write(dir.join("codec/codec.bend"), codec).unwrap();
        let golden = if format == Format::Bsatn { BSATN_GOLDEN } else { RAW_GOLDEN };
        let probe = PROBE_C.replace("GOLDEN_BYTES", golden).replace("RAW_FORMAT", if format == Format::RawBe { "1" } else { "0" });
        std::fs::write(dir.join("codec/codec.c"), outputs.c + &probe).unwrap();
        std::fs::write(dir.join("main.bend"), if format == Format::Bsatn { BSATN_MAIN } else { RAW_MAIN }).unwrap();
        command(Command::new(&bend).arg(dir.join("main.bend")).arg("-o").arg(dir.join("native.c")));
        command(Command::new("clang-19").args(["-O2", "-pthread", "-I"]).arg(native).arg("-I").arg(root.join("src/ws/native")).arg(dir.join("native.c")).arg("-o").arg(dir.join("native.build")));
        std::fs::rename(dir.join("native.build"), &executable).unwrap();
        std::fs::write(dir.join("inputs"), &inputs).unwrap();
        std::fs::write(dir.join("artifact.sha256"), digest(&executable)).unwrap();
    }
    let build = start.elapsed();
    let start = Instant::now();
    let stdout = command(Command::new("timeout").args(["--kill-after=2s", "10s"]).arg(&executable).args(["+RTS", "--threads", "1", "--gpu", "off"]));
    assert_eq!(stdout, if format == Format::Bsatn { "full typed=287454020,99,λ🦀,42,43,7,8,2309737967,19088743,4294967294,4294967295,8388352,3\nempty typed=100\nfull typed=287454020,99,λ🦀,42,43,7,8,2309737967,19088743,4294967294,4294967295,8388352,3\n" } else { "raw typed=1,2,3,4,5,6,7,3\nraw typed=1,2,3,4,5,6,7,3\n" });
    eprintln!("native {format:?}: build={build:?} test={:?} cache_hit={hit} evidence={}", start.elapsed(), dir.display());
}

const BSATN_SCHEMA: &str = "import Base\nimport ./src/bytes/main.bend as B\nimport ./src/bytes/types.bend as W\ntype Choice is Data:\n  Empty{}\n  Full{number: U32}\ntype Header is Data:\n  Head{choice: Choice, trailing: U32}\ntype Root is Type:\n  Packet{header: Header, text: String, array: Array<U32>, list: List<U32>, word: W.Word64, signed: W.Signed64, body: B.Buffer}\ndef main() -> U32:\n  999\n";
const RAW_SCHEMA: &str = "import Base\nimport ./src/bytes/main.bend as B\nimport ./src/bytes/types.bend as W\ntype Header is Data:\n  Head{version: W.Word64, token: W.Word64, epoch: W.Word64, direction: W.Word64, connection: W.Word64, sequence: W.Word64, due: W.Word64}\ntype Root is Type:\n  Frame{header: Header, body: B.Buffer}\n";
const BSATN_GOLDEN: &str = "1,68,51,34,17,99,0,0,0,6,0,0,0,206,187,240,159,166,128,2,0,0,0,42,0,0,0,43,0,0,0,2,0,0,0,7,0,0,0,8,0,0,0,103,69,35,1,239,205,171,137,255,255,255,255,254,255,255,255,3,0,0,0,0,255,127";
const RAW_GOLDEN: &str = "0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,3,0,0,0,0,0,0,0,4,0,0,0,0,0,0,0,5,0,0,0,0,0,0,0,6,0,0,0,0,0,0,0,7,0,255,127";

const CHECK_BEND: &str = r#"
type StaticSlots is Type:
  DStatic{a: U32,b: U32,c: U32,d: U32,e: U32,f: U32,g: U32,h: U32,i: U32,j: U32,k: U32,l: U32,m: U32,n: U32,o: U32,p: U32}
def check_result(result: DenseEncoded) -> Bytes.Buffer:
  match result:
    case DEncoded{_, _, words, length, _}:
      Bytes.ByteBuffer{words, length}
def Native.static(value: StaticSlots) -> IO(DenseEncoded):
  import "./codec.c"
  import "./codec.js"
def static_test() -> IO(Bytes.Buffer):
  do IO<Bytes.Buffer>:
    result : DenseEncoded <- Native.static(DStatic{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0})
    return check_result(result)
def Native.check(words: Array<U32>, length: U32, mode: U32) -> IO(DenseEncoded):
  import "./codec.c"
  import "./codec.js"
def check(buffer: Bytes.Buffer, mode: U32) -> IO(Bytes.Buffer):
  match buffer:
    case Bytes.ByteBuffer{words, length}:
      do IO<Bytes.Buffer>:
        result : DenseEncoded <- Native.check(words, length, mode)
        return check_result(result)
"#;

const PROBE_C: &str = r#"
static const uint8_t golden[]={GOLDEN_BYTES};
static uint64_t owned_pointer;
#define require(b) do { if (!(b)) { fprintf(stderr,"native codec proof failed line=%u: %s\n",__LINE__,#b); abort(); } } while (0)
WsByteView ws_socket_lease_view(uint64_t lease, uint32_t index) {
  require(lease!=0);
  return (WsByteView){.bytes=golden,.len=sizeof(golden),.kind=0,.code=index?WS_INVALID:0};
}
static Term bc_check(Env e, Term *f, IoWork *w) {
  (void)w; uint8_t *p; uint32_t cap;
  require(!byte_array_view(e,f[0],f[1],&p,&cap));
  if (f[2]==4) {
    require(!term_rfc(f[0]) && blk_loc(e.mem,f[0])<HEAP_OFF);
    Term payload[BC_SLOTS]={0}; BcCursor read={.in=golden,.limit=sizeof(golden)}; bc_read(e,7,&read,0,payload);
    const BcType *t=&bc_types[0]; uint32_t off=t->count>1?1:0;
    Term value=bc_make(e,t->variants[0].cid,t->variants[0].arity,payload+off);
    Term small=byte_array_new(e,4); *(uint32_t*)byte_array_data(e,small)=777;
    Term failed_args[]={small,0,value}; Term failed=bc_encode(e,failed_args,0,CID(DEncoded));
    const Term *failure=e.mem+term_peek(e.mem,failed);
    require(failure[2]==small && failure[3]==0 && failure[4]==2 && *(uint32_t*)byte_array_data(e,small)==777);
    term_drop(e,failed);
    read=(BcCursor){.in=golden,.limit=sizeof(golden)}; bc_read(e,7,&read,0,payload);
    value=bc_make(e,t->variants[0].cid,t->variants[0].arity,payload+off);
    Term args[]={f[0],0,value}; Term encoded=bc_encode(e,args,0,CID(DEncoded));
    const Term *fields=e.mem+term_peek(e.mem,encoded);
    require(fields[4]==0 && fields[3]==sizeof(golden) && blk_loc(e.mem,fields[2])>=HEAP_OFF);
    require(!memcmp(byte_array_data(e,fields[2]),golden,sizeof(golden)));
    for (uint32_t i=0;i<cap;++i) require(p[i]==0);
    return encoded;
  }
  if (f[2]==6) { if (RAW_FORMAT) f[1]=55; else { require(f[1]<cap); p[f[1]++]=0; } }
  else if (f[2]==0) { require(!term_rfc(f[0]) && blk_loc(e.mem,f[0])>=HEAP_OFF); owned_pointer=blk_loc(e.mem,f[0]); }
  else {
    require(owned_pointer==blk_loc(e.mem,f[0]));
    if (f[2]==1) {
      if (f[1]!=sizeof(golden) || memcmp(p,golden,sizeof(golden))) { fprintf(stderr,"golden actual length=%u expected=%zu bytes:",(unsigned)f[1],sizeof(golden)); for (uint32_t i=0;i<f[1];++i) fprintf(stderr,"%u,",p[i]); fprintf(stderr,"\n"); }
      require(f[1]==sizeof(golden) && !memcmp(p,golden,sizeof(golden)));
      Term slots[BC_SLOTS];
      for (uint32_t n=0;n<sizeof(golden)-(RAW_FORMAT?3:0);++n) { bc_result(e,p,n,0,slots,0); require(slots[0]==1 && slots[3]!=0); }
      if (!RAW_FORMAT) {
        uint8_t bad[sizeof(golden)+1]; memcpy(bad,p,sizeof(golden)); bad[sizeof(golden)]=0;
        bc_result(e,bad,sizeof(bad),0,slots,0); require(slots[0]==1 && slots[3]==6);
        bad[0]=2; bc_result(e,bad,sizeof(golden),0,slots,0); require(slots[3]==4);
        memcpy(bad,p,sizeof(golden)); bad[13]=192; bc_result(e,bad,sizeof(golden),0,slots,0); require(slots[3]==5);
        memcpy(bad,p,sizeof(golden)); memset(bad+9,255,4); bc_result(e,bad,sizeof(golden),0,slots,0); require(slots[3]==2);
        memcpy(bad,p,sizeof(golden)); memset(bad+19,0,4); bc_result(e,bad,sizeof(golden),0,slots,0); require(slots[3]==7);
        memcpy(bad,p,sizeof(golden)); memset(bad+19,255,4); bc_result(e,bad,sizeof(golden),0,slots,0); require(slots[3]==2);
      }
      Term copy=byte_array_new(e,cap); memcpy(byte_array_data(e,copy),p,f[1]);
      Term shared=term_keep(e,copy,1), original=shared;
      Term payload[BC_SLOTS]={0}; BcCursor read={.in=p,.limit=f[1]}; bc_read(e,7,&read,0,payload);
      const BcType *t=&bc_types[0]; uint32_t off=t->count>1?1:0;
      Term value=bc_make(e,t->variants[0].cid,t->variants[0].arity,payload+off);
      Term args[]={shared,0,value}; Term encoded=bc_encode(e,args,0,CID(DEncoded));
      const Term *fields=e.mem+term_peek(e.mem,encoded);
      require(fields[4]==0 && fields[3]==sizeof(golden) && blk_loc(e.mem,fields[2])!=blk_loc(e.mem,original));
      uint8_t *old; uint32_t oldcap; require(!bc_array_view(e,original,f[1],&old,&oldcap)); require(!memcmp(old,golden,sizeof(golden)));
      term_drop(e,encoded); term_drop(e,original);
    }
    if (f[2]==3) { require(f[1]==41 && p[0]==0 && p[1]==100 && p[2]==0 && p[3]==0 && p[4]==0 && p[9]==1); for (uint32_t i=5;i<41;++i) if (i!=9) require(p[i]==0); }
  }
  Term fields[]={BC_MARKER,1,f[0],f[1],0}; return bc_make(e,CID(DEncoded),5,fields);
}
static Term bc_static(Env e, Term *f, IoWork *w) {
  require(term_tag(f[0])==TAG_CTR && cid_arity(term_aux(f[0]))==16 && term_peek(e.mem,f[0])<HEAP_OFF);
  Term args[]={term_blk(false,5,term_peek(e.mem,f[0])),0,4}; return bc_check(e,args,w);
}
static void __attribute__((constructor)) probe_register(void) { io_eff(CID(Native.check),bc_check); io_eff(CID(Native.static),bc_static); }
"#;

const BSATN_MAIN: &str = r#"import Base
import ./codec/codec.bend as C
import ./schema.bend as S
import ./src/bytes/main.bend as B
import ./src/bytes/types.bend as W
def show_word(value: Array<U32> & U32) -> String:
  match value:
    case (_, number):
      U32.show(number)
def show_pair(value: Array<U32> & U32) -> String:
  match value:
    case (array, number):
      U32.show(number) ++ "," ++ show_word(array[1])
def report(value: C.Decode_Root) -> IO(Unit):
  match value:
    case C.DecodeFailed_Root{code}:
      IO.die(Unit, code, "decode failed")
    case C.Decoded_Root{S.Packet{S.Head{S.Full{number}, trailing}, text, array, Con{l0, Con{l1, Nil{}}}, W.U64{hi, lo}, W.I64{sh, sl}, B.ByteBuffer{words, length}}}:
      IO.print("full typed=" ++ U32.show(number) ++ "," ++ U32.show(trailing) ++ "," ++ text ++ "," ++ show_pair(array[0]) ++ "," ++ U32.show(l0) ++ "," ++ U32.show(l1) ++ "," ++ U32.show(hi) ++ "," ++ U32.show(lo) ++ "," ++ U32.show(sh) ++ "," ++ U32.show(sl) ++ "," ++ show_word(words[0]) ++ "," ++ U32.show(length))
    case C.Decoded_Root{S.Packet{S.Head{S.Empty{}, trailing}, _, _, _, _, _, _}}:
      IO.print("empty typed=" ++ U32.show(trailing))
    case _:
      IO.die(Unit, 1, "bad typed value")
def encoded(result: B.Buffer & U32) -> IO(B.Buffer):
  match result:
    case (buffer, 0):
      IO.pure(B.Buffer, buffer)
    case (_, code):
      IO.die(B.Buffer, code, "encode failed")
def report_input(result: B.Buffer & C.Decode_Root) -> IO(B.Buffer):
  match result:
    case (buffer, value):
      do IO<B.Buffer>:
        report(value)
        return buffer
def failed_input(result: B.Buffer & C.Decode_Root) -> IO(B.Buffer):
  match result:
    case (buffer, C.DecodeFailed_Root{6}):
      IO.pure(B.Buffer, buffer)
    case _:
      IO.die(B.Buffer, 1, "expected trailing failure")
def report_lease(result: File & C.Decode_Root) -> IO(File):
  match result:
    case (file, value):
      do IO<File>:
        report(value)
        return file
def failed_lease(result: File & C.Decode_Root) -> IO(Unit):
  match result:
    case (file, C.DecodeFailed_Root{8}):
      File.close(file)
    case _:
      IO.die(Unit, 1, "expected lease failure")
def main() -> IO(Unit):
  do IO<Unit>:
    static : B.Buffer <- C.static_test()
    _ : U32 <- B.release(static)
    buffer : B.Buffer <- B.new(128)
    buffer : B.Buffer <- C.check(buffer, 0)
    array : Array<U32> = [42 : U32*2n]
    array : Array<U32> = array[1] <- 43
    result : B.Buffer & U32 <- C.encode_Root(buffer, S.Packet{S.Head{S.Full{287454020}, 99}, "λ🦀", array, [7,8], W.U64{2309737967,19088743}, W.I64{4294967294,4294967295}, B.ByteBuffer{[8388352 : U32*1n],3}})
    buffer : B.Buffer <- encoded(result)
    buffer : B.Buffer <- C.check(buffer, 1)
    decoded : B.Buffer & C.Decode_Root <- C.decode_Root(buffer)
    buffer : B.Buffer <- report_input(decoded)
    result : B.Buffer & U32 <- C.encode_Root(buffer, S.Packet{S.Head{S.Empty{}, 100}, "", [0 : U32*1n], [], W.U64{0,0}, W.I64{0,0}, B.ByteBuffer{[0 : U32*1n],0}})
    buffer : B.Buffer <- encoded(result)
    buffer : B.Buffer <- C.check(buffer, 3)
    decoded : B.Buffer & C.Decode_Root <- C.decode_Root(buffer)
    buffer : B.Buffer <- report_input(decoded)
    buffer : B.Buffer <- C.check(buffer, 6)
    decoded : B.Buffer & C.Decode_Root <- C.decode_Root(buffer)
    buffer : B.Buffer <- failed_input(decoded)
    _ : U32 <- B.release(buffer)
    file : File <- IO.try(File, File.open("/dev/null", "r"))
    leased : File & C.Decode_Root <- C.decode_lease_Root(file, 0)
    file : File <- report_lease(leased)
    leased : File & C.Decode_Root <- C.decode_lease_Root(file, 1)
    failed_lease(leased)
    return Unit{}
"#;

const RAW_MAIN: &str = r#"import Base
import ./codec/codec.bend as C
import ./schema.bend as S
import ./src/bytes/main.bend as B
import ./src/bytes/types.bend as W
def report(value: C.Decode_Root) -> IO(Unit):
  match value:
    case C.DecodeFailed_Root{code}:
      IO.die(Unit, code, "decode failed")
    case C.Decoded_Root{S.Frame{S.Head{W.U64{_, a}, W.U64{_, b}, W.U64{_, c}, W.U64{_, d}, W.U64{_, e}, W.U64{_, f}, W.U64{_, g}}, B.ByteBuffer{_, length}}}:
      IO.print("raw typed=" ++ U32.show(a) ++ "," ++ U32.show(b) ++ "," ++ U32.show(c) ++ "," ++ U32.show(d) ++ "," ++ U32.show(e) ++ "," ++ U32.show(f) ++ "," ++ U32.show(g) ++ "," ++ U32.show(length))
def encoded(result: B.Buffer & U32) -> IO(B.Buffer):
  match result:
    case (buffer, 0):
      IO.pure(B.Buffer, buffer)
    case (_, code):
      IO.die(B.Buffer, code, "encode failed")
def report_input(result: B.Buffer & C.Decode_Root) -> IO(B.Buffer):
  match result:
    case (buffer, value):
      do IO<B.Buffer>:
        report(value)
        return buffer
def failed_input(result: B.Buffer & C.Decode_Root) -> IO(B.Buffer):
  match result:
    case (buffer, C.DecodeFailed_Root{3}):
      IO.pure(B.Buffer, buffer)
    case _:
      IO.die(B.Buffer, 1, "expected truncation failure")
def report_lease(result: File & C.Decode_Root) -> IO(File):
  match result:
    case (file, value):
      do IO<File>:
        report(value)
        return file
def failed_lease(result: File & C.Decode_Root) -> IO(Unit):
  match result:
    case (file, C.DecodeFailed_Root{8}):
      File.close(file)
    case _:
      IO.die(Unit, 1, "expected lease failure")
def main() -> IO(Unit):
  do IO<Unit>:
    static : B.Buffer <- C.static_test()
    _ : U32 <- B.release(static)
    buffer : B.Buffer <- B.new(128)
    buffer : B.Buffer <- C.check(buffer, 0)
    result : B.Buffer & U32 <- C.encode_Root(buffer, S.Frame{S.Head{W.U64{0,1}, W.U64{0,2}, W.U64{0,3}, W.U64{0,4}, W.U64{0,5}, W.U64{0,6}, W.U64{0,7}}, B.ByteBuffer{[8388352 : U32*1n],3}})
    buffer : B.Buffer <- encoded(result)
    buffer : B.Buffer <- C.check(buffer, 1)
    decoded : B.Buffer & C.Decode_Root <- C.decode_Root(buffer)
    buffer : B.Buffer <- report_input(decoded)
    buffer : B.Buffer <- C.check(buffer, 6)
    decoded : B.Buffer & C.Decode_Root <- C.decode_Root(buffer)
    buffer : B.Buffer <- failed_input(decoded)
    _ : U32 <- B.release(buffer)
    file : File <- IO.try(File, File.open("/dev/null", "r"))
    leased : File & C.Decode_Root <- C.decode_lease_Root(file, 0)
    file : File <- report_lease(leased)
    leased : File & C.Decode_Root <- C.decode_lease_Root(file, 1)
    failed_lease(leased)
    return Unit{}
"#;
