use crate::schema::*;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};

pub struct Outputs { pub bend: String, pub c: String, pub js: String, pub contract: String, pub validation: String }
fn relative(from: &Path, to: &Path) -> String {
    let a: Vec<_> = from.components().collect(); let b: Vec<_> = to.components().collect();
    let n = a.iter().zip(&b).take_while(|(a,b)| a == b).count();
    let mut p = PathBuf::new(); for _ in n..a.len() { p.push(".."); } for c in &b[n..] { if let Component::Normal(x) = c { p.push(x); } }
    let s = p.to_string_lossy(); if s.starts_with('.') { s.into() } else { format!("./{s}") }
}
fn kind(s: &Shape) -> usize { match s { Shape::U32 => 0, Shape::Bool => 1, Shape::String => 2, Shape::ListU32 => 3, Shape::ArrayU32 => 4, Shape::Blob => 5, Shape::Word64 | Shape::Signed64 => 6, Shape::Ref(id) => 7 + id } }
pub fn native_width(m: &Model, s: &Shape) -> usize {
    match s {
        Shape::Blob | Shape::Word64 | Shape::Signed64 => 4,
        Shape::Ref(id) => {
            let t = &m.types[*id];
            usize::from(t.variants.len() > 1) + t.variants.iter().map(|v| v.fields.iter().fold(2usize, |n, f| n.saturating_add(native_width(m, &f.shape)).min(249))).max().unwrap()
        }
        _ => 1,
    }
}
fn dto(s: &Shape) -> String { match s { Shape::Bool => "U32".into(), Shape::Blob => "DenseBlob".into(), Shape::Word64 | Shape::Signed64 => "DenseWord".into(), Shape::Ref(id) => format!("Dense{id}"), _ => source_type(s, &BTreeMap::new(), &[]) } }
fn source_type(s: &Shape, modules: &BTreeMap<PathBuf, String>, types: &[TypeDef]) -> String {
    match s { Shape::U32 => "U32".into(), Shape::Bool => "Bool".into(), Shape::String => "String".into(), Shape::ListU32 => "List<U32>".into(), Shape::ArrayU32 => "Array<U32>".into(), Shape::Blob => "Bytes.Buffer".into(), Shape::Word64 => "Words.Word64".into(), Shape::Signed64 => "Words.Signed64".into(), Shape::Ref(id) => format!("{}.{}", modules[&types[*id].module], types[*id].name) }
}
fn convert(s: &Shape, value: &str, marshal: bool) -> String {
    let prefix = if marshal { "marshal" } else { "unmarshal" };
    let suffix = match s { Shape::Bool => Some("bool".into()), Shape::Blob => Some("blob".into()), Shape::Word64 => Some("word".into()), Shape::Signed64 => Some("signed".into()), Shape::Ref(id) => Some(id.to_string()), _ => None };
    suffix.map_or_else(|| value.into(), |s| format!("{prefix}_{s}({value})"))
}
pub fn generate(m: &Model, out: &Path) -> Outputs {
    let mut modules = BTreeMap::new(); modules.insert(m.source.clone(), "Schema".into());
    let paths: BTreeSet<_> = m.types.iter().map(|t| t.module.clone()).collect();
    for p in paths { if !modules.contains_key(&p) { let alias = format!("Schema{}", modules.len()); modules.insert(p, alias); } }
    let shapes: Vec<_> = m.types.iter().flat_map(|t| &t.variants).flat_map(|v| &v.fields).map(|f| &f.shape).collect();
    let blob = shapes.contains(&&Shape::Blob); let word = shapes.contains(&&Shape::Word64); let signed = shapes.contains(&&Shape::Signed64); let boolean = shapes.contains(&&Shape::Bool);
    let mut bend = "import Base\n".to_string();
    for (p, alias) in &modules { bend += &format!("import {} as {alias}\n", relative(out, p)); }
    bend += &format!("import {} as Bytes\n", relative(out, &m.bytes_module));
    if word || signed { bend += &format!("import {} as Words\n", relative(out, m.words_module.as_ref().unwrap())); }
    bend += "\n";
    if blob { bend += "type DenseBlob is Type:\n  DBlob{marker0: U32, marker1: U32, words: Array<U32>, length: U32}\n\n"; }
    if word || signed { bend += "type DenseWord is Type:\n  DWord{marker0: U32, marker1: U32, hi: U32, lo: U32}\n\n"; }
    for (id, t) in m.types.iter().enumerate() {
        bend += &format!("type Dense{id} is Type:\n");
        for (tag, v) in t.variants.iter().enumerate() {
            bend += &format!("  D{id}V{tag}{{marker0: U32, marker1: U32");
            for f in &v.fields { bend += &format!(", {}: {}", f.name, dto(&f.shape)); } bend += "}\n";
        } bend += "\n";
    }
    if boolean { bend += "def marshal_bool(value: Bool) -> U32:\n  match value:\n    case False{}:\n      0\n    case True{}:\n      1\n\ndef unmarshal_bool(value: U32) -> Bool:\n  match value:\n    case 0:\n      False{}\n    case _:\n      True{}\n\n"; }
    if blob { bend += "def marshal_blob(value: Bytes.Buffer) -> DenseBlob:\n  match value:\n    case Bytes.ByteBuffer{words, length}:\n      DBlob{1145980243, 1, words, length}\n\ndef unmarshal_blob(value: DenseBlob) -> Bytes.Buffer:\n  match value:\n    case DBlob{_, _, words, length}:\n      Bytes.ByteBuffer{words, length}\n\n"; }
    for (yes, suffix, typ, ctor) in [(word, "word", "Word64", "U64"), (signed, "signed", "Signed64", "I64")] {
        if yes { bend += &format!("def marshal_{suffix}(value: Words.{typ}) -> DenseWord:\n  match value:\n    case Words.{ctor}{{hi, lo}}:\n      DWord{{1145980243, 1, hi, lo}}\n\ndef unmarshal_{suffix}(value: DenseWord) -> Words.{typ}:\n  match value:\n    case DWord{{_, _, hi, lo}}:\n      Words.{ctor}{{hi, lo}}\n\n"); }
    }
    fn order(m: &Model, id: usize, seen: &mut BTreeSet<usize>, out: &mut Vec<usize>) { if !seen.insert(id) { return; } for f in m.types[id].variants.iter().flat_map(|v| &v.fields) { if let Shape::Ref(n) = f.shape { order(m, n, seen, out); } } out.push(id); }
    let mut ordered = Vec::new(); let mut seen = BTreeSet::new(); for (_, id) in &m.roots { order(m, *id, &mut seen, &mut ordered); }
    for id in ordered {
        let t = &m.types[id]; let native = format!("{}.{}", modules[&t.module], t.name);
        for marshal in [true, false] {
            let (prefix, from, to) = if marshal { ("marshal", native.clone(), format!("Dense{id}")) } else { ("unmarshal", format!("Dense{id}"), native.clone()) };
            bend += &format!("def {prefix}_{id}(value: {from}) -> {to}:\n  match value:\n");
            for (tag, v) in t.variants.iter().enumerate() {
                let names: Vec<_> = (0..v.fields.len()).map(|n| format!("f{n}")).collect();
                let source_ctor = format!("{}.{}", modules[&t.module], v.name); let dense_ctor = format!("D{id}V{tag}");
                let (pattern, ctor) = if marshal { (format!("{source_ctor}{{{}}}", names.join(", ")), dense_ctor) } else { (format!("{dense_ctor}{{_ , _{}{}}}", if names.is_empty() { "" } else { ", " }, names.join(", ")), source_ctor) };
                let mut args: Vec<_> = v.fields.iter().zip(&names).map(|(f,n)| convert(&f.shape, n, marshal)).collect(); if marshal { args.splice(0..0, ["1145980243".into(), "1".into()]); }
                bend += &format!("    case {pattern}:\n      {ctor}{{{}}}\n", args.join(", "));
            } bend += "\n";
        }
    }
    bend += "type DenseEncoded is Type:\n  DEncoded{marker0: U32, marker1: U32, words: Array<U32>, length: U32, code: U32}\n\n";
    for (name, id) in &m.roots {
        let typ = format!("Schema.{name}");
        bend += &format!("type Decode_{name} is Type:\n  Decoded_{name}{{value: {typ}}}\n  DecodeFailed_{name}{{code: U32}}\n\ntype DenseResult_{name} is Type:\n  DSuccess_{name}{{marker0: U32, marker1: U32, value: Dense{id}}}\n  DFailure_{name}{{marker0: U32, marker1: U32, code: U32}}\n\ntype DenseInput_{name} is Type:\n  DInput_{name}{{marker0: U32, marker1: U32, words: Array<U32>, length: U32, result: DenseResult_{name}}}\n\n");
        bend += &format!("def result_{name}(value: DenseResult_{name}) -> Decode_{name}:\n  match value:\n    case DSuccess_{name}{{_, _, value}}:\n      Decoded_{name}{{unmarshal_{id}(value)}}\n    case DFailure_{name}{{_, _, code}}:\n      DecodeFailed_{name}{{code}}\n\n");
        bend += &format!("type DenseLease_{name} is Type:\n  DLease_{name}{{marker0: U32, marker1: U32, lease: File, result: DenseResult_{name}}}\n\ndef Native.encode_{name}(words: Array<U32>, length: U32, value: Dense{id}) -> IO(DenseEncoded):\n  import \"./codec.c\"\n  import \"./codec.js\"\n\ndef Native.decode_{name}(words: Array<U32>, length: U32) -> IO(DenseInput_{name}):\n  import \"./codec.c\"\n  import \"./codec.js\"\n\ndef Native.lease_{name}(lease: File, index: U32) -> IO(DenseLease_{name}):\n  import \"./codec.c\"\n  import \"./codec.js\"\n\ndef lease_result_{name}(result: DenseLease_{name}) -> File & Decode_{name}:\n  match result:\n    case DLease_{name}{{_, _, lease, decoded}}:\n      (lease, result_{name}(decoded))\n\n");
        bend += &format!("def encoded_{name}(result: DenseEncoded) -> Bytes.Buffer & U32:\n  match result:\n    case DEncoded{{_, _, words, length, code}}:\n      (Bytes.ByteBuffer{{words, length}}, code)\n\ndef input_{name}(result: DenseInput_{name}) -> Bytes.Buffer & Decode_{name}:\n  match result:\n    case DInput_{name}{{_, _, words, length, decoded}}:\n      (Bytes.ByteBuffer{{words, length}}, result_{name}(decoded))\n\ndef encode_{name}(buffer: Bytes.Buffer, value: {typ}) -> IO(Bytes.Buffer & U32):\n  match buffer:\n    case Bytes.ByteBuffer{{words, length}}:\n      do IO<Bytes.Buffer & U32>:\n        result : DenseEncoded <- Native.encode_{name}(words, length, marshal_{id}(value))\n        return encoded_{name}(result)\n\ndef decode_{name}(buffer: Bytes.Buffer) -> IO(Bytes.Buffer & Decode_{name}):\n  match buffer:\n    case Bytes.ByteBuffer{{words, length}}:\n      do IO<Bytes.Buffer & Decode_{name}>:\n        result : DenseInput_{name} <- Native.decode_{name}(words, length)\n        return input_{name}(result)\n\ndef decode_lease_{name}(lease: File, index: U32) -> IO(File & Decode_{name}):\n  do IO<File & Decode_{name}>:\n    result : File & DenseResult_{name} <- Native.lease_{name}(lease, index)\n    (lease, decoded) : File & DenseResult_{name} = result\n    return (lease, result_{name}(decoded))\n\n");
    }
    bend = bend.replace("result : File & DenseResult_", "result : DenseLease_");
    for (name, _) in &m.roots {
        bend = bend.replace(&format!("    (lease, decoded) : File & DenseResult_{name} = result\n    return (lease, result_{name}(decoded))"), &format!("    return lease_result_{name}(result)"));
    }
    let validation = bend.clone() + "def main() -> IO(Unit):\n  IO.pure(Unit, Unit{})\n";
    let max_width = m.types.iter().enumerate().map(|(id, _)| native_width(m, &Shape::Ref(id))).max().unwrap();
    let mut c = format!("#include \"byte_buffer.h\"\n#include \"abi.h\"\n#define BC_SLOTS {}\n#define BC_BE {}\n#define BC_WORD {}\n#define BC_BLOB {}\n", max_width + 8, u8::from(m.format == Format::RawBe), if word || signed { "CID(DWord)" } else { "0" }, if blob { "CID(DBlob)" } else { "0" });
    c += "typedef struct { uint32_t cid, count, arity; const uint32_t *fields; } BcVariant;\ntypedef struct { uint32_t count, width; const BcVariant *variants; } BcType;\n";
    let mut jtypes = Vec::new();
    for (id, t) in m.types.iter().enumerate() {
        let mut jvs = Vec::new();
        for (tag,v) in t.variants.iter().enumerate() {
            c += &format!("static const uint32_t bc_f{id}_{tag}[] = {{{}}};\n", if v.fields.is_empty() { "0".into() } else { v.fields.iter().map(|f| kind(&f.shape).to_string()).collect::<Vec<_>>().join(",") });
            jvs.push(json!({"cid":format!("CID(D{id}V{tag})"), "fields":v.fields.iter().map(|f| json!([f.name,kind(&f.shape)])).collect::<Vec<_>>() }));
        }
        c += &format!("static const BcVariant bc_v{id}[] = {{"); for (tag,v) in t.variants.iter().enumerate() { c += &format!("{{CID(D{id}V{tag}),{},{},bc_f{id}_{tag}}},", v.fields.len(), 2 + v.fields.iter().map(|f| native_width(m, &f.shape)).sum::<usize>()); } c += "};\n";
        jtypes.push(jvs);
    }
    c += "static const BcType bc_types[] = {"; for (id,t) in m.types.iter().enumerate() { c += &format!("{{{},{},bc_v{id}}},", t.variants.len(), native_width(m, &Shape::Ref(id))); } c += "};\n";
    c += &format!("#define BC_HAS_LIST {}\n", u8::from(shapes.contains(&&Shape::ListU32)));
    c += include_str!("runtime.c");
    let mut js = format!("const bcBe = {};\nconst bcTypes = {};\n", m.format == Format::RawBe, serde_json::to_string(&jtypes).unwrap());
    for (id,t) in m.types.iter().enumerate() { for tag in 0..t.variants.len() { js = js.replace(&format!("\"CID(D{id}V{tag})\""), &format!("CID(D{id}V{tag})")); } }
    js += &format!("const bcWord = {}; const bcBlob = {};\n", if word || signed { "CID(DWord)" } else { "0" }, if blob { "CID(DBlob)" } else { "0" });
    js += include_str!("runtime.js");
    if !shapes.contains(&&Shape::ListU32) { js = js.replace("CID(Con)", "0").replace("CID(Nil)", "0"); }
    for (name,id) in &m.roots {
        c += &format!("\n#if CID(Native.encode_{name})\nstatic Term bc_encode_{name}(Env e, Term *f, IoWork *w) {{ (void)w; return bc_encode(e,f,{id},CID(DEncoded)); }}\n#endif\n#if CID(Native.decode_{name})\nstatic Term bc_decode_{name}(Env e, Term *f, IoWork *w) {{ (void)w; return bc_decode_buffer(e,f,{id},CID(DInput_{name})); }}\n#endif\n#if CID(Native.lease_{name})\nstatic Term bc_lease_{name}(Env e, Term *f, IoWork *w) {{ (void)w; return bc_decode_lease(e,f,{id},CID(DLease_{name})); }}\n#endif\n");
        js += &format!("\nio_eff(CID(Native.encode_{name}), (words,length,value) => bcEncode(words,length,value,{id},CID(DEncoded)));\nio_eff(CID(Native.decode_{name}), (words,length) => bcDecode(words,length,{id},CID(DInput_{name}),CID(DSuccess_{name}),CID(DFailure_{name})));\nio_eff(CID(Native.lease_{name}), (lease,index) => ({{$:CID(DLease_{name}),marker0:1145980243,marker1:1,lease,result:{{$:CID(DFailure_{name}),marker0:1145980243,marker1:1,code:8}}}}));\n");
    }
    c += "static void __attribute__((constructor)) bc_register(void) {\n  bc_layout();\n"; for (name,id) in &m.roots { c += &format!("bc_arity(CID(DEncoded),5);\nbc_arity(CID(DSuccess_{name}),2+bc_types[{id}].width); bc_arity(CID(DFailure_{name}),3);\nbc_arity(CID(DInput_{name}),7+bc_types[{id}].width); bc_arity(CID(DLease_{name}),6+bc_types[{id}].width);\n#if CID(Native.encode_{name})\nio_eff(CID(Native.encode_{name}),bc_encode_{name});\n#endif\n#if CID(Native.decode_{name})\nio_eff(CID(Native.decode_{name}),bc_decode_{name});\n#endif\n#if CID(Native.lease_{name})\nio_eff(CID(Native.lease_{name}),bc_lease_{name});\n#endif\n"); } c += "}\n";
    let contract = json!({"version":1,"format":if m.format == Format::Bsatn {"bsatn"} else {"raw-be"},"max_bytes":MAX_BYTES,"max_nodes":MAX_NODES,"max_depth":MAX_DEPTH,"roots":m.roots,"types":m.types.iter().map(|t| json!({"name":t.name,"module":relative(m.source.parent().unwrap(),&t.module),"variants":t.variants.iter().map(|v| json!({"name":v.name,"fields":v.fields.iter().map(|f| json!({"name":f.name,"shape":format!("{:?}",f.shape)})).collect::<Vec<_>>()})).collect::<Vec<_>>()})).collect::<Vec<_>>(),"errors":{"shape":1,"bound":2,"truncated":3,"tag":4,"utf8":5,"trailing":6,"array_count":7,"lease":8},"native_headers":["byte_buffer.h","abi.h"],"compiler":"Bend 2.0.40","array_count":"nonzero power of two","lease_js":"unsupported; retains lease and returns code 8"});
    let mut contract = contract;
    contract["native_layout"] = json!({
        "marker":1145980243u32,"version":1,"max_inline_slots":248,
        "types":m.types.iter().enumerate().map(|(id,t)| json!({
            "inline_width":native_width(m,&Shape::Ref(id)),
            "variants":t.variants.iter().map(|v| {
                let mut offset=2; let offsets:Vec<_>=v.fields.iter().map(|f| { let start=offset; offset+=native_width(m,&f.shape); start }).collect();
                json!({"payload_arity":offset,"field_offsets":offsets})
            }).collect::<Vec<_>>()
        })).collect::<Vec<_>>()
    });
    Outputs { bend, c, js, contract:serde_json::to_string_pretty(&contract).unwrap()+"\n", validation }
}
pub fn write_outputs(out: &Path, outputs: &Outputs) -> std::io::Result<()> {
    fs::create_dir_all(out)?;
    for (name, text) in [("codec.bend", &outputs.bend), ("codec.c", &outputs.c), ("codec.js", &outputs.js), ("contract.json", &outputs.contract)] { fs::write(out.join(name), text)?; }
    Ok(())
}
