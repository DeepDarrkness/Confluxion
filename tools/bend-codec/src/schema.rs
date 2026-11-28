use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

pub const MAX_BYTES: usize = 1 << 20;
pub const MAX_NODES: usize = 1 << 18;
pub const MAX_DEPTH: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Location { pub file: PathBuf, pub line: usize, pub column: usize }
#[derive(Clone, Debug)]
pub struct Diagnostic { pub location: Location, pub message: String }
impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}: {}", self.location.file.display(), self.location.line, self.location.column, self.message)
    }
}
impl std::error::Error for Diagnostic {}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format { Bsatn, RawBe }
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shape { U32, Bool, String, ListU32, ArrayU32, Blob, Word64, Signed64, Ref(usize) }
#[derive(Clone, Debug)]
pub struct Field { pub name: String, pub shape: Shape, pub location: Location }
#[derive(Clone, Debug)]
pub struct Variant { pub name: String, pub fields: Vec<Field>, pub location: Location }
#[derive(Clone, Debug)]
pub struct TypeDef { pub name: String, pub module: PathBuf, pub variants: Vec<Variant>, pub location: Location }
#[derive(Clone, Debug)]
pub struct Model { pub source: PathBuf, pub types: Vec<TypeDef>, pub roots: Vec<(String, usize)>, pub format: Format, pub bytes_module: PathBuf, pub words_module: Option<PathBuf> }

#[derive(Clone)]
struct Token { text: String, loc: Location }
#[derive(Clone)]
struct Expr { name: String, args: Vec<Expr>, loc: Location }
#[derive(Clone)]
struct Decl { name: String, variants: Vec<(String, Vec<(String, Expr)>, Location)>, loc: Location, unsupported: Option<String> }
#[derive(Default)]
struct Module { imports: BTreeMap<String, PathBuf>, decls: BTreeMap<String, Decl> }
fn err(loc: &Location, message: impl Into<String>) -> Diagnostic { Diagnostic { location: loc.clone(), message: message.into() } }
fn tokenize(file: &Path, source: &str) -> Result<Vec<Token>, Diagnostic> {
    let mut out = Vec::new();
    for (line, text) in source.lines().enumerate() {
        let mut chars = text.char_indices().peekable();
        while let Some((col, c)) = chars.next() {
            if c == '#' { break; }
            if c.is_whitespace() { continue; }
            let loc = Location { file: file.into(), line: line + 1, column: col + 1 };
            let mut value = c.to_string();
            if c == '"' || c == '\'' {
                let mut closed = false;
                while let Some((_, next)) = chars.next() {
                    value.push(next);
                    if next == '\\' { if let Some((_, escaped)) = chars.next() { value.push(escaped); } }
                    else if next == c { closed = true; break; }
                }
                if !closed { return Err(err(&loc, "unterminated string or character")); }
            } else if c.is_alphanumeric() || c == '_' || c == '.' || c == '/' {
                while let Some((_, next)) = chars.peek() {
                    if next.is_alphanumeric() || "_./-".contains(*next) { value.push(*next); chars.next(); } else { break; }
                }
            }
            out.push(Token { text: value, loc });
        }
    }
    Ok(out)
}
struct Parser { ts: Vec<Token>, i: usize }
impl Parser {
    fn peek(&self) -> &str { self.ts.get(self.i).map_or("", |t| t.text.as_str()) }
    fn take(&mut self) -> Result<Token, Diagnostic> {
        let t = self.ts.get(self.i).cloned().ok_or_else(|| err(&self.ts.last().unwrap().loc, "unexpected end of declaration"))?;
        self.i += 1; Ok(t)
    }
    fn want(&mut self, s: &str) -> Result<Token, Diagnostic> {
        let t = self.take()?; if t.text != s { return Err(err(&t.loc, format!("expected {s}, found {}", t.text))); } Ok(t)
    }
    fn expr(&mut self) -> Result<Expr, Diagnostic> {
        let t = self.take()?;
        if !identifier(&t.text) { return Err(err(&t.loc, "unsupported field type expression")); }
        let mut args = Vec::new();
        if self.peek() == "<" {
            self.take()?;
            loop { args.push(self.expr()?); if self.peek() != "," { break; } self.take()?; }
            self.want(">")?;
        }
        Ok(Expr { name: t.text, args, loc: t.loc })
    }
}
fn identifier(s: &str) -> bool { s.split('.').all(|p| !p.is_empty() && p.chars().next().unwrap().is_alphabetic() && p.chars().all(|c| c.is_alphanumeric() || c == '_')) }
fn parse(file: &Path, source: &str) -> Result<Module, Diagnostic> {
    let mut p = Parser { ts: tokenize(file, source)?, i: 0 };
    let mut m = Module::default();
    while p.i < p.ts.len() {
        if p.ts[p.i].loc.column != 1 { p.i += 1; continue; }
        match p.peek() {
            "import" => {
                p.take()?; let path = p.take()?;
                if path.text == "Base" { continue; }
                if !path.text.starts_with("./") && !path.text.starts_with("../") { return Err(err(&path.loc, "only local schema imports are supported")); }
                p.want("as")?; let alias = p.take()?;
                let target = file.parent().unwrap().join(&path.text);
                let canonical = fs::canonicalize(&target).map_err(|e| err(&path.loc, format!("import {}: {e}", target.display())))?;
                if m.imports.insert(alias.text, canonical).is_some() { return Err(err(&alias.loc, "duplicate import alias")); }
            }
            "type" => {
                let start = p.take()?; let name = p.take()?;
                let end = (p.i..p.ts.len()).find(|&i| p.ts[i].loc.column == 1).unwrap_or(p.ts.len());
                let mut unsupported = None;
                if p.peek() == "<" { unsupported = Some("generic schema declarations are unsupported".into()); }
                let mut variants = Vec::new();
                if unsupported.is_none() {
                    p.want("is")?; let kind = p.take()?;
                    if kind.text != "Type" && kind.text != "Data" { return Err(err(&kind.loc, "schema kind must be Type or Data")); }
                    p.want(":")?;
                    while p.i < end {
                        let ctor = p.take()?; if !identifier(&ctor.text) { return Err(err(&ctor.loc, "expected constructor name")); }
                        p.want("{")?; let mut fields = Vec::new(); let mut names = BTreeSet::new();
                        while p.peek() != "}" {
                            let field = p.take()?;
                            if !identifier(&field.text) || !names.insert(field.text.clone()) { return Err(err(&field.loc, "invalid or duplicate field name")); }
                            p.want(":")?; fields.push((field.text, p.expr()?));
                            if p.peek() != "," { break; } p.take()?;
                        }
                        p.want("}")?; variants.push((ctor.text, fields, ctor.loc));
                    }
                    if variants.is_empty() || variants.len() > 256 { return Err(err(&start.loc, "a schema sum needs 1..256 variants")); }
                }
                p.i = end;
                let d = Decl { name: name.text.clone(), variants, loc: name.loc.clone(), unsupported };
                if m.decls.insert(name.text, d).is_some() { return Err(err(&name.loc, "duplicate datatype declaration")); }
            }
            _ => { p.i += 1; }
        }
    }
    Ok(m)
}
struct Loader { modules: BTreeMap<PathBuf, Module>, active: BTreeSet<(PathBuf, String)>, ids: BTreeMap<(PathBuf, String), usize>, types: Vec<TypeDef>, bytes: PathBuf, words: Option<PathBuf> }
impl Loader {
    fn module(&mut self, path: &Path) -> Result<(), Diagnostic> {
        if self.modules.contains_key(path) { return Ok(()); }
        let loc = Location { file: path.into(), line: 1, column: 1 };
        let text = fs::read_to_string(path).map_err(|e| err(&loc, e.to_string()))?;
        self.modules.insert(path.into(), parse(path, &text)?); Ok(())
    }
    fn resolve(&mut self, module: &Path, e: &Expr) -> Result<Shape, Diagnostic> {
        if e.args.is_empty() {
            match e.name.as_str() { "U32" => return Ok(Shape::U32), "Bool" => return Ok(Shape::Bool), "String" => return Ok(Shape::String), _ => {} }
        }
        if e.name == "Array" || e.name == "List" {
            if e.args.len() != 1 || e.args[0].name != "U32" || !e.args[0].args.is_empty() { return Err(err(&e.loc, "only Array<U32> and List<U32> are supported")); }
            return Ok(if e.name == "Array" { Shape::ArrayU32 } else { Shape::ListU32 });
        }
        if !e.args.is_empty() { return Err(err(&e.loc, "generic schema instances are unsupported")); }
        self.module(module)?;
        let (target, name) = if let Some((alias, name)) = e.name.split_once('.') {
            (self.modules[module].imports.get(alias).cloned().ok_or_else(|| err(&e.loc, format!("unknown import alias {alias}")))?, name.to_string())
        } else { (module.into(), e.name.clone()) };
        self.module(&target)?;
        let d = self.modules[&target].decls.get(&name).cloned().ok_or_else(|| err(&e.loc, format!("unsupported or undeclared type {}", e.name)))?;
        if target == self.bytes && name == "Buffer" {
            self.check_wrapper(&d, "ByteBuffer", &["Array<U32>", "U32"])?; return Ok(Shape::Blob);
        }
        if self.words.as_ref() == Some(&target) && (name == "Word64" || name == "Signed64") {
            self.check_wrapper(&d, if name == "Word64" { "U64" } else { "I64" }, &["U32", "U32"])?;
            return Ok(if name == "Word64" { Shape::Word64 } else { Shape::Signed64 });
        }
        if let Some(message) = &d.unsupported { return Err(err(&d.loc, message.clone())); }
        let key = (target.clone(), name.clone());
        if self.active.contains(&key) { return Err(err(&e.loc, "recursive schema shapes are unsupported")); }
        if let Some(id) = self.ids.get(&key) { return Ok(Shape::Ref(*id)); }
        if self.active.len() >= MAX_DEPTH || self.types.len() >= 4096 { return Err(err(&e.loc, "schema graph bound exceeded")); }
        self.active.insert(key.clone());
        let id = self.types.len(); self.ids.insert(key.clone(), id);
        self.types.push(TypeDef { name: d.name, module: target.clone(), variants: Vec::new(), location: d.loc });
        let mut variants = Vec::new(); let mut constructors = BTreeSet::new();
        for (name, source_fields, loc) in d.variants {
            if !constructors.insert(name.clone()) { return Err(err(&loc, "duplicate constructor")); }
            let mut fields = Vec::new();
            for (name, expr) in source_fields { let shape = self.resolve(&target, &expr)?; fields.push(Field { name, shape, location: expr.loc }); }
            if fields.len() > 64 { return Err(err(&loc, "constructor exceeds 64 field bound")); }
            variants.push(Variant { name, fields, location: loc });
        }
        self.types[id].variants = variants; self.active.remove(&key); Ok(Shape::Ref(id))
    }
    fn check_wrapper(&self, d: &Decl, ctor: &str, fields: &[&str]) -> Result<(), Diagnostic> {
        fn spelling(e: &Expr) -> String { if e.args.is_empty() { e.name.clone() } else { format!("{}<{}>", e.name, e.args.iter().map(spelling).collect::<Vec<_>>().join(",")) } }
        if d.variants.len() != 1 || d.variants[0].0 != ctor || d.variants[0].1.iter().map(|(_, e)| spelling(e)).collect::<Vec<_>>() != fields { return Err(err(&d.loc, "canonical byte/scalar declaration does not match native contract")); } Ok(())
    }
}
pub fn load(source: &Path, roots: &[String], format: Format) -> Result<Model, Diagnostic> {
    let loc = Location { file: source.into(), line: 1, column: 1 };
    let source = fs::canonicalize(source).map_err(|e| err(&loc, e.to_string()))?;
    if roots.is_empty() { return Err(err(&loc, "at least one root is required")); }
    let project = source.ancestors().find(|p| p.join("src/bytes").is_dir()).or_else(|| source.ancestors().find(|p| p.join("Cargo.toml").is_file())).ok_or_else(|| err(&loc, "schema must be inside a project containing src/bytes"))?;
    let bytes = project.join("src/bytes/main.bend");
    let bytes = fs::canonicalize(&bytes).unwrap_or(bytes);
    let words = fs::canonicalize(project.join("src/bytes/types.bend")).ok();
    let mut l = Loader { modules: BTreeMap::new(), active: BTreeSet::new(), ids: BTreeMap::new(), types: Vec::new(), bytes: bytes.clone(), words: words.clone() };
    let mut requested = roots.to_vec(); requested.sort(); requested.dedup();
    let mut resolved = Vec::new();
    for name in requested {
        if !identifier(&name) || name.contains('.') { return Err(err(&loc, "root must be a local datatype name")); }
        let e = Expr { name: name.clone(), args: Vec::new(), loc: loc.clone() };
        let Shape::Ref(id) = l.resolve(&source, &e)? else { return Err(err(&loc, "root must be a declared product or sum")); };
        resolved.push((name, id));
    }
    let model = Model { source, types: l.types, roots: resolved, format, bytes_module: bytes, words_module: words };
    for (id, t) in model.types.iter().enumerate() {
        if crate::generate::native_width(&model, &Shape::Ref(id)) > 248 {
            return Err(err(&t.location, "native DTO layout exceeds 248 inline slots (255-slot runtime constructor bound including result wrapper)"));
        }
    }
    if format == Format::RawBe { for (_, id) in &model.roots { check_raw(&model, *id, true)?; } }
    Ok(model)
}
fn check_raw(m: &Model, id: usize, final_position: bool) -> Result<(), Diagnostic> {
    for v in &m.types[id].variants {
        for (i, f) in v.fields.iter().enumerate() {
            let tail = final_position && i + 1 == v.fields.len();
            match f.shape { Shape::Blob if !tail => return Err(err(&f.location, "raw-be blob must be the final field on every enclosing path")), Shape::Ref(n) => check_raw(m, n, tail)?, _ => {} }
        }
    }
    Ok(())
}
