use bend_codec::{generate, load, write_outputs, Format};
use std::path::PathBuf;
use std::process::Command;

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let source = PathBuf::from(args.next().ok_or("usage: bend-codec schema.bend --out dir --root Type [--root Type] --format bsatn|raw-be")?);
    let mut out = None; let mut roots = Vec::new(); let mut format = Format::Bsatn;
    while let Some(arg) = args.next() {
        let value = args.next().ok_or_else(|| format!("missing value for {arg}"))?;
        match arg.as_str() {
            "--out" => out = Some(PathBuf::from(value)),
            "--root" => roots.push(value),
            "--format" => format = match value.as_str() { "bsatn" => Format::Bsatn, "raw-be" => Format::RawBe, _ => return Err("format must be bsatn or raw-be".into()) },
            _ => return Err(format!("unknown option {arg}").into()),
        }
    }
    let out = out.ok_or("--out is required")?;
    let model = load(&source,&roots,format)?;
    std::fs::create_dir_all(&out)?;
    let out = std::fs::canonicalize(out)?;
    let outputs = generate(&model,&out);
    let validation = out.join("codec_validation.bend");
    std::fs::write(&validation,&outputs.validation)?;
    let bend = std::env::var_os("BEND").unwrap_or_else(|| "bend".into());
    let version = Command::new(&bend).arg("version").output()?;
    if !version.status.success() || !String::from_utf8_lossy(&version.stdout).contains("2.0.40") {
        return Err("native layout requires pinned Bend 2.0.40".into());
    }
    let validation_c = out.join("codec_validation.c");
    let checked = Command::new(bend).arg(&validation).arg("-o").arg(&validation_c).output();
    std::fs::remove_file(validation)?;
    if validation_c.exists() { std::fs::remove_file(validation_c)?; }
    let checked = checked.map_err(|e| format!("native Bend frontend unavailable (set BEND to Bend 2.0.40): {e}"))?;
    if !checked.status.success() { return Err(format!("native Bend frontend rejected schema/adapters:\n{}\n{}",String::from_utf8_lossy(&checked.stdout),String::from_utf8_lossy(&checked.stderr)).into()); }
    write_outputs(&out,&outputs)?;
    Ok(())
}
fn main() { if let Err(e) = run() { eprintln!("bend-codec: {e}"); std::process::exit(1); } }
