use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");
    let output = env::var("OUT_DIR").unwrap();
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    println!(
        "cargo:rustc-env=CONFLUXION_WS_BUILD_MARKER=CONFLUXION_WS_BUILD:{output}:{stamp}:{}:END",
        std::process::id()
    );
}
