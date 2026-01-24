use std::env;
use std::path::PathBuf;

fn main() {
    if env::var_os("CARGO_FEATURE_RPC").is_none() {
        return;
    }

    let schema_dir = PathBuf::from("capnp");
    let schema_file = schema_dir.join("log.capnp");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));

    capnpc::CompilerCommand::new()
        .src_prefix(&schema_dir)
        .file(&schema_file)
        .output_path(out_dir.clone())
        .run()
        .expect("Cap'n Proto schema compilation failed");

    let generated_path = out_dir.join("log_capnp.rs");
    if let Ok(contents) = std::fs::read_to_string(&generated_path) {
        let updated = contents.replace(
            "dyn (::capnp::private::capability::ClientHook)",
            "dyn ::capnp::private::capability::ClientHook",
        );
        if updated != contents {
            std::fs::write(&generated_path, updated)
                .expect("Failed to update generated Cap'n Proto code");
        }
    }

    println!("cargo:rerun-if-changed=capnp/log.capnp");
}
