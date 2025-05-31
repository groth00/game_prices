use std::{env, error::Error, fs, path::PathBuf};

const PROTO_RELATIVE_DIR: &str = "../../Protobufs/webui";
const OUT_DIR: &str = "../games_proto/src";

const PROTO_FILES: [&str; 10] = [
    "common.proto",
    "common_base.proto",
    "service_achievements.proto",
    "service_steamcharts.proto",
    "service_store.proto",
    "service_storebrowse.proto",
    "service_storequery.proto",
    "service_storetopsellers.proto",
    "service_usernews.proto",
    "service_wishlist.proto",
];

pub fn generate_protos() -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(OUT_DIR)?;

    let mut cfg = prost_build::Config::new();
    cfg.protoc_executable("/opt/homebrew/bin/protoc")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .out_dir(OUT_DIR);

    let proto_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join(PROTO_RELATIVE_DIR);
    let out_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join(OUT_DIR);

    let protos: Vec<_> = PROTO_FILES.iter().map(|p| proto_dir.join(p)).collect();

    for file in &protos {
        println!("cargo:rerun-if-changed={}", file.to_str().unwrap());
    }

    cfg.compile_protos(&protos, &[proto_dir])?;

    let old_path = out_dir.join("_.rs");
    let new_path = out_dir.join("generated.rs");
    println!("writing to {new_path:?}");
    fs::rename(old_path, new_path)?;

    Ok(())
}
