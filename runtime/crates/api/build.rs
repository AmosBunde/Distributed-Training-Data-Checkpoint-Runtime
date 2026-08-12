fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Vendored protoc: no system dependency needed to build this crate.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);

    let proto_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../proto");
    let protos = ["runtime.proto", "checkpoint.proto", "data.proto"].map(|f| proto_dir.join(f));

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &[proto_dir])?;

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    Ok(())
}
