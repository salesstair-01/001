// Generates the gRPC code from proto/ without protoc: protox compiles the
// .proto files, tonic-build generates the Rust code (the KV server, and the
// directory client the node registers with).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kv = protox::compile(["../proto/s2z/kv/kv.proto"], ["../proto"])?;
    tonic_build::configure().build_client(false).compile_fds(kv)?;
    let dir = protox::compile(["../proto/s2z/directory/directory.proto"], ["../proto"])?;
    tonic_build::configure().build_server(false).compile_fds(dir)?;
    println!("cargo:rerun-if-changed=../proto");
    Ok(())
}
