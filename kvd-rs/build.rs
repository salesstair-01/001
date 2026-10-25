// Generates the gRPC code from proto/ without protoc: protox compiles the
// .proto files, tonic-build generates the Rust code.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = ["../proto/s2z/kv/kv.proto"];
    let fds = protox::compile(protos, ["../proto"])?;
    tonic_build::configure().build_client(false).compile_fds(fds)?;
    for p in protos {
        println!("cargo:rerun-if-changed={p}");
    }
    Ok(())
}
