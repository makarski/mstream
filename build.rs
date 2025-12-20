fn main() -> Result<(), Box<dyn std::error::Error>> {
    // compile pubsub libs
    // Enable server code generation for mock gRPC server in tests
    tonic_build::configure()
        .build_client(true)
        .build_server(true) // Enable server generation for mock testing
        .out_dir("src/pubsub/api")
        .compile_protos(
            &[
                "./protos/google/pubsub/v1/schema.proto",
                "./protos/google/pubsub/v1/pubsub.proto",
            ],
            &["protos"],
        )?;

    Ok(())
}
