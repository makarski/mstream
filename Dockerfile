FROM rust:1.91-bookworm

WORKDIR /app
COPY . .

# Install protobuf compiler and build dependencies
RUN apt-get update && apt-get install -y protobuf-compiler cmake libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

# Compile, strip symbols, and move binary to WORKDIR
RUN cargo build --release && \
    (strip target/release/mstream || true) && \
    mv target/release/mstream .

ENV RUST_LOG=info

# The app looks for "mstream-config.toml" in the current working directory
CMD ["./mstream"]
