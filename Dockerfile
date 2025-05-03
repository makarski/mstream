FROM rust:1.86.0-bookworm

WORKDIR /app
COPY . .

# Compile + (optionally) strip symbols
RUN cargo build --release && \
    strip target/release/mstream || true

CMD ["./target/release/mstream", "config.toml"]