FROM rust:1.84 AS chef

# Stop if a command fails
RUN set -eux
RUN apt-get update && apt-get install -y protobuf-compiler

# Only fetch crates.io index for used crates
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# cargo-chef will be cached from the second build onwards
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --release --bin etcd-client --features etcd-client-bin

FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/etcd-client /usr/local/bin
ENTRYPOINT ["/usr/local/bin/etcd-client"]
