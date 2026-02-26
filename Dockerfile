FROM rust:1-bookworm AS chef
RUN apt-get update && apt-get install -y --no-install-recommends \
    unzip curl \
    && rm -rf /var/lib/apt/lists/*
# Install protoc with well-known types included
RUN curl -fsSL https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protoc-28.3-linux-x86_64.zip -o protoc.zip \
    && unzip protoc.zip -d /usr/local \
    && rm protoc.zip
RUN cargo install cargo-chef --locked
WORKDIR /build

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/lorawan-multiplexer-converter /usr/bin/lorawan-multiplexer-converter
USER nobody:nogroup
ENTRYPOINT ["/usr/bin/lorawan-multiplexer-converter"]
