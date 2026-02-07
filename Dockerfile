FROM rust:1.92-slim-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ python3 nodejs default-jdk golang-go rustc \
    curl unzip make git \
    autoconf bison flex libprotobuf-dev libnl-route-3-dev \
    libtool pkg-config protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L https://github.com/JetBrains/kotlin/releases/download/v2.0.0/kotlin-compiler-2.0.0.zip -o /tmp/kotlin.zip \
    && unzip /tmp/kotlin.zip -d /opt \
    && rm /tmp/kotlin.zip \
    && ln -s /opt/kotlinc/bin/kotlinc /usr/bin/kotlinc

RUN git clone --branch 3.4 --depth 1 https://github.com/google/nsjail.git /tmp/nsjail \
    && cd /tmp/nsjail \
    && make \
    && cp nsjail /usr/local/bin/ \
    && rm -rf /tmp/nsjail

RUN mkdir -p /var/sandbox

WORKDIR /app
COPY --from=builder /app/target/release/solve-worker .

ENTRYPOINT ["./solve-worker"]
