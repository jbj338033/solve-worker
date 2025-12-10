FROM rust:1.91-slim-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3 \
    nodejs \
    default-jdk \
    golang-go \
    rustc \
    libcap-dev \
    libsystemd-dev \
    pkg-config \
    make \
    git \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L https://github.com/JetBrains/kotlin/releases/download/v2.0.0/kotlin-compiler-2.0.0.zip -o /tmp/kotlin.zip \
    && unzip /tmp/kotlin.zip -d /opt \
    && rm /tmp/kotlin.zip \
    && ln -s /opt/kotlinc/bin/kotlinc /usr/bin/kotlinc

RUN git clone https://github.com/ioi/isolate.git /tmp/isolate \
    && cd /tmp/isolate \
    && make isolate \
    && make install \
    && rm -rf /tmp/isolate

RUN mkdir -p /var/local/lib/isolate \
    && echo 'cg_root = /sys/fs/cgroup' >> /usr/local/etc/isolate

WORKDIR /app
COPY --from=builder /app/target/release/solve-worker .

ENV RUST_LOG=info
ENV REDIS_URL=redis://redis/
ENV MAX_BOXES=16

ENTRYPOINT ["./solve-worker"]
