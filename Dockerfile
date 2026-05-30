ARG TARGETARCH
# If TARGETARCH is amd64, result is x86_64. If arm64, result is aarch64.
ARG RUST_ARCH=${TARGETARCH/amd64/x86_64}
ARG RUST_ARCH=${RUST_ARCH/arm64/aarch64}
ARG TARGET=${RUST_ARCH}-unknown-linux-gnu
ARG RUSTFLAGS="-C target-feature=+crt-static"

FROM ghcr.io/profiidev/images/rust-gnu-builder:main@sha256:ef174ceced5263e76cec3fe70748558c69f3049c134425bb0b623b728cb6c5c9 AS backend-planner

ARG TARGET
ARG RUSTFLAGS

COPY backend/Cargo.toml backend/
COPY backend/entity/Cargo.toml backend/entity/
COPY backend/migration/Cargo.toml backend/migration/
COPY ./Cargo.lock ./Cargo.toml ./

RUN \
  --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=/app/target \
  cargo chef prepare --recipe-path recipe.json --bin backend

FROM ghcr.io/profiidev/images/rust-gnu-builder:main@sha256:ef174ceced5263e76cec3fe70748558c69f3049c134425bb0b623b728cb6c5c9 AS backend-builder

ARG TARGET
ARG RUSTFLAGS

COPY --from=backend-planner /app/recipe.json .

RUN \
  --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=/app/target \
  cargo chef cook --release --target $TARGET

COPY backend/Cargo.toml backend/
COPY backend/src backend/src
COPY backend/entity/Cargo.toml backend/entity/
COPY backend/entity/src backend/entity/src
COPY backend/migration/Cargo.toml backend/migration/
COPY backend/migration/src backend/migration/src
COPY ./Cargo.lock ./Cargo.toml ./

RUN \
  --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=/app/target \
  cd backend && cargo build --release --target $TARGET \
  && mv ../target/$TARGET/release/backend ../app

RUN mkdir -p /tmp/data/storage

FROM scratch

ENV DB_URL="sqlite:/data/forgejo-runner-s3-cache.db?mode=rwc"
ENV STORAGE_PATH="/data/storage"

COPY --from=backend-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=backend-builder /tmp/data /data/storage

WORKDIR /app
COPY --from=backend-builder /app/app /usr/local/bin/forgejo-runner-s3-cache

ENTRYPOINT ["forgejo-runner-s3-cache"]
