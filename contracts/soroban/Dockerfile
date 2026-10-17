# syntax=docker/dockerfile:1
#
# Deterministic build of one Soroban contract. The build context is
# contracts/soroban and the contract is picked with --build-arg CONTRACT=<crate>.
#
#   docker buildx build --platform linux/amd64 --target artifact \
#     --build-arg CONTRACT=attestation --output type=local,dest=out \
#     -f contracts/soroban/Dockerfile contracts/soroban
#
# The same commit produces the same <crate>.wasm on every host: the toolchain is
# pinned by the image tag, dependencies come from the committed Cargo.lock
# (--locked), the target platform is fixed, and absolute paths are remapped so
# they are not embedded in the binary. tools/verify_wasm compares the result
# with the code deployed on the network.
FROM rust:1.84.0-slim-bookworm AS build

ARG CONTRACT
ARG SOURCE_DATE_EPOCH=0

ENV CARGO_HOME=/usr/local/cargo \
    CARGO_INCREMENTAL=0 \
    CARGO_TERM_COLOR=never \
    SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH} \
    RUSTFLAGS="--remap-path-prefix=/build=/src --remap-path-prefix=/usr/local/cargo=/cargo --remap-path-prefix=/usr/local/rustup=/rustup"

RUN rustup target add wasm32-unknown-unknown

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY attestation attestation
COPY bridge_adapter bridge_adapter
COPY gas_profile gas_profile

RUN test -n "${CONTRACT}" \
 && cargo build --locked --release --target wasm32-unknown-unknown -p "${CONTRACT}" \
 && mkdir /out \
 && cp "target/wasm32-unknown-unknown/release/${CONTRACT}.wasm" /out/ \
 && cd /out \
 && sha256sum "${CONTRACT}.wasm" > "${CONTRACT}.wasm.sha256"

FROM scratch AS artifact
COPY --from=build /out/ /
