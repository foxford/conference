## -----------------------------------------------------------------------------
## Build
## -----------------------------------------------------------------------------
FROM clux/muslrust:nightly-2019-11-05 as build-stage

WORKDIR "/build"
COPY . .
RUN cargo build --release

## -----------------------------------------------------------------------------
## Package
## -----------------------------------------------------------------------------
FROM alpine

COPY --from=build-stage "/build/target/x86_64-unknown-linux-musl/release/conference" "/app/conference"

WORKDIR "/app"
ENTRYPOINT ["/app/conference"]
