FROM rust:bookworm AS build

WORKDIR /source
COPY . .
RUN cargo build --locked --release -p fault-cli

FROM gcr.io/distroless/cc-debian12:nonroot

LABEL org.opencontainers.image.source="https://github.com/fault-project/fault"
LABEL org.opencontainers.image.description="fault network fault-injection proxy"

COPY --from=build /source/target/release/fault /usr/local/bin/fault

ENTRYPOINT ["/usr/local/bin/fault"]
