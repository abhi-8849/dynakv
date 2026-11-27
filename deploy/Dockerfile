# Build stage: compiles the two static binaries. The Go toolchain is only needed here and is not shipped.
FROM golang:1.27 AS build

# All following commands run from this directory inside the image.
WORKDIR /src

# Copy only the module files first so the dependency download is cached until go.mod or go.sum change.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source tree (files listed in .dockerignore are left out).
COPY . .

# CGO_ENABLED=0 produces fully static binaries that run on the minimal image below; -trimpath removes local paths from them.
RUN CGO_ENABLED=0 go build -trimpath -o /out/dynakv ./cmd/dynakv \
 && CGO_ENABLED=0 go build -trimpath -o /out/dynakv-cli ./cmd/dynakv-cli

# An empty data directory to copy into the final image, so a fresh volume mounted there is writable by the non-root user.
RUN mkdir /out/data

# Final stage: distroless has no shell or package manager, only what a static binary needs. The nonroot tag runs as user 65532.
FROM gcr.io/distroless/static-debian12:nonroot

# The node binary and the operator CLI; the CLI lets you run "docker compose exec node1 /dynakv-cli ..." without installing Go.
COPY --from=build /out/dynakv /dynakv
COPY --from=build /out/dynakv-cli /dynakv-cli

# Data directory for the lsm engine, owned by the user the node runs as.
COPY --from=build --chown=nonroot:nonroot /out/data /data

# Run as the unprivileged user.
USER nonroot:nonroot

# Documentation only: gRPC API, gossip (TCP and UDP) and Prometheus metrics.
EXPOSE 7000 7946 9100

# Start a node; flags are supplied by the compose file or the Kubernetes manifests.
ENTRYPOINT ["/dynakv"]
