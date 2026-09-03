# ================================
# STAGE 1: Build the Go binary
# ================================
FROM golang:alpine AS builder

# Install security certificates & essential build tools
RUN apk add --no-cache ca-certificates git

# Set working directory inside container
WORKDIR /app

# Copy dependency definitions first (enables Docker layer caching)
COPY go.mod ./

# Copy the rest of the source code
COPY . .

# Build a statically linked binary with CGO disabled and optimizations stripped
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /app/log-cache-engine .

# ================================
# STAGE 2: Minimal Runtime Container
# ================================
FROM alpine:latest

# Install CA certificates for TLS/HTTPS outbound support if needed
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary from build stage
COPY --from=builder /app/log-cache-engine .

# Ensure nobody user owns /app so the engine can create and write the WAL file
RUN chown -R nobody:nobody /app

# Expose HTTP Telemetry API port
EXPOSE 8080

# Run as non-root user for security best practices
USER nobody:nobody

# Entrypoint command to start engine
ENTRYPOINT ["./log-cache-engine"]