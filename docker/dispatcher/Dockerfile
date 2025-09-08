
FROM alpine:edge

ARG VERSION

WORKDIR /app

RUN case "$TARGETPLATFORM" in \
      "linux/amd64")  URL="https://github.com/ResearchComputer/OpenComputeFramework/releases/download/${{VERSION}}/ocf-amd64" ;; \
      "linux/arm64")  URL="https://github.com/ResearchComputer/OpenComputeFramework/releases/download/${{VERSION}}/ocf-arm64" ;; \
      *) echo "Unsupported platform: $TARGETPLATFORM" && exit 1 ;; \
    esac && \
    curl -L "$URL" > ocf && \
    chmod +x ocf


RUN apk --no-cache add ca-certificates tzdata

ENTRYPOINT ["/app/ocf"]