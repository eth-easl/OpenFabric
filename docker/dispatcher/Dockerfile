FROM alpine:edge

ARG VERSION
ARG TARGETPLATFORM

WORKDIR /app

RUN apk --no-cache add ca-certificates tzdata curl && \
    case "$TARGETPLATFORM" in \
      "linux/arm64")  URL="https://github.com/ResearchComputer/OpenComputeFramework/releases/download/${VERSION}/ocf-arm64" ;; \
      *) URL="https://github.com/ResearchComputer/OpenComputeFramework/releases/download/${VERSION}/ocf-amd64" ;; \
    esac && \
    curl -L "$URL" > ocf && \
    chmod +x ocf

ENTRYPOINT ["/app/ocf"]