FROM --platform=linux/amd64 alpine:edge

ARG VERSION

WORKDIR /app

RUN case "$TARGETPLATFORM" in \
      "linux/arm64")  URL="https://github.com/ResearchComputer/OpenComputeFramework/releases/download/${{VERSION}}/ocf-arm64" ;; \
      *) URL="https://github.com/ResearchComputer/OpenComputeFramework/releases/download/${{VERSION}}/ocf-amd64" ;; \
    esac && \
    curl -L "$URL" > ocf && \
    chmod +x ocf


RUN apk --no-cache add ca-certificates tzdata

ENTRYPOINT ["/app/ocf"]