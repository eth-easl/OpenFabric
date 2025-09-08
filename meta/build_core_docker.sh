#!/bin/bash

# Default to latest if no version is specified
VERSION=${1:-latest}

# Validate version
case $VERSION in
    stable|latest|dev)
        ;;
    *)
        echo "Error: Version must be 'stable', 'latest', or 'dev'"
        exit 1
        ;;
esac

# Build and push with the specified version
docker build -f meta/Dockerfile.amd64 -t ghcr.io/researchcomputer/ocf:amd64-$VERSION . && docker push ghcr.io/researchcomputer/ocf:amd64-$VERSION