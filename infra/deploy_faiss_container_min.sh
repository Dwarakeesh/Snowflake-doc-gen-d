#!/bin/bash
set -e
# Build and push FAISS container (replace registry info)
docker build -t REPO/faiss-sim:latest -f infra/faiss/Dockerfile .
docker push REPO/faiss-sim:latest
# Register container behind API gateway as needed