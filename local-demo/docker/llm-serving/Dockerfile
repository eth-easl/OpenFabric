FROM golang:1.23 AS build

WORKDIR /app

COPY . /app
RUN cd /app/src/ && make build-release

FROM python:3.11-slim

WORKDIR /app
COPY --from=build /app/src/build/release/ocf-amd64 .

COPY local-demo/mocked-openai-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY local-demo/mocked-openai-api/src/app ./serving-engine


ENTRYPOINT ["/app/ocf-amd64"]