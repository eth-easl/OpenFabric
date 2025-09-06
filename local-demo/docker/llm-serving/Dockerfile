ARG OCF_IMAGE
FROM ${OCF_IMAGE} AS build


FROM python:3.11-slim


WORKDIR /app
COPY --from=build /app/ocf-amd64 .

COPY local-demo/mocked-openai-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY local-demo/mocked-openai-api/src/app ./serving-engine


ENTRYPOINT ["/app/ocf-amd64"]