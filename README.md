# Open Compute Framework


## Run Local Demo environment

### Start local demo environment
```bash
docker compose -f local-demo/docker-compose.yml up
```
### Query available service providers
```bash
curl --location 'http://localhost:8092/v1/dnt/table'
```
### Send Prompt
```bash
curl --location 'http://localhost:8092/v1/service/llm/v1/chat/completions' \
--header 'Content-Type: application/json' \
--data '{
    "model": "gpt-fake-1",
    "messages": [
      { "role": "system", "content": "You are a helpful assistant." },
      { "role": "user", "content": "Where is the headquarters of the Swiss National Supercomputing Centre?" }
    ],
    "temperature": 0.7
  }'
```

## Run with Docker
```bash
docker run -it -p 8092:8092 -p 43905:43905 --rm --name ocf ghcr.io/xiaozheyao/ocf:dev start --mode standalone
```



