# API Reference

The OCF backend exposes a RESTful API for interacting with the node.

## Base URL

Default: `http://localhost:8092/v1`

## Endpoints

### Service Discovery

#### `GET /dnt/table`
Retrieve the current Distributed Hash Table (DHT) routing table or service registry.

**Response:**
```json
{
  "peers": [
    {
      "id": "QmPeerID...",
      "addrs": ["/ip4/127.0.0.1/tcp/4001"]
    }
  ]
}
```

### Compute Services

#### `POST /service/llm/v1/chat/completions`
Send a chat completion request to a connected LLM provider. Compatible with OpenAI API format.

**Request Body:**
```json
{
  "model": "gpt-fake-1",
  "messages": [
    { "role": "user", "content": "Hello!" }
  ]
}
```

### System

#### `GET /system/status`
Get the current status of the node.

**Response:**
```json
{
  "status": "online",
  "version": "0.1.0",
  "peers": 12
}
```
