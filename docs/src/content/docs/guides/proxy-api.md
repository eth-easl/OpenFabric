---
title: Proxy Service API
description: Complete API documentation for the OCF Proxy Service
sidebar_position: 3
---

# OCF Proxy Service API Documentation

## Overview

The OCF Proxy Service is a FastAPI-based application that provides OpenAI-compatible endpoints for accessing AI models through the Open Compute Framework. It includes user authentication, API key management, and usage tracking.

## Base URL

```
http://localhost:8000
```

## Authentication

The API uses two authentication methods:

1. **JWT Token Authentication** - For user management endpoints
2. **API Key Authentication** - For AI model endpoints

### Web3 Wallet Authentication

Users authenticate by signing a message with their Ethereum wallet. The service verifies the signature using EIP-191 standards.

## API Endpoints

### 1. Authentication Endpoints

#### POST /api/auth/connect
Authenticate wallet and receive JWT token.

**Request Body:**
```json
{
  "address": "0x742d35Cc6634C0532925a3b8D4C9db96c4b4Db45",
  "signature": "0x...",
  "chain_id": 1
}
```

**Response:**
```json
{
  "token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "user": {
    "id": "user_id",
    "address": "0x742d35Cc6634C0532925a3b8D4C9db96c4b4Db45",
    "created_at": "2024-01-01T00:00:00Z"
  }
}
```

#### GET /api/user/profile
Get current user profile (requires JWT authentication).

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Response:**
```json
{
  "id": "user_id",
  "address": "0x742d35Cc6634C0532925a3b8D4C9db96c4b4Db45",
  "created_at": "2024-01-01T00:00:00Z"
}
```

### 2. API Key Management

#### POST /api/api-keys
Create a new API key (requires JWT authentication).

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Request Body:**
```json
{
  "name": "My API Key"
}
```

**Response:**
```json
{
  "id": "key_id",
  "name": "My API Key",
  "key": "sk_rc_abcdef123456...",
  "created_at": "2024-01-01T00:00:00Z",
  "last_used": null,
  "is_active": true
}
```

:::note Important
Store the API key securely when returned. It will not be shown again.
:::

#### GET /api/api-keys
List all user API keys (requires JWT authentication).

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Response:**
```json
{
  "apiKeys": [
    {
      "id": "key_id",
      "name": "My API Key",
      "created_at": "2024-01-01T00:00:00Z",
      "last_used": "2024-01-01T12:00:00Z",
      "is_active": true
    }
  ]
}
```

#### DELETE /api/api-keys/{key_id}
Deactivate an API key (requires JWT authentication).

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Response:**
```json
{
  "success": true
}
```

### 3. Models Endpoints

#### GET /api/models
Get available models (requires JWT authentication).

**Headers:**
```
Authorization: Bearer <jwt_token>
```

**Response:**
```json
[
  {
    "id": "llama-2-7b",
    "name": "llama-2-7b",
    "description": "AI model available on 1x RTX 4090",
    "author": "0x...",
    "blockchain": "ethereum",
    "price": "0.1",
    "tags": ["text-generation"],
    "huggingFaceId": null
  }
]
```

#### GET /api/models/public
Get available models without authentication.

**Response:**
```json
[
  {
    "id": "llama-2-7b",
    "name": "llama-2-7b",
    "description": "AI model available on 1x RTX 4090",
    "author": "0x...",
    "blockchain": "ethereum",
    "price": "0.1",
    "tags": ["text-generation"],
    "huggingFaceId": null
  }
]
```

### 4. OpenAI-Compatible Endpoints

#### POST /api/v1/chat/completions
Create chat completion (requires API key authentication).

**Headers:**
```
Authorization: Bearer <api_key>
Content-Type: application/json
```

**Request Body:**
```json
{
  "model": "llama-2-7b",
  "messages": [
    {
      "role": "user",
      "content": "Hello, how are you?"
    }
  ],
  "temperature": 0.7,
  "max_tokens": 2048,
  "stream": false
}
```

**Response:**
```json
{
  "id": "chatcmpl-abc123",
  "object": "chat.completion",
  "created": 1677652288,
  "model": "llama-2-7b",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "Hello! I'm doing well, thank you for asking..."
      },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 10,
    "completion_tokens": 15,
    "total_tokens": 25
  }
}
```

#### POST /api/v1/completions
Create text completion (requires API key authentication).

**Headers:**
```
Authorization: Bearer <api_key>
Content-Type: application/json
```

**Request Body:**
```json
{
  "model": "llama-2-7b",
  "prompt": "The weather today is",
  "max_tokens": 100
}
```

### 5. Legacy OpenAI Endpoints

These endpoints proxy directly to OCF without authentication:

- **POST /v1/chat/completions**
- **POST /v1/completions**
- **POST /v1/embeddings**
- **GET /v1/models**

Same request/response format as OpenAI API.

### 6. Utility Endpoints

#### GET /health
Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "service": "ocf-entry"
}
```

#### GET /
Service information and available endpoints.

**Response:**
```json
{
  "message": "OpenAI Compatible Proxy Service",
  "version": "1.0.0",
  "endpoints": [
    "POST /v1/chat/completions",
    "POST /v1/completions",
    "POST /v1/embeddings",
    "GET /v1/models",
    "POST /api/auth/connect",
    "GET /api/user/profile",
    "POST /api/api-keys",
    "GET /api/api-keys",
    "DELETE /api/api-keys/{key_id}",
    "GET /api/models",
    "GET /api/models/public",
    "POST /api/v1/chat/completions",
    "POST /api/v1/completions"
  ]
}
```

## Error Responses

All endpoints return standard HTTP status codes. Error responses follow this format:

```json
{
  "detail": "Error message description"
}
```

### Common Error Codes

| Status Code | Description |
|-------------|-------------|
| `401` | Authentication required or invalid credentials |
| `404` | Resource not found |
| `422` | Validation error |
| `500` | Internal server error |
| `502` | Bad gateway (upstream service error) |
| `504` | Gateway timeout |

## Usage Flow

### Step 1: Authenticate Wallet
Call `POST /api/auth/connect` with wallet signature to get JWT token.

```javascript
const authResponse = await fetch('/api/auth/connect', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    address: userWalletAddress,
    signature: walletSignature,
    chain_id: 1
  })
});

const { token, user } = await authResponse.json();
```

### Step 2: Create API Key
Use JWT token to call `POST /api/api-keys`.

```javascript
const apiKeyResponse = await fetch('/api/api-keys', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`
  },
  body: JSON.stringify({
    name: 'My App API Key'
  })
});

const { key } = await apiKeyResponse.json();
```

### Step 3: Use AI Models
Use API key to call AI endpoints.

```javascript
const chatResponse = await fetch('/api/v1/chat/completions', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${apiKey}`
  },
  body: JSON.stringify({
    model: 'llama-2-7b',
    messages: [
      { role: 'user', content: 'Hello!' }
    ],
    temperature: 0.7,
    max_tokens: 1000
  })
});

const completion = await chatResponse.json();
```

## Rate Limiting

The service includes rate limiting middleware to prevent abuse. Requests that exceed rate limits will receive a `429 Too Many Requests` response.

## CORS Support

The service allows cross-origin requests from all origins with full CORS support, making it easy to integrate with web applications.

## Environment Variables

The proxy service can be configured with the following environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `OCF_HEAD_URL` | `http://140.238.223.116:8092` | OCF head node URL |
| `SECRET_KEY` | `your-secret-key-here-change-in-production` | JWT signing secret |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | `1440` (24 hours) | JWT token expiration time |

## Security Considerations

- API keys are hashed using bcrypt before storage
- JWT tokens should be stored securely on the client side
- API keys should only be transmitted over HTTPS in production
- The `SECRET_KEY` should be changed from the default in production deployments

## Examples

### React Hook for Authentication

```javascript
import { useState, useCallback } from 'react';

export const useAuth = () => {
  const [user, setUser] = useState(null);
  const [token, setToken] = useState(localStorage.getItem('jwt_token'));

  const connectWallet = useCallback(async (address, signature, chainId) => {
    try {
      const response = await fetch('/api/auth/connect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ address, signature, chain_id: chainId })
      });

      const data = await response.json();
      setToken(data.token);
      setUser(data.user);
      localStorage.setItem('jwt_token', data.token);

      return data;
    } catch (error) {
      console.error('Authentication failed:', error);
      throw error;
    }
  }, []);

  const disconnect = useCallback(() => {
    setUser(null);
    setToken(null);
    localStorage.removeItem('jwt_token');
  }, []);

  return { user, token, connectWallet, disconnect };
};
```

### API Key Management

```javascript
export const apiKeys = {
  create: async (name, token) => {
    const response = await fetch('/api/api-keys', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`
      },
      body: JSON.stringify({ name })
    });
    return response.json();
  },

  list: async (token) => {
    const response = await fetch('/api/api-keys', {
      headers: {
        'Authorization': `Bearer ${token}`
      }
    });
    return response.json();
  },

  delete: async (keyId, token) => {
    const response = await fetch(`/api/api-keys/${keyId}`, {
      method: 'DELETE',
      headers: {
        'Authorization': `Bearer ${token}`
      }
    });
    return response.json();
  }
};
```

This API documentation should provide everything your frontend team needs to integrate with the OCF Proxy Service.