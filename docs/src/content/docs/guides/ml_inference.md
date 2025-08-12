---
title: Machine Learning as a Service
description: A guide in my new Starlight docs site.
---

We run several public workers that provide machine learning inference/training as a service. Under the hood each worker may run different backend, to support different types of machine learning models. We currently run the following backend: 
- **Triteia** (for large generative transformer models).
- **DeepSpeed-MII** (for text-to-image and some other models). 
- **Inferencia** (for other HuggingFace models unsupported).

The endpoint for our public workers starts with `https://api.research.computer/`. For example, the endpoint for Triteia is `https://api.research.computer/triteia/`.

## Triteia

Triteia is an inference engine that supports OpenAI-compatible APIs for large generative transformer models. Simply replace the endpoint with `https://api.research.computer/triteia/` to use Triteia.

### Using the global dispatcher (OpenAI-compatible)

If you are running your own global dispatcher, you can route OpenAI-compatible requests to any registered LLM worker via the `llm` service:

```bash
curl -sS -X POST \
  http://<dispatcher-host>:8092/v1/service/llm/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt2",           
    "messages": [
      {"role": "user", "content": "Say hello"}
    ]
  }'
```

Or with the OpenAI Python client:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://<dispatcher-host>:8092/v1/service/llm/v1",
    api_key="any-value",
)

resp = client.chat.completions.create(
    model="gpt2",  # selects a provider that registered this model
    messages=[{"role": "user", "content": "Say hello"}],
)
print(resp)
```

Notes:
- The dispatcher selects a provider that has registered the requested model (identity group match).
- Long-running AI requests are supported with extended timeouts.

## Inferencia

```python

import requests

response = requests.post(
    url="https://api.research.computer/inferencia/v1/predict",
    json={
        "model_name": "microsoft/deberta-large-mnli",
        "data": [{
            "text": ["You look amazing today,"],
            "top_k": 3,
        }]
    },
)
print(response.json())
```

The expected output is 

```json
{
    'model_name': 'microsoft:deberta-large-mnli', 
    'model_version': 'default', 
    'data': [
        [
            [
                {'label': 'NEUTRAL', 'score': 0.9754309058189392}, {'label': 'CONTRADICTION', 'score': 0.016230667009949684}, {'label': 'ENTAILMENT', 'score': 0.00833841785788536}
            ]
        ]
    ]
}
```