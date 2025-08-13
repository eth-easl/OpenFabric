import os
import subprocess

command = """curl http://148.187.108.172:8092/v1/p2p/<address>/v1/_service/llm/v1/chat/completions \
  -H 'Authorization: Bearer YOUR_API_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "swissai/apertus3-70b-15T-sft",
    "messages": [
      { "role": "system", "content": "You are a helpful assistant." },
      { "role": "user", "content": "What is the capital of France?" }
    ],
    "temperature": 0.7
  }'"""

address = "QmSNB58JK6TvpWpKqAQMJSmvZbzWLy5Qp9jkT8pNp9cJf5"
cmd = command.replace("<address>", address)
print(cmd)
# Use subprocess to avoid shell interpretation issues
subprocess.run(cmd, shell=True, check=True)