from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict


class ModelObject(BaseModel):
    id: str
    object: str


class ModelsListResponse(BaseModel):
    object: str
    data: List[ModelObject]


# Completion
class CompletionRequest(BaseModel):
    model: str
    prompt: Optional[str] = None
    max_tokens: Optional[int] = 16
    temperature: Optional[float] = 1.0


class Choice(BaseModel):
    text: str
    index: int
    logprobs: Optional[Any]
    finish_reason: Optional[str]


class CompletionResponse(BaseModel):
    id: str
    object: str
    created: int
    model: str
    choices: List[Choice]
    usage: Optional[Dict[str, int]]


# Chat
class ChatMessage(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[ChatMessage]
    max_tokens: Optional[int] = 16
    temperature: Optional[float] = 1.0


class ChatChoice(BaseModel):
    index: int
    message: ChatMessage
    finish_reason: Optional[str]


class ChatCompletionResponse(BaseModel):
    id: str
    object: str
    created: int
    model: str
    choices: List[ChatChoice]
    usage: Optional[Dict[str, int]]


# Embeddings
class EmbeddingRequest(BaseModel):
    model: str
    input: List[str]


class EmbeddingData(BaseModel):
    object: str
    embedding: List[float]
    index: int


class EmbeddingResponse(BaseModel):
    object: str
    data: List[EmbeddingData]
    model: str