"""Embedding adapter using AWS Bedrock Cohere Embed Multilingual v3."""

from __future__ import annotations

import asyncio
import json
import logging

import boto3
from botocore.config import Config

from app.domain.ports.llm.llm_provider import IEmbeddingProvider

logger = logging.getLogger(__name__)

# An embedding of one short query takes ~0.1-0.3 s. botocore's defaults
# (connect and read timeouts of 60 s, "legacy" retries) let one slow or
# dropped connection hold a search for a minute: `buscar_datasets` measured
# p50 1.4 s, p90 9.2 s and a maximum of 45 s in prod (api_usage, 2-3 Oct)
# while its SQL took 2.5 ms inside the index. "adaptive" retries back off on
# throttling instead of hammering, and three attempts in all of 5 s each
# bound the worst case at ~15 s instead of minutes (`total_max_attempts`:
# botocore's `max_attempts` counts only the retries).
_CONNECT_TIMEOUT_S = 3
_READ_TIMEOUT_S = 5
_MAX_ATTEMPTS = 3
# One client per process serves every request (see the APP scope in
# provider_registry); the default pool of 10 connections would make the
# eleventh concurrent search open and drop a connection of its own.
_MAX_POOL_CONNECTIONS = 32


class BedrockEmbeddingAdapter(IEmbeddingProvider):
    """IEmbeddingProvider implementation backed by Cohere Embed Multilingual v3 on AWS Bedrock.

    Uses boto3 with default AWS credentials (env vars or instance profile).
    Produces 1024-dimensional embeddings by default. The boto3 client is
    thread-safe and keeps its connections alive, so one instance should be
    shared: building one per request paid a new TLS handshake per search.
    """

    def __init__(
        self,
        region: str = "us-east-1",
        model: str = "cohere.embed-multilingual-v3",
        dimensions: int = 1024,
        *,
        connect_timeout: float = _CONNECT_TIMEOUT_S,
        read_timeout: float = _READ_TIMEOUT_S,
        max_attempts: int = _MAX_ATTEMPTS,
    ) -> None:
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=region,
            config=Config(
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                retries={"mode": "adaptive", "total_max_attempts": max_attempts},
                max_pool_connections=_MAX_POOL_CONNECTIONS,
            ),
        )
        self._model = model
        self._dimensions = dimensions

    def _invoke(self, texts: list[str], input_type: str) -> list[list[float]]:
        """Synchronous Bedrock invoke_model call (run in thread for async)."""
        # Cohere via Bedrock validates maxLength=2048 chars before truncation
        texts = [t[:2048] for t in texts]
        body = json.dumps(
            {
                "texts": texts,
                "input_type": input_type,
                "truncate": "END",
            }
        )
        response = self._client.invoke_model(
            modelId=self._model,
            contentType="application/json",
            accept="application/json",
            body=body,
        )
        result = json.loads(response["body"].read())
        return result["embeddings"]

    async def embed(self, text: str) -> list[float]:
        """Embed a single text for query-time similarity search."""
        embeddings = await asyncio.to_thread(self._invoke, [text], "search_query")
        return embeddings[0]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts for document indexing."""
        if not texts:
            return []
        # Cohere Bedrock supports up to 96 texts per call
        batch_size = 96
        all_embeddings: list[list[float]] = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            embeddings = await asyncio.to_thread(self._invoke, batch, "search_document")
            all_embeddings.extend(embeddings)
        return all_embeddings
