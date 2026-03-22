"""Embedding adapter using AWS Bedrock Cohere Embed Multilingual v3."""

from __future__ import annotations

import asyncio
import json
import logging

import boto3

from app.domain.ports.llm.llm_provider import IEmbeddingProvider

logger = logging.getLogger(__name__)


class BedrockEmbeddingAdapter(IEmbeddingProvider):
    """IEmbeddingProvider implementation backed by Cohere Embed Multilingual v3 on AWS Bedrock.

    Uses boto3 with default AWS credentials (env vars or instance profile).
    Produces 1024-dimensional embeddings by default.
    """

    def __init__(
        self,
        region: str = "us-east-1",
        model: str = "cohere.embed-multilingual-v3",
        dimensions: int = 1024,
    ) -> None:
        self._client = boto3.client("bedrock-runtime", region_name=region)
        self._model = model
        self._dimensions = dimensions

    def _invoke(self, texts: list[str], input_type: str) -> list[list[float]]:
        """Synchronous Bedrock invoke_model call (run in thread for async)."""
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
