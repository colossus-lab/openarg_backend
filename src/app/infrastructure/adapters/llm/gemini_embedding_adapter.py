from __future__ import annotations

import asyncio

import google.generativeai as genai

from app.domain.ports.llm.llm_provider import IEmbeddingProvider  # type: ignore[import-not-found]


class GeminiEmbeddingAdapter(IEmbeddingProvider):  # type: ignore[misc]
    def __init__(
        self,
        api_key: str,
        model: str = "gemini-embedding-001",
        dimensions: int = 768,
    ) -> None:
        genai.configure(api_key=api_key)
        self._model = f"models/{model}" if not model.startswith("models/") else model
        self._dimensions = dimensions

    async def embed(self, text: str) -> list[float]:
        result = await asyncio.to_thread(
            genai.embed_content,
            model=self._model,
            content=text,
            output_dimensionality=self._dimensions,
        )
        return result["embedding"]  # type: ignore[no-any-return]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        result = await asyncio.to_thread(
            genai.embed_content,
            model=self._model,
            content=texts,
            output_dimensionality=self._dimensions,
        )
        return result["embedding"]  # type: ignore[no-any-return]
