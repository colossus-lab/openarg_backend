from __future__ import annotations

import os

import pytest

# Ensure test env
os.environ.setdefault("APP_ENV", "test")


@pytest.fixture
def anyio_backend():
    return "asyncio"
