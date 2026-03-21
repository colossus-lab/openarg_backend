from __future__ import annotations

import os
import tomllib
from enum import StrEnum
from pathlib import Path
from typing import Any


class ValidEnvs(StrEnum):
    LOCAL = "local"
    DEV = "dev"
    PROD = "prod"
    TEST = "test"


def get_current_env() -> ValidEnvs:
    env = os.getenv("APP_ENV", "local")
    try:
        return ValidEnvs(env)
    except ValueError as err:
        msg = f"Invalid APP_ENV: {env}. Must be one of {[e.value for e in ValidEnvs]}"
        raise ValueError(msg) from err


def _find_config_dir() -> Path:
    config_dir = Path(__file__).resolve().parents[4] / "config"
    if not config_dir.exists():
        raise FileNotFoundError(f"Config directory not found: {config_dir}")
    return config_dir


def _read_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "rb") as f:
        return tomllib.load(f)


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = base.copy()
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_full_config(env: ValidEnvs | None = None) -> dict[str, Any]:
    if env is None:
        env = get_current_env()

    config_dir = _find_config_dir()
    env_dir = config_dir / env.value

    config = _read_toml(env_dir / "config.toml")
    secrets = _read_toml(env_dir / ".secrets.toml")

    return _merge_dicts(config, secrets)
