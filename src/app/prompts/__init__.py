from functools import lru_cache
from pathlib import Path

PROMPTS_DIR = Path(__file__).parent


@lru_cache(maxsize=None)
def _load_prompt_template(name: str) -> str:
    path = PROMPTS_DIR / f"{name}.txt"
    return path.read_text(encoding="utf-8")


def load_prompt(name: str, **kwargs: str) -> str:
    text = _load_prompt_template(name)
    return text.format(**kwargs) if kwargs else text
