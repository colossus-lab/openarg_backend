from pathlib import Path

PROMPTS_DIR = Path(__file__).parent


def load_prompt(name: str, **kwargs: str) -> str:
    path = PROMPTS_DIR / f"{name}.txt"
    text = path.read_text(encoding="utf-8")
    return text.format(**kwargs) if kwargs else text
