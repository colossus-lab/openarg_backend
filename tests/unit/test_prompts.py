from unittest.mock import patch

from app.prompts import _load_prompt_template, load_prompt


class TestPromptLoader:
    def setup_method(self) -> None:
        _load_prompt_template.cache_clear()

    def teardown_method(self) -> None:
        _load_prompt_template.cache_clear()

    def test_load_prompt_reuses_cached_template(self):
        with patch("app.prompts.Path.read_text", return_value="Hola {person}") as read_text:
            assert load_prompt("test_prompt", person="Ana") == "Hola Ana"
            assert load_prompt("test_prompt", person="Beto") == "Hola Beto"

        read_text.assert_called_once_with(encoding="utf-8")
