"""A streaming chunk is a fragment, not an answer — its edges carry meaning.

`_scrub_internal_identifiers` removes leaked `cache_*` names and then tidies
the whitespace those removals leave behind. Written for the assembled answer,
where trimming the ends is obviously right.

M8 (round v46) also started calling it per streaming chunk, so the user would
not see internal table names flash past mid-stream. Correct intent, but an LLM
emits tokens with the space attached to the front — "Entiendo", " que",
" querés" — and the unconditional `.strip()` deleted every one of them. The
browser concatenates chunks verbatim, so it assembled:

    Entiendo que querésverificaresas afirmaciones sobre presupuesto

Reported by a user on staging 2026-07-29, on the answer to the very question
this investigation began with. The content was right and the text was
unreadable — and nothing failed, because every existing test passed whole
sentences with no leading or trailing space.
"""

from __future__ import annotations

from app.application.pipeline.nodes.analyst import _scrub_internal_identifiers


def _stream(chunks: list[str]) -> str:
    """What the browser ends up showing: chunks scrubbed, then concatenated."""
    return "".join(_scrub_internal_identifiers(c) for c in chunks)


class TestStreamingPreservesWordBoundaries:
    def test_the_reported_sentence_survives_streaming(self) -> None:
        chunks = ["Entiendo", " que", " querés", " verificar", " esas", " afirmaciones"]
        assert _stream(chunks) == "Entiendo que querés verificar esas afirmaciones"

    def test_a_lone_space_is_not_swallowed(self) -> None:
        assert _stream(["hola", " ", "mundo"]) == "hola mundo"

    def test_markdown_list_markers_keep_their_space(self) -> None:
        """`-*"texto"*` in the reported output: the space after `-` was eaten."""
        assert _stream(["\n-", " ", '*"¿Cuál es?"*']) == '\n- *"¿Cuál es?"*'

    def test_a_leading_newline_survives(self) -> None:
        assert _stream(["párrafo uno.", "\n\n", "párrafo dos."]) == ("párrafo uno.\n\npárrafo dos.")

    def test_scrubbing_still_happens_mid_stream(self) -> None:
        """The security purpose is not sacrificed to fix the spacing."""
        out = _stream(["Hay", " 661 leyes", " en cache_leyes_sancionadas", " del período"])
        assert "cache_leyes_sancionadas" not in out
        assert "661 leyes" in out
        assert " del período" in out, "the following chunk keeps its separator"

    def test_a_chunk_that_is_entirely_a_leak_does_not_glue_its_neighbours(self) -> None:
        out = _stream(["total", " cache_presupuesto_2025", " correcto"])
        assert "cache_presupuesto_2025" not in out
        assert "totalcorrecto" not in out


class TestAssembledAnswerUnchanged:
    """The whole-answer contract these tests were written for still holds."""

    def test_clean_text_passes_through(self) -> None:
        clean = "El presupuesto 2024 fue de 96 billones de pesos."
        assert _scrub_internal_identifiers(clean) == clean

    def test_a_leak_is_still_removed(self) -> None:
        out = _scrub_internal_identifiers("Hubo 661 leyes (Fuente: cache_leyes_sancionadas).")
        assert "cache_leyes_sancionadas" not in out
        assert "661 leyes" in out

    def test_double_spaces_left_by_a_removal_are_collapsed(self) -> None:
        out = _scrub_internal_identifiers("el total cache_x de 5")
        assert "  " not in out

    def test_empty_stays_empty(self) -> None:
        assert _scrub_internal_identifiers("") == ""
