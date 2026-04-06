from __future__ import annotations

from unittest.mock import AsyncMock

from app.infrastructure.adapters.connectors.sesiones_adapter import SesionesAdapter


def _make_adapter() -> SesionesAdapter:
    return SesionesAdapter(AsyncMock())


class TestSesionesAdapterLocalSearch:
    def test_search_local_uses_term_index_and_returns_top_matches(self):
        adapter = _make_adapter()
        adapter._chunks = [  # noqa: SLF001
            {
                "periodo": 142,
                "reunion": 1,
                "speaker": "Juan Perez",
                "text": "inflacion inflacion presupuesto",
            },
            {
                "periodo": 142,
                "reunion": 2,
                "speaker": "Maria Gomez",
                "text": "inflacion empleo",
            },
            {
                "periodo": 143,
                "reunion": 1,
                "speaker": "Carlos Diaz",
                "text": "salud educacion",
            },
        ]  # noqa: SLF001
        adapter._chunk_term_counts = [  # noqa: SLF001
            {"inflacion": 2, "presupuesto": 1},
            {"inflacion": 1, "empleo": 1},
            {"salud": 1, "educacion": 1},
        ]
        adapter._speaker_lowers = ["juan perez", "maria gomez", "carlos diaz"]  # noqa: SLF001
        adapter._term_index = {  # noqa: SLF001
            "inflacion": {0, 1},
            "presupuesto": {0},
            "empleo": {1},
            "salud": {2},
        }
        adapter._period_index = {142: [0, 1], 143: [2]}  # noqa: SLF001
        adapter._loaded = True  # noqa: SLF001

        results = adapter._search_local("inflacion presupuesto", limit=2)  # noqa: SLF001

        assert results is not None
        assert len(results) == 2
        assert results[0]["reunion"] == 1
        assert results[1]["reunion"] == 2

    def test_search_local_filters_candidates_by_periodo(self):
        adapter = _make_adapter()
        adapter._chunks = [  # noqa: SLF001
            {"periodo": 142, "reunion": 1, "speaker": "Juan Perez", "text": "inflacion"},
            {"periodo": 143, "reunion": 2, "speaker": "Maria Gomez", "text": "inflacion"},
        ]  # noqa: SLF001
        adapter._chunk_term_counts = [{"inflacion": 1}, {"inflacion": 1}]  # noqa: SLF001
        adapter._speaker_lowers = ["juan perez", "maria gomez"]  # noqa: SLF001
        adapter._term_index = {"inflacion": {0, 1}}  # noqa: SLF001
        adapter._period_index = {142: [0], 143: [1]}  # noqa: SLF001
        adapter._loaded = True  # noqa: SLF001

        results = adapter._search_local("inflacion", periodo=143, limit=5)  # noqa: SLF001

        assert results is not None
        assert len(results) == 1
        assert results[0]["periodo"] == 143

    def test_search_local_boosts_matching_orador(self):
        adapter = _make_adapter()
        adapter._chunks = [  # noqa: SLF001
            {"periodo": 142, "reunion": 1, "speaker": "Juan Perez", "text": "inflacion"},
            {"periodo": 142, "reunion": 2, "speaker": "Maria Gomez", "text": "inflacion inflacion"},
        ]  # noqa: SLF001
        adapter._chunk_term_counts = [{"inflacion": 1}, {"inflacion": 2}]  # noqa: SLF001
        adapter._speaker_lowers = ["juan perez", "maria gomez"]  # noqa: SLF001
        adapter._term_index = {"inflacion": {0, 1}}  # noqa: SLF001
        adapter._period_index = {142: [0, 1]}  # noqa: SLF001
        adapter._loaded = True  # noqa: SLF001

        results = adapter._search_local("inflacion", orador="juan", limit=2)  # noqa: SLF001

        assert results is not None
        assert results[0]["speaker"] == "Juan Perez"

    def test_search_local_returns_none_when_no_term_matches(self):
        adapter = _make_adapter()
        adapter._chunks = [  # noqa: SLF001
            {"periodo": 142, "reunion": 1, "speaker": "Juan Perez", "text": "inflacion"},
        ]  # noqa: SLF001
        adapter._chunk_term_counts = [{"inflacion": 1}]  # noqa: SLF001
        adapter._speaker_lowers = ["juan perez"]  # noqa: SLF001
        adapter._term_index = {"inflacion": {0}}  # noqa: SLF001
        adapter._period_index = {142: [0]}  # noqa: SLF001
        adapter._loaded = True  # noqa: SLF001

        results = adapter._search_local("salud", limit=5)  # noqa: SLF001

        assert results is None
