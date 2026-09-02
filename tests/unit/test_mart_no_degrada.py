"""Un build no puede degradar la definición de un mart.

Un mart se construye leyendo su YAML del disco del container que agarró la
tarea, así que "este mart está arreglado" no es un hecho del sistema: es una
propiedad del filesystem de cada worker. Un container con imagen vieja
reconstruye con la definición vieja y revierte el dato en silencio.

Pasó: el 2026-09-02 06:00 UTC el barrido corrió sobre workers sin actualizar y
`legisladores_argentina` volvió de 328 a 584 filas — el chat contestó 512
diputados donde hay 256, durante 14 horas. El arreglo del YAML (v0.4.0) era
correcto; lo que faltaba era que no se pudiera deshacer solo.
"""

from __future__ import annotations

from typing import Any

import pytest

from app.infrastructure.celery.tasks.mart_tasks import (
    _definicion_degradada,
    _version_tuple,
)


class _Eng:
    def __init__(self, registrada: Any, *, rompe: bool = False) -> None:
        self.registrada, self.rompe = registrada, rompe

    def connect(self) -> Any:
        if self.rompe:
            raise RuntimeError("db caída")
        return self

    def execute(self, *_: Any, **__: Any) -> Any:
        return self

    def scalar(self) -> Any:
        return self.registrada

    def __enter__(self) -> _Eng:
        return self

    def __exit__(self, *_: Any) -> None:
        return None


@pytest.mark.parametrize(
    ("texto", "espera"),
    [("0.4.0", (0, 4, 0)), ("1.2", (1, 2)), ("", None), (None, None), ("0.4.0-rc1", None)],
)
def test_parseo_de_version(texto: str | None, espera: tuple | None) -> None:
    assert _version_tuple(texto) == espera


def test_rechaza_el_yaml_mas_viejo_que_el_registrado() -> None:
    """El caso real: worker con imagen vieja (0.3.0) contra el registro (0.4.0)."""
    assert _definicion_degradada(_Eng("0.4.0"), "legisladores_argentina", "0.3.0") == "0.4.0"


def test_deja_pasar_la_misma_version() -> None:
    """Reconstruir con la misma definición es la operación normal del barrido."""
    assert _definicion_degradada(_Eng("0.4.0"), "m", "0.4.0") is None


def test_deja_pasar_una_version_mas_nueva() -> None:
    assert _definicion_degradada(_Eng("0.3.0"), "m", "0.4.0") is None


@pytest.mark.parametrize(
    ("registrada", "en_disco"),
    [
        (None, "0.4.0"),  # mart nuevo, sin registro previo
        ("", "0.4.0"),
        ("no-semver", "0.4.0"),  # no se puede comparar
        ("0.4.0", "tampoco"),
        ("0.4.0", None),
    ],
)
def test_ante_la_duda_no_bloquea(registrada: Any, en_disco: Any) -> None:
    """Frenar un build por no saber comparar sería peor que el problema."""
    assert _definicion_degradada(_Eng(registrada), "m", en_disco) is None


def test_si_no_puede_leer_el_registro_no_bloquea() -> None:
    assert _definicion_degradada(_Eng("0.4.0", rompe=True), "m", "0.3.0") is None


def test_build_mart_tiene_escotilla_para_un_rollback_deliberado() -> None:
    """Bajar de versión a propósito tiene que ser posible — pero explícito."""
    import inspect

    from app.infrastructure.celery.tasks.mart_tasks import build_mart

    firma = inspect.signature(build_mart.run if hasattr(build_mart, "run") else build_mart)
    assert "allow_downgrade" in firma.parameters
    assert firma.parameters["allow_downgrade"].default is False


def test_el_rechazo_dice_las_dos_versiones() -> None:
    """Un rechazo sin los dos números manda a alguien a adivinar qué container
    tiene la imagen vieja."""
    import inspect

    from app.infrastructure.celery.tasks import mart_tasks

    src = inspect.getsource(mart_tasks.build_mart)
    assert '"status": "stale_definition"' in src
    assert "yaml_version_on_disk" in src
    assert "yaml_version_registered" in src
