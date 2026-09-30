"""Admin de Fundadores y créditos (`admin/supporters_router.py`).

El SQL se prueba contra Postgres real antes de desplegar (BEGIN … ROLLBACK en
staging); acá: que todo pida la clave de admin, la validación de lo que se
carga, el 404 de un mail sin cuenta, la idempotencia por referencia y que
cada carga deje un movimiento por tipo con quién la hizo.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.presentation.http.controllers.admin import supporters_router as mod


@pytest.fixture
def conn(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    monkeypatch.setattr(mod, "get_sync_engine", lambda: engine)
    return connection


def _sql(call) -> str:  # type: ignore[no-untyped-def]
    return str(call.args[0])


def test_every_route_requires_the_admin_key() -> None:
    for route in mod.router.routes:
        deps = [d.dependency.__name__ for d in route.dependencies]  # type: ignore[attr-defined]
        assert "verify_admin_key" in deps, route.path  # type: ignore[attr-defined]


def test_a_grant_must_carry_something() -> None:
    with pytest.raises(ValidationError):
        mod.CreditsRequest(email="a@b.com")


def test_negative_credits_are_rejected() -> None:
    with pytest.raises(ValidationError):
        mod.CreditsRequest(email="a@b.com", preguntas=-1)


def test_consumption_cannot_be_granted_by_hand() -> None:
    """`consumo` lo escribe sólo el descuento automático."""
    with pytest.raises(ValidationError):
        mod.CreditsRequest(email="a@b.com", preguntas=1, motivo="consumo")  # type: ignore[arg-type]


def test_unknown_email_is_404(conn: MagicMock) -> None:
    conn.execute.return_value.first.return_value = None
    with pytest.raises(HTTPException) as exc_info:
        mod.upsert_supporter(mod.SupporterRequest(email="nadie@example.com"), x_admin_actor=None)
    assert exc_info.value.status_code == 404


def test_founder_until_is_the_end_of_that_day(conn: MagicMock) -> None:
    conn.execute.return_value.first.return_value = SimpleNamespace(id=uuid4())
    out = mod.upsert_supporter(
        mod.SupporterRequest(email="a@b.com", hasta=date(2027, 3, 31), origen="cortesía"),
        x_admin_actor="dante@example.com",
    )
    assert out["hasta"].startswith("2027-03-31T23:59:59")
    upsert = conn.execute.call_args_list[-1]
    assert "ON CONFLICT (user_id) DO UPDATE" in _sql(upsert)
    assert upsert.args[1]["actor"] == "dante@example.com"


def test_repeated_reference_is_409_and_nothing_is_added(conn: MagicMock) -> None:
    user = SimpleNamespace(id=uuid4())
    already = SimpleNamespace()  # the movement with that reference exists
    conn.execute.return_value.first.side_effect = [user, already]
    with pytest.raises(HTTPException) as exc_info:
        mod.grant_credits(
            mod.CreditsRequest(
                email="a@b.com", preguntas=5, motivo="donacion", referencia="mp-123"
            ),
            x_admin_actor=None,
        )
    assert exc_info.value.status_code == 409
    assert not any("api_credit_balances" in _sql(c) for c in conn.execute.call_args_list)


def test_grant_adds_to_balance_and_logs_one_movement_per_type(conn: MagicMock) -> None:
    user = SimpleNamespace(id=uuid4())
    conn.execute.return_value.first.side_effect = [user]
    conn.execute.return_value.one.return_value = SimpleNamespace(preguntas=5, datos=0)
    out = mod.grant_credits(
        mod.CreditsRequest(email="a@b.com", preguntas=5),
        x_admin_actor="dante@example.com",
    )
    assert out["saldo"] == {"preguntas": 5, "datos": 0}
    movements = [c for c in conn.execute.call_args_list if "api_credit_movements" in _sql(c)]
    assert len(movements) == 1  # sólo preguntas: datos era 0
    assert movements[0].args[1]["t"] == "preguntas"
    assert movements[0].args[1]["a"] == "dante@example.com"
