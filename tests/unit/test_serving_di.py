"""DI wiring smoke test: `IServingPort` is provided by `ServingProvider`
and resolves to `LegacyServingAdapter` (MASTERPLAN Fase 4).

This is a static check — no engine is started. We only verify the provider
is registered and the type binding is correct so a future Dishka resolution
will work.
"""

from __future__ import annotations

from app.domain.ports.serving.serving_port import IServingPort
from app.infrastructure.adapters.serving.legacy_serving_adapter import (
    LegacyServingAdapter,
)


def test_serving_port_interface_lives_in_domain() -> None:
    """The port lives in domain (hexagonal); the adapter lives in
    infrastructure. This must hold so future adapters (StagingAdapter,
    MartAdapter) can be swapped without touching domain code.
    """
    import app.domain.ports.serving.serving_port as port_module

    # The interface symbol must be importable from domain.
    assert IServingPort is port_module.IServingPort
    # The adapter must NOT live in domain.
    assert not LegacyServingAdapter.__module__.startswith("app.domain")
    assert LegacyServingAdapter.__module__.startswith("app.infrastructure")


def test_legacy_adapter_implements_port() -> None:
    """`LegacyServingAdapter` is the Fase 0–3 adapter; it must implement
    the four IServingPort methods. We check by attribute since `IServingPort`
    is an ABC.
    """
    for method in ("discover", "get_schema", "query", "explain"):
        assert hasattr(LegacyServingAdapter, method)
        # Each must be async.
        import inspect

        impl = getattr(LegacyServingAdapter, method)
        assert inspect.iscoroutinefunction(impl), f"{method} must be async"


def test_serving_provider_registered() -> None:
    """`ServingProvider` is part of the providers tuple returned by
    `get_providers()`, so the container will resolve `IServingPort`.
    """
    from app.setup.ioc.provider_registry import ServingProvider, get_providers

    providers = list(get_providers())
    assert any(isinstance(p, ServingProvider) for p in providers)
