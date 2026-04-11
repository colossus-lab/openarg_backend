from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.infrastructure.auth import GoogleJwtValidator, InvalidGoogleToken

CLIENT_ID = "openarg-test-client.apps.googleusercontent.com"


@dataclass
class _FakeSigningKey:
    key: Any


class _FakeJwksClient:
    """Returns a single signing key regardless of the kid; tests that exercise
    unknown keys pass ``None`` and raise the same exception PyJWKClient would."""

    def __init__(self, public_key_pem: bytes) -> None:
        self._public_key = serialization.load_pem_public_key(public_key_pem)

    def get_signing_key_from_jwt(self, _token: str) -> _FakeSigningKey:  # noqa: D401
        return _FakeSigningKey(key=self._public_key)


class _ExplodingJwksClient:
    """Simulates PyJWKClient raising when it cannot resolve the kid."""

    def get_signing_key_from_jwt(self, _token: str) -> Any:
        from jwt.exceptions import PyJWKClientError

        raise PyJWKClientError("unable to find signing key")


def _make_key_pair() -> tuple[bytes, bytes]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return private_pem, public_pem


def _sign_token(
    private_pem: bytes,
    *,
    email: str = "user@example.com",
    email_verified: bool = True,
    iss: str = "https://accounts.google.com",
    aud: str = CLIENT_ID,
    exp_offset: int = 3600,
    iat_offset: int = 0,
    sub: str | None = "1234567890",
    extra: dict[str, Any] | None = None,
) -> str:
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": iss,
        "aud": aud,
        "exp": now + exp_offset,
        "iat": now + iat_offset,
        "email": email,
        "email_verified": email_verified,
    }
    if sub is not None:
        claims["sub"] = sub
    if extra:
        claims.update(extra)
    return jwt.encode(claims, private_pem, algorithm="RS256")


@pytest.fixture()
def keypair() -> tuple[bytes, bytes]:
    return _make_key_pair()


@pytest.fixture()
def validator(keypair: tuple[bytes, bytes]) -> GoogleJwtValidator:
    _, public_pem = keypair
    return GoogleJwtValidator(
        client_id=CLIENT_ID,
        jwks_client=_FakeJwksClient(public_pem),  # type: ignore[arg-type]
    )


async def test_validator_accepts_well_formed_token(
    validator: GoogleJwtValidator, keypair: tuple[bytes, bytes]
) -> None:
    private_pem, _ = keypair
    token = _sign_token(private_pem, email="Alice@Example.com")

    email = await validator.validate(token)

    assert email == "alice@example.com"


async def test_validator_rejects_empty_token(validator: GoogleJwtValidator) -> None:
    with pytest.raises(InvalidGoogleToken):
        await validator.validate("")


async def test_validator_rejects_expired_token(
    validator: GoogleJwtValidator, keypair: tuple[bytes, bytes]
) -> None:
    private_pem, _ = keypair
    token = _sign_token(private_pem, exp_offset=-3600)

    with pytest.raises(InvalidGoogleToken):
        await validator.validate(token)


async def test_validator_rejects_wrong_audience(
    validator: GoogleJwtValidator, keypair: tuple[bytes, bytes]
) -> None:
    private_pem, _ = keypair
    token = _sign_token(private_pem, aud="some-other-client.apps.googleusercontent.com")

    with pytest.raises(InvalidGoogleToken):
        await validator.validate(token)


async def test_validator_rejects_wrong_issuer(
    validator: GoogleJwtValidator, keypair: tuple[bytes, bytes]
) -> None:
    private_pem, _ = keypair
    token = _sign_token(private_pem, iss="https://evil.example.com")

    with pytest.raises(InvalidGoogleToken):
        await validator.validate(token)


async def test_validator_accepts_both_google_issuer_forms(
    validator: GoogleJwtValidator, keypair: tuple[bytes, bytes]
) -> None:
    private_pem, _ = keypair
    token_long = _sign_token(private_pem, iss="https://accounts.google.com")
    token_short = _sign_token(private_pem, iss="accounts.google.com")

    assert await validator.validate(token_long) == "user@example.com"
    assert await validator.validate(token_short) == "user@example.com"


async def test_validator_rejects_missing_email(
    validator: GoogleJwtValidator, keypair: tuple[bytes, bytes]
) -> None:
    private_pem, _ = keypair
    token = _sign_token(private_pem, email="")

    with pytest.raises(InvalidGoogleToken):
        await validator.validate(token)


async def test_validator_rejects_unverified_email(
    validator: GoogleJwtValidator, keypair: tuple[bytes, bytes]
) -> None:
    private_pem, _ = keypair
    token = _sign_token(private_pem, email_verified=False)

    with pytest.raises(InvalidGoogleToken):
        await validator.validate(token)


async def test_validator_rejects_tampered_signature(
    validator: GoogleJwtValidator, keypair: tuple[bytes, bytes]
) -> None:
    private_pem, _ = keypair
    token = _sign_token(private_pem)
    # Flip one character of the signature
    header, payload, sig = token.split(".")
    tampered_sig = "A" + sig[1:] if sig[0] != "A" else "B" + sig[1:]
    tampered = f"{header}.{payload}.{tampered_sig}"

    with pytest.raises(InvalidGoogleToken):
        await validator.validate(tampered)


async def test_validator_wraps_jwks_client_errors(keypair: tuple[bytes, bytes]) -> None:
    _, public_pem = keypair
    bad_validator = GoogleJwtValidator(
        client_id=CLIENT_ID,
        jwks_client=_ExplodingJwksClient(),  # type: ignore[arg-type]
    )
    private_pem, _ = keypair
    token = _sign_token(private_pem)

    with pytest.raises(InvalidGoogleToken):
        await bad_validator.validate(token)


async def test_validator_requires_client_id() -> None:
    with pytest.raises(ValueError):
        GoogleJwtValidator(client_id="")
