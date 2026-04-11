from __future__ import annotations

import asyncio
import logging
from typing import Any

import jwt
from jwt import PyJWKClient
from jwt.exceptions import InvalidTokenError

logger = logging.getLogger(__name__)

GOOGLE_JWKS_URI = "https://www.googleapis.com/oauth2/v3/certs"
GOOGLE_ISSUERS: frozenset[str] = frozenset(
    {"accounts.google.com", "https://accounts.google.com"}
)


class InvalidGoogleToken(Exception):
    """Raised when a Google OAuth ID token fails validation."""


class GoogleJwtValidator:
    """Validates Google OAuth ID tokens via JWKS.

    Holds a cached ``PyJWKClient``; share one instance per process.
    Google rotates signing keys roughly daily, so the cache means JWKS
    is fetched at most once per rotation per process.
    """

    def __init__(
        self,
        *,
        client_id: str,
        jwks_client: PyJWKClient | None = None,
        jwks_uri: str = GOOGLE_JWKS_URI,
        leeway_seconds: int = 60,
    ) -> None:
        if not client_id:
            raise ValueError("GoogleJwtValidator requires a non-empty client_id")
        self._client_id = client_id
        self._leeway_seconds = leeway_seconds
        self._jwks_client = jwks_client or PyJWKClient(
            jwks_uri,
            cache_keys=True,
            max_cached_keys=16,
        )

    async def validate(self, token: str) -> str:
        """Validate a Google ID token and return the verified lowercase email.

        Raises:
            InvalidGoogleToken: if the token is malformed, expired, tampered,
                signed with an unknown key, has an unexpected issuer/audience,
                or lacks a verified email claim.
        """
        if not token:
            raise InvalidGoogleToken("empty token")

        try:
            signing_key = await asyncio.to_thread(
                self._jwks_client.get_signing_key_from_jwt, token
            )
        except InvalidTokenError as exc:
            raise InvalidGoogleToken(f"jwks resolution failed: {exc}") from exc
        except Exception as exc:
            raise InvalidGoogleToken(f"jwks client error: {exc}") from exc

        try:
            claims: dict[str, Any] = jwt.decode(
                token,
                signing_key.key,
                algorithms=["RS256"],
                audience=self._client_id,
                issuer=list(GOOGLE_ISSUERS),
                leeway=self._leeway_seconds,
                options={
                    "require": ["exp", "iat", "iss", "aud", "sub"],
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_iat": True,
                    "verify_iss": True,
                    "verify_aud": True,
                },
            )
        except InvalidTokenError as exc:
            raise InvalidGoogleToken(f"token validation failed: {exc}") from exc

        email = claims.get("email")
        if not isinstance(email, str) or not email:
            raise InvalidGoogleToken("token missing email claim")

        if not claims.get("email_verified", False):
            raise InvalidGoogleToken("token email is not verified")

        return email.lower()
