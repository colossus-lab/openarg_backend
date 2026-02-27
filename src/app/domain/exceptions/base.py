from __future__ import annotations

from typing import Any

from app.domain.exceptions.error_codes import ErrorCode


class ApplicationError(Exception):
    def __init__(
        self,
        error_code: ErrorCode,
        details: dict[str, Any] | None = None,
        field: str | None = None,
    ):
        self.error_code = error_code
        self.definition = error_code.value
        self.http_status = self.definition.http_status
        self.details = details or {}
        self.field = field
        super().__init__(self.definition.default_message)

    def to_dict(self) -> dict[str, Any]:
        return self.definition.to_dict(details=self.details, field=self.field)


class DomainError(ApplicationError):
    pass


class InfrastructureError(ApplicationError):
    pass
