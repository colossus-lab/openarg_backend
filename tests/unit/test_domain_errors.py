from __future__ import annotations

import pytest

from app.domain.exceptions.base import ApplicationError, DomainError, InfrastructureError
from app.domain.exceptions.error_codes import ErrorCode


class TestApplicationError:
    def test_error_has_http_status(self):
        err = ApplicationError(ErrorCode.DS_NOT_FOUND)
        assert err.http_status == 404

    def test_error_to_dict(self):
        err = ApplicationError(ErrorCode.DS_NOT_FOUND, details={"id": "abc"})
        d = err.to_dict()
        assert d["code"] == "DS_001"
        assert d["details"]["id"] == "abc"

    def test_domain_error_is_application_error(self):
        err = DomainError(ErrorCode.DS_NOT_FOUND)
        assert isinstance(err, ApplicationError)

    def test_infrastructure_error_is_application_error(self):
        err = InfrastructureError(ErrorCode.SR_EMBEDDING_FAILED)
        assert isinstance(err, ApplicationError)

    def test_error_message(self):
        err = ApplicationError(ErrorCode.DS_NOT_FOUND)
        assert str(err) == "Dataset not found"
