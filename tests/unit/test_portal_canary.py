"""One small question per portal, and what counts as a wrong answer.

Ten portals were found dead in March by someone reading a list of failures by
hand. Nothing was watching, and there was nothing to watch: the collector only
touched a portal when it had work there, so a portal that quietly stopped
serving looked identical to one nobody had asked anything of.
"""

from __future__ import annotations

from unittest.mock import patch

from app.application.quality.portal_canary import probe


class _Resp:
    def __init__(self, status=200, ctype="text/csv", body=b"a,b\n1,2\n"):
        self.status_code = status
        self.headers = {"content-type": ctype}
        self._body = body

    def iter_bytes(self, chunk_size=None):
        if self._body:
            yield self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Client:
    def __init__(self, resp=None, raises=None):
        self._resp = resp
        self._raises = raises

    def stream(self, method, url):
        if self._raises:
            raise self._raises
        return self._resp

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _probe(resp=None, raises=None, **kw):
    with patch("httpx.Client", return_value=_Client(resp, raises)):
        return probe("https://x/y.csv", **kw)


def test_real_data_is_ok():
    assert _probe(_Resp()).verdict == "ok"


def test_a_200_that_returns_a_login_page_is_not_ok():
    """The failure that actually happens, and the one no status check sees.

    85 % of this catalogue's `html_as_data` cases are a Microsoft auth redirect
    served with a 200. That portal is up, unusable, and looks perfectly healthy
    to anything that only reads status codes.
    """
    r = _probe(_Resp(ctype="text/html", body=b"<!DOCTYPE html><html><head>Sign in"))
    assert r.verdict == "serving_html"
    assert "HTML" in r.detail


def test_html_is_fine_when_html_is_what_the_resource_declares():
    r = _probe(_Resp(ctype="text/html", body=b"<html>"), fmt="html")
    assert r.verdict == "ok"


def test_an_error_status_is_unreachable():
    r = _probe(_Resp(status=404))
    assert r.verdict == "unreachable"
    assert "404" in r.detail


def test_a_connection_failure_is_unreachable_and_names_the_reason():
    r = _probe(raises=OSError("Name or service not known"))
    assert r.verdict == "unreachable"
    assert "OSError" in r.detail


def test_an_empty_response_is_unreachable():
    assert _probe(_Resp(body=b"")).verdict == "unreachable"


def test_an_empty_first_chunk_is_tolerated_for_binary_formats():
    """A canary that flagged every zip would cry wolf on every run."""
    for fmt in ("zip", "xlsx", "pdf"):
        assert _probe(_Resp(body=b""), fmt=fmt).verdict == "ok", fmt


def test_a_csv_whose_body_merely_mentions_html_is_still_data():
    """Only the opening bytes are judged; a column called `html_url` is not a
    login page."""
    body = b"id,html_url\n1,https://x\n" + b"z" * 1000
    assert _probe(_Resp(body=body)).verdict == "ok"
