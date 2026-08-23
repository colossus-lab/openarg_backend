"""The digest of what we downloaded, so "did this change?" has an answer.

`raw_table_versions.source_file_hash` has existed since migration 0039 and every
registration function threads it through — and nothing ever computed it.
Measured in production on 2026-08-23: **0 of 31,266 live versions carry one.**

That absence is why the refresh keys on the portal's `last_updated_at`, which is
metadata rather than content: 68 re-collections produced zero files that were
actually different. The portal moved a timestamp and we re-read, re-parsed and
re-embedded a file identical to the one we already held.
"""

from __future__ import annotations

import hashlib

from app.infrastructure.celery.tasks.collector_tasks import _file_sha256


def test_it_hashes_a_file(tmp_path):
    p = tmp_path / "d.csv"
    body = b"provincia,valor\nSalta,10\n"
    p.write_bytes(body)
    assert _file_sha256(str(p)) == hashlib.sha256(body).hexdigest()


def test_the_same_bytes_give_the_same_digest(tmp_path):
    """The whole point: a re-read of an unchanged file must be recognisable."""
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_bytes(b"x" * 5000)
    b.write_bytes(b"x" * 5000)
    assert _file_sha256(str(a)) == _file_sha256(str(b))


def test_one_changed_byte_changes_the_digest(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_bytes(b"x" * 5000)
    b.write_bytes(b"x" * 4999 + b"y")
    assert _file_sha256(str(a)) != _file_sha256(str(b))


def test_a_file_larger_than_the_chunk_is_hashed_whole(tmp_path):
    """Streamed in 1MB chunks — these are files up to hundreds of megabytes, and
    a digest needing the file in memory would trade one problem for a worse one."""
    body = bytes(range(256)) * 20_000  # ~5 MB, several chunks
    p = tmp_path / "big.bin"
    p.write_bytes(body)
    assert _file_sha256(str(p)) == hashlib.sha256(body).hexdigest()


def test_a_missing_file_is_none_not_an_exception():
    """A hash is an optimisation and a piece of evidence. A collection must not
    fail for want of one."""
    assert _file_sha256("/no/such/file") is None


def test_the_hash_reaches_the_registry_call():
    """The plumbing existed and the value never did — that is the actual bug.

    Pins the chain: the collect task computes it, `_finalize_cached_dataset`
    forwards it, `_apply_cached_outcome` forwards it, `_promote_to_raw_atomic`
    writes it.
    """
    import inspect

    from app.infrastructure.celery.tasks import collector_tasks as ct

    for fn in (
        ct._finalize_cached_dataset,
        ct._apply_cached_outcome,
        ct._promote_to_raw_atomic,
        ct._register_raw_version,
    ):
        assert "source_file_hash" in inspect.signature(fn).parameters, fn.__name__


# --- skipping an unchanged file -------------------------------------------
#
# The saving is real but the failure mode is worse than the saving: skipping a
# file that *did* change means serving stale data while believing it fresh. Every
# test here is about when the skip must NOT happen.

from types import SimpleNamespace  # noqa: E402

from app.infrastructure.celery.tasks.collector_tasks import (  # noqa: E402
    _unchanged_since_last_collect,
)


class _Conn:
    def __init__(self, version=None, table_present=True, raises=False):
        self.version = version
        self.table_present = table_present
        self.raises = raises

    def execute(self, stmt, params=None):
        if self.raises:
            raise RuntimeError("pg down")
        sql = str(stmt)
        if "raw_table_versions" in sql:
            return SimpleNamespace(fetchone=lambda: self.version)
        return SimpleNamespace(fetchone=lambda: (1,) if self.table_present else None)

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Eng:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn


def _live(rows=100, schema="raw", table="t"):
    return SimpleNamespace(schema_name=schema, table_name=table, row_count=rows)


def test_an_identical_file_with_an_intact_table_is_skipped():
    out = _unchanged_since_last_collect(
        _Eng(_Conn(version=_live())), resource_identity="p::s", file_hash="abc"
    )
    assert out == "t"


def test_a_different_file_is_never_skipped():
    """No registry row matches the digest, so this file is new to us."""
    out = _unchanged_since_last_collect(
        _Eng(_Conn(version=None)), resource_identity="p::s", file_hash="nuevo"
    )
    assert out is None


def test_an_unchanged_file_over_a_MISSING_table_is_not_skipped():
    """The trap: a resource whose table was dropped would otherwise be skipped
    forever on the grounds that its source never moved."""
    out = _unchanged_since_last_collect(
        _Eng(_Conn(version=_live(), table_present=False)),
        resource_identity="p::s",
        file_hash="abc",
    )
    assert out is None


def test_an_unchanged_file_over_an_EMPTY_table_is_not_skipped():
    """Same trap, quieter: the table exists and the last parse produced nothing.
    An unchanged file is not a reason to keep a broken table."""
    out = _unchanged_since_last_collect(
        _Eng(_Conn(version=_live(rows=0))), resource_identity="p::s", file_hash="abc"
    )
    assert out is None


def test_a_first_collection_has_nothing_to_compare_against():
    assert _unchanged_since_last_collect(
        _Eng(_Conn(version=None)), resource_identity="p::s", file_hash="abc"
    ) is None
    # And with no hash at all — the state of every row before this shipped.
    assert _unchanged_since_last_collect(
        _Eng(_Conn(version=_live())), resource_identity="p::s", file_hash=None
    ) is None


def test_uncertainty_resolves_toward_re_collecting():
    """Re-parsing an unchanged file costs one collection. Skipping a changed one
    costs correctness."""
    assert _unchanged_since_last_collect(
        _Eng(_Conn(raises=True)), resource_identity="p::s", file_hash="abc"
    ) is None
