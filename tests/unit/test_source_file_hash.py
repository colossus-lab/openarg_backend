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
