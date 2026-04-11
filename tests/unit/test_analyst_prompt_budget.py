"""Unit tests for the analyst prompt budget enforcement (DEBT-011 fix).

Covers FR-025a/b/c/d from ``specs/001-query-pipeline/001d-analysis/spec.md``:

- FR-025a: ``ANALYST_PROMPT_MAX_CHARS`` exists as a single module-level
  constant and is honored.
- FR-025b: truncation follows the priority order memory → data → errors,
  and never touches static overhead. Raises ``ValueError`` when the
  static overhead alone is already over budget.
- FR-025c: each truncation leaves a human-readable sentinel naming the
  segment and the drop count.
- FR-025d: a single structured ``WARNING`` log is emitted when
  truncation fires, with budget + final size + per-segment drops.
"""

from __future__ import annotations

import logging

import pytest

from app.application.pipeline.nodes.analyst import (
    ANALYST_PROMPT_MAX_CHARS,
    _enforce_prompt_budget,
    _truncate_segment,
)


def test_budget_constant_exists_and_is_positive():
    """FR-025a: the constant is defined and not trivially zero."""
    assert isinstance(ANALYST_PROMPT_MAX_CHARS, int)
    assert ANALYST_PROMPT_MAX_CHARS > 1_000


def test_under_budget_is_a_noop():
    """A prompt that fits returns the three segments unchanged."""
    data_context = "tiny data"
    errors_block = "no errors"
    memory_ctx = "short memory"
    out_data, out_errors, out_memory = _enforce_prompt_budget(
        static_overhead=500,
        data_context=data_context,
        errors_block=errors_block,
        memory_ctx=memory_ctx,
    )
    assert out_data is data_context
    assert out_errors is errors_block
    assert out_memory is memory_ctx


def test_truncation_priority_memory_first(caplog):
    """FR-025b: memory_ctx is the first segment dropped when over budget."""
    # Pick sizes so that dropping memory alone is enough to fit.
    # static 1000 + budget headroom means available ≈ MAX - 1000.
    available = ANALYST_PROMPT_MAX_CHARS - 1000
    # We'll make memory overflow by 5000 chars; data + errors fit under.
    data_context = "D" * (available // 2 - 2500)
    errors_block = "E" * 500
    memory_ctx = "M" * (available // 2 + 7000)  # the only overflow

    caplog.set_level(logging.WARNING, logger="app.application.pipeline.nodes.analyst")

    out_data, out_errors, out_memory = _enforce_prompt_budget(
        static_overhead=1000,
        data_context=data_context,
        errors_block=errors_block,
        memory_ctx=memory_ctx,
    )
    # data_context and errors_block untouched because memory alone
    # absorbed the overflow.
    assert out_data == data_context
    assert out_errors == errors_block
    # memory_ctx was trimmed AND has a sentinel (FR-025c).
    assert "memory_ctx truncated" in out_memory
    assert len(out_memory) < len(memory_ctx)
    # Final assembled size fits.
    assert 1000 + len(out_data) + len(out_errors) + len(out_memory) <= ANALYST_PROMPT_MAX_CHARS
    # FR-025d: WARNING log emitted with the right shape.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("analyst prompt truncated" in r.getMessage() for r in warnings)


def test_truncation_spills_to_data_when_memory_not_enough(caplog):
    """FR-025b: after memory_ctx is fully dropped, data_context is next."""
    caplog.set_level(logging.WARNING, logger="app.application.pipeline.nodes.analyst")

    # Overflow is larger than memory_ctx, so data_context must also trim.
    data_context = "D" * (ANALYST_PROMPT_MAX_CHARS // 2)
    errors_block = "E" * 1000
    memory_ctx = "M" * 1000  # small — entirely droppable

    total = len(data_context) + len(errors_block) + len(memory_ctx)
    # Force a tight budget via large static overhead.
    static_overhead = ANALYST_PROMPT_MAX_CHARS - total + 5000  # 5000 over budget

    out_data, out_errors, out_memory = _enforce_prompt_budget(
        static_overhead=static_overhead,
        data_context=data_context,
        errors_block=errors_block,
        memory_ctx=memory_ctx,
    )
    # memory_ctx shrank (FR-025b priority 1).
    assert len(out_memory) < len(memory_ctx)
    # data_context also shrank (FR-025b priority 2).
    assert len(out_data) < len(data_context)
    assert "data_context truncated" in out_data
    # errors_block untouched because data was enough.
    assert out_errors == errors_block
    # Final size fits.
    final = static_overhead + len(out_data) + len(out_errors) + len(out_memory)
    assert final <= ANALYST_PROMPT_MAX_CHARS


def test_static_overhead_over_budget_raises():
    """FR-025b: if static alone is over budget, ValueError is raised.

    This is a programming bug in the prompt template, not a runtime
    condition, and must not be silently accepted.
    """
    with pytest.raises(ValueError, match="static prompt overhead"):
        _enforce_prompt_budget(
            static_overhead=ANALYST_PROMPT_MAX_CHARS + 1,
            data_context="",
            errors_block="",
            memory_ctx="",
        )


def test_truncate_segment_sentinel_format():
    """FR-025c: the sentinel names the label, and net reduction meets the target."""
    original = "A" * 1000
    out, dropped = _truncate_segment(
        original, over=400, label="memory_ctx", keep_from="tail"
    )
    # Net reduction must meet (actually exceed by sentinel_headroom) the
    # requested "over" so the caller's budget loop converges.
    assert dropped >= 400
    assert "memory_ctx truncated" in out
    assert "chars dropped" in out
    # Tail-kept means the sentinel sits at the beginning.
    assert out.startswith("[...")
    # And the new segment is strictly shorter than the original.
    assert len(out) < len(original)


def test_truncate_segment_keep_from_head():
    """Head-keeping puts the sentinel at the end — used for data_context."""
    original = "A" * 1000
    out, dropped = _truncate_segment(
        original, over=400, label="data_context", keep_from="head"
    )
    assert dropped >= 400
    assert out.endswith("]")  # sentinel at the end
    assert "data_context truncated" in out
    assert len(out) < len(original)


def test_truncate_segment_under_floor_returns_unchanged():
    """If keep_floor + sentinel overhead prevents shrinking, return unchanged.

    Here the segment is only 100 chars, floor=60 leaves 40 chars to
    drop, but the sentinel itself takes ~47 chars — so any attempt
    actually LENGTHENS the output. The helper detects the non-shrink
    and returns the original untouched so the caller can spill into
    the next priority segment instead of getting a noop that looks
    like progress.
    """
    segment = "A" * 100
    out, dropped = _truncate_segment(
        segment, over=50, label="data_context", keep_from="head", keep_floor=60
    )
    assert dropped == 0
    assert out == segment


def test_truncate_segment_no_overflow_is_noop():
    """over<=0 or empty segment returns unchanged."""
    assert _truncate_segment("hello", over=0, label="x") == ("hello", 0)
    assert _truncate_segment("", over=100, label="x") == ("", 0)
