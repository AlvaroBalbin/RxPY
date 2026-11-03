"""AI-assisted preservation controls for Disposable action initialization."""

import pytest

from reactivex.disposable import CompositeDisposable, Disposable


def test_absent_actions_support_repeated_disposal_and_context_management() -> None:
    for disposable in (Disposable(), Disposable(None), Disposable(action=None)):
        assert not disposable.is_disposed
        with disposable as entered:
            assert entered is disposable
            disposable.dispose()
            assert disposable.is_disposed
        disposable.dispose()


def test_initially_truthy_action_remains_callable_after_becoming_falsey() -> None:
    class Cleanup:
        def __init__(self) -> None:
            self.pending = ["resource"]
            self.calls: list[list[str]] = []

        def __len__(self) -> int:
            return len(self.pending)

        def __call__(self) -> None:
            self.calls.append(self.pending.copy())

    cleanup = Cleanup()
    disposable = Disposable(action=cleanup)
    assert disposable.action is cleanup
    assert cleanup.calls == []
    cleanup.pending.clear()
    assert not cleanup
    disposable.dispose()
    disposable.dispose()
    assert cleanup.calls == [[]]
    assert disposable.is_disposed


def test_reentrant_raising_cleanup_is_already_disposed_and_never_retried() -> None:
    states: list[bool] = []
    error = RuntimeError("cleanup failed")

    def cleanup() -> None:
        states.append(disposable.is_disposed)
        disposable.dispose()
        raise error

    disposable = Disposable(cleanup)
    assert states == []
    with pytest.raises(RuntimeError) as caught:
        disposable.dispose()
    assert caught.value is error
    assert disposable.is_disposed
    disposable.dispose()
    assert states == [True]


def test_nested_composite_preserves_cleanup_order_and_shared_action_once() -> None:
    events: list[str] = []
    first = Disposable(lambda: events.append("first"))
    shared = Disposable(lambda: events.append("shared"))
    last = Disposable(lambda: events.append("last"))
    inner = CompositeDisposable(Disposable(None), first, shared)
    outer = CompositeDisposable(inner, shared, last)

    assert events == []
    outer.dispose()
    inner.dispose()
    shared.dispose()
    outer.dispose()

    assert events == ["first", "shared", "last"]
    assert all(item.is_disposed for item in (first, shared, last, inner, outer))
    assert inner.length == outer.length == 0
