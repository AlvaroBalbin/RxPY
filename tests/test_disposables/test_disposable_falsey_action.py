"""AI-assisted regression for falsey disposable cleanup callbacks."""

from reactivex.disposable import Disposable


def test_disposable_invokes_falsey_action_once() -> None:
    class Cleanup:
        def __init__(self) -> None:
            self.calls = 0

        def __bool__(self) -> bool:
            return False

        def __call__(self) -> None:
            self.calls += 1

    cleanup = Cleanup()
    disposable = Disposable(cleanup)
    assert cleanup.calls == 0
    assert not disposable.is_disposed

    disposable.dispose()
    disposable.dispose()

    assert disposable.is_disposed
    assert cleanup.calls == 1
