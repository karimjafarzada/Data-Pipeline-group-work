from types import SimpleNamespace

import pytest

from pipeline import scheduler


def test_daily_scheduler_uses_incremental_and_continues_after_failure(monkeypatch):
    calls = []
    delays = []
    ticks = iter((0, 6, 86400, 86401))

    class StopSchedule(Exception):
        pass

    def run(args):
        calls.append(args)
        return 2 if len(calls) == 1 else 0

    def sleep(delay):
        delays.append(delay)
        if len(delays) == 2:
            raise StopSchedule

    monkeypatch.setattr(scheduler, "main", run)
    monkeypatch.setattr(scheduler, "time", SimpleNamespace(monotonic=lambda: next(ticks), sleep=sleep))

    with pytest.raises(StopSchedule):
        scheduler.schedule(86400)

    assert calls == [["run", "--mode", "incremental"]] * 2
    assert delays == [86394, 86399]
