import asyncio
import pytest

from observatory import rate_limiter as rl


def test_no_sleep_under_limit(monkeypatch):
    """No sleep should occur if we are under the calls/minute limit."""

    async def run():
        current = {"t": 0.0}

        def fake_time():
            return current["t"]

        sleeps = []

        async def fake_sleep(duration):
            sleeps.append(duration)
            # advance time by the sleep duration (simulate real passage of time)
            current["t"] += duration

        monkeypatch.setattr(rl, "time", fake_time)
        monkeypatch.setattr(asyncio, "sleep", fake_sleep)

        limiter = rl.RateLimiter(calls_per_minute=5)

        # Call a few times - should not trigger sleep
        await limiter.wait_if_needed()
        await limiter.wait_if_needed()
        await limiter.wait_if_needed()

        assert sleeps == []
        assert len(limiter.call_times) == 3

    asyncio.run(run())


def test_sleep_when_exceeding_limit(monkeypatch):
    """When we exceed the calls/minute limit the limiter should sleep for the correct duration."""

    async def run():
        current = {"t": 0.0}

        def fake_time():
            return current["t"]

        sleeps = []

        async def fake_sleep(duration):
            sleeps.append(duration)
            current["t"] += duration

        monkeypatch.setattr(rl, "time", fake_time)
        monkeypatch.setattr(asyncio, "sleep", fake_sleep)

        limiter = rl.RateLimiter(calls_per_minute=3)

        # Use up the allowed calls
        await limiter.wait_if_needed()
        await limiter.wait_if_needed()
        await limiter.wait_if_needed()

        # Move time forward a bit (still within 60s window)
        current["t"] = 30.0

        # This call should sleep for ~30 seconds (60 - (30 - oldest_call_time))
        await limiter.wait_if_needed()

        assert len(sleeps) == 1
        assert pytest.approx(sleeps[0], rel=1e-3) == 30.0

    asyncio.run(run())

def test_keyed_rate_limiter_rotation(monkeypatch):
    """When multiple keys are available they should be rotated and each respect their limit."""

    async def run():
        current = {"t": 0.0}

        def fake_time():
            return current["t"]

        sleeps = []

        async def fake_sleep(duration):
            sleeps.append(duration)
            current["t"] += duration

        monkeypatch.setattr(rl, "time", fake_time)
        monkeypatch.setattr(asyncio, "sleep", fake_sleep)

        # Two keys, one call per minute each
        limiter = rl.KeyedRateLimiter(keys=["k1", "k2"], calls_per_minute=1)

        k_first = await limiter.wait_and_get_key()
        k_second = await limiter.wait_and_get_key()

        # Should rotate and use different keys without sleeping
        assert k_first != k_second
        assert sleeps == []

        # Third call should require waiting (~60 seconds) because both keys used their quota
        await limiter.wait_and_get_key()

        assert len(sleeps) == 1
        assert pytest.approx(sleeps[0], rel=1e-3) == 60.0

    asyncio.run(run())


def test_keyed_status_reporting(monkeypatch, capsys):
    """Status should report per-key and aggregate usage and be included in the logged message."""

    async def run():
        current = {"t": 0.0}

        def fake_time():
            return current["t"]

        sleeps = []

        async def fake_sleep(duration):
            sleeps.append(duration)
            current["t"] += duration

        monkeypatch.setattr(rl, "time", fake_time)
        monkeypatch.setattr(asyncio, "sleep", fake_sleep)

        limiter = rl.KeyedRateLimiter(keys=["k1", "k2"], calls_per_minute=2)

        # Use up both keys fully (4 calls total)
        for _ in range(4):
            await limiter.wait_and_get_key()

        status = limiter.status()

        assert status["total_used"] == 4
        assert status["total_available"] == 0
        assert status["capacity"] == 4
        assert status["per_key"]["k1"]["used"] == 2
        assert status["per_key"]["k1"]["available"] == 0
        assert status["per_key"]["k2"]["used"] == 2
        assert status["per_key"]["k2"]["available"] == 0

        # Next call should log the 'All keys exhausted' message and sleep
        await limiter.wait_and_get_key()
        assert len(sleeps) == 1

    asyncio.run(run())

    # Confirm the printed message includes the aggregate total_used
    out = capsys.readouterr().out
    assert "total_used=4" in out