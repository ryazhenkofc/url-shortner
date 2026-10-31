import asyncio
from collections import Counter
from datetime import timedelta

import pytest

from app.cache import Entry, SingleFlight, ttl_for, utcnow
from app.codegen import ALPHABET, new_code, validate_alias, validate_url
from app.config import settings
from app.errors import BadRequest, Overloaded
from app.guard import Guard


def test_new_code_shape_and_distribution():
    codes = [new_code() for _ in range(88_000)]
    assert all(len(c) == 7 and set(c) <= set(ALPHABET) for c in codes)
    counts = Counter("".join(codes))
    expected = 88_000 * 7 / 62
    assert all(abs(counts[ch] - expected) < expected * 0.05 for ch in ALPHABET)


@pytest.mark.parametrize("alias", ["ab", "a" * 11, "bad alias", "ab$", "api", "ADMIN", "metrics"])
def test_alias_rejected(alias):
    with pytest.raises(BadRequest):
        validate_alias(alias)


@pytest.mark.parametrize("alias", ["abc", "a" * 10, "my_link-1"])
def test_alias_accepted(alias):
    validate_alias(alias)


@pytest.mark.parametrize("url", ["ftp://a.com/x", "javascript:alert(1)", "http:///path", "https://a.com/" + "x" * 2040,
                                 "http://short.test/x", "https://evil.com/x", "http://[bad/x", "https://a.com/\x00b",
                                 "https://a.com/\nb", "https://a.com/\ud800"])
def test_url_rejected(url):
    with pytest.raises(BadRequest):
        validate_url(url, "short.test", {"evil.com"})


def test_url_accepted():
    validate_url("https://example.com/a?b=1", "short.test", set())


async def test_single_flight_runs_once_and_shares_result():
    sf, calls = SingleFlight(), 0

    async def fn():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return "v"

    results = await asyncio.gather(*(sf.do("k", fn) for _ in range(100)))
    assert calls == 1 and set(results) == {"v"}


async def test_single_flight_error_reaches_all_waiters():
    sf = SingleFlight()

    async def fn():
        await asyncio.sleep(0.05)
        raise ValueError("boom")

    results = await asyncio.gather(*(sf.do("k", fn) for _ in range(10)), return_exceptions=True)
    assert all(isinstance(r, ValueError) for r in results)


async def test_guard_rejects_when_full():
    g = Guard(1, 0.05)
    async with g.slot():
        with pytest.raises(Overloaded):
            async with g.slot():
                pass


def test_ttl_jitter_within_10_percent():
    ttls = [ttl_for(Entry("ok", "https://a.com")) for _ in range(500)]
    assert min(ttls) >= settings.l2_ttl_s * 0.9 and max(ttls) <= settings.l2_ttl_s * 1.1


def test_ttl_capped_by_expiry():
    e = Entry("ok", "https://a.com", utcnow() + timedelta(hours=1))
    assert ttl_for(e) <= 3600
