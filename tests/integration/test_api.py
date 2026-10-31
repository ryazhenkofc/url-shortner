import os
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx
import pytest

BASE = os.environ["BASE_URL"]
OWNER = {"Authorization": "Bearer dev-key-123"}
OTHER = {"Authorization": "Bearer dev-key-456"}
URL = "https://example.com/very/long/path"


@pytest.fixture
def c():
    with httpx.Client(base_url=BASE, follow_redirects=False, timeout=10) as client:
        yield client


def alias():
    return "t" + uuid.uuid4().hex[:8]


def create(c, headers=OWNER, **body):
    return c.post("/api/v1/links", headers=headers, json={"long_url": URL, **body})


def test_create_and_redirect(c):
    r = create(c)
    assert r.status_code == 201
    got = c.get("/" + r.json()["code"])
    assert got.status_code == 302 and got.headers["location"] == URL


def test_alias_conflict(c):
    a = alias()
    assert create(c, alias=a).status_code == 201
    assert create(c, alias=a).status_code == 409


def test_idempotency_sequential(c):
    h = {**OWNER, "Idempotency-Key": str(uuid.uuid4())}
    assert create(c, h).json()["code"] == create(c, h).json()["code"]


def test_idempotency_parallel(c):
    h = {**OWNER, "Idempotency-Key": str(uuid.uuid4())}
    with ThreadPoolExecutor(20) as pool:
        rs = list(pool.map(lambda _: create(c, h), range(20)))
    assert {r.status_code for r in rs} == {201}
    assert len({r.json()["code"] for r in rs}) == 1


def test_expired_and_unknown(c):
    soon = (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat()
    code = create(c, expires_at=soon).json()["code"]
    time.sleep(3)
    assert c.get("/" + code).status_code == 410
    assert c.get("/nope" + uuid.uuid4().hex[:4]).status_code == 404


def test_auth_and_ownership(c):
    code = create(c).json()["code"]
    assert c.get(f"/api/v1/links/{code}").status_code == 401
    assert c.get(f"/api/v1/links/{code}", headers=OTHER).status_code == 403
    assert c.delete(f"/api/v1/links/{code}", headers=OTHER).status_code == 403


def test_delete_propagates_to_all_nodes(c):
    code = create(c).json()["code"]
    assert c.get("/" + code).status_code == 302
    assert c.delete(f"/api/v1/links/{code}", headers=OWNER).status_code == 204
    deadline, streak = time.time() + 10, 0
    while time.time() < deadline and streak < 12:       # 12 in a row ~ every node answered 410
        streak = streak + 1 if c.get("/" + code).status_code == 410 else 0
    assert streak == 12
    assert c.delete(f"/api/v1/links/{code}", headers=OWNER).status_code == 404


def replica_sql(sql):
    subprocess.run(["docker", "compose", "exec", "-T", "pg-replica", "psql", "-U", "shortener", "-d", "shortener",
                    "-c", sql], check=True, capture_output=True)


def test_delete_survives_replica_lag(c):
    code = create(c).json()["code"]
    time.sleep(1)                                       # let the row reach the replica
    replica_sql("SELECT pg_wal_replay_pause()")
    try:
        assert c.delete(f"/api/v1/links/{code}", headers=OWNER).status_code == 204
        assert c.get("/" + code).status_code == 410     # the replica still shows the row as active
    finally:
        replica_sql("SELECT pg_wal_replay_resume()")


def test_rate_limit_sends_retry_after(c):
    h = {"Authorization": "Bearer " + uuid.uuid4().hex}  # own bucket, does not slow other tests
    with ThreadPoolExecutor(30) as pool:
        rs = list(pool.map(lambda _: c.get("/api/v1/links/none", headers=h), range(120)))
    limited = [r for r in rs if r.status_code == 429]
    assert limited and all(r.headers.get("retry-after") == "1" for r in limited)


def test_malformed_urls_are_400(c):
    assert c.post("/api/v1/links", headers=OWNER, json={"long_url": "http://[bad/x"}).status_code == 400
    assert c.post("/api/v1/links", headers=OWNER, json={"long_url": "https://a.com/\x00b"}).status_code == 400


def test_created_alias_overrides_negative_cache(c):
    a = alias()
    assert c.get("/" + a).status_code == 404
    create(c, alias=a)
    deadline = time.time() + 2
    while time.time() < deadline and c.get("/" + a).status_code != 302:
        time.sleep(0.1)
    assert c.get("/" + a).status_code == 302
