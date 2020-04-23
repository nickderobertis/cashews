import asyncio
import sys
from unittest.mock import Mock

import pytest
from cashews.backends.interface import Backend, ProxyBackend
from cashews.backends.memory import Memory
from cashews.backends.redis import Redis

pytestmark = pytest.mark.asyncio


@pytest.fixture(name="cache")
async def _cache():
    return Memory()


async def test_set_get(cache):
    await cache.set("key", b"value")
    assert await cache.get("key") == b"value"


async def test_set_get_many(cache):
    await cache.set("key", b"value")
    assert await cache.get_many("key", "no_exists") == (b"value", None)


async def test_set_exist(cache):
    assert await cache.set("key", b"value")
    assert await cache.set("key", b"value", exist=True)
    assert not await cache.set("key2", b"value", exist=True)

    assert await cache.set("key2", b"value", exist=False)
    assert not await cache.set("key2", b"value", exist=False)


async def test_get_no_value(cache):
    assert await cache.get("key2") is None


async def test_incr(cache):
    assert await cache.incr("incr") == 1
    assert await cache.incr("incr") == 2
    assert await cache.get("incr") == 2


async def test_ping(cache):
    assert await cache.ping() == b"PONG"


async def test_expire(cache):
    await cache.set("key", b"value", expire=0.01)
    assert await cache.get("key") == b"value"
    await asyncio.sleep(0.01)
    assert await cache.get("key") is None


@pytest.mark.parametrize(
    ("method", "args", "defaults"),
    (
        ("get", ("key",), {"default": None}),
        ("set", ("key", "value"), {"exist": None, "expire": None}),
        ("incr", ("key",), None),
        ("delete", ("key",), None),
        ("expire", ("key", 10), None),
        ("ping", (), None),
        ("clear", (), None),
        ("set_lock", ("key", "value", 10), None),
        ("unlock", ("key", "value"), None),
        ("is_locked", ("key",), {"wait": None, "step": 0.1}),
    ),
)
async def test_proxy_backend(method, args, defaults):
    target = Mock(wraps=Backend())
    backend = ProxyBackend(target=target)

    await getattr(backend, method)(*args)
    if defaults:
        getattr(target, method).assert_called_once_with(*args, **defaults)
    else:
        getattr(target, method).assert_called_once_with(*args)


async def test_delete_match(cache: Backend):
    await cache.set("pref:test:test", b"value")
    await cache.set("pref:value:test", b"value2")
    await cache.set("pref:-:test", b"-")
    await cache.set("pref:*:test", b"*")

    await cache.set("ppref:test:test", b"value3")
    await cache.set("pref:test:tests", b"value3")

    await cache.delete_match("pref:*:test")

    assert await cache.get("pref:test:test") is None
    assert await cache.get("pref:value:test") is None
    assert await cache.get("pref:-:test") is None
    assert await cache.get("pref:*:test") is None

    assert await cache.get("ppref:test:test") is not None
    assert await cache.get("pref:test:tests") is not None


async def test_get_size(cache: Backend):
    await cache.set("test", b"1")
    assert await cache.get_size("test") == sys.getsizeof(b"1")


async def test_get_size_pattern(cache: Backend):
    await cache.set("target:test", b"1")
    await cache.set("no:test", b"1")
    assert await cache.get_size_match("target:*") == sys.getsizeof(b"1")
    assert await cache.get_size_match("*") == sys.getsizeof(b"1") * 2


async def test_listen(cache: Backend):
    m = Mock()
    asyncio.create_task(cache.listen("test:*", "get", "delete", reader=m))
    await asyncio.sleep(0)
    await cache.set("test:10", b"1")
    await cache.set("key", b"1")

    await cache.get("key")
    await cache.get("test:10")

    await asyncio.sleep(0)

    m.assert_called_with("get", "test:10")


async def test_safe_redis():
    redis = Redis(safe=True, address="redis://localhost:9223", hash_key=None)
    await redis.init()
    assert await redis.clear() is False
    assert await redis.set("test", "test") is False

    assert await redis.set_lock("test", "test", 1) is False
    assert await redis.unlock("test", "test") is None
    assert await redis.is_locked("test") is None

    assert await redis.get("test", default="no") == "no"
    assert await redis.get("test") is None
    assert await redis.get_many("test", "test2") == (None, None)

    assert await redis.get_expire("test") is None
    assert await redis.incr("test") is None
    assert await redis.get_size("test") == 0
    async for i in redis.keys_match("*"):
        assert False

    assert await redis.delete("test") == 0
