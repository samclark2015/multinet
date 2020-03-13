import pytest
from multinet.ado_request import AdoRequest
import logging
from threading import Condition


@pytest.fixture(scope="module")
def req():
    print("Hello")
    return AdoRequest()


def test_meta(req):
    meta = req.get_meta(("simple.test", "sinM", "timestampSeconds"))
    assert isinstance(meta, dict)
    logging.info(meta)


def test_get(req: AdoRequest):
    keys = [("simple.test", "sinM"), ("simple.test.sys5", "sinM")]
    data = req.get(*keys)
    assert isinstance(data, dict)
    assert all(key in data for key in keys)


def test_get_async(req):
    import math

    keys = [("simple.test", "sinM"), ("simple.test", "degM")]
    counter = 0
    condition = Condition()

    def cb(data, ppm_user):
        nonlocal counter
        assert all(key in data for key in keys)
        assert math.sin(math.radians(data[keys[1]])) == pytest.approx(data[keys[0]])
        counter += 1
        logging.debug("%d received", counter)
        with condition:
            condition.notify_all()

    req.get_async(cb, *keys)
    with condition:
        condition.wait_for(lambda: counter >= 4)
    req.cancel_async()


def test_get_entries_list(req):
    keys = [("simple.test:sinM"), ("simple.test.sys5:sinM")]
    ent = [tuple(key.split(":")) for key in keys]
    data = req.get(*ent)
    assert isinstance(data, dict)
    all(key in data for key in ent)

def test_set(req):
    from time import sleep
    val = req.get(("simple.test", "intS"))[("simple.test", "intS")]
    assert req.set(("simple.test", "intS", 254))
    val1 = req.get(("simple.test", "intS"))[("simple.test", "intS")]
    sleep(1)
    assert val1 == 254
    assert req.set(("simple.test", "intS", val))    