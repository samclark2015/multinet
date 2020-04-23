import pytest
from multinet.ado_request import AdoRequest
import logging
from threading import Condition, Thread
from multinet import filters
from time import sleep


@pytest.fixture(scope="function")
def req():
    return AdoRequest()


def test_array(req):
    data = req.get(("simple.test", "charArrayS"), ("simple.test", "charS"))
    assert isinstance(data[("simple.test", "charArrayS")], list)


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


def test_get_async_filter(req):
    import math

    set_vals = [1, 2, 2, 3, 4]
    keys = [("simple.test", "intS")]
    counter = 0
    set_counter = 0
    condition = Condition()

    def set_thread():
        nonlocal set_counter
        while set_counter < len(set_vals):
            _ = [req.set((*key, set_vals[set_counter])) for key in keys]
            set_counter += 1
            sleep(1.0)

    def cb(data, ppm_user):
        nonlocal counter
        assert all(key in data for key in keys)
        counter += 1
        logging.debug("%d received", counter)
        with condition:
            condition.notify_all()

    req.add_filter(filters.AnyChange())
    req.get_async(cb, *keys)
    Thread(target=set_thread).start()
    with condition:
        condition.wait_for(lambda: counter >= 4 or set_counter >= len(set_vals))
    req.cancel_async()
    assert (
        set_counter == len(set_vals) and counter == 4
    ), f"{set_counter} sets; {counter} received"


def test_get_entries_list(req):
    keys = [("simple.test:sinM"), ("simple.test.sys5:sinM")]
    ent = [tuple(key.split(":")) for key in keys]
    data = req.get(*ent)
    assert isinstance(data, dict)
    all(key in data for key in ent)


def test_set(req):
    from time import sleep

    val = req.get(("simple.test", "intS"))[("simple.test", "intS")]
    assert req.set(("simple.test", "intS", 254)) is None
    val1 = req.get(("simple.test", "intS"))[("simple.test", "intS")]
    sleep(1)
    assert val1 == 254
    assert req.set(("simple.test", "intS", val)) is None
