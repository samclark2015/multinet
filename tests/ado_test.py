import logging
from threading import Condition, Event, Thread
from time import sleep

import pytest
from cad_error import ADOErrors
from multinet import filters
from multinet.ado_request import AdoRequest
from multinet.request import MultinetResponse


@pytest.fixture(scope="function")
def req():
    return AdoRequest()


def test_array(req):
    data = req.get(("simple.test", "charArrayS"), ("simple.test", "charS"))
    assert isinstance(data, MultinetResponse)
    assert isinstance(data[("simple.test", "charArrayS")], (list, tuple))


def test_meta(req):
    meta = req.get_meta(("simple.test", "sinM", "timestampSeconds"))
    assert isinstance(meta, MultinetResponse)
    logging.info(meta)


def test_get(req: AdoRequest):
    keys = [("simple.test", "sinM"), ("simple.test.sys5", "sinM")]
    data = req.get(*keys)
    assert isinstance(data, MultinetResponse)
    assert all(key in data for key in keys)


def test_get_async(req: AdoRequest):
    import math

    keys = [("simple.test", "sinM"), ("simple.test", "degM")]
    counter = 0
    condition = Condition()

    def cb(data, _):
        nonlocal counter
        assert any(key in data for key in keys)
        # assert math.sin(math.radians(data[keys[1]])) == pytest.approx(data[keys[0]])
        counter += 1
        logging.debug("%d received", counter)
        with condition:
            condition.notify_all()

    req.get_async(cb, *keys)
    with condition:
        condition.wait_for(lambda: counter >= 4, 10)
    req.cancel_async()


def test_get_async_filter(req: AdoRequest):
    set_vals = [1, 2, 2, 3, 4]
    keys = [("simple.test", "intS")]
    counter = 0
    condition = Condition()

    set_event = Event()

    def set_thread():
        for i in set_vals:
            _ = [req.set((*key, i)) for key in keys]
            sleep(1.0)
        set_event.set()

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
    set_event.wait(timeout=len(set_vals) + 1)
    with condition:
        condition.wait_for(lambda: counter >= len(set(set_vals)), timeout=10)
    req.cancel_async()
    assert counter == len(set(set_vals)), f"{counter} received"


def test_get_entries_list(req):
    keys = [("simple.test:sinM"), ("simple.test.sys5:sinM")]
    ent = [tuple(key.split(":")) for key in keys]
    data = req.get(*ent)
    assert isinstance(data, MultinetResponse)
    all(key in data for key in ent)


def test_set(req: AdoRequest):
    from time import sleep

    val = req.get(("simple.test", "intS"))[("simple.test", "intS")]
    assert req.set(("simple.test", "intS", 254)).get_status("simple.test:intS") == 0
    val1 = req.get(("simple.test", "intS"))[("simple.test", "intS")]
    sleep(1)
    assert val1 == 254
    assert req.set(("simple.test", "intS", val)).get_status("simple.test:intS") == 0

def test_set_wrong_type(req: AdoRequest):
    err = req.set({"simple.test:longS": "bad value"})
    assert err.get_status("simple.test:longS") == ADOErrors.ADO_WRONG_TYPE
