import logging
from threading import Condition

import pytest
from cad_io.adoaccess import IORequest
from multinet.http_request import HttpRequest


@pytest.fixture(scope="module")
def req():
    return HttpRequest()


@pytest.fixture(scope="module")
def pyado2():
    return IORequest()


def test_set(req: HttpRequest, pyado2: IORequest):
    res = req.set(("simple.test", "intS", 7))
    assert not res, "HTTP Request failed"
    real_res = pyado2.get(("simple.test", "intS"))
    assert real_res[("simple.test", "intS")]["value"] == 7, "Value mismatch with pyado2"


def test_multiset(req: HttpRequest, pyado2: IORequest):
    res = req.set(
        ("simple.test", "intS", 7),
        ("simple.test", "floatS", 3.14),
        ("simple.test", "stringS", "Hello"),
    )
    assert not res, "HTTP Request failed"
    assert (
        pyado2.get(("simple.test", "intS"))[("simple.test", "intS")]["value"] == 7
    ), "Value mismatch with pyado2"


def test_async(req: HttpRequest):
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
        condition.wait_for(lambda: counter >= 20, 10)
    req.cancel_async()


@pytest.mark.skip()
def test_cdev(req: HttpRequest):
    req.get(("simple.cdev2", "doubleS"))
