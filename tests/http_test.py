import pytest
from multinet.http_request import HttpRequest
import logging
from threading import Condition
from cad_io.pyado2 import IORequest


@pytest.fixture(scope="module")
def req():
    return HttpRequest()


@pytest.fixture(scope="module")
def pyado2():
    return IORequest()


def test_set(req: HttpRequest, pyado2: IORequest):
    res = req.set(("simple.sam", "intS", 7))
    assert res, "HTTP Request failed"
    assert (
        pyado2.get(("simple.sam", "intS"))["simple.sam:intS"]["value"] == 7
    ), "Value mismatch with pyado2"


def test_multiset(req: HttpRequest, pyado2: IORequest):
    res = req.set(
        ("simple.sam", "intS", 7),
        ("simple.sam", "floatS", 3.14),
        ("simple.sam", "stringS", "Hello"),
    )
    assert res, "HTTP Request failed"
    assert (
        pyado2.get(("simple.sam", "intS"))["simple.sam:intS"]["value"] == 7
    ), "Value mismatch with pyado2"


def test_async(req: HttpRequest):
    import math

    keys = [("simple.sam", "sinM"), ("simple.sam", "degM")]
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
        condition.wait_for(lambda: counter >= 20)
    req.cancel_async()


def test_cdev(req: HttpRequest):
    req.get(("simple.cdev2", "doubleS"))