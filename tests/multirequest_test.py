import pytest
from multinet import Multirequest
import logging
from threading import Condition
import math


@pytest.fixture(scope="function")
def req():
    return Multirequest()


def test_async_handler(req: Multirequest):

    keys = [("simple.test", "sinM"), ("simple.test", "degM")]
    counter = 0
    condition = Condition()

    @req.async_handler(*keys)
    def cb(data, _):
        nonlocal counter
        assert all(key in data for key in keys)
        assert math.sin(math.radians(data[keys[1]])) == pytest.approx(data[keys[0]])
        counter += 1
        logging.debug("%d received", counter)
        with condition:
            condition.notify_all()

    with condition:
        condition.wait_for(lambda: counter >= 4)
    req.cancel_async()

