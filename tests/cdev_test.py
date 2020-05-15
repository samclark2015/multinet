import pytest
from multinet.cdev_request import CDEVRequest
import logging
from threading import Condition

pytest.skip("Not ready for testing", allow_module_level=True)


@pytest.fixture(scope="module")
def req():
    return CDEVRequest()


def test_get_async(req):
    keys = [("simple.cdev", "doubleS")]
    counter = 0
    condition = Condition()

    def cb(data, ppm):
        nonlocal counter
        assert all(key in data for key in keys)
        counter += 1
        logging.debug("%d received", counter)
        with condition:
            condition.notify_all()

    req.get_async(cb, *keys)
    with condition:
        condition.wait_for(lambda: counter >= 4, 10)
    req.cancel_async()
