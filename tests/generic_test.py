import logging
from random import randint
from threading import Condition, Thread
from time import sleep

import pytest

from multinet import filters
from multinet.ado_request import AdoRequest
from multinet.cdev_request import CDEVRequest
from multinet.http_request import HttpRequest
from multinet.request import Request


@pytest.mark.parametrize(
    "req,entries",
    [
        (AdoRequest(), [("simple.sam", "sinM", "timestampSeconds")]),
        (HttpRequest(), [("simple.sam", "sinM", "timestampSeconds")]),
        (CDEVRequest(), [("simple.cdev", "doubleS")]),
    ],
)
def test_meta(req, entries):
    meta = req.get_meta(*entries)
    assert isinstance(meta, dict)
    assert all(key in meta for key in entries)


@pytest.mark.parametrize(
    "req,entries",
    [
        (AdoRequest(), [("simple.sam", "sinM", "timestampSeconds")]),
        (HttpRequest(), [("simple.sam", "sinM", "timestampSeconds")]),
        (CDEVRequest(), [("simple.cdev", "doubleS")]),
    ],
)
def test_get(req: Request, entries):
    data = req.get(*entries)
    assert isinstance(data, dict)
    assert all(key in data for key in entries)


@pytest.mark.parametrize(
    "req,entries",
    [
        (AdoRequest(), [("simple.sam", "sinM")]),
        (HttpRequest(), [("simple.sam", "sinM")]),
        (CDEVRequest(), [("simple.cdev", "sinM")]),
    ],
)
def test_get_async(req: Request, entries):
    counter = 0
    condition = Condition()

    def cb(data, ppm_user):
        nonlocal counter
        assert all(key in data for key in entries)
        assert ppm_user == 1
        counter += 1
        logging.debug("%d received", counter)
        with condition:
            condition.notify_all()

    req.get_async(cb, *entries)
    with condition:
        condition.wait_for(lambda: counter >= 4, timeout=10)
    req.cancel_async()
    assert counter > 0


@pytest.mark.parametrize(
    "req,entries,set_vals",
    [
        (AdoRequest(), [("simple.sam", "intS")], [1, 2, 2, 3, 4]),
        (HttpRequest(), [("simple.sam", "intS")], [1, 2, 2, 3, 4]),
        (CDEVRequest(), [("simple.cdev", "doubleS")], [1, 2, 2, 3, 4]),
    ],
)
def test_get_async_filter(req: Request, entries, set_vals):
    counter = 0
    set_counter = 0
    condition = Condition()

    def set_thread():
        nonlocal set_counter
        while set_counter < len(set_vals):
            _ = [req.set((*key, set_vals[set_counter])) for key in entries]
            set_counter += 1
            sleep(1.0)

    def cb(data, _):
        nonlocal counter
        assert all(key in data for key in entries)
        counter += 1
        logging.debug("%d received", counter)
        with condition:
            condition.notify_all()

    req.add_filter(filters.AnyChange())
    req.get_async(cb, *entries)
    Thread(target=set_thread).start()
    with condition:
        condition.wait_for(
            lambda: counter >= 4 or set_counter >= len(set_vals), timeout=10
        )
    req.cancel_async()
    assert (
        set_counter == len(set_vals) and counter == 4
    ), f"{set_counter} sets; {counter} received"


@pytest.mark.parametrize(
    "req,entries",
    [
        (AdoRequest(), [("simple.sam", "intS", randint(0, 255))]),
        (HttpRequest(), [("simple.sam", "intS", randint(0, 255))]),
        (CDEVRequest(), [("simple.cdev", "doubleS", float(randint(0, 255)))]),
    ],
)
def test_set(req: Request, entries):
    for entry in entries:
        val = req.get(entry[:2])[entry[:2]]
        assert req.set(entry)
        val1 = req.get(entry[:2])[entry[:2]]
        assert val1 == entry[2]
        sleep(0.5)
        assert req.set((*entry[:2], val))
