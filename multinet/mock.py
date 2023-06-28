import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Thread
from typing import Any, Callable, Optional, Union

from cad_error import RhicError

from multinet.request import (Callback, Entry, Metadata, MultinetError,
                              MultinetResponse)

from .request import Request


@dataclass
class DynamicDummy:
    function: Callable[[dict[Entry, Any]], Any]
    period: float

    entry: Entry
    ppm_user: int

    last_called: datetime = field(default_factory=lambda: datetime.now(), compare=False)
    callbacks: list[Callback] = field(default_factory=list)


class MockRequest(Request):
    def __init__(self):
        super().__init__()
        self._thread: Optional[Thread] = None
        self._values: dict[int, MultinetResponse] = {}
        self._dynamics: dict[tuple(Entry, int), DynamicDummy] = {}

    def dummy_runner(self):
        while True:
            now = datetime.now()
            for dynamic in self._dynamics.values():
                if dynamic.last_called + timedelta(seconds=dynamic.period) > now:
                    continue
                add_mock_static(
                    dynamic.entry, dynamic.function(self._values), dynamic.ppm_user
                )
                for callback in dynamic.callbacks:
                    callback(
                        self.get(dynamic.entry, ppm_user=dynamic.ppm_user),
                        dynamic.ppm_user,
                    )
                dynamic.last_called = now
            period = (
                min(dummy.period for dummy in self._dynamics.values())
                if self._dynamics
                else 0.1
            )
            time.sleep(period)

    def get(self, *entries: Entry, ppm_user=1, **kwargs) -> MultinetResponse:
        resp = MultinetResponse()
        for entry in entries:
            if ppm_user not in self._values:
                resp[entry] = MultinetError(RhicError.ADO_WRONG_PPM_INDEX)
                continue
            ppm_data = self._values[ppm_user]
            if entry not in ppm_data:
                resp[entry] = MultinetError(RhicError.ADO_NO_SUCH_NAME)
                continue
            resp[entry] = ppm_data[entry]
        return resp

    def get_async(
        self, callback: Callback, *entries: Entry, immediate=False, ppm_user=1, **kwargs
    ) -> MultinetResponse:
        resp = MultinetResponse()
        for entry in entries:
            if (entry, ppm_user) not in self._dynamics:
                resp[entry] = MultinetError(RhicError.ADO_NO_SUCH_NAME)
                continue
            dynamic = self._dynamics[entry, ppm_user]
            dynamic.callbacks.append(callback)
            resp[entry] = MultinetError(RhicError.SUCCESS)
            if immediate:
                callback(self.get(entry, ppm_user=ppm_user), ppm_user)
        return resp

    def set(
        self, *entries: Entry, ppm_user=1, set_hist: Optional[bool] = None, **kwargs
    ) -> MultinetResponse:
        return super().set(*entries, ppm_user=ppm_user, set_hist=set_hist, **kwargs)

    def get_meta(self, *entries: Entry, ppm_user=1, **kwargs) -> MultinetResponse:
        return super().get_meta(*entries, ppm_user=ppm_user, **kwargs)

    def cancel_async(self):
        return super().cancel_async()

    def set_history(self, enabled: bool):
        return super().set_history(enabled)


instance = MockRequest()


def set_mock(enabled: bool = True):
    import multinet
    import multinet.ado_request
    import multinet.http_request
    import multinet.multirequest

    def dummy_class(*args, **kwargs):
        return instance
    
    setattr(multinet, "AdoRequest", dummy_class)
    setattr(multinet, "Multirequest", dummy_class)
    setattr(multinet.ado_request, "AdoRequest", dummy_class)
    setattr(multinet.http_request, "HttpRequest", dummy_class)
    setattr(multinet.multirequest, "Multirequest", dummy_class)


def add_mock_static(key: Entry, value: Any, ppm_user: int = 1):
    if ppm_user not in instance._values:
        ppm_dict = instance._values[ppm_user] = MultinetResponse()
    else:
        ppm_dict = instance._values[ppm_user]
    key = ppm_dict._tranform_key(key)
    ppm_dict[key] = value


def add_mock_dynamic(
    key: Entry,
    fn: Callable[[dict[Entry, Any]], Any],
    period: float,
    ppm_user: int = 1,
    initial_value: Any = None,
):
    if not instance._thread:
        instance._thread = thread = Thread(target=instance.dummy_runner, daemon=True)
        thread.start()
    instance._dynamics[key, ppm_user] = DynamicDummy(fn, period, key, ppm_user)
    if initial_value is not None:
        add_mock_static(key, initial_value, ppm_user)
