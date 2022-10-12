import socket
import warnings
from collections import defaultdict
from enum import Enum
from ipaddress import ip_address, ip_network
from typing import *

from cad_io import adoIf, cns

from .ado_request import AdoRequest
from .http_request import HttpRequest
from .request import (Callback, Entry, Metadata, MultinetError,
                      MultinetResponse, Request)


def is_controls_host(ip_addr=None):
    if not ip_addr:
        try:
            host_name = socket.gethostname()
            ip_addr = socket.gethostbyname(host_name)
        except:  # pylint: disable=bare-except
            warnings.warn("Unable to get Hostname and IP")
            return False
    ip_addr = ip_address(ip_addr)
    return ip_addr in ip_network("130.199.104.0/22") or ip_addr in ip_network(
        "130.199.108.0/22"
    )


class EntryType(Enum):
    ADO = 0
    CDEV = 1
    HTTP = 2

    @classmethod
    def get_type(cls, type_):
        if not is_controls_host():
            return cls.HTTP
        if type_ in ("ADO",):
            return cls.ADO
        elif type_ in ("CDEVDEVICE",):
            return cls.HTTP


class Multirequest(Request):
    _types: Dict[str, EntryType] = dict()

    def __init__(self):
        super().__init__()
        self._ado_req = AdoRequest()
        self._http_req = HttpRequest()

        self._requests = {
            EntryType.ADO: self._ado_req,
            EntryType.CDEV: self._http_req,
            EntryType.HTTP: self._http_req,
        }

    def get(self, *entries: Entry, **kwargs) -> Dict[Entry, Any]:
        entries, results = self._process_entries(entries)
        for type_ in entries:
            request = self._requests[type_]
            res = request.get(*entries[type_], **kwargs)
            results.update(res)
        return MultinetResponse(results)

    def get_async(
            self, callback: Callback, *entries: Entry, **kwargs
    ) -> Dict[Entry, MultinetError]:
        entries, errors = self._process_entries(entries)
        for type_ in entries:
            request = self._requests[type_]
            err = request.get_async(callback, *entries[type_], **kwargs)
            errors.update(err)
        return MultinetResponse(errors)

    def set_history(self, enabled):
        for req in self._requests.values():
            req.set_history(enabled)

    def clear_metadata(self):
        self._ado_req._io._handles.clear()
        adoIf._metadata_dict.clear()

    def get_meta(
            self, *entries, **kwargs
    ) -> Dict[Entry, Union[Metadata, MultinetError]]:
        entries, results = self._process_entries(entries)
        for type_ in entries:
            request = self._requests[type_]
            res = request.get_meta(*entries[type_], **kwargs)
            results.update(res)
        return MultinetResponse(results)

    def set(self, *entries: Entry, **kwargs) -> Dict[Entry, MultinetError]:
        entries, errors = self._process_entries(entries)
        for type_ in entries:
            request = self._requests[type_]
            err = request.set(*entries[type_], **kwargs)
            if err is not None:
                errors.update(err)
        return errors

    def cancel_async(self):
        for req in self._requests.values():
            req.cancel_async()

    def _process_entries(self, entries):
        entries, errors = self._parse_entries(entries)
        results = defaultdict(list)
        for entry in entries:
            device = entry[0]
            if device in self._types:
                type_ = self._types[device]
            else:
                cns_entry = cns.cnslookup(device)
                if cns_entry is None:
                    type_ = None
                else:
                    type_ = EntryType.get_type(cns_entry.type)
                    self._types[device] = type_
            if type_ is None:
                errors[entry] = MultinetError("CNS lookup failed")
            else:
                self.logger.debug("Using %s for %s", type_, entry)
                results[type_].append(entry)
        return results, errors


# Below for testing only
def cb(value, ppm_user):
    print(f"{value} for user {ppm_user}")


def timeout(time_out):
    seconds = 0
    while seconds < time_out:
        time.sleep(1)
        seconds += 1


if __name__ == "__main__":
    import time

    req = Multirequest()
    data = req.get(("simple.test", "longArrayS"), ("simple.test", "stringS"), ppm_user=1)
    print(data)
    data = req.get(("simple.test", "floatArrayS"))
    print(data)
    data = req.get(("simple.test", "stringS"), ppm_user=3)
    print(data)
    data = req.get(("simple.test", "degM"))
    print(50 * "-")
    req.get_async(cb, ("simple.test", "degM"), ("simple.test", "sinM"), ("simple.test", "varArrayS"),
                  ("simple.test", "stringS"), immediate=True, ppm_user=3)
    timeout(4)
    req.cancel_async()

    print(50 * "-")
    req.get_async(cb, ("simple.test", "stringS"), immediate=True, ppm_user=5)
    timeout(2)
    req.cancel_async()
