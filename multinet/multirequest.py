import socket
import logging
import warnings
from ipaddress import ip_address, ip_network
from collections import defaultdict
from enum import Enum
from typing import *

from cad import cns3

from .ado_request import AdoRequest
from .cdev_request import CDEVRequest
from .http_request import HttpRequest
from .request import Callback, Entry, Metadata, Request


def is_controls_host(ip_addr=None):
    if not ip_addr:
        try:
            host_name = socket.gethostname()
            ip_addr = socket.gethostbyname(host_name)
        except:  # pylint: disable=bare-except
            warnings.warn("Unable to get Hostname and IP")
            return False
    return ip_address(ip_addr) in ip_network("130.199.104.0/23") or ip_address(
        ip_addr
    ) in ip_network("130.199.108.0/23")


class EntryType(Enum):
    ADO = 0
    CDEV = 1
    HTTP = 2

    @classmethod
    def get_type(cls, type_):
        if is_controls_host():
            if type_ in ("ADO",):
                return cls.ADO
            elif type_ in ("CDEVDEVICE",):
                return cls.HTTP
        else:
            return cls.HTTP


class Multirequest(Request):
    _types: Dict[str, EntryType] = dict()

    def __init__(self):
        super().__init__()
        self._ado_req = AdoRequest()
        self._cdev_req = CDEVRequest()
        self._http_req = HttpRequest()

        self._requests = {
            EntryType.ADO: self._ado_req,
            EntryType.CDEV: self._cdev_req,
            EntryType.HTTP: self._http_req,
        }

    def get(self, *entries: Entry, **kwargs) -> Dict[Entry, Any]:
        results = dict()
        entries = self._process_entries(entries)
        for type_ in entries:
            request = self._requests[type_]
            res = request.get(*entries[type_], **kwargs)
            results.update(res)
        return results

    def get_async(self, callback: Callback, *entries: Entry, **kwargs) -> None:
        entries = self._process_entries(entries)
        for type_ in entries:
            request = self._requests[type_]
            request.get_async(callback, *entries[type_], **kwargs)

    # def async_handler(self, *entries, ppm_user: Union[int, List[int]] = 1):
    #     def callback(func: Callback, data: Dict[Entry, Any], ppm_user: int):
    #         try:
    #             func(data, ppm_user)
    #         except Exception as e:
    #             self.logger.warning(f"Error handling callback for {data.keys()}")
    #             self.logger.info(traceback.format_exc())

    #     def wrapper(func):
    #         if isinstance(ppm_user, Iterable):
    #             for user in ppm_user:
    #                 self.get_async(partial(callback, func), *entries, ppm_user=user)
    #         elif isinstance(ppm_user, int):
    #             self.get_async(partial(callback, func), *entries, ppm_user=ppm_user)
    #         else:
    #             raise ValueError("PPM User must be int 1 - 8, or list of ints 1 - 8")
    #         return func

    #     return wrapper

    def get_meta(self, *entries, **kwargs) -> Dict[Entry, Metadata]:
        results = dict()
        entries = self._process_entries(entries)
        for type_ in entries:
            request = self._requests[type_]
            res = request.get_meta(*entries[type_], **kwargs)
            results.update(res)
        return results

    def set(self, *entries: Entry, **kwargs) -> bool:
        results = True
        entries = self._process_entries(entries)
        for type_ in entries:
            request = self._requests[type_]
            results &= request.set(*entries[type_], **kwargs)
        return results

    def cancel_async(self):
        for req in self._requests.values():
            req.cancel_async()

    @classmethod
    def _process_entries(cls, entries):
        results = defaultdict(list)

        for entry in entries:
            device = entry[0]
            if device in cls._types:
                type_ = cls._types[device]
            else:
                cns_entry = cns3.cnslookup(device)
                type_ = EntryType.get_type(cns_entry.type)
                cls._types[device] = type_
            logging.debug("Using %s for %s", type_, entry)
            results[type_].append(entry)
        return results
