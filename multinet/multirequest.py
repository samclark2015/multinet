from collections import defaultdict
from enum import Enum
from typing import *
import traceback
from cad import cns3
from functools import partial, lru_cache

from .ado_request import AdoRequest
from .http_request import HttpRequest
from .request import Entry, Metadata, Request, Callback


class EntryType(Enum):
    ADO = 0
    CDEV = 1
    HTTP = 2

    @classmethod
    def get_type(cls, type_):
        if type_ in ("ADO",):
            return cls.ADO
        elif type_ in ("CDEVDEVICE",):
            return cls.HTTP
        else:
            return cls.HTTP


class Multirequest(Request):
    def __init__(self):
        # Entries should be grouped by FEC
        self._ado_req = AdoRequest()
        self._http_req = HttpRequest()

        self._requests = {
            EntryType.ADO: self._ado_req,
            EntryType.CDEV: self._http_req,
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

    def get_async(self, callback, *entries: Entry, **kwargs) -> None:
        entries = self._process_entries(entries)
        for type_ in entries:
            request = self._requests[type_]
            request.get_async(callback, *entries[type_], **kwargs)

    def async_handler(self, *entries, ppm_user: Union[int, List[int]] = 1):
        """
        Decorator for function which takes parameters device, param, values, ppm_user
        :param entries: One or more tuple(<ado_name>, <parameter_name>, [property_name]); property_name defaults to 'value'
        :param ppm_user: int; PPM User 1 - 8 (default 1)
        :return:
        """

        def callback(func: Callback, data: Dict[Entry, Any], ppm_user: int):
            try:
                func(data, ppm_user)
            except Exception as e:
                self.logger.warning(f"Error handling callback for {data.keys()}")
                self.logger.info(traceback.format_exc())

        def wrapper(func):
            if isinstance(ppm_user, Iterable):
                for user in ppm_user:
                    self.get_async(partial(callback, func), *entries, ppm_user=user)
            elif isinstance(ppm_user, int):
                self.get_async(partial(callback, func), *entries, ppm_user=ppm_user)
            else:
                raise ValueError("PPM User must be int 1 - 8, or list of ints 1 - 8")
            return func

        return wrapper

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
        self._ado_req.cancel_async()

    @staticmethod
    def _process_entries(entries):
        results = defaultdict(list)

        for entry in entries:
            device = entry[0]
            cns_entry = cns3.cnslookup(device)
            type_ = EntryType.get_type(cns_entry.type)
            results[type_].append(entry)
        return results

