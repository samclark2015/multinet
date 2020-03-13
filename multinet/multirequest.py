from collections import defaultdict, namedtuple
from enum import Enum
from itertools import groupby
from operator import itemgetter
from typing import *

from cad import cns3, pyado2

from .ado_request import AdoRequest
from .http_request import HttpRequest
from .request import Entry, Request, Metadata


class EntryType(Enum):
    ADO = 0
    CDEV = 1
    HTTP = 2

    @classmethod
    def get_type(cls, type_):
        if type_ in ("ADO",):
            return cls.ADO
        else:
            return cls.HTTP


class Multirequest(Request):
    def __init__(self):
        # Entries should be grouped by FEC
        self._entries = {}
        self._ado_req = AdoRequest()
        self._http_req = HttpRequest()

    def get(self, *entries: Entry, **kwargs) -> Dict[Entry, Any]:
        results = dict()
        entries = self._process_entries(entries)
        if entries[EntryType.ADO]:
            ado_results = self._ado_req.get(*entries[EntryType.ADO], **kwargs)
            results.update(ado_results)
        return results

    def get_async(self, callback, *entries: Entry, **kwargs) -> None:
        entries = self._process_entries(entries)
        if entries[EntryType.ADO]:
            self._ado_req.get_async(callback, *entries[EntryType.ADO], **kwargs)

    def get_meta(self, *entries, **kwargs) -> Dict[Entry, Metadata]:
        results = dict()
        entries = self._process_entries(entries)
        if entries[EntryType.ADO]:
            ado_results = self._ado_req.get_meta(*entries[EntryType.ADO], **kwargs)
            results.update(ado_results)
        return results

    def set(self, *entries: Entry, **kwargs) -> bool:
        results = True
        entries = self._process_entries(entries)
        if entries[EntryType.ADO]:
            ado_results = self._ado_req.set(*entries[EntryType.ADO], **kwargs)
            results &= ado_results
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


if __name__ == "__main__":
    mr = Multirequest()
    getv = mr.get(("simple.test", "shortS"), ppm_user=1)
    print(getv)
    mr.set(("simple.test", "shortS", 16), ppm_user=1)
    getv = mr.get(("simple.test", "shortS"))
    print(getv)
    mr.get_async(print, ("simple.test", "sinM"))
    import time

    time.sleep(5)
    mr.cancel_async()
