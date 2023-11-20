import copy
import time
import warnings
from itertools import groupby
from operator import itemgetter
from typing import *

from cad_error import RhicError
from cad_io import adoIf

from .request import (AsyncID, Callback, Entry, Metadata, MultinetError,
                      MultinetResponse, Request)


class AdoRequest(Request):
    _tid_map = {}

    def __init__(self):
        super().__init__()
        self._meta = {}
        self._handles = {}
        self._async_id_map: Dict[AsyncID, List[int]] = {}

    def default_ppm_user(self):
        try:
            r = AdoRequest()
            entry = ("injSpec.super", "agsPpmUserM")
            result = r.get(entry)
            ppm_user = result.get(entry)
            if ppm_user < 1 or ppm_user > 8:
                ppm_user = 1
        except:
            ppm_user = 1
        return ppm_user

    def get_async(
        self,
        callback: Callback,
        *entries: Entry,
        ppm_user: Union[int, Iterable[int]] =1,
        immediate=False,
        grouping="parameter",
        **kwargs,
    ) -> MultinetResponse[Entry, MultinetError]:
        if not callable(callback):
            raise ValueError("Callback must be callable")

        if "timestamp" in kwargs:
            warnings.warn("'timestamp' keyword argument deprecated; use 'valueAndTime' property instead.", DeprecationWarning)

        entries, response = self._parse_entries(entries, timestamps=kwargs.get("timestamp", False))

        metadata = self.get_meta(*entries)
        # if any of the meta data was not acquired, assume the device/parameter was not valid/available
        # and remove it from the request that will be sent out
        for dev, value in metadata.items():
            if isinstance(value, MultinetError):
                entries.remove(dev)
                response[dev] = value
        self._meta.update(metadata)

        if grouping == "ado":
            grouped_entries = [
                tuple(group) for _, group in groupby(entries, lambda e: e[0])
            ]
        elif grouping == "parameter":
            grouped_entries = [
                tuple(group) for _, group in groupby(entries, lambda e: e[0:2])
            ]
        elif grouping == "individual":
            grouped_entries = [(entry,) for entry in entries]
        else:
            raise ValueError(f"Invalid grouping type '{grouping}'")

        if not isinstance(ppm_user, Iterable):
            ppm_user = [ppm_user]

        # invalid_user will be used to allow only ONE default_ppm_user to be used when checking ppm_user array
        num_default_ppm_user = 0
        default_user = self.default_ppm_user()
        for puser in ppm_user:
            if puser < 1 or puser > 8:
                puser = default_user
                num_default_ppm_user += 1
            # avoid duplicate ppm requests
            if (ppm_user.count(puser) > 1) or (puser == default_user and num_default_ppm_user>0):
                continue
            self.logger.debug("args[%d]: %s", len(entries), entries)

            async_id = next(self._mreq_tid_iter)
            io_tids = []
            for group in grouped_entries:
                if immediate:
                    callback(
                        self.get(*group, ppm_user=puser), puser
                    )
                group = list(group)
                ado_name = group[0][0]
                handle = self._get_handle(ado_name)
                if not handle:
                    response.update(
                        dict.fromkeys(group, MultinetError(RhicError.IO_BAD_NAME))
                    )
                    continue
                tid, status = adoIf.adoGetAsync(
                    list=[(handle, *rest) for _, *rest in group],
                    ppmIndex=puser - 1,
                    callback=self._async_callback,
                )
                self._tid_map[tid] = (group, metadata, callback, self)
                io_tids.append(tid)
                for entry, st in zip(group, status):
                    response[entry] = None if st == 0 else MultinetError(st)
                
        self._async_id_map[async_id] = io_tids
        response.tid = async_id
        return response

    def get(
        self, *entries: Entry, ppm_user=1, **kwargs
    ) -> MultinetResponse[Entry, Any]:
        if "timestamp" in kwargs:
            warnings.warn("'timestamp' keyword argument deprecated; use 'valueAndTime' property instead.", DeprecationWarning)

        entries, response = self._parse_entries(entries, timestamps=kwargs.get("timestamp", False))
        if not isinstance(ppm_user, Iterable):
            ppm_user = [ppm_user]
        for i, ppm in enumerate(ppm_user):
            if ppm < 1 or ppm > 8:
                # if a ppm user is duplicated, we'll allow it since it's just a synchronous request.
                ppm_user[i] = self.default_ppm_user()

        for ado_name, group in groupby(entries, itemgetter(0)):
            group = list(group)
            handle = self._get_handle(ado_name)
            if not handle:
                response.update(
                    dict.fromkeys(group, MultinetError(RhicError.IO_BAD_NAME))
                )
                continue
            metadata = self.get_meta(*group)
            for puser in ppm_user:
                data, status = adoIf.adoGet(
                    list=[(handle, *rest) for _, *rest in group], ppmIndex=puser - 1
                )
                if data is None:
                    # this means none of the data reported successfully
                    data = []
                recv_time = time.time_ns()
                data_iter = iter(data)
                for entry, st in zip(group, status):
                    if st != 0:
                        if entry[-1] == "timestampSeconds":
                            response[entry] = int(recv_time // 1e9)
                            response[(*entry[:-1], "timeStampSource")] = "ArrivalLocal"
                        elif entry[-1] == "timestampNanoSeconds":
                            response[entry] = int(recv_time % 1e9)
                            response[(*entry[:-1], "timeStampSource")] = "ArrivalLocal"
                        else:
                            response[entry] = MultinetError(st)
                        continue
                    value = next(data_iter)
                    if value is None:
                        response[entry] = value
                        continue
                    key = entry
                    if len(ppm_user) > 1:
                        key = entry + (puser,)
                    response[key] = (
                        value[0] if metadata[entry]["count"] == 1 else list(value)
                    )
        return response

    def get_meta(
        self, *entries: Entry, **kwargs
    ) -> MultinetResponse[Entry, Union[Metadata, MultinetError]]:
        # first argument is always ADO
        # third  argument 'should' be value.  If it's the 'valueAndTime', 'valueAndTrigger', or 'timeInfo'
        # pseudo prop, change it.  If the tuple is != 3, no property was passed so we don't need to redefine.
        orig_entries = copy.deepcopy(entries)
        if len(entries[0]) == 3:
            entries = [(dev, param, prop) if (
                        prop != None and prop != 'valueAndTime' and prop != 'valueAndTrigger' and prop != 'timeInfo')
                       else (dev, param, 'value') for (dev, param, prop) in entries]
        entries, response = self._parse_entries(entries)
        for ado_name, group in groupby(entries, itemgetter(0)):
            handle = self._get_handle(ado_name)
            if not handle:
                response.update(
                    dict.fromkeys(group, MultinetError(RhicError.IO_BAD_NAME))
                )
                continue
            meta, st = adoIf.adoMetaData(handle)
            for entry in group:
                if not meta:
                    response[entry] = MultinetError("Metadata not available")
                    continue

                try:
                    if len(entry) == 1:
                        # response.update(
                        #     {
                        #         entry + key: value._asdict()
                        #         for key, value in meta.items()
                        #     }
                        # )
                        response[entry] = meta
                    if len(entry) == 2:
                        response[entry] = dict(meta[(entry[1], "value")]._asdict())
                    elif len(entry) == 3:
                        # make sure we use the original requested entry when returning results and not the modified
                        # entry used to deal with the pseudo props
                        e = [tup for tup in orig_entries if tup[0] == entry[0] and tup[1] == entry[1]]
                        response[e[0] if len(e) == 1 else entry] = dict(meta[(entry[1], entry[2])]._asdict())
                except KeyError:
                    response[entry] = MultinetError("Metadata not available")
        return response

    def set(
        self,
        *entries: Union[Entry, Dict[Entry, Any]],
        ppm_user=1,
        set_hist=None,
        **kwargs,
    ) -> MultinetResponse[Entry, MultinetError]:
        if ppm_user < 1 or ppm_user > 8:
            ppm_user = self.default_ppm_user()
        orig_sethist = None
        # Override sethistory for call
        if set_hist is not None:
            # Store original sethist state
            orig_sethist = not adoIf.setHistory.storageOff
            # Set overrride
            adoIf.keep_history(set_hist)

        entries, response = self._parse_sets(entries)
        for ado_name, group in groupby(entries, itemgetter(0)):
            group = list(group)
            handle = self._get_handle(ado_name)
            keys = [entry[:-1] for entry in group]

            if not handle:
                response.update(
                    dict.fromkeys(group, MultinetError(RhicError.IO_BAD_NAME))
                )
                continue

            metadata = self.get_meta(*keys)
            _, status = adoIf.adoSet(
                list=[(handle, *rest) for _, *rest in group], ppmIndex=ppm_user - 1
            )
            for entry, st in zip(keys, status):
                if st != 0:
                    response[entry] = MultinetError(st)
                else:
                    response[entry] = None

        if orig_sethist is not None:
            # Restore original sethist state if stored
            adoIf.keep_history(orig_sethist)
        return response

    def cancel_async(self, async_id: Union[MultinetResponse, AsyncID]=None):
        """Cancels active asyncs.

        Args:
            async_id (Union[MultinetResponse, AsyncID], optional): ID or response from `get_async()` call. Defaults to None.

        Raises:
            ValueError: Raised when async_id is MultinetResponse and was not returned from a `get_async()` call.
            TypeError: Raised when async_id is neither MultinetResponse nor AsyncID.
        """
        
        if isinstance(async_id, MultinetResponse):
            if async_id.tid is None:
                raise ValueError("MultinetResponse passed was not an async response")
            tid = async_id.tid
        elif isinstance(async_id, AsyncID):
            tid = async_id
        elif async_id is not None:
            raise TypeError("mreq_response must be of type MultinetResponse or AsyncID")

        if async_id is None:
            tids = [tid for tids in self._async_id_map.values() for tid in tids]
            self._async_id_map.clear()
        else:
            tids = self._async_id_map[async_id]
            del self._async_id_map[async_id]

        for tid in tids:
            adoIf.adoStopAsync(tid=tid)

    def set_history(self, enabled):
        """Enable or disable set history

        Args:
            enabled (bool): Enable set history if True, else disable
        """
        adoIf.keep_history(enabled)

    def _get_handle(self, name: str):
        if name not in self._handles:
            self._handles[name] = adoIf.create_ado(name)
        return self._handles[name]

    @classmethod
    def _async_callback(cls, arg):
        recv_time = time.time_ns()
        data, tid, requests, istatus, ppm_index = arg
        entries, metadata, callback, inst = cls._tid_map.get(tid, (None, None, None, None))
        if entries is None:
            # TODO: Race condition?
            return

        ppm_user = ppm_index + 1
        data_iter = iter(data)
        response = MultinetResponse()
        for entry, st in zip(entries, istatus):
            if st != 0:
                if entry[-1] == "timestampSeconds":
                    response[entry] = int(recv_time // 1e9)
                    response[(*entry[:-1], "timeStampSource")] = "ArrivalLocal"
                elif entry[-1] == "timestampNanoSeconds":
                    response[entry] = int(recv_time % 1e9)
                    response[(*entry[:-1], "timeStampSource")] = "ArrivalLocal"
                else:
                    response[entry] = MultinetError(st)                
                continue
            value = next(data_iter)
            response[entry] = (
                value[0] if metadata[entry]["count"] == 1 else list(value)
            )
        response = inst._filter_data(response, ppm_user)
        if response:
            callback(response, ppm_user)



if __name__ == "__main__":
    req = AdoRequest()
    resp = req.get(("simple.test", "charArrayS"))
    assert isinstance(resp[("simple.test", "charArrayS")], list)
