import getpass
import os
import socket
import sys
import threading
import warnings
from functools import lru_cache
from itertools import groupby
from operator import itemgetter
from typing import *

import requests

from .request import (Callback, Entry, Metadata, MultinetError,
                      MultinetResponse, Request)

HTTP_SERVER = "http://csgateway01.pbn.bnl.gov"


class HttpRequest(Request):
    def __init__(self, server=HTTP_SERVER, polling_period=1.0) -> None:
        super().__init__()
        self.server = server
        self.polling_period = polling_period
        self._context = {}
        self._callbacks: Dict[str, Tuple[Callback, bool]] = {}
        self._cancel_async = False
        self._lock = threading.Lock()
        self._set_hist = True
        self._flags = []

    @lru_cache(maxsize=32)
    def get_meta(
        self, *entries: Entry, **kwargs
    ) -> Dict[Entry, Union[Metadata, MultinetError]]:
        keys = ["name", "prop", "ppmuser"]
        metadata = {}
        for entry in entries:
            payload = dict(zip(keys, entry))
            httpreq = self.server + "/DeviceServer/api/device/metaData"
            self.logger.debug("request: %s", httpreq)
            self.logger.debug("GETTING ADO DATA: %s", payload)

            r = requests.get(
                httpreq, params=payload, headers={"Accept": "application/json"}
            )
            self.logger.debug("<requests.get: %s, text: %s", r, r.text)
            if r.status_code != requests.codes.ok:  # pylint: disable=no-member
                error = r.headers.get("CAD-Error")
                self.logger.error(
                    "Failed to get meta data - HTTP Error: %d, data: %s",
                    r.status_code,
                    error,
                )
                raise ValueError(error)
            else:
                metadata[entry] = r.json()
        return metadata

    def get(
        self, *entries: Entry, ppm_user=1, timestamp=True, **kwargs
    ) -> Dict[Entry, Any]:
        data = {}
        entries = self._parse_entries(entries)
        names, props = self._unpack_args(*entries)
        payload = dict(names=names, props=props, ppmuser=ppm_user)
        httpreq = self.server + "/DeviceServer/api/device/list/numeric/valueAndTime"
        self.logger.debug("request: %s", httpreq)
        self.logger.debug("GETTING ADO DATA: %s", payload)

        r = requests.get(
            httpreq, params=payload, headers={"Accept": "application/json"}
        )
        self.logger.debug("<requests.get: %s, text: %s", r, r.text)
        if r.status_code != requests.codes.ok:  # pylint: disable=no-member
            error = r.headers.get("CAD-Error")
            self.logger.error(
                "Failed to get data - HTTP Error: %d, data: %s",
                r.status_code,
                error,
            )
            data = {entry: MultinetError(error) for entry in entries}
        else:
            for entry in r.json():
                device = entry["device"]
                others = entry["property"].split(":")
                key: Entry = (device, *others)  # type: ignore
                if "error" in entry:
                    data[key] = MultinetError(entry["error"])
                else:
                    type_ = entry["type"]
                    if "value" in entry.keys():
                        value = entry["value"]
                        value = self._convert_value(value, type_)
                    else:
                        if entry["isarray"]:
                            value = entry["data"]
                        else:
                            value = entry["data"][0]

                    data[key] = value
                    if timestamp and "timestamp" in entry:
                        data[(*key[:2], "timestamp")] = entry["timestamp"]
        return MultinetResponse(data)

    def set(
        self, *entries: Entry, ppm_user=1, set_hist=None, **kwargs
    ) -> Dict[Entry, MultinetError]:
        if set_hist is None:
            set_hist = self._set_hist
        context = self._get_context(set_hist)
        names, props, values = self._unpack_args(*entries, is_set=True)
        payload = {
            "names": names,
            "props": props,
            "values": values,
            "ppmuser": ppm_user,
            "context": context,
        }
        url = self.server + "/DeviceServer/api/device/list/value"
        headers = {"Accept": "application/json"}
        self.logger.debug("PUT %s <%s>: %s", url, headers, payload)
        try:
            r = requests.put(url, params=payload, headers=headers)
            data = {}
        except requests.exceptions.RequestException as exc:
            data = {entry: MultinetError(exc) for entry in entries}
        if r.status_code != requests.codes.ok:  # pylint: disable=no-member
            error = r.headers.get("CAD-Error")
            self.logger.error(
                "Failed to set value - HTTP Error %d, data: %s", r.status_code, error
            )
            data = {entry: MultinetError(error) for entry in entries}
        return data

    def cancel_async(self):
        for flag in self._flags:
            flag.set()
        reqs = [
            self.server + "/DeviceServer/api/device/async/cancel?id=" + str(id)
            for id in self._callbacks
        ]
        for req in reqs:
            requests.get(req, headers={"Accept": "application/json"})

        self._flags.clear()
        self._callbacks.clear()

    def get_async(
        self,
        callback: Callback,
        *entries: Entry,
        ppm_user=1,
        immediate=False,
        timestamp=True,
        **kwargs,
    ) -> Dict[Entry, MultinetError]:
        """Asynchronous get.
        The user defined function callback(*args) will be called
        when any parameter in the list have been changed.
        """
        entries = self._parse_entries(entries)
        names, props = self._unpack_args(*entries)
        payload = {"names": names, "props": props, "ppmuser": ppm_user}
        url = HTTP_SERVER + "/DeviceServer/api/device/list/numeric/async"
        r = requests.get(url, params=payload)
        if r.status_code != requests.codes.ok:  # pylint: disable=no-member
            error = r.headers.get("CAD-Error")
            self.logger.error(
                "Failed to start async - HTTP Error %d, data: %s", r.status_code, error
            )
            return {entry: MultinetError(error) for entry in entries}
        rid = (
            r.text
        )  # subscription ID, should be used in subsequent polling for result.
        self._callbacks[rid] = callback, timestamp  # register the callback function
        self._cancel_async = False
        flag = threading.Event()
        self._flags.append(flag)
        thread = threading.Thread(
            target=self._async_thread, args=(rid, immediate, flag), daemon=True
        )
        thread.start()
        return {}

    def set_history(self, enabled):
        self._set_hist = enabled

    def _async_thread(self, rid, immediate, flag: threading.Event):
        # internal thread function. It polls for http results and
        # calls user callback when any data have been received
        payload = {"id": rid}
        headers = {"Accept": "application/json"}
        url = HTTP_SERVER + "/DeviceServer/api/device/async/result"
        count = 0
        callback, timestamp = self._callbacks[rid]
        while not flag.wait(self.polling_period):
            r = requests.get(url, payload, headers=headers)
            if r.status_code == requests.codes.ok:  # pylint: disable=no-member
                data = r.json()
                nvals = data["ndata"]
                if nvals > 0:
                    device_data = data["deviceData"]
                    for ppm_user, entries in groupby(
                        device_data, itemgetter("ppmuser")
                    ):
                        results = []
                        for entry in entries:
                            device = entry["device"]
                            others = entry["property"].split(":")
                            key: Entry = (device, *others)  # type: ignore
                            if len(key) == 2:
                                key = (*key, "value")

                            if "value" not in entry and "data" not in entry:
                                warnings.warn(f"Unable to get {key}; {entry['error']}")
                                continue
                            type_ = entry["type"]
                            if "data" in entry:
                                if entry["isarray"]:
                                    value = entry["data"]
                                else:
                                    value = entry["data"][0]
                            else:
                                value = entry["value"]
                                value = self._convert_value(value, type_)
                            ppm_user = entry["ppmuser"]
                            results.append((key, value))
                            if timestamp and "timestamp" in entry:
                                data[(*key[:2], "timestamp")] = entry["timestamp"]

                        with self._lock:
                            grouped = groupby(
                                sorted(results, key=itemgetter(0)),
                                itemgetter(0),
                            )
                            matched = [[v for v in g] for _, g in grouped]
                            for group in zip(*matched):
                                if immediate or count > 0:
                                    zipped_dict = dict(group)
                                    zipped_dict = self._filter_data(
                                        zipped_dict, ppm_user
                                    )
                                    if zipped_dict:
                                        callback(
                                            MultinetResponse(zipped_dict), ppm_user
                                        )  # call the user callback
                                count += 1
            else:
                error = r.headers.get("CAD-Error")
                self.logger.error(
                    "Failed to process async - HTTP Error: %d, %s", r.status_code, error
                )

                return 1
        self.logger.info("_async_thread: exiting")
        return 0

    def _get_context(self, with_sethist):
        if with_sethist not in self._context:
            pid = os.getpid()
            host = socket.gethostname()
            uname = getpass.getuser() if with_sethist else "none"
            proc = (sys.argv[0] or __name__) if with_sethist else "none"
            payload = {
                "user": uname,
                "procName": proc,
                "procId": pid,
                "machine": host,
            }

            httpreq = self.server + "/DeviceServer/api/device/context"
            # we don't need to process as json since this request will return io simple text value
            try:
                r = requests.get(httpreq, params=payload)  # type: ignore
            except requests.exceptions.RequestException as exc:
                self.logger.error("get context failed: %s", exc)
                return 2

            self._context[with_sethist] = r.text
        return self._context[with_sethist]

    @staticmethod
    def _convert_value(val, type_):
        # convert string to doubles (scalar or array), if possible.
        if val[0] == "[":
            val = [HttpRequest._convert_value(x, type_) for x in val[1:-1].split()]
        elif type_ in ("Byte", "Integer", "Long", "Short"):
            val = int(val)
        elif type_ in ("Float", "Double"):
            val = float(val)
        elif type_ != "String":
            warnings.warn(f"Unknown data type {type_}; interpreting as string")
        return val

    @staticmethod
    def _unpack_args(*entries: Entry, is_set=False) -> Tuple[str, ...]:
        entries = [tuple(ent) for ent in entries]
        names, props, values = [], [], []
        for entry in entries:
            device_name = entry[0]
            names.append(device_name)
            if is_set and len(entry) > 2:
                prop = entry[1:-1]
                props.append(":".join(prop))
                values.append(entry[-1])
            elif not is_set:
                prop = entry[1:]
                props.append(":".join(prop))
        if is_set:
            return ",".join(names), ",".join(props), ",".join(map(str, values))
        else:
            return ",".join(names), ",".join(props)
