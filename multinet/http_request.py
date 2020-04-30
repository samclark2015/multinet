import getpass
import os
import socket
import sys
import threading
import warnings
import time
from functools import lru_cache
from itertools import groupby
from operator import itemgetter
from typing import *
import requests

from .request import Entry, Metadata, Request, Callback, MultinetError

HTTP_SERVER = "http://csgateway01.pbn.bnl.gov"


class HttpRequest(Request):
    def __init__(self, server=HTTP_SERVER, polling_period=1.0) -> None:
        super().__init__()
        self.server = server
        self.polling_period = polling_period
        self._context = ""
        self._callbacks: Dict[str, Tuple[Callback, bool]] = {}
        self._cancel_async = False
        self._lock = threading.Lock()

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
        names, props = self._unpack_args(*entries)
        payload = dict(names=names, props=props, ppmuser=ppm_user)
        httpreq = self.server + "/DeviceServer/api/device/list/valueAndTime"
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
            data = {entry: MultinetError(error) for entry in entries}
        else:
            for entry in r.json():
                device = entry["device"]
                others = entry["property"].split(":")
                key: Entry = (device, *others)  # type: ignore
                if "error" in entry:
                    data[key] = MultinetError(entry["error"])
                else:
                    value = entry["value"]
                    type_ = entry["type"]
                    value = self._convert_value(value, type_)
                    data[key] = value
                    if timestamp and "timestamp" in entry:
                        data[(*key[:2], "timestamp")] = entry["timestamp"]
        return data

    def set(self, *entries: Entry, ppm_user=1, **kwargs) -> Dict[Entry, MultinetError]:
        context = self._get_context()
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
        self._cancel_async = True

    def get_async(
        self,
        callback: Callback,
        *entries: Entry,
        ppm_user=1,
        immediate=False,
        timestamp=True,
        **kwargs,
    ) -> Dict[Entry, MultinetError]:
        """ Asynchronous get. 
        The user defined function callback(*args) will be called 
        when any parameter in the list have been changed.       
        """
        names, props = self._unpack_args(*entries)
        payload = {"names": names, "props": props, "ppmuser": ppm_user}
        url = HTTP_SERVER + "/DeviceServer/api/device/list/async"
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
        thread = threading.Thread(target=self._async_thread, args=(rid, immediate))
        thread.start()
        return {}

    def _async_thread(self, rid, immediate):
        # internal thread function. It polls for http results and
        # calls user callback when any data have been received
        payload = {"id": rid}
        headers = {"Accept": "application/json"}
        url = HTTP_SERVER + "/DeviceServer/api/device/async/result"
        count = 0
        callback, timestamp = self._callbacks[rid]
        while not self._cancel_async:
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
                            if "value" not in entry:
                                warnings.warn(f"Unable to get {key}; {entry['error']}")
                                continue
                            value = entry["value"]
                            ppm_user = entry["ppmuser"]
                            type_ = entry["type"]
                            value = self._convert_value(value, type_)
                            results.append((key, value))
                            if timestamp and "timestamp" in entry:
                                data[(*key[:2], "timestamp")] = entry["timestamp"]
                        with self._lock:
                            grouped = groupby(
                                sorted(results, key=itemgetter(0)), itemgetter(0),
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
                                            zipped_dict, ppm_user
                                        )  # call the user callback
                                count += 1
                time.sleep(self.polling_period)
            else:
                error = r.headers.get("CAD-Error")
                self.logger.error(
                    "Failed to process async - HTTP Error: %d, %s", r.status_code, error
                )

                return 1
        self.logger.info("_getAsync_thread: exiting")
        return 0

    def _get_context(self):
        if not self._context:
            pid = str(os.getpid())
            host = socket.gethostname()
            uname = getpass.getuser()
            payload = {
                "user": uname,
                "procName": sys.argv[0] or __name__,
                "procId": int(pid),
                "machine": host,
            }

            httpreq = self.server + "/DeviceServer/api/device/context"
            # we don't need to process as json since this request will return io simple text value
            try:
                r = requests.get(httpreq, params=payload)  # type: ignore
            except requests.exceptions.RequestException as exc:
                self.logger.error("get context failed: %s", exc)
                return 2

            self._context = r.text
        return self._context

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
