import getpass
import http.client
import json
import os
import socket
import sys
import threading
import time
import warnings
from functools import lru_cache
from itertools import groupby
from operator import itemgetter
from typing import *

import requests
from cad_error import RhicError

from .request import (Callback, Entry, Metadata, MultinetError,
                      MultinetResponse, Request)

HTTP_SERVER = "http://csgateway01.pbn.bnl.gov"


class HttpRequest(Request):
    def __init__(self, server=HTTP_SERVER, polling_period=1.0) -> None:
        super().__init__()
        self.server = server
        self.polling_period = polling_period
        self._context = {}

        self._callbacks: Dict[str, Callback] = {}
        self._entries: dict[str, list[Entry]] = {}
        self._lock = threading.Lock()
        self._set_hist = True
        self._flag = threading.Event()
        self._thread: threading.Thread = None

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
                raise ValueError(error)
            else:
                metadata[entry] = r.json()
        return metadata

    def get(
        self, *entries: Entry, ppm_user: Union[int, Iterable[int]] =1, timestamp=True, **kwargs
    ) -> Dict[Entry, Any]:
        entries, data = self._parse_entries(entries)
        names, props = self._unpack_args(*entries)
        if isinstance(ppm_user, Iterable):
            warnings.warn("HttpRequest get does not support multiple ppm users.  Processing with first user in Iterable only", FutureWarning)
            ppm_user = ppm_user[0]
        payload = dict(names=names, props=props, ppmuser=ppm_user)
        httpreq = self.server + "/DeviceServer/api/device/list/numeric/valueAndTime"
        self.logger.debug("request: %s", httpreq)
        self.logger.debug("GETTING ADO DATA: %s", payload)

        r = requests.get(
            httpreq, params=payload, headers={"Accept": "application/json"}
        )
        recv_time = time.time_ns()
        self.logger.debug("<requests.get: %s, text: %s", r, r.text)
        if r.status_code != requests.codes.ok:  # pylint: disable=no-member
            error = r.headers.get("CAD-Error")
            data = {entry: MultinetError(error) for entry in entries}
        else:
            for entry in r.json():
                device = entry["device"]
                others = entry["property"].split(":")
                key: Entry = (device, *others)  # type: ignore
                if "error" in entry:
                    if key[-1] == "timestampSeconds":
                        data[key] = int(recv_time // 1e9)
                    elif key[-1] == "timestampNanoSeconds":
                        data[key] = int(recv_time % 1e9)
                    else:
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
            data = {entry: MultinetError(error) for entry in entries}
        return data

    def get_async(
        self,
        callback: Callback,
        *entries: Entry,
        ppm_user: Union[int, Iterable[int]]=1,
        immediate=False,
        timestamp=True,
        **kwargs,
    ) -> Dict[Entry, MultinetError]:
        if "timestamp" in kwargs:
            warnings.warn("'timestamp' keyword argument deprecated; use 'valueAndTime' property instead.", DeprecationWarning)

        entries, data = self._parse_entries(entries, timestamps=kwargs.get("timestamp", False))
        names, props = self._unpack_args(*entries)

        if isinstance(ppm_user, Iterable):
            warnings.warn("HttpRequest get_async does not support multiple ppm users.  Processing with first user in Iterable only", FutureWarning)
            ppm_user = ppm_user[0]

        payload = {"names": names, "props": props, "ppmuser": ppm_user}
        url = HTTP_SERVER + "/DeviceServer/api/device/list/numeric/async"

        r = requests.get(url, params=payload)
        if r.status_code != requests.codes.ok:  # pylint: disable=no-member
            error = r.headers.get("CAD-Error")
            return {entry: MultinetError(error) for entry in entries}
        
        # subscription ID, should be used in subsequent polling for result.
        rid = r.text
        with self._lock:
            self._callbacks[rid] = callback  # register the callback function
            self._entries[rid] = entries
            if self._thread is None:
                thread = self._thread = threading.Thread(
                    target=self._async_thread, daemon=True
                )
                thread.start()
        return data

    def cancel_async(self):
        self._flag.set()
        with self._lock:
            if self._thread:
                self._thread.join()
            self._thread = None

            reqs = [
                self.server + "/DeviceServer/api/device/async/cancel?id=" + str(id)
                for id in self._entries.keys()
            ]
            self._entries.clear()
            self._callbacks.clear()
            self._flag.clear()

        for req in reqs:
            requests.get(req, headers={"Accept": "application/json"})

    def set_history(self, enabled):
        self._set_hist = enabled

    def _async_thread(self):
        endpoint = "/DeviceServer/api/device/async/result"

        if self.server.startswith("https://"):
            client = http.client.HTTPSConnection(self.server.removeprefix("https://"))
        else:
            client = http.client.HTTPConnection(self.server.removeprefix("http://"))

        while not self._flag.wait(self.polling_period):        
            with self._lock:
                ids = list(self._entries.keys())

            responses = (
                client.request(
                    "GET",
                    endpoint + f"?id={id_}",
                    headers={"Accept": "application/json"},
                )
                or client.getresponse()
                for id_ in self._entries
            )
            id_responses = (
                response.begin() or (id_, response)
                for id_, response in zip(ids, responses)
                if response.status < 300
            )
            id_data = [(id, json.load(response)) for id, response in id_responses]
            
            recv_time = time.time_ns()

            for id_, group in id_data:
                response = MultinetResponse()
                callback = self._callbacks[id_]
                ppm_user = None

                group_data = {}

                if group["ndata"] == 0:
                    continue

                for item in group["deviceData"]:
                    device: str = item["device"]
                    prop: str = item["property"]
                    (param, prop) = (
                        prop.split(":", 1) if ":" in prop else (prop, "value")
                    )

                    if "error" in item:
                        response[device, param, prop] = MultinetError(item["error"])
                        continue
                    
                    if "data" in item:
                        value = item["data"]
                    elif "value" in item:
                        value = item["value"]
                    else:
                        response[device, param, prop] = MultinetError(RhicError.ADO_NO_DATA)
                        continue

                    if ppm_user is None:
                        ppm_user: int = item["ppmuser"]
                    elif item["ppmuser"] != ppm_user:
                        raise ValueError(
                            f"PPM User Mismatch in Async: {ppm_user} != {item['ppm_user']}"
                        )


                    if "isarray" in item and not item["isarray"]:
                        value = value[0]

                    group_data[device, param, prop] = value
                
                for key in self._entries[id_]:
                    if key in group_data:
                        response[key] = group_data[key]
                    elif key[-1] == "timestampSeconds":
                        response[key] = int(recv_time // 1e9)
                        response[(*key[:-1], "timeStampSource")] = "ArrivalLocal"
                    elif key[-1] == "timestampNanoSeconds":
                        response[key] = int(recv_time % 1e9)
                        response[(*key[:-1], "timeStampSource")] = "ArrivalLocal"
                    else:
                        response[key] = MultinetError(RhicError.ADO_DATA_MISSING)

                response = self._filter_data(response, ppm_user)
                if response:
                    callback(response, ppm_user)

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
