import threading
from itertools import groupby
from operator import itemgetter
from functools import lru_cache
from typing import *
from cad_io import cns

from .request import Entry, Metadata, Request, Callback, MultinetError


class AdoRequest(Request):
    def __init__(self):
        super().__init__()
        self.pyado_lock = threading.Lock()

        self.callbacks = {}
        self.async_receiver: Optional[cns.asyncReceiver] = None
        self.async_thread = None
        self.handles = {}
        self._async_keymap = {}

    def get_async(
        self,
        callback: Callback,
        *entries: Entry,
        ppm_user=1,
        timestamp=True,
        immediate=False,
        **kwargs,
    ) -> None:
        """
        Get ADO parameters synchronously
        :param callback: Callable object taking arguments device, param, data, ppm_user
        :param args: One or more tuple(<ado_name>, <parameter_name>, [property_name]); property_name defaults to 'value'
        :param timestamp: boolean; should timestamps be included (default True)
        :param ppm_user: int; PPM User 1 - 8 (default 1)
        :return: dict
        """
        if not callable(callback):
            raise ValueError("Callback must be callable")
        if ppm_user < 1 or ppm_user > 8:
            raise ValueError("PPM User must be 1 - 8")
        self.logger.debug("args[%d]: %s", len(entries), entries)

        if immediate:
            callback(
                self.get(*entries, ppm_user=ppm_user, timestamp=timestamp), ppm_user
            )

        request_list = self._unpack_args(*entries, timestamp_required=timestamp)
        if self.async_receiver is None:
            self.async_receiver = cns.asyncReceiver(self._unpack_callback)
            self.async_receiver.start()
            self.async_receiver.newdata()
            self.async_thread = threading.Thread(target=self._get_async_thread)
            self.async_thread.start()

        self.logger.debug("request_list: %s", request_list)
        for group in request_list:
            self._async_keymap.update({v: k for k, v in group.items()})
            values = group.values()
            status, tid = cns.adoGetAsync(
                list=(list(values), self.async_receiver), ppmIndex=ppm_user - 1
            )
            self.callbacks[tid] = (callback, group.keys())
            self.logger.debug("adoGetAsync status, tid: %s", (status, tid))
            if status:
                self.logger.error(
                    "adoGetAsync: %s, failed for %s", status, request_list
                )
            self.logger.debug("receiver_thread started, tid: %d", tid)

    def get(
        self, *entries: Entry, ppm_user=1, timestamp=True, **kwargs
    ) -> Dict[Entry, Any]:
        """
        Get ADO parameters synchronously
        :param args: One or more tuple(<ado_name>, <parameter_name>, [property_name]); property_name defaults to 'value'
        :param timestamp: boolean; should timestamps be included (default True)
        :param ppm_user: int; PPM User 1 - 8 (default 1)
        :return: dict
        """
        self.logger.debug("get(%s)", entries)
        request_list = self._unpack_args(*entries, timestamp_required=timestamp)
        rval = dict()
        # Check PPM User is valid
        if ppm_user < 1 or ppm_user > 8:
            raise MultinetError("PPM User must be 1 - 8")

        if len(request_list) == 0:
            msg = "ADO not found: " + str(entries)
            raise ValueError(msg)
        # v17#with self.pyadoLock:

        try:
            # cns-v300:adoreturn = cns.adoGet( list = request_list )
            for group in request_list:
                keys, values = group.keys(), group.values()
                group_return, err = cns.adoGet(list=list(values), ppmIndex=ppm_user - 1)
                if err == 0:
                    group_results = dict(
                        zip(
                            keys,
                            [v[0] if len(v) == 1 else list(v) for v in group_return],
                        )
                    )
                else:
                    group_results = {key: MultinetError(err) for key in keys}
                group_results = self._convert_ts(keys, group_results)
                rval.update(group_results)
        except IndexError:
            msg = f"One of the parameters is invalid: {entries}"
            self.logger.error(msg)
            raise MultinetError(msg)

        self.logger.debug("rval: %s", rval)
        return rval

    @lru_cache(maxsize=32)
    def get_meta(
        self, *entries: Entry, **kwargs
    ) -> Dict[Entry, Union[Metadata, MultinetError]]:
        """
        Get metadata for ado
        :param ado: Name of ADO; returns list of parameters
        :param param: Name of parameter (optional); returns list of properties & values
        :param all: Returns dict of all parameters, properties, and values (optional)
        :return: list or dict
        """
        # first argument is always ADO
        response = {}
        entries = self._unpack_args(*entries, timestamp_required=False)
        for group in entries:
            ado_handle = list(group.values())[0][0]
            meta = cns.adoMetaData(ado_handle)
            for entry in group.keys():
                if len(entry) == 1:
                    response[entry] = meta
                if len(entry) == 2:
                    response[entry] = meta[(entry[1], "value")]._asdict()
                elif len(entry) == 3:
                    response[entry] = meta[(entry[1], entry[2])]._asdict()
        return response

    def set(self, *entries: Entry, ppm_user=1, **kwargs) -> Optional[MultinetError]:
        """
        Synchronously set ADO parameters
        :param args: One or more tuple(<ado_name>, <parameter_name>, [property_name], <value>); property_name defaults to 'value'
        :param ppm_user: int; PPM User 1 - 8 (default 1)
        :return: True if successful
        """
        if ppm_user < 1 or ppm_user > 8:
            raise MultinetError("PPM User must be 1 - 8")
        request_list = self._unpack_args(
            *entries, timestamp_required=False, is_set=True
        )
        self.logger.debug("request_list: %s", request_list)
        if len(request_list) == 0:
            self.logger.error("Request not created")
            return MultinetError("Error creating request")
        # v17#with self.pyadoLock:
        errors = []
        for group in request_list:
            vals = list(group.values())
            err_code = cns.adoSet(list=vals, ppmIndex=ppm_user - 1)
            if err_code != 0:
                errors.append(f"Error setting {group}: {err_code}")
        return MultinetError("; ".join(errors)) if errors else None

    def cancel_async(self):
        self._async_keymap.clear()
        if self.async_receiver is not None:
            err_code = cns.adoStopAsync(self.async_receiver, 0)
            if err_code is None:
                self.logger.debug("Async server stopped")
                self.async_receiver = None
            else:
                self.logger.error(
                    "Error stopping async server; connections may still be alive! (return code %d)",
                    err_code,
                )
            self.logger.debug(
                "stopped async server, number of threads: %d", threading.active_count()
            )

    # Private Methods
    @staticmethod
    def _convert_ts(entries, data):
        for entry in entries:
            if entry in data and (*entry[:2], "timestampSeconds") in data:
                ts = float(data[(*entry[:2], "timestampSeconds")])
                if (*entry[:2], "timestampNanoSeconds") in data:
                    ts += data[(*entry[:2], "timestampNanoSeconds")] / 1e9
                del (
                    data[(*entry[:2], "timestampNanoSeconds")],
                    data[(*entry[:2], "timestampSeconds")],
                )
                data[(*entry[:2], "timestamp")] = ts
        return data

    def _unpack_args(
        self, *entries, timestamp_required=True, is_set=False
    ) -> List[Dict[Entry, Entry]]:
        entries = [tuple(ent) for ent in entries]
        request_list = []
        entries = groupby(entries, itemgetter(0))
        for ado_name, group_entries in entries:
            group_requests: Dict[Entry, Entry] = {}
            ado_handle = self._get_ado_handle(ado_name)
            if not ado_handle:
                break
            for entry in group_entries:
                param_name = entry[1]
                prop_name = (
                    entry[2]
                    if len(entry) == 4 and is_set or len(entry) == 3 and not is_set
                    else "value"
                )
                if is_set:
                    value = entry[-1]
                    try:
                        group_requests[entry] = (
                            ado_handle,
                            param_name,
                            prop_name,
                            value,
                        )
                    except IndexError:
                        self.logger.error(
                            "Set value missing for %s", str(entry),
                        )
                        return []
                else:
                    if timestamp_required:
                        group_requests[(ado_name, param_name, "timestampSeconds")] = (
                            ado_handle,
                            param_name,
                            "timestampSeconds",
                        )
                        group_requests[
                            (ado_name, param_name, "timestampNanoSeconds")
                        ] = (ado_handle, param_name, "timestampNanoSeconds")

                    group_requests[entry] = (ado_handle, param_name, prop_name)
            request_list.append(group_requests)
        return request_list

    def _get_async_thread(self):
        # separate thread to wait for data
        for data in self.async_receiver.newdata():  # type: ignore
            self._unpack_callback(data)
        self.logger.debug(
            "receiver_thread finished, number of threads: %d", threading.active_count()
        )

    def _unpack_callback(self, *args):
        with self.pyado_lock:
            values = args[0][0]
            tid = args[0][1]
            ppm_user = args[0][3] + 1  # ppmuser should be one for whole group
            sources = [(s[0], s[1], s[2]) for s in args[0][2]]
            sources = [
                self._async_keymap[k] for k in sources if k in self._async_keymap
            ]
            self.logger.debug("_callback_unpacker:sources:\n%s", sources)
            self.logger.debug("_callback_unpacker:values:\n%s", values)
            results = {}
            for i, key in enumerate(sources):
                try:
                    results[key] = (
                        values[i][0] if len(values[i]) == 1 else list(values[i])
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    self.logger.error("in unpackCallback: %s", exc)
                    self.logger.error("values[%i]", len(values))
                    self.logger.error("values[0][%i]", len(values[0]))
                    return

            filtered_results = self._filter_data(results, ppm_user)
            keys_to_remove = [
                [(*key[:2], "timestampSeconds"), ((*key[:2], "timestampNanoSeconds"))]
                for key in results
                if key not in filtered_results
            ]
            for keys in keys_to_remove:
                for key in keys:
                    if key in filtered_results:
                        del filtered_results[key]
            # now call the user callback function
            if filtered_results:
                callback, entries = self.callbacks[tid]
                filtered_results = self._convert_ts(entries, filtered_results)
                callback(filtered_results, ppm_user)

    def _get_ado_handle(self, name):
        """get handle to ADO if it was already created, 
        otherwise, create the new one"""
        if name in self.handles:
            ado_handle = self.handles[name]
        else:
            where = cns.cnslookup(name)
            if where is None:
                self.logger.warning("No such name %s", name)
                return None
            ado_handle = cns.adoName.create(where)
            if not isinstance(ado_handle, cns.adoName):
                self.logger.warning("No such ADO %s", name)
                return None

            # This section is essential only for subsequent adoSet,
            # it will need the metadataDict.
            meta_data = cns.adoMetaData(ado_handle)
            if not isinstance(meta_data, dict):
                self.logger.warning("Invalid metadata %s", meta_data)
                return None

            self.logger.debug("ado created: %s", name)
            self.handles[name] = ado_handle
        return ado_handle


if __name__ == "__main__":
    req = AdoRequest()
    resp = req.get(("simple.test", "charArrayS"))
    assert isinstance(resp[("simple.test", "charArrayS")], list)
