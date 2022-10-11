from itertools import groupby
from operator import itemgetter
from typing import *

from cad_io import cns3

from .request import (Callback, Entry, Metadata, MultinetError,
                      MultinetResponse, Request)


class AdoRequest(Request):
    def transform_data(self, entries, data):
        result = {}
        for (ado, param), props in data.items():
            for prop, value in props.items():
                result[(ado, param, prop)] = value

        return MultinetResponse(result)

    def __init__(self):
        super().__init__()
        self._meta = {}
        self._handles = {}

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
        ppm_user=1,
        timestamp=True,
        immediate=False,
        grouping="parameter",
        **kwargs,
    ) -> Dict[Entry, MultinetError]:
        """
        Get ADO parameters asynchronously

        Arguments:
                callback: Callable object taking arguments results_dict, ppm_user
                args: One or more tuple(<ado>, <parameter>, [property]); property defaults to 'value'
                ppm_user: int; PPM User 1 - 8 (default 1)
                timestamp: boolean; should timestamps be included (default True)
                immediate: boolean; should an initial get be performed immediately (default False)
            grouping: str; how async data should be reported (default "individual")
                Grouping choices:
                "ado": every parameter on the same ADO is reported at the same time
                "parameter": every property on the same parameter is reported at the same time
                "individual" (default): each passed parameter/property is reported individually

        Returns:
            dict: Any errors (empty means success)
        """
        if not callable(callback):
            raise ValueError("Callback must be callable")

        entries, errs = self._parse_entries(entries)

        metadata = self.get_meta(*entries)
        # if any of the meta data was not acquired, assume the device/parameter was not valid/available
        # and remove it from the request that will be sent out
        for dev, value in metadata.items():
            if isinstance(value, MultinetError):
                entries.remove(dev)
                errs[dev] = value
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

        if ppm_user < 1 or ppm_user > 8:
            ppm_user = self.default_ppm_user()
        self.logger.debug("args[%d]: %s", len(entries), entries)

        def transform(entries, data, cb):
            ppm_user = data["ppmuser"] + 1
            del data["ppmuser"]
            data = self.transform_data(entries, data)
            data = self._filter_data(data, ppm_user)
            if data:
                cb(data, ppm_user)

        for group in grouped_entries:
            if immediate:
                callback(
                    self.get(*group, ppm_user=ppm_user, timestamp=timestamp), ppm_user
                )
            self._io.get_async(
                lambda data: transform(entries, data, callback),
                *group,
                timestamp=timestamp,
                ppm_user=ppm_user,
            )
        return errs

    def get(
        self, *entries: Entry, ppm_user=1, timestamp=True, **kwargs
    ) -> Dict[Entry, Any]:
        """
        Get ADO parameters synchronously

        Arguments:
                args: One or more tuple(<ado_name>, <parameter_name>, [property_name]); property_name defaults to 'value'
                timestamp: boolean; should timestamps be included (default True)
                ppm_user: int; PPM User 1 - 8 (default 1)

        Returns:
            Dict[Entry, Any]: values from ADO, MultinetError if errors
        """
        entries, response = self._parse_entries(entries)
        if ppm_user < 1 or ppm_user > 8:
            ppm_user = self.default_ppm_user()

        for ado_name, group in groupby(entries, itemgetter(0)):
            group = list(group)
            handle = self._get_handle(ado_name)
            if not handle:
                response.update(dict.fromkeys(group, MultinetError("Metadata Not Available")))
                continue
            metadata = self.get_meta(*group)
            data, status = cns3.adoGet(list=[(handle, *rest) for _, *rest in group], ppmIndex=ppm_user - 1)
            data_iter = iter(data)
            for entry, st in zip(group, status):
                if st != 0:
                    response[entry] = MultinetError(st)
                    continue
                value = next(data_iter)
                response[entry] = value[0] if metadata[entry]["count"] == 1 else list(value)
        return response

    def get_meta(
        self, *entries: Entry, **kwargs
    ) -> Dict[Entry, Union[Metadata, MultinetError]]:
        """
        Get metadata for ado

        Arguments:
                ado: Name of ADO; returns list of parameters
                param: Name of parameter (optional); returns list of properties & values
                all: Returns dict of all parameters, properties, and values (optional)

        Returns:
            Dict[Entry, Union[MetaData, MultinetError]]: metadata from ADO
        """
        # first argument is always ADO
        entries, response = self._parse_entries(entries)
        for ado_name, group in groupby(entries, itemgetter(0)):
            handle = self._get_handle(ado_name)
            meta, st = cns3.adoMetaData(handle)
            for entry in group:
                if not meta:
                    response[entry] = MultinetError("Metadata not available")
                    continue

                try:
                    if len(entry) == 1:
                        response.update({entry + key: value._asdict() for key, value in meta.items()})
                    if len(entry) == 2:
                        response[entry] = dict(meta[(entry[1], "value")]._asdict())
                    elif len(entry) == 3:
                        response[entry] = dict(meta[(entry[1], entry[2])]._asdict())
                except KeyError:
                    response[entry] = MultinetError("Metadata not available")
        return response

    def set(
        self, *entries: Union[Entry, Dict[Entry, Any]], ppm_user=1, set_hist=None, **kwargs
    ) -> Dict[Entry, MultinetError]:
        """
        Synchronously set ADO parameters

        Arguments:
                args: One or more tuple(<ado_name>, <parameter_name>, [property_name], <value>); property_name defaults to 'value'
                ppm_user (int): PPM User 1 - 8 (default 1)

        Returns:
            bool: True if successful
        """
        if ppm_user < 1 or ppm_user > 8:
            ppm_user = self.default_ppm_user()
        orig_sethist = None
        # Override sethistory for call
        if set_hist is not None:
            # Store original sethist state
            orig_sethist = not cns3.setHistory.storageOff
            # Set overrride
            cns3.keepHistory(set_hist)
        # Call ADO set
        # self._io.set(*entries, ppm_user=ppm_user)
        if isinstance(entries[0], dict):
            values = entries[0].values()
            entries, response = self._parse_entries(entries[0].keys())
            entries = [(*entry, value) for entry, value in zip(entries, values)]
        else:
            entries, response = self._parse_entries(entries)

        for ado_name, group in groupby(entries, itemgetter(0)):
            group = list(group)
            handle = self._get_handle(ado_name)
            keys = [entry[:-1] for entry in group]

            if not handle:
                response.update(dict.fromkeys(keys, MultinetError("Metadata Not Available")))
                continue

            metadata = self.get_meta(*keys)
            _, status = cns3.adoSet(list=[(handle, *rest) for _, *rest in group], ppmIndex=ppm_user - 1)
            for entry, st in zip(keys, status):
                if st != 0:
                    response[entry] = MultinetError(st)
                else: 
                    response[entry] = None

        if orig_sethist is not None:
            # Restore original sethist state if stored
            cns3.keepHistory(orig_sethist)
        return response

    def cancel_async(self):
        # self._io.cancel_async()
        ...

    def set_history(self, enabled):
        cns3.keepHistory(enabled)
    

    def _get_handle(self, name: str):
        if name not in self._handles:
            self._handles[name] = cns3.CreateAdo(name)
        return self._handles[name]


if __name__ == "__main__":
    req = AdoRequest()
    resp = req.get(("simple.test", "charArrayS"))
    assert isinstance(resp[("simple.test", "charArrayS")], list)
