import threading
from itertools import groupby
from operator import itemgetter
from functools import lru_cache
from typing import *
from cad_io import cns3 as cns, adoaccess

from .request import Entry, Metadata, Request, Callback, MultinetError


class AdoRequest(Request):
    @staticmethod
    def transform_data(entries, data):
        ret_dict = {}
        flat_data = {
            (dev, param, prop): value
            for dev, param in data
            for prop, value in data[(dev, param)].items()
        }
        # Correlate explicitly requested entries
        for entry in entries:
            device, param = entry[:2]
            prop = entry[2] if len(entry) == 3 else "value"
            error_key = (device, param, "error")
            key = (device, param, prop)
            if error_key in flat_data:
                ret_dict[entry] = MultinetError(flat_data[error_key])
                del flat_data[error_key]
            elif key in flat_data:
                ret_dict[entry] = flat_data[key]
                del flat_data[key]
        # Add in additional implicit entries
        ret_dict.update(flat_data)
        return ret_dict

    def __init__(self):
        super().__init__()
        self._io = adoaccess.IORequest()

    def get_async(
        self,
        callback: Callback,
        *entries: Entry,
        ppm_user=1,
        timestamp=True,
        immediate=False,
        **kwargs,
    ) -> Dict[Entry, MultinetError]:
        """
        Get ADO parameters asynchronously

        Arguments:
        	callback: Callable object taking arguments results_dict, ppm_user
        	args: One or more tuple(<ado_name>, <parameter_name>, [property_name]); property_name defaults to 'value'
        	ppm_user: int; PPM User 1 - 8 (default 1)
        	timestamp: boolean; should timestamps be included (default True)
        	immediate: boolean; should an initial get be performed immediately (default False)

        Returns: 
            dict: Any errors (empty means success)
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

        def transform(entries, data, cb):
            ppm_user = data["ppmuser"] + 1
            del data["ppmuser"]
            data = self.transform_data(entries, data)
            cb(data, ppm_user)

        errs = {}
        for entry in entries:
            self._io.get_async(
                lambda data: transform(entries, data, callback),
                entry,
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
        data = self._io.get(*entries, timestamp=timestamp, ppm_user=ppm_user)
        return self.transform_data(entries, data)

    @lru_cache(maxsize=32)
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
        response = {}
        for ado_name, group in groupby(entries, itemgetter(0)):
            meta = self._io.get_meta(ado_name, all=True)
            for entry in group:
                try:
                    if len(entry) == 1:
                        response[entry] = meta
                    if len(entry) == 2:
                        response[entry] = dict(meta[(entry[1], "value")]._asdict())
                    elif len(entry) == 3:
                        response[entry] = dict(meta[(entry[1], entry[2])]._asdict())
                except KeyError:
                    response[entry] = MultinetError("Metadata not available")
        return response

    def set(self, *entries: Entry, ppm_user=1, **kwargs) -> Dict[Entry, MultinetError]:
        """
        Synchronously set ADO parameters

        Arguments:
        	args: One or more tuple(<ado_name>, <parameter_name>, [property_name], <value>); property_name defaults to 'value'
        	ppm_user (int): PPM User 1 - 8 (default 1)
            
        Returns: 
            bool: True if successful
        """
        if ppm_user < 1 or ppm_user > 8:
            raise MultinetError("PPM User must be 1 - 8")
        # v17#with self.pyadoLock:
        self._io.set(*entries, ppm_user=ppm_user)
        return {}

    def cancel_async(self):
        self._io.cancel_async()


if __name__ == "__main__":
    req = AdoRequest()
    resp = req.get(("simple.test", "charArrayS"))
    assert isinstance(resp[("simple.test", "charArrayS")], list)
