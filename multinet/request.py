import logging
import re
import traceback
import warnings
from abc import ABC, abstractmethod
from collections import UserDict
from functools import partial
from typing import *

from cad_io.cns3 import getErrorString

Entry = tuple
Metadata = Dict[str, Any]
Callback = Callable[[Dict[Entry, Any], int], None]
Filter = Callable[[Dict[Entry, Any], int], Dict[Entry, Any]]


class MultinetResponse(UserDict):
    def get_error(self, key: Entry):
        try:
            self[key]
            return None
        except MultinetError as exc:
            return exc

    def get_status(self, key: Entry):
        try:
            self[key]
            return 0
        except MultinetError as exc:
            return exc.error_code

    def get_errors(self):
        return {
            k: self.get(k, False)
            for k in self
            if isinstance(self.get(k, False), MultinetError)
        }

    def get(self, key: Entry, should_raise=True):
        try:
            return super().get(key)
        except MultinetError as exc:
            if should_raise:
                raise
            else:
                return exc

    def __getitem__(self, key: Entry) -> Any:
        key_trans = self._tranform_key(key)

        if any("*" in seg for seg in key_trans):
            return self._get_wildcard(key_trans)

        # Special cases
        if len(key_trans) == 3 and key_trans[2] == "valueAndTime":
            value = self[key_trans[0], key_trans[1], "value"]
            ts_second = self[key_trans[0], key_trans[1], "timestampSeconds"]
            ts_ns = self[key_trans[0], key_trans[1], "timestampNanoSeconds"]
            return (value, ts_second + ts_ns * 1e-9)

        # Fallthrough
        try:
            value = super().__getitem__(key_trans)
        except KeyError:
            raise KeyError(key) from None

        return value

    def __contains__(self, key: Any) -> bool:
        key = self._tranform_key(key)
        return super().__contains__(key)

    def _get_wildcard(self, key: Entry):
        subset = self.copy()
        for i, piece in enumerate(key):
            if "*" in piece:
                r = re.compile(piece.replace("*", ".*"))
                for k in list(subset):
                    if not r.match(k[i]):
                        del subset[k]
            else:
                for k in list(subset):
                    if k[i] != piece:
                        del subset[k]
        return subset

    def _tranform_key(self, key: Entry):
        if isinstance(key, str):
            key = tuple(key.split(":"))  # type: ignore

        if len(key) == 2:
            key = (key[0], key[1], "value")

        return key


class MultinetError(Exception):
    def __init__(self, err):
        err_string = getErrorString(err) if isinstance(err, int) else err
        super().__init__(err_string)


class Request(ABC):
    """Request interface"""

    logger = property(lambda self: logging.getLogger(self.__class__.__name__))

    def __init__(self):
        self._filters: List[Filter] = list()
        self._instance = None
        self._asyncs = []
        self._name = None

    @abstractmethod
    def get(self, *entries: Entry, ppm_user=1, **kwargs) -> Dict[Entry, Any]:
        """Get data from device synchronously

        Arguments:
            *entries (Entry): Entries, in form of (<device>, <param>, <prop>)

        Returns:
            Dict[Entry, Any]: Dictionary of return values
        """
        ...

    @abstractmethod
    def get_async(
        self, callback: Callback, *entries: Entry, immediate=False, ppm_user=1, **kwargs
    ) -> Dict[Entry, MultinetError]:
        """Get data from device asynchronously

        Arguments:
            callback (Callable[[Dict[Entry, Any]], None]): callback with arguments <data>, <ppm_user>
            *entries (Entry): Entries, in form of (<device>, <param>, <prop>)

        Keyword Arguments:
            immediate (bool): should callback be called immediately after get_async (default: False)
            ppm_user (int): which PPM user to listen for asynchronous data on (default: 1)
            timestamp(bool): include timestamp properties (ADOs only) (default: True)
        """
        ...

    @abstractmethod
    def get_meta(
        self, *entries: Entry, ppm_user=1, **kwargs
    ) -> Dict[Entry, Union[Metadata, MultinetError]]:
        """Get metadata for entries

        Arguments:
            *entries (Entry): Entries, in form of (<device>, <param>, <prop>)

        Returns:
            Dict[Entry, Metadata]: Metadata values
        """
        ...

    @abstractmethod
    def set(
        self, *entries: Entry, ppm_user=1, set_hist: Optional[bool] = None, **kwargs
    ) -> Dict[Entry, MultinetError]:
        """Set data

        Arguments:
            *entries (Entry): Entries, in form of (<device>, <param>, <prop>)
            ppm_user (int): PPM user to set (default: 1)
            set_hist (Optional[bool]): Enable/disable set history for this call only; uses global setting by default (default: None)

        Returns:
            bool: did set succeed
        """
        ...

    @abstractmethod
    def cancel_async(self):
        """Cancel all asynchronous requests"""
        ...

    @abstractmethod
    def set_history(self, enabled: bool):
        """Enable/disable set history globally

        Args:
            enabled (bool): Enabled or not
        """

    def add_filter(self, filter_: Filter):
        """Add filter for asynchronous requests

        Arguments:
            filter_ (Filter): filter function
        """
        self._filters.append(filter_)

    def start_asyncs(self):
        """Start serving async data, handled by @async_handler(...) decorated functions"""

        def callback(func, data, cb_ppm_user):
            try:
                func(data, cb_ppm_user)
            except Exception:
                self.logger.warning(f"Error handling callback for {data.keys()}")
                self.logger.info(traceback.format_exc())
                traceback.print_exc()

        for func, entries, ppm_user in self._asyncs:
            func = partial(func, self._instance) if self._instance else func
            func = partial(callback, func)
            if isinstance(ppm_user, Iterable):
                for user in ppm_user:
                    self.get_async(
                        func,
                        *entries,
                        ppm_user=user,
                        grouping="parameter",
                    )
            elif isinstance(ppm_user, int):
                self.get_async(
                    func,
                    *entries,
                    ppm_user=ppm_user,
                    grouping="parameter",
                )

    def async_handler(
        self,
        *entries: Union[Tuple[str, str], Tuple[str, str, str]],
        ppm_user: Union[int, Iterable[int]] = 1,
    ):
        """Function decorator to nicely set up an async handler function for some parameters

        Args:
            entries (Union[Tuple[str, str], Tuple[str, str, str]]): Parameters to receive updates for.
            ppm_user (Union[int, List[int]], optional): PPM user to listen on. May be single int or iterable of ints for multiple users. Defaults to 1.
        """

        def wrapper(func):
            if isinstance(ppm_user, (int, Iterable)):
                self._asyncs.append((func, entries, ppm_user))
            else:
                raise ValueError("PPM User must be int 1 - 8, or list of ints 1 - 8")
            return func

        return wrapper

    def _filter_data(self, data, ppm_user):
        for filter_ in self._filters:
            data = filter_(data, ppm_user)
        return data

    def _parse_entries(self, entries: Iterable[Entry]):
        ret = []
        errors = {}
        for entry in entries:
            if isinstance(entry, str):
                str_split = cast(
                    Union[Tuple[str, str], Tuple[str, str, str]],
                    tuple(entry.split(":")),
                )
                if len(str_split) not in (2, 3):
                    raise ValueError(f"Parameter {entry} too short")
                entry = str_split

            if len(entry) == 2:
                entry = (entry[0], entry[1], "value")

            entry = cast(Tuple[str, str, str], entry)
            # Check for psuedo properties & convert as needed
            if entry[2] == "valueAndTime":
                ret += [
                    (entry[0], entry[1], "value"),
                    (entry[0], entry[1], "timestampSeconds"),
                    (entry[0], entry[1], "timestampNanoSeconds"),
                ]
            elif entry[2] in ("timeInfo", "valueAndTrigger", "valueAndCycle"):
                errors[entry] = MultinetError(f"Pseudo-property {entry[2]} unsupported")
            else:
                ret.append(entry)

        return ret, errors

    ### Private magic-methods for Pythonicness ###
    # __enter__ and __exit__ define context manager (with ...: ...) functionality
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.cancel_async()

    # __get__ and __set_name__ allow Request objects to use @async_handler(...) on instance methods too!
    def __get__(self, instance, objtype=None):
        if hasattr(instance, self._name):
            mreq_inst = getattr(instance, self._name)
        else:
            mreq_inst = self.__class__()
            mreq_inst._instance = instance
            mreq_inst._asyncs = self._asyncs
            setattr(instance, self._name, mreq_inst)
        return mreq_inst

    def __set_name__(self, owner, name):
        self._name = "_{}_instance".format(name)
