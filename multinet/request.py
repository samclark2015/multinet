from abc import ABC, abstractmethod
from functools import partial
import traceback
from typing import *
import logging
from cad_io.cns3 import getErrorString

Entry = tuple
Metadata = Dict[str, Any]
Callback = Callable[[Dict[Entry, Any], int], None]
Filter = Callable[[Dict[Entry, Any], int], Dict[Entry, Any]]


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
        """Start serving async data, handled by @async_handler(...) decorated functions
        """

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
                        func, *entries, ppm_user=user, grouping="parameter",
                    )
            elif isinstance(ppm_user, int):
                self.get_async(
                    func, *entries, ppm_user=ppm_user, grouping="parameter",
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
