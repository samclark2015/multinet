from abc import ABC, abstractmethod
from typing import *
import logging

Entry = tuple
Metadata = Dict[str, Any]
Callback = Callable[[Dict[Entry, Any], int], None]
Filter = Callable[[Dict[Entry, Any], int], Dict[Entry, Any]]


class Request(ABC):
    """Request interface"""

    logger = property(lambda self: logging.getLogger(self.__class__.__name__))

    def __init__(self):
        self._filters: List[Filter] = list()

    @abstractmethod
    def get(self, *entries: Entry, **kwargs) -> Dict[Entry, Any]:
        """Get data from device synchronously
        
        Arguments:
            *entries {Entry} -- Entries, in form of (<device>, <param>, <prop>)
        
        Returns:
            Dict[Entry, Any] -- Dictionary of return values
        """
        ...

    @abstractmethod
    def get_async(self, callback: Callback, *entries: Entry, **kwargs) -> None:
        """Get data from device asynchronously
        
        Arguments:
            callback {Callable[[Dict[Entry, Any]], None]} -- callback with arguments <data>, <ppm_user>
            *entries {Entry} -- Entries, in form of (<device>, <param>, <prop>)
        """
        ...

    @abstractmethod
    def get_meta(self, *entries: Entry, **kwargs) -> Dict[Entry, Metadata]:
        """Get metadata for entries

        Arguments:
            *entries {Entry} -- Entries, in form of (<device>, <param>, <prop>)
        
        Returns:
            Dict[Entry, Metadata] -- Metadata values
        """
        ...

    @abstractmethod
    def set(self, *entries: Entry, **kwargs) -> bool:
        """Set data
        
        Arguments:
            *entries {Entry} -- Entries, in form of (<device>, <param>, <prop>)
        
        Returns:
            bool -- [description]
        """
        ...

    @abstractmethod
    def cancel_async(self):
        """Cancel all asynchronous requests"""
        ...

    def add_filter(self, filter_: Filter):
        self._filters.append(filter_)

    def _filter_data(self, data, ppm_user):
        for filter_ in self._filters:
            data = filter_(data, ppm_user)
        return data
