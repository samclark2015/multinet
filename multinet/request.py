from abc import ABC, abstractmethod
from typing import *
import logging

Entry = tuple
Metadata = Dict[str, Any]
Callback = Callable[[Dict[Entry, Any], int], None]


class Request(ABC):
    """Request interface"""

    @property
    @classmethod
    def logger(cls):
        return logging.getLogger(cls.__name__)

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
