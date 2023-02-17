"""Multinet is used to communicate with devices over various communications protocols

Example::

    from multinet import Multirequest
    
    # Create a multirequest instance
    request = Multirequest()

    # Getting data synchronously from an ADO device and a CDEV device in one call
    request.get(("simple.test", "intS"), ("simple.cdev", "degM"))
    # With a PPM User (this kwarg can be used on any call)
    request.get(("simple.test", "intS"), ppm_user=5)

    # Getting data asynchronously
    def callback(data, ppm_user):
        print(data, ppm_user)

    request.get_async(callback, ("simple.test", "sinM"), ("simple.cdev", "degM"))

    # Setting data
    request.set(("simple.test", "intS", 7), ("simple.cdev", "doubleS", 3.14))
"""

from .ado_request import AdoRequest
from .http_request import HttpRequest
from .multirequest import Multirequest
from .request import MultinetError

__all__ = ["Multirequest", "AdoRequest", "HttpRequest", "MultinetError"]
__docformat__ = "google"