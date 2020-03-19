# Python Multinet

## Purpose

The Multinet package provides Python developers a protocol-agnostic toolkit to access ADOs through the ADO, CDEV, and HTTP (via DeviceServer) protocols.

## Usage

```python
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
```

## Implementation Details
Three protocols are implemented: ADO, CDEV, and HTTP. Currently, due to the immature nature of the CDEV Python protocol library, the HTTP interface through DeviceServer is being used for all CDEV requests currently. In the future, direct communication with CDEV devices may be enabled allowing HTTP requests to be used as a fallback.