# Multinet for Python

## Purpose

The Multinet package provides Python developers a protocol-agnostic toolkit to access ADOs through the ADO, CDEV, and HTTP (via DeviceServer) protocols.

## Installation

To install into an active virtual environment, add `multinet` to your production.in file and run `cadpip switch p`.

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

**Note:** Device entries in Multinet are specified as tuples.  

For `get()` and `get_async()` requests, this is a 2- or 3-tuple consisting of `(device/ADO, parameter, [property])`.  
For `set()` requests, this is a 3- or 4-tuple consisting of `(device/ADO, parameter, [property], value)`.  
Where `[]` is optional.

These entries are passed in as positional arguments as seen above. Alternately, a list of entries can be passed using the "spreading" (`*`) operator:

``` python
entries = [("simple.test", "sinM"), ("simple.test", "degM")]
request.get(*entries)
```

## API Features

### Multiple Async Callbacks

Different callback functions may be defined for consecutive calls to `get_async`. These callbacks need not worry about race conditions with other callbacks, as they are guaranteed to be run serially. The callback should be a function with the signature:

```python
def cb(data: dict, ppm_user: int): ...
```

### PPM User

Specifying a PPM user is done by passing `ppm_user=1` to set, get, and get_async calls. Passing a value outside the range of `[1, 8]` will raise an exception.

### Timestamps

Timestamps are available under the key `(ado, param, "timestamp")` as a float representing Unix time in seconds with nanosecond resolution, if available. To disable timestamps, pass `timestamp=False` to get/get_async methods (enabled by default).

### Immediate Async

Calls to `get_async` may request an initial dataset to be processed immediately by the callback. Enable by passing `immediate=True` to get_async (disabled by default).

### Handling Errors

Errors getting an entry when calling `.get(...)` will result in the value for that entry being an instance of `MultinetError`.

```python
from multinet import Multirequest, MultinetError

req = Multirequest()
data = req.get(("simple.test", "intS"))
intS = data[("simple.test", "intS")]

if isinstance(intS, MultinetError):
    # handle error
    print("Error getting intS", intS)
else:
    # handle good data
    pass
```

Errors while setting will result in the `.set(...)` call returning a `MultinetError` instance.

```python
from multinet import Multirequest

req = Multirequest()
err = req.get(("simple.test", "intS", 4))

# If err is not None, then it is a MultinetError
if err is not None:
    print("Error setting intS", err)
```

## Async Grouping
Async requests issued to an ADO-type device may be grouped in various ways. This controls how data is returned and processed by the callback function. This is controlled by the `grouping="..."` keyword argument to the `get_async()` method. The options available are:

### "ado"
All data from a given ADO will be returned at once to the callback function. This includes all parameters and properties requested on an ADO. The "trigger" (i.e., what determines when the data is returned) is the first entry for that ADO.
``` python
request.get_async(print, ("simple.test", "sinM"), ("simple.test", "degM"), grouping="ado")
```
The print function will receive a dict like the following every time the "sinM:value" property is updated:
``` python
{("simple.test", "sinM"): 1.0, ("simple.test", "degM"): 90}
```

### "parameter"
All data from a given parameter will be returned at once to the callback function. This includes allproperties requested on a parameter. The "trigger" (i.e., what determines when the data is returned) is the first entry for that parameter.
``` python
request.get_async(print, ("simple.test", "sinM"), ("simple.test", "sinM", "timestampSeconds"), grouping="parameter")
```
The print function will receive a dict like the following every time the "sinM:value" property is updated:
``` python
{("simple.test", "sinM"): 1.0, ("simple.test", "degM", "timestampSeconds"): 1655477543628}
```

### "individual"
Each requested property is returned to the callback function immmediately when its value is updated. This is useful for obtaining the most up-to-date information, but makes correlation more complicated:
``` python
request.get_async(print, ("simple.test", "sinM"), ("simple.test", "sinM", "timestampSeconds"), grouping="parameter")
```
The print function will receive dicts like the following:
``` python
# Received when "sinM:value" is updated
{("simple.test", "sinM"): 1.0} 

# Received when "sinM:timestampSeconds" is updated
{("simple.test", "degM", "timestampSeconds"): 1655477543628} 
```

## Specifying a Protocol

You may not wish to use the flexibility of the Multinet package, and predefine which protocol to use when accessing devices. All request objects use the same interface. Doing this may offer very slight performance gains.

```python
# Explictly use ADO protocol
from multinet import AdoRequest

req = AdoRequest()
req.get(("simple.test", "intS")) #
```

## Implementation Details

Three protocols are implemented: ADO, CDEV, and HTTP. Currently, due to the immature nature of the CDEV Python protocol library, the HTTP interface through DeviceServer is being used for all CDEV requests currently. In the future, direct communication with CDEV devices may be enabled allowing HTTP requests to be used as a fallback.
