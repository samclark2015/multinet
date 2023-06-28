import math
import time

import numpy as np

import multinet.mock

multinet.mock.set_mock(True)
multinet.mock.add_mock_dynamic("simple.test:degM", lambda data: data[1]["simple.test:degM"] + 1, 1.0, initial_value=0)
multinet.mock.add_mock_dynamic("simple.test:sinM", lambda data: math.sin(math.radians(data[1]["simple.test:degM"])), 1.0, initial_value=0)

from multinet.multirequest import Multirequest

io = Multirequest()

print(io.get_async(print, "simple.test:degM", immediate=True))
print(io.get_async(print, "simple.test:sinM", immediate=True))
print(io.get_async(print, "simple.test:sds", immediate=True))
time.sleep(5.0)