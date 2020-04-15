from multinet import Multirequest
import timeit

params = [('simple.test', 'shortWatchM'), 
 ('simple.test', 'ushortWatchM'), 
 ('simple.test', 'ushortWatchStringM'), 
 ('simple.test', 'stringMonitorM'), 
 ('simple.test', 'menuM'), 
 ('simple.test', 'intM'), 
 ('simple.test', 'longM'), 
 ('simple.test', 'doubleM'), 
 ('simple.test', 'intArrayM'), 
 ('simple.test', 'degM'), 
 ('simple.test', 'sinM'), 
 ('simple.test', 'unixTimeM')]    


req = Multirequest()
responses = req.get(*params)

r = [(param, responses[param], responses[(*param, "timestampSeconds")] + responses[(*param, "timestampNanoSeconds")] / 1e-9) for param in params if param in responses])

print(r)
