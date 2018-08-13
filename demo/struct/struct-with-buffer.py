import struct
import time


_int16 = struct.Struct(">hq")
#print(_int16.pack(32,11))

start = time.time()
for i in range(0,1000000):
    
    _int16.pack(32,11)
stop= time.time()
print("time:" , stop-start)

