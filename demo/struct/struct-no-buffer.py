import struct
import ctypes
import time

_uint16 = struct.Struct(">H")
_uint16_buffer = ctypes.create_string_buffer(_uint16.size) #size=2 byte

_int32 = struct.Struct(">l")
_int32_buffer = ctypes.create_string_buffer(_int32.size) #size=4 byte

#TODO 判断参数合法性
def uint16_byte(uint16):
    _uint16.pack_into(_uint16_buffer,0,uint16)
    return _uint16_buffer
    #return bytes(_uint16_buffer)

def int32_byte(int32):
    _int32.pack_into(_int32_buffer,0,int32)
    return _int32_buffer
    #return bytes(_int32_buffer)

time.sleep(1)
def _create_struct(fmt):
    return struct.Struct(str(fmt))
struct_l = _create_struct(">l")

start = time.time()
for i in range(0,1000000):
    struct_l.pack(32)
stop= time.time()
print("time:" , stop-start)

time.sleep(1)

start = time.time()
for i in range(0,1000000):
    uint16_byte(32)
stop= time.time()
print("time:" , stop-start)

time.sleep(1)

start = time.time()
for i in range(0,1000000):
    int32_byte(32)
stop= time.time()
print("time:" , stop-start)