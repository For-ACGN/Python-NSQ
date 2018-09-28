import struct

_int16 = struct.Struct(">h")             #2 byte
_uint16 = struct.Struct(">H")            #2 byte
_int32 = struct.Struct(">l")              #4 byte
_uint32 = struct.Struct(">L")             #4 byte
_int64 = struct.Struct(">q")             #8 byte
_uint64 = struct.Struct(">Q")            #8 byte

#number to bytes
def int16_bytes(int16):
    return _int16.pack(int16)

def uint16_bytes(uint16):
    return _uint16.pack(uint16)

def int32_bytes(int32):
    return _int32.pack(int32)

def uint32_bytes(uint32):
    return _uint32.pack(uint32)

def int64_bytes(int64):
    return _int64.pack(int64)

def uint64_bytes(uint64):
    return _uint64.pack(uint64)

#bytes to number
def bytes_int16(bytes):
    if len(bytes) != 2:
        raise Exception("bytes length is not 2")
    return _int16.unpack(bytes)[0]

def bytes_uint16(bytes):
    if len(bytes) != 2:
        raise Exception("bytes length is not 2")
    return _uint16.unpack(bytes)[0]

def bytes_int32(bytes):
    if len(bytes) != 4:
        raise Exception("bytes length is not 4")
    return _int32.unpack(bytes)[0]

def bytes_uint32(bytes):
    if len(bytes) != 4:
        raise Exception("bytes length is not 4")
    return _uint32.unpack(bytes)[0]

def bytes_int64(bytes):
    if len(bytes) != 8:
        raise Exception("bytes length is not 8")
    return _int64.unpack(bytes)[0]

def bytes_uint64(bytes):
    if len(bytes) != 8:
        raise Exception("bytes length is not 8")
    return _uint64.unpack(bytes)[0]

def string_bytes(string, charset="utf-8", errors="strict"):
    if isinstance(string, bytes):
        return string
    if isinstance(string, str):
        return string.encode(charset, errors)
    raise TypeError("expected bytes or a string, not %r" % type(string))

def bytes_string(b, charset='utf-8'):
    if isinstance(b, str):
        return b
    if isinstance(b, bytes):
        return str(b, encoding=charset)
    raise TypeError("expected bytes or a string, not %r" % type(b))