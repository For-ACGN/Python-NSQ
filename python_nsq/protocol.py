import re

from .convert import bytes_int32

#client types
MAGIC_V2 = b"  V2"

#message
MESSAGE_SIZE = 4 #int32
FRAME_TYPE_SIZE = 4 #int32

#frame types
FRAME_TYPE_RESPONSE = 0
FRAME_TYPE_ERROR = 1
FRAME_TYPE_MESSAGE = 2

#valid topic channel name
VALID_NAME_REGEX = re.compile(r"^[\.a-zA-Z0-9_-]+(#ephemeral)?$") 

#check a topic or channel name for correctness
def check_name(name):
    assert isinstance(name, str) , "name is not string"
    if  len(name) > 64 or len(name) < 1:
        return False
    return VALID_NAME_REGEX.match(name)

def resolve_response(message):
    frame_type = bytes_int32(message[:FRAME_TYPE_SIZE])
    return (frame_type, message[FRAME_TYPE_SIZE:])