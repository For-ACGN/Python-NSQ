import json

from .convert import int32_bytes
from .convert import string_bytes

BYTE_SPACE = b" "
BYTE_NEWLINE = b"\n"

#General
IDENTIFY = b'IDENTIFY'
AUTH = b"AUTH"
NOP = b"NOP"
CLS = b"CLS"
#Producer
PUB = b"PUB"
MPUB = b"MPUB"
DPUB = b"DPUB"
#consumer
SUB = b"SUB"
RDY = b"RDY"
FIN = b"FIN"
REQ = b"REQ"
TOUCH = b"TOUCH"
#not used
PING = b"PING"
REGISTER = b"REGISTER"
UNREGISTER = b"UNREGISTER"

def _command(cmd, params, body): #pack command
    byte_params = b""
    byte_body = b""
    for i in range(0 , len(params)):
        byte_params += BYTE_SPACE + string_bytes(params[i])
    if body:
      assert isinstance(body, bytes), "body must be a bytestring"
      byte_body = int32_bytes(len(body)) + string_bytes(body)
      #byte_body.join((int32_bytes(len(body)) , string_bytes(body)))
    return b"".join((cmd, byte_params, BYTE_NEWLINE, byte_body))

def identify(data):
    return _command(IDENTIFY, [], string_bytes(json.dumps(data)))

def auth(secret):
    return _command(AUTH, [], string_bytes(secret))

def nop():
    return _command(NOP, [], None)

def ping():
    return _command(PING, [], None)

def start_close():
    return _command(CLS, [], None)

def publish(topic, data):
    return _command(PUB, [topic], string_bytes(data))

def multi_publish(topic, data):
    return _command(MPUB, [topic], string_bytes(data))

def deferred_publish(topic, data, delay):
    return _command(DPUB, [topic, str(delay)], string_bytes(data))

def subscribe(topic, channel):
    return _command(SUB, [topic, channel], None)

def ready(count):
    return _command(RDY, [str(count)], None)

def finish(id):
    return _command(FIN, [id], None)

def requeue(id, delay):
    return _command(REQ, [id, str(delay)], None)

def touch(id):
    return _command(TOUCH, [id], None)