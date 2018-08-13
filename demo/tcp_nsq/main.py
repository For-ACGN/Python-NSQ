import socket
import json
import time
import _thread

from _compat import bytes_types
from _compat import to_bytes
from _compat import struct_h, struct_l, struct_q

NL = b'\n'

def _command(cmd, params, body):
    body_data = b''
    params_data = b''
    
    if body:
        assert isinstance(body, bytes_types), 'body must be a bytestring'
        body_bytes = to_bytes(body)  # raises if not convertible to bytes
        body_data = struct_l.pack(len(body)) + body_bytes
    if len(params):
        params = [to_bytes(p) for p in params]
        params_data = b' ' + b' '.join(params)
    return b''.join((cmd, params_data, NL, body_data))
    
def identify(data):
    return _command(b'IDENTIFY', [] ,to_bytes(json.dumps(data)))

identify_data = {
            'short_id': "acg", # TODO remove when deprecating pre 1.0 support
            'long_id': "localhost", # TODO remove when deprecating pre 1.0 support
            'client_id': "test",
            'hostname': "forwhat",
            'heartbeat_interval': 5000,
            'feature_negotiation': True,
            'tls_v1': False,
            'snappy': False,
            'deflate': False,
            'deflate_level': 6,
            'output_buffer_timeout': 1000,
            'output_buffer_size': 16384,
            'sample_rate': 0,
            'user_agent': "new agent",
            'msg_timeout': 0,
            }
        
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("127.0.0.1", 4150))

s.send(b"  V2")   # send 魔术数字 (通信协议版本)

print(identify(identify_data))

s.send(identify(identify_data)) # 认证

msg = s.recv(1024)
print(msg)

time.sleep(1)

#命令
def pub(topic, data):
    return _command(b"PUB" , [topic] , data)

def subscribe(topic, channel):
    return _command(b"SUB", [topic,channel], None)

def ready(count):
    return _command(b"RDY", [str(count)],None)

def finish(id):
    return _command(b"FIN", [id],None)
 
def nop():
    return _command(b"NOP", [],None)
 
#解请求 看返回的类型
def unpack_response(data):
    frame = struct_l.unpack(data[:4])[0]
    return frame, data[4:]

#解消息
def decode_message(data):
    timestamp = struct_q.unpack(data[:8])[0]
    attempts = struct_h.unpack(data[8:10])[0]
    id = data[10:26]
    body = data[26:]
    return id, body, timestamp, attempts

    
def receive(threadName, delay):    #简单的处理 数据到达(TODO 对应解决粘包问题)
    while True:
        msg = s.recv(1024)
        #print(msg)
        resp = unpack_response(msg)
        #print(resp)
        frame_type = int.from_bytes(resp[1][:4], byteorder = 'big')
        #print(frame_type)
        if frame_type == 2: #2 = message
            message = decode_message(resp[1][4:])
            id = message[0]
            body = message[1]
            timestamp = message[2]
            attempts = message[3]
            #print(id,body,timestamp,attempts)
            #print("Message:",body)
            s.send(finish(id)) #finish
              
_thread.start_new_thread(receive,("Thread-1", 2, ))

"""
s.send(pub("test_topic1",b"n"))
time.sleep(1)
s.send(subscribe("test_topic1","acg"))
time.sleep(1)
s.send(ready(1))
time.sleep(1)
"""


start = time.time()
for i in range(0,1000000):
    #s.send(pub("test_topic",b"n"))  
    #pub("test_topic",b"n")
    s.send(b"a")
stop = time.time()
print("time:",stop-start)

#print(msg)

time.sleep(10000)

s.close()