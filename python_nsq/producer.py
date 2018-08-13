import threading

from python_nsq import connnection
from python_nsq import command
from python_nsq import protocol
from .channel import Channel
from .convert import int32_bytes
from .convert import uint32_bytes
from .version import VERSION

class Producer:
    def __init__(self,nsqd_tcp_address):
        assert isinstance(nsqd_tcp_address, str) , "nsqd_tcp_address is not string"
        addr = nsqd_tcp_address.split(":")
        self.conn_addr = (addr[0], int(addr[1]))
        self.conn_deadline = 60 # send
        self.conn_buffer_size = 1024
        self.conn = connnection.Conn(self.conn_addr, self.conn_buffer_size, self._router, self._conn_close)
        self.status = 0 #0=disconnect 1=connect
        self.status_lock = threading.Lock()
        self.response = Channel()
        self.send_lock = threading.Lock()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if (isinstance(exc_val, Exception)):
            pass # 报错
        self.stop()

    def _get_status(self):
        self.status_lock.acquire()
        status = self.status
        self.status_lock.release()
        return status
        
    def _set_status(self, status):
        self.status_lock.acquire()
        self.status = status
        self.status_lock.release()

    def _connect(self): #needn't lock
        err = self.conn.connect()
        if err != "":
            print(err)
            return False
        self.conn.send(protocol.MAGIC_V2, self.conn_deadline)
        #test identify_data
        identify_data = {
            'client_id': self.conn.local_address,
            'hostname': connnection.get_hostname(),
            'heartbeat_interval': 5000,
            'feature_negotiation': True,
            'tls_v1': False,
            'snappy': False,
            'deflate': False,
            'deflate_level': 6,
            'output_buffer_timeout': 1000,
            'output_buffer_size': 16384,
            'sample_rate': 0,
            'user_agent': "Python-NSQ/" + VERSION,
            'msg_timeout': 0,
            }
        self.conn.send(command.identify(identify_data), self.conn_deadline)
        self.status = 1
        return True
 
    #local_address is not used -> Connection.py 
    def _router(self, message, local_address): 
        response = protocol.resolve_response(message)
        frame_type = response[0]
        data = response[1]
        if frame_type == protocol.FRAME_TYPE_RESPONSE:
            self._handler_response(data)
        elif frame_type == protocol.FRAME_TYPE_ERROR:
            self._handler_error(data)
        else:
            print("[producer]: invaild frame type")

    def _conn_close(self, local_address, remote_address):
        self._set_status(0)
        self.response.clear()
        print("[producer]: connection " + local_address + " -> " + remote_address + " closed")
    
    def _handler_response(self, response):
        if response == b"OK":
            self._handler_response_ok()
        elif response == b"_heartbeat_":
            self._handler_response_heartbeat()
        elif response[:2] == b"{\"":
            print("[producer]: login successfully")
        else:
            print("[producer]: invaild response")
    
    def _handler_response_ok(self):
        self.response.write("ok")
        
    def _handler_response_heartbeat(self):
        self.send_lock.acquire()
        if not self.conn.send(command.nop(), self.conn_deadline):
            self.send_lock.release()
            print("[producer]: send heartbeat response error")
            return
        self.send_lock.release()
        print("[producer]: send heartbeat response")

    def _handler_error(self, error):
        self.response.write(error)
        print("[producer]: error: ", error)

    def stop(self):
        self.conn.close()
        self._set_status(0)
        self.response.clear()
        print("[producer] stop")
        
    def publish(self, topic, message):
        assert isinstance(topic, str), "topic is not string"
        if not isinstance(message, bytes):
            assert isinstance(message, str), "message is not string or bytes"
        self.send_lock.acquire()
        if not self._get_status():
            if not self._connect():
                self.send_lock.release()
                return 1 #connect error
        if not self.conn.send(command.publish(topic, message), self.conn_deadline):
            self._set_status(0)
            self.send_lock.release()
            return 2    #send error
        if self.response.read() != "ok":
            self.send_lock.release()
            return 3 #receive ok error
        self.send_lock.release()
        return 0

    def multi_publish(self, topic, message):
        assert isinstance(topic, str), "topic is not string"
        assert isinstance(message, list), "message is not list"
        self.send_lock.acquire()
        if not self.status:
            if not self._connect():
                self.send_lock.release()
                return 1 #connect error
        #pack message
        package = b""
        package += uint32_bytes(len(message))
        for i in range(0, len(message)):
            if not isinstance(message[i], bytes):
                assert isinstance(message[i], str), "message is not string or bytes"
            package += int32_bytes(len(message[i])) + message[i]   
        if not self.conn.send(command.multi_publish(topic, package), self.conn_deadline):
            self._set_status(0)
            self.send_lock.release()
            return 2    #send error
        if self.response.read() != "ok":
            self.send_lock.release()
            return 3 #receive ok error
        self.send_lock.release()
        return 0

    def deferred_publish(self, topic, message, delay): #ms
        assert isinstance(topic, str), "topic is not string"
        if not isinstance(message, bytes):
            assert isinstance(message, str), "message is not string or bytes"
        assert isinstance(delay, int), "delay is not int"
        self.send_lock.acquire()
        if not self.status:
            if not self._connect():
                self.send_lock.release()
                return 1 #connect error
        if not self.conn.send(command.deferred_publish(topic, message, delay), self.conn_deadline):  
            self._set_status(0)
            self.send_lock.release()
            return 2    #send error
        if self.response.read() != "ok":
            self.send_lock.release()
            return 3 #receive ok error
        self.send_lock.release()
        return 0