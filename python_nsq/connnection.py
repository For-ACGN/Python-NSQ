import socket
import _thread
import threading
import traceback

from .convert import bytes_int32
from .protocol import MESSAGE_SIZE

def get_hostname():
    return socket.gethostname()

class Conn: 
    #handler_message like
    #def handler_message(message):
    #   print()
    #buffer_size= 1024(producer) 4096(consumer)
    def __init__(self, address, buffer_size, handler_message, handler_close): 
        assert isinstance(address,tuple), "address is not tuple"
        assert isinstance(buffer_size,int), "buffer_size is not int"
        #assert isinstance(handler_message,types.FunctionType), "handler_message is not FunctionType"
        self.remote_address = address #tuple
        self.local_address = "" #str is fast 
        self.buffer_size = buffer_size
        self.handler_message = handler_message
        self.handler_close = handler_close
        self.conn = None
        self.status = 0  #0=closed 1=connected
        self.status_lock = threading.Lock()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if (isinstance(exc_val, Exception)):
            pass # 报错
        self.conn.close()

    def connect(self): #need manual connect(lock need)
        try:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conn.connect(self.remote_address)                      
            local = self.conn.getsockname()
            self.local_address = local[0]+":"+str(local[1])
            self.status_lock.acquire()
            self.status = 1
            self.status_lock.release()
            _thread.start_new_thread(self._receive, ())
            return ""
        except Exception:
            traceback.print_exc()
            return "[socket] connect "+self.remote_address[0]+":"+str(self.remote_address[1])+" faild"

    def send(self, data, deadline):    
        try:
            self.conn.settimeout(deadline)
            self.conn.sendall(data)#not send()
            return True
        except Exception:
            traceback.print_exc()
            self.close()
            return False
      
    def _receive(self):
        print("[socket] start receive thread")
        buffer = b""  #receive buffer
        data = b""     
        body_size = 0
        while True:
            try:
                buffer = self.conn.recv(self.buffer_size)
                if len(buffer) == 0:
                    self.close()
                    break
            except Exception:
                traceback.print_exc()
                self.close()
                break 
            else:
                data += buffer
                while True:
                    if len(data) < MESSAGE_SIZE:
                        break
                    body_size = bytes_int32(data[:MESSAGE_SIZE])
                    if len(data) < MESSAGE_SIZE + body_size:
                        break                   
                    self.handler_message(data[MESSAGE_SIZE:MESSAGE_SIZE+body_size], self.local_address)
                    data = data[MESSAGE_SIZE+body_size:]
        print("[socket] stop receive thread")
        #exit thread
    def close(self):
        self.status_lock.acquire()
        status = self.status
        self.status_lock.release()
        if status == 1:
            self.conn.close()
            self.status_lock.acquire()
            self.status = 0
            self.status_lock.release()
            remote_address = self.remote_address[0]+":"+str(self.remote_address[1])
            self.handler_close(self.local_address, remote_address)