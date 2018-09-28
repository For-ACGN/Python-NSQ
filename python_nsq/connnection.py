import _thread
import threading
import socket
import ssl
import json
import traceback

from python_nsq import protocol
from python_nsq import command

from .config import Config
from .config import TLS_Config
from .convert import bytes_int32
from .convert import bytes_string

class Conn:
    def __init__(self, remote_address, config, handler_message, handler_close):
        self.remote_address = remote_address
        self.config = config
        self.local_address = ""
        self.buffer_size = config.output_buffer_size
        self.dial_timeout = config.dial_timeout
        self.read_timeout = config.read_timeout
        self.write_timeout = config.write_timeout
        self.handler_message = handler_message
        self.handler_close = handler_close
        self.conn = None
        self.status = 0  #0=closed 1=connected
        self.status_mutex = threading.Lock()
        self.send_mutex = threading.Lock()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if (isinstance(exc_val, Exception)):
            pass
        self.close()

    def connect(self): #need manual connect(lock need)
        err = self._connect()
        if err != "":
            return err
        err = self.send(protocol.MAGIC_V2)
        if err != "":
            return "send magic v2 error" + err
        err = self._identify()
        if err != "":
            return err
        print("[Python-NSQ] INFO login successfully")
        self.receive_loop()
        return ""

    def close(self):
        self.status_mutex.acquire()
        status = self.status
        self.status_mutex.release()
        if status == 1:
            self.conn.shutdown(2) #close all
            self.conn.close()
            self.status_mutex.acquire()
            self.status = 0
            self.status_mutex.release()
            self.handler_close()

    def _connect(self): #new socket
        try:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conn.settimeout(self.dial_timeout)
            self.conn.connect(self.remote_address)
            local_address = self.conn.getsockname()
            self.local_address = local_address[0]+":"+str(local_address[1])
            self.status_mutex.acquire()
            self.status = 1
            self.status_mutex.release()
        except Exception:
            return traceback.format_exc()#limit=1
        return ""

    def _identify(self):
        err = self.send(command.identify(self.config.encode_identify()))
        if err != "":
            return "send identify message error" + err
        raw_response = self._receive_message()
        if len(raw_response) < protocol.FRAME_TYPE_SIZE:
            return "receive identify response error"
        response = protocol.resolve_response(raw_response)
        frame_type = response[0]
        data = response[1]
        if frame_type == protocol.FRAME_TYPE_RESPONSE:
            pass
        elif frame_type == protocol.FRAME_TYPE_ERROR:
            return bytes_string(data)
        elif frame_type == protocol.FRAME_TYPE_MESSAGE:
            pass
        else:
            return "invaild frame type"
        identify_response = json.loads(data) 
        print("[Python-NSQ] INFO Identify", identify_response)
        if identify_response["tls_v1"]:
            err = self._upgrade_tls()
            if err != "":
                return err
        if identify_response["auth_required"]:
            err = self._auth()
            if err != "":
                return err
        return ""

    def _upgrade_tls(self):
        tls_config = self.config.tls_config
        try:
            self.conn = ssl.wrap_socket(self.conn, tls_config.keyfile, tls_config.certfile, 
            tls_config.server_side, tls_config.cert_reqs, tls_config.ssl_version, tls_config.ca_certs, 
            tls_config.do_handshake_on_connect, tls_config.suppress_ragged_eofs, tls_config.ciphers)
        except Exception:
            return "\n" + traceback.format_exc()#limit=1
        return ""

    def _auth(self):
        if len(self.config.auth_secret) == 0:
            return "auth secret can't be NULL"
        err = self.send(command.auth(self.config.auth_secret))
        if err != "":
            return "send auth message error" + err
        response = self._receive_message()
        if len(response) < protocol.FRAME_TYPE_SIZE:
            return "receive auth message error"
        if response != b"\x00\x00\x00\x00OK":
            return "receive auth ok error " + bytes_string(response[protocol.FRAME_TYPE_SIZE:])
        raw_response = self._receive_message()
        if len(raw_response) < protocol.FRAME_TYPE_SIZE:
            return "receive auth response error"
        response = protocol.resolve_response(raw_response)
        frame_type = response[0]
        data = response[1]
        if frame_type == protocol.FRAME_TYPE_RESPONSE:
            pass
        elif frame_type == protocol.FRAME_TYPE_ERROR:
            return bytes_string(data)
        else:
            return "invaild frame type"
        auth_response = json.loads(data)
        print("[Python-NSQ] INFO Auth:", auth_response)
        return ""

    def send(self, data):
        self.send_mutex.acquire()
        try:
            self.conn.settimeout(self.write_timeout)
            self.conn.sendall(data)#not send()
        except Exception:
            self.close()
            self.send_mutex.release()
            return traceback.format_exc()#limit=1
        self.send_mutex.release()
        return ""

    def receive_loop(self): #start receive loop
        print("[Python-NSQ] INFO start receive thread")
        _thread.start_new_thread(self._receive, ())

    def _receive_message(self): #receive one message
        buffer = b""  #receive buffer
        data = b""
        body_size = 0
        while True:
            try:
                buffer = self.conn.recv(self.buffer_size)
            except Exception:
                traceback.print_exc()
                self.close()
                break
            if len(buffer) == 0: # !!!!!!! No Exception
                self.close()
                break
            data += buffer
            if len(data) < protocol.MESSAGE_SIZE:
                continue
            body_size = bytes_int32(data[:protocol.MESSAGE_SIZE])
            if len(data) < protocol.MESSAGE_SIZE + body_size:
                continue
            return data[protocol.MESSAGE_SIZE:protocol.MESSAGE_SIZE+body_size]
        return b""

    def _receive(self):
        buffer = b""  #receive buffer
        data = b""
        body_size = 0
        while True:
            try:
                buffer = self.conn.recv(self.buffer_size)
            except Exception:
                print(traceback.format_exc()) #limit=1
                break
            if len(buffer) == 0:
                break
            data += buffer
            while True:
                if len(data) < protocol.MESSAGE_SIZE:
                    break
                body_size = bytes_int32(data[:protocol.MESSAGE_SIZE])
                if len(data) < protocol.MESSAGE_SIZE + body_size:
                    break
                self.handler_message(data[protocol.MESSAGE_SIZE:protocol.MESSAGE_SIZE + body_size])
                data = data[protocol.MESSAGE_SIZE + body_size:]
        self.close()
        print("[Python-NSQ] INFO stop receive thread")