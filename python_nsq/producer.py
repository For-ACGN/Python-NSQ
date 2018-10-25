import threading

from . import connnection
from . import command
from . import protocol

from .config import Config
from .channel import Channel
from .convert import int32_bytes
from .convert import uint32_bytes
from .convert import bytes_string

class Producer:
    def __init__(self, nsqd_tcp_address, config):
        assert isinstance(nsqd_tcp_address, str) , "nsqd_tcp_address is not string"
        assert isinstance(config, Config) , "config is not config.Config"
        self.nsqd_tcp_address = nsqd_tcp_address
        self.config = config
        addr = nsqd_tcp_address.split(":")
        self.conn = connnection.Conn((addr[0], int(addr[1])), self.config, self._router, self._conn_close,self._handle_log)
        self.response = Channel()
        self.status = 0 #0=disconnect 1=connect
        self.status_mutex = threading.Lock()
        self.publish_mutex = threading.Lock()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if (isinstance(exc_val, Exception)):
            pass
        self.stop()

    def _check_connection(self): #don't need mutex
        self.status_mutex.acquire()
        status = self.status
        self.status_mutex.release()
        if not status:
            err = self.conn.connect()
            if err != "":
                return err
            self.status_mutex.acquire()
            self.status = 1
            self.status_mutex.release()
        return ""

    def _router(self, raw):
        response = protocol.resolve_response(raw)
        frame_type = response[0]
        data = response[1]
        if frame_type == protocol.FRAME_TYPE_RESPONSE:
            self._on_response(data)
        elif frame_type == protocol.FRAME_TYPE_ERROR:
            self._on_error(data)
        elif frame_type == protocol.FRAME_TYPE_MESSAGE:
            pass
        else:
            self._handle_log("ERROR", "invaild frame type")

    def _conn_close(self):
        self.status_mutex.acquire()
        self.status = 0
        self.status_mutex.release()
        self.response.close()
        local = self.conn.local_address
        remote = self.conn.remote_address[0] + ":" + str(self.conn.remote_address[1])
        self._handle_log("ERROR", "tcp connection " + local + " -> " + remote + " closed")

    def _on_response(self, response):
        if response == b"OK":
            self._handle_response_ok()
        elif response == b"_heartbeat_":
            self._handle_response_heartbeat()
        else:
            self._handle_log("ERROR", "invaild response")

    def _handle_response_ok(self):
        self.response.write("ok")

    def _handle_response_heartbeat(self):
        err = self.conn.send(command.nop())
        if err != "":
            self._handle_log("ERROR", "send heartbeat response error")
            return
        self._handle_log("DEBUG", "send heartbeat response successfully")

    def _on_error(self, err):
        self.response.write(bytes_string(err))

    def _handle_log(self, level, message):#TODO
        print("[Python-NSQ] " + level + " Producer " + self.config.client_id + " " + message)

    def publish(self, topic, message):
        assert isinstance(topic, str), "topic is not string"
        if not isinstance(message, bytes):
            assert isinstance(message, str), "message is not string or bytes"
        if len(message) == 0:
            return "message size is 0"
        self.publish_mutex.acquire()
        err = self._check_connection()
        if err != "":
            self.publish_mutex.release()
            return err
        err =  self.conn.send(command.publish(topic, message))
        if err != "":
            self.publish_mutex.release()
            return err    #send error
        response = self.response.read()
        if response == None:
            self.publish_mutex.release()
            return "connection has been closed"
        elif response != "ok": #response = error
            self.publish_mutex.release()
            return response 
        self.publish_mutex.release()
        return ""

    def multi_publish(self, topic, message):
        assert isinstance(topic, str), "topic is not string"
        assert isinstance(message, list), "message is not list"
        package = b"" #pack message
        package += uint32_bytes(len(message))
        if len(message) == 0:
            return "message list size is 0"
        for i in range(0, len(message)): 
            if not isinstance(message[i], bytes):
                assert isinstance(message[i], str), "message is not string or bytes"
            if len(message[i]) == 0:
                return "message " +str(i)+ " size is 0"
            package += int32_bytes(len(message[i])) + message[i]
        self.publish_mutex.acquire()
        err = self._check_connection()
        if err != "":
            self.publish_mutex.release()
            return err
        err = self.conn.send(command.multi_publish(topic, package))
        if err != "":
            self.publish_mutex.release()
            return err    #send error
        response = self.response.read()
        if response == None:
            self.publish_mutex.release()
            return "connection has been closed"
        elif response != "ok": #response = error
            self.publish_mutex.release()
            return response 
        self.publish_mutex.release()
        return ""

    def deferred_publish(self, topic, message, delay): #ms
        assert isinstance(topic, str), "topic is not string"
        if not isinstance(message, bytes):
            assert isinstance(message, str), "message is not string or bytes"
        assert isinstance(delay, int), "delay is not int"
        if len(message) == 0:
            return "message size is 0"
        self.publish_mutex.acquire()
        err = self._check_connection()
        if err != "":
            self.publish_mutex.release()
            return err
        err =  self.conn.send(command.deferred_publish(topic, message, delay))
        if err != "":
            self.publish_mutex.release()
            return err    #send error
        response = self.response.read()
        if response == None:
            self.publish_mutex.release()
            return "connection has been closed"
        elif response != "ok": #response = error
            self.publish_mutex.release()
            return response 
        self.publish_mutex.release()
        return ""

    def stop(self):
        self.publish_mutex.acquire()
        self.status_mutex.acquire()
        self.status = 0
        self.status_mutex.release()
        self.conn.close()
        self.response.close()
        self.publish_mutex.release()
        self._handle_log("INFO", "stopped")