import _thread
import threading
import json
import urllib3
import time

from . import connnection
from . import command
from . import protocol
from . import message

from .config import Config
from .convert import bytes_uint16
from .convert import bytes_int64
from .convert import bytes_string

class Consumer:
    def __init__(self, topic, channel, message_handler, config):
        assert isinstance(topic, str), "topic is not string"
        assert isinstance(channel, str), "channel is not string"
        assert isinstance(config, Config) , "config is not config.Config"
        self.topic = topic
        self.channel = channel
        self.message_handler = message_handler
        self.config = config
        self.status = 0 #0=not start 1=start 2=stopped
        self.status_mutex = threading.Lock() #operation mutex
        self.pool_manager = urllib3.PoolManager()
        self.nsqlookupd_http_addresses = []  #[string] "http://a:b/"
        self.nsqlookupd_http_addresses_mutex = threading.Lock()
        self.nsqd_tcp_addresses = {} # {"nsqd_tcp_address" : Client()}
        self.nsqd_tcp_addresses_mutex = threading.Lock()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if (isinstance(exc_val, Exception)):
            pass
        self.stop()

    def get_status(self):
        self.status_mutex.acquire()
        status = self.status
        self.status_mutex.release()
        return status

    def _nsqlookupd_loop(self): #discovery all nsqd address
        while True: #update nsqd_tcp_addresses
            self.status_mutex.acquire()
            if self.status != 1:
                self.status_mutex.release()
                return ""
            self._log_self("INFO", "Discovery NSQD start")
            self.nsqlookupd_http_addresses_mutex.acquire()
            for i in range(0, len(self.nsqlookupd_http_addresses)):
                try:
                    response = self.pool_manager.request("GET", 
                    self.nsqlookupd_http_addresses[i] + "nodes").data.decode("utf-8")
                except Exception:
                    self._log_self("WARNING", "connect nsqlookupd "+ self.nsqlookupd_http_addresses[i] +
                        " failed\n" + traceback.format_exc(limit=1))
                    continue
                nodes =  json.loads(response).get("producers","")
                for j in range(0, len(nodes)):
                    ip = nodes[j]["remote_address"].split(":")[0]
                    port = nodes[j]["tcp_port"]
                    nsqd_tcp_address = ip + ":" + str(port)
                    self.nsqd_tcp_addresses_mutex.acquire()
                    if not nsqd_tcp_address in self.nsqd_tcp_addresses:
                        nsqd = Client(nsqd_tcp_address, self.topic, self.channel, self.config, 
                            self._on_message, self._conn_close, self._log_nsqd)
                        self.nsqd_tcp_addresses[nsqd_tcp_address] = nsqd
                        self.nsqd_tcp_addresses_mutex.release()
                        err = nsqd.start()
                        if err != "":     
                            self._log_self("ERROR", "connect nsqd " + nsqd_tcp_address + " " + err)
                            continue
                    else:
                        self.nsqd_tcp_addresses_mutex.release()
            self.nsqlookupd_http_addresses_mutex.release()
            self._log_self("INFO", "Discovery NSQD finish")
            self.status_mutex.release()
            time.sleep(self.config.lookupd_poll_interval)
        return ""

    def _on_message(self, msg):
        control = Control(self.stop)#set Controller
        self.message_handler(control, msg)

    def _conn_close(self, nsqd_tcp_address): #delete nsqd
        if self.status == 2:
            return
        self.nsqd_tcp_addresses_mutex.acquire()
        del self.nsqd_tcp_addresses[nsqd_tcp_address]
        self.nsqd_tcp_addresses_mutex.release()

    def _log_nsqd(self, nsqd_tcp_address, level, message):#TODO
        print("[Python-NSQ] " + level + " Consumer " + self.config.client_id + " -> NSQD " + 
            nsqd_tcp_address + " " + message)

    def _log_self(self, level, message):#TODO
        print("[Python-NSQ] " + level + " Consumer " + self.config.client_id + " " + message)

    def connect_nsqlookupd(self, nsqlookupd_http_address):
        assert isinstance(nsqlookupd_http_address, str), "nsqlookupd is not string"
        self.status_mutex.acquire()
        self.nsqlookupd_http_addresses_mutex.acquire()
        if self.nsqlookupd_http_addresses.count(nsqlookupd_http_address) > 0:
            self.nsqlookupd_http_addresses_mutex.release()
            self.status_mutex.release()
            return nsqlookupd + " has been exist"
        self.nsqlookupd_http_addresses.append(nsqlookupd_http_address)
        self.nsqlookupd_http_addresses_mutex.release()
        self.status_mutex.release()
        return ""

    def connect_nsqlookupds(self, nsqlookupd_http_addresses):
        assert isinstance(nsqlookupd_http_addresses, list), "nsqlookupd_http_addresses is not list"
        for nsqlookupd_http_address in nsqlookupd_http_addresses:
            err = self.connect_nsqlookupd(nsqlookupd_http_address)
            if err != "":
                return err
        return ""

    def connect_nsqd(self, nsqd_tcp_address):
        assert isinstance(nsqd_tcp_address, str), "nsqd_tcp_address is not str"
        self.status_mutex.acquire()
        self.nsqd_tcp_addresses_mutex.acquire()
        if self.nsqd_tcp_address in self.nsqd_tcp_addresses:
            self.nsqd_tcp_addresses_mutex.release()
            self.status_mutex.release()
            return nsqd_tcp_address + " has been exist"
        nsqd = Client(nsqd_tcp_address, self.topic, self.channel, self.config, 
            self._on_message, self._conn_close, self._log)
        self.nsqd_tcp_addresses[nsqd_tcp_address] = nsqd
        self.nsqd_tcp_addresses_mutex.release()
        err = nsqd.start()
        if err != "":
            self.status_mutex.release()
            return err
        self.status_mutex.release()
        return ""

    def connect_nsqds(self, nsqd_tcp_addresses):
        assert isinstance(nsqd_tcp_addresses, list), "nsqd_tcp_addresses is not list"
        for nsqd_tcp_address in nsqd_tcp_addresses:
            err = self.connect_nsqd(nsqd_tcp_address)
            if err != "":
                return err
        return ""

    def disconnect_nsqlookupd(self, nsqlookupd):
        self.status_mutex.acquire()
        if self.status != 1:
            self.status_mutex.release()
            return "Consumer has been stopped"
        self.nsqlookupd_http_addresses_mutex.acquire()
        try:
            self.nsqlookupd_http_addresses.index(nsqlookupd)
        except:
            self.nsqlookupd_http_addresses_mutex.release()
            self.status_mutex.release()
            return nsqlookupd + " doesn't exist"
        self.nsqlookupd_http_addresses.remove(nsqlookupd)
        self.nsqlookupd_http_addresses_mutex.release()
        self.status_mutex.release()
        return ""

    def disconnect_nsqd(self, nsqd_tcp_address):
        self.status_mutex.acquire()
        if self.status != 1:
            self.status_mutex.release()
            return "Consumer has been stopped"
        self.nsqd_tcp_addresses_mutex.acquire()
        if not self.nsqd_tcp_address in self.nsqd_tcp_addresses:
            self.nsqd_tcp_addresses_mutex.release()
            self.status_mutex.release()
            return nsqd_tcp_address + " doesn't exist"
        self.nsqd_tcp_addresses[nsqd_tcp_address].stop()
        del self.nsqd_tcp_addresses[nsqd_tcp_address]
        self.nsqd_tcp_addresses_mutex.release()
        self.status_mutex.release()
        return ""

    def start(self):
        self.status_mutex.acquire()
        if self.status == 1:
            self.status_mutex.release()
            return "Consumer is start"
        if not protocol.check_name(self.topic):
           self.status_mutex.release()
           return "invaild topic"
        if not protocol.check_name(self.channel):
           self.status_mutex.release()
           return "invaild channel"
        self.status = 1
        self._log_self("INFO", "start")
        self.nsqd_tcp_addresses = {}
        self.status_mutex.release()
        return self._nsqlookupd_loop()

    def stop(self):
        self.status_mutex.acquire()
        if self.status != 1:
            self.status_mutex.release()
            return
        self.nsqlookupd_http_addresses.clear()
        nsqd_tcp_addresses = []
        for nsqd_tcp_address in self.nsqd_tcp_addresses:
            nsqd_tcp_addresses.append(nsqd_tcp_address)
        for i in range(0, len(nsqd_tcp_addresses)):
            self.nsqd_tcp_addresses[nsqd_tcp_addresses[i]].stop()
        del self.nsqd_tcp_addresses
        self.status = 2
        self.status_mutex.release()
        self._log_self("INFO", "stopped")

class Client: #client for nsqd
    def __init__(self, nsqd_tcp_address, topic, channel, config, on_message, conn_close, log):
        self.nsqd_tcp_address = nsqd_tcp_address
        self.topic = topic
        self.channel = channel
        self.config = config
        addr = nsqd_tcp_address.split(":")
        self.conn = connnection.Conn((addr[0], int(addr[1])), self.config, self._router, self._conn_close, self._log)
        self.status_mutex = threading.Lock()
        self.callback_on_message = on_message
        self.callback_conn_close = conn_close
        self.callback_log = log

    def __exit__(self, exc_type, exc_val, exc_tb):
        if (isinstance(exc_val, Exception)):
            pass
        self.stop()

    def _router(self, raw):
        response = protocol.resolve_response(raw)
        frame_type = response[0]
        data = response[1]
        if frame_type == protocol.FRAME_TYPE_MESSAGE:
            self._on_message(data)
        elif frame_type == protocol.FRAME_TYPE_RESPONSE:
            self._on_response(data)
        elif frame_type == protocol.FRAME_TYPE_ERROR:
            self._on_error(data)
        else:
            self.callback_log(self.nsqd_tcp_address,"ERROR", "invaild frame type")

    def _conn_close(self):
        local = self.conn.local_address
        remote = self.conn.remote_address[0] + ":" + str(self.conn.remote_address[1])
        self.callback_log(self.nsqd_tcp_address,"ERROR", "tcp connection " + local + " -> " + remote + " closed")
        self.callback_conn_close(self.nsqd_tcp_address)

    def _on_message(self, data):
        timestamp = bytes_int64(data[:8])
        attempts = bytes_uint16(data[8:10])
        id = data[10:26]
        body = data[26:]
        msg = message.Message(self.nsqd_tcp_address, timestamp, attempts, id, body, self.conn.send)
        if attempts > self.config.max_attempts:
            msg.finish()
            return
        self.callback_on_message(msg)

    def _on_response(self, response):
        if response == b"OK":
            self._handler_response_ok()
        elif response == b"_heartbeat_":
            self._handler_response_heartbeat()
        else:
            self.callback_log(self.nsqd_tcp_address,"ERROR", "invaild response")

    def _handler_response_ok(self):
        pass

    def _handler_response_heartbeat(self):
        err = self.conn.send(command.nop())
        if err != "":
            self.callback_log(self.nsqd_tcp_address,"ERROR", "send heartbeat response error")
            return
        self.callback_log(self.nsqd_tcp_address,"DEBUG", "send heartbeat successfully")

    def _on_error(self, err):
        self.callback_log(self.nsqd_tcp_address,"ERROR", bytes_string(err))
        
    def _log(self, level, log): # receive connection
        self.callback_log(self.nsqd_tcp_address, level, log)

    def start(self):
        err = self.conn.connect()
        if err != "":
            return err
        err = self.conn.send(command.subscribe(self.topic, self.channel))
        if err != "":
            return err
        self.conn.send(command.ready(self.config.max_in_flight))
        if err != "":
            return err
        return ""

    def stop(self):
        self.conn.close()
        self.callback_log(self.nsqd_tcp_address,"INFO", "stopped")

class Control:
    def __init__(self, stop):
        self._stop = stop

    def stop(self):
        self._stop()