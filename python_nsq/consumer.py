import _thread
import threading
import urllib3
import time
import json

from python_nsq import connnection
from python_nsq import command
from python_nsq import protocol
from python_nsq import convert
from python_nsq import message
from .version import VERSION

class Consumer:
    def __init__(self, topic, channel, message_handler, nslookupd_http_addresses, nslookupd_poll_interval):
        assert isinstance(topic, str), "topic is not string"
        assert isinstance(channel, str), "channel is not string"
        #assert message_handler
        assert isinstance(nslookupd_http_addresses, list), "nsqd_tcp_address is not list"
        assert isinstance(nslookupd_poll_interval, int), "nslookupd_poll_interval is not int"
        self.topic = topic
        self.channel = channel
        self.message_handler = message_handler
        self.nslookupd_poll_interval = nslookupd_poll_interval
        self.conn_deadline = 60 # send
        self.conn_buffer_size = 4096  
        self.status = 0 #0=disconnect 1=connect
        self.status_lock = threading.Lock()
        self.pool_manager = urllib3.PoolManager() #http pool manager
        self.nslookupd_http_addresses = nslookupd_http_addresses  #[string] "http://192.168.1.11:4161/"
        self.nslookupd_http_addresses_mutex = threading.Lock()
        self.nsqd_tcp_addresses = []    #[{"nslookupd_http_address": "http://", "nsqd_tcp_address": "a:b", "connected":  True}]
        self.nsqd_tcp_addresses_mutex = threading.Lock()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if (isinstance(exc_val, Exception)):
            pass # 报错
        self.stop()

    def _get_status(self):
        self.status_lock.acquire()
        status = self.status
        self.status_lock.release()
        return status
        
    def _set_status(self, status): #0=stop 1=start
        self.status_lock.acquire()
        self.status = status
        self.status_lock.release()

    def _nslookupd_loop(self): #discovery all nsqd tcp address
        while self._get_status():
            self.nslookupd_http_addresses_mutex.acquire()
            nslookupd_http_addresses = self.nslookupd_http_addresses
            self.nslookupd_http_addresses_mutex.release()
            #clear nsqd_tcp_addresses
            self.nsqd_tcp_addresses_mutex.acquire()
            i = 0
            while i < len(self.nsqd_tcp_addresses):
                element = self.nsqd_tcp_addresses[i]
                if element["connected"] == False:
                    self.nsqd_tcp_addresses.remove(element)
                    i -= 1
                i += 1
            self.nsqd_tcp_addresses_mutex.release()
            #update nsqd_tcp_addresses
            for i in range(0, len(nslookupd_http_addresses)): 
                nslookupd_http_address = nslookupd_http_addresses[i]
                response = self.pool_manager.request("GET", nslookupd_http_address + "nodes").data.decode("utf-8")
                nodes =  json.loads(response).get("producers","")
                self.nsqd_tcp_addresses_mutex.acquire()
                for j in range(0, len(nodes)):
                    ip = nodes[j]["remote_address"].split(":")[0]
                    port = nodes[j]["tcp_port"]
                    conn_addr = (ip, port)
                    nsqd = {
                    "nslookupd_http_address" : nslookupd_http_address,
                    "nsqd_tcp_address" : ip + ":" + str(port),                   
                    "connection" : connnection.Conn(conn_addr, self.conn_buffer_size, self._router, self._conn_close),
                    "local_address" : "",
                    "connected" : False,
                    }
                    if not self._check_nsqd_exist(nsqd):
                        self.nsqd_tcp_addresses.append(nsqd)
                self.nsqd_tcp_addresses_mutex.release()
            #connect new nsqd
            self.nsqd_tcp_addresses_mutex.acquire()
            for i in range(0, len(self.nsqd_tcp_addresses)): 
                if self.nsqd_tcp_addresses[i]["connected"] == False:
                    if self._connect_nsqd(self.nsqd_tcp_addresses[i]["connection"]):
                        local_address = self.nsqd_tcp_addresses[i]["connection"].local_address
                        self.nsqd_tcp_addresses[i]["local_address"] = local_address
                        self.nsqd_tcp_addresses[i]["connected"] = True
            self.nsqd_tcp_addresses_mutex.release()
            time.sleep(self.nslookupd_poll_interval)

    def _check_nsqd_exist(self, nsqd): #needn't mutex
        for i in range(0, len(self.nsqd_tcp_addresses)):
            src_nslookupd = nsqd["nslookupd_http_address"]
            src_nsqd = nsqd["nsqd_tcp_address"]
            dst_nslookupd = self.nsqd_tcp_addresses[i]["nslookupd_http_address"]
            dst_nsqd = self.nsqd_tcp_addresses[i]["nsqd_tcp_address"]
            if src_nslookupd == dst_nslookupd and src_nsqd == dst_nsqd:
                return True
        return False
        
    def _connect_nsqd(self, conn):
        err = conn.connect()
        if err != "":
            print(err)
            return False
        conn.send(protocol.MAGIC_V2, self.conn_deadline)
        #test identify_data
        identify_data = {
            'client_id': conn.local_address,
            'hostname': connnection.get_hostname(),
            'heartbeat_interval': 15000,
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
        conn.send(command.identify(identify_data), self.conn_deadline)
        conn.send(command.subscribe(self.topic, self.channel), self.conn_deadline)
        conn.send(command.ready(1), self.conn_deadline)
        return True
    
    def _send(self, message, deadline, local_address):
        conn = None
        self.nsqd_tcp_addresses_mutex.acquire()
        for i in range(0, len(self.nsqd_tcp_addresses)):
            if self.nsqd_tcp_addresses[i]["local_address"] == local_address:
                conn = self.nsqd_tcp_addresses[i]["connection"]
                break
        self.nsqd_tcp_addresses_mutex.release()
        if not conn:
            return False
        if conn.send(message, deadline):
            return True
        else:
            return False

    def _get_nsqd_tcp_address(self, local_address): #local_address -> remote_address(nsqd_tcp_address)
        nsqd_tcp_address = ""
        self.nsqd_tcp_addresses_mutex.acquire()
        for i in range(0, len(self.nsqd_tcp_addresses)):
            if self.nsqd_tcp_addresses[i]["local_address"] == local_address:
                nsqd_tcp_address = self.nsqd_tcp_addresses[i]["nsqd_tcp_address"]
                break
        self.nsqd_tcp_addresses_mutex.release()
        return nsqd_tcp_address
        
    def _router(self, message, local_address):
        nsqd_tcp_address = self._get_nsqd_tcp_address(local_address)
        response = protocol.resolve_response(message)
        frame_type = response[0]
        data = response[1]
        if frame_type == protocol.FRAME_TYPE_MESSAGE:
            self._handler_message(data, local_address, nsqd_tcp_address)
        elif frame_type == protocol.FRAME_TYPE_RESPONSE:
            self._handler_response(data, local_address, nsqd_tcp_address)
        elif frame_type == protocol.FRAME_TYPE_ERROR:
            self._handler_error(data, local_address, nsqd_tcp_address)
        else:
            print("[consumer] invaild frame type")

    def _handler_message(self, data, local_address, nsqd_tcp_address):
        timestamp = convert.bytes_int64(data[:8])
        attempts = convert.bytes_uint16(data[8:10])
        id = data[10:26]
        body = data[26:]
        msg = message.Message(nsqd_tcp_address, timestamp, attempts, id, body, self._send, self.conn_deadline, local_address)
        control = Control(self.stop)#set Controller
        self.message_handler(control, msg)
        
    def _handler_response(self, response, local_address, nsqd_tcp_address):
        if response == b"OK":
            self._handler_response_ok(local_address, nsqd_tcp_address)
        elif response == b"_heartbeat_":
            self._handler_response_heartbeat(local_address, nsqd_tcp_address)
        elif response[:2] == b"{\"":
            print("[consumer]: connect " + nsqd_tcp_address + " successfully")
        else:
            print("[consumer]: invaild response")
    
    def _handler_response_ok(self, local_address, nsqd_tcp_address):
        #print("ok")
        pass
        
    def _handler_response_heartbeat(self, local_address, nsqd_tcp_address):
        if not self._send(command.nop(), self.conn_deadline, local_address): #get tcp addr
            print("[consumer]: " + local_address+" -> "+nsqd_tcp_address, "send heartbeat response error")
        else:
            print("[consumer]: " + local_address+" -> "+nsqd_tcp_address, "send heartbeat response")

    def _handler_error(self, error, local_address, nsqd_tcp_address):
        print("error: ", local_address+" -> "+nsqd_tcp_address, error)

    def _conn_close(self, local_address, remote_address):
        #delete nsqd 
        self.nsqd_tcp_addresses_mutex.acquire()
        i = 0
        while i < len(self.nsqd_tcp_addresses):
            element = self.nsqd_tcp_addresses[i]
            if element["local_address"] == local_address:
                self.nsqd_tcp_addresses.remove(element)
                i -= 1
            i += 1
        self.nsqd_tcp_addresses_mutex.release()
        print("[consumer]: connection " + local_address + " -> " + remote_address + " closed")
        
    def start(self):
        self._set_status(1)
        _thread.start_new_thread(self._nslookupd_loop, ())

    def stop(self):
        self._set_status(0)
        self.nsqd_tcp_addresses_mutex.acquire()
        for i in range(0, len(self.nsqd_tcp_addresses)):
            self.nsqd_tcp_addresses[i]["connection"].close()
        self.nsqd_tcp_addresses_mutex.release()
        print("[consumer] stop")

class Control:
    def __init__(self, stop):
        self.stop = stop