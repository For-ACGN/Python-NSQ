import socket
import ssl

from .version import VERSION

class TLS_Config:
    def __init__(self):
        self.keyfile = None
        self.certfile = None
        self.server_side = False
        self.cert_reqs = ssl.CERT_REQUIRED
        self.ssl_version = ssl.PROTOCOL_TLS
        self.ca_certs = None
        self.do_handshake_on_connect = True
        self.suppress_ragged_eofs = True
        self.ciphers = None

class Config:
    def __init__(self):
        #identify_data
        hostname = socket.gethostname()
        self.client_id = hostname
        self.hostname = hostname
        self.user_agent = "Python-NSQ/" + VERSION   
        self.tls_v1 = False
        self.tls_config = TLS_Config()
        self.deflate = False #TODO
        self.deflate_level = 6 # min 1 max 9 default 6
        self.snappy = False #TODO
        self.heartbeat_interval = 15000 #ms
        self.sample_rate = 0 #min 0 max 99
        self.output_buffer_size = 16384 #bytes
        self.output_buffer_timeout = 250 #ms
        self.msg_timeout = 0 #ms
        #auth
        self.auth_secret = ""
        #consumer
        self.max_in_flight = 1 # min 0 default 1
        self.lookupd_poll_interval = 60 #!!!!! second min 10ms max 5 min default 60s
        self.max_requeue_delay = 90000  # min 0 max 60m default 90s
        self.max_attempts = 5 # min 0 max 65535 default 5
        #connection
        self.dial_timeout = 1 #!!!!! second
        self.read_timeout = 60 #!!!!! second min 100ms max 5m
        self.write_timeout = 60 #!!!!! second min 100ms max 5m

    def _validate(self): #type and range   #TODO set anytime
        #identify_data 
        assert isinstance(self.client_id, str), "Config.client_id is not str"
        assert isinstance(self.hostname, str), "Config.hostname is not str"
        assert isinstance(self.user_agent, str), "Config.user_agent is not str"
        assert isinstance(self.tls_v1, bool), "Config.tls_v1 is not bool"
        assert isinstance(self.tls_config, TLS_Config), "Config.tls_config is not TLS_Config"
        assert isinstance(self.deflate, bool), "Config.deflate is not bool"
        assert isinstance(self.deflate_level, int), "Config.deflate_level is not int"
        assert isinstance(self.snappy, bool), "Config.snappy is not bool"
        assert isinstance(self.heartbeat_interval, int), "Config.heartbeat_interval is not int"
        assert isinstance(self.sample_rate, int), "Config.sample_rate is not int"
        assert isinstance(self.output_buffer_size, int), "Config.output_buffer_size is not int"
        assert isinstance(self.output_buffer_timeout, int), "Config.output_buffer_timeout is not int"
        assert isinstance(self.msg_timeout, int), "Config.msg_timeout is not int"
        #auth
        assert isinstance(self.auth_secret, str), "Config.auth_secret is not str"
        #consumer
        assert isinstance(self.max_in_flight, int), "Config.max_in_flight is not int"
        assert isinstance(self.lookupd_poll_interval, int), "Config.lookupd_poll_interval is not int"
        assert isinstance(self.max_requeue_delay, int), "Config.max_requeue_delay is not int"
        assert isinstance(self.max_attempts, int), "Config.max_attempts is not int"
        #connection
        assert isinstance(self.dial_timeout, int), "Config.dial_timeout is not int"
        assert isinstance(self.read_timeout, int), "Config.read_timeout is not int"
        assert isinstance(self.write_timeout, int), "Config.write_timeout is not int"

    def encode_identify(self): #for identify
        self._validate()
        identify_data = {
            'client_id': self.client_id,
            'hostname': self.hostname,
            'user_agent': self.user_agent,
            'tls_v1': self.tls_v1,
            'deflate': self.deflate,
            'deflate_level': self.deflate_level,
            'snappy': self.snappy,
            'feature_negotiation': True,
            'heartbeat_interval': self.heartbeat_interval,
            'sample_rate': self.sample_rate,
            'output_buffer_timeout': self.output_buffer_timeout,
            'output_buffer_size': self.output_buffer_size,      
            'msg_timeout': self.msg_timeout,
            }
        return identify_data