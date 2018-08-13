import socket

class Conn:
    def init(self):
        self.tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.add = 100
    def connect(self):        
        self.tcp.connect(("192.168.1.11",4150))
    def send(self):
        self.tcp.send(b"  V2")
        print(self.tcp.getsockname())

aaa = Conn()
aaa.init()
aaa.connect()
aaa.send()
print(aaa.add)

bbb = Conn()
bbb.init()
bbb.connect()
bbb.send()