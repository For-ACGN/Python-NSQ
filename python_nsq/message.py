from python_nsq import command

class Message:
    def __init__(self, nsqd_address, timestamp, attempts, id, body, _send, _deadline, _local_address):
        assert isinstance(nsqd_address, str), "nsqd_address is not string"
        assert isinstance(timestamp, int) , "timestamp is not int"
        assert isinstance(attempts, int) , "attempts is not int"
        assert isinstance(id, bytes) , "id is not bytes"
        assert isinstance(body, bytes) , "body is not bytes"
        assert isinstance(_deadline, int) , "_deadline is not int"
        assert isinstance(_local_address, str) , "_local_address is not string"
        self.nsqd_address = nsqd_address
        self.timestamp = timestamp
        self.attempts = attempts
        self.id = id
        self.body = body
        self._send = _send
        self._deadline = _deadline
        self._local_address = _local_address
        
    def finish(self):
        if self._send(command.finish(self.id), self._deadline, self._local_address):
            if self._send(command.ready(10), self._deadline, self._local_address): #maybe error 
                return True            
        else:
            return False
        
    def requeue(self, delay): #ms
        if self._send(command.requeue(self.id, delay), self._deadline, self._local_address):
            return True
        else:
            return False
            
    def touch(self):
        if self._send(command.touch(self.id), self._deadline, self._local_address):
            return True
        else:
            return False