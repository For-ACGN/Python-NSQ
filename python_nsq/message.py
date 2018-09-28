from python_nsq import command

class Message:
    def __init__(self, nsqd_address, timestamp, attempts, id, body, send):
        self.nsqd_address = nsqd_address
        self.timestamp = timestamp
        self.attempts = attempts
        self.id = id
        self.body = body
        self._send = send

    def finish(self):
        self._send(command.finish(self.id))
        #self._send(command.ready(1))

    def requeue(self, delay): #ms
        self._send(command.requeue(self.id, delay))

    def touch(self):
        self._send(command.touch(self.id))