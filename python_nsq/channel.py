import threading


class Channel:
    def __init__(self):
        self.data = None
        self.cond = threading.Condition()

    def read(self):
        self.cond.acquire()
        if not self.data:
            self.cond.wait()
        data = self.data
        self.data = None
        self.cond.notify()
        self.cond.release()
        return data

    def write(self, data):
        self.cond.acquire()
        if self.data:
            self.cond.wait()
        self.data = data
        self.cond.notify()
        self.cond.release()

    def close(self):
        self.cond.acquire()
        self.data = None
        self.cond.notifyAll()
        self.cond.release()
