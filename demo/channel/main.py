import threading
import _thread
import time

class Channel:
    def __init__(self):
        self.data = None
        self.cond = threading.Condition()

    def read(self):
        self.cond.acquire()
        if not self.data:
            self.cond.wait()
        data = self.data
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

def read(chan):
    print(chan.read())
    time.sleep(1)
    print(chan.read())
        
def main():
    chan = Channel()
    _thread.start_new_thread(read, (chan,))
    time.sleep(1)
    chan.write(1)
    chan.write(2)
    chan.write(3)
    print(chan.read())

if __name__ == "__main__":
    main()