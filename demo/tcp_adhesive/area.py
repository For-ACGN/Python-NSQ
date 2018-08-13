import _thread
import time
import threading

class AAA:
    lock = threading.Lock()
    aaa = 0
    def add(self):
        for i in range(0,500000):
            self.lock.acquire()
            self.aaa += 1
            self.lock.release()
    
    def pr(self):
        print(self.aaa)

    def start(self):
        _thread.start_new_thread(self.add,())
        _thread.start_new_thread(self.add,())
        
def main():
    aaaa = AAA()
    aaaa.start()
    time.sleep(5)
    aaaa.pr()

if __name__ == "__main__":
    main()