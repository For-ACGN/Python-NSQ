import time
import _thread

def receive():
    while 1:
        try:
            time.sleep(10)
            assert false
        except:
            print("error")
            return
            
def main():
    _thread.start_new_thread(receive,())
    time.sleep(1000)
            
if __name__ == "__main__":
    main()