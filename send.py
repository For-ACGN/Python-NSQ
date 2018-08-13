import python_nsq

import time
import traceback

def main():
    print("[Python-NSQ] version:", python_nsq.version)
    producer = python_nsq.Producer("192.168.1.11:4150")
    producer2 = python_nsq.Producer("192.168.1.11:4153")
    while True:              
        for i in range(0,1):
            err = producer.publish("test_topic", b"acg")
            if err != 0:
                print("producer1 publish error")     
            else:
                #print("producer1 publish successfully")
                pass
            err = producer2.publish("test_topic", b"acg")
            if err != 0:
                print("producer2 publish error")
            else:
                #print("producer2 publish successfully")
                pass
        time.sleep(1)
        #producer.stop()
        #producer2.stop()
    try:
        pass
    except Exception:
        traceback.print_exc()
    
if __name__ == "__main__":
    main()

"""
            err = producer.multi_publish("test_topic", [b"acg", b"acgn", b"hello"])
            if err != 0:
                print("multi publish error")
            err = producer.deferred_publish("test_topic", b"delay", 5000) #5000ms
            if err != 0:
                print("deferred publish error")
"""