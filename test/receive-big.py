import python_nsq

import time
import traceback

def main():
    print("[Python-NSQ] version:", python_nsq.version)
    try:
        consumer = python_nsq.Consumer("test_topic", "acg", handler_message, ["http://192.168.1.11:4161/"], 15)
        consumer.start()
    except Exception:
        traceback.print_exc()
    while True:
        time.sleep(1000)
    consumer.stop()

def handler_message(control, message):
    #control.stop() #control the consumer
    print("[message]")
    print("nsqd_address:", message.nsqd_address)
    print("timestamp:   ", message.timestamp)
    print("id:          ", message.id)
    print("message:     ", len(message.body))
    print(" ")
    message.finish()
    
if __name__ == "__main__":
    main()