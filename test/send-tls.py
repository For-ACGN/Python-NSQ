import python_nsq
import time

def main():
    print("[Python-NSQ] version:", python_nsq.version)
    config = python_nsq.Config()
    config.tls_v1 = True
    config.tls_config.ca_certs = "ca.crt"   
    config.auth_secret = "python_nsq"
    producer = python_nsq.Producer("192.168.1.11:4150", config)
    producer2 = python_nsq.Producer("192.168.1.11:4153", config)
    while True:
        err = producer.publish("test_topic", b"acg")
        if err != "":
            print(err)     
        else:
            print("producer publish successfully")
        err = producer2.publish("test_topic", b"acg")
        if err != "":
            print(err)
        else:
            print("producer2 publish successfully")
        """
        err = producer.multi_publish("test_topic", [b"acg", b"acgn", b"hello"])
        if err != "":
            print(err)     
        else:
            print("multi publish successfully")

        err = producer.deferred_publish("test_topic", b"delay", 5000) #5000ms
        if err != "":
            print(err)     
        else:
            print("deferred publish successfully")
        """
        print("----------------------------------")
        time.sleep(5)
    producer.stop()

if __name__ == "__main__":
    main()
    
"""
    while True:
        producer.publish("test_topic", b"acg")
        producer2.publish("test_topic", b"acg")
        producer.multi_publish("test_topic", [b"acg", b"acgn", b"hello"])
        producer2.multi_publish("test_topic", [b"acg", b"acgn", b"hello"])
        producer.deferred_publish("test_topic", b"delay", 5000)
        producer2.deferred_publish("test_topic", b"delay", 5000)
        continue
"""