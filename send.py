import python_nsq
import time
import ssl

def main():
    print("[Python-NSQ] Version:", python_nsq.version)
    config1 = python_nsq.Config()
    config1.client_id = "Producer1"
    config1.tls_v1 = True
    config1.tls_config.ca_certs = "ca.crt"
    #config1.tls_config.ssl_version = ssl.PROTOCOL_TLSv1
    config1.auth_secret = "python_nsq"
    producer1 = python_nsq.Producer("192.168.1.11:4150", config1)
    config2 = python_nsq.Config()
    config2.client_id = "Producer2"
    config2.write_timeout = 2
    config2.read_timeout = 2
    config2.tls_v1 = True
    #config2.tls_config.ssl_version = ssl.PROTOCOL_TLSv1
    config2.tls_config.ca_certs = "ca.crt"
    config2.auth_secret = "python_nsq"
    producer2 = python_nsq.Producer("192.168.1.11:4153", config2)
    """
    while True:
        producer1.publish("test_topic", b"acg")
        producer2.publish("test_topic", b"acg")
        producer1.multi_publish("test_topic", [b"acg", b"acgn", b"hello"])
        producer2.multi_publish("test_topic", [b"acg", b"acgn", b"hello"])
        producer1.deferred_publish("test_topic", b"delay", 5000)
        producer2.deferred_publish("test_topic", b"delay", 5000)
        continue
    """
    while True:
        err = producer1.publish("test_topic", b"acg")
        if err != "":
            print(err)
        #else:
            #print("producer1 publish successfully")
        err = producer2.publish("test_topic", b"acg")
        if err != "":
            print(err)
        #else:
            #print("producer2 publish successfully")
        err = producer1.multi_publish("test_topic", [b"acg", b"acgn", b"hello"])
        if err != "":
            print(err)
        #else:
            #print("producer1 multi publish successfully")
        err = producer2.multi_publish("test_topic", [b"acg", b"acgn", b"hello"])
        if err != "":
            print(err)
        #else:
            #print("producer2 multi publish successfully")
        err = producer1.deferred_publish("test_topic", b"delay", 5000) #5000ms
        if err != "":
            print(err)
        #else:
            #print("producer1 deferred publish successfully")
        err = producer2.deferred_publish("test_topic", b"delay", 5000) #5000ms
        if err != "":
            print(err)
        #else:
            #print("producer2 deferred publish successfully")
        #print("----------------------------------")
        time.sleep(0.1)
    producer.stop()

if __name__ == "__main__":
    main()

"""
    while True:
        producer1.publish("test_topic", b"acg")
        producer2.publish("test_topic", b"acg")
        producer1.multi_publish("test_topic", [b"acg", b"acgn", b"hello"])
        producer2.multi_publish("test_topic", [b"acg", b"acgn", b"hello"])
        producer1.deferred_publish("test_topic", b"delay", 5000)
        producer2.deferred_publish("test_topic", b"delay", 5000)
        continue
"""