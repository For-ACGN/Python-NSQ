import python_nsq
import time

def main():
    print("[Python-NSQ] Version:", python_nsq.version)
    config = python_nsq.Config()
    config.client_id = "Producer"
    config.tls_v1 = True
    config.tls_config.ca_certs = "ca.crt"
    config.auth_secret = "python_nsq"
    producer = python_nsq.Producer("192.168.1.11:4150", config)
    while True:
        err = producer.publish("topic_test", b"acg")
        if err != "":
            print(err)
        err = producer.multi_publish("test_topic", [b"acg", b"acgn", b"hello"])
        if err != "":
            print(err)
        err = producer.deferred_publish("test_topic", b"delay", 5000) #5000ms
        if err != "":
            print(err)
        time.sleep(0.1)
    #producer.stop()

if __name__ == "__main__":
    main()