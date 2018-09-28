import python_nsq
import time

def main():
    print("[Python-NSQ] version:", python_nsq.version)
    config = python_nsq.Config()
    config.tls_v1 = True
    config.tls_config.ca_certs = "ca.crt"
    config.auth_secret = "python_nsq"
    config.lookupd_poll_interval = 15
    consumer = python_nsq.Consumer("test_topic", "test_channel", handler_message, config)
    consumer.connect_nsqlookupds(["http://192.168.1.11:4161/"])
    err = consumer.start()
    if err != "":
        print(err)
        return
    #consumer.stop()

def handler_message(control, message):
    #control.stop() #control the consumer
    time.sleep(4)
    content = "[message]\n"
    content += "nsqd_address: " + message.nsqd_address + "\n"
    content += "timestamp:    " + str(message.timestamp) + "\n"
    content += "id:           " + str(message.id) + "\n"
    content += "message:      " + str(message.body) + "\n "
    print(content)
    message.finish()

if __name__ == "__main__":
    main()