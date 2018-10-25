import python_nsq
import time
import ssl

def main():
    print("[Python-NSQ] Version:", python_nsq.version)
    config = python_nsq.Config()
    #config.tls_v1 = True
    config.tls_config.ca_certs = "ca.crt"
    #config.tls_config.ssl_version = ssl.PROTOCOL_TLSv1
    config.auth_secret = "python_nsq"
    config.lookupd_poll_interval = 15
    config.write_timeout = 2
    config.read_timeout = 2
    consumer = python_nsq.Consumer("test2", "Receive", handler_message, config)
    while True:
        consumer.connect_nsqlookupds(["http://192.168.1.11:4261/"])
        err = consumer.start()
        if err != "":
            print("error: " + err)
        print("exit")
        time.sleep(1)
    #consumer.stop()

def handler_message(control, message):
    #control.stop()#control the consumer
    content = "[message]\n"
    content += "nsqd_address: " + message.nsqd_address + "\n"
    content += "timestamp:    " + str(message.timestamp) + "\n"
    content += "id:           " + str(message.id) + "\n"
    content += "message:      " + str(message.body) + "\n "
    print(message.body)
    #print(len(content))
    if len(message.body) != 10240:
        print("error")
    message.finish()

if __name__ == "__main__":
    main()