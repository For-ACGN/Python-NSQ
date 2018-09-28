import python_nsq

def main():
    config = python_nsq.Config()
    config.lookupd_poll_interval = 15 #default 60s
    consumer = python_nsq.Consumer("test_topic", "test_channel", handler_message, config)
    consumer.connect_nsqlookupds(["http://192.168.1.11:4161/"])
    err = consumer.start()
    if err != "":
        print(err)
    #consumer.stop()

def handler_message(control, message):
    #control.stop() #control the consumer = consumer.stop()
    content = "[message]\n"
    content += "nsqd_address: " + message.nsqd_address + "\n"
    content += "timestamp:    " + str(message.timestamp) + "\n"
    content += "id:           " + str(message.id) + "\n"
    content += "message:      " + str(message.body) + "\n "
    print(content)
    message.finish()

if __name__ == "__main__":
    main()