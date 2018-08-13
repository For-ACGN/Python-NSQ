# Python-NSQ
Use Python-NSQ like go-nsq

Producer:
import python_nsq

def main():
    producer = python_nsq.Producer("127.0.0.1:4150")
    err = producer.publish("test_topic", b"acg")
        if err != 0:
            print("producer publish error")     
        else:
            print("producer publish successfully")
    
if __name__ == "__main__":
    main()
    
Consumer:
import python_nsq

def main():
    consumer = python_nsq.Consumer("test_topic", "acg", handler_message, ["http://127.0.0.1:4161/"], 15)
    consumer.start()
    while True:
        time.sleep(1000)
    consumer.stop()

def handler_message(control, message):
    #control.stop() #control the consumer
    print("nsqd_address:", message.nsqd_address)
    print("timestamp:   ", message.timestamp)
    print("id:          ", message.id)
    print("message:     ", message.body)
    message.finish()
    
if __name__ == "__main__":
    main()
