import python_nsq

def main():
    config = python_nsq.Config()
    producer = python_nsq.Producer("192.168.1.11:4150", config)
    err = producer.publish("test_topic", b"message")
    if err != "":
            print(err)
    producer.stop()

if __name__ == "__main__":
    main()