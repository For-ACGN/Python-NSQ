import python_nsq

def main():
    config = python_nsq.Config()
    producer = python_nsq.Producer("192.168.1.11:4250", config)
    err = producer.publish("test2", b"acg")
    if err != "":
        print(err)
    else:
        print("producer publish successfully")
    producer.stop()

if __name__ == "__main__":
    main()