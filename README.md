# Python-NSQ
Use Python-NSQ like go-nsq

Feature
-------
```
1. It works very much like go-nsq
   if you use go-nsq but need python, you will like it

2. Publish message synchronized
   you can receive result after pushish

3. Use only one third party library urllib3
   debug on Python 3.6.5 
   if you want to use urllib, there will be no third party library
```

Function
--------
```
1. Subscribe
2. Publish(multi&deferred)
3. Discovery
4. TLS
5. AUTH
```

Usage
-----
Ruquire urllib3

Producer:
```python
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
```
Consumer:
```python
import python_nsq

def main():
    config = python_nsq.Config()
    config.lookupd_poll_interval = 15 #default 60s
    consumer = python_nsq.Consumer("test_topic", "test_channel", handle_message, config)
    consumer.connect_nsqlookupds(["http://192.168.1.11:4161/"])
    err = consumer.start()
    if err != "":
        print(err)
    #consumer.stop()

def handle_message(control, message):
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
```

TLS & Authentication
```python
import python_nsq

def main():
    config = python_nsq.Config()
    config.tls_v1 = True
    #config.tls_config.ca_certs = "ca.crt"
    config.auth_secret = "python_nsq"
    producer = python_nsq.Producer("192.168.1.11:4150", config)
    err = producer.publish("test_topic", b"message")
    if err != "":
        print(err)
    producer.stop()

if __name__ == "__main__":
    main()
```

TODO List
---------
```
1.snappy and deflate
2.is_starved
3.backoff
4.RDY
5.mutliprocess or coroutines(because GIL .... useless)
6.logger(self._log)
7.sampling
8.channel to queue
```