import urllib3
import json

def main():
    url = "http://192.168.1.11:4161/nodes"
    pool_manager = urllib3.PoolManager()
    response = pool_manager.request("GET", url)
    nodes = json.loads(response.data.decode("utf-8"))
    print(nodes)
    producers = nodes.get("producerss","")
    print(len(producers))
    for i in range(0, len(producers)):
        print(producers[i].get("remote_address"))
    
if __name__ == "__main__":
    main()