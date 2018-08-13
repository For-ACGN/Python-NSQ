def main():
    aaa = ["192.168.1.11:4150", "192.168.1.11:4150"]
    addresses = []
    for i in range(0, len(aaa)):
        addr = aaa[i].split(":")       
        addresses.append((addr[0], int(addr[1])))
    print(addresses)
    

if __name__ == "__main__":
    main()
    