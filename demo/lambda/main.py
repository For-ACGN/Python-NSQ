def main():
    aaa = {
            b"OK": a,
            b"_heartbeat_": b,
    }.get(b"asd",None)()

def a():
    print("acg")
def b():
    print("acgacg")
    
if __name__ == "__main__":
    main()