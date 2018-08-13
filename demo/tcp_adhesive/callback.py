import types

def handler_message(message):
    print(message)

def call(callback):
    assert isinstance(callback,types.FunctionType), "invaild callback, callback is not FunctionType"
    callback("hello")
    
def main():
    call(handler_message)
    call(123)

if __name__ == "__main__":
    main()