import socket
from time import sleep

class lazy_TCP :
    def __init__(self, host, port, delay_close = 0) :
        self.host, self.port, self.delay_close = host, port, delay_close/1000
    
    def send(self, send_message, auto_encode = True, wait_return = False) :
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            if auto_encode : send_message = str.encode(send_message)
            s.sendall(send_message)
            if wait_return : data = s.recv(1024)
            else : data = None
            sleep(self.delay_close)
            return data
    
    def listen(self, debug = False, send_return = True) :
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s :
            s.bind((self.host, self.port))
            s.listen()
            conn, addr = s.accept()
            with conn:
                if debug : print(f'connect by {addr}')
                data = conn.recv(1024)
                if debug : print('receive :',data)
                if send_return : conn.sendall(data)
                sleep(self.delay_close)
        return data