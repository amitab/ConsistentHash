import json
import socket
import sys

class SocketStream(object):
    def __init__(self, sock=None):
        self.sock = sock

    def recv(self, sock=None):
        data = self.sock.recv(1024)
        while data and data[-1] != 59:
            data += self.sock.recv(1024)
        return json.loads(data[:-1].decode())

    def send(self, data, sock=None):
        data = json.dumps({'data': data}) + ";"
        self.sock.sendall(data.encode())

    def disconnect(self):
        self.sock.close()

class ServerSocket(SocketStream):
    def __init__(self, host, port):
        self.address = (host, port,)
        super(ServerSocket, self).__init__()

    def listen(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(self.address)
        self.sock.listen(1)

        print("Listening on {}:{}".format(self.address[0], self.address[1]))

    def accept(self):
        connection, client_address = self.sock.accept()
        return SocketStream(connection), client_address

class ClientSocket(SocketStream):
    def __init__(self, host, port):
        self.address = (host, port,)
        super(ClientSocket, self).__init__()

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(self.address)


