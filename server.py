from network import ServerSocket
import sys

class Server(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = ServerSocket(host, port)

    def run(self):
        self.sock.listen()
        while True:
            connection, client_address = self.sock.accept()
            print('connection from', client_address)
            print('Recieved: {}'.format(connection.recv()))
            data = {'status': 'ok'}
            connection.send(data)
            print("Sent: {}".format(data))
            print("Closing connection")
            connection.disconnect()

host = sys.argv[1]
port = int(sys.argv[2])
s = Server(host, port)
s.run()
