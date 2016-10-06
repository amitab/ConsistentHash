from network import ServerSocket

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
            connection.send({'status': 'ok'})
            print("Closing connection")
            connection.disconnect()

s = Server('localhost', 5000)
s.run()
