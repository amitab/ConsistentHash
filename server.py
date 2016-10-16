from network import ServerSocket
import sys
import time
import threading
import json

lag = 0

class Server(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = ServerSocket(host, port)
        self.con = []

    def handle_command(self, data):
        raise NotImplementedError

    def handle_client(self, sock):
        try:
            req = sock.recv()
            while req:
                print('Recieved: {}'.format(req))
                resp = self.handle_command(req)
                sock.send(resp)
                print('Sent: {}'.format(resp))
                req = sock.recv()
        except json.decoder.JSONDecodeError:
            # Disconnection from the other end
            pass
        finally:
            print("Closing connection")
            sock.disconnect()

    def activate(self):
        self.sock.listen()
        while True:
            connection, client_address = self.sock.accept()
            print('connection from', client_address)

            thd = threading.Thread(name=client_address, target=Server.handle_client, args=(self, connection,))
            print("Starting thread {}".format(client_address))
            self.con.append(thd)
            thd.start()

class PokemonGoServer(Server):
    def handle_command(self, req):
        global lag
        time.sleep(lag)
        if req.get('cmd', None) == 'ping':
            return {'status': 'alive'}
        return {'status': 'ok'}

if __name__ == '__main__':
    host = sys.argv[1]
    port = int(sys.argv[2])
    lag = int(sys.argv[3])
    s = PokemonGoServer(host, port)
    s.activate()
