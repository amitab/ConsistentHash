from network import ServerSocket
import sys
import time
import threading
import json
from concurrent.futures import ThreadPoolExecutor

lag = 0

class Server(object):
    def __init__(self, host, port, max_con=100, refuse=200):
        self.host = host
        self.port = port
        self.sock = ServerSocket(host, port)
        self.con = 0
        self.con_lock = threading.Lock()
        self.max_con = max_con
        self.refuse = refuse

    def handle_command(self, req):
        global lag
        time.sleep(lag)
        if req.get('cmd', None) == 'ping':
            return {'status': 'alive'}
        return {'status': 'ok'}

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
            self.con_lock.acquire()
            self.con -= 1
            self.con_lock.release()

    def activate(self):
        self.sock.listen()
        with ThreadPoolExecutor(max_workers=self.max_con) as executor:
            while True:
                connection, client_address = self.sock.accept()
                print('connection from', client_address)
                if self.con > self.refuse:
                    connection.send({'msg': 'too many connections'})
                    connection.disconnect()
                    continue

                executor.submit(Server.handle_client, self, connection)
                print("Starting thread {}".format(client_address))
                self.con_lock.acquire()
                self.con += 1
                self.con_lock.release()

if __name__ == '__main__':
    host = sys.argv[1]
    port = int(sys.argv[2])
    lag = int(sys.argv[3])
    s = Server(host, port)
    s.activate()
