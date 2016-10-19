from network import ServerSocket
import sys
import time
import threading
import json
from concurrent.futures import ThreadPoolExecutor
import logging
import argparse

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
                logging.debug('Recieved: {}'.format(req))
                resp = self.handle_command(req)
                sock.send(resp)
                logging.debug('Sent: {}'.format(resp))
                req = sock.recv()
        except json.decoder.JSONDecodeError:
            # Disconnection from the other end
            pass
        finally:
            logging.debug("Closing connection")
            sock.disconnect()
            self.con_lock.acquire()
            self.con -= 1
            self.con_lock.release()

    def activate(self):
        self.sock.listen()
        with ThreadPoolExecutor(max_workers=self.max_con) as executor:
            while True:
                connection, client_address = self.sock.accept()
                logging.debug('connection from {}'.format(client_address))
                if self.con > self.refuse:
                    connection.send({'msg': 'too many connections'})
                    connection.disconnect()
                    continue

                executor.submit(Server.handle_client, self, connection)
                logging.debug("Starting thread {}".format(client_address))
                self.con_lock.acquire()
                self.con += 1
                self.con_lock.release()

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s [%(levelname)s] - %(message)s', level=logging.DEBUG)

    parser = argparse.ArgumentParser(description='Run a Hash Ring.')
    parser.add_argument('-H', '--host', dest='host', type=str, default='localhost', help='Host to run the server on.')
    parser.add_argument('-p', '--port', dest='port', type=int, default=5003, help='Port to run the server on.')
    parser.add_argument('-l', '--lag', dest='lag', type=int, default=0, help='Simulate server lag')
    args = parser.parse_args()
    lag = args.lag

    s = Server(args.host, args.port)
    s.activate()
