from network import ClientSocket, ServerSocket
from collections import defaultdict
import sys
import time

debug_mode = True

def debug(text):
    global debug_mode
    if not debug_mode:
        return
    print("[DEBUG] {}".format(text))

class Node(object):
    ON = 0
    OFF = 1
    UNKNOWN = 2

    def __init__(self, host, port, key=None):
        self.sock = ClientSocket(host, port)
        self.address = (host, port)
        self.key = key if key else hash(self.address)
        self.status = Node.UNKNOWN
        self.resp_time = 0.0
        self.requests = 0

    def __hash__(self):
        return self.key

    def send_request(self, data):
        resp = None
        start = time.perf_counter()
        try:
            self.sock.connect()
            self.sock.send(data)
            resp = self.sock.recv()
        except Exception as err:
            self.status = Node.OFF
            raise err
        finally:
            self.sock.disconnect()
        end = time.perf_counter()

        # Multi thread?
        self.requests += 1
        self.resp_time = self.resp_time + ((end - start) - self.resp_time) / self.requests
        debug("Stats - tot. req: {}, avg. resp: {}".format(self.requests, self.resp_time))
        return resp

class HashRing(object):
    def __init__(self):
        self.node_hashes = []
        self.offline = []
        self.cur_invalid = []
        self.nodes = {} # How many threads can remove and add nodes ?
        self.sock = None

    def add_server(self, key, node):
        self.node_hashes.append(key)
        self.node_hashes.sort()
        self.nodes[key] = node

    def remove_server(self, key):
        self.node_hashes.remove(key)
        del self.nodes[key]
        return key

    def get_server(self, key):
        if key > self.node_hashes[-1] or key == self.node_hashes[0]:
            return (self.node_hashes[0], 0,)

        high = len(self.node_hashes) - 1
        low = 0
        mid = int((high + low)/2)
        while mid != high and mid != low:
            if self.node_hashes[mid] < key:
                low = mid
            elif self.node_hashes[mid] > key:
                high = mid
            else:
                return (node_hashes[mid], mid,)
            mid = int((high + low)/2)

        return (self.node_hashes[high], high,)

    def _find_server(self, key):
        h, idx = self.get_server(key)
        for i in range(0, len(self.node_hashes)):
            yield (idx + i) % len(self.node_hashes)

    def clean_invalid(self):
        if not self.cur_invalid:
            return
        self.offline.extend([self.remove_server(self.node_hashes[i]) for i in self.cur_invalid])
        self.cur_invalid.clear()

    def _send_request(self, key, data):
        self.clean_invalid()
        for i in self._find_server(key):
            try:
                if self.nodes[self.node_hashes[i]].status == Node.OFF:
                    continue
                debug("Trying server {} at {}".format(self.node_hashes[i], self.nodes[self.node_hashes[i]].address))
                s = self.nodes[self.node_hashes[i]]
                return s.send_request(data)
            except Exception as err:
                self.cur_invalid.append(i)
                debug("Server {} not avaliable: {}".format(self.node_hashes[i], err))
        raise Exception("No available server.")

    def send_request(self, key, data):
        debug("Trying to send {}".format(data))
        return self._send_request(key, data)

    def activate(self, host, port):
        self.sock = ServerSocket(host, port)
        self.sock.listen()
        while True:
            connection, client_address = self.sock.accept()
            print('connection from', client_address)
            data = connection.recv()
            print('Recieved: {}'.format(data))
            resp = self.send_request(data['key'], data)
            connection.send(resp)
            print('Sent: {}'.format(resp))
            print("Closing connection")
            connection.disconnect()

if __name__ == '__main__':
    h = HashRing()
    n1 = Node('localhost', 5000, 0)
    n2 = Node('localhost', 5001, 180)
    n3 = Node('localhost', 5002, 270)

    h.add_server(0, n1)
    h.add_server(180, n2)
    h.add_server(270, n3)

    h.activate(sys.argv[1], int(sys.argv[2]))

