from network import ServerSocket
from server import Server
import sys
import time
import threading
from client import Node, Client
from lock import RWLock
import logging
import argparse

# Default works as an angular hash
class HashRing(Server):
    def __init__(self, host, port, scale_time=60, quarantine_time=30, max_con=100, refuse=200, hash_max=360):
        self.hash_max = hash_max

        # Contains sorted list of keys to get the right server for a request
        self.hashes = []
        # Maps hashes to nodes
        self.ring = {}
        # Actual physical nodes
        self.nodes = []

        # Nodes that are offline
        self.offline = []

        self.scale_thread = None
        self.q_thread = None
        self.scale_time = scale_time
        self.quarantine_time = quarantine_time

        self.rw_lock = RWLock()

        super(HashRing, self).__init__(host, port, max_con, refuse)

    def scale(self):
        while True:
            time.sleep(self.scale_time)
            logging.debug("Running Scale")
            self.rw_lock.writer_acquire()
            # Multi threaded access
            num = sum([node.resp_time * node.requests for node in self.nodes if node.status != Client.OFF])
            den = sum([node.requests for node in self.nodes if node.status != Client.OFF])
            if den == 0:
                self.rw_lock.writer_release()
                continue
            avg_resp = num / den
            logging.debug("Average response time of all servers: {}".format(avg_resp))

            maidens = []
            knights = []
            for node in self.nodes:
                if node.status == Client.ON and node.requests > 0:
                    logging.debug("Node {} has a response time of {}".format(node.key, node.resp_time))
                    if node.resp_time > avg_resp * 1.25:
                        logging.debug("Node {} requires scaling".format(node.key))
                        maidens.append(node)
                    elif node.resp_time < avg_resp and not node.has_max_scale():
                        knights.append(node)
                node.reset_stats()

            for maiden in maidens:
                if not knights:
                    break
                if maiden.v_keys:
                    logging.debug("Node {} requires scale down".format(maiden.key))
                    self.remove_all_virtual_nodes(maiden)
                else:
                    knight = knights.pop()
                    logging.debug("Node {} scale up to support Node {}".format(knight.key, maiden.key))
                    idx = maiden.key
                    prev_idx = idx - 1
                    key = ((self.hash_max if idx == 0 else self.hashes[idx]) + self.hashes[prev_idx]) / 2
                    self.add_virtual_node(key, knight)

            self.rw_lock.writer_release()

    def add_virtual_node(self, key, node):
        # Multi threaded access
        self.hashes.append(key)
        self.hashes.sort()
        self.ring[key] = self.ring[node.key]
        node.register_v_key(key)

    def remove_all_virtual_nodes(self, node):
        # Multi threaded access
        self.hashes = [h for h in self.hashes if h not in node.v_keys]
        for key in node.v_keys:
            del self.ring[key]
        node.v_keys.clear()

    def add_server(self, key, node):
        self.rw_lock.writer_acquire()
        # Multi threaded access
        if key in self.hashes:
            self.rw_lock.writer_release()
            logging.error("Key '{}' already exists.".format(key))
            raise Exception("Key '{}' already exists.".format(key))
        if key > self.hash_max:
            self.rw_lock.writer_release()
            logging.error("Key '{}' exceedes maximum.".format(key))
            raise Exception("Key '{}' exceedes maximum.".format(key))
        self.hashes.append(key)
        self.hashes.sort()
        self.ring[key] = len(self.nodes)
        self.nodes.append(node)
        self.rw_lock.writer_release()

    def remove_server(self, key):
        self.rw_lock.writer_acquire()
        # Multi threaded access
        self.hashes.remove(key)
        del self.nodes[self.ring[key]]
        del self.ring[key]
        self.rw_lock.writer_release()
        return key

    def _get_server(self, key):
        # Multi threaded access
        if key > self.hashes[-1] or key == self.hashes[0]:
            return 0

        high = len(self.hashes) - 1
        low = 0
        mid = int((high + low)/2)
        while mid != high and mid != low:
            if self.hashes[mid] < key:
                low = mid
            elif self.hashes[mid] > key:
                high = mid
            else:
                return mid
            mid = int((high + low)/2)

        return low if self.hashes[low] > key else high

    def _find_server(self, key):
        # Multi threaded access
        idx = self._get_server(key)
        for i in range(0, len(self.hashes)):
            yield (idx + i) % len(self.hashes)

    def _send_request(self, key, data):
        for i in self._find_server(key):
            node = self.nodes[self.ring[self.hashes[i]]]
            if node.status == Client.OFF:
                continue
            logging.debug("Trying server {} at {}".format(self.hashes[i], node.address))
            try:
                return node.send_request(data)
            except Exception as err:
                logging.debug("Server {} not avaliable: {}".format(self.hashes[i], err))

        logging.error("No available servers.")
        return {'status': False, 'msg': 'No available servers.'}

    def send_request(self, key, data):
        # Multi threaded access
        logging.debug("Trying to send {}".format(data))
        self.rw_lock.reader_acquire()
        resp = self._send_request(key, data)
        self.rw_lock.reader_release()
        return resp

    def start_scale_thread(self):
        self.scale_thread = threading.Thread(name="scale_thread", target=HashRing.scale, args=(self,))
        self.scale_thread.start()

    def start_quarantine_thread(self):
        self.q_thread = threading.Thread(name="q_thread", target=HashRing.quarantine, args=(self,))
        self.q_thread.start()

    def quarantine(self):
        while True:
            time.sleep(self.quarantine_time)
            logging.debug("Running Quarantine")
            self.rw_lock.writer_acquire()

            offline = []
            online = []

            for h in self.hashes:
                node = self.nodes[self.ring[h]]
                if node.key != h or node.status in (Client.ON, Client.UNKNOWN,):
                    continue
                logging.debug("Node {} is now offline".format(h))
                offline.append(h)

            for h in self.offline:
                node = self.nodes[self.ring[h]]
                try:
                    node.connect()
                    logging.debug("Node {} is now online".format(h))
                    online.append(h)
                except:
                    pass

            self.hashes = [h for h in self.hashes if h not in offline] + online
            self.hashes.sort()
            self.offline = [h for h in self.offline if h not in online] + offline

            self.rw_lock.writer_release()

    def handle_command(self, data):
        if 'key' in data:
            return self.send_request(data['key'], data)
        elif 'add' in data:
            try:
                nodes = []
                for server in data['add']:
                    nodes.append(Node(server['host'], server['port'], server['key'], keep_alive=True))
                for node in nodes:
                    self.add_server(node.key, node)
                return {'status': True}
            except Exception as err:
                return {'status': False, 'msg': '{}'.format(err)}
        elif 'remove' in data:
            try:
                for server in data['remove']:
                    self.remove_server(server['key'])
                return {'status': True}
            except Exception as err:
                return {'status': False, 'msg': '{}'.format(err)}
        return {'status': False, 'msg': 'Unknown command'}

    def activate(self):
        self.start_scale_thread()
        self.start_quarantine_thread()
        super(HashRing, self).activate()

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s [%(levelname)s] - %(message)s', level=logging.DEBUG)

    parser = argparse.ArgumentParser(description='Run a Hash Ring.')
    parser.add_argument('-H', '--host', dest='host', type=str, default='localhost', help='Host to run the server on.')
    parser.add_argument('-p', '--port', dest='port', type=int, default=5003, help='Port to run the server on.')
    parser.add_argument('-s', '--scale-time', dest='scale_time', type=int, default=60, help='Interval between each scale attempt.')
    parser.add_argument('-q', '--quarantine-time', dest='q_time', type=int, default=30, help='Interval between each quarantine attempt.')
    parser.add_argument('-c', '--max-con', dest='max_con', type=int, default=100, help='Total number of connections in parallel.')
    parser.add_argument('-r', '--refuse', dest='refuse', type=int, default=200, help='Refuse connections beyond.')
    parser.add_argument('-m', '--max-hash', dest='max_hash', type=float, default=360, help='Maximum hash value')

    args = parser.parse_args()

    h = HashRing(args.host, args.port, args.scale_time, args.q_time, args.max_con, args.refuse, args.max_hash)
    h.activate()
"""
    n1 = Node('localhost', 5000, 0, keep_alive=True)
    n2 = Node('localhost', 5001, 180, keep_alive=True)
    n3 = Node('localhost', 5002, 270, keep_alive=True)

    h.add_server(0, n1)
    h.add_server(180, n2)
    h.add_server(270, n3)
"""
