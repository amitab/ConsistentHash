from network import ClientSocket, ServerSocket
from server import Server
from collections import defaultdict
import sys
import time
import threading

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
        self.key = key if key is not None else hash(self.address)
        self.status = Node.UNKNOWN
        self.resp_time = 0.0
        self.requests = 0
        self.v_keys = []

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

        # Multi threaded access
        self.requests += 1
        self.resp_time = self.resp_time + ((end - start) - self.resp_time) / self.requests
        debug("Stats - tot. req: {}, avg. resp: {}".format(self.requests, self.resp_time))
        return resp

    def register_v_key(self, key):
        # Multi threaded access
        self.v_keys.append(key)

# Default works as an angular hash
class HashRing(Server):
    def __init__(self, host, port, hash_max=360):
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

        super(HashRing, self).__init__(host, port)

    def scale(self):
        while True:
            time.sleep(10)
            debug("Running Scale")
            # Multi threaded access
            num = sum([node.resp_time * node.requests for node in self.nodes])
            den = sum([node.requests for node in self.nodes])
            if den == 0:
                return
            avg_resp = num / den

            maidens = []
            knights = []
            for node in self.nodes:
                if node.resp_time > avg_resp * 1.25:
                    debug("Node {} requires scaling".format(node.key))
                    maidens.append(node)
                else:
                    knights.append(node)

            for maiden in maidens:
                if not knights:
                    return
                if maiden.v_keys:
                    debug("Node {} requires scale down".format(maiden.key))
                    self.remove_all_virtual_nodes(maiden)
                else:
                    knight = knights.pop()
                    debug("Node {} scale up to support Node {}".format(knight.key, maiden.key))
                    idx = maiden.key
                    prev_idx = idx - 1
                    key = ((self.hash_max if idx == 0 else self.hashes[idx]) + self.hashes[prev_idx]) / 2
                    self.add_virtual_node(key, knight)

    def add_virtual_node(self, key, node):
        # Multi threaded access
        self.hashes.append(key)
        self.hashes.sort()
        self.ring[key] = self.ring[node.key]
        node.register_v_key(key)

    def remove_virtual_node(self, key):
        # Multi threaded access
        self.hashes.remove(key)
        del self.ring[key]

    def remove_all_virtual_nodes(self, node):
        # Multi threaded access
        self.hashes = [h for h in self.hashes if h not in node.v_keys]
        for key in node.v_keys:
            del self.ring[key]
        node.v_keys.clear()

    def add_server(self, key, node):
        # Multi threaded access
        self.hashes.append(key)
        self.hashes.sort()
        self.ring[key] = len(self.nodes)
        self.nodes.append(node)

    def remove_server(self, key):
        # Multi threaded access
        self.hashes.remove(key)
        del self.nodes[self.ring[key]]
        del self.ring[key]
        return key

    def deactivate_server(self, key):
        # Multi threaded access
        self.hashes.remove(key)
        self.offline.append(key)
        return key

    def deactivate_servers(self, keys):
        # Multi threaded access
        if not keys:
            return
        self.hashes = [h for h in self.hashes if h not in keys]
        self.offline.extend(keys)
        return True

    def get_server(self, key):
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
        idx = self.get_server(key)
        for i in range(0, len(self.hashes)):
            yield (idx + i) % len(self.hashes)

    def _send_request(self, key, data):
        # Multi threaded access
        invalid = []
        resp = None

        for i in self._find_server(key):
            node = self.nodes[self.ring[self.hashes[i]]]
            if node.status == Node.OFF:
                continue
            debug("Trying server {} at {}".format(self.hashes[i], node.address))
            try:
                resp = node.send_request(data)
                break
            except Exception as err:
                # Multi threaded access
                self.remove_all_virtual_nodes(node)
                invalid.append(node.key)
                debug("Server {} not avaliable: {}".format(self.hashes[i], err))

        # Multi threaded access
        self.deactivate_servers(invalid)
        if not resp:
            raise Exception("No available server.")
        return resp

    def send_request(self, key, data):
        # Multi threaded access
        debug("Trying to send {}".format(data))
        resp = self._send_request(key, data)
        debug("----------------------------------------------------")
        debug(self.hashes)
        debug(self.ring)
        debug(self.nodes)
        debug("----------------------------------------------------")
        return resp

    def start_scale_thread(self):
        self.scale_thread = threading.Thread(name="scale_thread", target=HashRing.scale, args=(self,))
        self.scale_thread.start()

    def start_quarantine_thread(self):
        self.q_thread = threading.Thread(name="q_thread", target=HashRing.quarantine, args=(self,))
        self.q_thread.start()

    def quarantine(self):
        while True:
            time.sleep(5)
            debug("Running Quarantine")

    def handle_command(self, data):
        return self.send_request(data['key'], data)

    def activate(self):
        #self.start_scale_thread()
        #self.start_quarantine_thread()
        super(HashRing, self).activate()

if __name__ == '__main__':
    h = HashRing(sys.argv[1], int(sys.argv[2]))
    n1 = Node('localhost', 5000, 0)
    n2 = Node('localhost', 5001, 180)
    n3 = Node('localhost', 5002, 270)

    h.add_server(0, n1)
    h.add_server(180, n2)
    h.add_server(270, n3)

    h.activate()
