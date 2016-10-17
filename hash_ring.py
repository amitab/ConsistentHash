from network import ServerSocket
from server import Server
import sys
import time
import threading
from client import Node, Client
from lock import RWLock

debug_mode = True

def debug(text):
    global debug_mode
    if not debug_mode:
        return
    print("[DEBUG] {}".format(text))

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

        self.rw_lock = RWLock()

        super(HashRing, self).__init__(host, port)

    def scale(self):
        while True:
            time.sleep(10)
            debug("Running Scale")
            self.rw_lock.writer_acquire()
            # Multi threaded access
            num = sum([node.resp_time * node.requests for node in self.nodes if node.status != Client.OFF])
            den = sum([node.requests for node in self.nodes if node.status != Client.OFF])
            if den == 0:
                self.rw_lock.writer_release()
                continue
            avg_resp = num / den
            print("AVG RESP TIME {}".format(avg_resp))

            maidens = []
            knights = []
            for node in self.nodes:
                if node.status == Client.ON and node.requests > 0:
                    print("Node {} response time {}".format(node.key, node.resp_time))
                    if node.resp_time > avg_resp * 1.25:
                        debug("Node {} requires scaling".format(node.key))
                        maidens.append(node)
                    elif node.resp_time < avg_resp and not node.has_max_scale():
                        knights.append(node)
                node.reset_stats()

            for maiden in maidens:
                if not knights:
                    break
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
            debug("Trying server {} at {}".format(self.hashes[i], node.address))
            try:
                return node.send_request(data)
            except Exception as err:
                debug("Server {} not avaliable: {}".format(self.hashes[i], err))

        raise Exception("No available server.")

    def send_request(self, key, data):
        # Multi threaded access
        debug("Trying to send {}".format(data))
        self.rw_lock.reader_acquire()
        resp = self._send_request(key, data)
        self.rw_lock.reader_release()
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
            self.rw_lock.writer_acquire()

            offline = []
            online = []

            for h in self.hashes:
                node = self.nodes[self.ring[h]]
                if node.key != h or node.status in (Client.ON, Client.UNKNOWN,):
                    continue
                debug("Node {} is now offline".format(h))
                offline.append(h)

            for h in self.offline:
                node = self.nodes[self.ring[h]]
                try:
                    node.connect()
                    debug("Node {} is now online".format(h))
                    online.append(h)
                except:
                    pass

            self.hashes = [h for h in self.hashes if h not in offline] + online
            self.hashes.sort()
            self.offline = [h for h in self.offline if h not in online] + offline

            self.rw_lock.writer_release()

    def handle_command(self, data):
        return self.send_request(data['key'], data)

    def activate(self):
        self.start_scale_thread()
        self.start_quarantine_thread()
        super(HashRing, self).activate()

if __name__ == '__main__':
    h = HashRing(sys.argv[1], int(sys.argv[2]))
    n1 = Node('localhost', 5000, 0, keep_alive=True)
    n2 = Node('localhost', 5001, 180, keep_alive=True)
    n3 = Node('localhost', 5002, 270, keep_alive=True)

    h.add_server(0, n1)
    h.add_server(180, n2)
    h.add_server(270, n3)

    h.activate()
