from network import ClientSocket

class Node(object):
    def __init__(self, host, port, key=None):
        self.sock = ClientSocket(host, port)
        self.address = (host, port)
        self.key = key if key else hash(self.address)

    def __hash__(self):
        return self.key

    def send_request(self, data):
        self.sock.connect()
        self.sock.send(data)
        resp = self.sock.recv()
        self.sock.disconnect()
        return resp

class HashRing(object):
    def __init__(self):
        self.nodes_hashes = []
        self.nodes = {}

    def add_server(self, key, node):
        self.node_hashes.append(key)
        self.node_hashes.sort()
        self.nodes[key] = node

    def remove_server(self, key):
        self.node_hashes.remove(key)
        del self.nodes[key]

    def get_server(self, key):
        for h in self.node_hashes:
            if h >= key:
                return self.nodes[h]
        return self.nodes[self.node_hashes[0]]

    def send_request(self, key, data):
        s = self.get_server(key)
        return s.send_request(data)
