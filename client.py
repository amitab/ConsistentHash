from network import ClientSocket
import time
import threading

class Client(object):
    ON = 0
    OFF = 1
    UNKNOWN = 2

    def __init__(self, host, port, key=None, keep_alive=False):
        self.sock = ClientSocket(host, port)
        self.address = (host, port)
        self.key = key if key is not None else hash(self.address)
        self.status = Client.UNKNOWN
        self.keep_alive = keep_alive
        self.sock_lock = threading.Lock()

    def __hash__(self):
        return self.key

    def connect(self):
        self.sock_lock.acquire()
        if self.status == Client.ON:
            self.sock_lock.release()
            return

        try:
            self.sock.connect()
            self.status = Client.ON
        except Exception as err:
            self.status = Client.OFF
            self.sock.disconnect()
            raise err
        finally:
            self.sock_lock.release()

    def send_request(self, data):
        resp = None
        try:
            self.sock_lock.acquire()
            if self.status != Client.ON:
                self.sock.connect()
                self.status = Client.ON
            self.sock.send(data)
            resp = self.sock.recv()
        except Exception as err:
            self.status = Client.OFF
            raise err
        finally:
            if not self.keep_alive:
                self.sock.disconnect()
                self.status = Client.OFF
            self.sock_lock.release()
        return resp

class Node(Client):
    def __init__(self, host, port, key=None, keep_alive=False, max_scale=2):
        self.resp_time = 0.0
        self.requests = 0
        self.v_keys = []
        self.max_scale = max_scale
        self.stats_lock = threading.Lock()
        self.scale_lock = threading.Lock()
        super(Node, self).__init__(host, port, key, keep_alive)

    def send_request(self, data):
        start = time.perf_counter()
        resp = super(Node, self).send_request(data)
        end = time.perf_counter()

        # Multi threaded access
        self.stats_lock.acquire()
        self.requests += 1
        self.resp_time = self.resp_time + ((end - start) - self.resp_time) / self.requests
        print("Stats - tot. req: {}, avg. resp: {}".format(self.requests, self.resp_time))
        self.stats_lock.release()
        return resp

    def register_v_key(self, key):
        # Multi threaded access
        self.scale_lock.acquire()
        self.v_keys.append(key)
        self.scale_lock.release()

    def reset_stats(self):
        self.stats_lock.acquire()
        self.requests = 0
        self.resp_time = 0
        self.stats_lock.release()

    def has_max_scale(self):
        self.scale_lock.acquire()
        has = len(self.v_keys) >= 2
        self.scale_lock.release()
        return has


