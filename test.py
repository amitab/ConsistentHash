from .client import Client

n = Client('localhost', 5003)
n.send_request({'add': [
    {'host': 'localhost', 'port': 5000, 'key': 0},
    {'host': 'localhost', 'port': 5001, 'key': 180},
    {'host': 'localhost', 'port': 5002, 'key': 270}
]})

n.send_request({'key': 12, 'data': 12})
n.send_request({'key': 120, 'data': 120})
n.send_request({'key': 280, 'data': 280})
