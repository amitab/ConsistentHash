from hash_ring import Node

n = Node('localhost', 5003)
n.send_request({'key': 12, 'data': 12})
n.send_request({'key': 120, 'data': 120})
n.send_request({'key': 280, 'data': 280})
