# Consistent Hashing

Read about it [here](https://en.wikipedia.org/wiki/Consistent_hashing).

# Consistent Hashing as a Load Balancer

Hashing could be used as a way to distribute requests among a set of servers based on a key. This is basically sharding. However, distributing requests doesn't automatically mean load balancing.

Consider two servers `A` and `B`. `A` takes keys from `0` to `100`. `B` takes keys `101` to `150`. What if content from `101` to `150` is more popular? `B` gets overloaded and `A` doesn't help out `B` when necessary.

To solve this issue here, we try to add another metric - the average response time of a server to decide when to redirect a request away from an overloaded server, even if the key dictates that it must go to that overloaded server.

# When to redirect

Consistent hashing takes place as usual as long as the average response time of the servers are almost the same.

To decide when the average response time is not normal, we pick a constant - `1.25`. We calculate the average response time of all the servers and compare each of the server's response times. If it exceeds `1.25 * overall_average_response_time`, then we know that the load isn't being distributed equally. Ofcourse, I have just chosen 1.25. It can be anything.

# How to redirect

We compile a list of all the slow responding servers and the fast servers. Here, the fast servers are the servers with response time less than the overall average. We then take one of the fast servers, spawn a 'virtual' node on the part of Hash Ring to divide the range of accepted keys between the fast and the slow server.

If the slow server were already helping out some other server, we get rid of it's virtual nodes.

The logic is located in `HashRing.scale` in `hash_ring.py`.

# How to run

```
$ python server.py -H localhost -p 5000
$ python server.py -H localhost -p 5001
$ python server.py -H localhost -p 5002

$ python hash_ring.py -H localhost -p 5003
$ python test.py
```
