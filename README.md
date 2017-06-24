MajorityRedis
=======

A collection of client-side algorithms and operations that use the
concept of obtaining "majority" vote across N independent Redis servers.

This project is experimental and not recommended for production.

Background
---

This library is based off of the
[Redlock](https://redis.io/topics/distlock) algorithm, which enables
distributed locks for redis.

The central idea is that multiple redis clients all want to find and
modify a number, but this number is defined redundantly in multiple
redis servers at once.  How to we ensure that these servers stay in sync
as clients modify their data?

Let us assume that we care very much about the servers remaining
strongly consistent.  I will motivate this assumption with a simple example.
Imagine client "A" wishes to multiply the value times a number and client
"B" wishes to subtract a number.   The order in which the clients
perform the operation affects the resulting value (ie ((initial_value -
A) / B) is not necessarily the same as (initial_value / B - A)).  Since
order matters, how do we ensure that client A's action applies to all
servers before client B's action applies to any 1 server?  We need a
distributed lock.

I want to take this one step further.  Imagine we have a queue of items,
where we want to process the next item out of the queue.  Because the
contents of the queue are valuable, we distribute redundant copies of
that queue across multiple independent redis servers.  How do we ensure
that the queues remain in sync while also not appointing any particular
server as the master?  We need a distributed locking queue.

This library provides basic data structures for the distributed Lock and
LockingQueue.  Unlike typical distributed databases, MajorityRedis
places the complexity of the data structure on the client rather than
the server.  The servers do not need to communicate with each other to
remain in sync.  But every time the client performs an operation, it
must get approval from the majority of servers.  While server-side code
is simpler, the client-side code is more complicated.


Implementation
---


**LockingQueue**:
  - A Distributed Queue implementation that guarantees only one client can
      get the object from the queue at a time.
  - Adapted from the Redlock algorithm (described in Redis documentation)

**Lock**:
  - A variant of the Redlock algorithm (descripted in Redis documentation)


**LockingQueue** and **Lock** Implementations have the following traits:
  - Strongly consistent (replicated Redis does not have this guarantee)
  - Decent partition tolerance
  - Fault tolerant and redundant
  - Self-healing. If a redis node dies while lock still owned, the client
    will update any new nodes that replaced the dead one with relevant info.


In progress:

**GET**
  - Get the value of a key from the majority of servers

**SET**
  - Set a key=value on the majority of servers

**GET** and **SET** implementations are:

  - Consistency guarantee is based on how often keys are accessed
  - (Key, Value) pairs will get lost or out of date if the majority of redis
    servers dies before a client gets or sets the key
  - Decent partition tolerance
  - Self-healing and try to ensure consistent state across cluster.

Please keep in mind that this is still in progress and everything here still
needs testing.


Quick Start:
====

start up redis servers
```
$ docker-compose up -d
```

Drop into an IPython shell
```
$ docker-compose run shell
Python 3.4.0 (default, Apr 11 2014, 13:05:11)
Type "copyright", "credits" or "license" for more information.

IPython 4.0.0-dev -- An enhanced Interactive Python.
?         -> Introduction and overview of IPython's features.
%quickref -> Quick reference.
help      -> Python's own help system.
object?   -> Details about 'object', use 'object??' for extra details.

In [1]: mr
Out[1]: <majorityredis.api.MajorityRedis at 0x7f4b77541710>
```
