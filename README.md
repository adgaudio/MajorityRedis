MajorityRedis
=======

A collection of client-side algorithms and operations that use the
concept of obtaining "majority" vote across N independent Redis servers.

This project is experimental and not recommended for production.


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
