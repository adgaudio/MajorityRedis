MajorityRedis
=======

A collection of client-side algorithms and operations that use the
concept of obtaining "majority" vote across N independent Redis servers.

**LockingQueue**:
  - A Distributed Queue implementation that guarantees only one client can
      get the object from the queue at a time.
  - Adapted from the Redlock algorithm (described in Redis documentation)

**Lock**:
  - A variant of the Redlock algorithm (descripted in Redis documentation)


**LockingQueue** and **Lock** Implementations are:
  - Strongly consistent (replicated redis does not have this guarantee)
  - Decent partition tolerance
  - Fault tolerant and redundant
  - Self-healing. If a redis node dies while lock still owned, the client
    will update any new nodes that replaced the dead one with relevant info.




In progress:

**MGET**
  - Get the value of a key from the majority of servers

**MSET**
  - Set a key=value on the majority of servers
