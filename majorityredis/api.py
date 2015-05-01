from functools import partial
import random
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

from . import exceptions
from .lockingqueue import LockingQueue
from .lock import Lock
from .getset import GetSet


class MajorityRedis(object):
    def __init__(self, clients, n_servers, timeout=5, Timer=threading.Timer,
                 map_async=ThreadPoolExecutor(sys.maxsize).map):
        """Initializes MajorityRedis connection to multiple independent
        non-replicated Redis Instances.  This MajorityRedis client contains
        algorithms and operations based on majority vote of the redis servers.

        Please initialize it by passing the following parameters:

        `clients` - a list of redis.StrictRedis clients,
            each connected to a different Redis server
        `n_servers` - the number of Redis servers in your cluster
            (whether or not you have a client connected to it)
            This should be a universally constant number.
            n_servers // 2 + 1 == quorum, or the smallest possible majority.
        `timeout` - number of seconds after which the lock is invalid.
            Increase if you have large network delays or long periods where
            your python code is paused while running long-running C code
        `Timer` - implements the threading.Timer api.  If you do not with to
            use Python's threading module, pass in something else here.
        `map_async` - a function of form map(func, iterable) that maps func on
            iterable sequence.
            By default, use concurrent.futures.ThreadPoolmap_async api
            If you don't want to use threads, pass in your own function
        """
        if len(clients) < n_servers // 2 + 1:
            raise exceptions.MajorityRedisException(
                "Must connect to at least half of the redis servers to"
                " obtain majority")
        self._Timer = Timer
        self._client_id = random.randint(0, sys.maxsize)
        self._clients = clients
        self._clock_drift = 0  # TODO
        self._map_async = map_async
        self._n_servers = n_servers
        self._polling_interval = timeout / 5.
        self._timeout = timeout

        getset = GetSet(self)
        self.get = getset.get
        self.set = getset.set
        self.Lock = Lock(self)
        self.LockingQueue = partial(LockingQueue, self)