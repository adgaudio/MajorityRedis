import random
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

from . import exceptions


class MajorityRedisBaseClass(object):
    def __init__(self, clients, n_servers, timeout=5, Timer=threading.Timer,
                 map_async=ThreadPoolExecutor(sys.maxsize).map):
        """
        `clients` - a list of redis.StrictRedis clients,
            each connected to a different Redis server
        `n_servers` - the number of Redis servers in your cluster
            (whether or not you have a client connected to it)
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
            raise exceptions.CannotObtainLock(
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
