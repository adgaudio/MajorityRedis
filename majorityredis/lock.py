import logging
import random
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

from . import util

log = logging.getLogger('majorityredis.lock')


SCRIPTS = dict(

    # returns 1 if locked, 0 otherwise.  return exception if created funky state
    # TODO: broken
    l_lock=dict(keys=('path', ), args=('client_id', 'expireat'), script="""
if 1 == redis.call("SETNX", KEYS[1], ARGV[1]) then
    if 1 == redis.call("EXPIREAT", KEYS[1], ARGV[2]) then
        return 1
    else return {err="invalid expireat"} end
else return 0 end
"""),

    # returns 1 if unlocked, 0 otherwise
    l_unlock=dict(keys=('path', ), args=('client_id', ), script="""
local rv = redis.call("GET", KEYS[1])
if rv == ARGV[1] then
    return redis.call("DEL", KEYS[1])
elseif rv == nil then return 1
else return 0 end
"""),
    # returns 1 if got lock extended, 0 otherwise
    l_extend_lock=dict(
        keys=('path', ), args=('expireat', 'client_id'), script="""
if ARGV[2] == redis.call("GET", KEYS[1]) then
    return redis.call("EXPIREAT", KEYS[1], ARGV[1])
else return 0 end
"""),
)


class Lock(object):
    """
    A Distributed Locking Queue implementation for Redis.

    The queue expects to receive at least 1 redis.StrictRedis client,
    where each client is connected to a different Redis server.
    When instantiating this class, if you do not ensure that the number
    of servers defined is always constant, you risk the possibility that
    the same lock may be obtained multiple times.
    """

    def __init__(self, clients, n_servers, timeout=5,
                 Timer=threading.Timer,
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
        self._clients = clients
        self._n_servers = n_servers
        self._timeout = timeout
        self._Timer = Timer
        self._map_async = map_async
        self._client_id = random.randint(0, sys.maxsize)
        self._polling_interval = timeout / 5.
        self._clock_drift = 0  # TODO

    def lock(self, path, extend_lock=True):
        """
        Attempt to lock a path on the majority of servers. Return True or False

        `extend_lock` - If True, extends the lock indefinitely in the
            background until the lock is explicitly consumed or
            we can no longer extend the lock.
            If False, you need to set a very large timeout or call
            extend_lock() before the lock times out.
            If a function, assume True and call function(h_k) if we
            ever fail to extend the lock.
        """
        t_start, t_expireat = util.get_expireat(self._timeout)
        rv = util.run_script(
            SCRIPTS, self._map_async, 'l_lock', self._clients,
            path=path, client_id=self._client_id, expireat=t_expireat)
        n = sum(x[1] == 1 for x in rv if not isinstance(x, Exception))
        if n < self._n_servers // 2 + 1:
            self.unlock(path)
            return False
        if not util.lock_still_valid(
                t_expireat, self._clock_drift, self._polling_interval):
            return False
        if extend_lock:
            util.continually_extend_lock_in_background(
                path, self.extend_lock, self._polling_interval, self._Timer,
                extend_lock)
            return t_expireat

    def unlock(self, path):
        """Remove the lock at given `path`
        Return % of servers that are currently unlocked"""
        rv = util.run_script(
            SCRIPTS, self._map_async, 'l_unlock', self._clients,
            path=path, client_id=self._client_id)
        cnt = sum(x[1] == 1 for x in rv if not isinstance(x, Exception))
        return float(cnt) / self._n_servers

    def extend_lock(self, path):
        """
        If you have received an item from the queue and wish to hold the lock
        on it for an amount of time close to or longer than the timeout, you
        must extend the lock!

        Returns one of the following:
            0 if failed to extend_lock
            number of seconds since epoch in the future when lock will expire
        """
        t_start, t_expireat = util.get_expireat(self._timeout)
        locks = util.run_script(
            SCRIPTS, self._map_async, 'l_extend_lock', self._clients,
            path=path, client_id=self._client_id, expireat=t_expireat)
        cnt = sum(x[1] == 1 for x in locks if not isinstance(x, Exception))
        if cnt < self._n_servers // 2 + 1:
            return False
        return util.lock_still_valid(
            t_expireat, self._clock_drift, self._polling_interval)
