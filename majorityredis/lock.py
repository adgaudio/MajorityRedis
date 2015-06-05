import random
import sys
import time

from . import util
from . import log
from . import exceptions

SCRIPTS = dict(

    # returns 1 if locked, 0 if could not lock.
    # return exception if invalid expireat (ie lock is already expired)
    l_lock=dict(keys=('path', ), args=('client_id', 'expireat'), script="""
if 1 == redis.call("SETNX", KEYS[1], ARGV[1]) then
    if 1 == redis.call("EXPIREAT", KEYS[1], ARGV[2]) then return 1
    else
      redis.call("DEL", KEYS[1])
      return {err="invalid expireat"} end
elseif ARGV[1] == redis.call("GET", KEYS[1]) then
    if 1 ~= redis.call("EXPIREAT", KEYS[1], ARGV[2]) then
      redis.call("DEL", KEYS[1])
      return {err="invalid expireat"} end
    return 1
else return 0 end
"""),

    # returns 1 if unlocked, 0 otherwise
    l_unlock=dict(keys=('path', ), args=('client_id', ), script="""
local rv = redis.call("GET", KEYS[1])
if rv == ARGV[1] then
    return redis.call("DEL", KEYS[1])
elseif rv == false then return 1
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
    A Distributed Lock implementation for Redis.  The is a variant of the
    Redlock algorithm.
    """
    def __init__(self, mr_client):
        """
        `mr_client` - an instance of the MajorityRedis client.
        """
        self._mr = mr_client
        self._lock_timeout = mr_client._lock_timeout
        if mr_client._threadsafe:
            self._client_id = random.randint(1, sys.maxsize)
        else:
            self._client_id = mr_client._client_id

        if self._lock_timeout < self._mr._polling_interval:
            log.warn((
                "lock_timeout is less than polling_interval, which means"
                " I cannot extend_lock in background"), extra=dict(
                    lock_timeout=self._lock_timeout,
                    polling_interval=self._mr._polling_interval))

    def lock(self, path, wait_for=None, extend_lock=True):
        """
        Attempt to lock a path on the majority of servers. Return True or False

        `wait_for` (int) Max num seconds to wait to acquire a lock if it is
            currently not lockable (owned by someone else or too many Server
            failures).  By default, return immediately, whether or not we have
            acquired the lock.
        `extend_lock` - If True, extends the lock indefinitely in the
            background until the lock is explicitly consumed or
            we can no longer extend the lock.
            If False, you need to set a very large timeout or call
            extend_lock() before the lock times out.
            If a function, assume True and call function(h_k) if we
            ever fail to extend the lock.
        """
        if not wait_for:
            func = self._lock
        else:
            # apply retry condition with backoff=ttl to sleep for, nretry=1,
            # condition=True
            tstart = time.time()

            def condition_func(rv):
                if isinstance(rv, Exception):
                    return False
                # stop retrying if got lock OR if exceeded the timeout
                return bool(rv) or time.time() - tstart > wait_for

            def backoff_func(prev_delay):
                # calculate how many seconds to try to acquire lock again
                # based on how much time we have left in ttl
                ttlstart = time.time()
                ttl = self._mr.ttl(path)
                if ttl == -2:
                    return 0  # node does not exist.  lockable immediately
                elif ttl == -1:
                    ttl = 1
                # bound ttl between (0 <= ttl <= secs_left_before_timeout)
                ttl = max(0, ttl - (time.time() - ttlstart) / 2.)
                ttl = min(ttl, wait_for - (time.time() - tstart))
                return ttl

            func = util.retry_condition(
                nretry=sys.maxsize, backoff=backoff_func
            )(
                self._lock, condition_func)
        try:
            return func(path, extend_lock)
        except exceptions.TooManyRetries:
            return False

    def _lock(self, path, extend_lock):
        """
        Attempt to lock a path on the majority of servers. Return True or False
        """
        t_start, t_expireat = util.get_expireat(self._lock_timeout)
        locks = util.run_script(
            SCRIPTS, self._mr._map_async, 'l_lock', self._mr._clients,
            path=path, client_id=self._client_id, expireat=t_expireat)
        n, locked_clients = 0, []
        for cli, is_locked in locks:
            if is_locked == 1:
                n += 1
                locked_clients.append(cli)
        if n < self._mr._n_servers // 2 + 1:
            self.unlock(path, clients=locked_clients)
            return False
        if not util.lock_still_valid(
                t_expireat, self._mr._clock_drift, self._mr._polling_interval):
            return False
        if extend_lock:
            util.continually_extend_lock_in_background(
                path, self.extend_lock, self._mr._polling_interval,
                self._mr._run_async, extend_lock, self._client_id)
        return t_expireat

    def unlock(self, path, clients=None):
        """Remove the lock at given `path` as long as the lock was created
        by this client.
        Return % of servers where this key is currently unlocked"""
        clients = clients or self._mr._clients
        locks = util.run_script(
            SCRIPTS, self._mr._map_async, 'l_unlock', clients,
            path=path, client_id=self._client_id)
        cnt = sum(is_unlocked for _, is_unlocked in locks
                  if not isinstance(is_unlocked, Exception))
        util.remove_background_thread(path, self._client_id)
        return 100. * cnt / self._mr._n_servers

    def extend_lock(self, path):
        """
        If you have received an item from the queue and wish to hold the lock
        on it for an amount of time close to or longer than the timeout, you
        must extend the lock!

        Returns one of the following:
            0 if failed to extend_lock
            number of seconds since epoch in the future when lock will expire
        """
        t_start, t_expireat = util.get_expireat(self._lock_timeout)
        locks = list(util.run_script(
            SCRIPTS, self._mr._map_async, 'l_extend_lock', self._mr._clients,
            path=path, client_id=self._client_id, expireat=t_expireat))
        cnt = sum(x[1] == 1 for x in locks)
        if cnt < self._mr._n_servers // 2 + 1:
            return False
        # Re-lock nodes where lock is lost. By this point we have majority
        # However, there's a possible race condition if we lock all clients,
        # described below.  Therefore, we only re-lock a minority of nodes.
        # 1. lock node.
        # 2a l_extend_lock on all
        # 2b. unlock() on all
        # 2c. re-lock (via l_lock) on all
        if util.lock_still_valid(
                t_expireat, self._mr._clock_drift, self._mr._polling_interval):
            # list(...) makes us block until response received from all servers
            list(util.run_script(
                SCRIPTS, self._mr._map_async, 'l_lock',
                [x[0] for x in locks if x[1] != 1],
                path=path, client_id=self._client_id, expireat=t_expireat))
        if util.lock_still_valid(
                t_expireat, self._mr._clock_drift, self._mr._polling_interval):
            return t_expireat
        return False
