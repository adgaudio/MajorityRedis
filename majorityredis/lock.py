from . import util


SCRIPTS = dict(

    # returns 1 if locked, 0 otherwise.  return exception if created funky state
    # TODO: broken
    l_lock=dict(keys=('path', ), args=('client_id', 'expireat'), script="""
if 1 == redis.call("SETNX", KEYS[1], ARGV[1]) then
    if 1 == redis.call("EXPIREAT", KEYS[1], ARGV[2]) then
        return 1
    else return {err="invalid expireat"} end
elseif ARGV[1] == redis.call("GET", KEYS[1]) then return 1
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
    A Distributed Lock implementation for Redis.  The is a variant of the
    Redlock algorithm.
    """
    def __init__(self, mr_client):
        """
        `mr_client` - an instance of the MajorityRedis client.
        """
        self._mr = mr_client

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
        t_start, t_expireat = util.get_expireat(self._mr._timeout)
        rv = util.run_script(
            SCRIPTS, self._mr._map_async, 'l_lock', self._mr._clients,
            path=path, client_id=self._mr._client_id, expireat=t_expireat)
        n = sum(x[1] == 1 for x in rv if not isinstance(x, Exception))
        if n < self._mr._n_servers // 2 + 1:
            self.unlock(path)
            return False
        if not util.lock_still_valid(
                t_expireat, self._mr._clock_drift, self._mr._polling_interval):
            return False
        if extend_lock:
            util.continually_extend_lock_in_background(
                path, self.extend_lock, self._mr._polling_interval,
                self._mr._Timer, extend_lock)
            return t_expireat

    def unlock(self, path):
        """Remove the lock at given `path`
        Return % of servers that are currently unlocked"""
        rv = util.run_script(
            SCRIPTS, self._mr._map_async, 'l_unlock', self._mr._clients,
            path=path, client_id=self._mr._client_id)
        cnt = sum(x[1] == 1 for x in rv if not isinstance(x, Exception))
        return float(cnt) / self._mr._n_servers

    def extend_lock(self, path):
        """
        If you have received an item from the queue and wish to hold the lock
        on it for an amount of time close to or longer than the timeout, you
        must extend the lock!

        Returns one of the following:
            0 if failed to extend_lock
            number of seconds since epoch in the future when lock will expire
        """
        t_start, t_expireat = util.get_expireat(self._mr._timeout)
        locks = util.run_script(
            SCRIPTS, self._mr._map_async, 'l_extend_lock', self._mr._clients,
            path=path, client_id=self._mr._client_id, expireat=t_expireat)
        cnt = sum(x[1] == 1 for x in locks if not isinstance(x, Exception))
        if cnt < self._mr._n_servers // 2 + 1:
            return False
        # Re-lock nodes where lock is lost. By this point we have majority
        if util.lock_still_valid(
                t_expireat, self._mr._clock_drift, self._mr._polling_interval):
            list(util.run_script(
                SCRIPTS, self._mr._map_async, 'l_lock', self._mr._clients,
                path=path, client_id=self._mr._client_id, expireat=t_expireat))
        return util.lock_still_valid(
            t_expireat, self._mr._clock_drift, self._mr._polling_interval)
