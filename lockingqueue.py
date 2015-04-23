"""
In progress.
Distributed Locking Queue for Redis adapted from the Redlock algorithm.

# TODO: consider 2 lists instead of sorted set if priority doesn't matter
# assuming that the keys cannot timeout in the middle of an eval
"""
from itertools import chain
import random
import sys
import time
import redis
import logging
log = logging.getLogger('redis.lockingqueue')


# f = findable set
# h_k = time-ordered hash of key in form   priority:insert_time_since_epoch:key
# expireat = seconds_since_epoch + 5 seconds + whatever redlock figured out
# client_id = owner of the lock, if we can obtain it.
# randint = a random integer that changes every time script is called


# Lua scripts that are sent to redis
SCRIPTS = dict(

    # returns 1 if got an item, and returns an error otherwise
    get=dict(keys=('Q', ), args=('client_id', 'expireat'), script="""
local h_k = redis.call("ZRANGE", KEYS[1], 0, 0)[1]
if nil == h_k then return {err="queue empty"} end
if 1 ~= redis.call("SETNX", h_k, ARGV[1]) then
  return {err="already locked"} end
if 1 ~= redis.call("EXPIREAT", h_k, ARGV[2]) then
  return {err="invalid expireat"} end
redis.call("ZINCRBY", KEYS[1], 1, h_k)
return h_k
"""),

    # returns 1 if got lock. Returns an error otherwise
    lock=dict(
        keys=('h_k', 'Q'), args=('expireat', 'randint', 'client_id'), script="""
if 0 == redis.call("SETNX", KEYS[1], ARGV[3]) then  -- did not get lock
  if redis.call("GET", KEYS[1]) == "completed" then
    redis.call("ZREM", KEYS[2], KEYS[1])
    return {err="already completed"}
  else
    local score = redis.call("ZSCORE", KEYS[2], KEYS[1])
    math.randomseed(tonumber(ARGV[2]))
    local num = math.random(math.floor(score) + 1)
    if num ~= 1 then
      redis.call("ZINCRBY", KEYS[2], (num-1)/score, KEYS[1])
    end
    return {err="already locked"}
  end
else
  redis.call("EXPIREAT", KEYS[1], ARGV[1])
  redis.call("ZINCRBY", KEYS[2], 1, KEYS[1])
  return 1
end
"""),

    # return 1 if extended lock, 0 if failed to extend lock.
    extend_lock=dict(keys=('h_k', ), args=('expireat', 'client_id'), script="""
if ARGV[2] == redis.call("GET", KEYS[1]) then
    redis.call("EXPIREAT", KEYS[1], ARGV[1])
    return 1
else
    return 0
end
"""),

    # returns 1 if removed, 0 if key was already removed.
    consume=dict(
        keys=('h_k', 'Q'), args=(), script="""
redis.call("SET", KEYS[1], "completed")
redis.call("PERSIST", KEYS[1])  -- or EXPIRE far into the future...
return(redis.call("ZREM", KEYS[2], KEYS[1]))
"""),

    # returns 1 if removed, 0 otherwise
    unlock=dict(
        keys=('h_k', ), args=('client_id', ), script="""
if ARGV[1] == redis.call("GET", KEYS[1]) then
    return(redis.call("DEL", KEYS[1]))
else
    return(0)
end
"""),
)


# Sha1 hash of each lua script.  Figured out at run time, and then cached here.
# { script_name: {client: sha, client2: sha, ...}, ...}
SHAS = {script_name: {} for script_name in SCRIPTS}


class CannotObtainLock(Exception):
    pass


class LockingQueue(object):
    """
    A Distributed Locking Queue implementation for Redis.

    The queue expects to receive at least 1 redis.StrictRedis client,
    where each client is connected to a different Redis server.
    When instantiating this class, if you do not ensure that the number
    of servers defined is always constant, you risk the possibility that
    the same lock may be obtained multiple times.
    """

    def __init__(self, queue_path, clients, n_servers, timeout=5):
        """
        `queue_path` - a Redis key specifying where the queued items are
        `clients` - a list of redis.StrictRedis clients,
            each connected to a different Redis server
        `n_servers` - the number of Redis servers in your cluster
            (whether or not you have a client connected to it)
        """
        if len(clients) < n_servers // 2 + 1:
            raise CannotObtainLock(
                "Must connect to at least half of the redis servers to"
                " obtain majority")
        self._polling_interval = 0  # TODO
        self._drift = 0  # TODO
        self._clients = clients
        self._timeout = timeout
        self._n_servers = float(n_servers)
        self._params = dict(
            Q=queue_path,
            client_id=random.randint(0, sys.maxint),
        )

    def extend_lock(self, h_k):
        _, t_expireat = get_expireat(self._timeout)
        locks = run_script(
            'extend_lock', self._clients,
            h_k=h_k, expireat=t_expireat, **self._params)
        if not self._have_majority(locks, h_k):
            return False
        if not self._lock_still_valid(t_expireat):
            return False
        return t_expireat

    def consume(self, h_k, clients=None):
        if clients is None:
            clients = self._clients
        n_success = sum(
            x[1] for x in run_script(
                'consume', clients, h_k=h_k, **self._params))
        return n_success / self._n_servers

    def put(self, item, priority=100):
        """
        Put item onto queue.  Priority defines whether to prioritize
        getting this item off the queue before other items.
        Priority is not guaranteed
        """
        h_k = "%d:%f:%s" % (priority, time.time(), item)
        cnt = 0.
        for cli in self._clients:
            try:
                cnt += cli.zadd(self._params['Q'], 0, h_k)
            except redis.RedisError as err:
                log.warning(
                    "Could not put item onto a redis server.", extra=dict(
                        error=err, error_type=type(err).__name__,
                        redis_client=cli))
                continue
        return cnt / self._n_servers

    def get(self, continue_to_extend_lock=True):
        """
        Attempt to get an item from queue and obtain a lock on it to
        guarantee nobody else has a lock on this item.

        Returns an item or None.  An empty return value does not necessarily
        mean the queue is (or was) empty, though it's probably nearly empty.

        `continue_to_extend_lock` - If True, spin up a background thread that
            extends the lock indefinitely until it is explicitly consumed or
            we can no longer communicate with the majority of redis servers.
            If False, you need to set a very large timeout or call
            extend_lock() before the lock times out.
        """
        t_start, t_expireat = get_expireat(self._timeout)
        client, h_k = run_script(
            'get', random.sample(self._clients, 1),
            expireat=t_expireat, **self._params)
        if isinstance(h_k, Exception):
            return
        if self._acquire_lock_majority(client, h_k, t_start, t_expireat):
            # TODO: continually_extend_lock_in_background(h_k)
            priority, insert_time, item = h_k.split(':', 2)
            return item

    def _acquire_lock_majority(self, client, h_k, t_start, t_expireat):
        """We've gotten and locked an item on a single redis instance.
        Attempt to get the lock on all remaining instances, and
        handle all scenarios where we fail to acquire the lock.

        Return True if acquired majority of locks, False otherwise.
        """
        locks = run_script(
            'lock', [x for x in self._clients if x != client],
            h_k=h_k, expireat=t_expireat, **self._params)
        locks = list(locks)
        if not self._verify_not_already_completed(locks, client, h_k):
            return False
        if not self._have_majority(locks, h_k, already_have_a_lock=True):
            return False
        if not self._lock_still_valid(t_expireat):
            return False
        return True

    def _verify_not_already_completed(self, locks, client, h_k):
        """If any Redis server reported that the key, `h_k`, was completed,
        return False and update all servers that don't know this fact.
        """
        completed = [(c, "%s" % l == "already completed") for c, l in locks]
        if any(x[1] for x in completed):
            list(run_script(
                'consume',
                clients=chain(
                    (cli for cli, marked_done in completed if not marked_done),
                    [client]),
                h_k=h_k, **self._params))
            return False
        return True

    def _have_majority(self, locks, h_k, already_have_a_lock=False):
        """Evaluate whether the number of obtained is > half the number of
        redis servers.  If didn't get majority, unlock the locks we got.

        `locks` - a list of (client, have_lock) pairs.
            client is one of the redis clients
            have_lock may be 0, 1 or an Exception
        `already_have_a_lock` is True if `locks` is does not include a
            you already have"""
        cnt = sum(x[1] == 1 for x in locks if not isinstance(x, Exception))
        cnt += already_have_a_lock
        if cnt < (self._n_servers // 2 + 1):
            log.debug("Could not get majority of locks for item.", extra=dict(
                h_k=h_k))
            list(run_script(
                'unlock', [cli for cli, lock in locks if lock == 1],
                h_k=h_k, **self._params))
            return False
        return True

    def _lock_still_valid(self, t_expireat):
        secs_left = \
            t_expireat - time.time() - self._drift - self._polling_interval
        if not secs_left > 0:
            return False
        return secs_left


def get_expireat(timeout):
    t = time.time()
    return t, t + timeout


def _get_sha(script_name, client, script):
    try:
        rv = SHAS[script_name][client]
    except KeyError:
        try:
            rv = SHAS[script_name][client] = client.script_load(script)
        except redis.RedisError as err:
            # this is pretty bad, but not a total blocker.
            rv = err
            log.warning(
                "Could not load script on redis server: %s" % err, extra=dict(
                    error=err, error_type=type(err).__name__,
                    redis_client=client))
    return rv


def _run_script(script_name, client, keys, args):
    sha = _get_sha(script_name, client)
    if isinstance(sha, Exception):
        return (client, sha)

    try:
        return (client, client.evalsha(sha, len(keys), keys, args))
    except redis.RedisError as err:
        log.warning(
            "%s" % err, extra=dict(
                error=err, error_type=type(err).__name__,
                redis_client=client, script_name=script_name))
        return (client, err)


def run_script(script_name, clients, **kwargs):
    keys = [kwargs[x] for x in SCRIPTS[script_name]['keys']]
    args = [kwargs[x] if x != 'randint' else random.randint(0, sys.maxint)
            for x in SCRIPTS[script_name]['args']]
    for client in clients:
        yield _run_script(script_name, client, keys, args)
