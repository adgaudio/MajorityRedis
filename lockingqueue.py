"""
In progress.
Distributed Locking Queue for Redis adapted from the Redlock algorithm.
"""
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
""")
)


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
        self._clients = clients
        self._timeout = timeout
        self._n_servers = float(n_servers)
        self._params = dict(
            Q=queue_path,
            client_id=random.randint(0, sys.maxint),
        )

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
        expireat = get_expireat(self._timeout)
        client, h_k = next(run_script(
            'get', self._clients, expireat=expireat, **self._params))
        locks = run_script(
            'lock', self._clients, skip_clients=[client],
            h_k=h_k, expireat=expireat, **self._params)
        priority, insert_time, item = h_k.split(':', 2)
        # TODO: verify that return value of locks is integer 0 or 1
        if sum(x[1] for x in locks) < (self.n_servers // 2 + 1):
            log.debug("Could not get majority of locks for item", extra=dict(
                h_k=h_k, item=item))
            return

        # continually_extend_lock_in_background(h_k)  # TODO
        return item

    def extend_lock(self, h_k):
        raise NotImplementedError("...")
        n_extended = sum(
            x[1] for x in run_script(
                'extend_lock', self._clients,
                h_k=h_k, expireat=get_expireat(self._timeout), **self._params))
        return n_extended / self._n_servers

    def consume(self, h_k):
        raise NotImplementedError("...")
        n_consumed = sum(
            x[1] for x in run_script(
                'extend_lock', self._clients, h_k=h_k, **self._params))
        return n_consumed / self._n_servers


def get_expireat(timeout):
    return time.time() + timeout


def send_script_to_redis(clients, script):
    for client in clients:
        try:
            yield client.script_load(script)
        except redis.RedisError:
            try:  # again
                yield client.script_load(script)
            except Exception as err:
                # this is pretty bad, but not a total blocker.
                log.warning(
                    "Could not load script on redis server", extra=dict(
                        error=err, error_type=type(err).__name__,
                        redis_client=client))
                yield None


def run_script(script_name, clients, skip_clients=(), **kwargs):
    if skip_clients and not all(x in clients for x in skip_clients):
        raise UserWarning("skip_clients must be a subset of clients")
    dct = SCRIPTS[script_name]
    keys = [kwargs[x] for x in dct['keys']]
    args = [kwargs[x] if x != 'randint' else random.randint(0, sys.maxint)
            for x in dct['args']]
    try:
        shas = dct['shas']
        maybe_recalcsha = True
    except KeyError:
        shas = dct['shas'] = list(send_script_to_redis(clients, dct['script']))
        maybe_recalcsha = False
    for i, (client, sha) in enumerate(zip(clients, shas)):
        if sha is None:
            if not maybe_recalcsha:
                continue
            try:  # still can't upload script to this redis server
                sha = client.script_load(dct['script'])
            except:
                continue
            assert dct['shas'][i] is None, "Code bug"
            dct['shas'][i] = sha
        try:
            yield (client, client.evalsha(sha, len(keys), keys, args))
        except redis.RedisError as err:
            log.warning(
                "%s" % err, extra=dict(
                    error=err, error_type=type(err).__name__,
                    redis_client=client, script_name=script_name))
            yield (client, None)


# TODO: consider 2 lists instead of sorted set if priority doesn't matter
# assuming that the keys cannot timeout in the middle of an eval
