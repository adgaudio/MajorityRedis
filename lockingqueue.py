"""
In progress.
Distributed Locking Queue for Redis adapted from the Redlock algorithm.

# TODO: consider 2 lists instead of sorted set if priority doesn't matter
# assuming that the keys cannot timeout in the middle of an eval
"""
import logging
import random
import sys
import time
import redis
import threading
import concurrent.futures

log = logging.getLogger('redis.lockingqueue')


# f = findable set
# h_k = ordered hash of key in form   priority:insert_time_since_epoch:key
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

    # return 1 if extended lock.  Returns an error otherwise.
    # otherwise
    extend_lock=dict(keys=('h_k', ), args=('expireat', 'client_id'), script="""
local rv = redis.call("GET", KEYS[1])
if ARGV[2] == rv then
    redis.call("EXPIREAT", KEYS[1], ARGV[1])
    return 1
elseif "completed" == rv then return {err="already completed"}
else return {err="expired"} end
"""),

    # returns 1 if removed, 0 if key was already removed.
    consume=dict(
        keys=('h_k', 'Q'), args=('client_id', ), script="""
if ARGV[1] ~= redis.call("GET", KEYS[1]) then
  return 0
else
  redis.call("SET", KEYS[1], "completed")
  redis.call("PERSIST", KEYS[1])  -- or EXPIRE far into the future...
  return(redis.call("ZREM", KEYS[2], KEYS[1]))
end
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

    # returns number of items in queue currently being processed
    # O(n)  -- eek!
    qsize=dict(
        keys=('Q', ), args=(), script="""
local taken = 0
local queued = 0
for _,k in ipairs(redis.call("ZRANGE", KEYS[1], 0, -1)) do
  local v = redis.call("GET", k)
  if v then taken = taken + 1
  else queued = queued + 1 end
end
return {taken, queued}
"""),
)


# Sha1 hash of each lua script.  Figured out at run time, and then cached here.
# { script_name: {client: sha, client2: sha, ...}, ...}
SHAS = {script_name: {} for script_name in SCRIPTS}


class CannotObtainLock(Exception):
    pass


class ConsumeError(Exception):
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

    def __init__(self, queue_path, clients, n_servers, timeout=5,
                 Timer=threading.Timer,
                 Executor=concurrent.futures.ThreadPoolExecutor):
        """
        `queue_path` - a Redis key specifying where the queued items are
        `clients` - a list of redis.StrictRedis clients,
            each connected to a different Redis server
        `n_servers` - the number of Redis servers in your cluster
            (whether or not you have a client connected to it)
        `Timer` - implements the threading.Timer api.  If you do not with to
            use Python's threading module, pass in something else here.
        `Executor` - implements the concurrent.futures.ThreadPoolExecutor api
            If you don't want to use threads, pass in your own Executor
        """
        if len(clients) < n_servers // 2 + 1:
            raise CannotObtainLock(
                "Must connect to at least half of the redis servers to"
                " obtain majority")
        self._Executor = Executor
        self._Timer = Timer
        self._polling_interval = timeout / 5.  # TODO
        self._drift = 0  # TODO
        self._clients = clients
        self._timeout = timeout
        self._n_servers = float(n_servers)
        self._params = dict(
            Q=queue_path,
            client_id=random.randint(0, sys.maxsize),
        )

    def size(self, queued=True, taken=True):
        """
        Return the approximate number of items in the queue, across all servers

        `queued` - number of items in queue that aren't being processed
        `taken` - number of items in queue that are currently being processed

        Because we cannot lock all redis servers at the same time and we don't
        store a lock/unlock history, we cannot get the exact number of items in
        the queue at a specific time.

        If you change the default parameters (taken=True, queued=True), the
        time complexity increases from O(log(n)) to O(n).
        """
        if not queued and not taken:
            raise UserWarning("Queued and taken cannot both be False")
        if taken and queued:
            return max(self._Executor(len(self._clients)).map(
                lambda cli: cli.zcard(self._params['Q']), self._clients))

        taken_queued_counts = (x[1] for x in run_script(
            self._Executor, 'qsize', self._clients, **(self._params)))
        if taken and not queued:
            return max(x[0] for x in taken_queued_counts)
        if queued and not taken:
            return max(x[1] for x in taken_queued_counts)

    def extend_lock(self, h_k):
        """
        If you have received an item from the queue and wish to hold the lock
        on it for an amount of time close to or longer than the timeout, you
        must extend the lock!

        Returns one of the following:
            -1 if a redis server reported that the item is completed
            0 if otherwise failed to extend_lock
            number of seconds since epoch in the future when lock will expire
        """
        _, t_expireat = get_expireat(self._timeout)
        locks = list(run_script(
            self._Executor, 'extend_lock', self._clients,
            h_k=h_k, expireat=t_expireat, **(self._params)))
        if not self._verify_not_already_completed(locks, h_k):
            return -1
        if not self._have_majority(locks, h_k):
            return 0
        if not self._lock_still_valid(t_expireat):
            return 0
        return t_expireat

    def consume(self, h_k, clients=None):
        """Remove item from queue"""
        if clients is None:
            clients = self._clients
        n_success = sum(
            x[1] for x in run_script(
                self._Executor, 'consume', clients, h_k=h_k, **(self._params)))
        if n_success == 0:
            raise ConsumeError(
                "Failed to mark the item as completed on any redis server")
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

    def get(self, extend_lock=True):
        """
        Attempt to get an item from queue and obtain a lock on it to
        guarantee nobody else has a lock on this item.

        Returns an (item, h_k) or None.  An empty return value does
        not necessarily mean the queue is (or was) empty, though it's probably
        nearly empty.  `h_k` uniquely identifies the queued item

        `keep_lock` - If True, spin up a background thread that
            extends the lock indefinitely until it is explicitly consumed or
            we can no longer communicate with the majority of redis servers.
            If False, you need to set a very large timeout or call
            extend_lock() before the lock times out.
        """
        t_start, t_expireat = get_expireat(self._timeout)
        client, h_k = next(run_script(
            self._Executor, 'get', random.sample(self._clients, 1),
            expireat=t_expireat, **(self._params)))
        if isinstance(h_k, Exception):
            return
        if self._acquire_lock_majority(client, h_k, t_start, t_expireat):
            if extend_lock:
                continually_extend_lock_in_background(h_k, self)
            priority, insert_time, item = h_k.decode().split(':', 2)
            return item, h_k

    def _acquire_lock_majority(self, client, h_k, t_start, t_expireat):
        """We've gotten and locked an item on a single redis instance.
        Attempt to get the lock on all remaining instances, and
        handle all scenarios where we fail to acquire the lock.

        Return True if acquired majority of locks, False otherwise.
        """
        locks = run_script(
            self._Executor, 'lock', [x for x in self._clients if x != client],
            h_k=h_k, expireat=t_expireat, **(self._params))
        locks = list(locks)
        locks.append((client, 1))
        if not self._verify_not_already_completed(locks, h_k):
            return False
        if not self._have_majority(locks, h_k):
            return False
        if not self._lock_still_valid(t_expireat):
            return False
        return True

    def _verify_not_already_completed(self, locks, h_k):
        """If any Redis server reported that the key, `h_k`, was completed,
        return False and update all servers that don't know this fact.
        """
        completed = ["%s" % l == "already completed" for _, l in locks]
        if any(completed):
            outdated_clients = [
                cli for (cli, _), marked_done in zip(locks, completed)
                if not marked_done]
            list(run_script(
                self._Executor,
                'consume',
                clients=outdated_clients,
                h_k=h_k, **(self._params)))
            return False
        return True

    def _have_majority(self, locks, h_k):
        """Evaluate whether the number of obtained is > half the number of
        redis servers.  If didn't get majority, unlock the locks we got.

        `locks` - a list of (client, have_lock) pairs.
            client is one of the redis clients
            have_lock may be 0, 1 or an Exception
        """
        cnt = sum(x[1] == 1 for x in locks if not isinstance(x, Exception))
        if cnt < (self._n_servers // 2 + 1):
            log.debug("Could not get majority of locks for item.", extra=dict(
                h_k=h_k))
            list(run_script(
                self._Executor,
                'unlock', [cli for cli, lock in locks if lock == 1],
                h_k=h_k, **(self._params)))
            return False
        return True

    def _lock_still_valid(self, t_expireat):
        if t_expireat < 0:
            return False
        secs_left = \
            t_expireat - time.time() - self._drift - self._polling_interval
        if not secs_left > 0:
            return False
        return secs_left


def continually_extend_lock_in_background(h_k, q):
    """
    Extend the lock on given key, `h_k` held by LockingQueue instance, `q`

    Once called, respawns itself indefinitely until extend_lock is unsuccessful
    """
    t_expireat = q.extend_lock(h_k)
    secs_left = q._lock_still_valid(t_expireat)
    if secs_left:
        t = q._Timer(
            min(max(secs_left - q._polling_interval, 0), q._polling_interval),
            continually_extend_lock_in_background, args=(h_k, q))
        t.daemon = True
        t.start()
    elif t_expireat == -1:
        log.debug(
            "Found that item was marked as completed. No longer extending lock",
            extra=dict(h_k=h_k))
        return
    else:
        log.error((
            "Failed to extend the lock.  You should completely stop"
            " processing this item ASAP"), extra=dict(item=h_k))
        raise CannotObtainLock(
            "Failed to extend the lock.  You should completely stop"
            " processing this item ASAP")


def get_expireat(timeout):
    t = time.time()
    return t, int(t + timeout)


def _get_sha(script_name, client):
    try:
        rv = SHAS[script_name][client]
    except KeyError:
        try:
            rv = SHAS[script_name][client] = \
                client.script_load(SCRIPTS[script_name]['script'])
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
        return (client, client.evalsha(sha, len(keys), *(keys + args)))
    except redis.RedisError as err:
        log.debug(
            "Redis Error: %s" % err, extra=dict(
                error=err, error_type=type(err).__name__,
                redis_client=client, script_name=script_name))
        return (client, err)


def run_script(Executor, script_name, clients, **kwargs):
    keys = [kwargs[x] for x in SCRIPTS[script_name]['keys']]
    args = [kwargs[x] if x != 'randint' else random.randint(0, sys.maxsize)
            for x in SCRIPTS[script_name]['args']]
    return Executor(len(clients)).map(
        lambda client: _run_script(script_name, client, keys, args), clients)
