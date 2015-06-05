"""
Distributed Locking Queue for Redis adapted from the Redlock algorithm.
"""
import random
import sys
import time
from itertools import chain

from . import util
from . import exceptions
from . import log


# Lua scripts that are sent to redis
SCRIPTS = dict(
    # keys:
    # h_k = ordered hash of key in form:  priority:insert_time_since_epoch:key
    # Q = sorted set of queued keys, h_k
    # Qi = sorted set mapping h_k to key for all known queued or completed items
    #
    # args:
    # expireat = seconds_since_epoch, presumably in the future
    # client_id = unique owner of the lock
    # randint = a random integer that changes every time script is called

    # returns 1
    lq_put=dict(keys=('Q', 'h_k'), args=(), script="""
redis.call("ZINCRBY", KEYS[1], 0, KEYS[2])
return 1
"""),

    # returns 1 if got an item, and returns an error otherwise
    lq_get=dict(keys=('Q', ), args=('client_id', 'expireat'), script="""
local h_k = redis.call("ZRANGE", KEYS[1], 0, 0)[1]
if false == h_k then return {err="queue empty"} end
if false == redis.call("SET", h_k, ARGV[1], "NX") then
  return {err="already locked"} end
if 1 ~= redis.call("EXPIREAT", h_k, ARGV[2]) then
  return {err="invalid expireat"} end
redis.call("ZINCRBY", KEYS[1], 1, h_k)
return h_k
"""),

    # returns 1 if got lock. Returns an error otherwise
    lq_lock=dict(
        keys=('h_k', 'Q'), args=('expireat', 'randint', 'client_id'), script="""
if false == redis.call("SET", KEYS[1], ARGV[3], "NX") then  -- did not get lock
  local rv = redis.call("GET", KEYS[1])
  if rv == "completed" then
    redis.call("ZREM", KEYS[2], KEYS[1])
    return {err="already completed"}
  elseif rv == ARGV[3] then
    if 1 ~= redis.call("EXPIREAT", KEYS[1], ARGV[1]) then
      return {err="invalid expireat"} end
    return 1
  else
    local score = tonumber(redis.call("ZSCORE", KEYS[2], KEYS[1]))
    math.randomseed(tonumber(ARGV[2]))
    local num = math.random(math.floor(score) + 1)
    if num ~= 1 then
      redis.call("ZINCRBY", KEYS[2], (num-1)/score, KEYS[1])
    end
    return {err="already locked"}
  end
else
  if 1 ~= redis.call("EXPIREAT", KEYS[1], ARGV[1]) then
    return {err="invalid expireat"} end
  redis.call("ZINCRBY", KEYS[2], 1, KEYS[1])
  return 1
end
"""),

    # return 1 if extended lock.  Returns an error otherwise.
    # otherwise
    lq_extend_lock=dict(
        keys=('h_k', ), args=('expireat', 'client_id'), script="""
local rv = redis.call("GET", KEYS[1])
if ARGV[2] == rv then
    if 1 ~= redis.call("EXPIREAT", KEYS[1], ARGV[1]) then
      return {err="invalid expireat"} end
    return 1
elseif "completed" == rv then return {err="already completed"}
elseif false == rv then return {err="expired"}
else return {err="lock stolen"} end
"""),

    # returns 1 if removed, 0 if key was already removed.
    lq_consume=dict(
        keys=('h_k', 'Q', 'Qi'), args=('client_id', ), script="""
local rv = redis.pcall("GET", KEYS[1])
if ARGV[1] == rv or "completed" == rv then
  redis.call("SET", KEYS[1], "completed")
  redis.call("PERSIST", KEYS[1])  -- or EXPIRE far into the future...
  redis.call("ZREM", KEYS[2], KEYS[1])
  if "completed" ~= rv then redis.call("INCR", KEYS[3]) end
  return 1
else return 0 end
"""),

    # returns nil.  markes job completed
    lq_completed=dict(
        keys=('h_k', 'Q', 'Qi'), args=(), script="""
if "completed" ~= redis.call("GET", KEYS[1]) then
  redis.call("INCR", KEYS[3])
  redis.call("SET", KEYS[1], "completed")
  redis.call("PERSIST", KEYS[1])  -- or EXPIRE far into the future...
  redis.call("ZREM", KEYS[2], KEYS[1])
end
"""),

    # returns 1 if removed, 0 otherwise
    lq_unlock=dict(
        keys=('h_k', ), args=('client_id', ), script="""
if ARGV[1] == redis.call("GET", KEYS[1]) then
    return redis.call("DEL", KEYS[1])
else return 0 end
"""),

    # returns number of items {(queued + taken), completed}
    # O(log(n))
    lq_qsize_fast=dict(
        keys=('Q', 'Qi'), args=(), script="""
return {redis.call("ZCARD", KEYS[1]), redis.call("INCRBY", KEYS[2], 0)}
"""),

    # returns number of items {in_queue, taken, completed}
    # O(n)  -- eek!
    lq_qsize_slow=dict(
        keys=('Q', 'Qi'), args=(), script="""
local taken = 0
local queued = 0
for _,k in ipairs(redis.call("ZRANGE", KEYS[1], 0, -1)) do
  local v = redis.call("GET", k)
  if "completed" ~= v then
    if v then taken = taken + 1
    else queued = queued + 1 end
  end
end
return {queued, taken, redis.call("INCRBY", KEYS[2], 0)}
"""),

    # returns whether an item is in queue or currently being processed.
    # raises an error if already completed.
    # O(1)
    lq_is_queued_h_k=dict(
        keys=('Q', 'h_k'), args=(), script="""
local taken = redis.call("GET", KEYS[2])
if "completed" == taken then
  return {err="already completed"}
elseif taken then return {true, false}
else return {false, false ~= redis.call("ZSCORE", KEYS[1], KEYS[2])} end
"""),

    # returns whether an item is in queue or currently being processed.
    # raises an error if already completed.
    # O(N * strlen(item)) -- eek!
    lq_is_queued_item=dict(
        keys=('Q', 'item'), args=(), script="""
for _,k in ipairs(redis.call("ZRANGE", KEYS[1], 0, -1)) do
  if string.sub(k, -string.len(KEYS[2])) == KEYS[2] then
    local taken = redis.call("GET", k)
    if taken then
      if "completed" == taken then return {err="already completed"} end
      return {true, false}
    else
    return {false, true} end
  end
end
return {false, false}
"""),

)


class LockingQueue(object):
    """
    A Distributed Locking Queue implementation for Redis.
    """

    def __init__(self, mr_client, queue_path):
        """
        `mr_client` - an instance of the MajorityRedis client.
        `queue_path` - a Redis key specifying where the queued items are
        """
        if mr_client._threadsafe:
            self._client_id = random.randint(1, sys.maxsize)
        else:
            self._client_id = mr_client._client_id

        self._mr = mr_client
        self._params = dict(
            Q=queue_path, Qi=".%s" % queue_path,
            client_id=self._client_id)

    def size(self, queued=True, taken=True, completed=False):
        """
        Return the approximate number of items in the queue, across all servers

        `queued` - number of items in queue that aren't being processed
        `taken` - number of items in queue that are currently being processed
        `completed` - number of items consumed from queue

        Because we cannot lock all redis servers at the same time and we don't
        store a lock/unlock history, we cannot get the exact number of items in
        the queue at a specific time.

        If the parameters, `taken` and `queued` are not both True or both False,
        the time complexity is O(n) and this can block Redis if you have
        a large queue.  Otherwise, complexity is O(log(n))
        """
        if not queued and not taken and not completed:
            raise UserWarning("At least one kwarg cannot be False")
        if taken == queued:
            counts = (x[1] for x in util.run_script(
                SCRIPTS, self._mr._map_async,
                'lq_qsize_fast', self._mr._clients, **(self._params))
                if not isinstance(x[1], Exception))
            if completed and taken:
                return max(x[0] + x[1] for x in counts)
            i = 0 if taken else 1
            return max(x[i] for x in counts)

        counts = (x[1] for x in util.run_script(
            SCRIPTS, self._mr._map_async,
            'lq_qsize_slow', self._mr._clients, **(self._params))
            if not isinstance(x[1], Exception))
        i = 0 if queued else 1
        if completed:
            return max(x[2] + x[i] for x in counts)
        else:
            return max(x[i] for x in counts)

    def is_queued(self, h_k=None, item=None, taken=True, queued=True,
                  completed=False):
        """
        Return True if item is queued on majority of servers, False otherwise

        `item` - A value that we've put into the queue one or more times
        `h_k` - the item hash that uniquely identifies a put

        `queued` - item is queued but not currently being processed
        `taken` - item is currently being processed
        `completed` - item has been consumed from queue

        If passing an item hash, `h_k`, runtime is O(1)
        If passing an `item` runtime is a slow O(N), and blocks your redis
            while running. Try not to use this too often with large queues.
            Keep in mind that one item can be put many times, so an item can
            map to many item hashes.  We return True if any of the item's
            item_hashes meets your query criteria (taken, queued, completed)
        """
        if not taken and not queued:
            raise UserWarning("either taken or queued must be True")
        results = list(self._is_queued(h_k, item))
        if h_k:
            self._verify_not_already_completed(results, h_k)
        nerrs, cnt = 0, 0
        clis = []
        for cli, taken_queued in results:
            clis.append((cli, taken_queued))
            if isinstance(taken_queued, Exception):
                if completed and str(taken_queued) == "already completed":
                    return True
                nerrs += 1
                if nerrs > self._mr._n_servers // 2:
                    raise exceptions.NoMajority(
                        "Too many exceptions from Redis servers")
            elif taken and queued:
                cnt += (taken_queued[0] == 1 or taken_queued[1] == 1)
            elif taken:
                cnt += taken_queued[0] == 1
            elif queued:
                cnt += taken_queued[1] == 1
            if cnt > self._mr._n_servers // 2:
                return True
        return False

    def _is_queued(self, h_k, item):
        if h_k:
            assert ':' in str(h_k), "did you pass wrong argument?"
            results = util.run_script(
                SCRIPTS, self._mr._map_async,
                'lq_is_queued_h_k', self._mr._clients, h_k=h_k, **self._params)
        elif item:
            results = util.run_script(
                SCRIPTS, self._mr._map_async,
                'lq_is_queued_item', self._mr._clients,
                item=":%s" % item, **self._params)
        else:
            raise UserWarning("Must pass item or item_hash.")
        return results

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
        _, t_expireat = util.get_expireat(self._mr._lock_timeout)
        locks = list(util.run_script(
            SCRIPTS, self._mr._map_async, 'lq_extend_lock', self._mr._clients,
            h_k=h_k, expireat=t_expireat, **(self._params)))
        if not self._verify_not_already_completed(locks, h_k):
            return -1
        if not self._have_majority(locks, h_k):
            return 0
        # Re-lock nodes where lock is lost
        # Recovers state if we lost the lock on any individual nodes but still
        # have majority,  This could cause extend_lock to timeout more
        # frequently, so it might not be a good idea if timeouts are very short
        # on the other hand, if we remove the list(...) call, this could create
        # a memory leak if polling_interval is too short.
        if util.lock_still_valid(
                t_expireat, self._mr._clock_drift, self._mr._polling_interval):
            list(util.run_script(
                SCRIPTS, self._mr._map_async, 'lq_lock',
                [cli for cli, rv in locks if "%s" % rv == "expired"],
                h_k=h_k, expireat=t_expireat, **(self._params)))
        return util.lock_still_valid(
            t_expireat, self._mr._clock_drift, self._mr._polling_interval)

    def consume(self, h_k):
        """Remove item from queue.  Return the percentage of servers we've
        successfully removed item on.

        If the returned value is < 50%, a minority of servers know that the
        item was consumed.  The the item could get locked again
        if this minority of servers is entirely unavailable while another
        client is getting items from the queue.

        You choose whether a return value < 50% is a failure.  You can also
        try to consume the same item twice.
        """
        clients = self._mr._clients
        n_success = sum(
            x[1] == 1 for x in util.run_script(
                SCRIPTS, self._mr._map_async,
                'lq_consume', clients, h_k=h_k, **self._params))
        util.remove_background_thread(h_k, self._client_id)
        if n_success == 0:
            raise exceptions.ConsumeError(
                "Failed to mark the item as completed on any redis server")
        return 100. * n_success / self._mr._n_servers

    def put(self, item, priority=100, retry_condition=None):
        """
        Put item onto queue.  Return tuple like (%, h_k), where % is
        the percentage of servers we've successfully put to and h_k is a
        time and priority dependent hash of the item.

        If the returned percentage value is < 50, a minority of servers know
        about the item.  If those servers die, this item will be lost.  Your
        options are:
            - accept this risk and move on
            - call this function with special parameter, retry_condition.

            this.put('a', 101, majorityredis.retry_condition(lambda x: x

        `item` (str) an item you wish to queue.
        `priority` (num) an option to get this item off the queue before other
            items.  Lower priority scores are gotten first.
            Priority is not guaranteed.

        `retry_condition` (func) continually retry calling this function until
            we successfully put to >50% of servers or a max limit is reached.
            see majorityredis.util.retry_condition for details

            >>> put('a', 100, retry_condition(nretry=10,
                                              backoff=lambda x: x + 1))

            If you wish, you may define a number > 50% like so:

            >>> put('a', 100, retry_condition(nretry=10,
                                              backoff=lambda x: x + 1,
                                              condition=lambda x: x[0] >= 80))
        """
        h_k = "%d:%f:%s" % (priority, time.time(), item)
        if retry_condition:
            put = retry_condition(self._put, lambda x: x[0] > 50)
        else:
            put = self._put
        return put(h_k)

    def _put(self, h_k):
        rv = util.run_script(
            SCRIPTS, self._mr._map_async, 'lq_put', self._mr._clients,
            h_k=h_k, **self._params)
        cnt = sum(x[1] == 1 for x in rv)
        return 100. * cnt / self._mr._n_servers, h_k

    def get(self, extend_lock=True, check_all_servers=True):
        """
        Attempt to get an item from queue and obtain a lock on it to
        guarantee nobody else has a lock on this item.

        Returns an (item, h_k) or None.  An empty return value does
        not necessarily mean the queue is (or was) empty, though it's probably
        nearly empty.  `h_k` uniquely identifies the queued item

        `extend_lock` - If True, extends the lock indefinitely in the
            background until the lock is explicitly consumed or
            we can no longer extend the lock.
            If False, you need to set a very large timeout or call
            extend_lock() before the lock times out.
            If a function, assume True and call function(h_k) if we
            ever fail to extend the lock.
        `check_all_servers` - If True, query all redis servers for an item.
            Attempt to obtain the lock on the first item received.
            If False, query only 1 redis server for an item and attempt to
            obtain a lock on it.  If False and one of the servers is not
            reachable, the min. chance you will get nothing from the queue is
            1 / n_servers.  If True, we always preference the fastest response.
        """
        t_start, t_expireat = util.get_expireat(self._mr._lock_timeout)
        client, h_k = self._get_candidate_keys(t_expireat, check_all_servers)
        if not h_k:
            return
        if self._acquire_lock_majority(client, h_k, t_start, t_expireat):
            if extend_lock:
                util.continually_extend_lock_in_background(
                    h_k, self.extend_lock, self._mr._polling_interval,
                    self._mr._run_async, extend_lock, self._client_id)
            priority, insert_time, item = h_k.decode().split(':', 2)
            return item, h_k

    def _get_candidate_keys(self, t_expireat, check_all_servers):
        """Choose one server to get an item from.  Return (client, key)

        If `check_all_servers` is True, use the results from the first server
        to that returns an item.  This could be dangerous because it
        preferences the fastest server.  If the slowest server for some reason
        had keys that other servers didn't have, these keys would be less likely
        to get synced to the other servers.
        """
        if check_all_servers:
            clis = list(self._mr._clients)
            random.shuffle(clis)
        else:
            clis = random.sample(self._mr._clients, 1)
        generator = util.run_script(
            SCRIPTS, self._mr._map_async,
            'lq_get', clis, expireat=t_expireat, **self._params)

        failed_candidates = []
        winner = (None, None)
        for cclient, ch_k in generator:
            if isinstance(ch_k, Exception):
                failed_candidates.append((cclient, ch_k))
            else:
                winner = (cclient, ch_k)
                return winner
        failed_clients = (
            cclient for cclient, ch_k in chain(generator, failed_candidates))
        list(util.run_script(
            SCRIPTS, self._mr._map_async,
            'lq_unlock', failed_clients,
            h_k=ch_k, **(self._params)))
        return winner

    def _acquire_lock_majority(self, client, h_k, t_start, t_expireat):
        """We've gotten and locked an item on a single redis instance.
        Attempt to get the lock on all remaining instances, and
        handle all scenarios where we fail to acquire the lock.

        Return True if acquired majority of locks, False otherwise.
        """
        locks = util.run_script(
            SCRIPTS, self._mr._map_async, 'lq_lock',
            [x for x in self._mr._clients if x != client],
            h_k=h_k, expireat=t_expireat, **(self._params))
        locks = list(locks)
        locks.append((client, 1))
        if not self._verify_not_already_completed(locks, h_k):
            return False
        if not self._have_majority(locks, h_k):
            return False
        if not util.lock_still_valid(
                t_expireat, self._mr._clock_drift, self._mr._polling_interval):
            return False
        return True

    def _verify_not_already_completed(self, locks, h_k):
        """If any Redis server reported that the key, `h_k`, was completed,
        return False and update all servers that don't know this fact.
        """
        locks = list(locks)
        completed = ["%s" % l == "already completed" for _, l in locks]
        if any(completed):
            self._heal_completed(h_k, locks)
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
        if cnt < (self._mr._n_servers // 2 + 1):
            log.warn("Could not get majority of locks for item.", extra=dict(
                h_k=h_k))
            list(util.run_script(
                SCRIPTS, self._mr._map_async,
                'lq_unlock', [cli for cli, lock in locks if lock == 1],
                h_k=h_k, **(self._params)))
            return False
        return True

    def _heal_completed(self, h_k, client_rv):
        """The given item hash, `h_k`, is "completed" on at least 1 client.
        Mark it completed on the other servers that are up and not sending
        exceptions"""
        outdated_clients = (
            cli for cli, rv in client_rv if not isinstance(rv, Exception))
        list(util.run_script(
            SCRIPTS, self._mr._map_async,
            'lq_completed', clients=outdated_clients,
            h_k=h_k, **(self._params)))
