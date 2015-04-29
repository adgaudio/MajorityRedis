import logging
import time
from itertools import chain

from .majorityredis_base import MajorityRedisBaseClass
from . import exceptions
from . import util

log = logging.getLogger('majorityredis.getset')

SCRIPTS = dict(

    # returns (value, timestamp) or an error
    gs_get=dict(keys=('path', 'hist'), args=(), script="""
local val = redis.call("GET", KEYS[1])
if nil == val then return {err="key does not exist"} end
local ts = redis.call("ZSCORE", KEYS[2], KEYS[1])
return {val, ts}
"""),

    # returns (prev_value, prev_timestamp) or (nil, nil)
    gs_set=dict(keys=('path', 'hist'), args=('val', 'ts'), script="""
local oldval = redis.call("GETSET", KEYS[1], ARGV[1])
if nil == oldval then return {nil, nil} end
local oldts = redis.call("ZSCORE", KEYS[2], KEYS[1])
redis.call("ZADD", KEYS[2], ARGV[2], KEYS[1])
return {oldval, oldts}
"""),

    # returns nil.  tries to update value with new value if it's newest one
    gs_reconcile=dict(keys=('path', 'hist'), args=('val', 'ts'), script="""
local ts = redis.call("ZSCORE", KEYS[2], KEYS[1])
if ts >= ARGV[2] then return end
redis.call("ZADD", KEYS[2], ARGV[2], KEYS[1])
redis.call("SET", KEYS[1], ARGV[1])
"""),
)


class GetSet(MajorityRedisBaseClass):
    def get(self, path, strongly_consistent=False):
        """
        Return value at given path.

        `path` - a Redis key
        `strongly_consistent` if False,
            If False, guarantees consistency as long as no more than half of
            the redis servers have died since the last get or set.
            If True, queries all servers to guarantee consistency.  The cost
            is possibly slower response
        """
        gen = util.run_script(
            SCRIPTS, self._map_async, 'gs_get', self._clients,
            path=path, hist=self._getset_hist_key)
        responses, winner, fail_cnt = self._getset_responses(
            self, gen, strongly_consistent)

        if strongly_consistent and fail_cnt == self._n_servers:
            raise exceptions.GetError(
                "Got errors from all redis servers")
        elif not strongly_consistent and fail_cnt >= self._n_servers // 2 + 1:
            raise exceptions.GetError(
                "Could not get majority agreement from Redis instances")
        # update the clients with stale values
        outdated_clients = (
            cli for cli, val_ts in responses if val_ts != winner)
        val, ts = winner
        list(util.run_script(
            SCRIPTS, self._map_async, 'gs_set', outdated_clients,
            path=path, hist=self._getset_hist_key, val=val, ts=ts))
        return val

    def set(self, path, value):
        """
        Set value at given path.  Fail if could not set on majority of servers
        """
        ts = time.time()
        gen = util.run_script(
            SCRIPTS, self._map_async, 'gs_set', self._clients,
            path=path, hist=self._getset_hist_key, val=value, ts=ts)
        old_values, _, fail_cnt = self._getset_responses(gen, False)
        if fail_cnt < self._n_servers // 2 + 1:
            return True
        else:
            log.warn(
                "Could not set value on majority of instances.  Attempting"
                " to safely roll back to last best known value", extra=dict(
                    path=path))
            old_values = list(old_values)
            max_ts, max_val = max((ts, val) for cli, (val, ts) in old_values)
            bad_clients = (
                cli for cli, (val, ts) in old_values
                if ts != max_ts and val != max_val)
            list(util.run_script(
                SCRIPTS, self._map_async, 'gs_reconcile', bad_clients,
                path=path, hist=self._getset_hist_key, val=max_val, ts=max_ts))
            return False

    def _getset_responses(self, gen, strongly_consistent):
        responses = []
        winner = (None, None)
        fail_cnt = 0
        quorum = self._n_servers // 2 + 1
        for client, val_ts in gen:
            if isinstance(val_ts, Exception):
                fail_cnt += 1
                continue
            responses.append((client, val_ts))
            if val_ts[1] > winner[1]:
                winner = val_ts
            if not strongly_consistent and len(responses) >= quorum:
                break
        return chain(responses, gen), winner, fail_cnt
