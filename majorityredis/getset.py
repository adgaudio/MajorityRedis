import time
from itertools import chain

from . import exceptions
from . import util
from . import log


SCRIPTS = dict(

    # returns (value, timestamp) or an error
    gs_get=dict(keys=('path', 'hist'), args=(), script="""
local val = redis.call("GET", KEYS[1])
if nil == val then return {err="key does not exist"} end
local ts = redis.call("ZSCORE", KEYS[2], KEYS[1])
return {val, ts}
"""),

    # returns (prev_value, prev_timestamp)
    gs_set=dict(keys=('path', 'hist'), args=('val', 'ts'), script="""
local oldts = redis.call("ZSCORE", KEYS[2], KEYS[1])
if oldts ~= false and tonumber(oldts) >= tonumber(ARGV[2]) then
  return {redis.call("GET", KEYS[1]), oldts}
else
  local oldval = redis.call("GETSET", KEYS[1], ARGV[1])
  redis.call("ZADD", KEYS[2], ARGV[2], KEYS[1])
  return {oldval, oldts}
end
"""),

)


class GetSet(object):
    _getset_hist_key = '.majorityredis_getset'

    def __init__(self, mr_client):
        """
        `mr_client` - an instance of the MajorityRedis client.
        """
        self._mr = mr_client

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
            SCRIPTS, self._mr._map_async, 'gs_get', self._mr._clients,
            path=path, hist=self._getset_hist_key)
        responses, winner, fail_cnt = self._getset_responses(
            gen, strongly_consistent)

        if strongly_consistent and fail_cnt == self._mr._n_servers:
            raise exceptions.GetError(
                "Got errors from all redis servers")
        elif not strongly_consistent:
            if fail_cnt >= self._mr._n_servers // 2 + 1:
                raise exceptions.GetError(
                    "Could not get majority agreement from Redis instances")
        # update the clients with stale values
        outdated_clients = (
            cli for cli, val_ts in responses if val_ts != winner)
        val, ts = winner
        list(util.run_script(
            SCRIPTS, self._mr._map_async, 'gs_set', outdated_clients,
            path=path, hist=self._getset_hist_key, val=val, ts=ts))
        return val

    def set(self, path, value):
        """
        Set value at given path.  Fail if could not set on majority of servers
        """
        ts = time.time()
        gen = util.run_script(
            SCRIPTS, self._mr._map_async, 'gs_set', self._mr._clients,
            path=path, hist=self._getset_hist_key, val=value, ts=ts)
        # returned values will be either
        #  Exception
        #  > ts (which means that this value should be already propagated or
        #    propagating, and another client is probably taking care of this,
        #    but doesn't hurt if we do it too)
        #  < ts (stuff we can consider to roll back)
        old_values, _, fail_cnt = self._getset_responses(gen, False)
        if fail_cnt < self._mr._n_servers // 2 + 1:
            return True
        else:
            old_values = list(old_values)
            if fail_cnt == self._mr._n_servers:
                log.warn(
                    "All servers returned exception when trying to set path",
                    extra=dict(path=path))
                return False
            log.debug(
                "Could not set value on majority of instances.  Attempting"
                " to safely roll back to last best known value", extra=dict(
                    path=path))
            max_ts, max_val = max((ts, val) for cli, (val, ts) in old_values)
            bad_clients = (
                cli for cli, (val, ts) in old_values
                if ts != max_ts and val != max_val)
            list(util.run_script(
                SCRIPTS, self._mr._map_async, 'gs_set', bad_clients,
                path=path, hist=self._getset_hist_key, val=max_val, ts=max_ts))
            return False

    def _getset_responses(self, gen, strongly_consistent):
        responses = []
        winner = (None, None)
        fail_cnt = 0
        quorum = self._mr._n_servers // 2 + 1
        for client, val_ts in gen:
            if isinstance(val_ts, Exception):
                fail_cnt += 1
                continue
            responses.append((client, val_ts))

            if winner[1] is None:
                winner = val_ts
            elif val_ts[1] is None:
                pass  # object does not exist
            elif val_ts[1] > winner[1]:
                winner = val_ts
            if not strongly_consistent and len(responses) >= quorum:
                break
        return chain(responses, gen), winner, fail_cnt
