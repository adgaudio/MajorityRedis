import time
from itertools import chain
from collections import defaultdict

from . import exceptions
from . import util
from . import log


SCRIPTS = dict(

    # returns (prev_value, prev_timestamp) and set value if ts is new enough
    # returns exception if did not set (due to nx or xx)
    gs_set=dict(keys=('path', 'hist'), args=('ts', 'val', 'nx_or_xx'),
                script="""
local oldts = redis.call("ZSCORE", KEYS[2], KEYS[1])
local oldval = redis.call("GET", KEYS[1])
if oldts ~= false and tonumber(oldts) > tonumber(ARGV[1]) then
  return {oldval, oldts, 0}
else
  -- set value
  local rv
  if '' == ARGV[3] then
    rv = redis.call("SET", KEYS[1], ARGV[2])
  else
    rv = redis.call("SET", KEYS[1], ARGV[2], ARGV[3]) end
  redis.call("ZADD", KEYS[2], tonumber(ARGV[1]), KEYS[1])
  if false == oldts then return {false, false, rv} end
  return {oldval, oldts, rv}
end
"""),

    # returns (prev_value, prev_timestamp, deleted_key)
    gs_delete=dict(keys=('path', 'hist'), args=('ts', ), script="""
local oldts = redis.call("ZSCORE", KEYS[2], KEYS[1])
local oldval = redis.pcall("GET", KEYS[1])
if oldts ~= false and tonumber(oldts) > tonumber(ARGV[1]) then
  return {oldval, oldts, 0}
else
  local rv = redis.call("DEL", KEYS[1])
  redis.call("ZADD", KEYS[2], tonumber(ARGV[1]), KEYS[1])
  if false == oldts then return {false, false, rv} end
  return {oldval, oldts, rv}
end
"""),

    # returns incremented value in form (rv, timestamp)
    gs_incrby=dict(keys=('path', 'hist'), args=('ts', 'val'), script="""
local oldts = redis.call("ZSCORE", KEYS[2], KEYS[1])
local oldval = redis.pcall("GET", KEYS[1])
if oldts ~= false and tonumber(oldts) > tonumber(ARGV[1]) then
  return {oldval, oldts, 0}
else
  local rv = redis.call("INCRBY", KEYS[1], ARGV[2])
  redis.call("ZADD", KEYS[2], tonumber(ARGV[1]), KEYS[1])
  if false == oldts then return {false, false, rv} end
  return {oldval, oldts, rv}
end
"""),

    # returns gotten value or nil in form (rv, timestamp)
    gs_get=dict(keys=('path', 'hist'), args=(), script="""
return {redis.call("GET", KEYS[1]), redis.call("ZSCORE", KEYS[2], KEYS[1])}
"""),

    # returns 1 if exists 0 otherwise in form (rv, timestamp)
    gs_exists=dict(keys=('path', 'hist'), args=(), script="""
return {redis.call("EXISTS", KEYS[1]), redis.call("ZSCORE", KEYS[2], KEYS[1])}
"""),

    # returns -2, -1, or a num >=0 in form (rv, timestamp)
    gs_ttl=dict(keys=('path', 'hist'), args=(), script="""
return {redis.call("TTL", KEYS[1]), redis.call("ZSCORE", KEYS[2], KEYS[1])}
"""),
)


class GetSet(object):
    def __init__(self, mr_client):
        """
        `mr_client` - an instance of the MajorityRedis client.
        """
        self._getset_hist_key = '%s%s' % (
            mr_client._getset_history_prefix, '.majorityredis_getset_history')
        self._mr = mr_client

    def exists(self, path):
        """Return True if path exists.  False otherwise.
        Does not try to heal nodes with incorrect values."""
        return bool(self._read_value('gs_exists', path))

    def ttl(self, path):
        """Calculate the ttl at given path"""
        return self._read_value('gs_ttl', path)

    def get(self, path):
        """Return value at given path, or None if it does not exist"""
        return self._read_value('gs_get', path, heal=True)

    def set(self, path, value, retry_condition=None, nx=None, xx=None):
        """
        Set value at given path.  nx and xx are redis SET options.  We do not
        support ex and px.

        `retry_condition` (func) continually retry calling this function until
            we successfully put to >50% of servers or a max limit is reached.
            see majorityredis.util.retry_condition for details
            retry_condition=retry_condition(nretry=10, ...)

        Return True if successful
        Return False if I safely didn't set on any servers.
          Someone else must have tried to set the value after me.
        Raise exception if I set on less than majority.
          At this point, the value of the key is in unknown state.
          If other clients get my value, they will
          spread it until someone else sets a more recent value.
          To ensure consistency, you could call set(...) again.
        """
        if nx and xx:
            raise UserWarning("cannot set both NX and XX")
        if value is None:
            value = ''
        if retry_condition:
            func = retry_condition(self._set, lambda rv: rv is True,
                                   raise_on_err=False)
        else:
            func = self._set
        return func(path, value, nx=nx, xx=xx)

    def _set(self, path, value, nx, xx):
        return bool(self._modify_path(
            path, 'gs_set',
            val=value, nx_or_xx=(nx and 'NX') or (xx and 'XX') or ''))

    def delete(self, path):
        """
        Delete key identified by `path`.

        Return True if successful, False if safely didn't delete from servers.
        Raise exception if I set on less than majority.  At this point, the
        key is in an inconsistent state and should be modified.
        """
        return bool(self._modify_path(path, 'gs_delete'))

    def incrby(self, path, value=1):
        """
        Increment the value stored at given path
        Return the incremented value
        """
        return int(self._modify_path(path, 'gs_incrby', val=value))

    def _heal(self, path, responses, winner, fail_cnt):
        """Update the clients with stale values.
        Return without checking results.  Even try servers that just failed"""
        outdated_clients = (
            cli for cli, val_ts in responses if val_ts != winner)
        val, ts = winner[0], winner[1]
        if val is None:
            util.run_script(
                SCRIPTS, self._mr._map_async, 'gs_delete', outdated_clients,
                path=path, hist=self._getset_hist_key, ts=ts)
        else:
            util.run_script(
                SCRIPTS, self._mr._map_async, 'gs_set', outdated_clients,
                path=path, hist=self._getset_hist_key, val=val, ts=ts,
                nx_or_xx='')

    def _parse_responses(self, gen):
        """Evaluate result of calling a lua script on redis servers where

        `gen` generator of form (client, (return_value, timestamp))

        Return (responses, winner, fail_cnt) where
          - responses is an iterable containing (client, val_ts) pairs
          - winner is a (value, timestamp) of the most recently updated value
            across all servers.
          - fail_cnt is the number of exceptions received"""
        responses = []
        winner = (None, None)
        failed = []
        quorum = self._mr._n_servers // 2 + 1
        for client, val_ts in gen:
            if isinstance(val_ts, Exception):
                failed.append((client, val_ts))
                continue
            responses.append((client, val_ts))

            if winner[1] is None:
                winner = val_ts
            elif val_ts[1] is not None and float(val_ts[1]) > float(winner[1]):
                winner = val_ts
            # this break is optional, could lead to greater chance of
            # inconsistency if majority of servers die before key is healed.
            if len(responses) >= quorum and winner[1] is not None:
                break
        if winner[1] is None and responses:
            # no timestamps exist for this key.
            # choose most frequently occurring value, in case the history isn't
            # used for this key.
            lst = [tuple(x[1]) for x in responses]
            winner = max(set(lst), key=lst.count)
        return chain(responses, failed, gen), winner, len(failed)

    def _modify_path(self, path, script_name,
                     rv_from_winner=False, **script_params):
        """
        Modify a key on all servers.  The type of modification is determined by
        `script_name`.  Assume the scripts called by this function all
        return (prev_value, prev_timestamp, 0|1) or an exception.
        The given function, `is_consistent_given_exceptions` should specially
        handle any error messages returned by the script to determine if the
        state of the modified path is still consistent in the cluster.
        """
        ts = time.time()
        gen = util.run_script(
            SCRIPTS, self._mr._map_async, script_name, self._mr._clients,
            path=path, hist=self._getset_hist_key, ts=ts, **script_params)
        responses, winner, fail_cnt = self._parse_responses(gen)

        if fail_cnt > self._mr._n_servers // 2:
            if self._is_modify_path_consistent_given_error(responses):
                return False  # state is consistent. didn't update anything
            raise exceptions.NoMajority(
                "You should probably set a value on this key to make it"
                " consistent again")

        # by this point, we reviewed the majority of (non-failing) responses
        if winner[1] is None or float(winner[1]) < ts:
            return winner[2]  # I am the most recent player to set this value
        else:
            log.debug("Someone else set a value after my request")
            # this would happen if there are long network delays or
            # communication issues.  propagate the winner value
            self._heal(path, responses, winner, fail_cnt)
            return False

    def _is_modify_path_consistent_given_error(self, gen):
        """
        On operations that modify key paths (ie the SET or DEL operations),
        If the majority of set operations failed because something prevented
        the modification (ie nx or xx for SET. or key does not exist for DEL),
        we only maintain consistency if, on the majority of servers,
        the previous (val, ts) is the same
        """
        cnt = defaultdict(int)
        for n, (cli, val_ts) in enumerate(gen):
            if not isinstance(val_ts, Exception):
                continue
            cnt[tuple(str(val_ts).split(':')[-2:])] += 1
            if n + 1 < self._mr._n_servers // 2 + 1:
                continue
            if any(val > self._mr._n_servers // 2 for val in cnt.values()):
                return True
        return False

    def _read_value(self, script_name, path, heal=False):
        """Run script on all servers and return the value on the server
        with most recent data.

        `heal` (bool) if True, make all servers look like the most up to
            date server.  Warning: if heal=True and the return value is not
            the value of at the path, you will overwrite the key with bad data!
        """
        gen = util.run_script(
            SCRIPTS, self._mr._map_async, script_name, self._mr._clients,
            path=path, hist=self._getset_hist_key)
        responses, winner, fail_cnt = self._parse_responses(gen)

        if fail_cnt == self._mr._n_servers:
            raise exceptions.NoMajority(
                "Got errors from all redis servers")
        if heal:
            self._heal(path, responses, winner, fail_cnt)
        if fail_cnt >= self._mr._n_servers // 2 + 1:
            raise exceptions.NoMajority(
                "Got errors from majority of redis servers")
        return winner[0]
