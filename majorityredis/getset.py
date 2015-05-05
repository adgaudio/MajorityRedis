import time
from itertools import chain

from . import exceptions
from . import util
from . import log


SCRIPTS = dict(

    # returns (value, timestamp) or (nil, nil)
    gs_get=dict(keys=('path', 'hist'), args=(), script="""
return {redis.call("GET", KEYS[1]), redis.call("ZSCORE", KEYS[2], KEYS[1])}
"""),

    # returns (prev_value, prev_timestamp) and set value if ts is new enough
    gs_set=dict(keys=('path', 'hist'), args=('val', 'ts'), script="""
local oldts = redis.call("ZSCORE", KEYS[2], KEYS[1])
local oldval = redis.call("GET", KEYS[1])
if oldts ~= false and tonumber(oldts) > tonumber(ARGV[2]) then
  return {oldval, oldts}
else
  redis.call("SET", KEYS[1], ARGV[1])
  redis.call("ZADD", KEYS[2], tonumber(ARGV[2]), KEYS[1])
  if false == oldts then return {false, false} end
  return {oldval, oldts}
end
"""),

    # returns 1 if exists 0 otherwise.
    gs_exists=dict(keys=('path', 'hist'), args=(), script="""
return {redis.call("EXISTS", KEYS[1]), redis.call("ZSCORE", KEYS[2], KEYS[1])}
"""),

    gs_ttl=dict(keys=('path', 'hist'), args=(), script="""
return {redis.call("TTL", KEYS[1]), redis.call("ZSCORE", KEYS[2], KEYS[1])}
"""),
)


class GetSet(object):
    _getset_hist_key = '.majorityredis_getset'

    def __init__(self, mr_client):
        """
        `mr_client` - an instance of the MajorityRedis client.
        """
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

    def set(self, path, value, retry_condition=None,
            ex=None, px=None, nx=None, xx=None):
        """
        Set value at given path.  ex, px, nx and xx are redis SET options.

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
        # TODO
        if ex or px or nx or xx:
            raise NotImplemented("TODO")

        if retry_condition:
            func = retry_condition(self._set, lambda rv: rv is True,
                                   raise_on_err=False)
        else:
            func = self._set
        return func(path, value, ex=ex, px=px, nx=nx, xx=xx)

    def _set(self, path, value, ex, px, nx, xx):
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
        responses, winner, fail_cnt = self._parse_responses(gen)

        if fail_cnt == self._mr._n_servers:
            return False
        elif fail_cnt >= self._mr._n_servers // 2 + 1:
            raise exceptions.NoMajority(
                "You should probably set a value on this key to make it"
                " consistent again")

        # by this point, we reviewed the majority of (non-failing) responses
        if winner[1] is None or float(winner[1]) < ts:
            return True  # I am the most recent player to set this value
        else:
            log.debug("Someone else set a value after my request")
            # this would happen if there are long network delays or
            # communication issues.
            # did I ever set to majority?  does it even matter?  let's just
            # propagate the winner value in case something happened.
            self._heal(path, responses, winner, fail_cnt)
            return False

    def _heal(self, path, responses, winner, fail_cnt):
        """Update the clients with stale values.
        Return without checking results"""
        outdated_clients = (
            cli for cli, val_ts in responses if val_ts != winner)
        val, ts = winner
        util.run_script(  # run asynchronously.
            SCRIPTS, self._mr._map_async, 'gs_set', outdated_clients,
            path=path, hist=self._getset_hist_key, val=val, ts=ts)

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
            if len(responses) >= quorum:
                break
        return chain(responses, failed, gen), winner, len(failed)

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
