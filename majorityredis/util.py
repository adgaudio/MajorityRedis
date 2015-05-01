from collections import defaultdict
import random
import redis
import sys
import time

from . import log


# A local cache
# Sha1 hash of each lua script.  Figured out at run time, and then cached here.
# { script_name: {client: sha, client2: sha, ...}, ...}
SHAS = defaultdict(dict)


def continually_extend_lock_in_background(
        h_k, extend_lock, polling_interval, Timer, callback):
    """
    Extend the lock on given key, `h_k` every `polling_interval` seconds

    Once called, respawns itself indefinitely until extend_lock is unsuccessful
    """
    secs_left = extend_lock(h_k)
    if secs_left == -1:
        log.debug(
            "Found that item was marked as completed. No longer extending lock",
            extra=dict(h_k=h_k))
        return
    elif secs_left:
        assert secs_left > 0, "Code bug: secs_left cannot be negative"
        t = Timer(
            min(max(secs_left - polling_interval, 0), polling_interval),
            continually_extend_lock_in_background,
            args=(h_k, extend_lock, polling_interval, Timer, callback))
        t.daemon = True
        t.start()
    else:
        log.error((
            "Failed to extend the lock.  You should completely stop"
            " processing this item."), extra=dict(item=h_k))
        if callable(callback):
            callback(h_k)


def lock_still_valid(t_expireat, clock_drift, polling_interval):
    if t_expireat < 0:
        return False
    secs_left = \
        t_expireat - time.time() - clock_drift - polling_interval
    if secs_left < 0:
        return False
    return secs_left


def get_expireat(timeout):
    t = time.time()
    return t, int(t + timeout)


def _get_sha(scripts, script_name, client):
    try:
        rv = SHAS[script_name][client]
    except KeyError:
        try:
            rv = SHAS[script_name][client] = \
                client.script_load(scripts[script_name]['script'])
        except redis.RedisError as err:
            # this is pretty bad, but not a total blocker.
            rv = err
            log.debug(
                "Could not load script on redis server: %s" % err, extra=dict(
                    error=err, error_type=type(err).__name__,
                    redis_client=client))
    return rv


def _run_script(scripts, script_name, client, keys, args):
    sha = _get_sha(scripts, script_name, client)
    if isinstance(sha, Exception):
        return (client, sha)

    try:
        return (client, client.evalsha(sha, len(keys), *(keys + args)))
    except redis.exceptions.NoScriptError:
        log.warn("server must have died since I've been running", extra=dict(
            redis_client=client, script_name=script_name))
        del SHAS[script_name][client]
        return _run_script(scripts, script_name, client, keys, args)
    except redis.exceptions.RedisError as err:
        log.debug(
            "Redis Error running script %s" % script_name,
            extra=dict(
                error=err, error_type=type(err).__name__,
                redis_client=client, script_name=script_name))
        return (client, err)


def run_script(scripts, map_async, script_name, clients, **kwargs):
    keys = [kwargs[x] for x in scripts[script_name]['keys']]
    args = [kwargs[x] if x != 'randint' else random.randint(0, sys.maxsize)
            for x in scripts[script_name]['args']]
    return map_async(
        lambda client: _run_script(scripts, script_name, client, keys, args),
        clients)
