from collections import defaultdict
import functools
import random
import redis
import sys
import time

from . import log
from . import exceptions


# A local cache
# Sha1 hash of each lua script.  Figured out at run time, and then cached here.
# { script_name: {client: sha, client2: sha, ...}, ...}
SHAS = defaultdict(dict)

BACKGROUND_TASKS = {}


def continually_extend_lock_in_background(
        h_k, extend_lock, polling_interval, run_async, callback, client_id):
    """
    Extend the lock on given key, `h_k` every `polling_interval` seconds

    Once called, respawns itself indefinitely until extend_lock is unsuccessful
    """
    if (h_k, client_id) in BACKGROUND_TASKS:
        log.debug("Already extending this lock in background.", extra=dict(
            h_k=h_k, task=extend_lock))
        return
    log.info("Spinning up background task", extra=dict(
        target_func=str(_continually_extend_lock_in_background), h_k=h_k))
    BACKGROUND_TASKS[(h_k, client_id)] = callback
    run_async(_continually_extend_lock_in_background,
              h_k, extend_lock, polling_interval, client_id)


def _continually_extend_lock_in_background(h_k, extend_lock, polling_interval,
                                           client_id):
    while True:
        secs_left = extend_lock(h_k)
        if (h_k, client_id) not in BACKGROUND_TASKS:
            log.debug(
                "No longer extending lock.", extra=dict(h_k=h_k))
        elif secs_left == -1:
            log.debug(
                "Found that item was marked as completed."
                " No longer extending lock", extra=dict(h_k=h_k))
        elif not secs_left:
            log.error((
                "Failed to extend the lock.  You should completely stop"
                " processing this item."), extra=dict(h_k=h_k))
        elif secs_left:
            assert secs_left > 0, "Code bug: secs_left cannot be negative"
            time.sleep(
                min(max(secs_left - polling_interval, 0), polling_interval))
            continue
        remove_background_thread(h_k, client_id)
        return


def remove_background_thread(h_k, client_id):
    try:
        callback = BACKGROUND_TASKS.pop((h_k, client_id))
    except KeyError:
        return
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
        rv = client.evalsha(sha, len(keys), *(keys + args))
        if isinstance(rv, list):
            rv = tuple(rv)
        return (client, rv)
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
                redis_client=client, script_name=script_name,
                script_keys=keys, script_args=args))
        return (client, err)


def run_script(scripts, map_async, script_name, clients, **kwargs):
    keys = [kwargs[x] for x in scripts[script_name]['keys']]
    args = [kwargs[x] if x != 'randint' else random.randint(1, sys.maxsize)
            for x in scripts[script_name]['args']]
    return map_async(
        lambda client: _run_script(scripts, script_name, client, keys, args),
        clients)


def retry_condition(
        nretry=5, backoff=lambda x: x + 1, condition=None, timeout=None):
    """
    A decorator that will call a wrapped function up to `nretry` times
    until the `condition` is met.

    `nretry` (int) max number of times to run decorated func
    `backoff` (func) a function that defines how much delay between retries.
        The function receives the previous delay as input. Initially, delay=0.
    `condition` (func, optional) a function that examines the return value
        of given function and returns True if the returned value is ok, False
        if we should retry.
    `timeout` (int) if given, defines max number of seconds we are willing to
        wait, regardless of number of retries we've set.
    """
    def _retry_until(f, condition2=None, raise_on_err=True):
        # the first defined condition overrides the second one.
        condition_func = condition or condition2
        if condition_func is None:
            raise UserWarning(
                "Must pass `condition` if using retry_condition as decorator")

        @functools.wraps(f)
        def _retry_until2(*args, **kwargs):
            t_start = time.time()
            n = 0
            delay = 0
            while n < nretry:
                n += 1
                try:
                    rv = f(*args, **kwargs)
                except Exception as err:
                    if raise_on_err:
                        raise
                    log.warn("Failed to run func. Retrying", extra=dict(
                        func=f, err=err))
                    rv = err
                if condition_func(rv):
                    return rv
                delay = backoff(delay)
                if timeout and (time.time() + delay > t_start + timeout):
                    raise exceptions.Timeout(f)
                time.sleep(delay)
            raise exceptions.TooManyRetries(f)
        return _retry_until2
    return _retry_until
