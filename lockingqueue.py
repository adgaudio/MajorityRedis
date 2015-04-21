# In progress.
# Distributed Locking Queue for Redis adapted from the Redlock algorithm.

# TODO: consider 2 lists instead of sorted set if priority doesn't matter
# assuming that the keys cannot timeout in the middle of an eval

# f = findable set
# expire_time = seconds_since_epoch + 5 seconds + whatever redlock figured out
# client_id = owner of the lock, if we can obtain it.
get_script = """
h_k, v = ZRANGE KEYS[1] 0 0 withscores
if h_k is empty: return

got_lock = SETNX h_k ARGV[2]
if not got_lock:
    return  # (because v must be nonzero)

EXPIREAT h_k ARGV[1]

ZADD KEYS[1] 1 h_k
return h_k
"""
# eval get_script 1 f expire_time client_id


# f = findable set
# h_k = time-ordered hash of key in form   priority:insert_time_since_epoch:key
# expire_time = seconds_since_epoch + 5 seconds + whatever redlock figured out
# random_seed = a random integer
# client_id = owner of the lock, if we can obtain it.
lock_script = """
SETNX KEYS[1] ARGV[3]
if not got_lock:
    if GET KEYS[1] == "completed":
        ZREM KEYS[2] KEYS[1]
        return "completed"
    else:
        score = ZSCORE KEYS[2] KEYS[1]
        math.randomseed(tonumber(ARGV[2]))
        num = math.random(math.floor(score) + 1)
        if (num != 1)
            ZINCRBY KEYS[1]  (num-1)/score  h_k
        return "already_locked"
else:
    EXPIREAT KEYS[1] ARGV[1]
    ZINCRBY KEYS[2] 1 KEYS[1]
    return "locked"
"""
# eval lock_script 2 h_k f expire_time random_seed client_id


# f = findable set
# h_k = time-ordered hash of key in form   priority:insert_time_since_epoch:key
release_script = """
SET KEYS[1] "completed"
PERSIST KEYS[1]  # or perhaps EXPIRE some very large amount of time...
ZREM KEYS[2] KEYS[1]
"""
# eval lock_script 2 h_k f


# f = findable set
# h_k = time-ordered hash of key in form   priority:insert_time_since_epoch:key
put_script = """
ZADD KEYS[1] 0 KEYS[2]
"""
# eval put_script 2 f h_k


# h_k = time-ordered hash of key in form   priority:insert_time_since_epoch:key
# expire_time = seconds_since_epoch + 5 seconds + whatever redlock figured out
# client_id = owner of the lock, if we can obtain it.
extend_lock = """
if GET KEYS[1] == ARGV[2]
    EXPIREAT KEYS[1] ARGV[1]
else:
    return "lost the lock!"
"""
# eval extend_lock 1 h_k expire_time client_id
