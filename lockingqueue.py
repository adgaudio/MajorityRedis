# In progress.
# Distributed Locking Queue for Redis adapted from the Redlock algorithm.

# TODO: consider 2 lists instead of sorted set if priority doesn't matter
# assuming that the keys cannot timeout in the middle of an eval

# f = findable set
# expire_time = seconds_since_epoch + 5 seconds + whatever redlock figured out
# client_id = owner of the lock, if we can obtain it.
# returns 1 if got an item, and returns an error otherwise
get_script = """
local h_k = redis.call("ZRANGE", KEYS[1], 0, 0)[1]
if nil == h_k then return {err="queue empty"} end
if 1 ~= redis.call("SETNX", h_k, ARGV[1]) then
  return {err="already locked"} end
if 1 ~= redis.call("EXPIREAT", h_k, ARGV[2]) then
  return {err="invalid expireat"} end
redis.call("ZINCRBY", KEYS[1], 1, h_k)
return h_k
"""
# eval get_script 1 f client_id expire_time


# f = findable set
# h_k = time-ordered hash of key in form   priority:insert_time_since_epoch:key
# expire_time = seconds_since_epoch + 5 seconds + whatever redlock figured out
# random_seed = a random integer that changes every time script is called
# client_id = owner of the lock, if we can obtain it.
# returns 1 if got lock. Returns an error otherwise
lock_script = """
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
"""
# eval lock_script 2 h_k f expire_time random_seed client_id
# x=`echo $x + 1|bc`; redis-cli --eval ./hello.lua h_k1 f , $(echo `date +%s` + 5 | bc) $x bashclient


# f = findable set
# h_k = time-ordered hash of key in form   priority:insert_time_since_epoch:key
# return 1 if removed, 0 if key was already removed. return val doesn't matter
release_script = """
redis.call("SET", KEYS[1], "completed")
redis.call("PERSIST", KEYS[1])  -- or EXPIRE far into the future...
return(redis.call("ZREM", KEYS[2], KEYS[1]))
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
# return 1 if extended lock, 0 if failed to extend lock.
# TODO: test this out
extend_lock = """
if ARGV[2] == redis.call("GET", KEYS[1]) then
    redis.call("EXPIREAT", KEYS[1], ARGV[1])
    return 1
else
    return 0
end
"""
# eval extend_lock 1 h_k expire_time client_id
