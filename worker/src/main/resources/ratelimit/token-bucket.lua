-- Token bucket with reservations, run atomically by Redis.
--
-- KEYS[1]  bucket key, one per user and channel
-- ARGV[1]  capacity: the largest burst
-- ARGV[2]  milliseconds to refill one token
-- ARGV[3]  longest wait, in milliseconds, a caller may reserve
--
-- Returns {1, 0} to send now, {2, wait_ms} for a slot reserved wait_ms from now, or
-- {0, wait_ms} when the next slot is further away than the longest wait (nothing is taken).
--
-- Tokens may go negative: each caller that finds the bucket empty reserves the next free
-- slot, so a burst is spread out at the refill rate instead of everyone retrying at once.
-- The time comes from Redis, so workers with skewed clocks share one timeline.

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_ms = tonumber(ARGV[2])
local max_wait_ms = tonumber(ARGV[3])

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
    tokens = capacity
    ts = now
end
tokens = math.min(capacity, tokens + math.max(0, now - ts) / refill_ms)

local wait = 0
if tokens < 1 then
    wait = math.ceil((1 - tokens) * refill_ms)
    if wait > max_wait_ms then
        return {0, wait}
    end
end

tokens = tokens - 1
redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', now)
-- Once the bucket would be full again it holds no information, so let it expire.
redis.call('PEXPIRE', key, math.ceil((capacity - tokens) * refill_ms) + 1000)

if wait == 0 then
    return {1, 0}
end
return {2, wait}
