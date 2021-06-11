local queue = ARGV[1]

local queue_json = redis.call('lrange', queue, 0, -1)
return queue_json
