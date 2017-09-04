local queue = ARGV[1]
local notifications = ARGV[2]
local job_json = ARGV[3]

redis.call('rpush', queue, job_json)
redis.call('publish', notifications, '')

