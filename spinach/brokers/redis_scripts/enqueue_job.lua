local broker_id = ARGV[1]
local queue = ARGV[2]
local notifications = ARGV[3]
local job_json = ARGV[4]
local job_id = ARGV[5]
local running_jobs_key = ARGV[6]

redis.call('rpush', queue, job_json)
redis.call('hdel', running_jobs_key, job_id)
redis.call('publish', notifications, '')

