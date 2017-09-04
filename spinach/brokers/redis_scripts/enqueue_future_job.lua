local future_jobs = ARGV[1]
local notifications = ARGV[2]
local at_timestamp = ARGV[3]
local job_json = ARGV[4]

redis.call('zadd', future_jobs, at_timestamp, job_json)
redis.call('publish', notifications, '')

