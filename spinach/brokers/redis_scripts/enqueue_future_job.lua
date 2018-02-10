local broker_id = ARGV[1]
local future_jobs = ARGV[2]
local notifications = ARGV[3]
local at_timestamp = ARGV[4]
local job_json = ARGV[5]
local job_id = ARGV[6]
local running_jobs_key = ARGV[7]

redis.call('zadd', future_jobs, at_timestamp, job_json)
redis.call('hdel', running_jobs_key, job_id)
redis.call('publish', notifications, '')

