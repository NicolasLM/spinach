local broker_id = ARGV[1]
local notifications = ARGV[2]
local running_jobs_key = ARGV[3]
local future_jobs = ARGV[4]
-- jobs starting at ARGV[5]


for i=5, #ARGV do
    local job_json = ARGV[i]
    local job = cjson.decode(job_json)
    local at_timestamp = job["at"] + 1  -- approximation to avoid starting a job before its real "at" date
    redis.call('zadd', future_jobs, at_timestamp, job_json)
    redis.call('hdel', running_jobs_key, job["id"])
end

redis.call('publish', notifications, '')
