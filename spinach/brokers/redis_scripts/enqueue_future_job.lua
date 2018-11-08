-- idempotency protected script, do not remove comment
local idempotency_token = ARGV[1]
local broker_id = ARGV[2]
local notifications = ARGV[3]
local running_jobs_key = ARGV[4]
local future_jobs = ARGV[5]
-- jobs starting at ARGV[6]

if not redis.call('set', idempotency_token, 'true', 'EX', 3600, 'NX') then
    redis.log(redis.LOG_WARNING, "Not reprocessing script")
    return -1
end

for i=6, #ARGV do
    local job_json = ARGV[i]
    local job = cjson.decode(job_json)
    local at_timestamp = job["at"] + 1  -- approximation to avoid starting a job before its real "at" date
    redis.call('zadd', future_jobs, at_timestamp, job_json)
    redis.call('hdel', running_jobs_key, job["id"])
end

redis.call('publish', notifications, '')
