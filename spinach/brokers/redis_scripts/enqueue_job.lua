-- idempotency protected script, do not remove comment
local idempotency_token = ARGV[1]
local notifications = ARGV[2]
local running_jobs_key = ARGV[3]
local namespace = ARGV[4]
local future_jobs = ARGV[5]
local max_concurrency_key = ARGV[6]
local current_concurrency_key = ARGV[7]
local from_failure = ARGV[8]

-- jobs starting at ARGV[9]

if not redis.call('set', idempotency_token, 'true', 'EX', 3600, 'NX') then
    redis.log(redis.LOG_WARNING, "Not reprocessing script")
    return -1
end

for i=9, #ARGV do
    local job_json = ARGV[i]
    local job = cjson.decode(job_json)
    if tonumber(from_failure) == 1 then
        -- job is being requeued after a failure, decrement its concurrency
        local max_concurrency = tonumber(redis.call('hget', max_concurrency_key, job['task_name']))
        if max_concurrency ~= nil and max_concurrency ~= -1 then
            redis.call('hincrby', current_concurrency_key, job['task_name'], -1)
        end
    end
    if job["status"] == 2 then
        -- job status is queued
        local queue = string.format("%s/%s", namespace, job["queue"])
        redis.call('rpush', queue, job_json)
    else
        -- job status is waiting
        local at_timestamp = job["at"] + 1  -- approximation to avoid starting a job before its real "at" date
        redis.call('zadd', future_jobs, at_timestamp, job_json)
    end
    redis.call('hdel', running_jobs_key, job["id"])
end

redis.call('publish', notifications, '')
