local dead_broker_id = ARGV[1]
local running_jobs_key = ARGV[2]
local all_brokers_hash_key = ARGV[3]
local all_brokers_zset_key = ARGV[4]
local namespace = ARGV[5]
local notifications = ARGV[6]
local max_concurrency_key = ARGV[7]
local current_concurrency_key = ARGV[8]

local num_enqueued_jobs = 0
local i = 1
local failed_jobs = {}

-- Get all jobs that were running on the broker before it died
local jobs_json = redis.call('hvals', running_jobs_key)

for _, job_json in ipairs(jobs_json) do
    local job = cjson.decode(job_json)
    if job["retries"] < job["max_retries"] then
        job["retries"] = job["retries"] + 1
        -- Set job status to queued:
        -- A major difference between retrying a job failing in a worker and
        -- a failing from a dead broker is that the dead broker one is
        -- automatically put in the queue, there is not waiting state because
        -- the backoff is not taken into account. Most likely the broker was
        -- dead for a while before it was noticed, this acts as the backoff.
        job["status"] = 2

        -- Serialize the job so that it can be put in the queue
        local job_json = cjson.encode(job)

        -- Decrement the current concurrency if we are tracking
        -- concurrency on the Task.
        local max_concurrency = tonumber(redis.call('hget', max_concurrency_key, job['task_name']))
        if max_concurrency ~= nil and max_concurrency ~= -1 then
            redis.call('hincrby', current_concurrency_key, job['task_name'], -1)
        end

        -- Enqueue the job
        local queue = string.format("%s/%s", namespace, job["queue"])
        redis.call('rpush', queue, job_json)
        num_enqueued_jobs = num_enqueued_jobs + 1
    else
        -- Keep track of jobs that exceeded the max_retries
        failed_jobs[i] = job_json
        i = i + 1
    end

    redis.call('hdel', running_jobs_key, job["id"])
end

-- Remove the broker from the list of brokers
redis.call('hdel', all_brokers_hash_key, dead_broker_id)
redis.call('zrem', all_brokers_zset_key, dead_broker_id)

if num_enqueued_jobs > 0 then
    redis.call('publish', notifications, '')
end

return {num_enqueued_jobs, failed_jobs}
