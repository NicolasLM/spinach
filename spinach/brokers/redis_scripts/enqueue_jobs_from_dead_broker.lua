local broker_id = ARGV[1]
local running_jobs_key = ARGV[2]
local namespace = ARGV[3]
local notifications = ARGV[4]

local num_enqueued_jobs = 0

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

        -- Enqueue the job
        local queue = string.format("%s/%s", namespace, job["queue"])
        redis.call('rpush', queue, job_json)
        num_enqueued_jobs = num_enqueued_jobs + 1
    end

    redis.call('hdel', running_jobs_key, job["id"])
end

if num_enqueued_jobs > 0 then
    redis.call('publish', notifications, '')
end

return num_enqueued_jobs
