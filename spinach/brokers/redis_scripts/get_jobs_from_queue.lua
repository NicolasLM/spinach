local queue = ARGV[1]
local running_jobs_key = ARGV[2]
local job_status_running = tonumber(ARGV[3])
local max_jobs = tonumber(ARGV[4])
local max_concurrency_key = ARGV[5]
local current_concurrency_key = ARGV[6]

local jobs = {}
local jobs_to_re_add = {}
local num_jobs_to_re_add = 0

local i = 1
repeat

    local job_json = redis.call('lpop', queue)
    if not job_json then
        break
    end

    local job = cjson.decode(job_json)
    local max_concurrency = tonumber(redis.call('hget', max_concurrency_key, job['task_name']))
    local current_concurrency = tonumber(redis.call('hget', current_concurrency_key, job['task_name']))

    if max_concurrency ~= nil and max_concurrency ~= -1 and current_concurrency >= max_concurrency then
        -- The max concurrency limit was reach on this Task, Re-add the
        -- job(s) to the front of the queue after the loop finishes.
        num_jobs_to_re_add = num_jobs_to_re_add + 1
        jobs_to_re_add[num_jobs_to_re_add] = job_json
    else
        job["status"] = job_status_running
        local job_json = cjson.encode(job)

        if job["max_retries"] > 0 then
            -- job is idempotent, must track if it's running
            redis.call('hset', running_jobs_key, job["id"], job_json)
            -- If tracking concurrency, bump the current value.
            if max_concurrency ~= -1 then
                redis.call('hincrby', current_concurrency_key, job['task_name'], 1)
            end
        end

        jobs[i] = job_json
        i = i + 1
    end

until i > max_jobs

-- Re-add any jobs that were popped but could not be run due to
-- max_concurrency limits. Loop in reverse order to keep the same
-- original ordering!
for i = #jobs_to_re_add, 1, -1 do
    redis.call('lpush', queue, jobs_to_re_add[i])
end

return cjson.encode(jobs)
