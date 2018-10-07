local broker_id = ARGV[1]
local namespace = ARGV[2]
local future_jobs = ARGV[3]
local notifications = ARGV[4]
local now = ARGV[5]
local job_status_queued = tonumber(ARGV[6])
local periodic_tasks_hash = ARGV[7]
local periodic_tasks_queue = ARGV[8]
-- uuids starting at ARGV[9]
-- lua in Redis cannot generate random UUIDs, so they are generated in Python
-- and passed with each calls

-- Get the future jobs that are due
-- Limit to fetching 1000 of them to avoid the script to take too long
local jobs_json = redis.call('zrangebyscore', future_jobs, '-inf', now, 'LIMIT', 0, 1000)
local jobs_moved = 0

-- Create jobs from due periodic tasks
local number_of_uuids = #ARGV + 1 - 9  -- as uuids start at ARGV[9]
local task_names = redis.call('zrangebyscore', periodic_tasks_queue, '-inf', now, 'LIMIT', 0, number_of_uuids)
for i, task_name in ipairs(task_names) do

    local task_json = redis.call('hget', periodic_tasks_hash, task_name)

    -- the key task_name may not exist in the hash if the periodic task was deleted
    if task_json == false then
        redis.call('zrem', periodic_tasks_queue, task_name)
    else
        local task = cjson.decode(task_json)
        local job = {}
        job["id"] = ARGV[9 + i - 1]
        job["status"] = job_status_queued
        job["task_name"] = task_name
        job["queue"] = task["queue"]
        job["max_retries"] = task["max_retries"]
        job["retries"] = 0
        job["at"] = tonumber(now)
        job["at_us"] = 0
        job["task_args"] = {}
        job["task_kwargs"] = {}
        table.insert(jobs_json, cjson.encode(job))

        local next_event_time = job["at"] + task["periodicity"]
        redis.call('zrem', periodic_tasks_queue, task_name)
        redis.call('zadd', periodic_tasks_queue, next_event_time, task_name)
    end
end

if not jobs_json then
    return jobs_moved
end

for i, job_json in ipairs(jobs_json) do
    local job = cjson.decode(job_json)
    local queue = string.format("%s/%s", namespace, job["queue"])
    job["status"] = job_status_queued
    local job_json_updated = cjson.encode(job)
    redis.call('rpush', queue, job_json_updated)
    redis.call('zrem', future_jobs, job_json)
    jobs_moved = jobs_moved + 1
end

if jobs_moved > 0 then
    redis.call('publish', notifications, '')
end

return jobs_moved

