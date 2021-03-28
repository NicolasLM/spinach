local namespace = ARGV[1]
local future_jobs = ARGV[2]
local notifications = ARGV[3]
local now = ARGV[4]
local job_status_queued = tonumber(ARGV[5])
local periodic_tasks_hash = ARGV[6]
local periodic_tasks_queue = ARGV[7]
local all_brokers_hash_key = ARGV[8]
local all_brokers_zset_key = ARGV[9]
local broker_info_json = ARGV[10]
local broker_dead_threshold_seconds = ARGV[11]
-- uuids starting at ARGV[12]
-- lua in Redis cannot generate random UUIDs, so they are generated in Python
-- and passed with each calls

-- Register the current broker keepalive
local broker_info = cjson.decode(broker_info_json)
redis.call('hset', all_brokers_hash_key, broker_info["id"], broker_info_json)
redis.call('zadd', all_brokers_zset_key, broker_info["last_seen_at"], broker_info["id"])

-- Get IDs of brokers that were not seen for a long time
local dead_brokers_id = redis.call('zrangebyscore', all_brokers_zset_key, '-inf', now - broker_dead_threshold_seconds, 'LIMIT', 0, 10)

-- Get the future jobs that are due
-- Limit to fetching 1000 of them to avoid the script to take too long
local jobs_json = redis.call('zrangebyscore', future_jobs, '-inf', now, 'LIMIT', 0, 1000)
local jobs_moved = 0

-- Create jobs from due periodic tasks
local number_of_uuids = #ARGV + 1 - 12  -- as uuids start at ARGV[12]
local task_names = redis.call('zrangebyscore', periodic_tasks_queue, '-inf', now, 'LIMIT', 0, number_of_uuids)
for i, task_name in ipairs(task_names) do

    local task_json = redis.call('hget', periodic_tasks_hash, task_name)

    -- the key task_name may not exist in the hash if the periodic task was deleted
    if task_json == false then
        redis.call('zrem', periodic_tasks_queue, task_name)
    else
        local task = cjson.decode(task_json)
        local job = {}
        job["id"] = ARGV[12 + i - 1]
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

return {jobs_moved, dead_brokers_id}

