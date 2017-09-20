local namespace = ARGV[1]
local future_jobs = ARGV[2]
local notifications = ARGV[3]
local now = ARGV[4]
local job_status_queued = ARGV[5]

local jobs_json = redis.call('zrangebyscore', future_jobs, '-inf', now, 'LIMIT', 0, 1)
local jobs_moved = 0

if not jobs_json then
    return jobs_moved
end

for i, job_json in ipairs(jobs_json) do
    local job = cjson.decode(job_json)
    local queue = string.format("%s/%s", namespace, job["queue"])
    job["status"] = job_status_queued
    redis.call('rpush', queue, job_json)
    redis.call('zrem', future_jobs, job_json)
    jobs_moved = jobs_moved + 1
end

if jobs_moved > 0 then
    redis.call('publish', notifications, '')
end

return jobs_moved

