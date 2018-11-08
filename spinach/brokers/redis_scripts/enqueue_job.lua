-- idempotency protected script, do not remove comment
local idempotency_token = ARGV[1]
local broker_id = ARGV[2]
local notifications = ARGV[3]
local running_jobs_key = ARGV[4]
local namespace = ARGV[5]
-- jobs starting at ARGV[6]

if not redis.call('set', idempotency_token, 'true', 'EX', 3600, 'NX') then
    redis.log(redis.LOG_WARNING, "Not reprocessing script")
    return -1
end

for i=6, #ARGV do
    local job_json = ARGV[i]
    local job = cjson.decode(job_json)
    local queue = string.format("%s/%s", namespace, job["queue"])
    redis.call('rpush', queue, job_json)
    redis.call('hdel', running_jobs_key, job["id"])
end

redis.call('publish', notifications, '')
