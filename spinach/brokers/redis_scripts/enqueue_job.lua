local broker_id = ARGV[1]
local notifications = ARGV[2]
local running_jobs_key = ARGV[3]
local namespace = ARGV[4]
-- jobs starting at ARGV[5]

for i=5, #ARGV do
    local job_json = ARGV[i]
    local job = cjson.decode(job_json)
    local queue = string.format("%s/%s", namespace, job["queue"])
    redis.call('rpush', queue, job_json)
    redis.call('hdel', running_jobs_key, job["id"])
end

redis.call('publish', notifications, '')
