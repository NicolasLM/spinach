local broker_id = ARGV[1]
local queue = ARGV[2]
local running_jobs_key = ARGV[3]
local job_status_running = tonumber(ARGV[4])

local job_json = redis.call('lpop', queue)
if not job_json then
    return nil
end

local job = cjson.decode(job_json)
job["status"] = job_status_running
local job_json = cjson.encode(job)

if job["max_retries"] == 0 then
    -- job is not idempotent, there is no use to track
    -- if it's running
    return job_json
end

redis.call('hset', running_jobs_key, job["id"], job_json)
return job_json
