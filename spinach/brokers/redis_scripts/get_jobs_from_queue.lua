local queue = ARGV[1]
local running_jobs_key = ARGV[2]
local job_status_running = tonumber(ARGV[3])
local max_jobs = tonumber(ARGV[4])

local jobs = {}

for i=1, max_jobs do

    local job_json = redis.call('lpop', queue)
    if not job_json then
        return cjson.encode(jobs)
    end

    local job = cjson.decode(job_json)
    job["status"] = job_status_running
    local job_json = cjson.encode(job)

    if job["max_retries"] > 0 then
        -- job is idempotent, must track if it's running
        redis.call('hset', running_jobs_key, job["id"], job_json)
    end

    jobs[i] = job_json

end

return cjson.encode(jobs)
