local running_jobs_key = ARGV[1]
local max_concurrency_key = ARGV[2]
local current_concurrency_key = ARGV[3]
local job_json = ARGV[4]

local job = cjson.decode(job_json)

-- Remove the job from the list of running jobs.
redis.call('hdel', running_jobs_key, job['id'])

-- Decrement current concurrency if max concurrency set on the Task.
local max_concurrency = tonumber(redis.call('hget', max_concurrency_key, job['task_name']))
if max_concurrency ~= nil and max_concurrency ~= -1 then
    redis.call('hincrby', current_concurrency_key, job['task_name'], -1)
end
