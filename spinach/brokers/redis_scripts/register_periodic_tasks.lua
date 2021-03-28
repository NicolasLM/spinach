local now = ARGV[1]
local periodic_tasks_hash = ARGV[2]
local periodic_tasks_queue = ARGV[3]
-- tasks to register starting at ARGV[4]


local function contains(t, e)
  return t[e]
end


local old_task_names = redis.call('hkeys', periodic_tasks_hash)
local new_task_names = {}

for i=4, #ARGV do
    local task_json = ARGV[i]
    local task = cjson.decode(task_json)
    local next_event_time = now + task["periodicity"]
    new_task_names[task["name"]] = true

    if redis.call('hexists', periodic_tasks_hash, task["name"]) == 0 then
        -- the periodic task is new, add it to the queue
        redis.call('zadd', periodic_tasks_queue, next_event_time, task["name"])
    else
        local existing_task_json = redis.call('hget', periodic_tasks_hash, task["name"])
        local existing_task = cjson.decode(existing_task_json)
        if existing_task["periodicity"] ~= task["periodicity"] then
            -- the periodic task already existed but the periodicity changed
            -- so it is reset
            redis.call('zadd', periodic_tasks_queue, next_event_time, task["name"])
        end
    end

    -- unconditionnally override the task in the hash
    redis.call('hset', periodic_tasks_hash, task["name"], task_json)
end


for i=1, #old_task_names do
    local old_task_name = old_task_names[i]
    if not contains(new_task_names, old_task_name) then
        redis.call('hdel', periodic_tasks_hash, old_task_name)
        redis.call('zrem', periodic_tasks_queue, old_task_name)
    end
end

