local broker_id = ARGV[1]
local now = ARGV[2]
local periodic_tasks_hash = ARGV[3]
local periodic_tasks_queue = ARGV[4]
-- tasks to register starting at ARGV[5]


local function contains(t, e)
  return t[e]
end


local old_task_names = redis.call('hkeys', periodic_tasks_hash)
local new_task_names = {}

for i=5, #ARGV do
    local task_json = ARGV[i]
    local task = cjson.decode(task_json)
    local next_event_time = now + task["periodicity"]
    new_task_names[task["name"]] = true

    if redis.call('hexists', periodic_tasks_hash, task["name"]) == 0 then
        redis.call('zadd', periodic_tasks_queue, next_event_time, task["name"])
    end
    redis.call('hset', periodic_tasks_hash, task["name"], task_json)
end


for i=1, #old_task_names do
    local old_task_name = old_task_names[i]
    if not contains(new_task_names, old_task_name) then
        redis.call('hdel', periodic_tasks_hash, old_task_name)
    end
end

