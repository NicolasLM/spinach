local max_concurrency_key = ARGV[1]
local current_concurrency_key = ARGV[2]
-- tasks to register starting at ARGV[3]


local function contains(t, e)
  return t[e]
end

local old_max_values = redis.call('hkeys', max_concurrency_key)
local old_current_values = redis.call('hkeys', current_concurrency_key)

local new_task_names = {}

for i=3, #ARGV do
    local task_json = ARGV[i]
    local task = cjson.decode(task_json)
    local max_concurrency = tonumber(task["max_concurrency"])
    if max_concurrency ~= -1 then
        new_task_names[task["name"]] = true

        -- Override max_concurrency whatever it is already set to, if
        -- anything.
        redis.call('hset', max_concurrency_key, task["name"], max_concurrency)
        -- Check to see if current_concurrency exists before initialising
        -- it.
        if redis.call('hexists', current_concurrency_key, task["name"]) == 0 then
            redis.call('hset', current_concurrency_key, task["name"], 0)
        end
    end
end

-- Delete concurrency keys for Tasks that no longer exist.
for i=1, #old_max_values do
    local old_task_name = old_max_values[i]
    if not contains(new_task_names, old_task_name) then
        redis.call('hdel', max_concurrency_key, old_task_name)
    end
end

for i=1, #old_current_values do
    local old_task_name = old_current_values[i]
    if not contains(new_task_names, old_task_name) then
        redis.call('hdel', current_concurrency_key, old_task_name)
    end
end
