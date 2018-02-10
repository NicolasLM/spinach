local broker_id = ARGV[1]
local namespace = ARGV[2]
local pattern = string.format("%s/*", namespace)

for _, key in ipairs(redis.call('keys', pattern)) do
    redis.call('del', key)
end
