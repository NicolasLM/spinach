local namespace = ARGV[1]

local pattern = string.format("%s/*", namespace)

for _, key in ipairs(redis.call('keys', pattern)) do
    redis.call('del', key)
end
