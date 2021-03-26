local broker_id = ARGV[1]
local all_brokers_hash_key = ARGV[2]
local all_brokers_zset_key = ARGV[3]

-- Remove the broker from the list of brokers
redis.call('hdel', all_brokers_hash_key, broker_id)
redis.call('zrem', all_brokers_zset_key, broker_id)
