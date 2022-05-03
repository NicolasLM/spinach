VERSION = '0.0.16'

DEFAULT_QUEUE = 'spinach'
DEFAULT_NAMESPACE = 'spinach'
DEFAULT_MAX_RETRIES = 0
DEFAULT_ENQUEUE_JOB_RETRIES = 4
DEFAULT_WORKER_NUMBER = 5

FUTURE_JOBS_KEY = '_future-jobs'
RUNNING_JOBS_KEY = '_running-jobs-on-broker-{}'
NOTIFICATIONS_KEY = '_notifications'
PERIODIC_TASKS_HASH_KEY = '_periodic_tasks_hash'
PERIODIC_TASKS_QUEUE_KEY = '_periodic_tasks_queue'
ALL_BROKERS_HASH_KEY = '_all_brokers_hash'
ALL_BROKERS_ZSET_KEY = '_all_brokers_zset'
MAX_CONCURRENCY_KEY = '_max_concurrency'
CURRENT_CONCURRENCY_KEY = '_current_concurrency'

WAIT_FOR_EVENT_MAX_SECONDS = 60
