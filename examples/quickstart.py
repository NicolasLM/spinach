from spinach import Engine, Tasks, MemoryBroker

tasks = Tasks()


@tasks.task(name='compute')
def compute(a, b):
    print('Computed {} + {} = {}'.format(a, b, a + b))


spin = Engine(MemoryBroker())
spin.attach_tasks(tasks)

# Schedule a job to be executed ASAP
spin.schedule('compute', 5, 3)

print('Starting workers, ^C to quit')
spin.start_workers()
