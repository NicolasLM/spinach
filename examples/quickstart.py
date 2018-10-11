from spinach import Engine, MemoryBroker

spin = Engine(MemoryBroker())


@spin.task(name='compute')
def compute(a, b):
    print('Computed {} + {} = {}'.format(a, b, a + b))


# Schedule a job to be executed ASAP
spin.schedule(compute, 5, 3)

print('Starting workers, ^C to quit')
spin.start_workers()
