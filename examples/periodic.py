from datetime import timedelta

from spinach import Engine, MemoryBroker

spin = Engine(MemoryBroker())
every_5_sec = timedelta(seconds=5)


@spin.task(name='make_coffee', periodicity=every_5_sec)
def make_coffee():
    print("Making coffee...")


print('Starting workers, ^C to quit')
spin.start_workers()
