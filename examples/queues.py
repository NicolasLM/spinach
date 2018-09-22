import time
import logging

from spinach import Engine, MemoryBroker


logging.basicConfig(
    format='%(asctime)s - %(threadName)s %(levelname)s: %(message)s',
    level=logging.DEBUG
)
spin = Engine(MemoryBroker())


@spin.task(name='fast', queue='high-priority')
def fast():
    time.sleep(1)


@spin.task(name='slow', queue='low-priority')
def slow():
    time.sleep(10)


spin.schedule('slow')
spin.schedule('fast')

spin.start_workers(number=1, queue='high-priority', stop_when_queue_empty=True)
