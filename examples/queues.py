import time
import logging

from spinach import Engine, Tasks, MemoryBroker


logging.basicConfig(
    format='%(asctime)s - %(threadName)s %(levelname)s: %(message)s',
    level=logging.DEBUG
)
tasks = Tasks()


@tasks.task(name='fast', queue='high-priority')
def fast():
    time.sleep(1)


@tasks.task(name='slow', queue='low-priority')
def slow():
    time.sleep(10)


spin = Engine(MemoryBroker())
spin.attach_tasks(tasks)

tasks.schedule('slow')
tasks.schedule('fast')

spin.start_workers(number=1, queue='low-priority')
