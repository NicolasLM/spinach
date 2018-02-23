from datetime import datetime, timezone
import enum
import json
from logging import getLogger
import math
from typing import Optional
import uuid

logger = getLogger(__name__)


class JobStatus(enum.Enum):
    """Possible status of a :class:`Job`.

    Life-cycle:

    - Newly created jobs first get the status `NOT_SET`
    - Future jobs are then set to `WAITING` until they are ready to be `QUEUED`
    - Jobs starting immediately get the status `QUEUED` directly when they are
      received by the broker
    - Jobs are set to `RUNNING` when a worker start their execution
        - if the job terminates without error it is set to `SUCCEEDED`
        - if the job terminates with an error and can be retried it is set to
          `WAITING` until it is ready to be queued again
        - if the job terminates with an error and cannot be retried it is set
          to `FAILED` for ever

    See :doc:`signals` to be notified on some of these status transitions.
    """

    NOT_SET = 0     #: Job created but not scheduled yet
    WAITING = 1     #: Job is scheduled to start in the future
    QUEUED = 2      #: Job is in a queue, ready to be picked by a worker
    RUNNING = 3     #: Job is being executed
    SUCCEEDED = 4   #: Job is finished, execution was successful
    FAILED = 5      #: Job failed and will not be retried


class Job:
    """Represent the execution of a :class:`Task` by background workers.

    The :class:`Job` class should not be instantiated by the user, instead jobs
    are automatically created when they are scheduled.

    :ivar id: UUID of the job
    :ivar status: :class:`JobStatus`
    :ivar task_name: string name of the task
    :ivar queue: string name of the queue
    :ivar at: timezone aware `datetime` representing the date at which the job
          should start
    :ivar max_retries: int representing how many times a failing job should be
          retried
    :ivar retries: int representing how many times the job was already executed
    :ivar task_args: optional tuple containing args passed to the task
    :ivar task_kwargs: optional dict containing kwargs passed to the task
    """

    __slots__ = ['id', 'status', 'task_name', 'queue', 'at', 'max_retries',
                 'retries', 'task_args', 'task_kwargs', 'task_func']

    def __init__(self, task_name: str, queue: str, at: datetime,
                 max_retries: int,
                 task_args: Optional[tuple]=None,
                 task_kwargs: Optional[dict]=None):
        self.id = uuid.uuid4()
        self.status = JobStatus.NOT_SET
        self.task_name = task_name
        self.queue = queue
        self.max_retries = max_retries
        self.retries = 0

        if at.tzinfo is None:
            # TZ naive datetime, make it a TZ aware datetime by assuming it
            # contains UTC time
            self.at = at.replace(tzinfo=timezone.utc)
            logger.debug('Job created from a naive datetime, assuming UTC')
        else:
            # TZ aware datetime, store it in its UTC representation
            self.at = at.astimezone(timezone.utc)

        self.task_args = task_args if task_args else tuple()
        self.task_kwargs = task_kwargs if task_kwargs else dict()

        # Populated by Spinach arbiter before passing to a worker
        self.task_func = None

    @property
    def should_retry(self) -> bool:
        return self.retries < self.max_retries

    @property
    def should_start(self) -> bool:
        return datetime.now(timezone.utc) >= self.at

    @property
    def at_timestamp(self) -> Optional[int]:
        return int(math.ceil(self.at.timestamp()))

    def serialize(self):
        return json.dumps({
            'id': str(self.id),
            'status': self.status.value,
            'task_name': self.task_name,
            'queue': self.queue,
            'max_retries': self.max_retries,
            'retries': self.retries,
            'at': int(self.at.timestamp()),  # seconds component
            'at_us': self.at.microsecond,    # microseconds component
            'task_args': self.task_args,
            'task_kwargs': self.task_kwargs
        }, sort_keys=True)

    @classmethod
    def deserialize(cls, job_json_string: str):
        job_dict = json.loads(job_json_string)
        at = datetime.fromtimestamp(job_dict['at'], tz=timezone.utc)
        at = at.replace(microsecond=job_dict['at_us'])
        job = Job(
            job_dict['task_name'],
            job_dict['queue'],
            at,
            job_dict['max_retries'],
            task_args=tuple(job_dict['task_args']),
            task_kwargs=job_dict['task_kwargs'],
        )
        job.id = uuid.UUID(job_dict['id'])
        job.status = JobStatus(job_dict['status'])
        job.retries = job_dict['retries']
        return job

    def __repr__(self):
        return 'Job <{} {} {}>'.format(
            self.task_name, self.status.name, self.id
        )

    def __eq__(self, other):
        for attr in self.__slots__:
            try:
                if not getattr(self, attr) == getattr(other, attr):
                    return False

            except AttributeError:
                return False

        return True
