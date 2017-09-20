from datetime import datetime, timezone
import enum
import json
from logging import getLogger
import math
from typing import Optional
import uuid

logger = getLogger(__name__)


class JobStatus(enum.Enum):

    NOT_SET = 0      # Job didn't even hit broker yet
    WAITING = 1      # Job is scheduled to start in the future
    QUEUED = 2       # Job is in a queue, ready to be picked by a worker
    RUNNING = 3      # Job is being executed
    SUCCEEDED = 4    # Job is finished, everything went well
    FAILED = 5       # Job failed for good


class Job:

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
            'at': self.at.timestamp(),
            'task_args': self.task_args,
            'task_kwargs': self.task_kwargs
        }, sort_keys=True)

    @classmethod
    def deserialize(cls, job_json_string: str):
        job_dict = json.loads(job_json_string)
        job = Job(
            job_dict['task_name'],
            job_dict['queue'],
            datetime.fromtimestamp(job_dict['at'], tz=timezone.utc),
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
