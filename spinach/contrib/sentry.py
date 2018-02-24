from typing import Optional

from spinach import signals


def register_sentry(raven_client, namespace: Optional[str]=None):
    """Register the Sentry integration.

    Exceptions making jobs fail are sent to Sentry. Note that exceptions
    resulting in the job being retried are not sent to Sentry.

    :param raven_client: configured Raven client used to sent errors to Sentry
    :param namespace: optionally only register the Sentry integration for a
                      particular Spinach :class:`Engine`.
    """

    @signals.job_started.connect_via(namespace)
    def job_started(*args, job=None, **kwargs):
        raven_client.context.activate()
        raven_client.transaction.push(job.task_name)

    @signals.job_finished.connect_via(namespace)
    def job_finished(*args, job=None, **kwargs):
        raven_client.transaction.pop(job.task_name)
        raven_client.context.clear()

    @signals.job_failed.connect_via(namespace)
    def job_failed(*args, job=None, **kwargs):
        raven_client.captureException(
            extra={attr: getattr(job, attr) for attr in job.__slots__}
        )
