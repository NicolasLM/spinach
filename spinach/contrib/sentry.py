from typing import Optional

from spinach import signals


def register_sentry(raven_client, namespace: Optional[str]=None,
                    send_retries: bool=False):
    """Register the Sentry integration.

    Exceptions making jobs fail are sent to Sentry.

    :param raven_client: configured Raven client used to sent errors to Sentry
    :param namespace: optionally only register the Sentry integration for a
           particular Spinach :class:`Engine`
    :param send_retries: whether to also send to Sentry exceptions resulting
           in a job being retried
    """

    @signals.job_started.connect_via(namespace)
    def job_started(namespace, job, **kwargs):
        raven_client.context.activate()
        raven_client.transaction.push(job.task_name)

    @signals.job_finished.connect_via(namespace)
    def job_finished(namespace, job, **kwargs):
        raven_client.transaction.pop(job.task_name)
        raven_client.context.clear()

    @signals.job_failed.connect_via(namespace)
    def job_failed(namespace, job, **kwargs):
        raven_client.captureException(
            extra={attr: getattr(job, attr) for attr in job.__slots__}
        )

    if send_retries:
        @signals.job_schedule_retry.connect_via(namespace)
        def job_schedule_retry(namespace, job, **kwargs):
            raven_client.captureException(
                extra={attr: getattr(job, attr) for attr in job.__slots__}
            )
