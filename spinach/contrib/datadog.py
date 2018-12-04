from typing import Optional

from spinach import signals


def register_datadog(tracer=None, namespace: Optional[str]=None,
                     service: str='spinach'):
    """Register the Datadog integration.

    Exceptions making jobs fail are sent to Sentry.

    :param tracer: optionally use a custom ddtrace Tracer instead of the global
           one.
    :param namespace: optionally only register the Datadog integration for a
           particular Spinach :class:`Engine`
    :param service: Datadog service associated with the trace, defaults to
           `spinach`
    """
    if tracer is None:
        from ddtrace import tracer

    @signals.job_started.connect_via(namespace)
    def job_started(namespace, job, **kwargs):
        tracer.trace(
            'spinach.task', service=service, span_type='worker',
            resource=job.task_name
        )

    @signals.job_finished.connect_via(namespace)
    def job_finished(namespace, job, **kwargs):
        root_span = tracer.current_root_span()
        for attr in job.__slots__:
            root_span.set_tag(attr, getattr(job, attr))
        root_span.finish()

    @signals.job_failed.connect_via(namespace)
    def job_failed(namespace, job, **kwargs):
        root_span = tracer.current_root_span()
        root_span.set_traceback()

    @signals.job_schedule_retry.connect_via(namespace)
    def job_schedule_retry(namespace, job, **kwargs):
        root_span = tracer.current_root_span()
        root_span.set_traceback()
