from sentry_sdk.integrations import Integration
from sentry_sdk.scope import Scope, ScopeType

from spinach import signals


class SpinachIntegration(Integration):
    """Register the Sentry SDK integration.

    Exceptions making jobs fail are sent to Sentry and performance
    tracing of Spinach tasks is enabled.

    :param send_retries: whether to also send to Sentry exceptions resulting
           in a job being retried
    """

    identifier = 'spinach'

    def __init__(self, send_retries: bool=False):
        self.send_retries = send_retries

    @staticmethod
    def setup_once():
        signals.job_started.connect(_job_started)
        signals.job_finished.connect(_job_finished)
        signals.job_failed.connect(_job_failed)
        signals.job_schedule_retry.connect(_job_schedule_retry)


def _job_started(namespace, job, **kwargs):

    current_scope = Scope(ty=ScopeType.CURRENT)
    Scope.set_current_scope(current_scope)

    isolation_scope = Scope(ty=ScopeType.ISOLATION)
    Scope.set_isolation_scope(isolation_scope)

    isolation_scope.transaction = job.task_name
    isolation_scope.clear_breadcrumbs()
    for attr in job.__slots__:
        isolation_scope.set_extra(attr, getattr(job, attr))

    # Transactions and spans are for tracing
    transaction = isolation_scope.start_transaction(
        op='task',
        name=job.task_name
    )
    # Transaction are meant to be used as a context manager,
    # but this does not fit the signals based approach well so
    # pretend that we use a context manager.
    transaction.__enter__()


def _job_finished(namespace, job, **kwargs):
    isolation_scope = Scope.get_isolation_scope()
    for attr in job.__slots__:
        isolation_scope.set_extra(attr, getattr(job, attr))
    transaction = isolation_scope.transaction
    if transaction is not None:
        transaction.__exit__(None, None, None)
    Scope.set_current_scope(None)
    Scope.set_isolation_scope(None)


def _job_failed(namespace, job, **kwargs):
    scope = Scope.get_isolation_scope()
    for attr in job.__slots__:
        scope.set_extra(attr, getattr(job, attr))
    scope.capture_exception()
    if scope.transaction is not None:
        scope.transaction.set_status("internal_error")


def _job_schedule_retry(namespace, job, **kwargs):
    scope = Scope.get_isolation_scope()
    for attr in job.__slots__:
        scope.set_extra(attr, getattr(job, attr))
    integration = scope.get_client().get_integration(SpinachIntegration)
    if integration is None:
        return

    if integration.send_retries:
        scope.capture_exception()
