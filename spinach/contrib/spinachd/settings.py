from django.conf import settings

from spinach import RedisBroker
from spinach.const import DEFAULT_NAMESPACE


SPINACH_BROKER = getattr(settings, 'SPINACH_BROKER', RedisBroker())
SPINACH_NAMESPACE = getattr(settings, 'SPINACH_NAMESPACE', DEFAULT_NAMESPACE)
SPINACH_ACTUAL_EMAIL_BACKEND = getattr(
    settings,
    'SPINACH_ACTUAL_EMAIL_BACKEND',
    'django.core.mail.backends.smtp.EmailBackend'
)
SPINACH_CLEAR_SESSIONS_PERIODICITY = getattr(
    settings,
    'SPINACH_CLEAR_SESSIONS_PERIODICITY',
    None
)
