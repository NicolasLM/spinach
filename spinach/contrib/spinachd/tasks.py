from importlib import import_module
from logging import getLogger
from typing import List

from django.apps import apps
from django.conf import settings
from django.core import mail
from django import VERSION as DJANGO_VERSION

from spinach import Tasks

from .settings import (
    SPINACH_ACTUAL_EMAIL_BACKEND,
    SPINACH_MAILER,
    SPINACH_CLEAR_SESSIONS_PERIODICITY as PERIODICITY
)

tasks = Tasks()
logger = getLogger(__name__)


def _get_connection():
    if DJANGO_VERSION >= (6, 1):
        return mail.mailers[SPINACH_MAILER]
    # Django < 6.1
    return mail.get_connection(SPINACH_ACTUAL_EMAIL_BACKEND)


@tasks.task(name='spinachd:send_emails')
def send_emails(messages: List[str]):
    from .mail import deserialize_email_messages
    messages = deserialize_email_messages(messages)
    connection = _get_connection()
    logger.info('Sending %d emails using %s.%s', len(messages),
                type(connection).__module__, type(connection).__qualname__)
    connection.send_messages(messages)


@tasks.task(name='spinachd:clear_expired_sessions', periodicity=PERIODICITY)
def clear_expired_sessions():
    if not apps.is_installed('django.contrib.sessions'):
        logger.info('django.contrib.sessions not installed, '
                    'not clearing expired sessions')
        return

    engine = import_module(settings.SESSION_ENGINE)
    try:
        engine.SessionStore.clear_expired()
    except NotImplementedError:
        logger.info("Session engine '%s' doesn't support clearing "
                    "expired sessions", settings.SESSION_ENGINE)
