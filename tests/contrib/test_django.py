import django
import django.conf
import pytest
from django.core.mail import send_mail
from django.core.mail.backends.console import EmailBackend
from django.core.management import call_command

from spinach import MemoryBroker

BACKGROUND = 'spinach.contrib.spinachd.mail.BackgroundEmailBackend'
CONSOLE = 'django.core.mail.backends.console.EmailBackend'


@pytest.fixture(scope='module', autouse=True)
def configure_django():
    config = dict(
        LOGGING_CONFIG=None,
        INSTALLED_APPS=('spinach.contrib.spinachd',),
        SPINACH_BROKER=MemoryBroker(),
    )
    if django.VERSION >= (6, 1):
        config['MAILERS'] = dict(
            default=dict(BACKEND=BACKGROUND),
            spinach=dict(BACKEND=CONSOLE)
        )
    else:
        config['EMAIL_BACKEND'] = BACKGROUND
        config['SPINACH_ACTUAL_EMAIL_BACKEND'] = CONSOLE
    django.conf.settings.configure(**config)
    django.setup()


# capsys fixture allows to capture stdout
def test_django_app(capsys):
    from spinach.contrib.spinachd import spin
    spin.schedule('spinachd:clear_expired_sessions')
    send_mail('Subject', 'Hello from email', 'sender@example.com',
              ['receiver@example.com'])

    call_command('spinach', '--stop-when-queue-empty')

    captured = capsys.readouterr()
    assert 'Hello from email' in captured.out


@pytest.mark.skipif(django.VERSION < (6, 1), reason='requires MAILERS')
def test_actual_connection_with_mailers():
    from spinach.contrib.spinachd import tasks

    connection = tasks._get_connection()
    assert isinstance(connection, EmailBackend)
    assert connection.alias == 'spinach'


@pytest.mark.skipif(django.VERSION >= (6, 1), reason='legacy configuration')
def test_actual_connection_without_mailers():
    from spinach.contrib.spinachd import tasks

    connection = tasks._get_connection()
    assert isinstance(connection, EmailBackend)
