import django
import django.conf
from django.core.mail import send_mail
from django.core.management import call_command

from spinach import MemoryBroker


# capsys fixture allows to capture stdout
def test_django_app(capsys):
    django.conf.settings.configure(
        LOGGING_CONFIG=None,
        INSTALLED_APPS=('spinach.contrib.spinachd',),
        EMAIL_BACKEND='spinach.contrib.spinachd.mail.BackgroundEmailBackend',
        SPINACH_BROKER=MemoryBroker(),
        SPINACH_ACTUAL_EMAIL_BACKEND='django.core.mail.backends.'
                                     'console.EmailBackend'
    )
    django.setup()

    from spinach.contrib.spinachd import spin
    spin.schedule('spinachd:clear_expired_sessions')
    send_mail('Subject', 'Hello from email', 'sender@example.com',
              ['receiver@example.com'])

    call_command('spinach', '--stop-when-queue-empty')

    captured = capsys.readouterr()
    assert 'Hello from email' in captured.out
