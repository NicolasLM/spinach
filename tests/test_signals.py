from unittest.mock import Mock

import pytest

from spinach import signals


def test_signal_no_receiver():
    signals.job_started.send()


def test_signal_multiple_send_args():
    with pytest.raises(TypeError):
        signals.job_started.send('foo', 'bar')


def test_signal_with_explicit_sender():
    sender = object()
    mock_receiver = Mock(spec={})
    signals.job_started.connect(mock_receiver)

    signals.job_started.send(sender)
    mock_receiver.assert_called_once_with(sender)


def test_signal_receiver_exception():
    mock_receiver = Mock(spec={}, side_effect=RuntimeError)
    signals.job_started.connect(mock_receiver)

    signals.job_started.send()


def test_signal_repr():
    assert repr(signals.job_started) == 'SafeNamedSignal "job_started"'
