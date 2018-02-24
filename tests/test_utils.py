import threading
from unittest.mock import Mock, patch, ANY

from redis import ConnectionError

from spinach import utils


def test_human_duration():
    assert utils.human_duration(0.00001) == '0 ms'
    assert utils.human_duration(0.001) == '1 ms'
    assert utils.human_duration(0.25) == '250 ms'
    assert utils.human_duration(1) == '1 s'
    assert utils.human_duration(2500) == '2500 s'


@patch('spinach.utils.time.sleep')
def test_run_forever(_):
    must_stop = threading.Event()
    logger = Mock()
    call_count = 0

    def func():
        nonlocal call_count
        call_count += 1

        if call_count == 1:
            return
        elif call_count == 2:
            raise RuntimeError('Foo')
        elif call_count == 3:
            raise ConnectionError('Bar')
        elif call_count == 4:
            must_stop.set()
            return

    utils.run_forever(func, must_stop, logger)
    assert call_count == 4
    logger.exception.assert_called_once_with(ANY)
    logger.warning.assert_called_once_with(ANY, ANY)
    assert must_stop.is_set()
