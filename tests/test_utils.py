from spinach import utils


def test_human_duration():
    assert utils.human_duration(0.00001) == '0 ms'
    assert utils.human_duration(0.001) == '1 ms'
    assert utils.human_duration(0.25) == '250 ms'
    assert utils.human_duration(1) == '1 s'
    assert utils.human_duration(2500) == '2500 s'
