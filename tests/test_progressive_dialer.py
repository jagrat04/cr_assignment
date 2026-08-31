from smartdialer.dialing.progressive import ProgressiveDialer


def test_progressive_is_strictly_one_to_one():
    dialer = ProgressiveDialer()
    assert dialer.calls_to_place(0) == 0
    assert dialer.calls_to_place(1) == 1
    assert dialer.calls_to_place(7) == 7


def test_progressive_never_negative():
    dialer = ProgressiveDialer()
    assert dialer.calls_to_place(-3) == 0
