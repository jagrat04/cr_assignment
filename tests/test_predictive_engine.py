from smartdialer.dialing.predictive import PredictivePacingEngine


def test_default_answer_rate_used_before_any_samples():
    engine = PredictivePacingEngine(default_answer_rate=0.3, engine_max_line_ratio=10.0)
    assert engine.sample_size == 0
    req = engine.request(available_agents=10, in_flight=0)
    assert req.answer_rate == 0.3
    assert req.ratio == 1 / 0.3  # 3.33, below the raised cap so it isn't clamped


def test_ratio_is_capped_by_engine_max_line_ratio():
    engine = PredictivePacingEngine(default_answer_rate=0.1, engine_max_line_ratio=3.0)
    req = engine.request(available_agents=10, in_flight=0)
    assert req.ratio == 3.0  # would be 10.0 uncapped


def test_higher_answer_rate_yields_lower_ratio():
    low = PredictivePacingEngine()
    for _ in range(30):
        low.record_outcome(answered=False)
    for _ in range(10):
        low.record_outcome(answered=True)  # ~25% answer rate

    high = PredictivePacingEngine()
    for _ in range(10):
        high.record_outcome(answered=False)
    for _ in range(30):
        high.record_outcome(answered=True)  # ~75% answer rate

    low_req = low.request(available_agents=10, in_flight=0)
    high_req = high.request(available_agents=10, in_flight=0)
    assert low_req.ratio > high_req.ratio
    assert low_req.requested_calls > high_req.requested_calls


def test_in_flight_reduces_requested_calls():
    engine = PredictivePacingEngine(default_answer_rate=0.5)
    for _ in range(25):
        engine.record_outcome(answered=True)
    for _ in range(25):
        engine.record_outcome(answered=False)

    no_in_flight = engine.request(available_agents=10, in_flight=0)
    with_in_flight = engine.request(available_agents=10, in_flight=no_in_flight.requested_calls)
    assert with_in_flight.requested_calls == 0


def test_engine_level_ratio_is_capped():
    engine = PredictivePacingEngine(engine_max_line_ratio=2.5)
    for _ in range(50):
        engine.record_outcome(answered=False)  # 0% -> would want an enormous ratio
    req = engine.request(available_agents=10, in_flight=0)
    assert req.ratio == 2.5


def test_avg_talk_time_tracks_only_answered_calls():
    engine = PredictivePacingEngine()
    engine.record_outcome(answered=False, talk_time=None)
    engine.record_outcome(answered=True, talk_time=100.0)
    engine.record_outcome(answered=True, talk_time=200.0)
    assert engine.avg_talk_time == 150.0


def test_window_size_bounds_memory():
    engine = PredictivePacingEngine(window_size=5)
    for i in range(20):
        engine.record_outcome(answered=(i % 2 == 0))
    assert engine.sample_size == 5
