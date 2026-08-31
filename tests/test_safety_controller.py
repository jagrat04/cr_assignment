from smartdialer.dialing.predictive import PacingRequest
from smartdialer.dialing.safety_controller import SafetyController, SafetyDecision


def make_request(requested_calls=5, ratio=1.5, sample_size=30, answer_rate=0.66):
    return PacingRequest(
        requested_calls=requested_calls, ratio=ratio, sample_size=sample_size,
        answer_rate=answer_rate, avg_talk_time=120.0,
    )


def test_falls_back_when_sample_size_too_small():
    controller = SafetyController(min_samples_for_predictive=20)
    verdict = controller.evaluate(make_request(sample_size=5), available_agents=10, in_flight=0)
    assert verdict.decision == SafetyDecision.FALLBACK_TO_PROGRESSIVE
    assert verdict.approved_calls == 10  # progressive-equivalent: one per agent


def test_approves_within_limits():
    controller = SafetyController(hard_max_line_ratio=3.0)
    # available_agents=10, ratio=1.3 -> target_lines=13, in_flight=10 -> 3 approved.
    # requested_calls must agree with that (as the real PredictivePacingEngine
    # always would) or the controller correctly reports it as a REDUCE/increase
    # relative to what was asked.
    verdict = controller.evaluate(make_request(requested_calls=3, ratio=1.3), available_agents=10, in_flight=10)
    assert verdict.decision == SafetyDecision.APPROVE
    assert verdict.approved_calls == 3


def test_hard_cap_reduces_oversized_request():
    controller = SafetyController(hard_max_line_ratio=1.2)
    # ratio implies far more lines than the hard cap allows
    verdict = controller.evaluate(make_request(requested_calls=50, ratio=5.0), available_agents=10, in_flight=0)
    assert verdict.decision in (SafetyDecision.REDUCE, SafetyDecision.REJECT)
    assert verdict.approved_calls <= 12  # 10 * 1.2


def test_hard_cap_never_exceeded_regardless_of_request_size():
    controller = SafetyController(hard_max_line_ratio=2.0)
    verdict = controller.evaluate(make_request(requested_calls=1000, ratio=100.0), available_agents=5, in_flight=0)
    assert verdict.approved_calls <= 10


def test_circuit_breaker_trips_on_high_provider_error_rate():
    controller = SafetyController(max_provider_error_rate=0.3)
    for _ in range(10):
        controller.record_provider_outcome(errored=True)
    verdict = controller.evaluate(make_request(), available_agents=10, in_flight=0)
    assert verdict.decision == SafetyDecision.FALLBACK_TO_PROGRESSIVE


def test_no_circuit_break_when_errors_are_rare():
    controller = SafetyController(max_provider_error_rate=0.3)
    for i in range(20):
        controller.record_provider_outcome(errored=(i == 0))  # 5% error rate
    verdict = controller.evaluate(make_request(requested_calls=2, ratio=1.2), available_agents=10, in_flight=0)
    assert verdict.decision != SafetyDecision.FALLBACK_TO_PROGRESSIVE


def test_abandonment_feedback_ratchets_pacing_scale_down():
    controller = SafetyController(target_abandon_rate=0.03, scale_down_factor=0.8)
    for _ in range(20):
        controller.record_answered_call(agent_was_ready=False)  # 100% abandonment
    controller.evaluate(make_request(requested_calls=10, ratio=2.0), available_agents=10, in_flight=0)
    assert controller.pacing_scale < 1.0


def test_abandonment_ratchet_compounds_across_ticks():
    controller = SafetyController(target_abandon_rate=0.03, scale_down_factor=0.8)
    for _ in range(20):
        controller.record_answered_call(agent_was_ready=False)
    for _ in range(5):
        controller.evaluate(make_request(requested_calls=10, ratio=2.0), available_agents=10, in_flight=0)
    # Repeated bad ticks should compound the reduction well below a single 0.8 step.
    assert controller.pacing_scale < 0.8


def test_pacing_scale_recovers_once_abandonment_is_low():
    controller = SafetyController(target_abandon_rate=0.03, scale_down_factor=0.5, scale_up_factor=1.5)
    for _ in range(20):
        controller.record_answered_call(agent_was_ready=False)
    controller.evaluate(make_request(requested_calls=10, ratio=2.0), available_agents=10, in_flight=0)
    scale_after_bad = controller.pacing_scale
    assert scale_after_bad < 1.0

    controller._abandon_outcomes.clear()
    for _ in range(20):
        controller.record_answered_call(agent_was_ready=True)  # 0% abandonment now
    controller.evaluate(make_request(requested_calls=10, ratio=2.0), available_agents=10, in_flight=0)
    assert controller.pacing_scale > scale_after_bad


def test_critical_abandonment_forces_full_fallback_even_at_pacing_floor():
    """Even after the ratchet has driven pacing_scale to its floor, a
    tight-capacity scenario can still keep abandonment elevated. Past the
    critical threshold the controller must stop trusting the floor and cut
    over to a full progressive fallback (zero speculative lines)."""
    controller = SafetyController(target_abandon_rate=0.03, critical_abandon_rate=0.12,
                                   min_pacing_scale=0.1)
    controller.pacing_scale = 0.1
    for _ in range(50):
        controller.record_answered_call(agent_was_ready=False)  # 100% abandonment, way past critical

    verdict = controller.evaluate(make_request(ratio=2.0), available_agents=10, in_flight=5)
    assert verdict.decision == SafetyDecision.FALLBACK_TO_PROGRESSIVE


def test_ratio_scaling_shrinks_steady_state_target_not_just_the_delta():
    """The whole point of scaling the ratio (not the raw delta) is that once
    in_flight has caught up to the target, further requests stay suppressed
    instead of drifting back up just because the instantaneous delta hit zero."""
    controller = SafetyController(target_abandon_rate=0.03, critical_abandon_rate=0.5, min_pacing_scale=0.1)
    controller.pacing_scale = 0.1
    # 10% abandonment: above target (ratchet stays engaged) but below the
    # critical threshold (full fallback shouldn't trigger for this test).
    for i in range(20):
        controller.record_answered_call(agent_was_ready=(i % 10 != 0))

    # ratio=2.0 means "double the agent count"; at scale=0.1 the effective
    # ratio should be close to 1.1, so for 10 agents the target is ~11, not ~20.
    request = make_request(requested_calls=0, ratio=2.0)  # delta already 0 at in_flight=11
    verdict = controller.evaluate(request, available_agents=10, in_flight=11)
    assert verdict.approved_calls == 0

    verdict2 = controller.evaluate(request, available_agents=10, in_flight=8)
    assert verdict2.approved_calls <= 3  # ~11 - 8, not anywhere near 20 - 8
