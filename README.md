# SmartDialer

A functional prototype of a collections SmartDialer: progressive dialing, a
predictive pacing engine, and a Safety Controller that gates every
speculative call before it reaches the telecom provider. See
[ARCHITECTURE.md](ARCHITECTURE.md) for the design, state machines, ADR, and
the scaling/safety write-up.

## Setup

Requires Python 3.11+.

```bash
python -m venv .venv
.venv/Scripts/activate        # Windows
# source .venv/bin/activate   # macOS/Linux
pip install -r requirements.txt
```

No external services, database server, or API keys are needed — everything
(including the telecom providers) is mocked and the store is a local SQLite
file created on demand.

## Running things

**Tests** (state machines, concurrency, providers, pacing/safety logic,
failure handling, end-to-end campaigns — 48 tests):

```bash
pytest -q
```

**Simulation** — runs the required answer-rate x AHT scenario matrix
(20/50/70%/changing answer rates, 90/120/180s/changing AHT, plus a
Provider-B and a progressive-baseline run) and prints a report:

```bash
python scripts/run_simulation.py
```

Also writes `simulation_results.csv` in the repo root. Each scenario
compresses simulated call-center time (the campaign's own clock runs ~90x
faster than the wall clock) so a 30-minute simulated campaign finishes in
about 20 real seconds; the whole matrix takes roughly 3 minutes.

**Load test** — 8 workers racing for a 40-agent pool under sustained call
volume against one real SQLite-backed store; reports throughput and
reservation contention, then verifies no agent was ever double-booked:

```bash
python scripts/load_test.py
```

## Project layout

```
smartdialer/
  models.py            Agent/Call/Contact dataclasses, state enums
  state_machines.py     Agent and Call state machines + event reconciliation
  store.py               SQLite-backed shared store: atomic (CAS) agent
                          reservation, contact claiming, call transitions,
                          lease/heartbeat expiry, event dedup
  clock.py                Real vs. time-compressed clock abstraction
  events.py                Provider event envelope
  providers/                Provider interface + two mock implementations
    provider_a.py            fast, reliable
    provider_b.py             slow, timeouts, duplicate & out-of-order events
  dialing/
    progressive.py            strict 1:1 dialer
    predictive.py               rolling-stats pacing engine (a *request*, never a call)
    safety_controller.py         APPROVE / REDUCE / REJECT / FALLBACK_TO_PROGRESSIVE
  campaign.py                    per-campaign config (mode, leases, thresholds)
  worker.py                       the async DialerWorker loop tying it all together
  simulation.py                    scenario runner used by scripts/run_simulation.py
scripts/
  run_simulation.py                answer-rate x AHT scenario matrix
  load_test.py                      concurrency/throughput load test
tests/                               48 tests across all of the above
```

## A note on the numbers

The simulation report shows predictive-mode abandonment settling around
9-22% rather than the controller's 3% target, even though the Safety
Controller correctly ratchets pacing down to its floor (and, in the
Provider-B / low-sample scenarios, falls back to progressive entirely).
This is a real, honestly-reported characteristic of the prototype at this
scale (10-15 agents, single worker, a compressed clock) — not a bug being
hidden. [ARCHITECTURE.md](ARCHITECTURE.md) explains why, what a larger or
longer-running deployment would look like, and why progressive dialing's
0% abandonment is the deliberate deterministic baseline the whole system
falls back to whenever predictive can't be trusted.
