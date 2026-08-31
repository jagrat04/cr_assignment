"""Runs the required answer-rate x AHT scenario matrix and prints a report.

    python scripts/run_simulation.py
"""

from __future__ import annotations

import asyncio
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from smartdialer.campaign import DialMode
from smartdialer.providers.provider_a import ProviderA
from smartdialer.providers.provider_b import ProviderB
from smartdialer.simulation import ScenarioResult, ScenarioSpec, constant, run_scenario, steps

DURATION = 900.0  # virtual seconds per scenario (15 simulated minutes)

SCENARIOS = [
    # --- Answer rate axis (AHT held at 120s) ---
    ScenarioSpec(name="predictive_ar20_aht120", mode=DialMode.PREDICTIVE,
                 answer_rate_schedule=constant(0.20), talk_time_schedule=constant(120), seed=1),
    ScenarioSpec(name="predictive_ar50_aht120", mode=DialMode.PREDICTIVE,
                 answer_rate_schedule=constant(0.50), talk_time_schedule=constant(120), seed=2),
    ScenarioSpec(name="predictive_ar70_aht120", mode=DialMode.PREDICTIVE,
                 answer_rate_schedule=constant(0.70), talk_time_schedule=constant(120), seed=3),
    ScenarioSpec(name="predictive_arCHANGING_aht120", mode=DialMode.PREDICTIVE,
                 answer_rate_schedule=steps(DURATION, [0.20, 0.70, 0.45]),
                 talk_time_schedule=constant(120), seed=4),

    # --- AHT axis (answer rate held at 50%) ---
    ScenarioSpec(name="predictive_ar50_aht90", mode=DialMode.PREDICTIVE,
                 answer_rate_schedule=constant(0.50), talk_time_schedule=constant(90), seed=5),
    ScenarioSpec(name="predictive_ar50_aht180", mode=DialMode.PREDICTIVE,
                 answer_rate_schedule=constant(0.50), talk_time_schedule=constant(180), seed=6),
    ScenarioSpec(name="predictive_ar50_ahtCHANGING", mode=DialMode.PREDICTIVE,
                 answer_rate_schedule=constant(0.50),
                 talk_time_schedule=steps(DURATION, [90, 180, 120]), seed=7),

    # --- Provider B (timeouts/duplicates/out-of-order) under predictive load ---
    ScenarioSpec(name="predictive_ar50_aht120_providerB", mode=DialMode.PREDICTIVE,
                 answer_rate_schedule=constant(0.50), talk_time_schedule=constant(120),
                 provider_cls=ProviderB, seed=8),

    # --- Progressive baseline for comparison (never over-dials, so 0% abandonment) ---
    ScenarioSpec(name="progressive_ar50_aht120", mode=DialMode.PROGRESSIVE,
                 answer_rate_schedule=constant(0.50), talk_time_schedule=constant(120), seed=9),
]


def print_report(results: list[ScenarioResult]) -> None:
    header = (f"{'scenario':<32} {'mode':<11} {'dials':>6} {'compl':>6} {'aband':>6} "
              f"{'connect%':>9} {'aband%':>8} {'util%':>7} {'scale':>6}")
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.name:<32} {r.mode:<11} {r.dials_placed:>6} {r.completed:>6} {r.abandoned:>6} "
            f"{r.connect_rate * 100:>8.1f}% {r.abandonment_rate * 100:>7.1f}% "
            f"{r.utilization_estimate * 100:>6.1f}% {r.final_pacing_scale:>6.2f}"
        )
    print()
    for r in results:
        if r.reason_breakdown:
            print(f"  {r.name}: failure reasons = {r.reason_breakdown}")


def write_csv(results: list[ScenarioResult], path: str) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["scenario", "mode", "n_agents", "elapsed_virtual_seconds", "dials_placed",
                          "completed", "failed", "abandoned", "connect_rate", "abandonment_rate",
                          "utilization_estimate", "final_pacing_scale", "final_answer_rate_estimate"])
        for r in results:
            writer.writerow([r.name, r.mode, r.n_agents, f"{r.elapsed_virtual_seconds:.1f}",
                              r.dials_placed, r.completed, r.failed, r.abandoned,
                              f"{r.connect_rate:.4f}", f"{r.abandonment_rate:.4f}",
                              f"{r.utilization_estimate:.4f}", f"{r.final_pacing_scale:.3f}",
                              f"{r.final_answer_rate_estimate:.3f}"])


async def main() -> None:
    results = []
    for spec in SCENARIOS:
        print(f"running {spec.name} ...", flush=True)
        result = await run_scenario(spec)
        results.append(result)

    print()
    print_report(results)

    out_path = os.path.join(os.path.dirname(__file__), "..", "simulation_results.csv")
    write_csv(results, out_path)
    print(f"\nwrote {os.path.abspath(out_path)}")


if __name__ == "__main__":
    asyncio.run(main())
