#!/usr/bin/env python3
"""
variance_check.py — Lance le MÊME item plusieurs fois de suite pour mesurer
la variance de coût et de durée entre runs.

Usage:
    python variance_check.py --item-id "BV1tb4y1M7K5_1-41_1-57" --n-runs 5
"""

import argparse
import json
import statistics
import time
from pathlib import Path

import httpx

from alive_common import run_one_item, cleanup_artifacts


def main():
    parser = argparse.ArgumentParser(description="Lance le même item N fois pour mesurer la variance de coût.")
    parser.add_argument("--item-id", required=True, help="MMAR item id à répéter")
    parser.add_argument("--n-runs", type=int, default=5, help="Nombre de répétitions")
    parser.add_argument("--out-dir", type=Path, default=Path("variance"))
    parser.add_argument("--pause-between", type=float, default=10.0,
                         help="Pause en secondes entre deux runs")
    parser.add_argument("--timeout", type=float, default=800.0)
    parser.add_argument("--quiet", action="store_true",
                         help="Réduit le détail affiché pendant chaque run")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    print(f"=== Variance check: {args.n_runs} runs sur item_id={args.item_id!r} ===\n")

    results = []
    with httpx.Client() as client:
        for i in range(1, args.n_runs + 1):
            print(f"\n{'='*60}\n[Run {i}/{args.n_runs}]\n{'='*60}")
            out_path = args.out_dir / f"{args.item_id}_run{i}.json"

            cleanup_artifacts(client, args.item_id, verbose=not args.quiet)

            try:
                result = run_one_item(
                    client, args.item_id,
                    timeout=args.timeout,
                    verbose=not args.quiet,
                )
                out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))
                results.append(result)
                print(f"\n[Run {i}/{args.n_runs}] OK — coût={result.get('total_credits_used')} crédits, "
                      f"durée={result.get('total_measured_duration_seconds')}s, "
                      f"étapes={result.get('n_chain_steps')}, erreurs={result.get('n_errors')}")
            except Exception as e:
                print(f"\n[Run {i}/{args.n_runs}] ÉCHEC — {e}")
                results.append(None)

            if i < args.n_runs and args.pause_between > 0:
                print(f"\n(pause {args.pause_between:.0f}s...)")
                time.sleep(args.pause_between)

    print(f"\n{'='*60}\n=== RÉSUMÉ VARIANCE ===\n{'='*60}")
    ok_results = [r for r in results if r is not None]
    if not ok_results:
        print("Aucun run n'a réussi, rien à comparer.")
        return

    costs = [r["total_credits_used"] for r in ok_results if r.get("total_credits_used") is not None]
    durations = [r["total_measured_duration_seconds"] for r in ok_results if r.get("total_measured_duration_seconds") is not None]
    n_steps_list = [r["n_chain_steps"] for r in ok_results]

    print(f"\nRuns réussis: {len(ok_results)}/{args.n_runs}")
    print(f"{'Run':<6}{'Coût (crédits)':<18}{'Durée (s)':<12}{'Étapes':<8}{'Erreurs':<8}{'Réponse':<10}")
    for i, r in enumerate(results, 1):
        if r is None:
            print(f"{i:<6}{'ÉCHEC':<18}")
            continue
        print(f"{i:<6}{r.get('total_credits_used', '?'):<18}{r.get('total_measured_duration_seconds', '?'):<12}"
              f"{r.get('n_chain_steps', '?'):<8}{r.get('n_errors', '?'):<8}{str(r.get('answer')):<10}")

    if len(costs) >= 2:
        print(f"\nCoût — moyenne: {statistics.mean(costs):.2f} | "
              f"écart-type: {statistics.stdev(costs):.2f} | "
              f"min: {min(costs):.2f} | max: {max(costs):.2f} | "
              f"écart relatif: {(max(costs)-min(costs))/statistics.mean(costs)*100:.0f}%")
    if len(durations) >= 2:
        print(f"Durée — moyenne: {statistics.mean(durations):.1f}s | "
              f"écart-type: {statistics.stdev(durations):.1f}s | "
              f"min: {min(durations):.1f}s | max: {max(durations):.1f}s")

    if len(set(n_steps_list)) > 1:
        print(f"\n[warning] Les runs n'ont PAS tous le même nombre d'étapes ({n_steps_list}) — "
              f"la comparaison par étape ci-dessous peut être décalée après le point de divergence.")

    print(f"\n--- Variance par étape (alignement par position, voir limite en tête de fichier) ---")
    max_steps = max(n_steps_list)
    for step_idx in range(max_steps):
        step_costs = []
        for r in ok_results:
            steps = r.get("per_step_credits_used", [])
            if step_idx < len(steps) and steps[step_idx].get("credits_used") is not None:
                step_costs.append(steps[step_idx]["credits_used"])
        if len(step_costs) >= 2:
            mean_c = statistics.mean(step_costs)
            stdev_c = statistics.stdev(step_costs)
            spread_pct = (max(step_costs) - min(step_costs)) / mean_c * 100 if mean_c else 0
            flag = "  <-- forte variance" if spread_pct > 50 else ""
            print(f"  Étape #{step_idx+1}: moyenne={mean_c:.2f}, écart-type={stdev_c:.2f}, "
                  f"min={min(step_costs):.2f}, max={max(step_costs):.2f}, "
                  f"écart relatif={spread_pct:.0f}%{flag}")
        elif len(step_costs) == 1:
            print(f"  Étape #{step_idx+1}: présente dans un seul run ({step_costs[0]:.2f}) — pas comparable")

    summary_path = args.out_dir / f"{args.item_id}_variance_summary.json"
    summary_path.write_text(json.dumps({
        "item_id": args.item_id,
        "n_runs": args.n_runs,
        "n_succeeded": len(ok_results),
        "costs": costs,
        "durations": durations,
        "n_steps_per_run": n_steps_list,
    }, ensure_ascii=False, indent=2))
    print(f"\nRésumé sauvé dans {summary_path}")


if __name__ == "__main__":
    main()