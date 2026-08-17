#!/usr/bin/env python3
"""
test_minimal_context.py — Teste s'il existe un plancher de tokens
incompressible (contexte système de la plateforme: "soul"/aliveness,
skills, tools...) en envoyant un input minimal, quasi vide, et en
regardant le tokens.prompt obtenu.

Deux séries sont lancées:
  A) context entièrement désactivé (room=false, aliveness=false, notice=false)
     — ce qu'on utilise déjà dans alive_common.py
  B) context entièrement activé (room=true, aliveness=true, notice=true)
     — pour mesurer l'écart attribuable à NOTRE toggle de contexte

Si même la série A (tout désactivé côté API) reste très élevée (proche
de ce qu'on observe avec le vrai prompt de tâche), ça confirme qu'il
existe un plancher de tokens qu'on ne peut pas réduire depuis l'API —
probablement l'aliveness/soul configuré au niveau de l'AGENT (pas du
run), ou d'autres skills/tools attachés en permanence.

Si la série A descend très bas (quelques centaines/milliers de tokens),
alors il n'y a pas de plancher fixe et la variance vient d'ailleurs
(à re-creuser côté prompt/room).

Usage:
    python test_minimal_context.py \\
        --thread-id "th-Jx3tzwfIlb7sNq8T" \\
        --deployment-id "dp-lwxDGuXSfML5Oebd" \\
        --n-runs 3

Variables d'environnement requises (.env ou export): ALIVE_TOKEN
"""

import argparse
import json
import os
import statistics
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()

BASE = "https://brain-bzh.alive.dev/api/v1"
TOKEN = os.environ.get("ALIVE_TOKEN")
if not TOKEN:
    sys.exit("Missing ALIVE_TOKEN (.env ou variable d'environnement).")

HEADERS = {"Authorization": f"Bearer {TOKEN}"}
JSON_HEADERS = {**HEADERS, "Content-Type": "application/json"}

MINIMAL_INPUT = "Read ./data/MMAR/MMAR-meta.json and extract item with the id : BV1tb4y1M7K5_1-41_1-57 . Output only the extracted item as JSON, do nothing else."


def run_once(client: httpx.Client, thread_id: str, deployment_id: str,
             context: dict) -> dict:
    """Un seul run non-streamé, input minimal, renvoie le tokens.prompt
    obtenu (et le dict complet pour inspection)."""
    body = {
        "input": MINIMAL_INPUT,
        "stream": False,
        "parent_id": None,
        "context": context,
        "conversation": "messages",
    }
    resp = client.post(
        f"{BASE}/threads/{thread_id}/deployments/{deployment_id}/runs",
        headers=JSON_HEADERS,
        json=body,
        timeout=300,
    )
    resp.raise_for_status()
    return resp.json()


def run_series(client: httpx.Client, thread_id: str, deployment_id: str,
               label: str, context: dict, n_runs: int) -> list:
    print(f"\n{'='*60}\nSérie {label} — context={context}\n{'='*60}")
    prompt_tokens = []
    for i in range(1, n_runs + 1):
        try:
            result = run_once(client, thread_id, deployment_id, context)
            tokens = result.get("tokens", {})
            pt = tokens.get("prompt")
            prompt_tokens.append(pt)
            print(f"  Run {i}/{n_runs}: tokens.prompt = {pt}  "
                  f"(completion={tokens.get('completion')}, "
                  f"reasoning={tokens.get('reasoning')}, "
                  f"credits_used={result.get('credits_used')})")
            print(f"    content: {(result.get('content') or '')[:100]}")
        except Exception as e:
            print(f"  Run {i}/{n_runs}: ÉCHEC — {e}")
    return [t for t in prompt_tokens if t is not None]


def main():
    parser = argparse.ArgumentParser(description="Teste l'existence d'un plancher de tokens de contexte système.")
    parser.add_argument("--thread-id", required=True)
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--n-runs", type=int, default=3,
                         help="Nombre de répétitions par série (A et B)")
    args = parser.parse_args()

    with httpx.Client() as client:
        series_a = run_series(
            client, args.thread_id, args.deployment_id,
            label="A (context tout désactivé)",
            context={"room": False, "aliveness": False, "notice": False},
            n_runs=args.n_runs,
        )
        series_b = run_series(
            client, args.thread_id, args.deployment_id,
            label="B (context tout activé)",
            context={"room": True, "aliveness": True, "notice": True},
            n_runs=args.n_runs,
        )

    print(f"\n{'='*60}\n=== RÉSULTAT ===\n{'='*60}")

    if series_a:
        print(f"\nSérie A (désactivé) — moyenne: {statistics.mean(series_a):.0f} tokens"
              + (f" | écart-type: {statistics.stdev(series_a):.0f}" if len(series_a) > 1 else ""))
    else:
        print("\nSérie A: aucun résultat exploitable.")

    if series_b:
        print(f"Série B (activé)     — moyenne: {statistics.mean(series_b):.0f} tokens"
              + (f" | écart-type: {statistics.stdev(series_b):.0f}" if len(series_b) > 1 else ""))
    else:
        print("Série B: aucun résultat exploitable.")

    if series_a and series_b:
        diff = statistics.mean(series_b) - statistics.mean(series_a)
        print(f"\nÉcart attribuable à notre toggle `context`: {diff:+.0f} tokens")

    if series_a:
        floor = min(series_a)
        print(f"\nPlancher observé (minimum de la série A, input quasi vide): {floor} tokens")
        print("Interprétation:")
        print("  - Si ce chiffre reste élevé (dizaines/centaines de milliers) malgré")
        print("    un input minimal et context désactivé -> plancher incompressible")
        print("    probable au niveau agent (aliveness/soul/skills), pas réductible")
        print("    depuis l'API run-level. À confirmer avec Jean-Charles.")
        print("  - Si ce chiffre est bas (quelques centaines/milliers) -> pas de")
        print("    plancher fixe; la variance observée sur le vrai prompt de tâche")
        print("    vient d'ailleurs (prompt lui-même, room, ou autre chose à creuser).")


if __name__ == "__main__":
    main()