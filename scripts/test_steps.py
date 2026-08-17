#!/usr/bin/env python3
"""
test_steps.py — Teste séparément chaque étape du pipeline d'entrée, pour
isoler précisément où la variance de tokens apparaît :

  STEP 1: Lire MMAR-meta.json seul, ne rien extraire (juste confirmer
          la lecture). Isole le coût de la lecture brute du fichier.
  STEP 2: Lire + extraire un item précis, sans toucher à l'audio.
          (= ce qu'on vient de tester avec test_minimal_context.py)
  STEP 3: Lire + extraire + appeler audio_split via le SCRIPT empaqueté
          uniquement (pas d'implémentation inline). Isole le coût de
          l'appel au skill, sans l'ambiguïté inline/script.
  STEP 4: Comme STEP 3 mais SANS forcer le script (permet l'inline) —
          pour comparer directement l'effet de la contrainte.

Chaque étape est répétée N fois pour avoir une vraie distribution, pas
un point isolé.

Usage:
    python test_steps.py --thread-id "th-XXX" --deployment-id "dp-XXX" \\
        --item-id "BV1tb4y1M7K5_1-41_1-57" --n-runs 3 --steps 1,2,3,4

Variables d'environnement requises (.env ou export): ALIVE_TOKEN
"""

import argparse
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

CONTEXT_OFF = {"room": False, "aliveness": False, "notice": False}


def build_steps(item_id: str) -> dict:
    return {
        1: (
            "ws_read (full file) — baseline",
            'Read ./data/MMAR/MMAR-meta.json using ws_read. Output only: the '
            'total number of items in the file, as a single integer. Nothing else.'
        ),
        1.5: (
            "ws_grep (targeted) — existence check only",
            f'Use ws_grep to search for the exact string "{item_id}" in '
            f'./data/MMAR/MMAR-meta.json. Do NOT use ws_read on this file — '
            f'do not load the full file into context under any circumstance. '
            f'Output only: FOUND or NOT_FOUND. Nothing else.'
        ),
        2: (
            "ws_read + extract item (no audio) — baseline",
            f'Read ./data/MMAR/MMAR-meta.json and extract item with the id : '
            f'{item_id} . Output only the extracted item as JSON, do nothing else.'
        ),
        2.5: (
            "ws_grep + extract item (no audio) — targeted",
            f'Use ws_grep to locate the entry for id "{item_id}" in '
            f'./data/MMAR/MMAR-meta.json. Do NOT use ws_read on this file — '
            f'do not load the full file into context. Use only the matched '
            f'line(s)/context returned by ws_grep to build the item\'s JSON '
            f'object. Output only the extracted item as JSON, do nothing else.'
        ),
        3: (
            "Read + extract + audio_split (script-only)",
            f'Read ./data/MMAR/MMAR-meta.json and extract item with the id : '
            f'{item_id} . Get its audio_path. Use audio_split skill via the '
            f'bundled script ONLY: python skills/audio_split/scripts/split_audio.py '
            f'<audio_path> ./data/artifacts/{item_id}/. Do NOT implement the '
            f'split inline. Output only the JSON returned by the script, nothing else.'
        ),
        4: (
            "Read + extract + audio_split (inline allowed)",
            f'Read ./data/MMAR/MMAR-meta.json and extract item with the id : '
            f'{item_id} . Get its audio_path. Use the audio_split skill '
            f'(either the bundled script or an inline implementation, your choice) '
            f'to split the audio into ./data/artifacts/{item_id}/. Output only '
            f'the resulting JSON, do nothing else.'
        ),
    }


def run_once(client: httpx.Client, thread_id: str, deployment_id: str, prompt: str) -> dict:
    body = {
        "input": prompt,
        "stream": False,
        "parent_id": None,
        "context": CONTEXT_OFF,
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


def run_step(client: httpx.Client, thread_id: str, deployment_id: str,
             step_num: int, label: str, prompt: str, n_runs: int) -> list:
    print(f"\n{'='*60}\nSTEP {step_num}: {label}\n{'='*60}")
    print(f"Prompt: {prompt[:150]}...\n")
    prompt_tokens = []
    for i in range(1, n_runs + 1):
        try:
            result = run_once(client, thread_id, deployment_id, prompt)
            tokens = result.get("tokens", {})
            pt = tokens.get("prompt")
            prompt_tokens.append(pt)
            print(f"  Run {i}/{n_runs}: tokens.prompt = {pt}  "
                  f"(completion={tokens.get('completion')}, "
                  f"credits_used={result.get('credits_used')})")
            print(f"    content: {(result.get('content') or '')[:120]}")
        except Exception as e:
            print(f"  Run {i}/{n_runs}: ÉCHEC — {e}")
    return [t for t in prompt_tokens if t is not None]


def main():
    parser = argparse.ArgumentParser(description="Teste chaque étape du pipeline d'entrée séparément.")
    parser.add_argument("--thread-id", required=True)
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--item-id", required=True)
    parser.add_argument("--n-runs", type=int, default=3, help="Répétitions par étape")
    parser.add_argument("--steps", default="1,1.5,2,2.5,3,4",
                         help="Étapes à tester, séparées par des virgules (ex: 1,1.5,2,2.5,3,4)")
    args = parser.parse_args()

    steps_to_run = [float(s) if "." in s else int(s) for s in args.steps.split(",")]
    all_steps = build_steps(args.item_id)

    results = {}
    with httpx.Client() as client:
        for step_num in steps_to_run:
            if step_num not in all_steps:
                print(f"[skip] Étape {step_num} inconnue.")
                continue
            label, prompt = all_steps[step_num]
            tokens = run_step(client, args.thread_id, args.deployment_id,
                               step_num, label, prompt, args.n_runs)
            results[step_num] = (label, tokens)

    print(f"\n{'='*60}\n=== RÉSUMÉ COMPARATIF ===\n{'='*60}\n")
    print(f"{'Étape':<8}{'Description':<45}{'Moyenne':<12}{'Écart-type':<12}{'Min':<10}{'Max':<10}")
    for step_num in sorted(results.keys()):
        label, tokens = results[step_num]
        if len(tokens) >= 2:
            mean_t = statistics.mean(tokens)
            stdev_t = statistics.stdev(tokens)
            print(f"{step_num:<8}{label:<45}{mean_t:<12.0f}{stdev_t:<12.0f}{min(tokens):<10}{max(tokens):<10}")
        elif len(tokens) == 1:
            print(f"{step_num:<8}{label:<45}{tokens[0]:<12}{'N/A':<12}{tokens[0]:<10}{tokens[0]:<10}")
        else:
            print(f"{step_num:<8}{label:<45}{'ÉCHEC':<12}")

    print("\nLecture: si le saut de tokens (moyenne) se produit entre l'étape N et N+1,")
    print("la cause se situe dans ce qui est ajouté à cette étape précise.")
    print("Si l'écart-type explose à une étape donnée alors qu'il était stable avant,")
    print("cette étape est la source de la VARIANCE (pas juste du coût de base).")


if __name__ == "__main__":
    main()