import argparse
import json
import sys
import time
import traceback
from pathlib import Path
 
import httpx
 
from alive_common import run_one_item
 
 
def load_items(items_file, cli_items: list) -> list:
    items = list(cli_items)
    if items_file:
        for line in items_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                items.append(line)
    seen = set()
    ordered = []
    for it in items:
        if it not in seen:
            seen.add(it)
            ordered.append(it)
    return ordered
 
 
def main():
    parser = argparse.ArgumentParser(description="Batch séquentiel du pipeline sur plusieurs items MMAR.")
    parser.add_argument("--items-file", type=Path, default=None,
                         help="Fichier texte, un item_id par ligne")
    parser.add_argument("--item-id", action="append", default=[],
                         help="Item id individuel (répétable: --item-id A --item-id B)")
    parser.add_argument("--timeout", type=float, default=1800.0,
                         help="Timeout de la connexion SSE par item, en secondes")
    parser.add_argument("--pause-between", type=float, default=10.0,
                         help="Pause en secondes entre deux items (laisse la room se calmer)")
    parser.add_argument("--out-dir", type=Path, default=Path("results"))
    parser.add_argument("--quiet", action="store_true",
                         help="Réduit le détail affiché pendant chaque run (juste les jalons)")
    args = parser.parse_args()
 
    items = load_items(args.items_file, args.item_id)
    if not items:
        sys.exit("Aucun item fourni. Utilise --items-file ou --item-id.")
 
    args.out_dir.mkdir(parents=True, exist_ok=True)
 
    print(f"=== Batch: {len(items)} item(s) ===")
    for i, it in enumerate(items, 1):
        print(f"  {i}. {it}")
    print()
 
    summary = {
        "total_items": len(items),
        "succeeded": 0,
        "failed": 0,
        "results": [],
    }
 
    with httpx.Client() as client:
        for i, item_id in enumerate(items, 1):
            print(f"\n{'='*60}\n[{i}/{len(items)}] item_id={item_id!r}\n{'='*60}")
            item_out_path = args.out_dir / f"{item_id}.json"
 
            try:
                result = run_one_item(
                    client, item_id,
                    timeout=args.timeout,
                    verbose=not args.quiet,
                )
                item_out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))
                summary["succeeded"] += 1
                summary["results"].append({
                    "item_id": item_id,
                    "status": "ok",
                    "answer": result.get("answer"),
                    "confidence": result.get("confidence"),
                    "n_reasoning_outputs_detected": result.get("n_reasoning_outputs_detected"),
                    "total_credits_used": result.get("total_credits_used"),
                    "out_file": str(item_out_path),
                })
                
                print(f"\n[{i}/{len(items)}] OK — Answer: {result.get('answer')} "
                      f"(confidence={result.get('confidence')}, "
                      f"coût={result.get('total_credits_used')} crédits)")
 
            except Exception as e:
                error_info = {
                    "item_id": item_id,
                    "status": "error",
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                }
                item_out_path.write_text(json.dumps(error_info, ensure_ascii=False, indent=2))
                summary["failed"] += 1
                summary["results"].append({
                    "item_id": item_id,
                    "status": "error",
                    "error": str(e),
                    "out_file": str(item_out_path),
                })
                print(f"\n[{i}/{len(items)}] ÉCHEC — {e}")
 
            if i < len(items) and args.pause_between > 0:
                print(f"\n(pause {args.pause_between:.0f}s avant le prochain item...)")
                time.sleep(args.pause_between)
 
    summary_path = args.out_dir / "summary.json"
    total_batch_cost = sum(
        r.get("total_credits_used") or 0 for r in summary["results"] if r["status"] == "ok"
    )
    summary["total_credits_used"] = total_batch_cost
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
 
    print(f"\n{'='*60}\n=== RÉSUMÉ ===")
    print(f"Total: {summary['total_items']} | Réussis: {summary['succeeded']} | "
          f"Échecs: {summary['failed']} | Coût cumulé: {total_batch_cost} crédits")
    for r in summary["results"]:
        status_tag = "OK" if r["status"] == "ok" else "ERREUR"
        print(f"  [{status_tag}] {r['item_id']}: "
              f"{r.get('answer', r.get('error'))}")
    print(f"\nDétails sauvés dans {args.out_dir}/ (un fichier par item + summary.json)")
 
 
if __name__ == "__main__":
    main()
 