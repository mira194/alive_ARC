#!/usr/bin/env python3
"""
alive_common.py — Logique partagée pour piloter le pipeline Alive.
Utilisé par script1.py (un item) et script2.py (batch séquentiel).

Historique: la V1 suivait la chaîne par polling de GET /threads/{id}/messages
avec une détection de fin par "silence" (aucun nouveau message pendant N
secondes). Deux limites de cette approche:
  - coût (credits_used) non récupérable pour les étapes en cascade,
  - deux runs concurrents dans la même room étaient mélangés dans le suivi
    (aucun moyen de savoir à quel run appartient un message).

Suite à l'échange avec Jean-Charles, l'API expose maintenant un mode
`"follow": "chain"` sur POST .../runs (avec stream: true): la connexion SSE
reste ouverte sur TOUTE la chaîne after_deployment, pas seulement le premier
maillon. Chaque étape émet son propre run.completed (ou run.error), avec
execution_id + credits_used. Ça règle les deux limites d'un coup:
  - coût exact par étape ET total du pipeline (somme des credits_used),
  - plus de polling/heuristique de silence: le flux se termine tout seul
    quand la chaîne est réellement finie, et il est scopé à NOTRE run —
    plus de risque de mélange avec un run concurrent dans la même room.

Un mode de secours par polling reste possible via l'enrichissement annoncé
de GET /messages (chaque message lié à un run porte maintenant un objet
`run` avec execution_id/credits_used). Il est utilisé automatiquement dans
run_one_item() si la connexion SSE se termine sans qu'une vraie réponse
finale (Reasoning Generator) ait été détectée — signe probable que la
connexion a été coupée avant [DONE] (ex: timeout d'inactivité d'un proxy
pendant une étape lente comme la boucle de vérification), alors que le
pipeline continue de tourner normalement côté serveur.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv

load_dotenv()

BASE = "https://brain-bzh.alive.dev/api/v1"

TOKEN = os.environ.get("ALIVE_TOKEN")
WORKSPACE_ID = os.environ.get("ALIVE_WORKSPACE_ID")
THREAD_ID = os.environ.get("ALIVE_THREAD_ID")
ENTRY_DEPLOYMENT_ID = os.environ.get("ALIVE_ENTRY_DEPLOYMENT_ID")
# Deployment DÉDIÉ au nettoyage (rm -rf), séparé de ENTRY_DEPLOYMENT_ID.
# IMPORTANT: ne jamais réutiliser ENTRY_DEPLOYMENT_ID pour ça — il a toute
# la chaîne after_deployment (LALM1, LALM2, ...) accrochée derrière lui,
# donc un simple appel de nettoyage déclencherait AUSSI toute la cascade
# coûteuse à chaque fois (le trigger se déclenche sur la fin du run, peu
# importe son contenu). Crée un deployment séparé, trigger=manual, SANS
# aucun after_deployment pointant dessus, et mets son id ici.
CLEANUP_DEPLOYMENT_ID = os.environ.get("ALIVE_CLEANUP_DEPLOYMENT_ID")

if not all([TOKEN, WORKSPACE_ID, THREAD_ID, ENTRY_DEPLOYMENT_ID]):
    sys.exit(
        "Missing env vars. Set ALIVE_TOKEN, ALIVE_WORKSPACE_ID, "
        "ALIVE_THREAD_ID, ALIVE_ENTRY_DEPLOYMENT_ID (.env ou variables d'env)."
    )

HEADERS = {"Authorization": f"Bearer {TOKEN}"}
JSON_HEADERS = {**HEADERS, "Content-Type": "application/json"}

ENTRY_PROMPT_TEMPLATE = '''Use ws_grep to locate the entry for id "{item_id}" in ./data/MMAR/MMAR-meta.json.
Do NOT use ws_read on this file — do not load the full file into context 
under any circumstance. Use only the matched line(s)/context returned by 
ws_grep to extract these fields from the matched item: question, choices, 
modality, category, sub_category, language, audio_path. Keep these values — 
you will need them again for the final JSON below, after the audio_split 
step (which returns different fields and does NOT include them).

Use audio_split skill via the bundled script ONLY:
  python skills/audio_split/scripts/split_audio.py <audio_path> ./data/artifacts/<id>/
Do NOT implement the split inline (do not write your own librosa/soundfile 
code, do not load the raw audio array yourself). Call the script above via 
a single tool call. Its output gives you ONLY: full, seg1, seg2, seg3, 
duration_seconds, segment_duration. It does NOT give you question/choices/
modality/category/sub_category/language — you must carry those over from 
the ws_grep step above, they are not part of the script's output.

Write JSON to ./data/artifacts/<id>/metadata.json, MERGING both sources 
(the ws_grep fields AND the audio_split script fields) into ONE object 
with exactly these keys — do not omit any, do not add any other keys:
{{
  "id": "{item_id}",
  "question": "<question>",
  "choices": [<4 choices>],
  "modality": "<modality>",
  "category": "<category>",
  "sub_category": "<sub_category>",
  "language": "<language>",
  "full": "./data/artifacts/<id>/audio_full.wav",
  "seg1": "./data/artifacts/<id>/audio_seg1.wav",
  "seg2": "./data/artifacts/<id>/audio_seg2.wav",
  "seg3": "./data/artifacts/<id>/audio_seg3.wav",
  "duration_seconds": <duration>,
  "segment_duration": <segment_duration>
}}

Output: JSON only, no text.

This is ONE execution pass only: one grep, one audio_split call, 
one file write. Do not repeat, retry, or make additional exploratory passes.'''


def build_entry_input(item_id: str) -> str:
    return ENTRY_PROMPT_TEMPLATE.format(item_id=item_id)


def peek_latest_execution_id(client: httpx.Client, after_msg_id: str, verbose: bool = True) -> str:
    """Un seul appel GET /messages (pas une boucle de polling complète) pour
    voir si un execution_id DIFFÉRENT de celui qu'on suit est visible côté
    plateforme, au moment précis où on détecte une boucle — avant d'annuler.
    EXPÉRIMENTAL: aucune confirmation que les messages en cours de
    génération apparaissent ici avant leur run.completed. Si ça ne renvoie
    rien d'utile, cancel_run continue avec l'id qu'on suivait déjà — ça ne
    peut pas aggraver la situation, juste ne pas l'améliorer."""
    try:
        resp = client.get(
            f"{BASE}/threads/{THREAD_ID}/messages",
            headers=HEADERS,
            params={"after": after_msg_id, "limit": 10},
            timeout=10.0,
        )
        resp.raise_for_status()
        msgs = resp.json().get("data", [])
        for m in reversed(msgs):  # le plus récent en premier
            run_info = m.get("run") or {}
            exec_id = run_info.get("execution_id")
            if exec_id:
                if verbose:
                    print(f"[peek] execution_id trouvé via GET /messages: {exec_id}")
                return exec_id
        if verbose:
            print(f"[peek] aucun nouveau message/execution_id trouvé "
                  f"({len(msgs)} message(s) inspecté(s) après {after_msg_id}).")
    except Exception as e:
        if verbose:
            print(f"[peek] échec — {e}")
    return None


def get_execution_status(client: httpx.Client, execution_id: str, verbose: bool = True) -> dict:
    """Interroge GET /executions/{execution_id} pour connaître le VRAI statut
    d'une exécution, indépendamment de ce qu'on pense savoir côté client.
    Utile pour diagnostiquer un décalage entre l'execution_id qu'on suit
    (dernier vu via run.created) et ce qui tourne réellement côté serveur —
    si ce statut est déjà terminal (completed/cancelled/error) avant même
    qu'on tente d'annuler, ça confirme que notre id est périmé plutôt que
    d'attendre un 409 ambigu après coup."""
    try:
        resp = client.get(
            f"{BASE}/executions/{execution_id}",
            headers=HEADERS,
            timeout=15.0,
        )
        if resp.status_code == 404:
            return {"status": "not_found"}
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"status": "error", "detail": str(e)}


def cancel_run(client: httpx.Client, execution_id: str, verbose: bool = True) -> bool:
    """Annule RÉELLEMENT un run en cours côté serveur. Contrairement à la
    simple fermeture de notre connexion SSE (qui ne stoppe ni la génération
    ni la facturation — confirmé par la doc de cet endpoint: "Closing the
    SSE stream does not cancel"), ceci envoie un vrai signal d'arrêt.
    Retourne 200 dès que le signal est envoyé (pas forcément déjà annulé —
    il faudrait poll GET /executions/{id} pour le statut terminal 'cancelled'
    si on veut une confirmation stricte, ce qu'on ne fait pas ici pour ne
    pas ralentir davantage un run déjà en train de dériver)."""
    try:
        resp = client.post(
            f"{BASE}/threads/{THREAD_ID}/runs/{execution_id}/cancel",
            headers=HEADERS,
            timeout=15.0,
        )
        if resp.status_code == 200:
            if verbose:
                print(f"[cancel] Signal d'annulation envoyé pour {execution_id}.")
            return True
        elif resp.status_code == 409:
            if verbose:
                print(f"[cancel] {execution_id} déjà terminé (409), rien à annuler.")
            return False
        else:
            if verbose:
                print(f"[cancel] Échec inattendu ({resp.status_code}) pour {execution_id}.")
            return False
    except Exception as e:
        if verbose:
            print(f"[cancel] ÉCHEC réseau lors de l'annulation de {execution_id} — {e}")
        return False


def run_chain_stream(client: httpx.Client, item_id: str,
                      verbose: bool = True, timeout: float = 6000.0,
                      max_step_chars: int = 6000) -> tuple:
    """Triggers the entry deployment with follow="chain" and stream=true,
    and reads the SSE connection until the WHOLE cascade finishes (not just
    the first agent). Returns (completed_events, error_events, aborted),
    where each completed_event is a dict shaped like a run.completed
    payload plus a locally-measured duration_seconds."""
    body = {
        "input": build_entry_input(item_id),
        "stream": True,
        "parent_id": None,
        "context": {"room": False, "aliveness": False, "notice": False},
        "conversation": "messages",
        "follow": "chain",
    }

    completed_events = []
    error_events = []
    created_at_by_execution = {}
    current_step_text = []
    current_step_execution_id = None
    entry_execution_id = None  # id de la TOUTE PREMIÈRE étape (kickoff),
    # jamais réinitialisé. Selon la doc mise à jour de /cancel: annuler cet
    # id stoppe TOUTE la cascade after_deployment qui en descend, même si
    # cette première étape est déjà terminée — c'est maintenant le moyen
    # le plus fiable d'annuler une boucle, peu importe quelle étape
    # exacte de la chaîne est en train de dériver.
    last_known_message_id = None  # dernier message.id vu (via run.completed),
    # utilisé pour ancrer un GET /messages ponctuel au moment d'une détection
    # de boucle (peek_latest_execution_id) — voir bloc circuit-breaker.
    awaiting_cancel_confirmation = False  # True juste après un cancel_run(),
    # tant qu'on n'a pas revu run.created/run.completed/run.error — pendant
    # ce laps de temps, l'annulation est asynchrone et des deltas résiduels
    # peuvent encore arriver pour l'étape déjà annulée. On les ignore pour
    # ne pas re-détecter une "boucle" sur ce reliquat et re-spammer des
    # messages de coupure inutiles.
    aborted = False

    if verbose:
        print(f"Triggering chain (follow=chain) for item_id={item_id!r}...")
        print("\n--- Suivi de la chaîne (SSE) ---\n")

    with client.stream(
        "POST",
        f"{BASE}/threads/{THREAD_ID}/deployments/{ENTRY_DEPLOYMENT_ID}/runs",
        headers=JSON_HEADERS,
        json=body,
        timeout=timeout,
    ) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if not line or not line.startswith("data:"):
                continue
            raw = line[len("data:"):].strip()
            if raw == "[DONE]":
                break
            event = json.loads(raw)
            etype = event.get("type")

            if etype == "run.created":
                exec_id = event.get("execution_id")
                created_at_by_execution[exec_id] = time.monotonic()
                current_step_text = []
                current_step_execution_id = exec_id
                if entry_execution_id is None:
                    entry_execution_id = exec_id  # capturé UNE SEULE fois, jamais réécrasé
                awaiting_cancel_confirmation = False  # nouvelle étape confirmée, on reprend la surveillance normale
                if verbose:
                    print(f"[run.created] execution_id={exec_id}")

            elif etype == "run.started":
                # Nouvel événement (mise à jour serveur suite à notre
                # signalement): porte l'execution_id des étapes ENFANTS de
                # la cascade after_deployment — ce que run.created ne
                # donnait jamais pour ces étapes. On le traite comme un
                # marqueur de nouvelle étape, au même titre que run.created.
                exec_id = event.get("execution_id")
                if exec_id:
                    created_at_by_execution[exec_id] = time.monotonic()
                    current_step_text = []
                    current_step_execution_id = exec_id
                    awaiting_cancel_confirmation = False
                    if verbose:
                        print(f"[run.started] execution_id={exec_id} (étape enfant de la cascade)")

            elif etype == "run.node":
                if verbose:
                    node = event.get("node")
                    data = event.get("data")
                    if data:
                        print(f"[step] {node}: {json.dumps(data, ensure_ascii=False)[:200]}")
                    else:
                        print(f"[step] {node}")

            elif etype == "run.output_text.delta":
                if awaiting_cancel_confirmation:
                    # Reliquat post-annulation (asynchrone) — on l'ignore
                    # silencieusement, pas de comptage ni de re-détection,
                    # en attendant confirmation que l'étape est vraiment finie.
                    continue

                delta = event.get("delta", "")
                current_step_text.append(delta)
                if verbose:
                    print(delta, end="", flush=True)

                full_text_so_far = "".join(current_step_text)
                total_len = len(full_text_so_far)

                repetition_detected = False
                if total_len > 600:
                    tail = full_text_so_far[-300:]
                    if full_text_so_far[:-300].count(tail[:150]) >= 1:
                        repetition_detected = True

                if total_len > max_step_chars or repetition_detected:
                    aborted = True
                    reason = "répétition détectée" if repetition_detected else f"{max_step_chars} caractères dépassés"
                    if verbose:
                        print(f"\n\n[circuit-breaker] Étape en cours interrompue ({reason}) — "
                              f"signe probable de boucle/rumination de génération.")
                    # Annulation RÉELLE côté serveur. Suite à la mise à jour
                    # de Jean-Charles: annuler entry_execution_id (le tout
                    # premier id de la chaîne) stoppe DÉSORMAIS toute la
                    # cascade after_deployment qui en descend, même si
                    # cette première étape est elle-même déjà terminée.
                    # C'est la méthode la plus fiable — on n'a plus besoin
                    # de deviner l'id exact de l'étape en cours de dérive.
                    ids_to_cancel = set()
                    if entry_execution_id:
                        ids_to_cancel.add(entry_execution_id)
                    if current_step_execution_id:
                        ids_to_cancel.add(current_step_execution_id)

                    # Conservé en filet de sécurité supplémentaire, au cas
                    # où entry_execution_id ne couvrirait pas 100% des cas.
                    if last_known_message_id:
                        peeked_id = peek_latest_execution_id(client, last_known_message_id, verbose=verbose)
                        if peeked_id and peeked_id not in ids_to_cancel:
                            if verbose:
                                print(f"[peek] execution_id supplémentaire trouvé via "
                                      f"GET /messages — ajouté à l'annulation.")
                            ids_to_cancel.add(peeked_id)

                    for eid in ids_to_cancel:
                        # Diagnostic AVANT d'annuler: si le statut est déjà
                        # terminal, cet id est probablement périmé — preuve
                        # automatique plutôt que de reconstituer après coup.
                        status_check = get_execution_status(client, eid, verbose=verbose)
                        real_status = status_check.get("status")
                        if verbose:
                            print(f"[diagnostic] Statut réel de {eid} avant annulation: {real_status!r}")
                        if real_status in ("completed", "cancelled", "error", "not_found"):
                            if verbose:
                                print(f"[diagnostic][SUSPECT] {eid} déjà terminal ({real_status}) "
                                      f"alors que du texte continue d'arriver pour cette étape.")
                        cancel_run(client, eid, verbose=verbose)
                    # On NE FERME PAS notre connexion SSE — on continue
                    # d'écouter sur le même flux. L'annulation est
                    # asynchrone (la doc dit juste "signal dispatché", pas
                    # "arrêté") — on passe en mode "attente de confirmation"
                    # pour ignorer les deltas résiduels de cette étape et ne
                    # pas re-déclencher sur ce reliquat.
                    current_step_text = []
                    current_step_execution_id = None
                    awaiting_cancel_confirmation = True
                    if verbose:
                        print("[circuit-breaker] En attente de confirmation d'arrêt "
                              "(reliquat de génération ignoré)...")
                    continue

            elif etype == "run.completed":
                awaiting_cancel_confirmation = False  # étape confirmée terminée
                exec_id = event.get("execution_id")
                started = created_at_by_execution.get(exec_id)
                event["_measured_duration_seconds"] = (
                    round(time.monotonic() - started, 1) if started is not None else None
                )
                completed_events.append(event)
                if event.get("id"):
                    last_known_message_id = event["id"]
                if verbose:
                    preview = (event.get("content") or "")[:150].replace("\n", " ")
                    extra_fields = {k: v for k, v in event.items()
                                     if k not in ("type", "content", "_measured_duration_seconds")}
                    print(f"\n[run.completed] {json.dumps(extra_fields, ensure_ascii=False)}")
                    print(f"  content: {preview}")
                # IMPORTANT: on "clôture" le suivi de cette étape dès qu'elle
                # se termine normalement — sans ça, si des deltas résiduels
                # arrivent avant le prochain run.created (artefact réseau,
                # chevauchement côté plateforme...), on les collerait sur cet
                # execution_id déjà terminé, et une éventuelle annulation
                # tenterait à tort d'annuler un id déjà fini (→ 409 trompeur,
                # comme observé).
                if exec_id == current_step_execution_id:
                    current_step_execution_id = None
                    current_step_text = []

            elif etype == "run.error":
                awaiting_cancel_confirmation = False  # étape confirmée terminée (en erreur)
                exec_id = event.get("execution_id")
                started = created_at_by_execution.get(exec_id)
                event["_measured_duration_seconds"] = (
                    round(time.monotonic() - started, 1) if started is not None else None
                )
                error_events.append(event)
                if verbose:
                    print(f"\n[run.error] execution_id={exec_id}: {event.get('error')}")
                if exec_id == current_step_execution_id:
                    current_step_execution_id = None
                    current_step_text = []

    return completed_events, error_events, aborted


def poll_fallback(client: httpx.Client, resume_after_msg_id: str,
                   already_seen_ids: set, quiet_period: float = 240.0,
                   max_wait: float = 500.0, poll_interval: float = 2.0,
                   verbose: bool = True) -> list:
    """Filet de sécurité si la connexion SSE se coupe avant [DONE]."""
    recovered = []
    cursor = resume_after_msg_id
    last_new_msg_time = time.monotonic()
    started = time.monotonic()

    if verbose:
        print(f"\n[fallback] Connexion SSE terminée sans réponse finale détectée — "
              f"reprise par polling depuis {cursor}...\n")

    while True:
        elapsed = time.monotonic() - started
        if elapsed > max_wait:
            if verbose:
                print(f"\n[fallback][timeout] max_wait ({max_wait}s) dépassé, arrêt.")
            break

        try:
            resp = client.get(
                f"{BASE}/threads/{THREAD_ID}/messages",
                headers=HEADERS,
                params={"after": cursor, "limit": 50},
                timeout=30.0,
            )
            resp.raise_for_status()
            new_msgs = resp.json().get("data", [])
        except httpx.TimeoutException:
            if verbose:
                print("[fallback][warning] timeout réseau sur ce poll, on réessaie...")
            time.sleep(poll_interval)
            continue

        if new_msgs:
            for m in new_msgs:
                if m["id"] in already_seen_ids:
                    continue
                run_info = m.get("run") or {}
                event = {
                    "id": m["id"],
                    "execution_id": run_info.get("execution_id"),
                    "content": m.get("content", ""),
                    "credits_used": run_info.get("credits_used"),
                    "tokens": run_info.get("tokens"),
                    "_measured_duration_seconds": None,
                }
                recovered.append(event)
                already_seen_ids.add(m["id"])
                if verbose:
                    preview = (m.get("content") or "")[:150].replace("\n", " ")
                    print(f"[fallback][msg {m['id']}] credits_used={event['credits_used']}: {preview}")
            cursor = new_msgs[-1]["id"]
            last_new_msg_time = time.monotonic()
        else:
            quiet_for = time.monotonic() - last_new_msg_time
            if quiet_for > quiet_period:
                if verbose:
                    print(f"\n[fallback][stable] Aucun nouveau message depuis "
                          f"{quiet_for:.0f}s, on considère la chaîne terminée.")
                break

        time.sleep(poll_interval)

    return recovered


def parse_answer_and_cot(content: str) -> dict:
    stripped = re.sub(r"^```(?:json)?\s*|\s*```$", "", content.strip(), flags=re.MULTILINE)
    try:
        obj = json.loads(stripped)
        answer = obj.get("answer") or obj.get("selected_answer")
        reasoning = obj.get("reasoning")
        if isinstance(reasoning, dict):
            cot = "\n\n".join(f"{k}: {v}" for k, v in reasoning.items())
        elif isinstance(reasoning, str):
            cot = reasoning
        else:
            cot = None
        if answer is not None:
            return {
                "answer": answer,
                "confidence": obj.get("confidence"),
                "chain_of_thought": cot,
                "raw_content": content,
            }
    except json.JSONDecodeError:
        pass

    answer_match = re.search(r"(?:Answer|Final Answer)\s*[:\-]\s*(.+?)(?:\n\n|\Z)",
                              content, re.IGNORECASE | re.DOTALL)
    cot_match = re.search(r"(?:Chain of Thought|CoT|Reasoning)\s*[:\-]\s*(.+)",
                           content, re.IGNORECASE | re.DOTALL)
    if answer_match:
        return {
            "answer": answer_match.group(1).strip(),
            "confidence": None,
            "chain_of_thought": cot_match.group(1).strip() if cot_match else None,
            "raw_content": content,
        }
    return None


def find_final_answer(events: list, entry_fallback: dict, verbose: bool = True) -> tuple:
    """Scans completed events for real Reasoning Generator outputs."""
    candidates = []
    for e in events:
        content = e.get("content", "")
        stripped = re.sub(r"^```(?:json)?\s*|\s*```$", "", content.strip(), flags=re.MULTILINE)
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(obj.get("reasoning"), dict) and obj.get("answer") is not None:
            parsed = parse_answer_and_cot(content)
            if parsed:
                candidates.append((e, parsed))

    if not candidates:
        parsed = parse_answer_and_cot(entry_fallback.get("content", ""))
        return (parsed, 0)

    if len(candidates) > 1 and verbose:
        print(f"\n[warning] {len(candidates)} sorties de Reasoning Generator détectées "
              f"dans ce run (attendu: 1) — inattendu avec follow=chain, à investiguer. "
              f"On garde la DERNIÈRE.")
        for e, p in candidates:
            print(f"    - {e.get('execution_id')}: answer={p['answer']}, confidence={p.get('confidence')}")

    return (candidates[-1][1], len(candidates))


def cleanup_artifacts(client: httpx.Client, item_id: str, verbose: bool = True) -> bool:
    """Supprime ./data/artifacts/<item_id>/ avant de relancer un run sur le
    même item. Nécessaire car les chemins de fichiers sont déterministes
    (basés sur l'item_id, pas sur le run) — sans ce nettoyage, un run qui
    échoue à écrire un fichier laisse l'étape suivante lire le fichier
    laissé par un run précédent, ce qui fausse silencieusement les mesures
    de variance (et la justesse) d'un batch de tests répétés sur le même item.

    Passe par un run minimal et non streamé sur CLEANUP_DEPLOYMENT_ID (pas
    ENTRY_DEPLOYMENT_ID — voir l'avertissement au chargement du module) qui
    exécute la suppression via ws_bash (pas d'endpoint API dédié à la
    suppression de fichiers de workspace dans la doc actuelle)."""
    deployment_id = CLEANUP_DEPLOYMENT_ID
    if not deployment_id:
        if verbose:
            print("\n[cleanup][ATTENTION] ALIVE_CLEANUP_DEPLOYMENT_ID non défini — "
                  "utilisation de ENTRY_DEPLOYMENT_ID comme repli. Ceci va AUSSI "
                  "déclencher toute la chaîne after_deployment (LALM1, LALM2, ...) "
                  "à chaque nettoyage, en plus du vrai run qui suit juste après: "
                  "coût potentiellement doublé sur chaque itération, invisible dans "
                  "ces stats. Crée un deployment séparé sans trigger en aval et "
                  "mets son id dans ALIVE_CLEANUP_DEPLOYMENT_ID dès que possible.")
        deployment_id = ENTRY_DEPLOYMENT_ID

    cleanup_input = (
        f'Use ws_bash to run exactly this command: '
        f'rm -rf ./data/artifacts/{item_id}/ . '
        f'Do not do anything else — no file read, no analysis, no other tool call. '
        f'Output only: cleaned'
    )
    body = {
        "input": cleanup_input,
        "stream": False,
        "parent_id": None,
        "context": {"room": False, "aliveness": False, "notice": False},
        "conversation": "messages",
    }
    if verbose:
        print(f"\n[cleanup] Suppression de ./data/artifacts/{item_id}/ avant le run...")
    try:
        resp = client.post(
            f"{BASE}/threads/{THREAD_ID}/deployments/{deployment_id}/runs",
            headers=JSON_HEADERS,
            json=body,
            timeout=120,
        )
        resp.raise_for_status()
        result = resp.json()
        content = (result.get("content") or "").strip().lower()
        success = "clean" in content
        if verbose:
            print(f"[cleanup] {'OK' if success else 'incertain'} — content: {content[:100]!r}")
        return success
    except Exception as e:
        if verbose:
            print(f"[cleanup] ÉCHEC — {e}")
        return False


def run_one_item(client: httpx.Client, item_id: str,
                  verbose: bool = True, timeout: float = 1800.0,
                  enable_fallback: bool = True) -> dict:
    """End-to-end: trigger the chain (follow=chain), collect every step's
    run.completed, parse the final answer, and sum the real per-step cost."""
    run_started_at = datetime.now(timezone.utc)

    completed_events, error_events, aborted_by_circuit_breaker = run_chain_stream(
        client, item_id, verbose=verbose, timeout=timeout,
    )
    run_ended_at = datetime.now(timezone.utc)

    if not completed_events:
        return {
            "thread_id": THREAD_ID,
            "entry_deployment_id": ENTRY_DEPLOYMENT_ID,
            "item_id": item_id,
            "n_chain_steps": 0,
            "n_errors": len(error_events),
            "n_reasoning_outputs_detected": 0,
            "used_polling_fallback": False,
            "aborted_by_circuit_breaker": aborted_by_circuit_breaker,
            "total_credits_used": None,
            "total_measured_duration_seconds": 0.0,
            "total_tokens": {"prompt": 0, "completion": 0, "reasoning": 0},
            "per_step_credits_used": [],
            "run_started_at": run_started_at.isoformat(),
            "run_ended_at": run_ended_at.isoformat(),
            "answer": None, "confidence": None, "chain_of_thought": None,
            "raw_content": None,
            "errors": error_events,
        }

    entry_event = completed_events[0]
    parsed, n_final_candidates = find_final_answer(completed_events, entry_event, verbose=verbose)

    # Si le SSE s'est terminé sans qu'on ait trouvé une vraie réponse finale
    # (Reasoning Generator), la connexion a probablement été coupée avant la
    # fin réelle de la chaîne côté serveur — on tente une reprise par polling.
    # NOTE: ce fallback est déclenché même après un abort du circuit-breaker
    # (choix assumé) — la chaîne continue de tourner côté serveur qu'on
    # coupe ou non notre SSE, donc autant essayer de récupérer la suite.
    # Limite connue: le fallback n'est pas scopé par run, un mélange avec
    # les messages d'un autre run dans la même room reste possible.
    used_fallback = False
    if n_final_candidates == 0 and enable_fallback:
        already_seen_ids = {e.get("id") for e in completed_events if e.get("id")}
        resume_after = completed_events[-1].get("id")
        if resume_after:
            recovered = poll_fallback(
                client, resume_after_msg_id=resume_after,
                already_seen_ids=already_seen_ids, verbose=verbose,
            )
            if recovered:
                used_fallback = True
                completed_events.extend(recovered)
                parsed, n_final_candidates = find_final_answer(
                    completed_events, entry_event, verbose=verbose,
                )

    if parsed is None:
        parsed = {"answer": None, "confidence": None, "chain_of_thought": None,
                  "raw_content": completed_events[-1].get("content", "")}

    per_step_credits = [
        {
            "execution_id": e.get("execution_id"),
            "credits_used": e.get("credits_used"),
            "duration_seconds": e.get("_measured_duration_seconds"),
            "tokens": e.get("tokens"),
        }
        for e in completed_events
    ]
    total_credits = sum((e.get("credits_used") or 0) for e in completed_events)
    total_duration = sum((e.get("_measured_duration_seconds") or 0) for e in completed_events)

    total_prompt_tokens = sum((e.get("tokens") or {}).get("prompt") or 0 for e in completed_events)
    total_completion_tokens = sum((e.get("tokens") or {}).get("completion") or 0 for e in completed_events)
    total_reasoning_tokens = sum((e.get("tokens") or {}).get("reasoning") or 0 for e in completed_events)

    return {
        "thread_id": THREAD_ID,
        "entry_deployment_id": ENTRY_DEPLOYMENT_ID,
        "item_id": item_id,
        "n_chain_steps": len(completed_events),
        "n_errors": len(error_events),
        "n_reasoning_outputs_detected": n_final_candidates,
        "used_polling_fallback": used_fallback,
        "aborted_by_circuit_breaker": aborted_by_circuit_breaker,
        "total_credits_used": total_credits,
        "total_measured_duration_seconds": round(total_duration, 1),
        "total_tokens": {
            "prompt": total_prompt_tokens,
            "completion": total_completion_tokens,
            "reasoning": total_reasoning_tokens,
        },
        "per_step_credits_used": per_step_credits,
        "run_started_at": run_started_at.isoformat(),
        "run_ended_at": run_ended_at.isoformat(),
        "errors": error_events,
        **parsed,
    }