"""Bounded Phase 4 Gmail-side dry-run; never calls a mutating API."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from newsletter_registry import RegistryError, canonical_identity, load_registry, resolve_source

SIDE_EFFECTS = {k: 0 for k in (
    "notion_page_create", "notion_block_update", "source_status_update",
    "onedrive_write", "file_move_delete", "production_ledger_write",
    "downstream_dispatch", "translation_call", "daily_send", "weekly_send",
    "newsflow_send", "schedule_config_runtime_change")}

def _body_sha(body):
    return hashlib.sha256(body.encode("utf-8")).hexdigest() if body else ""

def collect_gmail_items(*, registry_path, source_ids, max_emails, lookback_days, message_ids=None):
    """Read bounded Gmail metadata/body only; never imports a Notion writer."""
    registry = load_registry(registry_path)
    senders = [sender for source in registry["sources"] if source["source_id"] in source_ids for sender in source["senders"]]
    query = "from:(" + " OR ".join(senders) + f") newer_than:{int(lookback_days)}d"
    from sync_substack import get_gmail_service, get_emails, normalized_sender_email
    emails = get_emails(get_gmail_service(), query, max_results=int(max_emails), message_ids=set(message_ids or []))
    rows = []
    for email in emails:
        sender = normalized_sender_email(email.get("from", ""))
        source = resolve_source(registry, sender=sender)
        rows.append({"source_id": source["source_id"], "sender": sender,
                     "gmail_message_id": email.get("id"), "subject": email.get("subject", ""),
                     "received_at": email.get("date") or email.get("internal_date"),
                     "body": email.get("body_text") or email.get("body_html") or ""})
    return rows

def run_dry_run(items, *, registry_path, source_ids, run_manifest_id, manifest_sha,
                registry_sha_expected, repo_sha, mutator_spies=None):
    if set(source_ids) != {"citrini", "capitalflow", "sleepy"}:
        raise ValueError("source_ids must be exactly citrini,capitalflow,sleepy")
    registry = load_registry(registry_path)
    for name, spy in (mutator_spies or {}).items():
        if getattr(spy, "call_count", 0):
            raise AssertionError(f"dry-run mutator called: {name}")
    if registry["registry_sha"] != registry_sha_expected:
        raise RegistryError("registry sha mismatch against manifest")
    groups = {}
    rows = []
    for item in items:
        try:
            source = resolve_source(registry, source_id=item.get("source_id"), sender=item.get("sender"))
            identity = canonical_identity(source, item)
            body = (item.get("body") or "").strip()
            candidate = {"item": item, "source": source, "identity": identity,
                         "body": body, "body_sha256": _body_sha(body)}
            groups.setdefault(identity, []).append(candidate)
        except RegistryError as exc:
            rows.append({"source_id": item.get("source_id"), "gmail_message_id": item.get("gmail_message_id"),
                         "terminal_state": str(exc), "extraction_count": 0, "route_consumption_count": 0})
    for identity, candidates in groups.items():
        winner = sorted(candidates, key=lambda x: (bool(x["body"]), x["body_sha256"], x["item"].get("gmail_message_id", "")), reverse=True)[0]
        for candidate in candidates:
            item, source = candidate["item"], candidate["source"]
            is_winner = candidate is winner
            admitted = is_winner and bool(candidate["body"])
            state = "ADMITTED" if admitted else ("HELD_PREVIEW_OR_EMPTY" if is_winner else "DUPLICATE")
            rows.append({"source_id": source["source_id"], "gmail_message_id": item.get("gmail_message_id"),
                         "identity": identity, "body_sha256": candidate["body_sha256"],
                         "completeness": "substantive" if candidate["body"] else "preview_or_empty",
                         "terminal_state": state, "extraction_count": int(admitted),
                         "route_consumption_count": 0})
    counts = {state: sum(1 for row in rows if row["terminal_state"] == state) for state in
              ("ADMITTED", "HELD_PREVIEW_OR_EMPTY", "HELD_UNKNOWN_SOURCE", "HELD_AMBIGUOUS_SOURCE", "HELD_LINEAGE_MISSING", "EXCLUDED", "DUPLICATE", "FAILED")}
    return {"schema_version": "phase4-dry-run-2", "stage": "substack", "run_manifest_id": run_manifest_id,
            "manifest_sha": manifest_sha, "registry_sha": registry["registry_sha"],
            "repo_sha": repo_sha, "discovered": len(items), "counts": counts,
            "extraction_count": sum(row["extraction_count"] for row in rows),
            "route_consumption_count": 0, "rows": rows, "side_effects": SIDE_EFFECTS.copy(),
            "no_send": True, "no_write": True}

def write_isolated_receipt(receipt, path):
    target = Path(path).resolve()
    if "newsletter-phase4" not in str(target) and target.parent != Path("/tmp"):
        raise ValueError("receipt path must be an isolated Phase 4 artifact path")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-json"); parser.add_argument("--from-gmail", action="store_true"); parser.add_argument("--output", required=True)
    parser.add_argument("--max-emails", type=int, default=50); parser.add_argument("--lookback-days", type=int, default=21); parser.add_argument("--message-ids", default="")
    parser.add_argument("--manifest-id", required=True); parser.add_argument("--manifest-sha", required=True)
    parser.add_argument("--registry-sha", required=True); parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--source-ids", default="citrini,capitalflow,sleepy")
    args = parser.parse_args()
    if bool(args.input_json) == args.from_gmail: parser.error("choose exactly one of --input-json or --from-gmail")
    data = (json.loads(Path(args.input_json).read_text(encoding="utf-8")) if args.input_json else
            collect_gmail_items(registry_path=Path(__file__).with_name("newsletter_registry.json"), source_ids=args.source_ids.split(","), max_emails=args.max_emails, lookback_days=args.lookback_days, message_ids=[x for x in args.message_ids.split(",") if x]))
    receipt = run_dry_run(data, registry_path=Path(__file__).with_name("newsletter_registry.json"),
                          source_ids=args.source_ids.split(","), run_manifest_id=args.manifest_id,
                          manifest_sha=args.manifest_sha, registry_sha_expected=args.registry_sha, repo_sha=args.repo_sha)
    write_isolated_receipt(receipt, args.output)

if __name__ == "__main__": main()
