"""Bounded Gmail header-only probes for the frozen Phase 4 coverage gate.

This module intentionally never requests Gmail message bodies or attachments.
Each probe is an independent exact-subject query with a fixed 21-day window and
maximum of ten metadata messages. It emits only isolated receipt metadata.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
from pathlib import Path

from newsletter_registry import load_registry, resolve_source


SIDE_EFFECTS = {k: 0 for k in (
    "notion_page_create", "notion_block_update", "source_status_update",
    "onedrive_write", "file_move_delete", "production_ledger_write",
    "downstream_dispatch", "translation_call", "daily_send", "weekly_send",
    "newsflow_send", "schedule_config_runtime_change")}

PROBES = {
    "citrini": "Protection Matters",
    "capitalflow": "Capital Flows Livestream Tomorrow",
    "sleepy": "Software is going nuclear",
}
PROBE_LOOKBACK_DAYS = 21
PROBE_MAX_RESULTS = 10
METADATA_HEADERS = ("From", "Message-ID", "Subject", "Date")


def _sha256(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8", errors="ignore")).hexdigest()


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value or "")).strip().casefold()


def _normalized_sender(value: str) -> str:
    match = re.search(r"<([^>]+)>", value or "")
    return (match.group(1) if match else (value or "")).strip().lower()


def _header_map(headers):
    return {str(h.get("name", "")).casefold(): str(h.get("value", ""))
            for h in headers or [] if h.get("name")}


def _query(subject: str, lookback_days: int) -> str:
    escaped = subject.replace('"', '\\"')
    return f'subject:"{escaped}" newer_than:{int(lookback_days)}d'


def _metadata_match(*, source_id, expected_subject, message_id, headers):
    values = _header_map(headers)
    subject = html.unescape(values.get("subject", ""))
    sender = _normalized_sender(values.get("from", ""))
    gmail_id = str(message_id or "")
    external_id = values.get("message-id", "").strip()
    date = values.get("date", "").strip()
    exact_subject = _norm(subject) == _norm(expected_subject)
    required_present = bool(sender and gmail_id and external_id and subject and date)
    if not exact_subject:
        terminal = "EXCLUDED"
    elif not required_present:
        terminal = "HELD_LINEAGE_MISSING"
    else:
        terminal = "ADMITTED"
    return {
        "source_id": source_id,
        "sender": sender,
        "subject_sha256": _sha256(subject),
        "date": date,
        "gmail_message_id_sha256": _sha256(gmail_id),
        "message_id_header_sha256": _sha256(external_id),
        "terminal_state": terminal,
        "body_requested": False,
        "attachments_requested": False,
    }


def run_header_probe(*, registry_path, service, run_manifest_id, manifest_sha,
                     registry_sha_expected, repo_sha,
                     lookback_days=PROBE_LOOKBACK_DAYS,
                     max_results=PROBE_MAX_RESULTS):
    if int(lookback_days) != PROBE_LOOKBACK_DAYS:
        raise ValueError("header probe lookback is fixed at 21 days")
    if int(max_results) != PROBE_MAX_RESULTS:
        raise ValueError("header probe max results is fixed at 10 per source")
    registry = load_registry(registry_path)
    if registry["registry_sha"] != registry_sha_expected:
        raise ValueError("registry sha mismatch against manifest")

    probes = []
    for source_id, subject in PROBES.items():
        query = _query(subject, lookback_days)
        listed = service.users().messages().list(
            userId="me", q=query, maxResults=max_results
        ).execute().get("messages", [])
        matches = []
        for message in listed[:max_results]:
            message_id = message.get("id", "")
            metadata = service.users().messages().get(
                userId="me", id=message_id, format="metadata",
                metadataHeaders=list(METADATA_HEADERS)
            ).execute()
            headers = metadata.get("payload", {}).get("headers", [])
            matches.append(_metadata_match(
                source_id=source_id, expected_subject=subject,
                message_id=message_id, headers=headers
            ))
        exact = [m for m in matches if m["terminal_state"] != "EXCLUDED"]
        if len(exact) == 0:
            terminal = "TRUE_NO_RECENT_MAIL"
        elif len(exact) == 1 and exact[0]["terminal_state"] == "ADMITTED":
            terminal = "ADMITTED"
        elif len(exact) == 1:
            terminal = exact[0]["terminal_state"]
        else:
            terminal = "HELD_AMBIGUOUS_SOURCE"
        probes.append({
            "source_id": source_id,
            "expected_subject": subject,
            "query": query,
            "lookback_days": lookback_days,
            "max_results": max_results,
            "listed_count": len(listed),
            "match_count": len(exact),
            "matches": matches,
            "terminal_state": terminal,
        })

    return {
        "schema_version": "phase4-header-probe-1",
        "stage": "substack-header-only",
        "run_manifest_id": run_manifest_id,
        "manifest_sha": manifest_sha,
        "registry_sha": registry["registry_sha"],
        "repo_sha": repo_sha,
        "probe_subjects": PROBES.copy(),
        "metadata_headers": list(METADATA_HEADERS),
        "body_requested": False,
        "attachments_requested": False,
        "probes": probes,
        "side_effects": SIDE_EFFECTS.copy(),
        "no_send": True,
        "no_write": True,
        "route_consumption_count": 0,
    }


def write_isolated_receipt(receipt, path):
    target = Path(path).resolve()
    if "newsletter-phase4" not in str(target) and target.parent != Path("/tmp"):
        raise ValueError("receipt path must be an isolated Phase 4 artifact path")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--manifest-id", required=True)
    parser.add_argument("--manifest-sha", required=True)
    parser.add_argument("--registry-sha", required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--lookback-days", type=int, default=PROBE_LOOKBACK_DAYS)
    parser.add_argument("--max-results", type=int, default=PROBE_MAX_RESULTS)
    args = parser.parse_args()

    from sync_substack import get_gmail_service
    receipt = run_header_probe(
        registry_path=Path(__file__).with_name("newsletter_registry.json"),
        service=get_gmail_service(), run_manifest_id=args.manifest_id,
        manifest_sha=args.manifest_sha, registry_sha_expected=args.registry_sha,
        repo_sha=args.repo_sha, lookback_days=args.lookback_days,
        max_results=args.max_results,
    )
    write_isolated_receipt(receipt, args.output)


if __name__ == "__main__":
    main()
