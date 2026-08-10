"""Fail-closed loader for the Phase 4 three-source registry."""
from __future__ import annotations
import copy, hashlib, json
from pathlib import Path

class RegistryError(ValueError):
    pass

def _canonical_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")

def registry_sha(data):
    payload = copy.deepcopy(data); payload["registry_sha"] = ""
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()

def load_registry(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if data.get("schema_version") != "1.0" or not isinstance(data.get("sources"), list):
        raise RegistryError("invalid registry schema")
    actual = registry_sha(data)
    if data.get("registry_sha") != actual:
        raise RegistryError(f"registry sha mismatch: declared={data.get('registry_sha')} actual={actual}")
    ids = [item.get("source_id") for item in data["sources"]]
    if len(ids) != len(set(ids)) or not all(ids): raise RegistryError("source ids must be unique and non-empty")
    return data

def resolve_source(data, *, source_id=None, sender=None):
    candidates = data["sources"]
    if source_id: candidates = [x for x in candidates if x["source_id"] == source_id]
    if sender:
        sender = sender.lower(); candidates = [x for x in candidates if sender in {s.lower() for s in x["senders"]}]
    if not candidates: raise RegistryError("HELD_UNKNOWN_SOURCE")
    if len(candidates) != 1: raise RegistryError("HELD_AMBIGUOUS_SOURCE")
    return candidates[0]

def canonical_identity(source, item):
    if source.get("episode_key") and item.get("episode_key"):
        return f"{source['source_id']}:episode:{item['episode_key']}"
    if not item.get("gmail_message_id"): raise RegistryError("HELD_LINEAGE_MISSING")
    return f"{source['source_id']}:gmail:{item['gmail_message_id']}"
