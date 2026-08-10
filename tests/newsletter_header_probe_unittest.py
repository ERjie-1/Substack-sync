import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from newsletter_header_probe import (  # noqa: E402
    METADATA_HEADERS,
    PROBE_LOOKBACK_DAYS,
    PROBE_MAX_RESULTS,
    PROBES,
    run_header_probe,
    write_isolated_receipt,
)
from newsletter_registry import load_registry, registry_sha  # noqa: E402


ROOT = Path(__file__).parents[1]
REGISTRY = ROOT / "newsletter_registry.json"
REGISTRY_SHA = "eb2f5e0f6d1f58232cd99dc96e4b9b6dcada2f7976f4f13df39caf84368fbe85"


def headers(sender, subject, *, message_id="<message@example>", date="Mon, 10 Aug 2026 10:00:00 +0000"):
    values = [
        {"name": "From", "value": sender},
        {"name": "Message-ID", "value": message_id},
        {"name": "Subject", "value": subject},
        {"name": "Date", "value": date},
    ]
    return values


def default_records():
    return {
        "gmail-citrini-1": headers("Citrini <citrini@substack.com>", "Protection Matters"),
        "gmail-capitalflow-1": headers("Capital Flows <capitalflows@substack.com>", "Capital Flows Livestream Tomorrow"),
        "gmail-sleepy-1": headers("SleepySol <sleepysol@substack.com>", "Software is going nuclear"),
    }


class FakeExecute:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class FakeMessages:
    def __init__(self, records=None, query_ids=None):
        self.records = records or default_records()
        self.query_ids = query_ids or {
            "Protection Matters": ["gmail-citrini-1"],
            "Capital Flows Livestream Tomorrow": ["gmail-capitalflow-1"],
            "Software is going nuclear": ["gmail-sleepy-1"],
        }
        self.list_calls = []
        self.get_calls = []

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        ids = []
        for subject, candidates in self.query_ids.items():
            if subject.casefold() in kwargs["q"].casefold():
                ids = candidates
                break
        return FakeExecute({"messages": [{"id": item} for item in ids]})

    def get(self, **kwargs):
        self.get_calls.append(kwargs)
        return FakeExecute({"payload": {"headers": self.records[kwargs["id"]]}})


class FakeUsers:
    def __init__(self, messages):
        self._messages = messages

    def messages(self):
        return self._messages


class FakeService:
    def __init__(self, messages):
        self._users = FakeUsers(messages)

    def users(self):
        return self._users


class HeaderProbeTests(unittest.TestCase):
    def setUp(self):
        self.kw = dict(
            registry_path=REGISTRY,
            run_manifest_id="probe-1",
            manifest_sha="manifest-sha",
            registry_sha_expected=REGISTRY_SHA,
            repo_sha="head-sha",
        )

    def test_registry_and_fixed_bounds(self):
        self.assertEqual(registry_sha(load_registry(REGISTRY)), REGISTRY_SHA)
        self.assertEqual(set(PROBES), {"citrini", "capitalflow", "sleepy"})
        self.assertEqual(PROBE_LOOKBACK_DAYS, 21)
        self.assertEqual(PROBE_MAX_RESULTS, 10)

    def test_exact_subject_probes_compare_declared_sender(self):
        messages = FakeMessages()
        out = run_header_probe(service=FakeService(messages), **self.kw)
        self.assertEqual(len(out["probes"]), 3)
        self.assertTrue(all(p["terminal_state"] == "SENDER_MATCH" for p in out["probes"]))
        self.assertTrue(all(p["matches"][0]["sender_match"] for p in out["probes"]))
        self.assertTrue(all(call["format"] == "metadata" for call in messages.get_calls))
        self.assertTrue(all(call["metadataHeaders"] == list(METADATA_HEADERS) for call in messages.get_calls))
        self.assertFalse(out["body_requested"])
        self.assertFalse(out["attachments_requested"])
        self.assertTrue(all(v == 0 for v in out["side_effects"].values()))
        self.assertTrue(out["no_write"] and out["no_send"])

    def test_zero_exact_subject_is_query_scope_unverified(self):
        messages = FakeMessages(query_ids={s: [] for s in PROBES.values()})
        out = run_header_probe(service=FakeService(messages), **self.kw)
        self.assertTrue(all(p["terminal_state"] == "UNVERIFIED_NO_EXACT_SUBJECT_MATCH" for p in out["probes"]))

    def test_sender_mismatch_is_not_admitted(self):
        records = default_records()
        records["gmail-capitalflow-1"] = headers("wrong@example.com", "Capital Flows Livestream Tomorrow")
        out = run_header_probe(service=FakeService(FakeMessages(records)), **self.kw)
        capitalflow = out["probes"][1]
        self.assertEqual(capitalflow["terminal_state"], "SENDER_MISMATCH")
        self.assertFalse(capitalflow["matches"][0]["sender_match"])

    def test_missing_or_invalid_from_fails_closed(self):
        records = default_records()
        records["gmail-citrini-1"] = headers("not-an-email", "Protection Matters")
        out = run_header_probe(service=FakeService(FakeMessages(records)), **self.kw)
        citrini = out["probes"][0]
        self.assertEqual(citrini["terminal_state"], "HELD_LINEAGE_MISSING")
        self.assertEqual(citrini["matches"][0]["observed_sender"], "")

    def test_multiple_exact_subject_matches_are_ambiguous(self):
        records = default_records()
        records["gmail-citrini-2"] = headers("citrini@substack.com", "Protection Matters", message_id="<message2@example>")
        ids = {"Protection Matters": ["gmail-citrini-1", "gmail-citrini-2"],
               "Capital Flows Livestream Tomorrow": ["gmail-capitalflow-1"],
               "Software is going nuclear": ["gmail-sleepy-1"]}
        out = run_header_probe(service=FakeService(FakeMessages(records, ids)), **self.kw)
        self.assertEqual(out["probes"][0]["terminal_state"], "HELD_AMBIGUOUS_SOURCE")

    def test_fixed_bounds_reject_expansion(self):
        with self.assertRaises(ValueError):
            run_header_probe(service=FakeService(FakeMessages()), lookback_days=22, **self.kw)
        with self.assertRaises(ValueError):
            run_header_probe(service=FakeService(FakeMessages()), max_results=11, **self.kw)

    def test_workflow_truth_table_is_mutually_exclusive(self):
        workflow = (ROOT / ".github/workflows/sync.yml").read_text()
        self.assertIn("inputs.phase4_dry_run == true && inputs.phase4_header_probe != true", workflow)
        self.assertIn("inputs.phase4_header_probe == true && inputs.phase4_dry_run != true", workflow)
        self.assertIn("inputs.phase4_dry_run != true && inputs.phase4_header_probe != true", workflow)

    def test_receipt_path_isolated(self):
        out = run_header_probe(service=FakeService(FakeMessages()), **self.kw)
        with tempfile.TemporaryDirectory(prefix="newsletter-phase4-") as d:
            write_isolated_receipt(out, str(Path(d) / "receipt.json"))


if __name__ == "__main__":
    unittest.main()
