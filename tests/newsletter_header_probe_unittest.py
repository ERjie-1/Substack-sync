import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

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


class FakeExecute:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class FakeMessages:
    def __init__(self, responses):
        self.responses = responses
        self.list_calls = []
        self.get_calls = []

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        return FakeExecute({"messages": [{"id": item} for item in self.responses]})

    def get(self, **kwargs):
        self.get_calls.append(kwargs)
        return FakeExecute({"payload": {"headers": [
            {"name": "From", "value": "Example <citrini@substack.com>"},
            {"name": "Message-ID", "value": "<message@example>"},
            {"name": "Subject", "value": "Protection Matters"},
            {"name": "Date", "value": "Mon, 10 Aug 2026 10:00:00 +0000"},
        ]}})


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

    def test_exact_subject_probes_are_metadata_only(self):
        messages = FakeMessages(["gmail-citrini-1"])
        out = run_header_probe(service=FakeService(messages), **self.kw)
        self.assertEqual(len(out["probes"]), 3)
        self.assertTrue(all(p["query"].startswith('subject:"') for p in out["probes"]))
        self.assertTrue(all(p["max_results"] == 10 and p["lookback_days"] == 21 for p in out["probes"]))
        self.assertTrue(all(call["format"] == "metadata" for call in messages.get_calls))
        self.assertTrue(all(call["metadataHeaders"] == list(METADATA_HEADERS) for call in messages.get_calls))
        self.assertTrue(out["body_requested"] is False)
        self.assertTrue(out["attachments_requested"] is False)
        self.assertTrue(all(v == 0 for v in out["side_effects"].values()))
        self.assertTrue(out["no_write"] and out["no_send"])

    def test_empty_probe_is_true_no_recent_mail_only_for_exact_subject(self):
        class EmptyMessages(FakeMessages):
            def list(self, **kwargs):
                self.list_calls.append(kwargs)
                return FakeExecute({"messages": []})
        out = run_header_probe(service=FakeService(EmptyMessages([])), **self.kw)
        self.assertTrue(all(p["terminal_state"] == "TRUE_NO_RECENT_MAIL" for p in out["probes"]))

    def test_missing_required_header_fails_closed(self):
        messages = FakeMessages(["gmail-citrini-1"])
        original = messages.get
        def missing_message_id(**kwargs):
            result = original(**kwargs).execute()
            result["payload"]["headers"] = [h for h in result["payload"]["headers"] if h["name"] != "Message-ID"]
            return FakeExecute(result)
        messages.get = missing_message_id
        out = run_header_probe(service=FakeService(messages), **self.kw)
        citrini = out["probes"][0]
        self.assertEqual(citrini["terminal_state"], "HELD_LINEAGE_MISSING")
        self.assertEqual(citrini["matches"][0]["terminal_state"], "HELD_LINEAGE_MISSING")

    def test_fixed_bounds_reject_expansion(self):
        with self.assertRaises(ValueError):
            run_header_probe(service=FakeService(FakeMessages([])), lookback_days=22, **self.kw)
        with self.assertRaises(ValueError):
            run_header_probe(service=FakeService(FakeMessages([])), max_results=11, **self.kw)

    def test_receipt_path_isolated(self):
        out = run_header_probe(service=FakeService(FakeMessages([])), **self.kw)
        with tempfile.TemporaryDirectory(prefix="newsletter-phase4-") as d:
            write_isolated_receipt(out, str(Path(d) / "receipt.json"))


if __name__ == "__main__":
    unittest.main()
