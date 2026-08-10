import tempfile
import unittest
from unittest.mock import Mock
from unittest.mock import patch
import types, sys
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parents[1]))
from newsletter_dry_run import run_dry_run, write_isolated_receipt
from newsletter_registry import load_registry, registry_sha

ROOT = Path(__file__).parents[1]
REGISTRY = ROOT / "newsletter_registry.json"
REGISTRY_SHA = "eb2f5e0f6d1f58232cd99dc96e4b9b6dcada2f7976f4f13df39caf84368fbe85"

class NewsletterDryRunTests(unittest.TestCase):
    def setUp(self):
        self.kw = dict(registry_path=REGISTRY, source_ids=["citrini", "capitalflow", "sleepy"], run_manifest_id="run-1", manifest_sha="b74b0004", registry_sha_expected=REGISTRY_SHA, repo_sha="847085f")
    def test_registry_is_frozen(self):
        data = load_registry(REGISTRY); self.assertEqual(registry_sha(data), REGISTRY_SHA)
    def test_three_source_states_and_zero_writes(self):
        rows=[{"source_id":"citrini","sender":"citrini@substack.com","gmail_message_id":"g1","body":"complete"},{"source_id":"capitalflow","sender":"capitalflows@substack.com","gmail_message_id":"g2","body":""},{"source_id":"sleepy","sender":"sleepysol@substack.com","gmail_message_id":"g3","body":"body","episode_key":"ep1"}]
        out=run_dry_run(rows, **self.kw)
        self.assertEqual(out["discovered"],3); self.assertEqual(out["counts"]["ADMITTED"],2); self.assertEqual(out["counts"]["HELD_PREVIEW_OR_EMPTY"],1); self.assertTrue(out["no_write"]); self.assertTrue(out["no_send"]); self.assertTrue(all(v==0 for v in out["side_effects"].values()))
    def test_richer_duplicate_wins_independent_of_order(self):
        a={"source_id":"sleepy","gmail_message_id":"g-shell","body":"","episode_key":"ep1"}; b={"source_id":"sleepy","gmail_message_id":"g-body","body":"full","episode_key":"ep1"}
        for rows in ([a,b],[b,a]):
            out=run_dry_run(rows, **self.kw); self.assertEqual(out["counts"]["ADMITTED"],1); self.assertEqual(out["counts"]["DUPLICATE"],1); self.assertEqual(out["extraction_count"],1)
    def test_receipt_path_isolated(self):
        out=run_dry_run([], **self.kw)
        with tempfile.TemporaryDirectory(prefix="newsletter-phase4-") as d: write_isolated_receipt(out, str(Path(d)/"receipt.json"))
    def test_mutator_spies_are_not_called(self):
        spies={name: Mock() for name in ("notion", "ledger", "translation", "dispatch", "send")}
        out=run_dry_run([], mutator_spies=spies, **self.kw)
        for spy in spies.values(): spy.assert_not_called()
    def test_real_gmail_collector_read_only_denylist(self):
        google=types.ModuleType("google"); oauth2=types.ModuleType("google.oauth2"); creds=types.ModuleType("google.oauth2.credentials"); auth=types.ModuleType("google.auth"); transport=types.ModuleType("google.auth.transport"); req=types.ModuleType("google.auth.transport.requests"); api=types.ModuleType("googleapiclient"); discovery=types.ModuleType("googleapiclient.discovery")
        creds.Credentials=type("Credentials",(),{}); req.Request=type("Request",(),{}); discovery.build=lambda *a,**k: object()
        oauth2.credentials=creds; auth.transport=transport; transport.requests=req; api.discovery=discovery
        with patch.dict(sys.modules,{"google":google,"google.oauth2":oauth2,"google.oauth2.credentials":creds,"google.auth":auth,"google.auth.transport":transport,"google.auth.transport.requests":req,"googleapiclient":api,"googleapiclient.discovery":discovery}):
            import sync_substack
            with patch.object(sync_substack, "get_gmail_service", return_value=object()), \
             patch.object(sync_substack, "get_emails", return_value=[{"id":"g1","from":"citrini@substack.com","body_text":"<p>Body</p>","subject":"x"}]), \
             patch.object(sync_substack, "write_message_ledger") as ledger, \
             patch.object(sync_substack, "update_recent_empty_statuses") as status, \
             patch.object(sync_substack, "translate_blocks_deepseek") as translation:
                from newsletter_dry_run import collect_gmail_items
                rows=collect_gmail_items(registry_path=REGISTRY,source_ids=["citrini","capitalflow","sleepy"],max_emails=3,lookback_days=1)
        self.assertEqual(rows[0]["gmail_message_id"], "g1")
        ledger.assert_not_called(); status.assert_not_called(); translation.assert_not_called()
    def test_canonical_body_is_stable_for_html_and_plain(self):
        from body_canonical import canonical_body
        self.assertEqual(canonical_body("<p>A\u00a0B</p><div>C</div>"), canonical_body("A B\nC"))
if __name__ == "__main__": unittest.main()
