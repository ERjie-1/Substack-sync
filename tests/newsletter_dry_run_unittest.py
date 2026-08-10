import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parents[1]))
from newsletter_dry_run import run_dry_run, write_isolated_receipt
from newsletter_registry import load_registry, registry_sha

ROOT = Path(__file__).parents[1]
REGISTRY = ROOT / "newsletter_registry.json"
REGISTRY_SHA = "191556090b5112fb0c2a0b77c39ebd3507bd82e76dd70c20d033c111033561ff"

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
if __name__ == "__main__": unittest.main()
