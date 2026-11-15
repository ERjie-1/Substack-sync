# Failure-only completion and label evidence (task #576)

Three inspected natural runs failed on one 25-block batch with 15 parsed
translations. The original response text was not retained. Those counts alone
do not distinguish omitted labels from duplicates or a provider output limit.

Keep the original prompt, model, request body, max_tokens, timeout, batching,
parser, completeness gate and page-create ordering. The same successful API
content is returned as a string-compatible value with a fixed allowlisted
finish_reason. Non-string content keeps its original return behavior. Missing,
non-string or unrecognized reasons become `unknown`, never arbitrary text.

Only the existing incomplete-translation branch persists four extra fields:
finish_reason, missing_chunk_labels, duplicate_chunk_labels and the number of
extra duplicate occurrences. Labels are numeric indices within the expected
batch; arbitrary/out-of-range values and response text are not saved. The
matcher equals the existing parser. Missing means the parser returned None;
this does not introduce new empty-string rejection or repair old parsing.
Known `length` can locate a completion boundary but is not proof that the
whole article was omitted or that a specific token setting must change.

No model/source request, retry, partial-page write, risk closure or workflow
configuration is added. Existing failure diagnostics are retained; this patch
makes no broader claim about sanitizing the previous error-message fields.
A successful batch's metadata is transient and not added to the article.

Offline: `python3 -B -m unittest discover -s tests -p 'translation_failure_diagnostics_unittest.py' -v`.
Ten tests extract only translation definitions via AST: no whole-app import,
environment/credential reads or network client. They cover missing/duplicate
labels, safe completion reasons, unchanged empty-string behavior, single-call
HTTP failure, request parameters and source-byte preservation. Three fixtures
compare frozen f51e1377 baseline/candidate success payloads, original failure
messages/fields and request arguments exactly after excluding new fields.
This is synthetic evidence, not replay of the private article or live model.

Candidate only; review before installing. Natural failure must validate new
receipt fields later. Do not rerun a model call, close coverage risks or weaken
completeness to manufacture acceptance. Revert this commit to roll back.
