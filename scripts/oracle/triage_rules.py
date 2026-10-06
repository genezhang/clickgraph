"""Category rules for the known-wrong corpus entries (scorecard MISMATCH /
CG_ERROR). `run_corpus.py` applies them when it writes
tests/corpus/expected/triage.json.

A rule is (category, ref, field, pattern). Rules that put an entry in a
NON-bug category match the FAILURE DETAIL (the error text or the differing
rows), never the query text alone, so a real bug in a query that merely
mentions a pattern is not hidden. UNTYPED_BOOLEAN and the row-level
EXTENSION cases are not rules here: the runner CONFIRMS them by re-running a
rewritten query on Neo4j (run_corpus.CONFIRMING_REWRITES). Rules that
only attach an existing issue to a BUG may match the entry name. The first
matching rule wins; the default is BUG under the tracking issue.

Categories:
  BUG               ClickGraph returns wrong rows or fails where Neo4j answers
  KNOWN_LOUD        deliberate refusal of a shape tracked by an open issue
  DESIGN_REFUSAL    ClickGraph refuses a query over an unknown label/type or a
                    schema-impossible pattern; Cypher answers it (with no
                    match for the impossible part). No wrong rows, but not
                    Cypher semantics.
  UNTYPED_BOOLEAN   (runner-confirmed) a UInt8 flag the schema does not
                    declare boolean: ClickGraph reads `= true` as `= 1`
  EXTENSION         ClickGraph-specific semantics (`.id` aliases the node id;
                    an unmapped property is read from the table column)
  CORPUS_TAG        the corpus entry's schema tag does not hold its labels
                    (the source test registers its own schema)
"""
import re

TRACKING = "#1316"

RULES = [
    ("CORPUS_TAG", None, "detail", r"label (TestUser|PatternCompUser) not found"),
    ("DESIGN_REFUSAL", None, "detail", r"label NonExistent not found|type NONEXISTENT not found|Invalid relationship pattern|non-transitive"),
    ("KNOWN_LOUD", "#588", "detail", r"\(#588\)"),
    ("KNOWN_LOUD", "#987", "detail", r"closed variable-length path on a fully-unlabeled"),
    ("KNOWN_LOUD", "#641", "detail", r"undirected hop chained onto another optional hop"),
    ("KNOWN_LOUD", "#1169", "detail", r"references both endp"),
    ("KNOWN_LOUD", "#1308", "detail", r"\(#1308\)|outside that path is not supported"),
    ("KNOWN_LOUD", "#1233", "detail", r"undirected hop \(`t\d+`\) next to a variable-length path"),
    ("KNOWN_LOUD", "#1182", "detail", r"would be joined on a predicate that does not mention it|cannot be tied to the pa|independent pattern in one MATCH"),
    ("EXTENSION", None, "detail", r"Identifier 'x\.nonexistent_property' cannot be resolved"),
    ("BUG", "#1190", "detail", r"Cannot determine join keys|t2\.followed_id"),
    ("BUG", "#933", "name", r"shared_anchor_comma"),
    ("BUG", "#1235", "name", r"test_1192__optional_back_path|test_1195__"),
    ("BUG", "#1210", "name", r"test_1181__"),
]


def triage(record):
    for category, ref, field, pattern in RULES:
        if re.search(pattern, record[field]):
            return {"category": category, "ref": ref or TRACKING}
    return {"category": "BUG", "ref": TRACKING}
