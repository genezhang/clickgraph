"""Category rules for the known-wrong corpus entries (scorecard MISMATCH /
CG_ERROR). `run_corpus.py --triage` applies them to its last run and writes
tests/corpus/expected/triage.json. A rule is (category, ref, predicate over
the run record); the first match wins; unmatched entries are UNTRIAGED, which
is the cue to look at them.

Categories:
  BUG               ClickGraph returns wrong rows or fails where Neo4j answers
  KNOWN_LOUD        deliberate refusal of a shape tracked by an open issue
  DESIGN_REFUSAL    ClickGraph refuses where Cypher returns an empty result
                    (unknown label/type, schema-impossible pattern): no wrong
                    rows, but not Cypher semantics either
  UNTYPED_BOOLEAN   a UInt8 flag the schema does not declare boolean: the
                    engine treats `= true` as `= 1`, strict Cypher does not
  EXTENSION         ClickGraph-specific semantics (`.id` alias, unmapped
                    property read from the table column)
  CORPUS_TAG        the corpus entry's schema tag does not hold its labels
                    (the source test registers its own schema)
"""
import re

TRACKING = "#1316"

RULES = [
    ("CORPUS_TAG", None, r"label (TestUser|PatternCompUser) not found"),
    ("DESIGN_REFUSAL", None, r"label NonExistent not found|type NONEXISTENT not found|Invalid relationship pattern|non-transitive"),
    ("KNOWN_LOUD", "#588", r"\(#588\)"),
    ("KNOWN_LOUD", "#987", r"closed variable-length path on a fully-unlabeled"),
    ("KNOWN_LOUD", "#641", r"undirected hop chained onto another optional hop"),
    ("KNOWN_LOUD", "#1169", r"references both endp"),
    ("KNOWN_LOUD", "#1308", r"\(#1308\)|outside that path is not supported"),
    ("KNOWN_LOUD", "#1233", r"undirected hop \(`t\d+`\) next to a variable-length path"),
    ("KNOWN_LOUD", "#1182", r"would be joined on a predicate that does not mention it|cannot be tied to the pa|independent pattern in one MATCH"),
    ("BUG", "#1190", r"Cannot determine join keys|t2\.followed_id"),
    ("BUG", "#933", r"shared_anchor_comma"),
    ("UNTYPED_BOOLEAN", None, r"is_active = true"),
    ("EXTENSION", "#411", r"RETURN a\.id, b\.id"),
    ("EXTENSION", None, r"nonexistent_property|r\.post_id"),
    ("BUG", "#1235", r"test_1192__optional_back_path|test_1195__"),
    ("BUG", "#1210", r"test_1181__"),
]


def triage(record):
    hay = " ".join([record["name"], record["cypher"], record["detail"]])
    for category, ref, pattern in RULES:
        if re.search(pattern, hay):
            return {"category": category, "ref": ref or TRACKING}
    return {"category": "BUG", "ref": TRACKING}
