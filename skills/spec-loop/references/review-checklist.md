# Review checklist (spec-loop step 2)

- [ ] Verifier first: every claim names its number or test.
- [ ] Additive only: no changes to existing endpoints, queries, or goldens.
- [ ] Flag-off identical: default path byte-identical (corpus gate).
- [ ] Hot path untouched: no graph walks, no env reads, no network on await.
- [ ] Propensity/metrics: decisions log what evaluation needs.
- [ ] Cache safety: no per-request flags that bypass the cache key.
- [ ] Scale math: volumes, cardinalities, TTL, and budgets stated.
- [ ] Cost: dominant line identified with a sampling control.
- [ ] Provenance: external claims cite mechanics, not hype.
- [ ] Record what this review changed before showing the spec.
