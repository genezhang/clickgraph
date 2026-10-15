---
name: spec-loop
description: This skill should be used when the user asks to "run spec-driven development", "spec this outcome-loop change", "implement the next loop stage", or "take a spec through review to green gates". Drives spec → implement → review → verify for ClickGraph loop work.
---

# Spec Loop

One stage at a time. Do not start the next stage until the current stage's
gates are green.

1. **Spec** — write or update the stage spec in `docs/design/`. State the
   verifier first: every claim names the number or test that checks it.
   Keep changes additive; flag-off behavior must stay byte-identical.
2. **Review** — self-review the spec against `references/review-checklist.md`
   before showing it. Record what the review changed.
3. **Implement** — smallest diff honoring SOLID (see
   `references/solid-contract.md`): pure logic separate from I/O, dialect
   dispatch through existing traits, no new core branches, no new
   dependencies without justification.
4. **Verify** — run the `outcome-loop-verify` skill gates. Fix red before
   proceeding.
5. **Close** — update README + CHANGELOG, commit, report. Push only when all
   gates are green.

## References

- Review checklist: see `references/review-checklist.md`.
- SOLID contract for new code: see `references/solid-contract.md`.
