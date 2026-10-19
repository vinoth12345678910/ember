# Step 10 - Documentation

## What we are building
`docs/phase5/design.md` (the seven-part design), one note per step, `docs/PROTOCOL.md` (RAFT_MESSAGE, METADATA v2, error codes 9-13, state machine commands), the README update, `docs/ember-part6-notebooklm.md`, and corrected numbers in `docs/phase4/bug-injection.md`.

## Why it is designed this way
The design document answers the questions a reader asks in order: what happens to one record, why two logs, why apply can be replayed, why Raft instead of ISR, and *what exactly breaks when*. The failure matrix is the most important table: it states what the client sees for each failure, including the cases where the answer is "the record may or may not exist".

## Alternatives I rejected and why
Writing only code comments: they explain a line, not a decision. Claiming "exactly once": false; the docs say at-least-once and name the duplicate sources.

## What goes wrong if you get it wrong
A doc that promises more than the tests prove misleads the next reader (and the author in six months). Every guarantee listed in `design.md` section 6 has a test named in the step notes; every non-guarantee is listed explicitly.

## Tradeoffs and honest limits
Docs were written alongside the code and corrected where measurements disagreed (for example the Phase 4 bug-injection table, whose numbers changed after CheckQuorum became part of the random scenarios).

## How it is tested
Commands in the README and the scripts were run for real. Numbers come from the files named in step 9 and from the Maven output.
