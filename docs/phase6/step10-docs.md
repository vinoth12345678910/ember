# Step 10 - Documentation

## What we are building
`docs/phase6/design.md` (lifecycle diagram, durable vs soft state, determinism, fencing with a worked zombie, eager rebalance vs Kafka, failure matrix, guarantees, limitations), one note per step, `docs/PROTOCOL.md` (seven APIs, error codes 14-20, state-machine commands), the README, and `docs/ember-part7-notebooklm.md`.

## Why it is designed this way
The design document answers the questions in the order a reader asks them: what does a consumer do, why are heartbeats not in the log, why can `apply()` never compute an assignment, how does a zombie get stopped, what do I lose on a rebalance, and what exactly breaks when something fails. The failure matrix lists what the user *sees*, including the cases where the honest answer is "duplicates".

## Alternatives I rejected and why
Documenting only the happy path; claiming exactly-once (false - the docs name the duplicate sources and bound the window).

## What goes wrong if you get it wrong
A document that promises more than the tests prove misleads the next reader. Each guarantee in section 7 of `design.md` has a named test (step 7 and 8); each non-guarantee is listed.

## Tradeoffs and honest limits
Numbers in the docs come only from the files and commands named in step 9 and from the Maven output; the throughput windows are very short and said to be.

## How it is tested
The commands in the README and the scripts were run for real (step 9).
