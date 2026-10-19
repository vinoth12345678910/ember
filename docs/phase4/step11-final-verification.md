# Step 11 - Final verification

## What we are building
Nothing new: a last end-to-end proof from the repository root, and two demonstrations with the real tool: a healthy run (election, crash, re-election) and a deliberately broken run that must produce a violation report.

## The Raft rules involved
All of Figure 2 (through the whole test suite) and Figure 3 (through the checkers).

## Why it is designed this way
`mvn clean verify` from the root rebuilds every module from scratch (no stale classes), runs every test of all five modules, and builds the shaded jars, including the `SimMain` jar of `ember-raft`. Then the tool is used the way a learner would use it:
* `SimMain --seed=42 --nodes=5 --scenario=leader-crash` shows the life of a cluster: an election, replication, the leader's death, a split vote, a re-election in a higher term, the old leader returning as a follower.
* `SimMain --scenario=figure8 --bug=COMMIT_OLD_TERM_BY_COUNTING` shows what a safety violation report looks like: the invariant name, a plain-language explanation, the seed, and the last trace lines.

## What goes wrong if you get it wrong
A green test run on an incremental build can hide a stale class file; a tool that works only inside the IDE may not work from the jar.

## Alternatives I rejected and why
Only running tests: it would not show that the shipped jar works or what the human-facing output looks like.

## Tradeoffs and honest limits
The demonstrations are single runs, illustrations rather than statistics; the statistics are in `bug-injection.md` and the long run.

## How it is tested
This step is the test; the output is reproduced in the final report.
