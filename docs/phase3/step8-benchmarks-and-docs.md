# Step 8 - Benchmarks with the real jars, and the documentation

## What we are building
Real measurements of what partitions change, taken with the shaded jars and the CLI `bench` command, written up in `docs/phase3/bench.md`; plus README, PROTOCOL.md and the NotebookLM document.

## Why it is designed this way
**Measure the claim, not the theory.** The theory: "each partition has its own lock and its own fsync, so partitions run in parallel". The measurement can disagree with the theory, and ours partly did (fsync-per-message gained only ~20%, not 4x). A benchmark that only confirms what you expected teaches nothing; one that surprises you shows where the real limit is.

**A fresh broker and an empty data directory for every run.** Old data changes segment counts, page-cache state and recovery time. The bench also records where each partition ends *before* it starts, so it reads back exactly its own records, and it verifies count and per-partition contiguity: a fast result is worthless if records were lost.

**A side experiment to explain a number.** When the partition scaling looked weak, a 30-line program measured raw `FileChannel.force()` throughput with 1, 4 and 8 threads on separate files. It showed the disk itself scales poorly under parallel fsyncs. Without it, "the disk is the limit" would be a guess.

**Docs generated from reality.** The hex dumps are printed by a program that fails if its labels do not add up to the real bytes, and the README's example session is copied from a real run of the jars (a first draft contained made-up output; it was replaced).

## Alternatives I rejected and why
* JMH: great for micro-benchmarks, wrong tool for an end-to-end TCP + disk question.
* Running broker and client on different machines: not available, and loopback removes network noise from the comparison.
* Reporting only the best of N runs: B was repeated three times and the spread is shown.

## Tradeoffs and honest limits
Client and broker share one laptop; one disk; numbers are not portable. A, with 20,000 records, was run once per cell. Explanations of *why* in `bench.md` are labelled as reasoning when they were not isolated by an experiment.

## How it is tested
The bench's own verification line (`verify: OK`) is checked in every run, and `CliTest` runs a small multi-producer bench in the test suite. Documentation facts (flags, commands) are exercised by the CLI tests and by the final run of the real jars in Step 9.
