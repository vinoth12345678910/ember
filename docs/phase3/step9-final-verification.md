# Step 9 - Final verification

## What we are building
Nothing new: proof that the finished system works as the real, packaged programs, not just inside the test JVM.

## Why it is designed this way
Unit and integration tests run everything in one JVM with test-specific wiring. Things that only show up with the real jars: the shaded jar missing a class, the `Main-Class` pointing at the wrong place, the shutdown hook not running on SIGTERM/SIGINT, the data-directory lock working *between processes* (the in-JVM tests can only check the same-JVM path), and data surviving a real restart of the real process.

The checks, in order: `mvn clean verify` from the root; start the broker jar; start a **second broker process** on the same directory (must refuse); `create-topic` with 4 partitions (and a duplicate: error 8); produce 20 keyed records; consume them all; print the metadata table; stop with SIGTERM (exit 143, hook output); restart; consume again (same 20 records, same end offsets); try to re-create with a different count (still TOPIC_ALREADY_EXISTS); stop with a real SIGINT (exit 130, hook output).

## Alternatives I rejected and why
* Trusting the test suite alone: it cannot see packaging or signal handling.
* `kill -INT` on a background job in a script: a non-interactive shell starts background jobs with SIGINT ignored, so the signal would be silently dropped; the SIGINT check is therefore done from a process that restores the default handler.

## Tradeoffs and honest limits
It is a manual, one-machine smoke test; the numbers and output are reproduced in the final report and README.

## How it is tested
This step *is* the test; its output is recorded in the final report.
