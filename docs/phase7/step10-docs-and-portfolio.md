# Step 10 - Documentation and portfolio material

## What we are building
`docs/phase7/design.md` (metrics map, what the checker proves and cannot, the checker's mutation story, coordinated omission, the advertised-address problem, Docker choices, the bugs found); `docs/OPERATIONS.md` (runbook with a symptom table, backup/restore, alert rules as text); a rewritten `README.md` (pitch, ASCII architecture, quickstart, guarantees and non-guarantees, measured numbers with the machine, failure matrix, structure, what I would build next, what I learned); `docs/PROTOCOL.md` (advertised addresses); `.github/workflows/ci.yml`; `.gitignore`; `LICENSE`; and four documents written to be **said aloud** in very simple English: `docs/DEMO-SCRIPT.md`, `docs/INTERVIEW-QA.md`, `docs/ember-part8-notebooklm.md`, and `docs/RESUME-BULLETS.md` (resume bullets use only measured numbers, each with its proof file named).

## Why it is designed this way
A project like this is judged in a conversation, not by reading code: someone asks "what happens when the leader dies?" and you answer in 30 seconds. So the documents are shaped for speaking: short sentences, the strong technical words kept (Raft, leader, commit, fsync, partition), every claim tied to a test or a file. The README says what is measured and what is not, and puts the machine description next to every performance number, because a number without its recipe is a rumour.

## Alternatives I rejected and why
* A marketing README with round numbers: every number here is from a file in `docs/benchmarks/` or a test.
* One giant document: the design documents are per phase because the reader asks different questions at different depths.
* Hiding limitations: each document lists them (README "Known limitations", `design.md` part 2, `OPERATIONS.md`).

## What goes wrong if you get it wrong
A claim without a source (for example "zero data loss") is the first thing an interviewer probes. The guarantees table therefore says *what* is guaranteed and *under which condition* (a majority alive, the Raft log fsynced), and the chaos checker's own limits are written down.

## Tradeoffs and honest limits
The three read-aloud documents deliberately avoid long sentences, which makes them less precise than the design documents; they point to those for detail. The CI workflow runs the whole default suite, including the in-process chaos runs, which depend on timing; on a slow shared runner they can be flaky and may need longer timeouts. I could not run GitHub Actions from here; the workflow is written and read, not exercised.

## How it is tested
Commands in the README, the demo script and the runbook were run (the Docker cluster, the CLI commands, the chaos tool, `--check-only`). Numbers come from `docs/benchmarks/` and the Maven output. Nothing was pushed.
