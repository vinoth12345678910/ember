# Step 9 - Proving the checkers can see: injected bugs

## What we are building
`BugDetectionTest`: for each deliberately wrong behaviour in `Bug` (one tiny `if` each in the core), run up to 3,000 random seeds and assert that at least one seed makes an invariant checker fire - of a sensible kind. Plus the control: with `Bug.NONE` the same seeds produce zero violations.

## The Raft rules involved
Each bug removes or breaks one rule:
| Bug | Rule broken | Law that should break |
|---|---|---|
| `VOTE_WITHOUT_LOG_CHECK` | R5 / 5.4.1 election restriction | Leader Completeness, State Machine Safety |
| `COMMIT_OLD_TERM_BY_COUNTING` | R15 / 5.4.2 (Figure 8) | Leader Completeness, State Machine Safety |
| `FORGET_VOTED_FOR_ON_RESTART` | Figure 2 persistent `votedFor` | Election Safety |
| `ACK_BEFORE_SYNC` | outbox rule (state durable before replies) | Leader Completeness, State Machine Safety |
| `TRUNCATE_ON_EVERY_APPEND` | R12 (never truncate on a match) | Log Matching, State Machine Safety, Leader Completeness |

## Why it is designed this way
**A test suite that has never failed proves nothing.** After thousands of green seeds, how do we know the checkers are not simply blind? The honest answer is *mutation testing*: break the algorithm on purpose, in the ways the paper warns about, and check that the safety net catches it. Each injected bug is "a real mistake an implementer could make", so catching it demonstrates that the simulator *plus* the checkers would catch the same mistake in a future change (Phase 5 will touch this code again).

**Bugs live behind `config.injectedBug()`.** Each is one clearly-marked branch, default `Bug.NONE`. They are in `src/main` so the simulator (also in `src/main`) can inject them; Phase 5 must never set anything but `NONE` (documented in the enum).

**If a bug survives 3,000 seeds, improve the generator, not the assertion.** A surviving bug means the random scenarios are too gentle (not enough crashes at the right moment, too little reordering). The fix is a nastier scenario (e.g. "crash right after sending a message", added in Step 4/8 precisely for `ACK_BEFORE_SYNC`), never a weaker check.

**Control group.** `Bug.NONE` over thousands of seeds must give exactly zero violations; otherwise "caught" proves nothing (a checker that fires on everything also "catches" bugs).

## What goes wrong if you get it wrong
A checker that is too weak lets `ACK_BEFORE_SYNC` through (the crash window is tiny and needs a crash at exactly the wrong instant), or never notices a leader missing a committed entry. A checker that is too strong reports false alarms on correct runs.

## Alternatives I rejected and why
* Hand-writing one test per bug: shows the checker works for one schedule, not that random exploration finds it.
* Mutating the compiled code automatically (PIT etc.): needs a dependency; a handful of hand-picked, meaningful bugs is clearer for learning.

## Tradeoffs and honest limits
Five bugs is a sample, not a proof. Detection counts depend on the scenario generator; they are reported honestly in `docs/phase4/bug-injection.md`.

## How it is tested
`BugDetectionTest` itself; every bug reports which checker fired and how many seeds it took. The Figure 8 scenario in `ScriptedScenariosTest` independently catches `COMMIT_OLD_TERM_BY_COUNTING` with one fixed schedule.
