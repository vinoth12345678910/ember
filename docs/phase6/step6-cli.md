# Step 6 - CLI: consume --group, group describe, group offsets

## What we are building
```
consume --group=<id> --topic=a,b [--from=earliest|latest] [--max=N] [--session-ms=..] [--commit-interval-ms=..] [--follow]
group describe --group=<id>
group offsets  --group=<id>
```
Every older command works as before. `consume` without `--group` is still the manual consumer.

## Why it is designed this way
A group is hard to understand without seeing it. `group describe` is the "who owns which table, and how far behind are we" screen: the generation (seating-chart version), the state, each member with its assignment and, for every partition, its owner, the committed offset, the log end offset and the LAG (log end minus committed). Lag is the one number an operator watches: if it keeps growing, the group is not keeping up.

For a partition without a committed offset the lag is shown as log end minus log start (everything is still unread) and the committed column shows `-`.

`consume --group` behaves like the old `consume`: it stops when it is caught up, unless `--follow` is given; `--max=N` stops after N records. Ctrl+C / SIGTERM stops it cleanly: the consumer commits what it has printed, leaves the group (so its partitions move to the others at once instead of after the session timeout) and exits. `kill -9` gives no such goodbye, which is exactly what the demo uses to show the session timeout at work.

## Alternatives I rejected and why
* A separate `ember-group` tool: more jars to build for no benefit.
* Printing lag per member only: partitions are what move between members; lag per partition shows exactly where the backlog sits.
* Committing after every record in the CLI: it would turn a read into thousands of Raft writes. Auto-commit every few seconds, and once at exit, bounds the repeats.

## What goes wrong if you get it wrong
* A CLI that exits without leaving keeps a ghost member for a whole session timeout: its partitions sit idle although everyone else is ready.
* A CLI that prints the offsets of the last *fetch* instead of the committed ones would make the lag column lie.

## Tradeoffs and honest limits
* `group describe` asks the coordinator (the applied state of the leader) and each partition's leader for its end offset: the numbers are a moment apart, so lag can be briefly negative-looking; it is clamped at 0.
* No `group delete` or offset reset commands (non-goals).

## How it is tested
`GroupCliTest` runs the real `EmberCli.run` against a 3-broker cluster: consume with a group and `--max`, a second run resumes after the committed offsets, `group describe` and `group offsets` output (generation, members, assignment, committed, end, lag), an unknown group, and bad flags.
