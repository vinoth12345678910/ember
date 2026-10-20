# Step 3 - Fault-injection hooks (off by default)

## What we are building
A small `FaultInjector` interface with a do-nothing default (`FaultInjector.NONE`) and a mutable `ConfigurableFaults` implementation, plus four hooks:

| Hook | Where | What it does when ON |
|---|---|---|
| slow fsync | `FileRaftStorage.sync()` (inside the timed region) | adds a delay to every Raft log fsync, so the `raft_fsync_seconds` histogram visibly rises |
| drop peer messages | `PeerNetwork.send()` | drops each message to a peer with a given probability (seeded `Random`) |
| delay peer messages | `PeerNetwork.send()` | delivers each message to a peer after a fixed delay (order is preserved) |
| pause the broker | `GroupRuntime` (before every task of its mailbox) | the group threads park until resumed: the broker looks frozen (like `SIGSTOP`) to its peers |

Link blocking and the client-side "stop the consumer's heartbeats" already existed (`blockPeer`, `pauseHeartbeatsForTesting`).

## Why it is designed this way
A chaos tool needs to *cause* the things we want to survive: a slow disk, a lossy network, a frozen process. Doing that inside the JVM keeps tests fast and seedable. The price is that the production classes contain a few lines that exist only for tests. The rules that keep this honest:

* **Off by default.** Every class gets `FaultInjector.NONE` unless a test passes another one. `NONE.active()` is a constant `false`, so the JIT removes the check; a call site looks like `if (faults.active()) { ... }`. There is no flag, system property or endpoint that turns a fault on in a real broker.
* **Small and clearly named.** One interface, four methods, all documented as test hooks.
* **A pause is a freeze, not a crash.** Holding the Raft worker threads makes the broker stop answering peers and stop applying entries, while its sockets stay open: exactly what a stuck process, a long GC pause or `docker pause` looks like to the cluster. On `close()`/`crash()` the broker resumes first so shutdown cannot hang.

## Alternatives I rejected and why
* Byte-code agents or a proxy in front of every socket (toxiproxy-style): more moving parts, and not needed for in-process runs. For the Docker runs we use real tools (`docker pause`, `docker network disconnect`, `docker kill`) instead.
* A system property that enables faults in any broker: too easy to leave on by accident.
* Random failures inside the production code: faults must be driven from outside, by a seeded schedule that the test prints.

## What goes wrong if you get it wrong
A hook that costs time when off would distort every benchmark in the project. A pause that blocks the *shutdown* path would hang the test suite. A drop that reorders messages of one group would test something Raft is not asked to survive (TCP never reorders); the delay hook therefore uses one fixed delay, which keeps order.

## Tradeoffs and honest limits
* A paused broker still accepts TCP connections and answers the admin port, which a stopped process would not; clients see timeouts, not refused connections.
* Delay is per message, not per bandwidth; there is no jitter.
* The hooks cannot corrupt data. Disk corruption is covered by the file-level fuzz tests of Phase 5.

## How it is tested
`FaultHooksTest`: a normal broker uses `NONE`; each hook works when on (drop probability 1.0 raises `peer_messages_dropped_total`, probability 0 drops nothing; a delay on the leader delays acknowledged produces by at least that delay; slow fsync raises the fsync histogram sum by at least the injected time; pausing a follower freezes its applied index while the others commit, and resuming lets it catch up; pausing the leader makes the others elect a new one).
