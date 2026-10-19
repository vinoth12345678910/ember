# Step 1 - Module scaffolding, types, and the storage contract

## What we are building
A new module `ember-raft` that depends on nothing but JUnit, and the vocabulary every later step uses: `Role`, `EntryType`, `LogEntry`, the four Raft messages, `RaftConfig`, the test-only `Bug` enum, the `RaftStorage` interface and an in-memory implementation that can **crash**.

## The Raft rules involved
* Figure 2, "Persistent state on all servers": `currentTerm`, `votedFor`, `log[]` must be on stable storage before responding to RPCs.
* Figure 2, "Volatile state": `commitIndex`, `lastApplied`, `nextIndex[]`, `matchIndex[]` are *not* persisted (rebuilt after a restart, R18).
* Section 5.1-5.2: terms are logical clocks; log indexes start at 1 (we use index 0 = "nothing", term 0).

## Why it is designed this way
**Raft is a state machine, not a program that talks to the network.** The core never opens a socket, reads a clock or starts a thread. It is a function: *(state, event) -> (new state, messages to send)*. That is what makes it testable: we can feed it any sequence of events - including the awful ones a real network produces - and replay a failure exactly.

**Storage is an interface with a durability barrier.** `RaftStorage.sync()` means "everything written before this call survives a crash". Real disks lie in exactly this way: a `write()` that returned may vanish on power loss until `fsync` completes (you saw this in Phase 1). `InMemoryRaftStorage` models it with two copies: a **pending** copy (what the node sees) and a **durable** copy (what survives). `sync()` copies pending to durable; `crash()` throws pending away. A node that "forgets" to sync before promising something will lose that promise in the simulator, exactly like on a real machine.

**Term and vote are written together** (`setTermAndVote`). If they could be written separately, a crash between the two writes could leave "I am in term 7" with a stale vote from term 6 - and the node might then vote twice in term 7.

**`LogEntry` holds a `byte[]`.** Java arrays compare by identity, so `equals/hashCode` are written by hand to compare *content*, and the array is copied on the way in and out so nobody can mutate a log entry that is already in the log. (An entry that changes after being replicated would be a silent consensus bug.) `hashCode` never uses `Enum.hashCode`, which is an identity hash and would differ between JVM runs - the simulator's "same seed, same trace hash" promise depends on this.

**`AppendEntriesResponse` carries `matchIndex`.** The follower tells the leader "I verified your log up to here". Then the leader never needs to remember which request a response belongs to - important because responses arrive late, duplicated and out of order.

**`Bug` lives in the config.** Each injected bug is one clearly-marked `if` in the core, so later we can prove that our checkers actually detect broken Raft (Step 9). It must never be set in production.

## What goes wrong if you get it wrong
* Persist term and vote separately -> double voting after a crash -> two leaders in one term.
* Make `sync()` a no-op in the test storage -> the simulator can never find an "ack before fsync" bug.
* Let `LogEntry` use array identity -> two identical entries compare unequal, and *every* log comparison in the checkers is wrong.

## Alternatives I rejected and why
* Persisting inside `RaftNode` with `java.nio`: ties the core to the file system and makes crash testing impossible.
* A storage that applies writes immediately and has a separate "disk" file image: the same thing, more code.
* Making `LogEntry` a class with a `List<Byte>`: allocation heavy; the copy-on-boundary array is cheaper.

## Tradeoffs and honest limits
`InMemoryRaftStorage.sync()` copies the whole pending log when it changed (O(log length)); fine for tests with hundreds of entries, not for production (Phase 5 will batch and append to a real file). `crash()` models "unsynced writes are lost completely"; real disks can also keep *part* of an unsynced write (torn writes). Phase 1's checksums handle that at the file level.

## How it is tested
`InMemoryRaftStorageTest`: unsynced writes are lost on `crash()`, synced ones survive; truncate; `termAt`/`entryAt` bounds; reads see pending state; pre-populated constructor; term+vote atomic. `LogEntryTest`: content equality, defensive copies. `RaftConfigTest`: validation (self in cluster, min <= max, heartbeat < min) and sorted cluster.
