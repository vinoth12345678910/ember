# Step 3 - The new module `ember-replication`: FileRaftStorage and the command codecs

## What we are building
* A new module `ember-replication` (depends on `ember-raft`, `ember-storage`, `ember-protocol`; **no Netty**) - the place where real threads and real files meet the pure Raft core.
* `FileRaftStorage`: a disk implementation of `RaftStorage` for one Raft group, under `<dataDir>/__raft/<topic>-<partition>/`:
  * a **`state`** file holding `currentTerm` and `votedFor` with a CRC, replaced atomically on every `setTermAndVote`;
  * an append-only **`log`** file of framed, checksummed entries; `sync()` = `force(false)`.
* The two state-machine **command formats** (`PRODUCE_BATCH`, `CREATE_TOPIC`) and their strict codecs.

## Why it is designed this way
**The Raft log is the source of truth, so it must be crash-proof.** In Phase 4 `InMemoryRaftStorage` *modeled* a disk (pending vs durable). `FileRaftStorage` is the real thing and has to satisfy the same contract: after `sync()` returns, everything written before survives power loss; anything not synced may vanish, in whole or in part.

**`state` is replaced atomically.** `currentTerm` + `votedFor` must change together and survive. We write `state.tmp`, fsync it, `rename` it over `state` (atomic on POSIX), then fsync the directory so the rename itself is durable - the same dance as the Phase 3 topic metadata file. A crash at any point leaves either the old complete file or the new complete file, plus maybe a stale `state.tmp`, which is simply deleted on the next start. If `state` exists but its CRC fails we **refuse to start**: guessing a vote could make the node vote twice in a term, which is much worse than being down.

**`log` frames: `[length][crc][index][term][type][payload]`.** Exactly the lesson of Phase 1: a length prefix to find the next entry, a CRC to detect corruption, the index so a missing or duplicated entry is detected too (continuity: entry `n` must follow entry `n-1`). On open we scan from the start, verify every entry, and **truncate at the first bad one** (torn tail after a crash, or a bit flip). Cost of that choice: a corrupt entry in the *middle* silently drops everything after it from this replica; the replica then looks like a follower that is behind and is repaired from the leader - safe as long as a majority still has the data, and the same trade-off Kafka makes. We never *return* a corrupt entry.

**Memory index, positional reads.** We keep two arrays (file position and term per index) so `termAt(i)` - which Raft calls constantly - never touches the disk, and read entry bodies with positional reads (like Phase 1's `Segment`). A small LRU cache of recent entries makes the hot path (leader replicating the entries it just wrote) disk-free.

**`truncateFrom`** just truncates the file at that entry's start. Raft only ever truncates a suffix (conflicting uncommitted entries), which is why a plain `FileChannel.truncate` is enough.

**Command codecs are strict and live next to the state machines.** A `PRODUCE_BATCH` carries the **leader's timestamp inside the command**: if every replica stamped records with its own clock, replicas would store different timestamps and retention would diverge. `CREATE_TOPIC` is decided at apply time (duplicate -> `TOPIC_ALREADY_EXISTS`), deterministically on every replica.

**A switch for tests and benchmarks only.** `fsync=false` (flag `--unsafe-no-raft-fsync` at the broker) skips every fsync. Raft's safety argument assumes durable storage; with this switch a power cut can lose acknowledged data. It is off by default and documented as unsafe.

## Alternatives I rejected and why
* Reusing the Phase 1 `Log` for the Raft log: it has no `truncateFrom`, and the Raft log needs index continuity checks and `(index, term)` lookups. Step 5's notes discuss unifying the two logs as real systems do.
* A database (SQLite, RocksDB): a dependency, and it hides the lesson.
* One file for state and log: rewriting the log on every vote would be absurd; the two have different update patterns.

## What goes wrong if you get it wrong
* Writing `state` in place: a crash mid-write leaves a torn file -> a node that forgot its vote or term.
* Returning an entry without verifying its CRC: a flipped bit becomes a replicated, committed lie.
* Not checking index continuity: a lost entry in the middle would shift every later index.
* Skipping the directory fsync after the rename: the new state file could vanish after a crash even though the data was fsynced.

## Tradeoffs and honest limits
Entries are written twice in the whole system (Raft log and, once committed, the partition's data log); step 4 explains why. The Raft log grows forever (no snapshots / compaction: a non-goal; on restart a replica replays from index 1). Open time grows with the log. An in-process "crash" in tests cannot lose OS page-cache data; the real `kill -9` demo and the fuzz tests (torn tails, bit flips) cover what we can.

## How it is tested
Persistence across reopen; model-based test (a long random sequence of append / truncate / setTermAndVote / sync / reopen applied to both `FileRaftStorage` and `InMemoryRaftStorage` must always show the same visible state); torn tail fuzz (cut the file at every byte) and bit-flip fuzz (recover to a clean prefix, never a corrupt entry); a leftover `state.tmp`; corrupt `state` refuses to open; term+vote atomicity; large entries; unsafe mode still functionally correct. Command codecs: round trips and every malformed case.
