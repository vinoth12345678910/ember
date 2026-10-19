# Step 4 - Netty wiring for CREATE_TOPIC / METADATA v0+v1, and the data directory lock

## What we are building
* `RequestHandler` dispatches the new CREATE_TOPIC API and answers METADATA in the version the request used.
* `DataDirLock`: on start the broker takes an exclusive lock on `<dataDir>/.ember.lock`; a second broker on the same directory fails immediately with a clear message.

## Why it is designed this way
**Handler change is tiny on purpose.** All rules live in `BrokerApis` (Step 3). The Netty handler only turns bytes into requests and responses into bytes. Because Netty/zero-copy code did not need to change, the Phase 2 pipeline, executor group and fetch path stay exactly as tested.

**Response in the request's version.** The handler passes `header.apiVersion()` to the response codec. A v0 client therefore gets exactly the v0 bytes it always got.

**Why a directory lock?** Two brokers pointed at the same folder would both append to the same segment files, each believing it owns the end of the file. Their writes would interleave and overwrite each other and nothing would crash - the data would just be silently wrong. An exclusive OS file lock turns that into an immediate, loud startup failure (think of `postmaster.pid` in PostgreSQL or Mongo's `mongod.lock`). The OS releases the lock automatically if the process dies, so a crash never leaves a stale lock that blocks restart (unlike a "PID file" we would have to clean up).

**Two kinds of "someone else has it".** `FileChannel.tryLock()` returns `null` when *another process* holds the lock, but throws `OverlappingFileLockException` when the lock is held by *this JVM* (tests start two brokers in one JVM). We handle both. We additionally remember held directories in a static set and check it *before* touching the file: on POSIX systems closing any file descriptor of a file can drop the process's lock on it, so a failed second attempt in the same JVM must not even open the lock file.

**Order on start.** Lock first, then open logs: a broker that cannot get the lock must not touch any log.

## Alternatives I rejected and why
* PID file: can be left stale after a crash; needs liveness checks.
* Locking each segment file: protects the files but not the metadata directory, and gives errors late (on first append) instead of at startup.
* Doing nothing and documenting it: silent corruption is the worst failure mode a storage system has.

## Tradeoffs and honest limits
* Advisory lock: protects against two Ember brokers, not against an unrelated program writing into the folder.
* File locks on network file systems (NFS) can be unreliable.

## How it is tested
Same-JVM second broker fails fast with the message "another Ember broker is using this data directory" and the first keeps working; after the first closes a new one starts; a failed start releases nothing it does not own. Raw-socket integration tests cover CREATE_TOPIC (success, duplicate, invalid), METADATA v0 and v1 bytes, an unsupported METADATA version, and CREATE_TOPIC with version 1.
