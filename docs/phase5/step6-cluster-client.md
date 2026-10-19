# Step 6 - The cluster-aware client

## What we are building
* `ClientConfig`: a bootstrap list (`host:port,...`), `retries` (10), `retryBackoffMs` (100, doubling to 2000).
* `ClusterClient`: asks any bootstrap broker for **METADATA v2**, caches brokers / controller / partition leaders, keeps one lazily-opened `EmberClient` connection per broker, sends PRODUCE and FETCH to the partition **leader** and CREATE_TOPIC to the **controller**, and on a retriable error or a lost connection refreshes metadata, backs off and retries.
* `EmberProducer` / `EmberConsumer` keep their public API; built from `host,port` (or a bootstrap list) they route through the cluster client.
* CLI: `--bootstrap=h1:p1,h2:p2`; `metadata` prints brokers, controller, and per partition leader / replicas / offsets; `bench` gets `--retries` and reports how many sends were retried.

## Why it is designed this way
**The client does the routing, like Kafka.** A broker never forwards a request: it answers `NOT_LEADER_OR_FOLLOWER` and the client goes where the leader is. That keeps brokers simple and avoids hidden extra hops, at the price of a smarter client (this step).

**Retriable vs. fatal.** `UNKNOWN_TOPIC_OR_PARTITION` (the broker has not applied the topic yet), `NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE` (an election is going on), `NOT_CONTROLLER`, `BROKER_OVERLOADED` and `REQUEST_TIMED_OUT` all mean "try again, probably after refreshing". Anything else (`INVALID_REQUEST`, `OFFSET_OUT_OF_RANGE`, `TOPIC_ALREADY_EXISTS`...) is the real answer and surfaces at once. `UNKNOWN_TOPIC_OR_PARTITION` is retried only a few times: a topic that truly does not exist should fail in well under a second, not after the full retry budget.

**At-least-once, said loudly.** If a connection dies or a request times out during PRODUCE, the client cannot know whether the record was committed. Retrying may store it twice. The same holds for `NOT_LEADER_OR_FOLLOWER` returned *after* the leader proposed the record and then lost leadership. Duplicates are possible; the idempotent producer is a later phase.

**One connection per broker, opened lazily and replaced after failure.** A dead broker costs one failed connect per refresh, not a stuck client.

**The old single-connection mode still exists.** `EmberProducer(EmberClient, ClientConfig)` keeps the Phase 3 behaviour (one connection, "refresh metadata once, retry once"): useful for tests that want to control exactly what the client sees.

## Alternatives I rejected and why
* Brokers proxying requests to the leader: hides failures, doubles traffic, breaks the "ordered per connection" reasoning.
* A background metadata thread: more moving parts; metadata is refreshed on demand, when something fails.
* Infinite retries: a client must eventually tell the caller something.

## What goes wrong if you get it wrong
* Retrying a non-retriable error (`TOPIC_ALREADY_EXISTS`, `OFFSET_OUT_OF_RANGE`) just burns time.
* Never refreshing after `NOT_LEADER_OR_FOLLOWER`: the client keeps hammering the old leader.
* Treating `REQUEST_TIMED_OUT` as "failed, safe to retry": silent duplicates.
* A single shared connection with no reconnect: after one broker restart the client is dead forever.

## Tradeoffs and honest limits
Retries sleep in the client (a library may; the tests may not). Metadata can be stale for one request; that is what the retry loop is for. One request in flight per connection and per client object, as before. Duplicates are possible on retry.

## How it is tested
Client unit tests with a fake server (retriable codes retried then fatal ones not; backoff budget; connection failure fails over to the next bootstrap broker; controller redirect). Cluster integration tests in Step 7 drive the real thing through leader failover.
