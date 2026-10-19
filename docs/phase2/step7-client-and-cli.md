# Step 7 - The client library, the CLI, and the runnable jars

## What we are building
* `EmberClient`: a small *blocking* client (plain `java.net.Socket`) with `produce`, `fetch`, `metadata`.
* `EmberCli`: a command line on top (`produce`, `fetch`, `metadata`, `bench`).
* Shaded "fat" jars so `java -jar ember-broker-...-all.jar` and `java -jar ember-client-...-all.jar` just work.

## Why it is designed this way
**Blocking and one request at a time.** Think of `fetch()` in browser JS with `await`: send, wait, get the answer. For a first client this is the easiest thing to reason about: methods are `synchronized`, so two threads can share a client without interleaving bytes on the socket. A fast client uses one `EmberClient` per thread (the concurrent test does exactly this).

**No Netty in the client.** A client library should be light. `Socket` + `DataInputStream` are enough when you only have one request in flight.

**Correlation id is checked.** The client increments a counter per request and verifies the echo. If it ever differs, the stream is out of sync (for example after a timeout, a late answer arrives and would be mistaken for the next answer). The only safe reaction is to close the connection. The same rule applies after a read timeout.

**Validate the frame length before allocating.** The response starts with an INT32 length. If a broken server sends 2 GB we must not `new byte[2_000_000_000]`. We compare against `maxFrameBytes` first (the lesson from the record format again).

**CRC is verified here.** The broker streams bytes straight from disk and never inspects them, so `LogRecord.decode` on the client is the only place corruption can be detected.

**Errors.** A non-zero error code becomes an `EmberException` that carries the `ErrorCode`, so callers can `catch` and look at `e.errorCode()` (like checking `response.status` in an HTTP client).

**Shaded jar.** Maven builds a normal jar with only our classes. `maven-shade-plugin` merges all dependencies (Netty for the broker) into one `-all` jar with a `Main-Class` entry, like Spring Boot's repackaged jar.

**Why the integration tests use the real client.** The broker module has a *test-scope* dependency on `ember-client`, so the same client users get is the one proving the broker works. (The main code direction in the design is unchanged: client and broker never depend on each other in production code.)

## Alternatives I rejected and why
* Async/pipelining client: much more state (a map of in-flight requests); a later phase.
* A client that reconnects automatically: hides failures, makes duplicate sends (at-least-once) invisible. Better to be explicit for now.
* Using `java.net.http`: it speaks HTTP, not our protocol.

## Tradeoffs and honest limits
* One request in flight: throughput of a single client is bounded by round-trip time x batch size.
* No automatic retry, no reconnect, no metadata-based routing (single broker).
* After any I/O error the client closes itself; create a new one.

## How it is tested
Client-only tests use a tiny fake server (correlation-id mismatch, error code -> `EmberException`, read timeout, oversize response length, corrupt record bytes). Broker integration tests use the real client; CLI subcommands are run in-process against a real broker.
