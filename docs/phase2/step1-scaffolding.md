# Step 1 - Scaffolding (multi-module Maven)

## What we are building
Three new empty Maven modules next to `ember-storage`: `ember-protocol`, `ember-broker`, `ember-client`, plus parent-pom changes (module list, Netty BOM, shade-plugin version).

## Why it is designed this way
Think of Spring multi-module projects: each module is a jar with a clear job, and the arrows between them only point one way.

    ember-storage  <-  ember-protocol  <-  ember-broker (+ Netty)
                                      <-  ember-client (plain sockets)

* The **protocol** module knows how bytes look on the wire, nothing about networking libraries. Both sides can share it, so client and broker can never disagree about a layout.
* The **client has no Netty**: a user who just wants to talk to Ember should not drag a networking framework into their app.
* The **storage** module has no idea a network exists. That keeps the disk logic testable alone.
* Netty comes in through a **BOM** (bill of materials): one version number for all Netty artifacts, so `netty-handler` and `netty-codec` can't drift apart.
* The shade plugin builds a "fat" `-all` jar so `java -jar` works (like Spring Boot's repackage).

A Phase 1 surprise: the production classes (`Log`, `Segment`, ...) were sitting in `src/test/java`. Other modules cannot depend on test sources, so they moved to `src/main/java` (no test file touched).

## Alternatives I rejected and why
* One big module: easy now, but nothing would stop the client from importing Netty classes by accident.
* Gradle: the project already uses Maven.

## Tradeoffs and honest limits
More poms to maintain. The protocol module depends on storage only for `LogRecord`, because the on-disk record format *is* the fetch wire format.

## How it is tested
One `SmokeTest` per new module, so `mvn test` proves every module compiles and runs tests.
