# Ember Part 8: Seeing inside, and testing the truth (Phase 7)

**Simple English on purpose. Short sentences. The strong technical words stay: Raft, leader, commit, fsync, partition.**

## One-minute pitch

Ember already worked. It copied data to three brokers. It survived a dead leader. Consumers worked in groups. But how do I know it is really right? And how do I see what it is doing?

Phase 7 answers three questions.

1. **What is it doing now?** I added metrics, health checks and a status page. Like the dashboard of a car.
2. **Can I trust what it told its clients?** I built a chaos tool. It breaks the cluster on purpose. Then a separate checker reads what the clients saw. It checks nine rules. It is like an exam marker who does not trust the student.
3. **Can I run it somewhere else?** I put it in Docker. I ran a long test. I measured the speed and wrote down all the limits.

The most important lesson: the new tools found real bugs. Some were in Ember. One was in the tool itself.

---

## Step by step

### Step 0: Check first
**Why.** Do not build on something broken. I ran the full build. It was green (the full build, all green). I also checked that Docker was installed. The program was there, but the Docker engine was not running. I started it for the work and closed it after.

### Step 1: The metrics library
**Why first.** To fix a problem, you must see it.
**Analogy.** A car dashboard. You do not open the engine to see if it is hot.
- A **counter** only goes up (like the trip meter).
- A **gauge** shows a value now (like the speedometer).
- A **histogram** counts how many things took 1 ms, 2 ms, 4 ms, and so on. It shows the shape of the delays, not just one average.

I wrote my own. Why? The project rule says no extra libraries. And I wanted to learn what a histogram really is.
**A bug here.** A bucket label printed as `5.0E-5` instead of `0.00005`. A test caught it.

### Step 2: Put the metrics inside, and the admin page
**Why.** Metrics are only useful if the broker fills them.
- The Raft core stays pure. It has no metrics code. It only tells a listener "I stepped down". The broker turns that into a number.
- A small web server shows `/metrics`, `/health/live`, `/health/ready` and `/status`.
- **Ready is not the same as alive.** Alive means the program runs. Ready means it has caught up and can serve. A broker that just restarted is alive but not ready.
**A real bug.** `/health/ready` said "ready" on a broker that had applied nothing. The reason: Raft remembers the leader *before* it checks that the logs match. So "applied 0, committed 0" looked caught up. The fix: a group must have learned a commit index of at least 1.

### Step 3: Tools to break things (hooks)
**Why.** To test failures, I must be able to cause them. A slow disk. Lost messages. A frozen broker.
**Rule.** The hooks are OFF by default. No flag turns them on in a real broker. When off, they cost nothing.

### Step 4: The chaos tool and the checker
**Analogy.** The checker is an **exam marker who does not trust the student**. It does not ask the broker "were you correct?". It only looks at what the clients were told and what they read.

**The history.** Every operation is written down twice: when it starts and when it ends. The result is one of three: OK (the broker said saved), FAIL (surely not saved), INFO (unknown, like a timeout).

**Nine rules (P1 to P9).** No lost saved write. No two reads that disagree. No invented values. Unique offsets. Order per key. Reads never go back. Every saved message reaches the group. No wrong commit accepted. All copies equal at the end.

**The checker is tested too.** I take a good history. I put in one bug. The checker must find it. I did this 13 times, for 13 kinds of bugs. The good history must give zero problems. Without this, a green result means nothing.

**The faults.** Crash and restart. Freeze (like pressing pause). Cut a broker off. Cut one link. Slow fsync. Drop or delay messages. Freeze a consumer. Only one broker is down at a time, so a majority always lives.

### Step 5: Better benchmark, cluster commands, advertised address
- **Percentiles** (p50, p99, p99.9), warm-up, CSV file.
- **Fixed-rate mode.** It avoids a trap called *coordinated omission*. Story: a shop freezes for 10 minutes. A test that waits for each customer records one slow customer. But in real life, ten more customers arrive and wait. The fixed-rate test sends on a schedule and counts all of them.
- **Advertised address.** A broker has two names. Inside Docker, `ember2`. From my laptop, `localhost:19093`. Clients must get the name that works for them. Brokers still talk to each other with the inside name.

### Step 6: Docker
One command starts three brokers. Some choices and their reasons:
- The start script uses `exec`, so Docker's stop signal reaches Java. Without it, every stop is a kill after 10 seconds.
- The Java heap follows the container memory limit.
- The health check is my own command, because the image has no `curl`.
- No automatic restart: the chaos tool decides when a broker dies.

### Step 7: Run it for real
Three brokers in Docker. I killed a leader container while a producer ran. All 900 messages were still there. Then a 5-minute chaos run: 18 faults, 102,840 saved messages, **0 violations**. Prometheus saw the leader change. Grafana showed the dashboard.

### Step 8: The soak test
**Why.** Some bugs only show after a long time. A thread that is never closed. A file that stays open. A test of seconds cannot see them.
I ran 15 minutes, with three real brokers, a steady load, and a crash or freeze every 30 to 60 seconds. Every 10 seconds I read the numbers: threads, file handles, memory after garbage collection.
**Result.** Three runs of 15 minutes, 15 faults each, **0 violations**, every saved message reached the group. Threads (52 to 55) and open files (96 to 111) stayed flat on all three brokers. The heap number was not clear: the gauge jumps, and three runs gave three answers. So I show it, but I do not use it to say pass or fail. I changed the rule after I saw the result, and I wrote that down. It does not prove there is no heap leak.

### Step 9: Final benchmarks
One laptop. All three brokers on **one disk**. See the numbers below, and the honest reading.

### Step 10 and 11: Docs, final check
The README, the runbook, the demo script, this guide. At the end: a full build and the slow tests.

---

## Bugs and surprises (all real)

1. **"Ready" too early.** The readiness page said ready on a broker that had applied nothing. Raft stores the leader before checking the logs. Fixed with a rule: no commit index learned means not ready. A flaky test showed it. I proved the cause by reading the Raft code.
2. **Messages could be skipped (an old Phase 6 bug).** A consumer read partition A, moved the position, then the fetch of partition B failed. The method threw an error and dropped A's messages. The positions stayed moved. Those messages were skipped for ever. I found it by reading the code while I built the chaos workload. Fixed. A test with a fake fetcher fails on the old code and passes on the new.
3. **A consumer with no partitions for ever.** A topic was created. A moment later, consumers joined. The coordinator did not know the topic yet, so it gave an empty assignment. An idempotent rejoin returned the same empty answer, always. It happened 1 time in 8 runs of a test. Fixed: the coordinator now checks the topic list again on every tick.
4. **The first alarm of my checker was wrong.** It said a "zombie commit" was accepted. I looked at the saved history. My own recorder wrote "commit OK" when `commitSync()` returned. But `commitSync()` returns without sending anything when there is nothing to commit. A false acknowledgement in my history. Fixed at the source. Lesson: a checker is only as good as its input.
5. **A reused cluster gave 41 violations.** The old topic held data from the last run. The checker saw values it never produced. Fixed: every run uses a new topic and a new group.
6. **A leak alarm that was not a leak.** In the first soak, node 2 looked like it was leaking threads (37 then 55). I looked at the raw numbers. Node 2 had been crashed and restarted five times. After each restart it starts with few threads and grows back to 55. All nodes end at 55. My analysis compared across restarts. Fix: ignore the first two minutes of each process life. I did not hide the first result; I kept it and ran the soak again.
7. **A benchmark timed its own goodbye (Phase 6).** The clock included `close()`. That made auto-commit look slower.
8. **Four tests of mine were wrong.** They assumed broker 1 was the leader, or compared two numbers while they were still changing, or used a "next free number" that the next phase used. Fixed by making the setup exact.

## Real numbers

- **Tests:** 747 tests, 0 failures (metrics 15, storage 55, raft 182, protocol 97, replication 92, client 39, broker 215, chaos 52), default run about 5.5 minutes; the cluster-long profile adds three 120 s chaos runs (8 chaos run tests, 7.5 minutes), all green
- **Chaos, in process:** three fixed seeds, 20 seconds each, all fault types. Zero violations. Three seeds of 120 seconds in the slow profile.
- **Chaos, Docker, 5 minutes:** 102,840 saved writes, 18 faults (6 kills, 4 freezes, 5 network cuts, 3 frozen consumers), 7,588 duplicates in group delivery (6.9%), **0 violations**.
- **Failover (20 kills):** median **770 ms**, p90 979 ms, max 1,598 ms. With faster election timers: median **559 ms**.
- **Catch-up:** a broker that missed 100,000 messages was ready in 2.2 to 2.8 seconds.
- **Speed (4 producers, batches of 100, one laptop, one disk):** with fsync, 3,491 messages per second (p50 115 ms). Without fsync, 126,680 per second (p50 2.6 ms). One broker with fsync: 9,845 per second.
- **The latency knee (single messages, fixed rate):** p99 is 44 ms at 25% load, 49 ms at 50% load, **143 ms at 75% load**.
- **Machine:** Apple M4, 10 cores, 16 GiB, macOS, Java 21.

**Reading the numbers honestly.** fsync is the cost. Three brokers on one disk means three fsyncs on the same disk for each message. Also, 12 partitions were slower than 3. The producer sends one request per partition, one after the other. These are facts about this test setup. They are not claims about real servers.

## Glossary

- **Metric.** A number the broker shows about itself.
- **Counter, gauge, histogram.** Only up; value now; shape of the delays.
- **Prometheus.** A tool that reads the metrics every few seconds and keeps them.
- **Grafana.** A tool that draws graphs from Prometheus.
- **Health check.** A question the system asks: are you alive? are you ready?
- **Chaos test.** A test that breaks things on purpose.
- **Nemesis.** The part of the chaos tool that causes the faults.
- **History.** The list of what every client did and was told.
- **Checker.** A program that reads a history and looks for broken rules.
- **Mutation test.** Put a bug in on purpose to check that the test finds it.
- **Coordinated omission.** A benchmark mistake: it waits for a stall instead of counting the requests that should have been sent.
- **Soak test.** A long test to find slow leaks.
- **Advertised address.** The address the broker tells clients to use.
- **Docker, container, volume.** A packaged program; a running copy of it; saved data outside the copy.
- **fsync.** Force the data onto the disk.
- **Quorum / majority.** More than half of the brokers.

## Two prompts for NotebookLM

1. **Video overview.** "Make a video overview of Ember Phase 7 for a student who has built a replicated log. Use simple English. Explain three ideas: a metrics dashboard like a car dashboard; a chaos test with a checker like an exam marker who does not trust the student; and coordinated omission with the shop story. Then tell the story of the false alarm: my checker said a zombie commit was accepted, but my own recorder wrote a commit that never happened. End with the honest limits of the benchmarks."
2. **Quiz.** "Write 12 questions with answers from this document. Include: why ready is different from alive; why the checker is tested with injected bugs; what a history is and what OK, FAIL and INFO mean; what coordinated omission is; why the first soak result looked like a leak but was not; why 12 partitions were slower than 3; and why all numbers come with the words 'one laptop, one disk'. Mix multiple choice and short answers."
