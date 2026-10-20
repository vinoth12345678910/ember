# Interview questions and answers (25)

**Read these out loud. Short sentences. Honest about limits.**
Ember = my message broker, written from zero in Java. Like a small Kafka.

---

**1. What is Ember, in one minute?**
It is a message broker. Producers send messages. Consumers read them. Messages go to topics. A topic has partitions. Ember keeps three copies of every partition on three brokers. It uses Raft to choose a leader. A message is "saved" only when most brokers have it on disk. I also built consumer groups, metrics, and a chaos test tool.

**2. Why Raft and not ISR, like Kafka?**
Kafka keeps a list of replicas that are "in sync". It saves a message when all of them have it. If a replica is slow, Kafka removes it from the list. That list can shrink to just the leader. Then, if the leader dies, you must choose: stop, or lose saved data. Raft has one simple rule: a message is saved when a majority has it. No list to manage. Raft never loses a saved message while a majority is alive. The price: it needs 2 of 3 brokers, so it tolerates fewer failures for the same number of copies.

**3. What happens when the leader dies?**
The other brokers stop hearing its heartbeat. After a random time, one of them asks for votes. It needs a majority. The one with the most up-to-date log can win. The new leader starts. Clients get an error like "not leader", ask for fresh metadata, and send to the new leader. In my tests, a producer is back about one second after `kill -9`. Every message that was saved is still there.

**4. Why two logs per partition?**
The Raft log is the truth. It is on disk, with fsync, and it holds every decision. The data log is what consumers read. It is filled only from messages that are committed. Think: the Raft log is the signed minutes of a meeting. The data log is the newspaper that readers see. If the newspaper is lost, I can print it again from the minutes. The price: every message is written twice, so it uses more disk and more time.

**5. What is idempotent apply?**
After a restart, Ember replays the Raft log into the data log. But the data log may already hold part of it. "Idempotent" means doing it twice gives the same result. For each batch, Ember compares where replay is with how many records the data log already has. Skip it, append all, or append only the missing tail. No gaps, no duplicates. I tested a crash at every byte of the file.

**6. What is the high watermark here?**
In Kafka, it is the offset up to which data is safe to read. In Ember I do not need a separate one. The data log only holds committed messages. So the end of the data log on the leader is the safe end. A new leader waits until it has applied everything from its own term before it serves reads.

**7. How do consumer groups make sure two consumers never own one partition?**
With a generation number. It is like the version of a seating chart. Every join, leave or crash makes a new generation. The coordinator makes the whole new chart and saves it in Raft with one command: "this is the new chart, built on generation N". If someone else changed it first, the command is rejected and the coordinator tries again. Every offset commit also carries the generation. The check happens inside Raft, in order. So an old consumer cannot write over the new owner.

**8. What is a zombie consumer?**
A consumer that was frozen, for example a long pause. Its partitions went to someone else. When it wakes up, it does not know. It tries to commit its old offsets. The commit has the old generation. Ember rejects it. The zombie gets an error, drops its state and joins again. The new owner's offsets are not touched.

**9. Why do heartbeats not go through Raft?**
A consumer sends a heartbeat every few seconds. With many consumers, that is many writes. Each Raft write is an fsync on a majority. The log would be full of entries that are worth nothing one minute later. So the coordinator keeps heartbeat times in memory. Only changes of the group and offset commits go to Raft. The price: if the coordinator dies, the new one does not know the times. It gives everyone a full new session. So a dead consumer is found up to one session later.

**10. What is at-least-once, and where do duplicates come from?**
At-least-once means: a message is never lost, but it can arrive twice. Duplicates come from three places. One: a producer sends, gets a timeout, and sends again, but the first one had worked. Two: a consumer processes a message, then crashes before it commits. The next consumer starts from the saved place and does it again. Three: a rebalance. In my chaos runs, about 5 to 15 percent of deliveries were duplicates when I killed things on purpose. Ember does not do exactly-once.

**11. How did you test it?**
In layers. Unit tests for the storage and the codecs. A simulator for the Raft core: it runs thousands of random bad schedules and checks the Raft safety rules after each event. Integration tests with real brokers and real sockets. And a chaos tool: it runs a workload while it kills, freezes and isolates brokers. It writes a history of what clients saw. A separate checker reads the history and checks nine rules.

**12. What bugs did you find?**
Real ones. A few examples. One: `/health/ready` said "ready" on a broker that had applied nothing. Two: a consumer could skip messages if the second partition's fetch failed after the first one worked. Three: a consumer that joined right after its topic was created could get no partitions for ever. Four: my chaos checker raised a false alarm, because my own recorder wrote a commit that never happened. Five: Raft messages and client requests shared one thread pool, and that caused a deadlock. Finding these is the reason the tools exist.

**13. What is coordinated omission?**
A benchmark mistake. The tester sends the next request only after the last one is done. If the server freezes for 10 seconds, the test records one slow request. But real users keep arriving. They would all have waited. The test "omitted" them. My fixed-rate mode sends on a schedule and measures from the time the request should have been sent. A stall then shows up in many requests, as it should.

**14. How does the checker work? Why do you trust it?**
It is a pure function: history in, list of violations out. It checks nine rules, for example: no saved message is lost, no two reads disagree, per-key order is kept, all copies are equal at the end. I do not trust it by default. I tested it with mutations: I took a good history, put in one bug, and checked that the checker finds it. I did this for every kind of bug. It found all of them. And the untouched history gives zero.

**15. What can the chaos test NOT prove?**
It is a test, not a proof. It sees one run. A bug that needs a rare timing can hide. The seed repeats the list of faults, not the exact timing. It cannot see things no client read. It does not test power loss or bad disks. It breaks one broker at a time, so a majority is always alive. These are real limits.

**16. What would break at scale?**
Several things. One coordinator handles all consumer groups. Kafka spreads groups over many brokers. The Raft log never shrinks, because I have no snapshots. So disk grows and restart gets slow. Every partition is its own Raft group, and every broker is in every group. So ten thousand partitions would mean a lot of timers and messages. No adding or removing brokers. One request at a time per connection. These are known limits.

**17. How is it different from Kafka?**
Kafka has an ISR list, and a separate controller (ZooKeeper or KRaft). Ember uses one idea, Raft, for everything. Kafka has many more features: compaction, transactions, exactly-once, quotas, security, tiered storage. Kafka scales to much bigger clusters. Ember is small and I can explain every line. Ember also has strong rules: it prefers to stop than to lose a saved message.

**18. What does `fsync` cost, and why keep it on?**
Every saved message needs an fsync on a majority. In my tests, one offset commit takes about 8 ms with fsync, and about 0.26 ms without. Without fsync, a power cut can lose messages that Ember already said were saved. So fsync is on by default. I have a flag to turn it off, `--unsafe-no-raft-fsync`. The name says why. I use it only for benchmarks.

**19. What is a partition key and why does it matter?**
The key decides the partition. Same key, same partition. A partition keeps order. So all messages with one key stay in order. Across partitions there is no order. The producer uses a hash (murmur2) on the key.

**20. What happens if two brokers die?**
With three brokers, there is no majority. Nothing can be saved. Produces fail with a timeout or "not available". Nothing wrong is saved. Nothing is lost. When one broker comes back, a majority exists again, and the cluster goes on. I have a test for this: during the outage, zero messages are acknowledged.

**21. What is CheckQuorum?**
A leader that is cut off from everyone can think it is still the leader. CheckQuorum makes it step down if it has not heard from a majority for one election timeout. Without it, a cut-off leader could answer reads with old data for a long time.

**22. How do clients find the right broker?**
The client knows a list of brokers to start. It asks one for metadata. The answer lists every broker and the leader of each partition. The client sends produce and fetch to the partition leader. If it gets "not leader", it asks again. For consumer groups, it asks "who is the coordinator?" first. That broker is the leader of one special Raft group.

**23. What is the advertised address?**
A broker can have different names for different people. Inside Docker, others call it `ember2:9092`. From my laptop, it is `localhost:19093`. The metadata must tell clients the name that works for the client. So each broker has an internal address (for Raft) and an advertised address (for clients). Ember supports one advertised address per broker. Kafka supports several listeners.

**24. What did you learn?**
Three things. One: distributed bugs live in the glue code, not in the algorithm. The Raft core was fine; the thread pool and the readiness check were not. Two: a check is only as good as its input. My first alarm was my own bad recording. Three: if I cannot say what a number means, I should not report it. A benchmark that includes `close()` in its timer gave me a wrong result until I saw it.

**25. What would you build next?**
Raft snapshots and log compaction, so the log does not grow for ever. An idempotent producer, so retries do not make duplicates. Sharded group coordinators. Cooperative rebalancing, to cut duplicates. Adding and removing brokers. Tiered storage. Then, security: TLS and logins.
