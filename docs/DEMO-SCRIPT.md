# Demo script: 5 minutes

**Read this out loud. The sentences are short on purpose.**
Ember is my own message broker, like a small Kafka. I wrote it from zero in Java.

## Before the demo (do this once, not on stage)

1. Start Docker Desktop.
2. In the project folder, build once: `mvn -q package -DskipTests`
3. Open **three** terminal windows. In each one, type:
   ```
   alias ember='java -jar ember-client/target/ember-client-0.1.0-SNAPSHOT-all.jar'
   export B=localhost:19092,localhost:19093,localhost:19094
   export A=localhost:18081,localhost:18082,localhost:18083
   ```
4. Run the chaos tool once before the demo and keep its history file. You will check it at the end. (Command is in minute 5.)
5. Make the browser ready: Grafana at `http://localhost:13000` (start with `docker compose --profile observability up -d --wait`).

---

## Minute 1: Start the cluster

**Type (window 1):**
```
docker compose up -d --wait
ember cluster status --admin=$A
```

**The screen shows:** three brokers, all `ready`. A table with Raft groups. One leader for each group.

**Say:**
- "This is a cluster of three brokers. They run in Docker."
- "Each broker keeps a full copy of the data."
- "They use Raft. Raft means they vote for one leader. The others follow."
- "All three say `ready`. So the cluster is healthy."

---

## Minute 2: Make a topic and send messages

**Type (window 1):**
```
ember create-topic --topic=orders --partitions=3 --bootstrap=$B
ember produce --topic=orders --count=300 --keys=30 --value=order --bootstrap=$B
ember metadata --topic=orders --bootstrap=$B
```

**The screen shows:** `created topic orders with 3 partition(s)`, then `produced 300 records`, then a table. Each partition has a leader and replicas `1,2,3`.

**Say:**
- "A topic is like a list of messages. I split it into three parts. We call them partitions."
- "I sent 300 messages. Every message has a key. The same key always goes to the same partition. So the order of one key is kept."
- "Look at the table. Each partition has its own leader. The leaders are on different brokers."
- "A message is only said to be saved after most brokers have it on disk. Two out of three."

---

## Minute 3: A consumer group

**Type (window 2):**
```
ember consume --group=shop --topic=orders --follow --session-ms=3000 --bootstrap=$B
```
**Then (window 1):**
```
ember group describe --group=shop --bootstrap=$B
```

**The screen shows (window 2):** all 300 messages scroll by. **Window 1:** the group, one member, three partitions, `total lag: 0`.

**Say:**
- "A consumer group is a team of readers. They share the work."
- "Now there is one reader. It reads all three partitions."
- "Lag is zero. That means the reader is not behind."
- "The group saves its place on the broker. If the reader dies, a new one starts from the saved place."

---

## Minute 4: Kill the leader. Nothing is lost.

**Type (window 1) to start sending:**
```
ember produce --topic=orders --count=2000 --keys=50 --value=live --bootstrap=$B
```
**Wait about 5 seconds. Then type (window 3):**
```
ember metadata --topic=orders --bootstrap=$B | grep "orders  *0"
docker kill ember2
```
(Use the broker number that leads partition 0. In my run it was 2.)

**The screen shows:** the producer keeps going. Maybe one short pause. No error. After it ends, type (window 3):
```
ember cluster status --admin=$A
ember consume --topic=orders --from-beginning --bootstrap=$B | tail -1
```
It shows one broker `UNREACHABLE`, new leaders on the other two, and `(2300 records)`.

**Say:**
- "I send 2000 more messages. At the same time I kill the leader. This is like a crash. No goodbye."
- "The producer did not stop. It waited a moment, and it found the new leader."
- "Now look. Two brokers are alive. They chose new leaders by themselves."
- "I read everything back. 2300 messages. 300 plus 2000. Nothing is lost."
- "This is the main promise. If Ember said `saved`, the message is saved. Even if the leader dies."

**Then type:**
```
docker start ember2
```
**Say:** "I start the broker again. It catches up alone. No one helps it."

---

## Minute 5: Look inside, and the checker

**Do (browser):** open Grafana. Show the panels: produce rate, Raft role, leader changes.

**Say:**
- "This is the dashboard. It is my own metrics code. No library."
- "Here you can see the leader change. This line is the moment I killed the broker."

**Type (window 1):**
```
java -jar ember-chaos/target/ember-chaos-0.1.0-SNAPSHOT-all.jar --check-only=chaos-history.jsonl
```

**The screen shows:** `checked ... events: 0 violation(s)`.

**Say:**
- "Before the demo, I ran a chaos test for five minutes. It killed brokers, froze them and cut the network. About 100 thousand messages."
- "It wrote down everything the clients saw. This is the history file."
- "Now a separate checker reads that file. It does not trust the broker. It checks nine rules. For example: no saved message is lost. No two readers see different data. All copies are the same."
- "Zero violations."
- "I also tested the checker. I put bugs inside the history on purpose. It found all of them. So a green result means something."
- "That is Ember. Thank you."

**Clean up after the demo:** `docker compose --profile observability down -v`

---

## The 60-second version (the elevator pitch)

- "Ember is a message broker I built from zero in Java. It is like a small Kafka."
- "It keeps three copies of the data. It uses Raft to choose a leader."
- "A message is saved only when most brokers have it on disk."
- "I can kill the leader, and no saved message is lost. A new leader starts in about one second."
- "Consumers work in groups. They share the work and save their place."
- "To test it, I wrote a chaos tool. It breaks the cluster on purpose. A checker then reads what the clients saw. It found real bugs in my own code."
- "I can show you in five minutes."

---

## If something goes wrong during the demo

- **Docker is slow to start.** Say: "Docker takes a moment." Wait for `ready`. Do not type while it starts.
- **`ember cluster status` shows `NOT READY` for a short time.** This is normal after a start. Wait ten seconds. Say: "It is replaying its log. It says not ready until it is done. That is on purpose."
- **The producer shows an error after the kill.** It should not. Say: "The client gives up after many tries. Let me run it again." Then run the produce command again. The records that already arrived stay saved.
- **You killed the wrong broker (not the leader).** Say: "This one was a follower. Nothing changes. Now I kill the leader." Look at `ember metadata` again and kill the right one.
- **The count at the end is not exactly 2300.** It can be a little higher if you ran the produce twice. It must never be lower. Say: "A retry can make a duplicate. Ember promises no loss, not no duplicates. That is called at-least-once."
- **Grafana is empty.** The data needs 10 seconds. Refresh. Or skip it and show `ember cluster metrics --admin=localhost:18081 --filter=ember_raft_role`.
- **The checker shows a violation.** Do not hide it. Say: "The checker found a problem. This is why I built it." Show the first line. Then say you will fix it after the demo.
- **Port already in use.** Run `docker compose down -v` and start again.
