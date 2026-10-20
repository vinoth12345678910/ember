package com.ember.chaos;

import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static com.ember.chaos.HistoryRecorder.data;

/**
 * Collector for brokers we can only reach over the network (docker compose, separate processes): data-log checksums come from
 * the debug endpoint GET /debug/partition-checksum (brokers must run with --enable-debug-endpoints), group and topic state from
 * GET /status. Raft logs cannot be compared this way; that part of P9 needs the data directories.
 */
public final class ExternalCollector implements Collector {

    private final List<String> adminAddresses;       // host:port of the admin endpoint of broker 1..n
    private final String topic;
    private final int partitions;

    public ExternalCollector(List<String> adminAddresses, String topic, int partitions) {
        this.adminAddresses = List.copyOf(adminAddresses);
        this.topic = topic;
        this.partitions = partitions;
    }

    private Map<String, Object> checksum(String address, int partition) throws Exception {
        AdminClient.Response r = AdminClient.get(address, "/debug/partition-checksum?topic=" + topic + "&partition=" + partition);
        if (r.status() != 200) throw new IllegalStateException(address + " answered " + r.status() + " for the checksum (start brokers with --enable-debug-endpoints)");
        return Json.parseObject(r.body());
    }

    @SuppressWarnings("unchecked")
    private String groupStateText(String address) throws Exception {
        Map<String, Object> status = Json.parseObject(AdminClient.get(address, "/status").body());
        return Json.write(status.get("consumerGroups"));
    }

    @SuppressWarnings("unchecked")
    private String metaStateText(String address) throws Exception {
        Map<String, Object> status = Json.parseObject(AdminClient.get(address, "/status").body());
        StringBuilder sb = new StringBuilder();
        for (Object t : (List<Object>) status.get("topics")) {
            Map<String, Object> topicJson = (Map<String, Object>) t;
            sb.append(topicJson.get("name")).append('=').append(((List<Object>) topicJson.get("partitions")).size()).append(';');
        }
        return sb.toString();
    }

    @Override
    public boolean awaitConverged(long timeoutMs) {
        return InProcessCluster.await(timeoutMs, () -> {
            try {
                String reference = null;
                for (String address : adminAddresses) {
                    StringBuilder fp = new StringBuilder();
                    for (int p = 0; p < partitions; p++) {
                        Map<String, Object> c = checksum(address, p);
                        fp.append(c.get("startOffset")).append('-').append(c.get("endOffset")).append('-').append(c.get("crc32")).append(';');
                    }
                    fp.append(groupStateText(address));
                    if (reference == null) reference = fp.toString();
                    else if (!reference.equals(fp.toString())) return false;
                }
                return true;
            } catch (Exception e) {
                return false;
            }
        });
    }

    @Override
    public void collect(HistoryRecorder rec) throws Exception {
        int replica = 0;
        for (String address : adminAddresses) {
            replica++;
            for (int p = 0; p < partitions; p++) {
                Map<String, Object> c = checksum(address, p);
                rec.replica(data("scope", "data", "group", topic + "-" + p, "replica", Integer.toString(replica), "start", ((Number) c.get("startOffset")).longValue(),
                        "end", ((Number) c.get("endOffset")).longValue(), "crc", ((Number) c.get("crc32")).longValue()));
            }
            rec.replica(data("scope", "groupstate", "group", "groups", "replica", Integer.toString(replica), "digest", InProcessCollector.crc(groupStateText(address))));
            rec.replica(data("scope", "metastate", "group", "meta", "replica", Integer.toString(replica), "digest", InProcessCollector.crc(metaStateText(address))));
        }
    }
}
