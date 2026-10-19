package com.ember.client;

import com.ember.protocol.*;
import com.ember.storage.LogRecord;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Blocking client: one request in flight at a time (all methods are synchronized).
 * After ANY I/O or framing problem the connection is closed, because we can no longer tell
 * which response belongs to which request. Create a new client to continue.
 */
public class EmberClient implements AutoCloseable {

    private final ClientConfig config;
    private final Socket socket;
    private final OutputStream out;
    private final DataInputStream in;
    private int nextCorrelationId = 1;

    public EmberClient(String host, int port) throws IOException {
        this(host, port, ClientConfig.defaults());
    }

    public EmberClient(String host, int port, ClientConfig config) throws IOException {
        this.config = config;
        this.socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(config.readTimeoutMs());
            socket.connect(new InetSocketAddress(host, port), config.connectTimeoutMs());
            this.out = socket.getOutputStream();
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
        } catch (IOException e) {
            socket.close();
            throw e;
        }
    }

    /** Phase 2 convenience: produce to partition 0. */
    public synchronized long produce(String topic, List<ProducerRecord> records) throws IOException {
        return produce(topic, 0, records);
    }

    /** Sends the records to one partition and returns the offset of the first one (the rest follow contiguously). */
    public synchronized long produce(String topic, int partition, List<ProducerRecord> records) throws IOException {
        ProduceResponse r = (ProduceResponse) call(new ProduceRequest(topic, partition, records), (short) 0);
        if (r.recordCount() != records.size()) {
            throw fail("broker acknowledged " + r.recordCount() + " records but we sent " + records.size());
        }
        return r.baseOffset();
    }

    public synchronized FetchResult fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        FetchResponse r = (FetchResponse) call(new FetchRequest(topic, partition, offset, maxBytes), (short) 0);
        List<LogRecord> records;
        try {
            records = RecordDecoder.decodeAll(r.records());      // verifies every CRC
        } catch (RuntimeException e) {
            throw fail("corrupt records in fetch response: " + e.getMessage());
        }
        if (!records.isEmpty() && records.get(0).offset() != offset) {
            throw fail("asked for offset " + offset + " but the first record is " + records.get(0).offset());
        }
        return new FetchResult(offset, r.logStartOffset(), r.highWatermark(), records);
    }

    /** Speaks METADATA v1: every partition comes with its leader. No topics means "all topics". */
    public synchronized MetadataResponse metadata(String... topics) throws IOException {
        return (MetadataResponse) call(new MetadataRequest(List.of(topics)), (short) 1);
    }

    /** METADATA v2: all brokers, the controller, and every partition's leader and replicas. */
    public synchronized com.ember.protocol.ClusterMetadataResponse clusterMetadata(String... topics) throws IOException {
        return (com.ember.protocol.ClusterMetadataResponse) call(new MetadataRequest(List.of(topics)), (short) 2);
    }

    /** Creates a topic; throws EmberException(TOPIC_ALREADY_EXISTS) if it exists. Returns its partition count. */
    public synchronized int createTopic(String topic, int partitions) throws IOException {
        return ((CreateTopicResponse) call(new CreateTopicRequest(topic, partitions), (short) 0)).partitionCount();
    }

    private Response call(Request request, short apiVersion) throws IOException {
        int correlationId = nextCorrelationId++;
        byte[] bytes;
        try {
            ByteBuffer frame = RequestCodec.encodeFrame(new RequestHeader(request.apiKey(), apiVersion, correlationId), request);
            bytes = new byte[frame.remaining()];
            frame.get(bytes);
        } catch (ProtocolException e) {
            // our own caller's mistake (e.g. a bad topic name): nothing was sent, the connection is fine
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        try {
            out.write(bytes);
            out.flush();

            int length = in.readInt();
            if (length < 6 || length > config.maxFrameBytes()) {         // 6 = correlationId + errorCode
                throw fail("response frame length " + length + " is outside 6.." + config.maxFrameBytes());
            }
            byte[] body = new byte[length];
            in.readFully(body);

            ResponseCodec.Decoded decoded = ResponseCodec.decode(request.apiKey(), apiVersion, ByteBuffer.wrap(body));
            if (decoded.correlationId() != correlationId) {
                throw fail("correlation id mismatch: sent " + correlationId + " got " + decoded.correlationId());
            }
            if (decoded.errorCode() != ErrorCode.NONE) {
                throw new EmberException(decoded.errorCode());           // the connection is still in sync
            }
            return decoded.body();
        } catch (EmberException e) {
            throw e;
        } catch (IOException e) {
            closeQuietly();
            throw e;
        } catch (ProtocolException e) {
            throw fail("malformed response: " + e.getMessage());
        }
    }

    private IOException fail(String message) {
        closeQuietly();
        return new IOException(message);
    }

    private void closeQuietly() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // nothing more we can do
        }
    }

    @Override
    public synchronized void close() throws IOException {
        socket.close();
    }
}
