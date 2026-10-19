package com.ember.broker;

import com.ember.protocol.TopicNames;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.*;

/**
 * Remembers each topic's partition count in {@code <dataDir>/_meta/<topic>.properties}.
 * Files are replaced atomically (temp file, fsync, rename): after a crash a reader sees either the
 * old complete file or the new complete file, never half of one.
 */
final class TopicMetaStore {

    static final String DIR_NAME = "_meta";
    private static final String SUFFIX = ".properties";
    private static final System.Logger LOG = System.getLogger(TopicMetaStore.class.getName());

    private final Path dir;

    TopicMetaStore(Path dataDir) throws IOException {
        this.dir = dataDir.resolve(DIR_NAME);
        Files.createDirectories(dir);
    }

    Path fileFor(String topic) {
        return dir.resolve(topic + SUFFIX);
    }

    boolean exists(String topic) {
        return Files.exists(fileFor(topic));
    }

    /** topic -> partition count, sorted by topic. Leftover temp files from a crash are removed. */
    Map<String, Integer> loadAll() throws IOException {
        Map<String, Integer> result = new TreeMap<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : files.toList()) {
                String name = file.getFileName().toString();
                if (name.endsWith(".tmp")) {
                    Files.deleteIfExists(file);            // a write that never reached its rename
                    continue;
                }
                if (!name.endsWith(SUFFIX)) {
                    LOG.log(System.Logger.Level.WARNING, "ignoring unexpected file in {0}: {1}", dir, name);
                    continue;
                }
                String topic = name.substring(0, name.length() - SUFFIX.length());
                if (!TopicNames.isValid(topic)) {
                    LOG.log(System.Logger.Level.WARNING, "ignoring metadata file with an invalid topic name: {0}", name);
                    continue;
                }
                result.put(topic, read(topic, file));
            }
        }
        return result;
    }

    private static int read(String topic, Path file) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(Files.readString(file, StandardCharsets.UTF_8)));
        try {
            int partitions = Integer.parseInt(props.getProperty("partitions", "").trim());
            if (partitions < 1) {
                throw new NumberFormatException();
            }
            return partitions;
        } catch (NumberFormatException e) {
            // refuse rather than guess: a wrong count silently remaps every key
            throw new IllegalStateException("metadata file for topic '" + topic + "' (" + file
                    + ") does not contain a valid 'partitions=N'; refusing to start");
        }
    }

    void write(String topic, int partitions) throws IOException {
        Path target = fileFor(topic);
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        byte[] bytes = ("partitions=" + partitions + "\n").getBytes(StandardCharsets.UTF_8);
        try (FileChannel ch = FileChannel.open(tmp, CREATE, TRUNCATE_EXISTING, WRITE)) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);                                  // the bytes are on disk BEFORE the rename makes them visible
        }
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        syncDirectory();
    }

    void delete(String topic) throws IOException {
        Files.deleteIfExists(fileFor(topic));
    }

    /** Makes the rename itself durable. Not supported on every platform, so best effort. */
    private void syncDirectory() {
        try (FileChannel ch = FileChannel.open(dir, READ)) {
            ch.force(true);
        } catch (IOException | RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, "could not fsync directory {0}: {1}", dir, e.toString());
        }
    }
}
