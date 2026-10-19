package com.ember.broker;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Exclusive lock on {@code <dataDir>/.ember.lock}. Two brokers appending to the same segment files
 * would silently corrupt them, so the second one must fail at startup. The OS drops the lock when
 * the process dies, so a crash never leaves a stale lock behind.
 */
final class DataDirLock implements AutoCloseable {

    /** Directories locked by THIS JVM. Checked first: see the note in acquire(). */
    private static final Set<Path> HELD_IN_THIS_JVM = ConcurrentHashMap.newKeySet();

    private final Path key;
    private final FileChannel channel;
    private final FileLock lock;
    private boolean released;

    private DataDirLock(Path key, FileChannel channel, FileLock lock) {
        this.key = key;
        this.channel = channel;
        this.lock = lock;
    }

    static DataDirLock acquire(Path dataDir) throws IOException {
        Files.createDirectories(dataDir);
        Path key = dataDir.toRealPath();
        if (!HELD_IN_THIS_JVM.add(key)) {
            // We never even open the file here: on POSIX systems closing ANY descriptor of a file can
            // release the lock the process holds on it, which would unlock the first broker.
            throw inUse(dataDir);
        }
        FileChannel channel = null;
        try {
            channel = FileChannel.open(key.resolve(".ember.lock"), CREATE, WRITE);
            FileLock lock = channel.tryLock();               // null: another process holds it
            if (lock == null) {
                throw inUse(dataDir);
            }
            return new DataDirLock(key, channel, lock);
        } catch (OverlappingFileLockException e) {           // same JVM, defensive: handled above already
            closeQuietly(channel);
            HELD_IN_THIS_JVM.remove(key);
            throw inUse(dataDir);
        } catch (IOException | RuntimeException e) {
            closeQuietly(channel);
            HELD_IN_THIS_JVM.remove(key);
            throw e;
        }
    }

    private static IllegalStateException inUse(Path dataDir) {
        return new IllegalStateException("another Ember broker is using this data directory: " + dataDir
                + " (stop it first, or pick a different --data-dir)");
    }

    @Override
    public synchronized void close() {
        if (released) {
            return;
        }
        released = true;
        try {
            lock.release();
        } catch (IOException ignored) {
            // closing the channel below releases it anyway
        }
        closeQuietly(channel);
        HELD_IN_THIS_JVM.remove(key);
    }

    private static void closeQuietly(FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // nothing useful to do
            }
        }
    }
}
