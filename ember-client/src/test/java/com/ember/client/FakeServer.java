package com.ember.client;

import com.ember.protocol.RequestCodec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/** A one-connection server that lets a test script exactly what the "broker" answers. */
final class FakeServer implements AutoCloseable {

    interface Script {
        void run(Connection c) throws IOException, InterruptedException;
    }

    static final class Connection {
        final DataInputStream in;
        final DataOutputStream out;

        Connection(Socket s) throws IOException {
            in = new DataInputStream(s.getInputStream());
            out = new DataOutputStream(s.getOutputStream());
        }

        RequestCodec.Decoded readRequest() throws IOException {
            int length = in.readInt();
            byte[] body = new byte[length];
            in.readFully(body);
            return RequestCodec.decode(ByteBuffer.wrap(body));
        }

        void writeFrame(ByteBuffer body) throws IOException {
            out.writeInt(body.remaining());
            byte[] bytes = new byte[body.remaining()];
            body.get(bytes);
            out.write(bytes);
            out.flush();
        }
    }

    private final ServerSocket server;
    private final Thread thread;
    private final CountDownLatch release = new CountDownLatch(1);
    private volatile Throwable scriptFailure;

    FakeServer(Script script) throws IOException {
        server = new ServerSocket(0);
        thread = new Thread(() -> {
            try (Socket s = server.accept()) {
                script.run(new Connection(s));
                release.await();                       // keep the socket open until the test is over
            } catch (Throwable t) {
                if (!server.isClosed()) {
                    scriptFailure = t;
                }
            }
        }, "fake-server");
        thread.setDaemon(true);
        thread.start();
    }

    int port() {
        return server.getLocalPort();
    }

    @Override
    public void close() throws IOException {
        release.countDown();
        server.close();
    }
}
