package com.ember.broker;

import com.ember.protocol.*;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;

/** A bare-bones socket client for protocol-level tests (garbage, pipelining, oversize frames). */
final class RawClient implements AutoCloseable {

    private final Socket socket;
    private final OutputStream out;
    private final DataInputStream in;

    RawClient(int port) throws IOException {
        socket = new Socket("localhost", port);
        socket.setSoTimeout(10_000);                     // a hung test fails instead of hanging forever
        out = socket.getOutputStream();
        in = new DataInputStream(socket.getInputStream());
    }

    void sendRaw(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    void send(int correlationId, Request request) throws IOException {
        send(correlationId, (short) 0, request);
    }

    void send(int correlationId, short apiVersion, Request request) throws IOException {
        sendFrame(RequestCodec.encodeFrame(new RequestHeader(request.apiKey(), apiVersion, correlationId), request));
    }

    void sendFrame(ByteBuffer frame) throws IOException {
        byte[] bytes = new byte[frame.remaining()];
        frame.get(bytes);
        sendRaw(bytes);
    }

    /** Sends an arbitrary header + body with a correct length prefix. */
    void sendRawBody(int apiKey, int version, int correlationId, byte[] body) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4 + 8 + body.length);
        buf.putInt(8 + body.length).putShort((short) apiKey).putShort((short) version).putInt(correlationId).put(body);
        buf.flip();
        sendFrame(buf);
    }

    /** Reads one response frame body (the bytes after the length). */
    ByteBuffer readFrame() throws IOException {
        int length = in.readInt();
        byte[] body = new byte[length];
        in.readFully(body);
        return ByteBuffer.wrap(body);
    }

    ResponseCodec.Decoded read(ApiKey api) throws IOException {
        return ResponseCodec.decode(api, readFrame());
    }

    ResponseCodec.Decoded read(ApiKey api, short apiVersion) throws IOException {
        return ResponseCodec.decode(api, apiVersion, readFrame());
    }

    /** True once the broker has closed the connection (EOF or reset). */
    boolean awaitClosed() throws IOException {
        try {
            return in.read() == -1;
        } catch (SocketException e) {
            return true;
        } catch (EOFException e) {
            return true;
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
