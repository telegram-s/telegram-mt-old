package org.telegram.mtproto.transport;

import org.telegram.mtproto.log.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

/**
 * Author: Korshakov Stepan
 * Created: 13.08.13 14:56
 */
public class TcpContext {

    private class Package {
        public Package() {

        }

        private Package(byte[] data, boolean useFastConfirm) {
            this.data = data;
            this.useFastConfirm = useFastConfirm;
        }

        public byte[] data;
        public boolean useFastConfirm;
    }

    private final String TAG;

    private static final AtomicInteger contextLastId = new AtomicInteger(1);

    private static final int READ_TIMEOUT = 1000;

    private final String ip;
    private final int port;
    private final boolean useChecksum;

    private int sentPackets;
    private int receivedPackets;

    private boolean isClosed;
    private boolean isBroken;

    private Socket socket;

    private ReaderThread readerThread;

    private WriterThread writerThread;

    private TcpContextCallback callback;

    private final int contextId;

    public TcpContext(String ip, int port, boolean checksum, TcpContextCallback callback) throws IOException {
        this.TAG = "Transport#" + hashCode();
        this.contextId = contextLastId.incrementAndGet();
        this.ip = ip;
        this.port = port;
        this.useChecksum = checksum;
        this.socket = new Socket(ip, port);
        this.socket.setKeepAlive(true);
        this.socket.setTcpNoDelay(true);
        if (!useChecksum) {
            socket.getOutputStream().write(0xef);
        }
        this.isClosed = false;
        this.isBroken = false;
        this.callback = callback;
        this.readerThread = new ReaderThread();
        this.writerThread = new WriterThread();
        this.readerThread.start();
        this.writerThread.start();
    }

    public int getContextId() {
        return contextId;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public boolean isUseChecksum() {
        return useChecksum;
    }

    public int getSentPackets() {
        return sentPackets;
    }

    public int getReceivedPackets() {
        return receivedPackets;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public boolean isBroken() {
        return isBroken;
    }

    public void postMessage(byte[] data, boolean useFastConfirm) {
        writerThread.pushPackage(new Package(data, useFastConfirm));
    }

    public synchronized void close() {
        if (!isClosed) {
            Logger.w(TAG, "Manual context closing");
            isClosed = true;
            isBroken = false;
            try {
                readerThread.interrupt();
            } catch (Exception e) {
                Logger.t(TAG, e);
            }
            try {
                writerThread.interrupt();
            } catch (Exception e) {
                Logger.t(TAG, e);
            }
        }
    }

    private synchronized void onMessage(byte[] data) {
        if (isClosed) {
            return;
        }

        callback.onRawMessage(data, this);
    }

    private synchronized void onError(int errorCode) {
        if (isClosed) {
            return;
        }

        callback.onError(errorCode, this);
    }

    private synchronized void breakContext() {
        if (!isClosed) {
            Logger.w(TAG, "Breaking context");
            isClosed = true;
            isBroken = true;
            try {
                readerThread.interrupt();
            } catch (Exception e) {
                Logger.t(TAG, e);
            }
            try {
                writerThread.interrupt();
            } catch (Exception e) {
                Logger.t(TAG, e);
            }
        }

        callback.onChannelBroken(this);
    }

    private class ReaderThread extends Thread {
        private ReaderThread() {
            setPriority(Thread.MIN_PRIORITY);
        }

        @Override
        public void run() {
            try {
                while (!isClosed && !isInterrupted()) {
                    try {
                        if (socket.isClosed()) {
                            breakContext();
                            return;
                        }
                        if (!socket.isConnected()) {
                            breakContext();
                            return;
                        }

                        InputStream stream = socket.getInputStream();

                        byte[] pkg = null;
                        if (useChecksum) {
                            int length = readInt(stream);
                            if (arrayEq(intToBytes(length), "HTTP".getBytes())) {
                                Logger.d(TAG, "Received HTTP package");
                                breakContext();
                                return;
                            }
                            Logger.d(TAG, "Start reading message: " + length);
                            if (length == 0) {
                                breakContext();
                                return;
                            }

                            if ((length >> 31) != 0) {
                                Logger.d(TAG, "fast confirm: " + length);
                                callback.onFastConfirm(length);
                                continue;
                            }

                            //1 MB
                            if (length >= 1024 * 1024 * 1024) {
                                Logger.d(TAG, "Too big package");
                                breakContext();
                                return;
                            }

                            int packetIndex = readInt(stream);
                            if (length == 4) {
                                onError(packetIndex);
                                Logger.d(TAG, "Received error");
                                breakContext();
                                return;
                            }
                            pkg = readBytes(length - 12, stream);
                            int readCrc = readInt(stream);
                            CRC32 crc32 = new CRC32();
                            crc32.update(intToBytes(length));
                            crc32.update(intToBytes(packetIndex));
                            crc32.update(pkg);
                            if (readCrc != (int) crc32.getValue()) {
                                Logger.d(TAG, "Incorrect CRC");
                                breakContext();
                                return;
                            }

                            if (receivedPackets != packetIndex) {
                                Logger.d(TAG, "Incorrect packet index");
                                breakContext();
                                return;
                            }

                            receivedPackets++;
                        } else {
                            int headerLen = readByte(stream);

                            if (headerLen == 0x7F) {
                                headerLen = readByte(stream) + (readByte(stream) << 8) + (readByte(stream) << 16);
                            }
                            int len = headerLen * 4;
                            pkg = readBytes(len, READ_TIMEOUT, stream);
                        }
                        if (pkg.length == 4) {
                            int message = readInt(pkg);
                            onError(message);
                        } else {
                            try {
                                onMessage(pkg);
                            } catch (Exception e) {
                                Logger.t(TAG, e);
                                Logger.d(TAG, "Message processing error");
                                breakContext();
                            }
                        }
                    } catch (IOException e) {
                        Logger.t(TAG, e);
                        breakContext();
                        return;
                    }
                }
            } catch (Exception e) {
                Logger.t(TAG, e);
                breakContext();
            }
        }
    }

    private class WriterThread extends Thread {
        private final ConcurrentLinkedQueue<Package> packages = new ConcurrentLinkedQueue<Package>();

        public void pushPackage(Package p) {
            packages.add(p);
            synchronized (packages) {
                packages.notifyAll();
            }
        }

        @Override
        public void run() {
            while (!isBroken) {
                Package p;
                synchronized (packages) {
                    p = packages.poll();
                    if (p == null) {
                        try {
                            packages.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            return;
                        }
                        p = packages.poll();
                    }
                }
                if (p == null) {
                    if (isBroken) {
                        return;
                    } else {
                        continue;
                    }
                }

                try {

                    byte[] data = p.data;
                    boolean useConfimFlag = p.useFastConfirm;

                    if (useChecksum) {
                        OutputStream stream = socket.getOutputStream();
                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

                        int len = data.length + 12;
                        if (useConfimFlag) {
                            len |= (1 << 31);
                        }
                        writeInt(len, outputStream);
                        writeInt(sentPackets, outputStream);
                        writeByteArray(data, outputStream);
                        CRC32 crc32 = new CRC32();
                        crc32.update(intToBytes(len));
                        crc32.update(intToBytes(sentPackets));
                        crc32.update(data);
                        writeInt((int) (crc32.getValue() & 0xFFFFFFFF), outputStream);
                        writeByteArray(outputStream.toByteArray(), stream);
                        stream.flush();
                    } else {
                        OutputStream stream = socket.getOutputStream();
                        if (data.length / 4 >= 0x7F) {
                            int len = data.length / 4;
                            writeByte(0x7F, stream);
                            writeByte(len & 0xFF, stream);
                            writeByte((len >> 8) & 0xFF, stream);
                            writeByte((len >> 16) & 0xFF, stream);
                        } else {
                            writeByte(data.length / 4, stream);
                        }
                        writeByteArray(data, stream);
                        stream.flush();
                    }
                    sentPackets++;
                } catch (Exception e) {
                    Logger.t(TAG, e);
                    breakContext();
                }
            }
        }
    }


    public static void writeByteArray(byte[] data, OutputStream stream) throws IOException {
        stream.write(data);
    }

    public static byte[] intToBytes(int value) {
        return new byte[]{
                (byte) (value & 0xFF),
                (byte) ((value >> 8) & 0xFF),
                (byte) ((value >> 16) & 0xFF),
                (byte) ((value >> 24) & 0xFF)};
    }

    public static void writeInt(int value, OutputStream stream) throws IOException {
        stream.write((byte) (value & 0xFF));
        stream.write((byte) ((value >> 8) & 0xFF));
        stream.write((byte) ((value >> 16) & 0xFF));
        stream.write((byte) ((value >> 24) & 0xFF));
    }

    public static void writeByte(int v, OutputStream stream) throws IOException {
        stream.write(v);
    }

    public static void writeByte(byte v, OutputStream stream) throws IOException {
        stream.write(v);
    }

    public static byte[] readBytes(int count, int timeout, InputStream stream) throws IOException {
        byte[] res = new byte[count];
        int offset = 0;
        long start = System.nanoTime();
        while (offset < res.length) {
            int readed = stream.read(res, offset, res.length - offset);
            if (readed > 0) {
                offset += readed;
            } else {
                if (System.nanoTime() - start > timeout * 1000000L) {
                    throw new IOException();
                }
                Thread.yield();
            }
        }
        return res;
    }

    public static byte[] readBytes(int count, InputStream stream) throws IOException {
        byte[] res = new byte[count];
        int offset = 0;
        while (offset < res.length) {
            int readed = stream.read(res, offset, res.length - offset);
            if (readed > 0) {
                offset += readed;
            } else if (readed < 0) {
                throw new IOException();
            } else {
                Thread.yield();
            }
        }
        return res;
    }

    public static int readInt(InputStream stream) throws IOException {
        int a = stream.read();
        if (a < 0) {
            throw new IOException();
        }
        int b = stream.read();
        if (b < 0) {
            throw new IOException();
        }
        int c = stream.read();
        if (c < 0) {
            throw new IOException();
        }
        int d = stream.read();
        if (d < 0) {
            throw new IOException();
        }

        return a + (b << 8) + (c << 16) + (d << 24);
    }

    public static int readInt(byte[] src) {
        return readInt(src, 0);
    }

    public static int readInt(byte[] src, int offset) {
        int a = src[offset + 0] & 0xFF;
        int b = src[offset + 1] & 0xFF;
        int c = src[offset + 2] & 0xFF;
        int d = src[offset + 3] & 0xFF;

        return a + (b << 8) + (c << 16) + (d << 24);
    }

    public static int readByte(InputStream stream) throws IOException {
        int res = stream.read();
        if (res < 0) {
            throw new IOException();
        }
        return res;
    }

    public static boolean arrayEq(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i])
                return false;
        }
        return true;
    }
}