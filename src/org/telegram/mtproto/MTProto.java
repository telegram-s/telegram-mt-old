package org.telegram.mtproto;

import org.telegram.mtproto.log.Logger;
import org.telegram.mtproto.pq.PqAuth;
import org.telegram.mtproto.schedule.PreparedPackage;
import org.telegram.mtproto.schedule.Scheduller;
import org.telegram.mtproto.secure.Entropy;
import org.telegram.mtproto.time.TimeOverlord;
import org.telegram.mtproto.tl.*;
import org.telegram.mtproto.transport.TcpContext;
import org.telegram.mtproto.transport.TcpContextCallback;
import org.telegram.tl.DeserializeException;
import org.telegram.tl.StreamingUtils;
import org.telegram.tl.TLObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.telegram.mtproto.secure.CryptoUtils.*;
import static org.telegram.tl.StreamingUtils.*;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 03.11.13
 * Time: 8:14
 */
public class MTProto {
    private static final String TAG = "MTProto";

    private static final int SCHEDULLER_TIMEOUT = 15 * 1000;//15 sec

    private static final int ERROR_MSG_ID_TOO_SMALL = 16;
    private static final int ERROR_MSG_ID_TOO_BIG = 17;
    private static final int ERROR_MSG_ID_BITS = 18;
    private static final int ERROR_CONTAINER_MSG_ID_INCORRECT = 19;
    private static final int ERROR_TOO_OLD = 20;
    private static final int ERROR_SEQ_NO_TOO_SMALL = 32;
    private static final int ERROR_SEQ_NO_TOO_BIG = 33;
    private static final int ERROR_SEQ_EXPECTED_EVEN = 34;
    private static final int ERROR_SEQ_EXPECTED_ODD = 35;
    private static final int ERROR_BAD_SERVER_SALT = 48;
    private static final int ERROR_BAD_CONTAINER = 64;

    private MTProtoContext protoContext;

    private int desiredConnectionCount;
    private final HashSet<TcpContext> contexts = new HashSet<TcpContext>();
    private TcpContextCallback tcpListener;
    private ConnectionFixerThread connectionFixerThread;

    private final Scheduller scheduller;
    private SchedullerThread schedullerThread;

    private final ConcurrentLinkedQueue<MTMessage> inQueue = new ConcurrentLinkedQueue<MTMessage>();
    private ResponseProcessor responseProcessor;

    private byte[] authKey;
    private byte[] authKeyId;
    private byte[] serverSalt;
    private byte[] session;

    private boolean isClosed;

    private String address;
    private int port;

    private MTProtoCallback callback;

    public MTProto(byte[] authKey, byte[] serverSalt, String address, int port, MTProtoCallback callback) {
        this.authKey = authKey;
        this.serverSalt = serverSalt;
        this.address = address;
        this.port = port;
        this.callback = callback;
        this.authKeyId = substring(SHA1(authKey), 12, 8);
        this.protoContext = new MTProtoContext();
        this.desiredConnectionCount = 2;
        this.session = Entropy.generateSeed(8);
        this.tcpListener = new TcpListener();
        this.connectionFixerThread = new ConnectionFixerThread();
        this.connectionFixerThread.start();
        this.scheduller = new Scheduller();
        this.schedullerThread = new SchedullerThread();
        this.schedullerThread.start();
        this.responseProcessor = new ResponseProcessor();
        this.responseProcessor.start();
    }

    public void closeConnections() {
        synchronized (contexts) {
            for (TcpContext context : contexts) {
                context.close();
            }
            contexts.clear();
            contexts.notifyAll();
        }
    }

    public void sendMessage(TLObject request, long timeout) {
        scheduller.postMessage(request, timeout);
        synchronized (scheduller) {
            scheduller.notifyAll();
        }
    }

    private void onMTMessage(MTMessage mtMessage) {
        try {
            TLObject intMessage = protoContext.deserializeMessage(new ByteArrayInputStream(mtMessage.getContent()));
            onMTProtoMessage(mtMessage.getMessageId(), intMessage);
        } catch (DeserializeException e) {
            onApiMessage(mtMessage.getContent());
        } catch (IOException e) {
            e.printStackTrace();
            // ???
        }
    }

    public void onApiMessage(byte[] data) {
        Logger.d(TAG, "ApiMessage: " + Integer.toHexString(readInt(data)));
    }

    public void onMTProtoMessage(long msgId, TLObject object) {
        if (object instanceof MTBadMessage) {
            MTBadMessage badMessage = (MTBadMessage) object;
            Logger.d(TAG, "BadMessage: " + badMessage.getErrorCode());
            scheduller.onMessageConfirmed(badMessage.getBadMsgId());
            long time = scheduller.getMessageIdGenerationTime(badMessage.getBadMsgId());
            if (time != 0) {
                if (badMessage.getErrorCode() == ERROR_MSG_ID_TOO_BIG
                        || badMessage.getErrorCode() == ERROR_MSG_ID_TOO_SMALL) {
                    long delta = System.nanoTime() / 1000000 - time;
                    System.out.println("Package time: " + new Date((badMessage.getBadMsgId() >> 32) * 1000).toString());
                    System.out.println("New time: " + new Date((msgId >> 32) * 1000).toString());
                    TimeOverlord.getInstance().onForcedServerTimeArrived((msgId >> 32) * 1000, delta);
                    if (badMessage.getErrorCode() == ERROR_MSG_ID_TOO_BIG) {
                        scheduller.resetMessageId();
                    }
                    scheduller.resendAsNewMessage(badMessage.getBadMsgId());
                    requestSchedule();
                } else if (badMessage.getErrorCode() == ERROR_SEQ_NO_TOO_BIG || badMessage.getErrorCode() == ERROR_SEQ_NO_TOO_SMALL) {
                    if (scheduller.isMessageFromCurrentGeneration(badMessage.getBadMsgId())) {
                        session = Entropy.generateSeed(8);
                        scheduller.resetSession();
                    }
                    scheduller.resendAsNewMessage(badMessage.getBadMsgId());
                } else if (badMessage.getErrorCode() == ERROR_BAD_SERVER_SALT) {
                    serverSalt = ((MTBadServerSalt) badMessage).getNewSalt();
                    scheduller.resendMessage(badMessage.getBadMsgId());
                } else if (badMessage.getErrorCode() == ERROR_BAD_CONTAINER ||
                        badMessage.getErrorCode() == ERROR_CONTAINER_MSG_ID_INCORRECT) {
                    scheduller.resendMessage(badMessage.getBadMsgId());
                } else if (badMessage.getErrorCode() == ERROR_TOO_OLD) {
                    scheduller.resendAsNewMessage(badMessage.getBadMsgId());
                }
            }
        } else if (object instanceof MTMsgsAck) {
            MTMsgsAck ack = (MTMsgsAck) object;
            for (Long ackMsgId : ack.getMessages()) {
                scheduller.onMessageConfirmed(ackMsgId);
            }
        } else if (object instanceof MTRpcResult) {
            MTRpcResult result = (MTRpcResult) object;
            int responseConstructor = readInt(result.getContent());
            if (responseConstructor == MTRpcError.CLASS_ID) {
                try {
                    MTRpcError error = (MTRpcError) protoContext.deserializeMessage(result.getContent());
                    Logger.d(TAG, "rpc_error: " + result.getMessageId() + " " + error.getErrorCode() + ": " + error.getMessage());
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            } else {
                Logger.d(TAG, "rpc_result: " + result.getMessageId() + " #" + Integer.toHexString(responseConstructor));
            }
            scheduller.onMessageConfirmed(result.getMessageId());
            long time = scheduller.getMessageIdGenerationTime(result.getMessageId());
            if (time != 0) {
                long delta = System.nanoTime() / 1000000 - time;
                TimeOverlord.getInstance().onMethodExecuted(result.getMessageId(), msgId, delta);
            }
        } else if (object instanceof MTPong) {
            MTPong pong = (MTPong) object;
            Logger.d(TAG, "pong: " + pong.getPingId());
            scheduller.onMessageConfirmed(pong.getMessageId());
            long time = scheduller.getMessageIdGenerationTime(pong.getMessageId());
            if (time != 0) {
                long delta = System.nanoTime() / 1000000 - time;
                TimeOverlord.getInstance().onMethodExecuted(pong.getMessageId(), msgId, delta);
            }
        } else {
            Logger.d(TAG, object.toString());
        }
    }

    public void requestSchedule() {
        synchronized (scheduller) {
            scheduller.notifyAll();
        }
    }

    private byte[] encrypt(int seqNo, long messageId, byte[] content) throws IOException {
        ByteArrayOutputStream messageBody = new ByteArrayOutputStream();
        writeByteArray(serverSalt, messageBody);
        writeByteArray(session, messageBody);
        writeLong(messageId, messageBody);
        writeInt(seqNo, messageBody);
        writeInt(content.length, messageBody);
        writeByteArray(content, messageBody);

        byte[] innerData = messageBody.toByteArray();
        byte[] msgKey = substring(SHA1(innerData), 4, 16);
        int fastConfirm = readInt(SHA1(innerData)) | (1 << 31);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeByteArray(authKeyId, out);
        writeByteArray(msgKey, out);

        byte[] sha1_a = SHA1(msgKey, substring(authKey, 0, 32));
        byte[] sha1_b = SHA1(substring(authKey, 32, 16), msgKey, substring(authKey, 48, 16));
        byte[] sha1_c = SHA1(substring(authKey, 64, 32), msgKey);
        byte[] sha1_d = SHA1(msgKey, substring(authKey, 96, 32));

        byte[] aesKey = concat(substring(sha1_a, 0, 8), substring(sha1_b, 8, 12), substring(sha1_c, 4, 12));
        byte[] aesIv = concat(substring(sha1_a, 8, 12), substring(sha1_b, 0, 8), substring(sha1_c, 16, 4), substring(sha1_d, 0, 8));

        byte[] encoded = AES256IGEEncrypt(align(innerData, 16), aesIv, aesKey);
        writeByteArray(encoded, out);
        return out.toByteArray();
    }

    private MTMessage decrypt(byte[] data) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(data);
        byte[] msgAuthKey = readBytes(8, stream);
        for (int i = 0; i < authKeyId.length; i++) {
            if (msgAuthKey[i] != authKeyId[i]) {
                Logger.w(TAG, "Unsupported msgAuthKey");
                throw new SecurityException();
            }
        }
        byte[] msgKey = readBytes(16, stream);

        byte[] sha1_a = SHA1(msgKey, substring(authKey, 8, 32));
        byte[] sha1_b = SHA1(substring(authKey, 40, 16), msgKey, substring(authKey, 56, 16));
        byte[] sha1_c = SHA1(substring(authKey, 72, 32), msgKey);
        byte[] sha1_d = SHA1(msgKey, substring(authKey, 104, 32));

        byte[] aesKey = concat(substring(sha1_a, 0, 8), substring(sha1_b, 8, 12), substring(sha1_c, 4, 12));
        byte[] aesIv = concat(substring(sha1_a, 8, 12), substring(sha1_b, 0, 8), substring(sha1_c, 16, 4), substring(sha1_d, 0, 8));

        int totalLen = data.length - 8 - 16;
        byte[] encMessage = readBytes(totalLen, stream);

        byte[] rawMessage = AES256IGEDecrypt(encMessage, aesIv, aesKey);

        ByteArrayInputStream bodyStream = new ByteArrayInputStream(rawMessage);
        byte[] serverSalt = readBytes(8, bodyStream);
        byte[] session = readBytes(8, bodyStream);
        long messageId = readLong(bodyStream);
        int mes_seq = StreamingUtils.readInt(bodyStream);

        int msg_len = StreamingUtils.readInt(bodyStream);
        byte[] message = readBytes(msg_len, bodyStream);

        if (!arrayEq(session, this.session)) {
            throw new TransportSecurityException();
        }

        return new MTMessage(messageId, mes_seq, message);
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

    private class SchedullerThread extends Thread {
        private SchedullerThread() {
            setName("Scheduller#" + hashCode());
        }

        @Override
        public void run() {
            while (!isClosed) {
                synchronized (scheduller) {
                    if (contexts.size() == 0) {
                        try {
                            scheduller.wait(SCHEDULLER_TIMEOUT);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }

                TcpContext context = null;
                synchronized (contexts) {
                    TcpContext[] currentContexts = contexts.toArray(new TcpContext[0]);
                    if (currentContexts.length != 0) {
                        context = currentContexts[0];
                    } else {
                        continue;
                    }
                }

                synchronized (scheduller) {
                    PreparedPackage preparedPackage = scheduller.doSchedule();
                    if (preparedPackage == null) {
                        try {
                            scheduller.wait(SCHEDULLER_TIMEOUT);
                        } catch (InterruptedException e) {
                            return;
                        }
                        continue;
                    }

                    try {
                        byte[] data = encrypt(preparedPackage.getSeqNo(), preparedPackage.getMessageId(), preparedPackage.getContent());
                        if (!context.isClosed()) {
                            context.postMessage(data, false);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private class ResponseProcessor extends Thread {
        public ResponseProcessor() {
            setName("ResponseProcessor#" + hashCode());
        }

        @Override
        public void run() {
            while (!isClosed) {
                synchronized (inQueue) {
                    if (inQueue.isEmpty()) {
                        try {
                            inQueue.wait();
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                    if (inQueue.isEmpty()) {
                        continue;
                    }
                }
                MTMessage message = inQueue.poll();
                onMTMessage(message);
            }
        }
    }

    private class ConnectionFixerThread extends Thread {
        private ConnectionFixerThread() {
            setName("ConnectionFixerThread#" + hashCode());
        }

        @Override
        public void run() {
            while (!isClosed) {
                synchronized (contexts) {
                    if (contexts.size() >= desiredConnectionCount) {
                        try {
                            contexts.wait();
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }

                try {
                    TcpContext context = new TcpContext(address, port, false, tcpListener);
                    if (isClosed) {
                        return;
                    }
                    synchronized (contexts) {
                        contexts.add(context);
                    }
                    synchronized (scheduller) {
                        scheduller.notifyAll();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }
    }

    private class TcpListener implements TcpContextCallback {

        @Override
        public void onRawMessage(byte[] data, TcpContext context) {
            if (isClosed) {
                return;
            }
            try {
                MTMessage decrypted = decrypt(data);
                if (readInt(decrypted.getContent()) == MTMessagesContainer.CLASS_ID) {
                    try {
                        TLObject object = protoContext.deserializeMessage(new ByteArrayInputStream(decrypted.getContent()));
                        if (object instanceof MTMessagesContainer) {
                            for (MTMessage mtMessage : ((MTMessagesContainer) object).getMessages()) {
                                inQueue.add(mtMessage);
                                synchronized (inQueue) {
                                    inQueue.notifyAll();
                                }
                            }
                        }
                    } catch (DeserializeException e) {
                        // Ignore this
                        Logger.t(TAG, e);
                    }
                } else {
                    inQueue.add(decrypted);
                    synchronized (inQueue) {
                        inQueue.notifyAll();
                    }
                }
            } catch (IOException e) {
                Logger.t(TAG, e);
            }
        }

        @Override
        public void onError(int errorCode, TcpContext context) {
            if (isClosed) {
                return;
            }
        }

        @Override
        public void onChannelBroken(TcpContext context) {
            if (isClosed) {
                return;
            }
            synchronized (contexts) {
                contexts.remove(context);
                contexts.notifyAll();
            }
        }

        @Override
        public void onFastConfirm(int hash) {
            if (isClosed) {
                return;
            }
        }
    }
}
