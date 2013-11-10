package org.telegram.mtproto;

import org.telegram.mtproto.log.Logger;
import org.telegram.mtproto.schedule.PreparedPackage;
import org.telegram.mtproto.schedule.Scheduller;
import org.telegram.mtproto.secure.Entropy;
import org.telegram.mtproto.state.AbsMTProtoState;
import org.telegram.mtproto.state.ConnectionInfo;
import org.telegram.mtproto.state.KnownSalt;
import org.telegram.mtproto.time.TimeOverlord;
import org.telegram.mtproto.tl.*;
import org.telegram.mtproto.transport.TcpContext;
import org.telegram.mtproto.transport.TcpContextCallback;
import org.telegram.tl.DeserializeException;
import org.telegram.tl.StreamingUtils;
import org.telegram.tl.TLMethod;
import org.telegram.tl.TLObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
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

    private static final int MESSAGES_CACHE = 100;
    private static final int MESSAGES_CACHE_MIN = 10;

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

    private static final int PING_TIMEOUT = 60 * 1000;
    private static final int RESEND_TIMEOUT = 60 * 1000;

    private static final int FUTURE_REQUEST_COUNT = 64;
    private static final int FUTURE_MINIMAL = 5;
    private static final long FUTURE_TIMEOUT = 30 * 60 * 1000;//30 secs

    private MTProtoContext protoContext;

    private int desiredConnectionCount;
    private final HashSet<TcpContext> contexts = new HashSet<TcpContext>();
    private TcpContextCallback tcpListener;
    private ConnectionFixerThread connectionFixerThread;

    private final Scheduller scheduller;
    private SchedullerThread schedullerThread;

    private final ConcurrentLinkedQueue<MTMessage> inQueue = new ConcurrentLinkedQueue<MTMessage>();
    private ResponseProcessor responseProcessor;

    private final ArrayList<Long> receivedMessages = new ArrayList<Long>();

    private byte[] authKey;
    private byte[] authKeyId;
    private byte[] session;

    private boolean isClosed;

    private MTProtoCallback callback;

    private AbsMTProtoState state;

    private long futureSaltsRequestedTime = Long.MIN_VALUE;
    private long futureSaltsRequestId = -1;

    private int roundRobin = 0;

    public MTProto(AbsMTProtoState state, MTProtoCallback callback, CallWrapper callWrapper) {
        this.state = state;
        this.callback = callback;
        this.authKey = state.getAuthKey();
        this.authKeyId = substring(SHA1(authKey), 12, 8);
        this.protoContext = new MTProtoContext();
        this.desiredConnectionCount = 2;
        this.session = Entropy.generateSeed(8);
        this.tcpListener = new TcpListener();
        this.connectionFixerThread = new ConnectionFixerThread();
        this.connectionFixerThread.start();
        this.scheduller = new Scheduller(callWrapper);
        this.schedullerThread = new SchedullerThread();
        this.schedullerThread.start();
        this.responseProcessor = new ResponseProcessor();
        this.responseProcessor.start();
    }

    public void close() {
        if (!isClosed) {
            this.isClosed = true;
            if (this.connectionFixerThread != null) {
                this.connectionFixerThread.interrupt();
            }
            if (this.schedullerThread != null) {
                this.schedullerThread.interrupt();
            }
            if (this.responseProcessor != null) {
                this.responseProcessor.interrupt();
            }
            closeConnections();
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void closeConnections() {
        synchronized (contexts) {
            for (TcpContext context : contexts) {
                context.close();
                scheduller.onConnectionDies(context.getContextId());
            }
            contexts.clear();
            contexts.notifyAll();
        }
    }

    private boolean needProcessing(long messageId) {
        synchronized (receivedMessages) {
            if (receivedMessages.contains(messageId)) {
                return false;
            }

            if (receivedMessages.size() > MESSAGES_CACHE_MIN) {
                boolean isSmallest = true;
                for (Long l : receivedMessages) {
                    if (messageId > l) {
                        isSmallest = false;
                        break;
                    }
                }

                if (isSmallest) {
                    return false;
                }
            }

            while (receivedMessages.size() >= MESSAGES_CACHE - 1) {
                receivedMessages.remove(0);
            }
            receivedMessages.add(messageId);
        }

        return true;
    }


    public int sendRpcMessage(TLMethod request, long timeout, boolean highPriority) {
        return sendMessage(request, timeout, true, highPriority);
    }

    public int sendMessage(TLObject request, long timeout, boolean isRpc, boolean highPriority) {
        int id = scheduller.postMessage(request, isRpc, timeout, highPriority);
        synchronized (scheduller) {
            scheduller.notifyAll();
        }

        if (futureSaltsRequestedTime - System.nanoTime() > FUTURE_TIMEOUT * 1000L) {
            int count = state.maximumCachedSalts((int) (TimeOverlord.getInstance().getServerTime() / 1000));
            if (count < FUTURE_MINIMAL) {
                futureSaltsRequestId = scheduller.postMessage(new MTGetFutureSalts(FUTURE_REQUEST_COUNT), false, FUTURE_TIMEOUT);
                futureSaltsRequestedTime = System.nanoTime();
            }
        }

        return id;
    }

    private void onMTMessage(MTMessage mtMessage) {
        if (mtMessage.getSeqNo() % 2 == 1) {
            scheduller.confirmMessage(mtMessage.getMessageId());
        }
        if (!needProcessing(mtMessage.getMessageId())) {
            Logger.d(TAG, "Ignoring messages #" + mtMessage.getMessageId());
            return;
        }
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

    private void onApiMessage(byte[] data) {
        callback.onApiMessage(data, this);
    }

    private void onMTProtoMessage(long msgId, TLObject object) {
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
                    requestSchedule();
                } else if (badMessage.getErrorCode() == ERROR_BAD_SERVER_SALT) {
                    long salt = ((MTBadServerSalt) badMessage).getNewSalt();
                    // Sync time
                    long delta = System.nanoTime() / 1000000 - time;
                    TimeOverlord.getInstance().onMethodExecuted(badMessage.getBadMsgId(), msgId, delta);
                    state.badServerSalt(salt);
                    scheduller.resendAsNewMessage(badMessage.getBadMsgId());
                    requestSchedule();
                } else if (badMessage.getErrorCode() == ERROR_BAD_CONTAINER ||
                        badMessage.getErrorCode() == ERROR_CONTAINER_MSG_ID_INCORRECT) {
                    scheduller.resendMessage(badMessage.getBadMsgId());
                    requestSchedule();
                } else if (badMessage.getErrorCode() == ERROR_TOO_OLD) {
                    scheduller.resendAsNewMessage(badMessage.getBadMsgId());
                    requestSchedule();
                }
            }
        } else if (object instanceof MTMsgsAck) {
            MTMsgsAck ack = (MTMsgsAck) object;
            for (Long ackMsgId : ack.getMessages()) {
                scheduller.onMessageConfirmed(ackMsgId);
                int id = scheduller.mapSchedullerId(ackMsgId);
                if (id > 0) {
                    callback.onConfirmed(id);
                }
            }
        } else if (object instanceof MTRpcResult) {
            MTRpcResult result = (MTRpcResult) object;

            int id = scheduller.mapSchedullerId(result.getMessageId());
            if (id > 0) {
                int responseConstructor = readInt(result.getContent());
                if (responseConstructor == MTRpcError.CLASS_ID) {
                    try {
                        MTRpcError error = (MTRpcError) protoContext.deserializeMessage(result.getContent());

                        if (error.getErrorCode() == 420) {
                            if (error.getErrorTag().startsWith("FLOOD_WAIT_")) {
                                // Secs
                                int delay = Integer.parseInt(error.getErrorTag().substring("FLOOD_WAIT_".length()));
                                scheduller.resendAsNewMessageDelayed(result.getMessageId(), delay * 1000);
                                requestSchedule();
                                return;
                            }
                        }
                        if (error.getErrorCode() == 401) {
                            if (error.getErrorTag().equals("AUTH_KEY_UNREGISTERED") ||
                                    error.getErrorTag().equals("AUTH_KEY_INVALID") ||
                                    error.getErrorTag().equals("USER_DEACTIVATED") ||
                                    error.getErrorTag().equals("SESSION_REVOKED") ||
                                    error.getErrorTag().equals("SESSION_EXPIRED")) {
                                Logger.w(TAG, "Auth key invalidated");
                                callback.onAuthInvalidated(this);
                                close();
                                return;
                            }
                        }

                        callback.onRpcError(id, error.getErrorCode(), error.getMessage(), this);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
                } else {
                    Logger.d(TAG, "rpc_result: " + result.getMessageId() + " #" + Integer.toHexString(responseConstructor));
                    callback.onRpcResult(id, result.getContent(), this);
                }
            } else {
                Logger.d(TAG, "ignored rpc_result: " + result.getMessageId());
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
        } else if (object instanceof MTFutureSalts) {
            MTFutureSalts salts = (MTFutureSalts) object;
            scheduller.onMessageConfirmed(salts.getRequestId());

            long time = scheduller.getMessageIdGenerationTime(salts.getRequestId());

            if (time > 0) {
                KnownSalt[] knownSalts = new KnownSalt[salts.getSalts().size()];
                for (int i = 0; i < knownSalts.length; i++) {
                    MTFutureSalt salt = salts.getSalts().get(i);
                    knownSalts[i] = new KnownSalt(salt.getValidSince(), salt.getValidUntil(), salt.getSalt());
                }

                long delta = System.nanoTime() / 1000000 - time;
                TimeOverlord.getInstance().onForcedServerTimeArrived(salts.getNow(), delta);
                state.mergeKnownSalts(salts.getNow(), knownSalts);
            }
        } else if (object instanceof MTMessageDetailedInfo) {
            MTMessageDetailedInfo detailedInfo = (MTMessageDetailedInfo) object;
            if (receivedMessages.contains(detailedInfo.getAnswerMsgId())) {
                scheduller.confirmMessage(detailedInfo.getAnswerMsgId());
            } else {
                long time = scheduller.getMessageIdGenerationTime(detailedInfo.getMsgId());
                if (time > 0) {
                    scheduller.postMessage(new MTNeedResendMessage(new long[]{detailedInfo.getAnswerMsgId()}), false, RESEND_TIMEOUT);
                } else {
                    scheduller.confirmMessage(detailedInfo.getAnswerMsgId());
                }
            }
        } else if (object instanceof MTNewMessageDetailedInfo) {
            MTNewMessageDetailedInfo detailedInfo = (MTNewMessageDetailedInfo) object;
            if (receivedMessages.contains(detailedInfo.getAnswerMsgId())) {
                scheduller.confirmMessage(detailedInfo.getAnswerMsgId());
            } else {
                scheduller.postMessage(new MTNeedResendMessage(new long[]{detailedInfo.getAnswerMsgId()}), false, RESEND_TIMEOUT);
            }
        }

        Logger.d(TAG, "Arrived: " + object.toString());
    }

    public void requestSchedule() {
        synchronized (scheduller) {
            scheduller.notifyAll();
        }
    }

    private EncryptedMessage encrypt(int seqNo, long messageId, byte[] content) throws IOException {
        long salt = state.findActualSalt((int) (TimeOverlord.getInstance().getServerTime() / 1000));
        ByteArrayOutputStream messageBody = new ByteArrayOutputStream();
        writeLong(salt, messageBody);
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
        EncryptedMessage res = new EncryptedMessage();
        res.data = out.toByteArray();
        res.fastConfirm = fastConfirm;
        return res;
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
                Logger.d(TAG, "Scheduller Iteration");
                synchronized (scheduller) {
                    if (contexts.size() == 0) {
                        try {
                            long delay = scheduller.getSchedullerDelay();
                            Logger.d(TAG, "Scheduller delay: " + delay);
                            if (delay > 0) {
                                scheduller.wait(delay);
                            }
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                }

                TcpContext context = null;
                synchronized (contexts) {
                    TcpContext[] currentContexts = contexts.toArray(new TcpContext[0]);
                    if (currentContexts.length != 0) {
                        roundRobin = (roundRobin + 1) % currentContexts.length;
                        context = currentContexts[roundRobin];
                    } else {
                        continue;
                    }
                }

                Logger.d(TAG, "Getting packages");
                synchronized (scheduller) {
                    PreparedPackage preparedPackage = scheduller.doSchedule(context.getContextId());
                    if (preparedPackage == null) {
                        try {
                            long delay = scheduller.getSchedullerDelay();
                            Logger.d(TAG, "Scheduller delay: " + delay);
                            if (delay > 0) {
                                scheduller.wait(delay);
                            }
                        } catch (InterruptedException e) {
                            return;
                        }
                        continue;
                    }

                    try {
                        EncryptedMessage msg = encrypt(preparedPackage.getSeqNo(), preparedPackage.getMessageId(), preparedPackage.getContent());
                        if (preparedPackage.isHighPriority()) {
                            scheduller.registerFastConfirm(preparedPackage.getMessageId(), msg.fastConfirm);
                        }
                        if (!context.isClosed()) {
                            context.postMessage(msg.data, preparedPackage.isHighPriority());
                        } else {
                            scheduller.onConnectionDies(context.getContextId());
                        }
                        Logger.d(TAG, "Sent: " + Integer.toHexString(msg.fastConfirm));
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
                Logger.d(TAG, "Response Iteration");
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
                Logger.d(TAG, "Connection Fixer Iteration");
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
                    ConnectionInfo info = state.fetchConnectionInfo();
                    TcpContext context = new TcpContext(info.getAddress(), info.getPort(), false, tcpListener);
                    if (isClosed) {
                        return;
                    }
                    scheduller.postMessageDelayed(new MTPing(Entropy.generateRandomId()), false, PING_TIMEOUT, 0, context.getContextId(), false);
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
                        Logger.e(TAG, e);
                    }
                } else {
                    inQueue.add(decrypted);
                    synchronized (inQueue) {
                        inQueue.notifyAll();
                    }
                }
            } catch (IOException e) {
                Logger.e(TAG, e);
            }
        }

        @Override
        public void onError(int errorCode, TcpContext context) {
            if (isClosed) {
                return;
            }

            // Fully maintained at transport level: TcpContext dies
        }

        @Override
        public void onChannelBroken(TcpContext context) {
            if (isClosed) {
                return;
            }
            scheduller.onConnectionDies(context.getContextId());
            requestSchedule();
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
            scheduller.onMessageFastConfirmed(hash);
            int[] ids = scheduller.mapFastConfirm(hash);
            for (int id : ids) {
                callback.onConfirmed(id);
            }
        }
    }

    private class EncryptedMessage {
        public byte[] data;
        public int fastConfirm;
    }
}
