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
import org.telegram.mtproto.transport.ConnectionType;
import org.telegram.mtproto.transport.TcpContext;
import org.telegram.mtproto.transport.TcpContextCallback;
import org.telegram.mtproto.transport.TransportRate;
import org.telegram.tl.DeserializeException;
import org.telegram.tl.StreamingUtils;
import org.telegram.tl.TLMethod;
import org.telegram.tl.TLObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.telegram.mtproto.secure.CryptoUtils.*;
import static org.telegram.mtproto.util.TimeUtil.getUnixTime;
import static org.telegram.tl.StreamingUtils.*;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 03.11.13
 * Time: 8:14
 */
public class MTProto {

    private static final AtomicInteger instanceIndex = new AtomicInteger(1000);

    private static final int MESSAGES_CACHE = 100;
    private static final int MESSAGES_CACHE_MIN = 10;

    private static final int MAX_INTERNAL_FLOOD_WAIT = 10;//10 sec

    private static final int PING_INTERVAL_REQUEST = 60000;
    private static final int PING_INTERVAL = 75;//75 secs

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

    private final String TAG;
    private final int INSTANCE_INDEX;

    private MTProtoContext protoContext;

    private int desiredConnectionCount;
    private final HashSet<TcpContext> contexts = new HashSet<TcpContext>();
    private final HashMap<Integer, Integer> contextConnectionId = new HashMap<Integer, Integer>();
    private final HashSet<Integer> connectedContexts = new HashSet<Integer>();
    private final HashSet<Integer> initedContext = new HashSet<Integer>();
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

    private TransportRate connectionRate;

    private long lastPingTime = System.nanoTime() / 1000000L - PING_INTERVAL_REQUEST * 10;

    public MTProto(AbsMTProtoState state, MTProtoCallback callback, CallWrapper callWrapper, int connectionsCount) {
        this.INSTANCE_INDEX = instanceIndex.incrementAndGet();
        this.TAG = "MTProto#" + INSTANCE_INDEX;
        this.state = state;
        this.connectionRate = new TransportRate(state.getAvailableConnections());
        this.callback = callback;
        this.authKey = state.getAuthKey();
        this.authKeyId = substring(SHA1(authKey), 12, 8);
        this.protoContext = new MTProtoContext();
        this.desiredConnectionCount = connectionsCount;
        this.session = Entropy.generateSeed(8);
        this.tcpListener = new TcpListener();
        this.scheduller = new Scheduller(this, callWrapper);
        this.schedullerThread = new SchedullerThread();
        this.schedullerThread.start();
        this.responseProcessor = new ResponseProcessor();
        this.responseProcessor.start();
        this.connectionFixerThread = new ConnectionFixerThread();
        this.connectionFixerThread.start();
    }

    public void reloadConnectionInformation() {
        this.connectionRate = new TransportRate(state.getAvailableConnections());
    }

    public int getInstanceIndex() {
        return INSTANCE_INDEX;
    }

    @Override
    public String toString() {
        return "mtproto#" + INSTANCE_INDEX;
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

    public void forgetMessage(int id) {
        scheduller.forgetMessage(id);
    }

    public int sendRpcMessage(TLMethod request, long timeout, boolean highPriority) {
        return sendMessage(request, timeout, true, highPriority);
    }

    public int sendMessage(TLObject request, long timeout, boolean isRpc, boolean highPriority) {
        int id = scheduller.postMessage(request, isRpc, timeout, highPriority);
        synchronized (scheduller) {
            scheduller.notifyAll();
        }

        return id;
    }

    private void onMTMessage(MTMessage mtMessage) {
        if (futureSaltsRequestedTime - System.nanoTime() > FUTURE_TIMEOUT * 1000L) {
            Logger.d(TAG, "Salt check timeout");
            int count = state.maximumCachedSalts(getUnixTime(mtMessage.getMessageId()));
            if (count < FUTURE_MINIMAL) {
                Logger.d(TAG, "Too fiew actual salts: " + count + ", requesting news");
                futureSaltsRequestId = scheduller.postMessage(new MTGetFutureSalts(FUTURE_REQUEST_COUNT), false, FUTURE_TIMEOUT);
                futureSaltsRequestedTime = System.nanoTime();
            }
        }

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
        Logger.d(TAG, "MTProtoMessage: " + object.toString());

        if (object instanceof MTBadMessage) {
            MTBadMessage badMessage = (MTBadMessage) object;
            Logger.d(TAG, "BadMessage: " + badMessage.getErrorCode() + " #" + badMessage.getBadMsgId());
            scheduller.onMessageConfirmed(badMessage.getBadMsgId());
            long time = scheduller.getMessageIdGenerationTime(badMessage.getBadMsgId());
            if (time != 0) {
                if (badMessage.getErrorCode() == ERROR_MSG_ID_TOO_BIG
                        || badMessage.getErrorCode() == ERROR_MSG_ID_TOO_SMALL) {
                    long delta = System.nanoTime() / 1000000 - time;
                    TimeOverlord.getInstance().onForcedServerTimeArrived((msgId >> 32) * 1000, delta);
                    if (badMessage.getErrorCode() == ERROR_MSG_ID_TOO_BIG) {
                        scheduller.resetMessageId();
                    }
                    scheduller.resendAsNewMessage(badMessage.getBadMsgId());
                    requestSchedule();
                } else if (badMessage.getErrorCode() == ERROR_SEQ_NO_TOO_BIG || badMessage.getErrorCode() == ERROR_SEQ_NO_TOO_SMALL) {
                    if (scheduller.isMessageFromCurrentGeneration(badMessage.getBadMsgId())) {
                        Logger.d(TAG, "Resetting session");
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
                    Logger.d(TAG, "Reschedule messages because bad_server_salt #" + badMessage.getBadMsgId());
                    scheduller.resendAsNewMessage(badMessage.getBadMsgId());
                    requestSchedule();
                } else if (badMessage.getErrorCode() == ERROR_BAD_CONTAINER ||
                        badMessage.getErrorCode() == ERROR_CONTAINER_MSG_ID_INCORRECT) {
                    scheduller.resendMessage(badMessage.getBadMsgId());
                    requestSchedule();
                } else if (badMessage.getErrorCode() == ERROR_TOO_OLD) {
                    scheduller.resendAsNewMessage(badMessage.getBadMsgId());
                    requestSchedule();
                } else {
                    Logger.d(TAG, "Ignored BadMsg #" + badMessage.getErrorCode() + " (" + badMessage.getBadMsgId() + ", " + badMessage.getBadMsqSeqno() + ")");
                    scheduller.forgetMessageByMsgId(badMessage.getBadMsgId());
                }
            } else {
                Logger.d(TAG, "Unknown package #" + badMessage.getBadMsgId());
            }
        } else if (object instanceof MTMsgsAck) {
            MTMsgsAck ack = (MTMsgsAck) object;
            String log = "";
            for (Long ackMsgId : ack.getMessages()) {
                scheduller.onMessageConfirmed(ackMsgId);
                if (log.length() > 0) {
                    log += ", ";
                }
                log += ackMsgId;
                int id = scheduller.mapSchedullerId(ackMsgId);
                if (id > 0) {
                    callback.onConfirmed(id);
                }
            }
            Logger.d(TAG, "msgs_ack: " + log);
        } else if (object instanceof MTRpcResult) {
            MTRpcResult result = (MTRpcResult) object;

            Logger.d(TAG, "rpc_result: " + result.getMessageId());

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
                                if (delay <= MAX_INTERNAL_FLOOD_WAIT) {
                                    scheduller.resendAsNewMessageDelayed(result.getMessageId(), delay * 1000);
                                    requestSchedule();
                                    return;
                                }
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
                        scheduller.forgetMessage(id);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
                } else {
                    Logger.d(TAG, "rpc_result: " + result.getMessageId() + " #" + Integer.toHexString(responseConstructor));
                    callback.onRpcResult(id, result.getContent(), this);
                    scheduller.forgetMessage(id);
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
            scheduller.forgetMessageByMsgId(pong.getMessageId());
            long time = scheduller.getMessageIdGenerationTime(pong.getMessageId());
            if (time != 0) {
                long delta = System.nanoTime() / 1000000 - time;
                TimeOverlord.getInstance().onMethodExecuted(pong.getMessageId(), msgId, delta);
            }
        } else if (object instanceof MTFutureSalts) {
            MTFutureSalts salts = (MTFutureSalts) object;
            scheduller.onMessageConfirmed(salts.getRequestId());
            scheduller.forgetMessageByMsgId(salts.getRequestId());

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
            Logger.d(TAG, "msg_detailed_info: " + detailedInfo.getMsgId() + ", answer: " + detailedInfo.getAnswerMsgId());
            if (receivedMessages.contains(detailedInfo.getAnswerMsgId())) {
                scheduller.confirmMessage(detailedInfo.getAnswerMsgId());
            } else {
                int id = scheduller.mapSchedullerId(detailedInfo.getMsgId());
                if (id > 0) {
                    scheduller.postMessage(new MTNeedResendMessage(new long[]{detailedInfo.getAnswerMsgId()}), false, RESEND_TIMEOUT);
                } else {
                    scheduller.confirmMessage(detailedInfo.getAnswerMsgId());
                    scheduller.forgetMessageByMsgId(detailedInfo.getMsgId());
                }
            }
        } else if (object instanceof MTNewMessageDetailedInfo) {
            MTNewMessageDetailedInfo detailedInfo = (MTNewMessageDetailedInfo) object;
            Logger.d(TAG, "msg_new_detailed_info: " + detailedInfo.getAnswerMsgId());
            if (receivedMessages.contains(detailedInfo.getAnswerMsgId())) {
                scheduller.confirmMessage(detailedInfo.getAnswerMsgId());
            } else {
                scheduller.postMessage(new MTNeedResendMessage(new long[]{detailedInfo.getAnswerMsgId()}), false, RESEND_TIMEOUT);
            }
        } else {
            Logger.d(TAG, "Ignored MTProto message " + object.toString());
        }
    }

    private void internalSchedule() {
        long time = System.nanoTime() / 1000000;
        if (time - lastPingTime > PING_INTERVAL_REQUEST) {
            lastPingTime = time;
            synchronized (contexts) {
                for (TcpContext context : contexts) {
                    scheduller.postMessageDelayed(
                            new MTPingDelayDisconnect(Entropy.generateRandomId(), PING_INTERVAL),
                            false, PING_INTERVAL_REQUEST, 0, context.getContextId(), false);
                }
            }
        }
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

        int bodySize = rawMessage.length - 32;

        if (msg_len % 4 != 0) {
            throw new SecurityException();
        }

        if (msg_len > bodySize) {
            throw new SecurityException();
        }

        if (msg_len - bodySize > 15) {
            throw new SecurityException();
        }

        byte[] message = readBytes(msg_len, bodyStream);

        byte[] checkHash = SHA1(concat(serverSalt, session, longToBytes(messageId), intToBytes(mes_seq), intToBytes(msg_len), message));

        if (!arrayEq(substring(checkHash, 4, 16), msgKey)) {
            throw new SecurityException();
        }

        if (!arrayEq(session, this.session)) {
            return null;
        }

        if (TimeOverlord.getInstance().getTimeAccuracy() < 10 * 1000) {
            long time = (messageId >> 32) * 1000;
            long serverTime = TimeOverlord.getInstance().getServerTime();

            if (serverTime + 30 * 1000 < time) {
                return null;
            }

            if (time < serverTime - 300 * 1000) {
                return null;
            }
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
                            long delay = scheduller.getSchedullerDelay(false);
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

                Logger.d(TAG, "doSchedule");
                internalSchedule();
                synchronized (scheduller) {
                    PreparedPackage preparedPackage = scheduller.doSchedule(context.getContextId(), initedContext.contains(context.getContextId()));
                    if (preparedPackage == null) {
                        try {
                            long delay = scheduller.getSchedullerDelay(true);
                            if (delay > 0) {
                                scheduller.wait(delay);
                            }
                        } catch (InterruptedException e) {
                            return;
                        }
                        continue;
                    }

                    Logger.d(TAG, "MessagePushed (#" + context.getContextId() + "): time:" + getUnixTime(preparedPackage.getMessageId()));
                    Logger.d(TAG, "MessagePushed (#" + context.getContextId() + "): seqNo:" + preparedPackage.getSeqNo() + ", msgId" + preparedPackage.getMessageId());

                    try {
                        EncryptedMessage msg = encrypt(preparedPackage.getSeqNo(), preparedPackage.getMessageId(), preparedPackage.getContent());
                        if (preparedPackage.isHighPriority()) {
                            scheduller.registerFastConfirm(preparedPackage.getMessageId(), msg.fastConfirm);
                        }
                        if (!context.isClosed()) {
                            context.postMessage(msg.data, preparedPackage.isHighPriority());
                            initedContext.add(context.getContextId());
                        } else {
                            scheduller.onConnectionDies(context.getContextId());
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

                ConnectionType type = connectionRate.tryConnection();
                try {
                    TcpContext context = new TcpContext(MTProto.this, type.getHost(), type.getPort(), false, tcpListener);
                    if (isClosed) {
                        return;
                    }
                    scheduller.postMessageDelayed(new MTPing(Entropy.generateRandomId()), false, PING_TIMEOUT, 0, context.getContextId(), false);
                    synchronized (contexts) {
                        contexts.add(context);
                        contextConnectionId.put(context.getContextId(), type.getId());
                    }
                    synchronized (scheduller) {
                        scheduller.notifyAll();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    connectionRate.onConnectionFailure(type.getId());
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
                if (decrypted == null) {
                    return;
                }
                if (!connectedContexts.contains(context.getContextId())) {
                    connectedContexts.add(context.getContextId());
                    connectionRate.onConnectionSuccess(contextConnectionId.get(context.getContextId()));
                }

                Logger.d(TAG, "MessageArrived (#" + context.getContextId() + "): time: " + getUnixTime(decrypted.getMessageId()));
                Logger.d(TAG, "MessageArrived (#" + context.getContextId() + "): seqNo: " + decrypted.getSeqNo() + ", msgId:" + decrypted.getMessageId());

                if (readInt(decrypted.getContent()) == MTMessagesContainer.CLASS_ID) {
                    try {
                        TLObject object = protoContext.deserializeMessage(new ByteArrayInputStream(decrypted.getContent()));
                        if (object instanceof MTMessagesContainer) {
                            for (MTMessage mtMessage : ((MTMessagesContainer) object).getMessages()) {
                                inQueue.add(mtMessage);
                            }
                            synchronized (inQueue) {
                                inQueue.notifyAll();
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
                synchronized (contexts) {
                    context.close();
                    if (!connectedContexts.contains(context.getContextId())) {
                        connectionRate.onConnectionFailure(contextConnectionId.get(context.getContextId()));
                    }
                    contexts.remove(context);
                    contexts.notifyAll();
                    scheduller.onConnectionDies(context.getContextId());
                }
            }
        }

        @Override
        public void onError(int errorCode, TcpContext context) {
            if (isClosed) {
                return;
            }

            Logger.d(TAG, "OnError (#" + context.getContextId() + "): " + errorCode);

            // Fully maintained at transport level: TcpContext dies
        }

        @Override
        public void onChannelBroken(TcpContext context) {
            if (isClosed) {
                return;
            }
            int contextId = context.getContextId();
            Logger.d(TAG, "onChannelBroken (#" + contextId + ")");
            synchronized (contexts) {
                contexts.remove(context);
                contexts.notifyAll();
                if (connectedContexts.contains(contextId)) {
                    if (contextConnectionId.containsKey(contextId)) {
                        connectionRate.onConnectionFailure(contextConnectionId.get(contextId));
                    }
                }
            }
            scheduller.onConnectionDies(context.getContextId());
            requestSchedule();
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
