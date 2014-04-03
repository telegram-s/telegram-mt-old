package org.telegram.mtproto;

import org.telegram.actors.*;
import org.telegram.mtproto.backoff.ExponentalBackoff;
import org.telegram.mtproto.log.Logger;
import org.telegram.mtproto.schedule.PrepareSchedule;
import org.telegram.mtproto.schedule.PreparedPackage;
import org.telegram.mtproto.schedule.Scheduller;
import org.telegram.mtproto.secure.Entropy;
import org.telegram.mtproto.state.AbsMTProtoState;
import org.telegram.mtproto.state.KnownSalt;
import org.telegram.mtproto.time.TimeOverlord;
import org.telegram.mtproto.tl.*;
import org.telegram.mtproto.transport.ConnectionType;
import org.telegram.mtproto.transport.TcpContext;
import org.telegram.mtproto.transport.TcpContextCallback;
import org.telegram.mtproto.transport.TransportRate;
import org.telegram.mtproto.util.BytesCache;
import org.telegram.tl.DeserializeException;
import org.telegram.tl.StreamingUtils;
import org.telegram.tl.TLMethod;
import org.telegram.tl.TLObject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
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

    private static final int MODE_GENERAL = 0;
    private static final int MODE_KEEP_ALIVE_LOW = 1;
    private static final int MODE_LOW = 2;

    private static final AtomicInteger instanceIndex = new AtomicInteger(1000);

    private static final int MESSAGES_CACHE = 100;
    private static final int MESSAGES_CACHE_MIN = 10;

    private static final int MAX_INTERNAL_FLOOD_WAIT = 10;//10 sec

    private static final int PING_INTERVAL_REQUEST = 60000;// 1 min
    private static final int PING_INTERVAL = 75;//75 secs

    private static final int PING_INTERVAL_REQUEST_LOW_MODE = 5 * 60 * 1000; // 5 Min
    private static final int PING_INTERVAL_LOW_MODE = 6 * 60 * 1000; // 6 min

    private static final int CONNECTION_KEEP_ALIVE_LOW = 30 * 1000; // 30 secs

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

    private static final boolean USE_CHECKSUM = false;

    private final String TAG;
    private final int INSTANCE_INDEX;

    private MTProtoContext protoContext;

    private int desiredConnectionCount;
    private final HashSet<TcpContext> contexts = new HashSet<TcpContext>();
    private final HashMap<Integer, Integer> contextConnectionId = new HashMap<Integer, Integer>();
    private final HashSet<Integer> connectedContexts = new HashSet<Integer>();
    private final HashSet<Integer> initedContext = new HashSet<Integer>();
    private TcpContextCallback tcpListener;

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

    private ExponentalBackoff exponentalBackoff;

    private ActorSystem actorSystem;

    private int mode = MODE_GENERAL;

    private ConnectorMessenger connectionActor;

    public MTProto(AbsMTProtoState state, MTProtoCallback callback, CallWrapper callWrapper, int connectionsCount) {
        this.INSTANCE_INDEX = instanceIndex.incrementAndGet();
        this.TAG = "MTProto#" + INSTANCE_INDEX;
        this.exponentalBackoff = new ExponentalBackoff(TAG + "#BackOff");
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
        this.actorSystem = new ActorSystem();
        this.actorSystem.addThread("connector");
        this.connectionActor = new ConnectorMessenger(new ConnectionActor(actorSystem).self(), null);
        this.connectionActor.check();
    }

    public AbsMTProtoState getState() {
        return state;
    }

    public void resetNetworkBackoff() {
        this.exponentalBackoff.reset();
    }

    public void reloadConnectionInformation() {
        this.connectionRate = new TransportRate(state.getAvailableConnections());
    }

    public int getInstanceIndex() {
        return INSTANCE_INDEX;
    }

    /* package */ Scheduller getScheduller() {
        return scheduller;
    }

    /* package */ int getDesiredConnectionCount() {
        return desiredConnectionCount;
    }

    @Override
    public String toString() {
        return "mtproto#" + INSTANCE_INDEX;
    }

    public void close() {
        if (!isClosed) {
            this.isClosed = true;
//            if (this.connectionFixerThread != null) {
//                this.connectionFixerThread.interrupt();
//            }
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
            connectionActor.check();
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
        Logger.d(TAG, "sendMessage #" + id + " " + request.toString());
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
            if (Logger.LOG_IGNORED) {
                Logger.d(TAG, "Ignoring messages #" + mtMessage.getMessageId());
            }
            return;
        }
        try {
            TLObject intMessage = protoContext.deserializeMessage(new ByteArrayInputStream(mtMessage.getContent()));
            onMTProtoMessage(mtMessage.getMessageId(), intMessage);
        } catch (DeserializeException e) {
            onApiMessage(mtMessage.getContent());
        } catch (IOException e) {
            Logger.e(TAG, e);
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
                    if (Logger.LOG_IGNORED) {
                        Logger.d(TAG, "Ignored BadMsg #" + badMessage.getErrorCode() + " (" + badMessage.getBadMsgId() + ", " + badMessage.getBadMsqSeqno() + ")");
                    }
                    scheduller.forgetMessageByMsgId(badMessage.getBadMsgId());
                }
            } else {
                if (Logger.LOG_IGNORED) {
                    Logger.d(TAG, "Unknown package #" + badMessage.getBadMsgId());
                }
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
                        BytesCache.getInstance().put(result.getContent());

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
                        Logger.e(TAG, e);
                        return;
                    }
                } else {
                    Logger.d(TAG, "rpc_result: " + result.getMessageId() + " #" + Integer.toHexString(responseConstructor));
                    callback.onRpcResult(id, result.getContent(), this);
                    BytesCache.getInstance().put(result.getContent());
                    scheduller.forgetMessage(id);
                }
            } else {
                if (Logger.LOG_IGNORED) {
                    Logger.d(TAG, "ignored rpc_result: " + result.getMessageId());
                }
                BytesCache.getInstance().put(result.getContent());
            }
            scheduller.onMessageConfirmed(result.getMessageId());
            long time = scheduller.getMessageIdGenerationTime(result.getMessageId());
            if (time != 0) {
                long delta = System.nanoTime() / 1000000 - time;
                TimeOverlord.getInstance().onMethodExecuted(result.getMessageId(), msgId, delta);
            }
        } else if (object instanceof MTPong) {
            MTPong pong = (MTPong) object;
            if (Logger.LOG_PING) {
                Logger.d(TAG, "pong: " + pong.getPingId());
            }
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
        } else if (object instanceof MTNewSessionCreated) {
            callback.onSessionCreated(this);
        } else {
            if (Logger.LOG_IGNORED) {
                Logger.d(TAG, "Ignored MTProto message " + object.toString());
            }
        }
    }

    private void internalSchedule() {
        long time = System.nanoTime() / 1000000;
        if (mode == MODE_GENERAL) {
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

    private byte[] optimizedSHA(byte[] serverSalt, byte[] session, long msgId, int seq, int len, byte[] data, int datalen) {
        try {
            MessageDigest crypt = MessageDigest.getInstance("SHA-1");
            crypt.reset();
            crypt.update(serverSalt);
            crypt.update(session);
            crypt.update(longToBytes(msgId));
            crypt.update(intToBytes(seq));
            crypt.update(intToBytes(len));
            crypt.update(data, 0, datalen);
            return crypt.digest();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }

    private MTMessage decrypt(byte[] data, int offset, int len) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(data);
        stream.skip(offset);
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

        int totalLen = len - 8 - 16;
        byte[] encMessage = BytesCache.getInstance().allocate(totalLen);
        readBytes(encMessage, 0, totalLen, stream);

        byte[] rawMessage = BytesCache.getInstance().allocate(totalLen);
        long decryptStart = System.currentTimeMillis();
        AES256IGEDecryptBig(encMessage, rawMessage, totalLen, aesIv, aesKey);
        Logger.d(TAG, "Decrypted in " + (System.currentTimeMillis() - decryptStart) + " ms");
        BytesCache.getInstance().put(encMessage);

        ByteArrayInputStream bodyStream = new ByteArrayInputStream(rawMessage);
        byte[] serverSalt = readBytes(8, bodyStream);
        byte[] session = readBytes(8, bodyStream);
        long messageId = readLong(bodyStream);
        int mes_seq = StreamingUtils.readInt(bodyStream);

        int msg_len = StreamingUtils.readInt(bodyStream);

        int bodySize = totalLen - 32;

        if (msg_len % 4 != 0) {
            throw new SecurityException();
        }

        if (msg_len > bodySize) {
            throw new SecurityException();
        }

        if (msg_len - bodySize > 15) {
            throw new SecurityException();
        }

        byte[] message = BytesCache.getInstance().allocate(msg_len);
        readBytes(message, 0, msg_len, bodyStream);

        BytesCache.getInstance().put(rawMessage);

        byte[] checkHash = optimizedSHA(serverSalt, session, messageId, mes_seq, msg_len, message, msg_len);

        if (!arrayEq(substring(checkHash, 4, 16), msgKey)) {
            throw new SecurityException();
        }

        if (!arrayEq(session, this.session)) {
            return null;
        }

        if (TimeOverlord.getInstance().getTimeAccuracy() < 10) {
            long time = (messageId >> 32);
            long serverTime = TimeOverlord.getInstance().getServerTime();

            if (serverTime + 30 < time) {
                Logger.w(TAG, "Incorrect message time: " + time + " with server time: " + serverTime);
                // return null;
            }

            if (time < serverTime - 300) {
                Logger.w(TAG, "Incorrect message time: " + time + " with server time: " + serverTime);
                // return null;
            }
        }

        return new MTMessage(messageId, mes_seq, message, message.length);
    }

    private class SchedullerThread extends Thread {
        private SchedullerThread() {
            setName("Scheduller#" + hashCode());
        }

        @Override
        public void run() {
            setPriority(Thread.MIN_PRIORITY);
            PrepareSchedule prepareSchedule = new PrepareSchedule();
            while (!isClosed) {
                if (Logger.LOG_THREADS) {
                    Logger.d(TAG, "Scheduller Iteration");
                }

                int[] contextIds;
                synchronized (contexts) {
                    TcpContext[] currentContexts = contexts.toArray(new TcpContext[0]);
                    contextIds = new int[currentContexts.length];
                    for (int i = 0; i < contextIds.length; i++) {
                        contextIds[i] = currentContexts[i].getContextId();
                    }
                }

                synchronized (scheduller) {
                    scheduller.prepareScheduller(prepareSchedule, contextIds);
                    if (prepareSchedule.isDoWait()) {
                        if (Logger.LOG_THREADS) {
                            Logger.d(TAG, "Scheduller:wait " + prepareSchedule.getDelay());
                        }
                        try {
                            scheduller.wait(prepareSchedule.getDelay());
                        } catch (InterruptedException e) {
                            Logger.e(TAG, e);
                            return;
                        }
                        continue;
                    }
                }

                TcpContext context = null;
                synchronized (contexts) {
                    TcpContext[] currentContexts = contexts.toArray(new TcpContext[0]);
                    outer:
                    for (int i = 0; i < currentContexts.length; i++) {
                        int index = (i + roundRobin + 1) % currentContexts.length;
                        for (int allowed : prepareSchedule.getAllowedContexts()) {
                            if (currentContexts[index].getContextId() == allowed) {
                                context = currentContexts[index];
                                break outer;
                            }
                        }

                    }

                    if (currentContexts.length != 0) {
                        roundRobin = (roundRobin + 1) % currentContexts.length;
                    }
                }

                if (context == null) {
                    if (Logger.LOG_THREADS) {
                        Logger.d(TAG, "Scheduller: no context");
                    }
                    continue;
                }

                if (Logger.LOG_THREADS) {
                    Logger.d(TAG, "doSchedule");
                }

                internalSchedule();
                synchronized (scheduller) {
                    long start = System.currentTimeMillis();
                    PreparedPackage preparedPackage = scheduller.doSchedule(context.getContextId(), initedContext.contains(context.getContextId()));
                    if (Logger.LOG_THREADS) {
                        Logger.d(TAG, "Schedulled in " + (System.currentTimeMillis() - start) + " ms");
                    }
                    if (preparedPackage == null) {
                        continue;
                    }

                    if (Logger.LOG_THREADS) {
                        Logger.d(TAG, "MessagePushed (#" + context.getContextId() + "): time:" + getUnixTime(preparedPackage.getMessageId()));
                        Logger.d(TAG, "MessagePushed (#" + context.getContextId() + "): seqNo:" + preparedPackage.getSeqNo() + ", msgId" + preparedPackage.getMessageId());
                    }

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
                        Logger.e(TAG, e);
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
            setPriority(Thread.MIN_PRIORITY);
            while (!isClosed) {
                if (Logger.LOG_THREADS) {
                    Logger.d(TAG, "Response Iteration");
                }
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
                BytesCache.getInstance().put(message.getContent());
            }
        }
    }

    private class ResponseActor extends ReflectedActor {

        public ResponseActor(ActorSystem system) {
            super(system, "response", "response");
        }

        protected void onNewMessage(MTMessage message) {
            onMTMessage(message);
            BytesCache.getInstance().put(message.getContent());
        }

        protected void onRawMessage(byte[] data, int offset, int len, int contextId) {

        }
    }

    private class ResponseMessenger extends ActorMessenger {

        protected ResponseMessenger(ActorReference reference) {
            super(reference, null);
        }

        protected ResponseMessenger(ActorReference reference, ActorReference sender) {
            super(reference, sender);
        }

        public void onMessage(MTMessage message) {
            talkRaw("new", message);
        }

        public void onRawMessage(byte[] data, int offset, int len, int contextId) {
            talkRaw("raw", data, offset, len, contextId);
        }

        @Override
        public ActorMessenger cloneForSender(ActorReference sender) {
            return new ResponseMessenger(reference, sender);
        }
    }

    private class ConnectionActor extends ReflectedActor {

        private ConnectionActor(ActorSystem system) {
            super(system, "connector", "connector");
        }

        @Override
        protected void registerMethods() {
            registerMethod("check")
                    .enabledBackOff()
                    .enableSingleShot();
        }

        protected void onCheckMessage() throws Exception {
            synchronized (contexts) {
                if (contexts.size() >= desiredConnectionCount) {
                    return;
                }
            }

            ConnectionType type = connectionRate.tryConnection();
            try {
                TcpContext context = new TcpContext(MTProto.this, type.getHost(), type.getPort(), USE_CHECKSUM, tcpListener);
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
                Logger.e(TAG, e);
                connectionRate.onConnectionFailure(type.getId());
                throw e;
            }

            self().talk("check", null);
        }
    }

    private class ConnectorMessenger extends ActorMessenger {

        private ConnectorMessenger(ActorReference reference) {
            super(reference, null);
        }

        private ConnectorMessenger(ActorReference reference, ActorReference sender) {
            super(reference, sender);
        }

        public void check() {
            talkRaw("check");
        }

        @Override
        public ActorMessenger cloneForSender(ActorReference sender) {
            return new ConnectorMessenger(reference, sender);
        }
    }

    private class TcpListener implements TcpContextCallback {

        @Override
        public void onRawMessage(byte[] data, int offset, int len, TcpContext context) {
            if (isClosed) {
                return;
            }
            try {
                MTMessage decrypted = decrypt(data, offset, len);
                if (decrypted == null) {
                    Logger.d(TAG, "message ignored");
                    return;
                }
                if (!connectedContexts.contains(context.getContextId())) {
                    connectedContexts.add(context.getContextId());
                    exponentalBackoff.onSuccess();
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
                        BytesCache.getInstance().put(decrypted.getContent());
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
                        exponentalBackoff.onFailureNoWait();
                        connectionRate.onConnectionFailure(contextConnectionId.get(context.getContextId()));
                    }
                    contexts.remove(context);
                    connectionActor.check();
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
                if (!connectedContexts.contains(contextId)) {
                    if (contextConnectionId.containsKey(contextId)) {
                        exponentalBackoff.onFailureNoWait();
                        connectionRate.onConnectionFailure(contextConnectionId.get(contextId));
                    }
                }
                connectionActor.check();
            }
            scheduller.onConnectionDies(context.getContextId());
            requestSchedule();
        }

        @Override
        public void onFastConfirm(int hash) {
            if (isClosed) {
                return;
            }
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