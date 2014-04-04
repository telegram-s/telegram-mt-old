package org.telegram.mtproto;

import org.telegram.actors.*;
import org.telegram.mtproto.log.Logger;
import org.telegram.mtproto.schedule.Scheduller;
import org.telegram.mtproto.secure.Entropy;
import org.telegram.mtproto.state.AbsMTProtoState;
import org.telegram.mtproto.state.KnownSalt;
import org.telegram.mtproto.time.TimeOverlord;
import org.telegram.mtproto.tl.*;
import org.telegram.mtproto.transport.*;
import org.telegram.mtproto.util.BytesCache;
import org.telegram.tl.DeserializeException;
import org.telegram.tl.TLMethod;
import org.telegram.tl.TLObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.telegram.mtproto.secure.CryptoUtils.*;
import static org.telegram.tl.StreamingUtils.*;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 03.11.13
 * Time: 8:14
 */
public class MTProto {

    public static final int MODE_GENERAL = 0;
    public static final int MODE_GENERAL_LOW_MODE = 1;
    public static final int MODE_FILE = 2;
    public static final int MODE_PUSH = 3;

    private static final AtomicInteger instanceIndex = new AtomicInteger(1000);

    private static final int MESSAGES_CACHE = 100;
    private static final int MESSAGES_CACHE_MIN = 10;

    private static final int MAX_INTERNAL_FLOOD_WAIT = 10;//10 sec

    private static final int PING_INTERVAL_REQUEST = 60000;// 1 min
    private static final int PING_INTERVAL = 75;//75 secs

    private static final int PING_PUSH_REQUEST = 9 * 60 * 1000; // 5 Min

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
    private static final long FUTURE_TIMEOUT = 5 * 60 * 60 * 1000;//5 min
    private static final long FUTURE_NO_TIME_TIMEOUT = 15 * 60 * 1000;//15 sec

    private final String TAG;
    private final int INSTANCE_INDEX;

    private MTProtoContext protoContext;
    private ActorSystem actorSystem;

    private byte[] authKey;
    private byte[] authKeyId;
    private byte[] session;
    private AbsMTProtoState state;

    private int desiredConnectionCount;
    private TransportPool transportPool;

    private int mode = MODE_GENERAL;
    private final Scheduller scheduller;
    private final ArrayList<Long> receivedMessages = new ArrayList<Long>();
    private ResponseActor.ResponseMessenger responseActor;
    private MTProtoCallback callback;
    private InternalActionsActor.Messenger actionsActor;

    private boolean isClosed;

    public MTProto(AbsMTProtoState state,
                   MTProtoCallback callback,
                   CallWrapper callWrapper,
                   int connectionsCount,
                   int mode) {
        this.INSTANCE_INDEX = instanceIndex.incrementAndGet();
        this.TAG = "MTProto#" + INSTANCE_INDEX;
        this.mode = mode;
        this.actorSystem = new ActorSystem();
        this.actorSystem.addThread("response");
        this.actorSystem.addThread("connector");
        this.actorSystem.addThread("scheduller");
        this.state = state;
        this.callback = callback;
        this.authKey = state.getAuthKey();
        this.authKeyId = substring(SHA1(authKey), 12, 8);
        this.protoContext = MTProtoContext.getInstance();
        this.desiredConnectionCount = connectionsCount;
        this.session = Entropy.generateSeed(8);
        this.scheduller = new Scheduller(this, callWrapper);
        this.scheduller.postMessage(new MTPing(Entropy.generateRandomId()), false, Long.MAX_VALUE);
        this.responseActor = new ResponseActor(actorSystem).messenger();
        this.actionsActor = new InternalActionsActor(actorSystem).messenger();
        this.transportPool = new TransportTcpPool(this, new TransportPoolCallback() {
            @Override
            public void onMTMessage(MTMessage message) {
                responseActor.onMessage(message);
            }

            @Override
            public void onFastConfirm(int hash) {
                // We might not send this to response actor for providing faster confirmation
                int[] ids = scheduller.mapFastConfirm(hash);
                for (int id : ids) {
                    MTProto.this.callback.onConfirmed(id);
                }
            }
        }, desiredConnectionCount);
        switch (mode) {
            case MODE_GENERAL:
            case MODE_PUSH:
                transportPool.switchMode(TransportPool.MODE_DEFAULT);
                break;
            case MODE_GENERAL_LOW_MODE:
            case MODE_FILE:
                transportPool.switchMode(TransportPool.MODE_LOWMODE);
                break;

        }
        this.actionsActor.ping();
        this.actionsActor.requestSalts();
    }

    public AbsMTProtoState getState() {
        return state;
    }

    public void resetNetworkBackoff() {
        transportPool.resetConnectionBackoff();
    }

    public void reloadConnectionInformation() {
        transportPool.reloadConnectionInformation();
    }

    public int getInstanceIndex() {
        return INSTANCE_INDEX;
    }

    public Scheduller getScheduller() {
        return scheduller;
    }

    public byte[] getSession() {
        return session;
    }

    public byte[] getAuthKeyId() {
        return authKeyId;
    }

    public byte[] getAuthKey() {
        return authKey;
    }

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public int sendRpcMessage(TLMethod request, long timeout, boolean highPriority) {
        int id = scheduller.postMessage(request, true, timeout, highPriority);
        Logger.d(TAG, "sendMessage #" + id + " " + request.toString());
        return id;
    }

    public void forgetMessage(int id) {
        scheduller.forgetMessage(id);
    }

    public void switchMode(int mode) {
        if (this.mode != mode) {
            this.mode = mode;

            switch (mode) {
                case MODE_GENERAL:
                case MODE_PUSH:
                    transportPool.switchMode(TransportPool.MODE_DEFAULT);
                    break;
                case MODE_GENERAL_LOW_MODE:
                case MODE_FILE:
                    transportPool.switchMode(TransportPool.MODE_LOWMODE);
                    break;

            }

            actionsActor.ping();
        }
    }

    public void close() {
        if (!isClosed) {
            this.isClosed = true;
            this.actorSystem.close();
            this.transportPool.close();
        }
    }

    // Finding message type
    private void onMTMessage(MTMessage mtMessage) {
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
            callback.onApiMessage(mtMessage.getContent(), this);
        } catch (IOException e) {
            Logger.e(TAG, e);
            // ???
        }
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
                } else if (badMessage.getErrorCode() == ERROR_SEQ_NO_TOO_BIG || badMessage.getErrorCode() == ERROR_SEQ_NO_TOO_SMALL) {
                    if (scheduller.isMessageFromCurrentGeneration(badMessage.getBadMsgId())) {
                        Logger.d(TAG, "Resetting session");
                        session = Entropy.generateSeed(8);
                        transportPool.onSessionChanged(session);
                        scheduller.resetSession();
                    }
                    scheduller.resendAsNewMessage(badMessage.getBadMsgId());
                } else if (badMessage.getErrorCode() == ERROR_BAD_SERVER_SALT) {
                    long salt = ((MTBadServerSalt) badMessage).getNewSalt();
                    // Sync time
                    long delta = System.nanoTime() / 1000000 - time;
                    TimeOverlord.getInstance().onMethodExecuted(badMessage.getBadMsgId(), msgId, delta);
                    state.badServerSalt(salt);
                    Logger.d(TAG, "Reschedule messages because bad_server_salt #" + badMessage.getBadMsgId());
                    scheduller.resendAsNewMessage(badMessage.getBadMsgId());
                    actionsActor.requestSalts();
                } else if (badMessage.getErrorCode() == ERROR_BAD_CONTAINER ||
                        badMessage.getErrorCode() == ERROR_CONTAINER_MSG_ID_INCORRECT) {
                    scheduller.resendMessage(badMessage.getBadMsgId());
                } else if (badMessage.getErrorCode() == ERROR_TOO_OLD) {
                    scheduller.resendAsNewMessage(badMessage.getBadMsgId());
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

    @Override
    public String toString() {
        return "mtproto#" + INSTANCE_INDEX;
    }

    private class InternalActionsActor extends ReflectedActor {

        public InternalActionsActor(ActorSystem system) {
            super(system, "internal_actions", "scheduller");
        }

        public Messenger messenger() {
            return new Messenger(self());
        }

        @Override
        protected void registerMethods() {
            registerMethod("requestSalts")
                    .enableSingleShot();
            registerMethod("pingDelay")
                    .enableSingleShot();
        }

        public void onRequestSaltsMessage() {
            Logger.d(TAG, "Salt check timeout");
            if (TimeOverlord.getInstance().getTimeAccuracy() > 1000) {
                Logger.d(TAG, "Time is not accurate: " + TimeOverlord.getInstance().getTimeAccuracy());
                messenger().requestSaltsDelayed(FUTURE_NO_TIME_TIMEOUT);
                return;
            }
            int count = state.maximumCachedSalts((int) (TimeOverlord.getInstance().getServerTime() / 1000));
            if (count < FUTURE_MINIMAL) {
                Logger.d(TAG, "Too few actual salts: " + count + ", requesting news");
                scheduller.postMessage(new MTGetFutureSalts(FUTURE_REQUEST_COUNT), false, FUTURE_TIMEOUT);
            }
            messenger().requestSaltsDelayed(FUTURE_TIMEOUT);
        }

        public void onPingDelayMessage() {
            if (mode == MODE_GENERAL) {
                Logger.d(TAG, "Ping delay disconnect for " + PING_INTERVAL + " sec");
                scheduller.postMessage(new MTPingDelayDisconnect(Entropy.generateRandomId(), PING_INTERVAL),
                        false, PING_INTERVAL_REQUEST);
                messenger().pingDelayed(PING_INTERVAL_REQUEST);
            } else if (mode == MODE_PUSH) {
                scheduller.postMessage(new MTPing(Entropy.generateRandomId()), false, PING_PUSH_REQUEST);
                messenger().pingDelayed(PING_PUSH_REQUEST);
            }
        }

        private class Messenger extends ActorMessenger {

            protected Messenger(ActorReference reference) {
                super(reference, null);
            }

            public void ping() {
                talkRaw("pingDelay");
            }

            public void pingDelayed(long delayed) {
                talkRawDelayed("pingDelay", delayed);
            }

            public void requestSalts() {
                talkRaw("requestSalts");
            }

            public void requestSaltsDelayed(long delay) {
                talkRaw("requestSalts", delay);
            }

            @Override
            public ActorMessenger cloneForSender(ActorReference sender) {
                return null;
            }
        }
    }

    private class ResponseActor extends ReflectedActor {

        public ResponseActor(ActorSystem system) {
            super(system, "response", "response");
        }

        public void onNewMessage(MTMessage message) {
            onMTMessage(message);
            BytesCache.getInstance().put(message.getContent());
        }

        public ResponseMessenger messenger() {
            return new ResponseMessenger(self());
        }

        private class ResponseMessenger extends ActorMessenger {

            protected ResponseMessenger(ActorReference reference) {
                super(reference, null);
            }

            public void onMessage(MTMessage message) {
                talkRaw("new", message);
            }

            @Override
            public ActorMessenger cloneForSender(ActorReference sender) {
                return new ResponseMessenger(reference);
            }
        }
    }
}