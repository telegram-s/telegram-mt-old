package org.telegram.mtproto.transport;

import org.telegram.actors.ActorMessenger;
import org.telegram.actors.ActorReference;
import org.telegram.actors.ActorSystem;
import org.telegram.actors.ReflectedActor;
import org.telegram.mtproto.MTProto;
import org.telegram.mtproto.backoff.ExponentalBackoff;
import org.telegram.mtproto.log.Logger;
import org.telegram.mtproto.schedule.PrepareSchedule;
import org.telegram.mtproto.schedule.PreparedPackage;
import org.telegram.mtproto.schedule.Scheduller;
import org.telegram.mtproto.secure.Entropy;
import org.telegram.mtproto.tl.MTMessage;
import org.telegram.mtproto.tl.MTPing;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import static org.telegram.mtproto.util.TimeUtil.getUnixTime;

/**
 * Created by ex3ndr on 03.04.14.
 */
public class TransportTcpPool extends TransportPool {

    private static final String TAG = "TransportTcpPool";

    private int desiredConnectionCount;

    private static final boolean USE_CHECKSUM = false;

    private final HashSet<TcpContext> contexts = new HashSet<TcpContext>();
    private final HashMap<Integer, Integer> contextConnectionId = new HashMap<Integer, Integer>();
    private final HashSet<Integer> connectedContexts = new HashSet<Integer>();
    private final HashSet<Integer> initedContext = new HashSet<Integer>();

    private static final int PING_TIMEOUT = 60 * 1000;

    private ActorSystem actorSystem;

    private TransportRate connectionRate;

    private TcpListener tcpListener;

    private ConnectionActor.ConnectorMessenger connectionActor;

    private int roundRobin = 0;

    private ExponentalBackoff exponentalBackoff;

    private SchedullerThread schedullerThread;

    public TransportTcpPool(MTProto proto, TransportPoolCallback callback, int connectionCount) {
        super(proto, callback);
        this.exponentalBackoff = new ExponentalBackoff(TAG);
        this.desiredConnectionCount = connectionCount;
        this.actorSystem = proto.getActorSystem();
        this.tcpListener = new TcpListener();
        this.connectionActor = new ConnectionActor(actorSystem).messenger();
        this.connectionRate = new TransportRate(proto.getState().getAvailableConnections());
        this.schedullerThread = new SchedullerThread();
        this.schedullerThread.start();
        this.connectionActor.check();
    }

    @Override
    public void onSchedullerUpdated(Scheduller scheduller) {
        synchronized (scheduller) {
            scheduller.notifyAll();
        }
    }

    private class ConnectionActor extends ReflectedActor {

        private ConnectorMessenger messenger;

        private ConnectionActor(ActorSystem system) {
            super(system, "connector", "connector");
            this.messenger = new ConnectorMessenger(self());
        }

        public ConnectorMessenger messenger() {
            return messenger;
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
                TcpContext context = new TcpContext(proto, type.getHost(), type.getPort(), USE_CHECKSUM, tcpListener);
                synchronized (contexts) {
                    contexts.add(context);
                    contextConnectionId.put(context.getContextId(), type.getId());
                }
                scheduller.postMessageDelayed(new MTPing(Entropy.generateRandomId()), false, PING_TIMEOUT, 0, context.getContextId(), false);
            } catch (IOException e) {
                connectionRate.onConnectionFailure(type.getId());
                throw e;
            }

            messenger().check();
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
    }

    private class SchedullerActor extends ReflectedActor {

        public SchedullerActor(ActorSystem system) {
            super(system, "scheduller", "scheduller");
        }

        public void onScheduleMessage() {

        }
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

                onMTMessage(decrypted);
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
        }

        @Override
        public void onFastConfirm(int hash) {
            if (isClosed) {
                return;
            }
            TransportTcpPool.this.onFastConfirm(hash);
        }
    }
}