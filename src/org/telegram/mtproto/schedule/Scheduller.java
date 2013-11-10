package org.telegram.mtproto.schedule;

import org.omg.PortableServer.ServantRetentionPolicy;
import org.telegram.mtproto.CallWrapper;
import org.telegram.mtproto.log.Logger;
import org.telegram.mtproto.time.TimeOverlord;
import org.telegram.mtproto.tl.MTMessage;
import org.telegram.mtproto.tl.MTMessagesContainer;
import org.telegram.mtproto.tl.MTMsgsAck;
import org.telegram.tl.TLMethod;
import org.telegram.tl.TLObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 03.11.13
 * Time: 8:51
 */
public class Scheduller {
    // Share identity values across all connections to avoid collisions
    private static AtomicInteger messagesIds = new AtomicInteger(1);
    private static HashMap<Long, Long> idGenerationTime = new HashMap<Long, Long>();

    private static final int SCHEDULLER_TIMEOUT = 15 * 1000;//15 sec

    private static final long CONFIRM_TIMEOUT = 60 * 1000 * 1000 * 1000L;//60 sec

    private static final int MAX_WORKLOAD_SIZE = 1024;
    private static final long RETRY_TIMEOUT = 15 * 1000;

    private SortedMap<Integer, SchedullerPackage> messages = Collections.synchronizedSortedMap(new TreeMap<Integer, SchedullerPackage>());
    private HashSet<Long> currentMessageGeneration = new HashSet<Long>();
    private HashSet<Long> confirmedMessages = new HashSet<Long>();

    private long firstConfirmTime = 0;

    private long lastMessageId = 0;
    private int seqNo = 0;

    private CallWrapper wrapper;

    public Scheduller(CallWrapper wrapper) {
        this.wrapper = wrapper;
    }

    private synchronized long generateMessageId() {
        long messageId = TimeOverlord.getInstance().createWeakMessageId();
        if (messageId <= lastMessageId) {
            messageId = lastMessageId = lastMessageId + 4;
        }
        while (idGenerationTime.containsKey(messageId)) {
            messageId += 4;
        }
        idGenerationTime.put(messageId, getCurrentTime());
        currentMessageGeneration.add(messageId);
        return messageId;
    }

    public synchronized int generateSeqNoWeak() {
        return seqNo * 2;
    }

    public synchronized int generateSeqNo() {
        int res = seqNo * 2 + 1;
        seqNo++;
        return res;
    }

    private long getCurrentTime() {
        return System.nanoTime() / 1000000;
    }

    public long getMessageIdGenerationTime(long msgId) {
        if (idGenerationTime.containsKey(msgId)) {
            return idGenerationTime.get(msgId);
        }
        return 0;
    }

    public int postMessageDelayed(TLObject object, boolean isRpc, long timeout, int delay, int contextId, boolean highPrioroty) {
        int id = messagesIds.incrementAndGet();
        SchedullerPackage schedullerPackage = new SchedullerPackage(id);
        schedullerPackage.object = object;
        schedullerPackage.addTime = getCurrentTime();
        schedullerPackage.scheduleTime = schedullerPackage.addTime + delay * 1000L * 1000L;
        schedullerPackage.expiresTime = schedullerPackage.scheduleTime + timeout;
        schedullerPackage.isRpc = isRpc;
        schedullerPackage.queuedToChannel = contextId;
        schedullerPackage.priority = highPrioroty ? PRIORITY_HIGH : PRIORITY_NORMAL;
        messages.put(id, schedullerPackage);
        return id;
    }

    public int postMessage(TLObject object, boolean isApi, long timeout) {
        return postMessageDelayed(object, isApi, timeout, 0, -1, false);
    }

    public int postMessage(TLObject object, boolean isApi, long timeout, boolean highPrioroty) {
        return postMessageDelayed(object, isApi, timeout, 0, -1, highPrioroty);
    }

    public long getSchedullerDelay() {
        long minDelay = SCHEDULLER_TIMEOUT;
        long time = getCurrentTime();
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            if (schedullerPackage.state == STATE_QUEUED) {
                if (schedullerPackage.scheduleTime <= time) {
                    minDelay = 0;
                } else {
                    long delta = (time - schedullerPackage.scheduleTime) / (1000L * 1000L);
                    minDelay = Math.min(delta, minDelay);
                }
            }
        }
        return minDelay;
    }

    public void registerFastConfirm(long msgId, int fastConfirm) {
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            boolean contains = false;
            for (Long relatedMsgId : schedullerPackage.relatedMessageIds) {
                if (relatedMsgId == msgId) {
                    contains = true;
                    break;
                }
            }
            if (contains) {
                schedullerPackage.relatedFastConfirm.add(fastConfirm);
            }
        }
    }

    public int mapSchedullerId(long msgId) {
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            if (schedullerPackage.messageId == msgId) {
                return schedullerPackage.id;
            }
        }
        return 0;
    }

    public void resetMessageId() {
        lastMessageId = 0;
    }

    public void resetSession() {
        lastMessageId = 0;
        seqNo = 0;
        currentMessageGeneration.clear();
    }

    public boolean isMessageFromCurrentGeneration(long msgId) {
        return currentMessageGeneration.contains(msgId);
    }

    public void resendAsNewMessage(long msgId) {
        resendAsNewMessageDelayed(msgId, 0);
    }

    public void resendAsNewMessageDelayed(long msgId, int delay) {
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            if (schedullerPackage.relatedMessageIds.contains(msgId)) {
                schedullerPackage.idGenerationTime = 0;
                schedullerPackage.messageId = 0;
                schedullerPackage.seqNo = 0;
                schedullerPackage.relatedMessageIds.clear();
                schedullerPackage.state = STATE_QUEUED;
                schedullerPackage.scheduleTime = getCurrentTime() + delay * 1000L * 1000L;
            }
        }
    }

    public void resendMessage(long msgId) {
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            if (schedullerPackage.relatedMessageIds.contains(msgId)) {
                schedullerPackage.relatedMessageIds.clear();
                schedullerPackage.state = STATE_QUEUED;
            }
        }
    }

    public int[] mapFastConfirm(int fastConfirm) {
        ArrayList<Integer> res = new ArrayList<Integer>();
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            if (schedullerPackage.state == STATE_SENT) {
                if (schedullerPackage.relatedFastConfirm.contains(fastConfirm)) {
                    res.add(schedullerPackage.id);
                }
            }
        }
        int[] res2 = new int[res.size()];
        for (int i = 0; i < res2.length; i++) {
            res2[i] = res.get(i);
        }
        return res2;
    }

    public void onMessageFastConfirmed(int fastConfirm) {
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            if (schedullerPackage.state == STATE_SENT) {
                if (schedullerPackage.relatedFastConfirm.contains(fastConfirm)) {
                    schedullerPackage.state = STATE_CONFIRMED;
                }
            }
        }
    }

    public void onMessageConfirmed(long msgId) {
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            if (schedullerPackage.state == STATE_SENT) {
                if (schedullerPackage.relatedMessageIds.contains(msgId)) {
                    schedullerPackage.state = STATE_CONFIRMED;
                }
            }
        }
    }

    public void confirmMessage(long msgId) {
        confirmedMessages.add(msgId);
        if (firstConfirmTime == 0) {
            firstConfirmTime = System.nanoTime();
        }
    }

    public void unableToSendMessage(long messageId) {
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            if (schedullerPackage.state == STATE_SENT) {
                boolean contains = false;
                for (Long relatedMsgId : schedullerPackage.relatedMessageIds) {
                    if (relatedMsgId == messageId) {
                        contains = true;
                        break;
                    }
                }
                if (contains) {
                    schedullerPackage.state = STATE_QUEUED;
                }
            }
        }
    }

    private ArrayList<SchedullerPackage> actualPackages(int contextId) {
        ArrayList<SchedullerPackage> foundedPackages = new ArrayList<SchedullerPackage>();
        long time = getCurrentTime();
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            if (schedullerPackage.queuedToChannel != -1 && contextId != schedullerPackage.queuedToChannel) {
                continue;
            }
            boolean isPendingPackage = false;
            if (schedullerPackage.state == STATE_QUEUED) {
                if (schedullerPackage.scheduleTime <= time) {
                    isPendingPackage = true;
                }
            } else if (schedullerPackage.state == STATE_SENT) {
                if (getCurrentTime() <= schedullerPackage.expiresTime) {
                    if (getCurrentTime() - schedullerPackage.lastAttemptTime >= RETRY_TIMEOUT) {
                        isPendingPackage = true;
                    }
                }
            }

            if (isPendingPackage) {
                if (schedullerPackage.serialized == null) {
                    try {
                        if (schedullerPackage.isRpc) {
                            schedullerPackage.serialized = wrapper.wrapObject((TLMethod) schedullerPackage.object).serialize();
                        } else {
                            schedullerPackage.serialized = schedullerPackage.object.serialize();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        messages.remove(schedullerPackage);
                        continue;
                    }
                }

                foundedPackages.add(schedullerPackage);
            }
        }
        return foundedPackages;
    }

    public PreparedPackage doSchedule(int contextId) {
        ArrayList<SchedullerPackage> foundedPackages = actualPackages(contextId);

        if (foundedPackages.size() == 0 &&
                (confirmedMessages.size() == 0 || (System.nanoTime() - firstConfirmTime) < CONFIRM_TIMEOUT)) {
            return null;
        }

        boolean useHighPriority = false;

        for (SchedullerPackage p : foundedPackages) {
            if (p.priority == PRIORITY_HIGH) {
                useHighPriority = true;
                break;
            }
        }

        ArrayList<SchedullerPackage> packages = new ArrayList<SchedullerPackage>();

        if (useHighPriority) {
            Logger.d("Scheduller", "Using high priority scheduling");
            int totalSize = 0;
            for (SchedullerPackage p : foundedPackages) {
                if (p.priority == PRIORITY_HIGH) {
                    packages.add(p);
                    totalSize += p.serialized.length;
                    if (totalSize > MAX_WORKLOAD_SIZE) {
                        break;
                    }
                }
            }
        } else {
            int totalSize = 0;
            for (SchedullerPackage p : foundedPackages) {
                packages.add(p);
                totalSize += p.serialized.length;
                if (totalSize > MAX_WORKLOAD_SIZE) {
                    break;
                }
            }
        }

        Logger.d("Scheduller", "Iteration: count: " + packages.size() + ", confirm:" + confirmedMessages.size());

        if (foundedPackages.size() == 0 && confirmedMessages.size() != 0) {
            MTMsgsAck ack = new MTMsgsAck(confirmedMessages.toArray(new Long[0]));
            confirmedMessages.clear();
            try {
                return new PreparedPackage(generateSeqNoWeak(), generateMessageId(), ack.serialize(), useHighPriority);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        } else if (foundedPackages.size() == 1 && confirmedMessages.size() == 0) {
            SchedullerPackage schedullerPackage = foundedPackages.get(0);
            schedullerPackage.state = STATE_SENT;
            if (schedullerPackage.idGenerationTime == 0) {
                schedullerPackage.idGenerationTime = getCurrentTime();
                schedullerPackage.messageId = generateMessageId();
                schedullerPackage.seqNo = generateSeqNo();
                schedullerPackage.relatedMessageIds.add(schedullerPackage.messageId);
            }
            schedullerPackage.writtenToChannel = contextId;
            schedullerPackage.lastAttemptTime = getCurrentTime();
            return new PreparedPackage(schedullerPackage.seqNo, schedullerPackage.messageId, schedullerPackage.serialized, useHighPriority);
        } else {
            MTMessagesContainer container = new MTMessagesContainer();
            if (confirmedMessages.size() > 0 && !useHighPriority) {
                try {
                    MTMsgsAck ack = new MTMsgsAck(confirmedMessages.toArray(new Long[0]));
                    container.getMessages().add(new MTMessage(generateMessageId(), generateSeqNoWeak(), ack.serialize()));
                    confirmedMessages.clear();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            for (SchedullerPackage schedullerPackage : foundedPackages) {
                schedullerPackage.state = STATE_SENT;
                if (schedullerPackage.idGenerationTime == 0) {
                    schedullerPackage.idGenerationTime = getCurrentTime();
                    schedullerPackage.messageId = generateMessageId();
                    schedullerPackage.seqNo = generateSeqNo();
                    schedullerPackage.relatedMessageIds.add(schedullerPackage.messageId);
                }
                schedullerPackage.writtenToChannel = contextId;
                schedullerPackage.lastAttemptTime = getCurrentTime();
                container.getMessages().add(new MTMessage(schedullerPackage.messageId, schedullerPackage.seqNo, schedullerPackage.serialized));
            }
            long containerMessageId = generateMessageId();
            int containerSeq = generateSeqNoWeak();

            for (SchedullerPackage schedullerPackage : foundedPackages) {
                schedullerPackage.relatedMessageIds.add(containerMessageId);
            }

            try {
                return new PreparedPackage(containerSeq, containerMessageId, container.serialize(), useHighPriority);
            } catch (IOException e) {
                // Might not happens
                e.printStackTrace();
                return null;
            }
        }
    }

    public void onConnectionDies(int connectionId) {
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            if (schedullerPackage.queuedToChannel != -1 && schedullerPackage.queuedToChannel == connectionId) {
                // messages.remove(schedullerPackage);
            } else {
                if (schedullerPackage.state == STATE_SENT && schedullerPackage.writtenToChannel == connectionId) {
                    schedullerPackage.state = STATE_QUEUED;
                    schedullerPackage.lastAttemptTime = 0;
                }
            }
        }
    }

    private static final int PRIORITY_HIGH = 1;
    private static final int PRIORITY_NORMAL = 0;

    private static final int STATE_QUEUED = 0;
    private static final int STATE_SENT = 1;
    private static final int STATE_CONFIRMED = 2;

    private class SchedullerPackage {

        public SchedullerPackage(int id) {
            this.id = id;
        }

        public int id;

        public TLObject object;
        public byte[] serialized;

        public long addTime;
        public long scheduleTime;
        public long expiresTime;
        public long lastAttemptTime;

        public int writtenToChannel = -1;

        public int queuedToChannel = -1;

        public int state = STATE_QUEUED;

        public int priority = PRIORITY_NORMAL;

        public long idGenerationTime;
        public long messageId;
        public int seqNo;
        public HashSet<Integer> relatedFastConfirm = new HashSet<Integer>();
        public HashSet<Long> relatedMessageIds = new HashSet<Long>();

        public boolean isRpc;
    }
}
