package org.telegram.mtproto.schedule;

import org.telegram.mtproto.log.Logger;
import org.telegram.mtproto.time.TimeOverlord;
import org.telegram.mtproto.tl.MTMessage;
import org.telegram.mtproto.tl.MTMessagesContainer;
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
    private static final int MAX_WORKLOAD_SIZE = 1024;
    private static final long RETRY_TIMEOUT = 3 * 1000;

    private TreeMap<Integer, SchedullerPackage> messages = new TreeMap<Integer, SchedullerPackage>();
    private HashMap<Long, Long> idGenerationTime = new HashMap<Long, Long>();
    private HashSet<Long> currentMessageGeneration = new HashSet<Long>();

    private AtomicInteger messagesIds = new AtomicInteger(0);

    private long lastMessageId = 0;
    private int seqNo = 0;

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
        System.out.println("Generated time: " + new Date((messageId >> 32) * 1000).toString());
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

    public void postMessage(TLObject object, long timeout) {
        SchedullerPackage schedullerPackage = new SchedullerPackage();
        schedullerPackage.object = object;
        schedullerPackage.addTime = getCurrentTime();
        schedullerPackage.expiresTime = schedullerPackage.addTime + timeout;
        messages.put(messagesIds.incrementAndGet(), schedullerPackage);
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
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            boolean contains = false;
            for (Long relatedMsgId : schedullerPackage.relatedMessageIds) {
                if (relatedMsgId == msgId) {
                    contains = true;
                    break;
                }
            }
            if (contains) {
                schedullerPackage.idGenerationTime = 0;
                schedullerPackage.messageId = 0;
                schedullerPackage.seqNo = 0;
                schedullerPackage.relatedMessageIds.clear();
                schedullerPackage.state = STATE_QUEUED;
            }
        }
    }

    public void resendMessage(long msgId) {
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            boolean contains = false;
            for (Long relatedMsgId : schedullerPackage.relatedMessageIds) {
                if (relatedMsgId == msgId) {
                    contains = true;
                    break;
                }
            }
            if (contains) {
                schedullerPackage.relatedMessageIds.clear();
                schedullerPackage.state = STATE_QUEUED;
            }
        }
    }

    public void onMessageConfirmed(long msgId) {
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            if (schedullerPackage.state == STATE_SENT) {
                boolean contains = false;
                for (Long relatedMsgId : schedullerPackage.relatedMessageIds) {
                    if (relatedMsgId == msgId) {
                        contains = true;
                        break;
                    }
                }
                if (contains) {
                    schedullerPackage.state = STATE_CONFIRMED;
                }
            }
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

    public PreparedPackage doSchedule() {
        int totalSize = 0;
        ArrayList<SchedullerPackage> foundedPackages = new ArrayList<SchedullerPackage>();
        for (SchedullerPackage schedullerPackage : messages.values().toArray(new SchedullerPackage[0])) {
            boolean isPendingPackage = false;
            if (schedullerPackage.state == STATE_QUEUED) {
                isPendingPackage = true;
            } else if (schedullerPackage.state == STATE_SENT) {
                if (getCurrentTime() < schedullerPackage.expiresTime) {
                    if (getCurrentTime() - schedullerPackage.lastAttemptTime > RETRY_TIMEOUT) {
                        isPendingPackage = true;
                    }
                }
            }

            if (isPendingPackage) {
                if (schedullerPackage.serialized == null) {
                    try {
                        schedullerPackage.serialized = schedullerPackage.object.serialize();
                    } catch (IOException e) {
                        e.printStackTrace();
                        messages.remove(schedullerPackage);
                        continue;
                    }
                }

                foundedPackages.add(schedullerPackage);
                totalSize += schedullerPackage.serialized.length;

                if (totalSize > MAX_WORKLOAD_SIZE) {
                    break;
                }
            }
        }
        if (foundedPackages.size() == 0) {
            return null;
        }

        Logger.d("Scheduller", "PackageSize: " + totalSize + ", count: " + foundedPackages.size());

        if (foundedPackages.size() == 1) {
            SchedullerPackage schedullerPackage = foundedPackages.get(0);
            schedullerPackage.state = STATE_SENT;
            if (schedullerPackage.idGenerationTime == 0) {
                schedullerPackage.idGenerationTime = getCurrentTime();
                schedullerPackage.messageId = generateMessageId();
                schedullerPackage.seqNo = generateSeqNo();
                schedullerPackage.relatedMessageIds.add(schedullerPackage.messageId);
            }
            schedullerPackage.lastAttemptTime = getCurrentTime();
            return new PreparedPackage(schedullerPackage.seqNo, schedullerPackage.messageId, schedullerPackage.serialized);
        } else {
            MTMessagesContainer container = new MTMessagesContainer();
            for (SchedullerPackage schedullerPackage : foundedPackages) {
                schedullerPackage.state = STATE_SENT;
                if (schedullerPackage.idGenerationTime == 0) {
                    schedullerPackage.idGenerationTime = getCurrentTime();
                    schedullerPackage.messageId = generateMessageId();
                    schedullerPackage.seqNo = generateSeqNo();
                    schedullerPackage.relatedMessageIds.add(schedullerPackage.messageId);
                }
                schedullerPackage.lastAttemptTime = getCurrentTime();
                container.getMessages().add(new MTMessage(schedullerPackage.messageId, schedullerPackage.seqNo, schedullerPackage.serialized));
            }
            long containerMessageId = generateMessageId();
            int containerSeq = generateSeqNoWeak();

            for (SchedullerPackage schedullerPackage : foundedPackages) {
                schedullerPackage.relatedMessageIds.add(containerMessageId);
            }

            try {
                return new PreparedPackage(containerSeq, containerMessageId, container.serialize());
            } catch (IOException e) {
                // Might not happens
                e.printStackTrace();
                return null;
            }
        }
    }

    private static final int STATE_QUEUED = 0;
    private static final int STATE_SENT = 1;
    private static final int STATE_CONFIRMED = 2;

    private class SchedullerPackage {
        public TLObject object;
        public byte[] serialized;

        public long addTime;
        public long expiresTime;
        public long lastAttemptTime;
        public int state = STATE_QUEUED;

        public long idGenerationTime;
        public long messageId;
        public int seqNo;
        public ArrayList<Long> relatedMessageIds = new ArrayList<Long>();
    }
}
