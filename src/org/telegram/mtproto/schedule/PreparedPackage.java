package org.telegram.mtproto.schedule;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 03.11.13
 * Time: 19:59
 */
public class PreparedPackage {
    private int seqNo;
    private long messageId;
    private byte[] content;

    public PreparedPackage(int seqNo, long messageId, byte[] content) {
        this.seqNo = seqNo;
        this.messageId = messageId;
        this.content = content;
    }

    public int getSeqNo() {
        return seqNo;
    }

    public long getMessageId() {
        return messageId;
    }

    public byte[] getContent() {
        return content;
    }
}
