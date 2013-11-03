package org.telegram.mtproto.tl;

import org.telegram.tl.TLContext;
import org.telegram.tl.TLObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.telegram.tl.StreamingUtils.*;

/**
 * Created with IntelliJ IDEA.
 * User: ex3ndr
 * Date: 03.11.13
 * Time: 8:45
 */
public class MTBadServerSalt extends TLObject {

    public static final int CLASS_ID = 0xedab447b;

    private long messageId;
    private int seqNo;
    private int errorNo;
    private byte[] newSalt;

    public MTBadServerSalt(long messageId, int seqNo, int errorNo, byte[] newSalt) {
        this.messageId = messageId;
        this.seqNo = seqNo;
        this.errorNo = errorNo;
        this.newSalt = newSalt;
    }

    public MTBadServerSalt() {

    }

    public long getMessageId() {
        return messageId;
    }

    public int getSeqNo() {
        return seqNo;
    }

    public int getErrorNo() {
        return errorNo;
    }

    public byte[] getNewSalt() {
        return newSalt;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void serializeBody(OutputStream stream) throws IOException {
        writeLong(messageId, stream);
        writeInt(seqNo, stream);
        writeInt(errorNo, stream);
        writeByteArray(newSalt, stream);
    }

    @Override
    public void deserializeBody(InputStream stream, TLContext context) throws IOException {
        messageId = readLong(stream);
        seqNo = readInt(stream);
        errorNo = readInt(stream);
        newSalt = readBytes(8, stream);
    }
}
