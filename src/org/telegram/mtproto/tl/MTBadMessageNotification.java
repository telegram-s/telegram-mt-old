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
 * Time: 8:47
 */
public class MTBadMessageNotification extends TLObject {

    public static final int CLASS_ID = 0xa7eff811;

    private long badMsgId;
    private int badMsqSeqno;
    private int errorCode;

    public MTBadMessageNotification(long badMsgId, int badMsqSeqno, int errorCode) {
        this.badMsgId = badMsgId;
        this.badMsqSeqno = badMsqSeqno;
        this.errorCode = errorCode;
    }

    public MTBadMessageNotification() {

    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    public long getBadMsgId() {
        return badMsgId;
    }

    public int getBadMsqSeqno() {
        return badMsqSeqno;
    }

    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public void serializeBody(OutputStream stream) throws IOException {
        writeLong(badMsgId, stream);
        writeInt(badMsqSeqno, stream);
        writeInt(errorCode, stream);
    }

    @Override
    public void deserializeBody(InputStream stream, TLContext context) throws IOException {
        badMsgId = readLong(stream);
        badMsqSeqno = readInt(stream);
        errorCode = readInt(stream);
    }
}
