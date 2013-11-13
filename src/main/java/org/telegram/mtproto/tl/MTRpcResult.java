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
 * Time: 22:06
 */
public class MTRpcResult extends TLObject {

    public static final int CLASS_ID = 0xf35c6d01;

    private long messageId;
    private byte[] content;

    public MTRpcResult(long messageId, byte[] content) {
        this.messageId = messageId;
        this.content = content;
    }

    public MTRpcResult() {

    }

    public long getMessageId() {
        return messageId;
    }

    public byte[] getContent() {
        return content;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void serializeBody(OutputStream stream) throws IOException {
        writeLong(messageId, stream);
        writeByteArray(content, stream);
    }

    @Override
    public void deserializeBody(InputStream stream, TLContext context) throws IOException {
        messageId = readLong(stream);
        content = readBytes(stream.available(), stream);
    }

    @Override
    public String toString() {
        return "rpc_result#f35c6d01";
    }
}
